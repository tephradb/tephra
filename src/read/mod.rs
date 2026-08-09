//! Layer 5: off-thread read paths.
//!
//! Reads run on the **caller's own thread**, over an immutable snapshot the writer
//! publishes at each commit. There is no reader pool and no channel hop, and the writer
//! thread is never touched: sealed segments are shared as immutable `Arc`s (lock-free), and
//! how far a read may see is an atomically published watermark.
//!
//! [`ReadCore`] holds the shared state; a cloneable [`ReadHandle`] runs a [`Query`] against
//! it with [`ReadHandle::read`], returning a lending [`Reads`] iterator of events in
//! ascending position order.
//!
//! ## Point-in-time semantics
//!
//! A [`Reads`] is pinned to the watermark read at call time ([`Reads::watermark`]): it
//! returns a consistent *prefix* of the log, not a live view, and a caller cannot tell "no
//! more events" from "no more events yet". That distinction is what a [`Subscription`]
//! adds: it holds a `cursor` (the last-delivered position, an exclusive lower bound), reads
//! `(cursor, watermark]` through this same path, advances the cursor to the pinned
//! watermark, and *blocks* until the watermark advances again. Catch-up and live-tail are
//! the one operation repeated, so the handoff has no gap (reads are `after`-exclusive) and
//! no duplicate (the cursor advances to the pinned watermark). The blocking is a condvar in
//! [`ReadCore`] the writer signals at each commit; the atomic watermark keeps the hot read
//! path lock-free.
//!
//! ## Snapshot / watermark ordering
//!
//! The writer publishes the segment set (on rollover) **before** the watermark (every
//! commit); a reader loads the watermark **before** the segment set. With acquire/release
//! ordering this guarantees the loaded snapshot always covers the loaded watermark: if a
//! reader observes watermark `W`, the segment set it then loads was published no earlier
//! than the one current when `W` was stored, and segment sets only grow.
//!
//! ## Index-driven reads
//!
//! The planner ([`estimate_matches`] + [`choose`])
//! picks between two modes per read by estimating the result size from exact posting
//! lengths: a **selective** query
//! is answered through the index (sealed segments via their on-disk index [`search`], the
//! **active** segment via a watermark-bounded [`ActiveView`](crate::index::ActiveView) over
//! the shared `Arc<ActiveTail>` the writer publishes), then each matching event
//! is fetched by position; a **broad** query streams a sequential log scan instead, avoiding
//! one random fetch per event. The choice only ever changes which correct path runs. A
//! degraded segment never answers short: an unindexable sealed segment, or an active segment
//! that latched unindexable, falls back to scanning its own range regardless of the verdict.

use std::cmp::Ordering;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering as AtomicOrdering};
use std::sync::{Arc, Condvar, Mutex, RwLock};
use std::time::Duration;

use thiserror::Error;

use seglog::read::Reader;

use crate::Position;
use crate::event::{DecodeError, Event, EventRef};
use crate::index::{
    Access, ActiveTail, IndexSegment, SegmentIndex, choose, estimate_matches, search,
};
use crate::log::set::{LogError, Record, Scan, Segment, SegmentSet, SegmentSource};
use crate::query::Query;

use crate::index::IndexSet;

mod subscribe;

pub use subscribe::Subscription;

/// Configuration for the read paths: the index-vs-scan cost model's tuning.
#[derive(Clone, Copy, Debug)]
pub struct ReadConfig {
    /// The planner's `K`: the index is chosen only when the post-pruning range is at least
    /// `scan_bias` times the estimated result count, so larger values bias toward scanning
    /// at the margin and `1` disables the bias (index whenever the estimate does not exceed
    /// the range). Provisional default pending the read-path benchmark; it changes only
    /// which correct path runs, never the answer.
    pub scan_bias: u32,
}

impl Default for ReadConfig {
    fn default() -> Self {
        // Index when the estimated result is at most ~1/4 of the range. A placeholder until
        // `benches/read_path.rs` locates the real crossover (the deliverable is the
        // benchmark, not a tuned `K`).
        ReadConfig { scan_bias: 4 }
    }
}

/// An immutable snapshot of what is readable: the sealed log segments and their on-disk
/// indexes (aligned one-for-one), plus the active log segment. Shared behind an `Arc`;
/// grown only on rollover.
///
/// Implements [`SegmentSource`] so the one zero-copy [`Scan`] serves reads unchanged:
/// sealed segments occupy logical indices `0..sealed_log.len()`, the
/// active segment sits at `sealed_log.len()`.
pub struct Snapshot {
    header_size: u64,
    sealed_log: Vec<Arc<Segment>>,
    /// Aligned with `sealed_log`: `sealed_index[i]` indexes `sealed_log[i]`, or `None` if
    /// that segment is unindexable (a query touching it scans the log for its range).
    sealed_index: Vec<Option<Arc<IndexSegment>>>,
    active_log: Arc<Segment>,
    /// The active segment's shared in-memory index. A reader queries it lock-free through a
    /// watermark-bounded [`ActiveView`](crate::index::ActiveView), so the active range is
    /// index-driven like the sealed ranges rather than scanned.
    active_index: Arc<ActiveTail>,
}

impl Snapshot {
    /// Captures the writer's current segments. Called on the writer thread at a rollover
    /// (and once at startup), so `set`'s sealed segments and `index`'s sealed indexes are
    /// aligned.
    pub(crate) fn capture(set: &SegmentSet, index: &IndexSet) -> Snapshot {
        let sealed_log: Vec<Arc<Segment>> = set.sealed_arcs().to_vec();
        let index_arcs = index.sealed_index_arcs();
        // Align defensively: index and log seal together, but never index a log segment we
        // do not have an index handle for.
        let sealed_index: Vec<Option<Arc<IndexSegment>>> = (0..sealed_log.len())
            .map(|i| index_arcs.get(i).cloned().flatten())
            .collect();
        Snapshot {
            header_size: set.header_size(),
            sealed_log,
            sealed_index,
            active_log: set.active_arc(),
            active_index: index.active_tail_arc(),
        }
    }
}

// The snapshot crosses to reader threads as an immutable `Arc`, so it must be `Send + Sync`.
// Its active index (`Arc<ActiveTail>`) is the only interior-mutable member; locking this in
// keeps a future non-`Sync` addition from silently regressing the read path.
const _: fn() = || {
    fn is_send<T: Send>() {}
    fn is_sync<T: Sync>() {}
    is_send::<Snapshot>();
    is_sync::<Snapshot>();
};

impl SegmentSource for Snapshot {
    fn header_size(&self) -> u64 {
        self.header_size
    }

    fn segment_count(&self) -> usize {
        self.sealed_log.len()
    }

    fn segment_at(&self, idx: usize) -> Option<&Arc<Segment>> {
        match idx.cmp(&self.sealed_log.len()) {
            Ordering::Less => self.sealed_log.get(idx),
            Ordering::Equal => Some(&self.active_log),
            Ordering::Greater => None,
        }
    }
}

/// The parking side of the wakeup: a condvar subscribers block on until the watermark
/// advances, used **only** by [`Subscription`]. The hot read path never touches it; it reads
/// the atomic [`ReadCore::watermark`] directly. `subscribers` gates the writer's `wake` so a
/// commit with no subscribers pays one atomic load instead of a mutex acquire.
struct Notify {
    /// Guards nothing but the condvar wait/notify handshake (the state lives in the atomics).
    lock: Mutex<()>,
    cv: Condvar,
    /// Set once at coordinator shutdown; a parked subscriber wakes and reports `Closed`.
    closed: AtomicBool,
    /// Live subscriber count, so `wake` can skip the lock when there are none.
    subscribers: AtomicUsize,
}

/// The shared read state, held by both the writer thread (to publish) and every
/// [`ReadHandle`] (to read). Cheap to share: an `Arc<ReadCore>`.
pub struct ReadCore {
    /// The current segment snapshot. Swapped only on rollover (a normal commit leaves it
    /// untouched), so cloning the inner `Arc` under a brief read lock is a single op, never
    /// held across query evaluation.
    segments: RwLock<Arc<Snapshot>>,
    /// Last durable (and index-fed) position. Stored on every commit.
    watermark: AtomicU64,
    /// Subscriber parking, separate from the lock-free watermark (see [`Notify`]).
    notify: Notify,
}

/// The outcome of a bounded [`Subscription::wait_timeout`]: the watermark advanced, the wait
/// timed out (retry, or check an external shutdown flag), or the store closed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WaitOutcome {
    /// The watermark advanced past the cursor; new events are available.
    Advanced,
    /// The timeout elapsed with no advance and no close.
    TimedOut,
    /// The write coordinator shut down; no further events will ever arrive.
    Closed,
}

impl ReadCore {
    /// The initial core, capturing `set`/`index` as they stand and pinning the watermark to
    /// the current tip.
    pub(crate) fn new(set: &SegmentSet, index: &IndexSet) -> Arc<ReadCore> {
        Arc::new(ReadCore {
            segments: RwLock::new(Arc::new(Snapshot::capture(set, index))),
            watermark: AtomicU64::new(set.last_position().get()),
            notify: Notify {
                lock: Mutex::new(()),
                cv: Condvar::new(),
                closed: AtomicBool::new(false),
                subscribers: AtomicUsize::new(0),
            },
        })
    }

    /// Publishes a new segment snapshot (writer thread, on rollover). Must run **before**
    /// [`publish_watermark`](Self::publish_watermark) for a batch, so a reader that observes
    /// the new watermark also sees the segment covering it.
    pub(crate) fn publish_segments(&self, snapshot: Snapshot) {
        *self.segments.write().unwrap() = Arc::new(snapshot);
    }

    /// Publishes the durable tip (writer thread, every commit). `SeqCst` (which is also a
    /// release, preserving the snapshot-before-watermark ordering readers rely on): the
    /// stronger order is load-bearing for the subscriber-wakeup gate in [`wake`](Self::wake),
    /// where this store, the count increment, and the count load form a store-buffer pattern
    /// that only a single total order rules out. One `mfence`-class barrier per commit,
    /// negligible against the fsync on the same path.
    pub(crate) fn publish_watermark(&self, tip: Position) {
        self.watermark.store(tip.get(), AtomicOrdering::SeqCst);
    }

    /// Loads a consistent `(watermark, snapshot)` pair: watermark first (acquire), then the
    /// snapshot, so the snapshot always covers the watermark (see the module ordering note).
    fn load(&self) -> (Position, Arc<Snapshot>) {
        let watermark = Position::new(self.watermark.load(AtomicOrdering::Acquire));
        let snapshot = Arc::clone(&self.segments.read().unwrap());
        (watermark, snapshot)
    }

    /// The watermark for the parked-subscriber gate decision in [`wait_past`](Self::wait_past),
    /// loaded `SeqCst`. This is the **fourth** operation of the store-buffer pattern in
    /// [`wake`](Self::wake): an `Acquire` load here would not be ordered against the writer's
    /// `SeqCst` watermark store and count load in the single total order, so on a weakly-ordered
    /// target it could be satisfied before the count increment is globally visible, letting a
    /// `wake` read count `0`, skip the notify, and leave this subscriber parked below a newer
    /// tip. `SeqCst` closes that hole.
    fn watermark_gate(&self) -> Position {
        Position::new(self.watermark.load(AtomicOrdering::SeqCst))
    }

    /// Wakes every parked subscriber (writer thread, after [`publish_watermark`] on every
    /// commit). Gated on the subscriber count so a commit with no subscribers pays a single
    /// atomic load, not a mutex acquire.
    ///
    /// The gate is a store-buffer (Dekker) pattern, forbidden only when **all four** of its
    /// accesses are `SeqCst`: this store's preceding `publish_watermark` (watermark store), the
    /// count `load` here, the count `fetch_add` in
    /// [`register_subscriber`](Self::register_subscriber), and the parked subscriber's watermark
    /// load ([`watermark_gate`](Self::watermark_gate) in [`wait_past`](Self::wait_past)).
    /// Register runs (in program order) before a subscriber first reads the watermark. In the
    /// single `SeqCst` total order, a `wake` that reads count `0` is ordered before that
    /// increment, hence its watermark store is ordered before the subscriber's gate read, so the
    /// subscriber observes this commit's tip and never parks below it. A `wake` that instead
    /// sees the increment takes the lock, and the lock-guarded re-read of the watermark in
    /// `wait_past` is the ordinary condvar handshake. Either way no wakeup is lost. (The count
    /// only rises to 1 once per subscription and stays until drop, so the same argument covers
    /// every later park, not just the first.) An `Acquire` gate read would leave the fourth
    /// access unordered and reintroduce the lost wakeup on a weakly-ordered target.
    pub(crate) fn wake(&self) {
        if self.notify.subscribers.load(AtomicOrdering::SeqCst) == 0 {
            return;
        }
        let _guard = self.notify.lock.lock().unwrap();
        self.notify.cv.notify_all();
    }

    /// Marks the store closed and wakes every parked subscriber (writer thread, at shutdown).
    /// `closed` is set (release) **before** taking the lock, so a waiter that evaluates
    /// `closed` under the lock and decides to park cannot miss a close whose `notify_all` is
    /// still pending (that notify also needs the lock).
    pub(crate) fn close(&self) {
        self.notify.closed.store(true, AtomicOrdering::Release);
        let _guard = self.notify.lock.lock().unwrap();
        self.notify.cv.notify_all();
    }

    /// Registers a new subscriber. Called from `Subscription::new` **before** the subscription
    /// first reads the watermark (via `poll_batch`/`wait`), which is the ordering [`wake`] and
    /// the `SeqCst` increment depend on: the increment must precede the first watermark read,
    /// not merely the first `wait`.
    fn register_subscriber(&self) {
        self.notify.subscribers.fetch_add(1, AtomicOrdering::SeqCst);
    }

    /// Balances [`register_subscriber`](Self::register_subscriber) on [`Subscription`] drop.
    /// `Relaxed` is fine: an over-count only costs a spurious `wake` lock, never a lost wakeup.
    fn deregister_subscriber(&self) {
        self.notify
            .subscribers
            .fetch_sub(1, AtomicOrdering::Relaxed);
    }

    /// Blocks until the watermark advances past `cursor` or the store closes. With
    /// `timeout = None` it waits indefinitely (returning only `Advanced` or `Closed`); with a
    /// timeout it may also return `TimedOut`. The watermark is re-read (`SeqCst`, see
    /// [`watermark_gate`](Self::watermark_gate)) under `lock` on each pass, which is what makes
    /// the wakeup lossless against the writer's watermark store + count-gated notify (see
    /// [`wake`](Self::wake)).
    fn wait_past(&self, cursor: Position, timeout: Option<Duration>) -> WaitOutcome {
        let mut guard = self.notify.lock.lock().unwrap();
        loop {
            if self.notify.closed.load(AtomicOrdering::Acquire) {
                return WaitOutcome::Closed;
            }
            if self.watermark_gate() > cursor {
                return WaitOutcome::Advanced;
            }
            match timeout {
                None => guard = self.notify.cv.wait(guard).unwrap(),
                Some(dur) => {
                    let (g, res) = self.notify.cv.wait_timeout(guard, dur).unwrap();
                    guard = g;
                    if res.timed_out() {
                        // Re-check the predicate once more before reporting a timeout: an
                        // advance or close may have landed as the timeout fired.
                        if self.notify.closed.load(AtomicOrdering::Acquire) {
                            return WaitOutcome::Closed;
                        }
                        if self.watermark_gate() > cursor {
                            return WaitOutcome::Advanced;
                        }
                        return WaitOutcome::TimedOut;
                    }
                }
            }
        }
    }
}

/// A cloneable, `Send + Sync` handle for reads. Every clone shares the same [`ReadCore`];
/// reads run on the calling thread and never touch the writer thread.
#[derive(Clone)]
pub struct ReadHandle {
    core: Arc<ReadCore>,
    config: ReadConfig,
}

impl ReadHandle {
    pub(crate) fn new(core: Arc<ReadCore>, config: ReadConfig) -> ReadHandle {
        ReadHandle { core, config }
    }

    /// Reads events matching `query`, ascending, strictly after `after`, up to the
    /// watermark pinned now. The result is a lending iterator (it borrows its own decode
    /// buffer per item), so consume it with `while let Some(item) = reads.next()`.
    pub fn read(&self, query: Query, after: Position) -> Reads {
        let (watermark, snapshot) = self.core.load();
        Reads::plan(snapshot, &query, after, watermark, &self.config)
    }

    /// Starts a [`Subscription`] over `query`, resuming strictly after `after`: it catches up
    /// on everything already durable, then tails live events with no gap and no duplicate at
    /// the boundary. See [`Subscription`].
    pub fn subscribe(&self, query: Query, after: Position) -> Subscription {
        Subscription::new(Arc::clone(&self.core), self.config, query, after)
    }
}

/// One event yielded by [`Reads`]: its global position and a borrowed view. The borrow is
/// valid only until the next [`Reads::next`] call.
#[derive(Clone, Copy, Debug)]
pub struct Sequenced<'a> {
    pub position: Position,
    pub event: EventRef<'a>,
}

/// Why a read failed. Every variant is an integrity or I/O failure; a query that simply
/// matches nothing yields an empty stream, not an error.
#[derive(Debug, Error)]
pub enum ReadError {
    #[error("log error during read: {0}")]
    Log(Arc<LogError>),
    #[error("corrupt event during read: {0}")]
    Corrupt(DecodeError),
}

/// A lending iterator over the events matching a read, in ascending position order.
///
/// Three internal shapes (chosen by `Reads::plan` via the cost model): the bypass path
/// streams a log scan, either unfiltered (`Query::all`, zero-copy) or filtered (a broad
/// query, copying one matched record at a time); the indexed path plans the ascending
/// *positions* (a `Vec<Position>`, cheap `u64`s, small for a selective query) and fetches
/// each event on demand into a single-record buffer it lends from. None buffers a growing
/// *event* result; only the indexed path materializes a position list, and the planner routes
/// broad results to the streaming bypass path instead.
pub struct Reads {
    watermark: Position,
    pending_err: Option<ReadError>,
    mode: Mode,
}

/// A reader cached for the segment of the last fetched position, reused across consecutive
/// positions in the same segment (planned positions are ascending, so they cluster by
/// segment), rather than opening one fd per event.
struct CachedReader {
    seg_idx: usize,
    segment: Arc<Segment>,
    reader: Reader<0>,
}

/// The indexed read path's state: an ascending, watermark-clamped position list, the reader
/// cached for the last segment, and the single-record buffer events are lent from.
struct IndexedState {
    snapshot: Arc<Snapshot>,
    positions: std::vec::IntoIter<Position>,
    reader: Option<CachedReader>,
    buf: Option<Record>,
}

/// The filtered-scan path's state: the streaming scan, the query to filter by, and a
/// single-event buffer each matched record is copied into (as an owned [`Event`], which
/// carries the decode offset computed for the filter, so yielding it needs no second parse)
/// so the yielded event borrows the buffer rather than the scan.
struct ScanFilteredState {
    scan: Scan<Arc<Snapshot>>,
    query: Query,
    buf: Option<(Position, Event)>,
}

enum Mode {
    /// Bypass, unfiltered: a zero-copy streaming scan of the whole `(after, watermark]`
    /// range, yielding every record. This is the `Query::all` projection-catch-up path, the
    /// highest-volume read, kept at disk bandwidth with no per-event copy.
    Scan { scan: Box<Scan<Arc<Snapshot>>> },
    /// Bypass, filtered: the same streaming scan, but keeping only records matching `query`.
    /// The planner routes a broad (but not full-log) query here so a large result set never
    /// materializes a position list or refetches. Each matched record is copied out because a
    /// lending iterator cannot conditionally return its borrow from a loop on stable Rust; the
    /// copy is paid only per *matched* event, the same as the indexed path's per-event buffer.
    ScanFiltered(Box<ScanFilteredState>),
    /// Indexed: a pre-planned ascending position list (already clamped to the watermark),
    /// each event fetched on demand and lent from `buf`, reusing one reader per segment.
    Indexed(Box<IndexedState>),
    /// Nothing to yield (empty range, or a leading planning error carried in `pending_err`).
    Done,
}

impl Reads {
    /// The watermark this read was pinned to. A later read or subscription resumed from here
    /// continues with no gap (the subscription resume seam).
    pub fn watermark(&self) -> Position {
        self.watermark
    }

    /// Plans the read. Estimates the result size from exact posting lengths and
    /// picks the cheaper mode: a broad query streams a filtered log scan
    /// ([`Access::Scan`]), a selective one gathers positions from the index and fetches
    /// events on demand ([`Access::Index`]). The choice only ever changes which correct path
    /// runs: both return the identical positions.
    fn plan(
        snapshot: Arc<Snapshot>,
        query: &Query,
        after: Position,
        watermark: Position,
        config: &ReadConfig,
    ) -> Reads {
        if after >= watermark {
            return Reads {
                watermark,
                pending_err: None,
                mode: Mode::Done,
            };
        }

        let (estimate, width) = estimate_read(&snapshot, query, after, watermark);
        let access = choose(estimate, width, config.scan_bias);
        #[cfg(feature = "tracing")]
        tracing::debug!(
            ?access,
            estimate,
            width,
            scan_bias = config.scan_bias,
            "read planner verdict"
        );

        match access {
            // Broad: stream a scan of the whole range, never materializing positions. A
            // full-log query needs no filter and stays zero-copy; any other broad query
            // filters (and copies) per matched event. The `Query::all` check picks the
            // cheaper scan *implementation*; it does not bypass the cost model, which already
            // routed the query here.
            Access::Scan => {
                let scan = Scan::start(Arc::clone(&snapshot), after.next(), watermark);
                // Only the filtered-scan mode retains the query, so it is the one path that
                // clones; the zero-copy full-log scan and the indexed path borrow it and drop
                // the borrow here. This keeps a repeated caller (a subscription polling every
                // round) from re-allocating the query on the index and full-scan paths.
                let mode = if matches!(*query, Query::All) {
                    Mode::Scan {
                        scan: Box::new(scan),
                    }
                } else {
                    Mode::ScanFiltered(Box::new(ScanFilteredState {
                        scan,
                        query: query.clone(),
                        buf: None,
                    }))
                };
                Reads {
                    watermark,
                    pending_err: None,
                    mode,
                }
            }
            // Selective: plan the ascending positions from the index, fetch on demand.
            Access::Index => match plan_positions(&snapshot, query, after, watermark) {
                Ok(positions) => Reads {
                    watermark,
                    pending_err: None,
                    mode: Mode::Indexed(Box::new(IndexedState {
                        snapshot,
                        positions: positions.into_iter(),
                        reader: None,
                        buf: None,
                    })),
                },
                Err(err) => Reads {
                    watermark,
                    pending_err: Some(err),
                    mode: Mode::Done,
                },
            },
        }
    }

    /// The next matching event, or `None` at the end. A lending iterator (it yields a borrow
    /// of `self`), so it is not `std::iter::Iterator`.
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> Option<Result<Sequenced<'_>, ReadError>> {
        if let Some(err) = self.pending_err.take() {
            self.mode = Mode::Done;
            return Some(Err(err));
        }
        match &mut self.mode {
            Mode::Done => None,
            // Unfiltered: yield every record, zero-copy (the event borrows the scan buffer).
            Mode::Scan { scan } => match scan.next()? {
                Ok(record) => {
                    let position = record.position;
                    match EventRef::from_bytes(record.data) {
                        Ok(event) => Some(Ok(Sequenced { position, event })),
                        Err(err) => Some(Err(ReadError::Corrupt(err))),
                    }
                }
                Err(err) => Some(Err(ReadError::Log(Arc::new(err)))),
            },
            // Filtered: advance to the next record matching `query`, copying the match into an
            // owned `Event` so the yielded event borrows `buf` (a lending iterator cannot
            // conditionally return its borrow of the scan from a loop). Decoding happens once:
            // the filter decode's offset is preserved by `to_owned`, so `as_ref` on the way out
            // reparses nothing. The filter is the same `Query::matches` the scan oracle uses, so
            // this yields exactly the indexed path's positions.
            Mode::ScanFiltered(state) => {
                let ScanFilteredState { scan, query, buf } = state.as_mut();
                loop {
                    match scan.next()? {
                        Ok(record) => {
                            let event = match EventRef::from_bytes(record.data) {
                                Ok(event) => event,
                                Err(err) => return Some(Err(ReadError::Corrupt(err))),
                            };
                            if query.matches(event) {
                                *buf = Some((record.position, event.to_owned()));
                                break;
                            }
                        }
                        Err(err) => return Some(Err(ReadError::Log(Arc::new(err)))),
                    }
                }
                let (position, event) = buf.as_ref().unwrap();
                Some(Ok(Sequenced {
                    position: *position,
                    event: event.as_ref(),
                }))
            }
            Mode::Indexed(state) => {
                let IndexedState {
                    snapshot,
                    positions,
                    reader,
                    buf,
                } = state.as_mut();
                let position = positions.next()?;
                // Locate the owning segment (the shared lookup), opening a fresh reader only
                // when the segment changes from the previous position.
                let Some((seg_idx, _)) = snapshot.locate(position) else {
                    return Some(Err(ReadError::Log(Arc::new(LogError::NotFound {
                        position,
                    }))));
                };
                if reader.as_ref().map(|c| c.seg_idx) != Some(seg_idx) {
                    let segment = Arc::clone(snapshot.segment_at(seg_idx).unwrap());
                    match segment.open_reader() {
                        Ok(r) => {
                            *reader = Some(CachedReader {
                                seg_idx,
                                segment,
                                reader: r,
                            })
                        }
                        Err(err) => return Some(Err(ReadError::Log(Arc::new(err)))),
                    }
                }
                let cached = reader.as_mut().unwrap();
                let local = (position.get() - cached.segment.base_position().get()) as usize;
                match cached.segment.read_at_local(&mut cached.reader, local) {
                    Ok(Some(record)) => {
                        *buf = Some(record);
                        let bytes = &buf.as_ref().unwrap().data;
                        match EventRef::from_bytes(bytes) {
                            Ok(event) => Some(Ok(Sequenced { position, event })),
                            Err(err) => Some(Err(ReadError::Corrupt(err))),
                        }
                    }
                    // A planned position at or below the watermark must exist; a miss is an
                    // integrity failure, not a normal empty result.
                    Ok(None) => Some(Err(ReadError::Log(Arc::new(LogError::NotFound {
                        position,
                    })))),
                    Err(err) => Some(Err(ReadError::Log(Arc::new(err)))),
                }
            }
        }
    }

    /// Collects the whole read into owned `(position, event)` pairs. Convenience for
    /// callers that want the full result at once (and for tests); the streaming `next` is
    /// the primitive.
    pub fn collect_owned(mut self) -> Result<Vec<(Position, crate::event::Event)>, ReadError> {
        let mut out = Vec::new();
        while let Some(item) = self.next() {
            let seq = item?;
            out.push((seq.position, seq.event.to_owned()));
        }
        Ok(out)
    }
}

/// Estimates the read's result size and its post-pruning range width for the planner
/// ([`choose`]). Mirrors [`plan_positions`]'s pruning so the estimate covers exactly the
/// segments the read will touch: sealed segments through their index, plus the active tail.
/// An unindexable segment (sealed, or the active tail latched unindexable) is treated as
/// fully broad, since the indexed path would scan its whole range anyway. The estimate is an
/// upper bound, capped at the width, so the verdict only ever errs toward scanning.
fn estimate_read(
    snapshot: &Snapshot,
    query: &Query,
    after: Position,
    watermark: Position,
) -> (u64, u64) {
    let wm = watermark.get();
    let mut estimate: u64 = 0;
    let mut width: u64 = 0;

    for (seg, index) in snapshot.sealed_log.iter().zip(snapshot.sealed_index.iter()) {
        let base = seg.base_position();
        let count = seg.event_count();
        if count == 0 {
            continue;
        }
        let effective_max = (base.get() + count - 1).min(wm);
        if effective_max <= after.get() {
            continue;
        }
        let seg_width = segment_width(after, base, effective_max);
        width += seg_width;
        estimate += match index {
            Some(index_seg) => estimate_segment(index_seg.as_ref(), query, seg_width),
            // Unindexable: scanned wholesale by the indexed path, so as broad as it gets.
            None => seg_width,
        };
    }

    let active_base = snapshot.active_log.base_position();
    if watermark >= active_base {
        let seg_width = segment_width(after, active_base, wm);
        width += seg_width;
        estimate += if snapshot.active_index.is_unindexable() {
            seg_width
        } else {
            estimate_segment(&snapshot.active_index.view(watermark), query, seg_width)
        };
    }

    (estimate.min(width), width)
}

/// The number of positions in `(after, effective_max]` that fall within a segment based at
/// `base`: `[max(after + 1, base), effective_max]`, or `0` if that range is empty.
fn segment_width(after: Position, base: Position, effective_max: u64) -> u64 {
    let first = first_after(after, base).get();
    (effective_max + 1).saturating_sub(first)
}

/// The per-segment result estimate ([`estimate_matches`]), also emitting one per-item trace
/// line so a mis-chosen plan can be attributed to the item whose estimate was off (and so
/// the per-item data that would justify per-item mode mixing is captured).
fn estimate_segment<I: SegmentIndex>(index: &I, query: &Query, seg_width: u64) -> u64 {
    let estimate = estimate_matches(index, query, seg_width);
    #[cfg(feature = "tracing")]
    if let Query::Items(items) = query {
        for (item, spec) in items.iter().enumerate() {
            tracing::trace!(
                segment_base = index.base().get(),
                item,
                estimate = crate::index::estimate_item(index, spec, seg_width),
                seg_width,
                "planner per-item estimate"
            );
        }
    }
    estimate
}

/// Gathers the ascending positions matching `query` in `(after, watermark]`: sealed
/// segments through their index (or a scan of their range if unindexable), then the active
/// segment's range through a bounded scan. Sealed segments are disjoint and ordered and the
/// active range is last, so concatenating in this order is already globally ascending.
fn plan_positions(
    snapshot: &Arc<Snapshot>,
    query: &Query,
    after: Position,
    watermark: Position,
) -> Result<Vec<Position>, ReadError> {
    let mut out = Vec::new();
    let wm = watermark.get();

    for (seg, index) in snapshot.sealed_log.iter().zip(snapshot.sealed_index.iter()) {
        let base = seg.base_position();
        let count = seg.event_count();
        if count == 0 {
            continue;
        }
        // Clamp the upper bound to the pinned watermark. `load` reads the watermark, then
        // clones a possibly-newer snapshot, so a sealed segment can extend past `watermark`
        // (a rollover raced this read). Never plan a position the read may not see, or
        // `read_at_local` later reports a spurious NotFound for it.
        let effective_max = (base.get() + count - 1).min(wm);
        if effective_max <= after.get() {
            continue; // nothing in `(after, watermark]` here
        }
        match index {
            // Indexed: take the ascending postings up to the watermark.
            Some(index_seg) => {
                out.extend(search(index_seg.as_ref(), query, after).take_while(|p| p.get() <= wm))
            }
            // Unindexable sealed segment: scan its own (watermark-clamped) range rather
            // than answer short.
            None => scan_positions_into(
                snapshot,
                query,
                first_after(after, base),
                Position::new(effective_max),
                &mut out,
            )?,
        }
    }

    // Active segment's range: query the shared active tail through a watermark-bounded view,
    // index-driven like the sealed segments rather than scanned. The view exposes
    // only locals at or before the pinned watermark, so it never yields a position past `wm`,
    // and its positions are all above every sealed segment's (disjoint, active range last), so
    // the concatenation stays globally ascending.
    //
    // Unless the active segment latched unindexable: then its columns are truncated relative to
    // the watermark (feeding stopped but positions kept arriving), so the view would answer
    // short or wrong. Fall back to a log scan of the range, mirroring the sealed `None` arm.
    // The flag is read live off the shared tail because the latch fires mid-segment with no
    // snapshot republish.
    let active_base = snapshot.active_log.base_position();
    if watermark >= active_base {
        if snapshot.active_index.is_unindexable() {
            scan_positions_into(
                snapshot,
                query,
                first_after(after, active_base),
                watermark,
                &mut out,
            )?;
        } else {
            let view = snapshot.active_index.view(watermark);
            out.extend(search(&view, query, after));
        }
    }

    Ok(out)
}

/// Inclusive start of the `(after, ...]` range within a segment based at `base`: the first
/// position both strictly greater than `after` and not before `base`, i.e.
/// `max(after + 1, base)`.
fn first_after(after: Position, base: Position) -> Position {
    Position::new(after.get().max(base.get().saturating_sub(1)) + 1)
}

/// Scans `first..=upto` and appends the positions of matching events (ascending) to `out`.
fn scan_positions_into(
    snapshot: &Arc<Snapshot>,
    query: &Query,
    first: Position,
    upto: Position,
    out: &mut Vec<Position>,
) -> Result<(), ReadError> {
    let mut scan = Scan::start(Arc::clone(snapshot), first, upto);
    while let Some(item) = scan.next() {
        let record = item.map_err(|err| ReadError::Log(Arc::new(err)))?;
        let event = EventRef::from_bytes(record.data).map_err(ReadError::Corrupt)?;
        if query.matches(event) {
            out.push(record.position);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::{Event, EventType, Tag, Tags};
    use crate::index::IndexSet;
    use crate::log::set::SegmentConfig;
    use crate::query::QueryItem;
    use smallvec::SmallVec;
    use tempfile::TempDir;

    fn tags(items: &[&str]) -> Tags {
        Tags::new(
            items
                .iter()
                .map(|s| Tag::new(s).unwrap())
                .collect::<SmallVec<[Tag; 4]>>(),
        )
        .unwrap()
    }

    fn event(ty: &str, tag_strs: &[&str]) -> Event {
        Event::new(&EventType::new(ty).unwrap(), &tags(tag_strs), b"x").unwrap()
    }

    /// The clamp guarding finding-#1's race: `load` reads the watermark, then clones a
    /// possibly-newer snapshot, so a snapshot can carry sealed (and active) events past the
    /// pinned watermark. `plan_positions` must never plan a position beyond it, on either the
    /// indexed sealed arm or the active-range scan. Driving `plan_positions` directly with a
    /// watermark below the true tip reproduces the effect deterministically, without racing.
    #[test]
    fn plan_positions_is_clamped_to_the_pinned_watermark() {
        let dir = TempDir::new().unwrap();
        // Tiny segments so the log seals several indexed segments; every event matches the
        // query, so the expected answer is exactly the dense prefix up to the watermark.
        let mut set = SegmentSet::open(dir.path(), SegmentConfig::new(512)).unwrap();
        for _ in 0..60 {
            set.append_batch(&[event("Enrolled", &["course:c1"]).as_bytes()])
                .unwrap();
        }
        assert!(set.sealed_len() >= 2, "need several sealed segments");
        let index = IndexSet::open(&set).unwrap();
        let snapshot = Arc::new(Snapshot::capture(&set, &index));

        let query = Query::item(QueryItem::with_tags(tags(&["course:c1"])));
        let last = set.last_position().get();

        // For every watermark below the true tip (spanning sealed-indexed and active
        // ranges), the plan is exactly 1..=watermark: nothing past it leaks in.
        for wm in 1..=last {
            let planned =
                plan_positions(&snapshot, &query, Position::ZERO, Position::new(wm)).unwrap();
            let got: Vec<u64> = planned.iter().map(|p| p.get()).collect();
            let expected: Vec<u64> = (1..=wm).collect();
            assert_eq!(got, expected, "watermark {wm}");
        }
    }

    /// Regression: when the active segment latches unindexable, feeding stops but positions
    /// keep arriving, so the tail's columns are truncated relative to the watermark. A reader
    /// must not trust the short tail; it scans the log for the active range instead (mirroring
    /// the sealed unindexable arm). Reproduced white-box with a tail deliberately fed
    /// only a prefix of a fully-durable single segment, then latched.
    #[test]
    fn active_unindexable_reader_scans_the_log_for_complete_results() {
        let dir = TempDir::new().unwrap();
        // One roomy segment: all six events land in the active segment, no rollover.
        let mut set = SegmentSet::open(dir.path(), SegmentConfig::new(1 << 16)).unwrap();
        for _ in 0..6 {
            set.append_batch(&[event("Enrolled", &["course:c1"]).as_bytes()])
                .unwrap();
        }
        let last = set.last_position(); // 6

        // A tail fed only the first four of the six durable events, then latched unindexable:
        // exactly the state `IndexSet` reaches when the fifth event trips the u16 type limit.
        let active_base = set.active_base();
        let tail = ActiveTail::new(active_base);
        let mut scan = set.scan_from(active_base);
        for _ in 0..4 {
            let record = scan.next().unwrap().unwrap();
            let ev = EventRef::from_bytes(record.data).unwrap();
            tail.push(record.position, ev).unwrap();
        }
        drop(scan);
        tail.mark_unindexable();
        assert_eq!(
            tail.len(),
            4,
            "tail is truncated below the durable tip of 6"
        );

        let snapshot = Arc::new(Snapshot {
            header_size: set.header_size(),
            sealed_log: Vec::new(),
            sealed_index: Vec::new(),
            active_log: set.active_arc(),
            active_index: Arc::new(tail),
        });

        // Every event matches, so the complete answer is the dense 1..=6. The truncated tail
        // alone would return only 1..=4 (short); the scan fallback restores completeness.
        let query = Query::item(QueryItem::with_tags(tags(&["course:c1"])));
        let planned = plan_positions(&snapshot, &query, Position::ZERO, last).unwrap();
        let got: Vec<u64> = planned.iter().map(|p| p.get()).collect();
        assert_eq!(got, (1..=6).collect::<Vec<_>>());
    }
}
