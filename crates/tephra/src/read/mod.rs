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
use std::vec::IntoIter;

use thiserror::Error;

use seglog::read::Reader;

use crate::Position;
use crate::event::{DecodeError, Event, EventRef};
use crate::index::{
    Access, ActiveTail, IndexSegment, SegmentIndex, choose, estimate_matches, search, search_back,
};
use crate::log::set::{
    LogError, Record, RecordRef, Scan, ScanBack, Segment, SegmentSet, SegmentSource,
};
use crate::query::{Matches, Query};

use crate::index::IndexSet;

#[cfg(feature = "async")]
pub mod pool;
mod subscribe;

pub use subscribe::{DEFAULT_MAX_BATCH_EVENTS, Subscription};

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
    /// The async counterpart of the condvar, for [`Subscription::next_batch_async`]. Notified
    /// alongside `cv` on every commit and at close, so an async subscriber parked in
    /// [`wait_past_async`](ReadCore::wait_past_async) wakes on the same signal the blocking one
    /// does. Kept separate from the hot read path, which never touches it.
    #[cfg(feature = "async")]
    async_event: event_listener::Event,
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
                #[cfg(feature = "async")]
                async_event: event_listener::Event::new(),
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

    /// The durable tip (last readable position), loaded on its own without cloning the
    /// snapshot. `Acquire`, matching [`load`](Self::load): callers that only need the head
    /// (a lag gauge, say) pay a single atomic load rather than planning a read to reach the
    /// watermark.
    fn head(&self) -> Position {
        Position::new(self.watermark.load(AtomicOrdering::Acquire))
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
        // Wake async subscribers too. `notify` registers no ordering the count gate above does
        // not already cover: a live async subscriber has incremented `subscribers` before its
        // first watermark read (see `wait_past_async`), so a `wake` reaching here observed it.
        #[cfg(feature = "async")]
        self.notify.async_event.notify(usize::MAX);
    }

    /// Marks the store closed and wakes every parked subscriber (writer thread, at shutdown).
    /// `closed` is set (release) **before** taking the lock, so a waiter that evaluates
    /// `closed` under the lock and decides to park cannot miss a close whose `notify_all` is
    /// still pending (that notify also needs the lock).
    pub(crate) fn close(&self) {
        self.notify.closed.store(true, AtomicOrdering::Release);
        let _guard = self.notify.lock.lock().unwrap();
        self.notify.cv.notify_all();
        // `closed` is set (release) before this notify, and `notify` establishes a happens-before
        // with the woken listener, so an async waiter resumed here observes `closed` on its
        // re-check and reports `Closed` (see `wait_past_async`).
        #[cfg(feature = "async")]
        self.notify.async_event.notify(usize::MAX);
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

    /// The async analogue of [`wait_past`](Self::wait_past) with no timeout: awaits until the
    /// watermark advances past `cursor` (`Advanced`) or the store closes (`Closed`), yielding to
    /// the executor while parked so a caller can `select!` it against its own mailbox. Backs
    /// [`Subscription::next_batch_async`].
    ///
    /// The listener is registered **before** the final watermark/closed re-check: `event-listener`
    /// only wakes listeners created before a `notify`, so registering first closes the same window
    /// the condvar loop closes by re-reading under its lock. The watermark is read `SeqCst`
    /// ([`watermark_gate`](Self::watermark_gate)) for the same store-buffer reason as the blocking
    /// path (see [`wake`](Self::wake)): the async subscriber's `SeqCst` count increment in
    /// `register_subscriber` runs before this first `SeqCst` load, so a `wake` that skips on a zero
    /// count is ordered before it and its watermark store is already visible here.
    #[cfg(feature = "async")]
    async fn wait_past_async(&self, cursor: Position) -> WaitOutcome {
        loop {
            if self.notify.closed.load(AtomicOrdering::Acquire) {
                return WaitOutcome::Closed;
            }
            if self.watermark_gate() > cursor {
                return WaitOutcome::Advanced;
            }
            let listener = self.notify.async_event.listen();
            // Re-check after registering: a commit (or close) landing between the checks above
            // and `listen` still notifies this listener, so it is never lost.
            if self.notify.closed.load(AtomicOrdering::Acquire) {
                return WaitOutcome::Closed;
            }
            if self.watermark_gate() > cursor {
                return WaitOutcome::Advanced;
            }
            listener.await;
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
    ///
    /// `limit` caps the number of matched events yielded (`None` = unlimited). It is pushed
    /// into planning, so a selective read does work proportional to `limit`, not to the
    /// query's full result. Together with `after` (an exclusive lower bound) it forms a
    /// stateless pagination cursor: read a page, then read again with `after` set to the last
    /// position, with no gap and no duplicate at the seam.
    pub fn read(&self, query: &Query, after: Position, limit: Option<u64>) -> Reads {
        let (watermark, snapshot) = self.core.load();
        Reads::plan(snapshot, query, after, watermark, &self.config, limit)
    }

    /// Reads events matching `query` in **descending** position order, strictly before
    /// `before`, up to the watermark pinned now, capped at `limit`. The newest-first dual of
    /// [`read`](Self::read): `before` is an exclusive **upper** bound (as `after` is an
    /// exclusive lower one), so `read_back(query, Position::MAX, limit)` starts at the durable
    /// tip. The result is the same lending iterator; consume it with `while let Some(item) =
    /// reads.next()`.
    ///
    /// `limit` caps the events yielded, counting from the tip down, so a newest-first page does
    /// work proportional to `limit`. Together with `before` it is a stateless pagination
    /// cursor: read a page, then read again with `before` set to the oldest position returned,
    /// with no gap and no duplicate at the seam. Ideal for an event explorer showing recent
    /// events first, one page at a time.
    pub fn read_back(&self, query: &Query, before: Position, limit: Option<u64>) -> Reads {
        let (watermark, snapshot) = self.core.load();
        // The top of the reverse range: strictly below `before`, and never past the pinned
        // tip. `saturating_sub` makes `before <= 1` (nothing below it) an empty read.
        let upto = Position::new(before.get().saturating_sub(1).min(watermark.get()));
        Reads::plan_back(snapshot, query, upto, watermark, &self.config, limit)
    }

    /// The current durable tip: the last position any read may see, as published at the most
    /// recent commit. A single atomic load, so a caller sampling it often (a lag gauge
    /// computing `head - cursor` per module) pays no planning cost. Point-in-time like a
    /// [`Reads::watermark`]: a later read pinned here resumes with no gap.
    pub fn head(&self) -> Position {
        self.core.head()
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

/// Why a read failed. `Log` and `Corrupt` are integrity or I/O failures; a query that simply
/// matches nothing yields an empty stream, not an error. `Aborted` is raised only by the
/// async [`pool`] when the worker running a read panics, so a truncated result cannot be
/// mistaken for a completed one.
#[derive(Debug, Error)]
pub enum ReadError {
    #[error("log error during read: {0}")]
    Log(Arc<LogError>),
    #[error("corrupt event during read: {0}")]
    Corrupt(DecodeError),
    #[error("read aborted before completion: the worker running it panicked")]
    Aborted,
}

/// A lending iterator over the events matching a read, in ascending position order for a
/// forward read ([`ReadHandle::read`]) or descending for a backward one
/// ([`ReadHandle::read_back`]).
///
/// Internal shapes (chosen by `Reads::plan` via the cost model): the bypass path streams a log
/// scan, either unfiltered (`Query::all`, zero-copy) or filtered (a broad query, copying one
/// matched record at a time), with a forward ([`Scan`]) and a backward ([`ScanBack`]) variant;
/// the indexed path plans the *positions* (a `Vec<Position>`, cheap `u64`s, small for a
/// selective query, ascending or descending) and fetches each event on demand into a
/// single-record buffer it lends from. Only the indexed path materializes a position list, and
/// the planner routes broad results to the streaming bypass path instead.
pub struct Reads {
    watermark: Position,
    pending_err: Option<ReadError>,
    /// Remaining result budget, `None` when unlimited. Decremented once per attempted yield;
    /// when it reaches zero the read stops with a *limit* stop (see [`is_exhausted`]). The
    /// indexed path also truncates its planned positions to the budget up front, so this is a
    /// second line of defense there and the sole enforcement for the streaming scan modes.
    ///
    /// [`is_exhausted`]: Reads::is_exhausted
    remaining: Option<u64>,
    /// Set only when the read stopped because it hit its `remaining` cap, so a caller can
    /// distinguish a capped stop from a genuinely drained range. Never set on an unlimited
    /// read, so `None` from [`next`](Reads::next) there always means exhaustion.
    hit_limit: bool,
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
    positions: IntoIter<Position>,
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

/// The backward counterpart of [`ScanFilteredState`]: the same filtered-scan state over a
/// reverse [`ScanBack`] instead of a forward [`Scan`].
struct ScanBackFilteredState {
    scan: ScanBack<Arc<Snapshot>>,
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
    /// Backward bypass, unfiltered: the reverse of [`Scan`](Mode::Scan), a zero-copy descending
    /// window scan of `(after, upto]` yielding every record newest-first.
    ScanBack { scan: Box<ScanBack<Arc<Snapshot>>> },
    /// Backward bypass, filtered: the reverse of [`ScanFiltered`](Mode::ScanFiltered), a broad
    /// query streamed descending, each matched record copied out of the window buffer.
    ScanBackFiltered(Box<ScanBackFilteredState>),
    /// Indexed: a pre-planned position list (already clamped to the range), ascending for a
    /// forward read or descending for a backward one, each event fetched on demand and lent
    /// from `buf`, reusing one reader per segment.
    Indexed(Box<IndexedState>),
    /// Nothing to yield (empty range, or a leading planning error carried in `pending_err`).
    Done,
}

/// The direction a [`Reads`] runs in: forward yields ascending from an exclusive lower bound,
/// backward yields descending from an exclusive upper bound. Chosen once at planning; the
/// forward path is exactly as it was, so backwards reads add no cost to it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Direction {
    Forward,
    Backward,
}

impl Reads {
    /// The watermark this read was pinned to. A later read or subscription resumed from here
    /// continues with no gap (the subscription resume seam).
    pub fn watermark(&self) -> Position {
        self.watermark
    }

    /// Whether the read stopped because its `(after, watermark]` range drained, as opposed to
    /// hitting its result `limit`. Meaningful once [`next`](Self::next) has returned `None`.
    /// An unlimited read is always exhausted at its `None`; a limited read is exhausted only
    /// when it yielded fewer events than the cap. A [`Subscription`] relies on this to advance
    /// its cursor past a drained tail but never past a merely capped one.
    pub fn is_exhausted(&self) -> bool {
        !self.hit_limit
    }

    /// Plans a forward read over `(after, watermark]`, ascending. A thin wrapper over
    /// [`plan_directional`](Self::plan_directional) that keeps the exact signature the read
    /// handle and subscriptions already call.
    fn plan(
        snapshot: Arc<Snapshot>,
        query: &Query,
        after: Position,
        watermark: Position,
        config: &ReadConfig,
        limit: Option<u64>,
    ) -> Reads {
        Reads::plan_directional(
            Direction::Forward,
            snapshot,
            query,
            after,
            watermark,
            watermark,
            config,
            limit,
        )
    }

    /// Plans a backward read over `(ZERO, upto]`, descending, pinned to `watermark`. `upto` is
    /// the top of the range (the caller clamps it below the exclusive `before` and to the
    /// pinned tip); the range's lower bound is always the start of the log.
    fn plan_back(
        snapshot: Arc<Snapshot>,
        query: &Query,
        upto: Position,
        watermark: Position,
        config: &ReadConfig,
        limit: Option<u64>,
    ) -> Reads {
        Reads::plan_directional(
            Direction::Backward,
            snapshot,
            query,
            Position::ZERO,
            upto,
            watermark,
            config,
            limit,
        )
    }

    /// Plans the read in either direction. Estimates the result size from exact posting lengths
    /// over the range `(after, upto]` and picks the cheaper mode: a broad query streams a
    /// filtered log scan ([`Access::Scan`]), a selective one gathers positions from the index
    /// and fetches events on demand ([`Access::Index`]). The `direction` selects the forward
    /// ([`Scan`]) or backward ([`ScanBack`]) streaming variant and whether the indexed
    /// positions are ascending or descending; the estimate and verdict are identical either
    /// way (the match set does not depend on order). `watermark` is stored for
    /// [`Reads::watermark`] and equals `upto` for a forward read.
    #[allow(clippy::too_many_arguments)]
    fn plan_directional(
        direction: Direction,
        snapshot: Arc<Snapshot>,
        query: &Query,
        after: Position,
        upto: Position,
        watermark: Position,
        config: &ReadConfig,
        limit: Option<u64>,
    ) -> Reads {
        // Nothing to read: an empty range, or an explicit zero cap.
        if after >= upto || limit == Some(0) {
            return Reads {
                watermark,
                pending_err: None,
                remaining: limit,
                hit_limit: false,
                mode: Mode::Done,
            };
        }

        let (estimate, width) = estimate_read(&snapshot, query, after, upto);
        let access = choose(estimate, width, config.scan_bias);
        #[cfg(feature = "tracing")]
        tracing::debug!(
            ?access,
            ?direction,
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
            // routed the query here. `ScanBack` is the exact reverse of `Scan`, so the forward
            // scan path is untouched by the backward one.
            Access::Scan => {
                // Only the filtered-scan modes retain the query, so they are the paths that
                // clone; the zero-copy full-log scans and the indexed path borrow it and drop
                // the borrow here. This keeps a repeated caller (a subscription polling every
                // round) from re-allocating the query on the index and full-scan paths.
                let is_all = matches!(*query, Query::All);
                let mode = match direction {
                    Direction::Forward => {
                        let scan = Scan::start(Arc::clone(&snapshot), after.next(), upto);
                        if is_all {
                            Mode::Scan {
                                scan: Box::new(scan),
                            }
                        } else {
                            Mode::ScanFiltered(Box::new(ScanFilteredState {
                                scan,
                                query: query.clone(),
                                buf: None,
                            }))
                        }
                    }
                    Direction::Backward => {
                        let scan = ScanBack::start(Arc::clone(&snapshot), after.next(), upto);
                        if is_all {
                            Mode::ScanBack {
                                scan: Box::new(scan),
                            }
                        } else {
                            Mode::ScanBackFiltered(Box::new(ScanBackFilteredState {
                                scan,
                                query: query.clone(),
                                buf: None,
                            }))
                        }
                    }
                };
                Reads {
                    watermark,
                    pending_err: None,
                    remaining: limit,
                    hit_limit: false,
                    mode,
                }
            }
            // Selective: plan the positions from the index (ascending forward, descending
            // backward), fetch on demand. The position list is truncated to `limit`, so the
            // fetch loop and the list's memory are both bounded by the cap, not by the query's
            // full result. The `Mode::Indexed` fetch loop is direction-agnostic: it fetches by
            // arbitrary position, and a descending list still clusters by segment.
            Access::Index => {
                let planned = match direction {
                    Direction::Forward => plan_positions(&snapshot, query, after, upto, limit),
                    Direction::Backward => plan_positions_back(&snapshot, query, upto, limit),
                };
                match planned {
                    Ok(positions) => Reads {
                        watermark,
                        pending_err: None,
                        remaining: limit,
                        hit_limit: false,
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
                        remaining: limit,
                        hit_limit: false,
                        mode: Mode::Done,
                    },
                }
            }
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
        // Enforce the result cap before touching the source: one budget unit per attempted
        // yield (each `next` yields at most one matched event, looping past non-matches
        // internally, so this counts matched events). When the budget is spent, stop with a
        // limit stop recorded for `is_exhausted` rather than draining the source further.
        match &mut self.remaining {
            Some(0) => {
                self.hit_limit = true;
                self.mode = Mode::Done;
                return None;
            }
            Some(n) => *n -= 1,
            None => {}
        }
        match &mut self.mode {
            Mode::Done => None,
            // Unfiltered: yield every record, zero-copy (the event borrows the scan buffer). The
            // backward arm is the reverse of the forward one, newest-first from the window buffer;
            // both decode through the one shared helper. The filter is the same `Query::matches`
            // the scan oracle uses, so the filtered arms yield exactly the indexed path's
            // positions.
            Mode::Scan { scan } => scan.next().map(decode_record),
            Mode::ScanBack { scan } => scan.next().map(decode_record),
            Mode::ScanFiltered(state) => {
                let ScanFilteredState { scan, query, buf } = state.as_mut();
                next_filtered(scan, query, buf)
            }
            Mode::ScanBackFiltered(state) => {
                let ScanBackFilteredState { scan, query, buf } = state.as_mut();
                next_filtered(scan, query, buf)
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
    limit: Option<u64>,
) -> Result<Vec<Position>, ReadError> {
    let mut out = Vec::new();
    let wm = watermark.get();
    // The cap as an absolute target length. Each arm collects at most up to it, and the loop
    // stops touching further segments once it is reached, so a limited read plans O(limit)
    // positions rather than the query's full result.
    let cap = limit.map(|k| k as usize);

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
            // Indexed: take the ascending postings up to the watermark, no more than the cap
            // still allows.
            Some(index_seg) => {
                let iter = search(index_seg.as_ref(), query, after).take_while(|p| p.get() <= wm);
                extend_capped(&mut out, iter, cap);
            }
            // Unindexable sealed segment: scan its own (watermark-clamped) range rather
            // than answer short.
            None => scan_positions_into(
                snapshot,
                query,
                first_after(after, base),
                Position::new(effective_max),
                &mut out,
                cap,
            )?,
        }
        // Segments are disjoint and ascending, so once the cap is met the prefix gathered so
        // far is the globally-ascending answer: stop before opening any further segment.
        if let Some(k) = cap
            && out.len() >= k
        {
            out.truncate(k);
            return Ok(out);
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
                cap,
            )?;
        } else {
            let view = snapshot.active_index.view(watermark);
            extend_capped(&mut out, search(&view, query, after), cap);
        }
    }

    // The arms above already respect the cap, so this only guards the empty-range edge; keep
    // it so the postcondition (`out.len() <= limit`) is obvious at the return.
    if let Some(k) = cap {
        out.truncate(k);
    }
    Ok(out)
}

/// Inclusive start of the `(after, ...]` range within a segment based at `base`: the first
/// position both strictly greater than `after` and not before `base`, i.e.
/// `max(after + 1, base)`.
fn first_after(after: Position, base: Position) -> Position {
    Position::new(after.get().max(base.get().saturating_sub(1)) + 1)
}

/// The shared lending-iterator shape of a forward [`Scan`] and a backward [`ScanBack`]: one data
/// record at a time, borrowing an internal buffer. Lets the read-path helpers below drive either
/// direction from one body, so the forward and backward paths cannot drift.
trait RecordScan {
    fn next_record(&mut self) -> Option<Result<RecordRef<'_>, LogError>>;
}

impl RecordScan for Scan<Arc<Snapshot>> {
    fn next_record(&mut self) -> Option<Result<RecordRef<'_>, LogError>> {
        self.next()
    }
}

impl RecordScan for ScanBack<Arc<Snapshot>> {
    fn next_record(&mut self) -> Option<Result<RecordRef<'_>, LogError>> {
        self.next()
    }
}

/// Extends `out` with positions from `iter`, respecting the absolute `cap` (an already-gathered
/// prefix may fill part of it). The one definition of the capped take used by every indexed
/// segment arm in both planning directions; `saturating_sub` keeps it correct even if a caller
/// ever overshoots the cap.
fn extend_capped(
    out: &mut Vec<Position>,
    iter: impl Iterator<Item = Position>,
    cap: Option<usize>,
) {
    match cap {
        Some(k) => out.extend(iter.take(k.saturating_sub(out.len()))),
        None => out.extend(iter),
    }
}

/// Appends the positions of events matching `query` from a lending scan to `out`, stopping once
/// `out` reaches `cap`. Shared by the forward and backward unindexable-segment fallbacks; the
/// caller supplies the scan (and so the direction). `cap` is an absolute target length, since
/// `out` may already hold positions from earlier segments. Callers gate on the cap before
/// constructing the scan, so this is only entered with `out.len() < cap`.
fn scan_positions_matching<S: RecordScan>(
    mut scan: S,
    query: &Query,
    out: &mut Vec<Position>,
    cap: Option<usize>,
) -> Result<(), ReadError> {
    while let Some(item) = scan.next_record() {
        let record = item.map_err(|err| ReadError::Log(Arc::new(err)))?;
        let event = EventRef::from_bytes(record.data).map_err(ReadError::Corrupt)?;
        if query.matches(event) {
            out.push(record.position);
            if let Some(k) = cap
                && out.len() >= k
            {
                break;
            }
        }
    }
    Ok(())
}

/// Decodes one raw scanned record into a [`Sequenced`], mapping a log or decode failure to the
/// read error. Shared by the forward and backward unfiltered arms of [`Reads::next`].
fn decode_record(item: Result<RecordRef<'_>, LogError>) -> Result<Sequenced<'_>, ReadError> {
    match item {
        Ok(record) => match EventRef::from_bytes(record.data) {
            Ok(event) => Ok(Sequenced {
                position: record.position,
                event,
            }),
            Err(err) => Err(ReadError::Corrupt(err)),
        },
        Err(err) => Err(ReadError::Log(Arc::new(err))),
    }
}

/// Advances a lending scan to the next record matching `query`, copies it into `buf` (owned, so
/// the yielded event borrows `buf` rather than the scan), and returns it. Shared by the forward
/// and backward filtered arms of [`Reads::next`]: a lending iterator cannot conditionally return
/// its borrow of the scan from a loop, so the one matched record is buffered. Decoding happens
/// once; `to_owned` preserves the filter decode's offset, so `as_ref` on the way out reparses
/// nothing.
fn next_filtered<'a, S: RecordScan>(
    scan: &mut S,
    query: &Query,
    buf: &'a mut Option<(Position, Event)>,
) -> Option<Result<Sequenced<'a>, ReadError>> {
    loop {
        match scan.next_record()? {
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

/// Forward unindexable-segment fallback: scans `first..=upto` ascending and appends matching
/// positions. Gates on the cap before opening a scan, since [`Scan::start`] opens a reader.
fn scan_positions_into(
    snapshot: &Arc<Snapshot>,
    query: &Query,
    first: Position,
    upto: Position,
    out: &mut Vec<Position>,
    cap: Option<usize>,
) -> Result<(), ReadError> {
    if let Some(k) = cap
        && out.len() >= k
    {
        return Ok(());
    }
    scan_positions_matching(
        Scan::start(Arc::clone(snapshot), first, upto),
        query,
        out,
        cap,
    )
}

/// Gathers the **descending** positions matching `query` in `(ZERO, upto]`: the active
/// segment's range first (it holds the highest positions), then sealed segments high-to-low,
/// each yielding its matches descending through [`search_back`] (or a reverse scan of its
/// range if unindexable). Segments are disjoint and ordered, so concatenating highest-first is
/// already globally descending. The mirror of [`plan_positions`]; the same watermark clamp
/// keeps it from planning a position past `upto` if a rollover raced this read.
fn plan_positions_back(
    snapshot: &Arc<Snapshot>,
    query: &Query,
    upto: Position,
    limit: Option<u64>,
) -> Result<Vec<Position>, ReadError> {
    let mut out = Vec::new();
    let wm = upto.get(); // inclusive upper bound of the range
    let cap = limit.map(|k| k as usize);

    // Active segment first: it holds the highest positions. Bounded above by `upto` (never past
    // the pinned tip), mirroring the forward active arm.
    let active_base = snapshot.active_log.base_position();
    if upto >= active_base {
        if snapshot.active_index.is_unindexable() {
            scan_positions_back_into(snapshot, query, active_base, upto, &mut out, cap)?;
        } else {
            let view = snapshot.active_index.view(upto);
            // Saturating `+ 1`, matching the sealed arm below, so `upto == Position::MAX` cannot
            // overflow the exclusive upper bound (harmless today since the caller clamps `upto`
            // to the watermark, but the two arms must agree on overflow discipline).
            let before = Position::new(upto.get().saturating_add(1));
            extend_capped(&mut out, search_back(&view, query, before), cap);
        }
        if let Some(k) = cap
            && out.len() >= k
        {
            out.truncate(k);
            return Ok(out);
        }
    }

    // Sealed segments high index to low, so the concatenation stays globally descending.
    for (seg, index) in snapshot
        .sealed_log
        .iter()
        .zip(snapshot.sealed_index.iter())
        .rev()
    {
        let base = seg.base_position();
        let count = seg.event_count();
        if count == 0 {
            continue;
        }
        if base.get() > wm {
            continue; // whole segment sits above the upper bound
        }
        // Top position in this segment within range: `upto` for the one straddling segment,
        // else the segment's own last position. The `.min(wm)` also clamps a sealed segment a
        // racing rollover extended past the pinned tip (as in `plan_positions`).
        let effective_max = (base.get() + count - 1).min(wm);
        match index {
            Some(index_seg) => {
                let before = Position::new(effective_max.saturating_add(1));
                extend_capped(
                    &mut out,
                    search_back(index_seg.as_ref(), query, before),
                    cap,
                );
            }
            None => scan_positions_back_into(
                snapshot,
                query,
                base,
                Position::new(effective_max),
                &mut out,
                cap,
            )?,
        }
        if let Some(k) = cap
            && out.len() >= k
        {
            out.truncate(k);
            return Ok(out);
        }
    }

    if let Some(k) = cap {
        out.truncate(k);
    }
    Ok(out)
}

/// Backward unindexable-segment fallback: the reverse of [`scan_positions_into`]. Scans
/// `[first, upto]` descending and appends matching positions (descending).
fn scan_positions_back_into(
    snapshot: &Arc<Snapshot>,
    query: &Query,
    first: Position,
    upto: Position,
    out: &mut Vec<Position>,
    cap: Option<usize>,
) -> Result<(), ReadError> {
    if let Some(k) = cap
        && out.len() >= k
    {
        return Ok(());
    }
    scan_positions_matching(
        ScanBack::start(Arc::clone(snapshot), first, upto),
        query,
        out,
        cap,
    )
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
                .map(|s| Tag::new(*s).unwrap())
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
                plan_positions(&snapshot, &query, Position::ZERO, Position::new(wm), None).unwrap();
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
        let planned = plan_positions(&snapshot, &query, Position::ZERO, last, None).unwrap();
        let got: Vec<u64> = planned.iter().map(|p| p.get()).collect();
        assert_eq!(got, (1..=6).collect::<Vec<_>>());
    }

    /// `plan_positions` returns at most `limit` positions, and they are the leading prefix of
    /// the unlimited plan. Exercised across sealed-indexed and active ranges (tiny segments
    /// force several seals) and across the segment-boundary truncation (a cap that lands
    /// mid-way through the segments still yields the globally-ascending prefix).
    #[test]
    fn plan_positions_honors_the_limit() {
        let dir = TempDir::new().unwrap();
        let mut set = SegmentSet::open(dir.path(), SegmentConfig::new(512)).unwrap();
        for _ in 0..60 {
            set.append_batch(&[event("Enrolled", &["course:c1"]).as_bytes()])
                .unwrap();
        }
        assert!(set.sealed_len() >= 2, "need several sealed segments");
        let index = IndexSet::open(&set).unwrap();
        let snapshot = Arc::new(Snapshot::capture(&set, &index));

        let query = Query::item(QueryItem::with_tags(tags(&["course:c1"])));
        let watermark = set.last_position();
        let full = plan_positions(&snapshot, &query, Position::ZERO, watermark, None).unwrap();
        assert_eq!(full.len(), 60);

        // Every cap yields exactly the leading prefix, whichever range it lands in.
        for limit in [0u64, 1, 5, 30, 59, 60, 61, 1000] {
            let planned =
                plan_positions(&snapshot, &query, Position::ZERO, watermark, Some(limit)).unwrap();
            let want = &full[..(limit as usize).min(full.len())];
            assert_eq!(planned, want, "limit {limit}");
        }
    }

    /// The lending `Reads` iterator yields no more than `limit` events across all modes, the
    /// yielded events are the leading prefix of the unlimited read, and `is_exhausted` reports
    /// a capped stop (not a drained range) while an unlimited or under-cap read reports drained.
    #[test]
    fn reads_next_respects_the_limit_and_reports_exhaustion() {
        let dir = TempDir::new().unwrap();
        let mut set = SegmentSet::open(dir.path(), SegmentConfig::new(512)).unwrap();
        for _ in 0..40 {
            set.append_batch(&[event("Enrolled", &["course:c1"]).as_bytes()])
                .unwrap();
        }
        let index = IndexSet::open(&set).unwrap();

        let collect = |limit: Option<u64>, scan_bias: u32| -> (Vec<u64>, bool) {
            let snapshot = Arc::new(Snapshot::capture(&set, &index));
            let watermark = set.last_position();
            let mut reads = Reads::plan(
                snapshot,
                &Query::item(QueryItem::with_tags(tags(&["course:c1"]))),
                Position::ZERO,
                watermark,
                &ReadConfig { scan_bias },
                limit,
            );
            let mut out = Vec::new();
            while let Some(item) = reads.next() {
                out.push(item.unwrap().position.get());
            }
            (out, reads.is_exhausted())
        };

        // Force the index arm (bias 1) and the scan arm (bias u32::MAX): both honor the cap
        // identically, yielding the same leading prefix and the same exhaustion verdict.
        for scan_bias in [1u32, u32::MAX] {
            let (full, full_exhausted) = collect(None, scan_bias);
            assert_eq!(full, (1..=40).collect::<Vec<_>>(), "bias {scan_bias}");
            assert!(full_exhausted, "an unlimited read is exhausted at its end");

            let (capped, capped_exhausted) = collect(Some(10), scan_bias);
            assert_eq!(capped, (1..=10).collect::<Vec<_>>(), "bias {scan_bias}");
            assert!(!capped_exhausted, "a capped read is not exhausted");

            let (zero, zero_exhausted) = collect(Some(0), scan_bias);
            assert!(zero.is_empty(), "a zero cap yields nothing");
            assert!(
                !zero_exhausted,
                "a zero cap is a limit stop, not exhaustion"
            );

            let (over, over_exhausted) = collect(Some(1000), scan_bias);
            assert_eq!(
                over,
                (1..=40).collect::<Vec<_>>(),
                "a cap above the result returns all"
            );
            assert!(over_exhausted, "an under-cap read drains and is exhausted");
        }
    }

    /// The reverse plan is exactly the forward plan reversed across sealed-indexed and active
    /// ranges; bounding above by `upto` yields the descending prefix at or below it, and a
    /// `limit` yields the descending prefix from the tip.
    #[test]
    fn plan_positions_back_mirrors_plan_positions() {
        let dir = TempDir::new().unwrap();
        let mut set = SegmentSet::open(dir.path(), SegmentConfig::new(512)).unwrap();
        for _ in 0..60 {
            set.append_batch(&[event("Enrolled", &["course:c1"]).as_bytes()])
                .unwrap();
        }
        assert!(set.sealed_len() >= 2, "need several sealed segments");
        let index = IndexSet::open(&set).unwrap();
        let snapshot = Arc::new(Snapshot::capture(&set, &index));
        let query = Query::item(QueryItem::with_tags(tags(&["course:c1"])));
        let last = set.last_position();

        let forward = plan_positions(&snapshot, &query, Position::ZERO, last, None).unwrap();
        let want: Vec<Position> = forward.iter().rev().copied().collect();
        assert_eq!(
            plan_positions_back(&snapshot, &query, last, None).unwrap(),
            want
        );

        // Bounded above mid-log, across the sealed-indexed and active ranges.
        for upto in [1u64, 5, 30, 59, 60] {
            let back = plan_positions_back(&snapshot, &query, Position::new(upto), None).unwrap();
            let want: Vec<Position> = forward
                .iter()
                .filter(|p| p.get() <= upto)
                .rev()
                .copied()
                .collect();
            assert_eq!(back, want, "upto {upto}");
        }

        // Limit yields the descending prefix from the tip.
        let full: Vec<Position> = forward.iter().rev().copied().collect();
        for limit in [0u64, 1, 5, 30, 60, 61] {
            let back = plan_positions_back(&snapshot, &query, last, Some(limit)).unwrap();
            assert_eq!(
                back,
                full[..(limit as usize).min(full.len())],
                "limit {limit}"
            );
        }
    }

    /// The lending `Reads` reverse read yields the exact reverse of the forward read across all
    /// verdicts (index, scan, scan-filtered, forced via `scan_bias`), honors the `limit` from
    /// the tip, and reports exhaustion the same way.
    #[test]
    fn reads_back_is_the_reverse_of_reads_forward() {
        let dir = TempDir::new().unwrap();
        let mut set = SegmentSet::open(dir.path(), SegmentConfig::new(512)).unwrap();
        for i in 0..40u64 {
            // Half the events also carry student:s1, so a selective query has real work.
            let carries: &[&str] = if i % 2 == 0 {
                &["course:c1"]
            } else {
                &["course:c1", "student:s1"]
            };
            set.append_batch(&[event("Enrolled", carries).as_bytes()])
                .unwrap();
        }
        let index = IndexSet::open(&set).unwrap();

        let collect_forward = |query: &Query, scan_bias: u32| -> Vec<u64> {
            let snapshot = Arc::new(Snapshot::capture(&set, &index));
            let wm = set.last_position();
            let mut reads = Reads::plan(
                snapshot,
                query,
                Position::ZERO,
                wm,
                &ReadConfig { scan_bias },
                None,
            );
            let mut out = Vec::new();
            while let Some(item) = reads.next() {
                out.push(item.unwrap().position.get());
            }
            out
        };
        let collect_back =
            |query: &Query, scan_bias: u32, limit: Option<u64>| -> (Vec<u64>, bool) {
                let snapshot = Arc::new(Snapshot::capture(&set, &index));
                let wm = set.last_position();
                let mut reads =
                    Reads::plan_back(snapshot, query, wm, wm, &ReadConfig { scan_bias }, limit);
                let mut out = Vec::new();
                while let Some(item) = reads.next() {
                    out.push(item.unwrap().position.get());
                }
                (out, reads.is_exhausted())
            };

        let queries = [
            Query::all(),
            Query::item(QueryItem::with_tags(tags(&["student:s1"]))),
        ];
        // bias 1 favors the index, bias u32::MAX forces the scan: together they cover the
        // indexed, unfiltered-scan, and filtered-scan reverse paths.
        for query in &queries {
            for scan_bias in [1u32, u32::MAX] {
                let want: Vec<u64> = collect_forward(query, scan_bias)
                    .into_iter()
                    .rev()
                    .collect();

                let (back, exhausted) = collect_back(query, scan_bias, None);
                assert_eq!(back, want, "query {query:?} bias {scan_bias}");
                assert!(
                    exhausted,
                    "an unlimited reverse read is exhausted at its end"
                );

                let (capped, capped_exhausted) = collect_back(query, scan_bias, Some(5));
                let want_capped: Vec<u64> = want.iter().take(5).copied().collect();
                assert_eq!(
                    capped, want_capped,
                    "capped query {query:?} bias {scan_bias}"
                );
                assert!(!capped_exhausted, "a capped reverse read is not exhausted");
            }
        }
    }
}
