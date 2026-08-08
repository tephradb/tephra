//! Layer 5: off-thread read paths.
//!
//! Reads run on the **caller's own thread**, over an immutable snapshot the writer
//! publishes at each commit. There is no reader pool and no channel hop, and the writer
//! thread is never touched: sealed segments are shared as immutable `Arc`s (lock-free), and
//! how far a read may see is an atomically published watermark (CLAUDE.md 6, 9).
//!
//! [`ReadCore`] holds the shared state; a cloneable [`ReadHandle`] runs a [`Query`] against
//! it with [`ReadHandle::read`], returning a lending [`Reads`] iterator of events in
//! ascending position order.
//!
//! ## Point-in-time semantics
//!
//! A [`Reads`] is pinned to the watermark read at call time ([`Reads::watermark`]): it
//! returns a consistent *prefix* of the log, not a live view, and a caller cannot tell "no
//! more events" from "no more events yet". That distinction is phase 7's (subscriptions);
//! exposing the pinned watermark is the seam a later read/subscription resumes from with no
//! gap.
//!
//! ## Snapshot / watermark ordering
//!
//! The writer publishes the segment set (on rollover) **before** the watermark (every
//! commit); a reader loads the watermark **before** the segment set. With acquire/release
//! ordering this guarantees the loaded snapshot always covers the loaded watermark: if a
//! reader observes watermark `W`, the segment set it then loads was published no earlier
//! than the one current when `W` was stored, and segment sets only grow.
//!
//! ## Phase 6a scope
//!
//! Sealed segments are answered through their on-disk index ([`search`]); the **active**
//! segment's range is answered by a bounded log scan (the active tail index stays
//! writer-private until 6b gives it a shared, watermark-published form). An unindexable
//! sealed segment falls back to scanning its own range, so a read never returns a short
//! answer. The index-vs-scan cost model is 6c; here selective queries use the index for
//! sealed ranges and scan the (bounded) active range.

use std::cmp::Ordering;
use std::sync::Arc;
use std::sync::RwLock;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};

use thiserror::Error;

use seglog::read::Reader;

use crate::Position;
use crate::event::{DecodeError, EventRef};
use crate::index::{IndexSegment, search};
use crate::log::set::{LogError, Record, Scan, Segment, SegmentSet, SegmentSource};
use crate::query::Query;

use crate::index::IndexSet;

/// Configuration for the read paths. A placeholder in 6a; the index-vs-scan cost model
/// (6c) adds its knobs here.
#[derive(Clone, Copy, Debug, Default)]
pub struct ReadConfig {}

/// An immutable snapshot of what is readable: the sealed log segments and their on-disk
/// indexes (aligned one-for-one), plus the active log segment. Shared behind an `Arc`;
/// grown only on rollover.
///
/// Implements [`SegmentSource`] so the one zero-copy [`Scan`] serves reads unchanged
/// (CLAUDE.md 5.4): sealed segments occupy logical indices `0..sealed_log.len()`, the
/// active segment sits at `sealed_log.len()`.
pub struct Snapshot {
    header_size: u64,
    sealed_log: Vec<Arc<Segment>>,
    /// Aligned with `sealed_log`: `sealed_index[i]` indexes `sealed_log[i]`, or `None` if
    /// that segment is unindexable (a query touching it scans the log for its range).
    sealed_index: Vec<Option<Arc<IndexSegment>>>,
    active_log: Arc<Segment>,
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
        }
    }
}

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

/// The shared read state, held by both the writer thread (to publish) and every
/// [`ReadHandle`] (to read). Cheap to share: an `Arc<ReadCore>`.
pub struct ReadCore {
    /// The current segment snapshot. Swapped only on rollover (a normal commit leaves it
    /// untouched), so cloning the inner `Arc` under a brief read lock is a single op, never
    /// held across query evaluation (CLAUDE.md 9).
    segments: RwLock<Arc<Snapshot>>,
    /// Last durable (and index-fed) position. Stored on every commit.
    watermark: AtomicU64,
}

impl ReadCore {
    /// The initial core, capturing `set`/`index` as they stand and pinning the watermark to
    /// the current tip.
    pub(crate) fn new(set: &SegmentSet, index: &IndexSet) -> Arc<ReadCore> {
        Arc::new(ReadCore {
            segments: RwLock::new(Arc::new(Snapshot::capture(set, index))),
            watermark: AtomicU64::new(set.last_position().get()),
        })
    }

    /// Publishes a new segment snapshot (writer thread, on rollover). Must run **before**
    /// [`publish_watermark`](Self::publish_watermark) for a batch, so a reader that observes
    /// the new watermark also sees the segment covering it.
    pub(crate) fn publish_segments(&self, snapshot: Snapshot) {
        *self.segments.write().unwrap() = Arc::new(snapshot);
    }

    /// Publishes the durable tip (writer thread, every commit). Release-ordered so a reader
    /// acquiring it sees all segment/offset writes ordered before it.
    pub(crate) fn publish_watermark(&self, tip: Position) {
        self.watermark.store(tip.get(), AtomicOrdering::Release);
    }

    /// Loads a consistent `(watermark, snapshot)` pair: watermark first (acquire), then the
    /// snapshot, so the snapshot always covers the watermark (see the module ordering note).
    fn load(&self) -> (Position, Arc<Snapshot>) {
        let watermark = Position::new(self.watermark.load(AtomicOrdering::Acquire));
        let snapshot = Arc::clone(&self.segments.read().unwrap());
        (watermark, snapshot)
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
        Reads::plan(snapshot, query, after, watermark, &self.config)
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
/// Two internal shapes (chosen by [`Reads::plan`]): the bypass path streams a filtered log
/// scan and never buffers the result; the indexed path plans the ascending *positions* (a
/// `Vec<Position>`, cheap `u64`s, small for a selective query) and fetches each event on
/// demand into a single-record buffer it lends from. Neither buffers a growing *event*
/// result; the position list is materialized only on the indexed path, and 6c routes broad
/// results to the streaming bypass path.
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

enum Mode {
    /// Bypass: a sequential scan of the whole `(after, watermark]` range. Streaming, so a
    /// huge projection never materializes. In 6a this path is only `Query::all`, so every
    /// record matches and there is no per-item filter; 6c adds broad-projection filtering.
    /// Both non-trivial variants are boxed: a `Scan` and the indexed state each own large
    /// buffers, so keeping the enum small avoids a bloated `Reads` for the `Done` case.
    Scan { scan: Box<Scan<Arc<Snapshot>>> },
    /// Indexed: a pre-planned ascending position list (already clamped to the watermark),
    /// each event fetched on demand and lent from `buf`, reusing one reader per segment.
    Indexed(Box<IndexedState>),
    /// Nothing to yield (empty range, or a leading planning error carried in `pending_err`).
    Done,
}

impl Reads {
    /// The watermark this read was pinned to. A later read or subscription resumed from here
    /// continues with no gap (the phase-7 seam).
    pub fn watermark(&self) -> Position {
        self.watermark
    }

    /// Plans the read: `Query::all` (and, in 6c, broad projections) streams a filtered log
    /// scan; a selective query gathers positions from the sealed indexes and the bounded
    /// active-range scan, then fetches events on demand.
    fn plan(
        snapshot: Arc<Snapshot>,
        query: Query,
        after: Position,
        watermark: Position,
        _config: &ReadConfig,
    ) -> Reads {
        if after >= watermark {
            return Reads {
                watermark,
                pending_err: None,
                mode: Mode::Done,
            };
        }

        // Bypass: a full-log query streams a scan rather than planning positions. Every
        // record matches `Query::all`, so no per-item filter is needed here.
        if matches!(query, Query::All) {
            let scan = Box::new(Scan::start(Arc::clone(&snapshot), after.next(), watermark));
            return Reads {
                watermark,
                pending_err: None,
                mode: Mode::Scan { scan },
            };
        }

        match plan_positions(&snapshot, &query, after, watermark) {
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

    // Active segment's range: no shared active index in 6a, so scan it (bounded by one
    // segment and the watermark). 6b replaces this with the watermark-published active tail.
    let active_base = snapshot.active_log.base_position();
    if watermark >= active_base {
        scan_positions_into(
            snapshot,
            query,
            first_after(after, active_base),
            watermark,
            &mut out,
        )?;
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
}
