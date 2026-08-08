//! [`IndexSet`]: the index counterpart of the log's [`SegmentSet`], owning one on-disk
//! [`IndexSegment`] per sealed log segment plus the in-memory [`ActiveTail`] for the
//! active one, and answering a [`Query`] across all of them.
//!
//! Ownership mirrors the log: sealed segments are immutable `Arc<IndexSegment>` (reading
//! them is lock-free), and the active tail is fed in position order at the commit seam. As
//! of phase 6b the active tail is an `Arc<ActiveTail>`, append-only and shared: the writer
//! feeds it while reader threads query it lock-free through a watermark-bounded
//! [`ActiveView`](super::ActiveView), so the set is `Sync` and its active `Arc` is published
//! into the read snapshot (CLAUDE.md 9).
//!
//! Cross-segment combination is **ordered concatenation, not a k-merge**: segments are
//! position-disjoint and ordered, so each one's per-query output is a globally-ascending,
//! non-overlapping run and concatenating them in order is already sorted (CLAUDE.md 2).
//!
//! A degraded segment (one whose log holds more distinct types than the dense type
//! column can address) is never silently skipped: [`search_all`](IndexSet::search_all)
//! errors with the unanswerable range so the caller falls back to a log scan, rather than
//! returning a short answer (CLAUDE.md 7).

use std::fs::File;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use thiserror::Error;

use crate::Position;
use crate::event::{DecodeError, EventRef};
use crate::log::set::{LogError, PositionRange, SegmentSet};
use crate::query::Query;

use super::ActiveTail;
use super::recovery::Rebuilder;
use super::search;
use super::segment::{IndexSegment, write_segment_file};

const NAME_DIGITS: usize = 20;

/// One sealed segment's slot: its disjoint position range and, when indexable, the
/// on-disk index over it. `seg` is `None` only for a segment whose log exceeds the
/// per-segment type limit, which is not a real workload but must error a covering query
/// rather than answer it short.
struct SealedIndex {
    base: Position,
    count: u64,
    seg: Option<Arc<IndexSegment>>,
}

impl SealedIndex {
    /// The last position this segment covers, or `None` if it is empty.
    fn max_position(&self) -> Option<u64> {
        (self.count > 0).then(|| self.base.get() + self.count - 1)
    }
}

/// The set of index segments over one log.
pub struct IndexSet {
    /// `{log_dir}/index`, where `.idx` files live, one per sealed `.log`.
    dir: PathBuf,
    /// Sealed, immutable index segments in `base` order, disjoint and contiguous.
    sealed: Vec<SealedIndex>,
    /// The index over the active log segment, fed in position order. Shared: reader threads
    /// hold clones of this `Arc` (via the read snapshot) and query it lock-free through a
    /// watermark-bounded [`ActiveView`](super::ActiveView).
    active: Arc<ActiveTail>,
    /// Base position of the active segment.
    active_base: Position,
    /// Number of events assigned to the active segment. Equals `active.len()` while the
    /// segment is indexable; kept separately so the true range is known even after the
    /// active tail is latched unindexable (feeding stops but positions keep arriving).
    active_span: u64,
    /// Set if the active segment exceeded the per-segment type limit; a covering query
    /// then errors rather than reading a partial index.
    active_unindexable: bool,
}

impl IndexSet {
    /// Opens the index for `set`, rebuilding anything missing, corrupt, or never
    /// persisted from the log (the source of truth).
    ///
    /// For each sealed log segment, a valid `.idx` whose base and event count match is
    /// loaded; otherwise it is rebuilt by scanning that segment and sealing a fresh
    /// `.idx`. The active segment's tail is always rebuilt by scan (never persisted, like
    /// the offset sidecar), which also covers a durable-but-unindexed tail after a crash.
    pub fn open(set: &SegmentSet) -> Result<Self, IndexError> {
        let dir = set.dir().join("index");
        if !dir.exists() {
            std::fs::create_dir_all(&dir).map_err(|source| IndexError::io(&dir, source))?;
            if let Some(parent) = dir.parent()
                && !parent.as_os_str().is_empty()
            {
                sync_dir(parent).map_err(|source| IndexError::io(parent, source))?;
            }
        }

        let mut sealed = Vec::new();
        for (base, count) in set.sealed_segments() {
            let path = dir.join(index_file_name(base));
            let entry = match load_valid(&path, base, count)? {
                Some(seg) => SealedIndex {
                    base,
                    count,
                    seg: Some(Arc::new(seg)),
                },
                None => build_and_seal(set, &dir, base, count)?,
            };
            sealed.push(entry);
        }

        let active_base = set.active_base();
        let active_count = set.next_position() - active_base;
        let rebuilt = rebuild_range(set, active_base, active_count)?;

        Ok(IndexSet {
            dir,
            sealed,
            active: Arc::new(rebuilt.index),
            active_base,
            active_span: rebuilt.count,
            active_unindexable: rebuilt.unindexable,
        })
    }

    /// The active tail's shared handle, for publishing into a read snapshot. Readers query it
    /// lock-free through a watermark-bounded [`ActiveView`](super::ActiveView).
    pub(crate) fn active_tail_arc(&self) -> Arc<ActiveTail> {
        Arc::clone(&self.active)
    }

    /// The per-sealed-segment index handles, aligned with the log's sealed segments, for
    /// publishing into a read snapshot. `None` marks an unindexable sealed segment (a query
    /// touching it must scan the log for that range).
    pub(crate) fn sealed_index_arcs(&self) -> Vec<Option<Arc<IndexSegment>>> {
        self.sealed.iter().map(|s| s.seg.clone()).collect()
    }

    /// Feeds one committed event into the active tail at its assigned `position`.
    ///
    /// Best-effort: the write is already durable when this runs, so a too-many-types
    /// rejection never fails the write. It latches the active segment unindexable (a
    /// covering query will error, and off-thread reads scan the log for its range), and
    /// feeding stops mutating the tail while positions keep arriving.
    pub fn push(&mut self, position: Position, event: EventRef<'_>) {
        self.active_span += 1;
        if self.active_unindexable {
            return;
        }
        if self.active.push(position, event).is_err() {
            self.active_unindexable = true;
            // Publish the latch on the shared tail too: the latch fires mid-segment (no
            // rollover, so no snapshot republish), and an off-thread reader must see it live
            // to fall back to a log scan instead of trusting the now-truncated columns.
            self.active.mark_unindexable();
            #[cfg(feature = "tracing")]
            tracing::error!(
                "segment at base {} exceeds the per-segment type limit; queries over it will error until it seals and is rebuilt",
                self.active_base
            );
        }
    }

    /// Seals the active tail (the just-completed log segment) and starts a fresh one at
    /// `new_base`. Called on a log rollover, before the new batch is fed.
    ///
    /// The in-memory segment is published to the sealed set before the file is written, so
    /// readers see it without waiting on the fsync. A failed write is logged and ignored:
    /// the in-memory segment is authoritative for this process and rebuild-on-open
    /// re-derives the file next start (CLAUDE.md 2). It is never turned into a write
    /// failure or an unpublish.
    pub fn seal_active(&mut self, new_base: Position) {
        let base = self.active_base;
        let count = self.active_span;

        if self.active_unindexable {
            self.sealed.push(SealedIndex {
                base,
                count,
                seg: None,
            });
        } else {
            debug_assert_eq!(
                count,
                self.active.len() as u64,
                "active span tracks tail len"
            );
            let data: Arc<[u8]> = Arc::from(IndexSegment::encode(&self.active));
            let seg = IndexSegment::from_bytes(Arc::clone(&data))
                .expect("a just-encoded index segment must validate");
            // Publish first: reader visibility does not wait on fsync.
            self.sealed.push(SealedIndex {
                base,
                count,
                seg: Some(Arc::new(seg)),
            });
            let path = self.dir.join(index_file_name(base));
            if let Err(err) = write_segment_file(&path, &data) {
                // In-memory segment is authoritative; the next open rebuilds the file.
                #[cfg(feature = "tracing")]
                tracing::error!("failed to persist index segment {path:?}: {err}");
                let _ = err;
            }
        }

        self.active = Arc::new(ActiveTail::new(new_base));
        self.active_base = new_base;
        self.active_span = 0;
        self.active_unindexable = false;
    }

    /// Positions matching `query`, ascending, deduped, strictly after `after`, across
    /// every segment.
    ///
    /// Prunes segments whose whole range is at or before `after` by header comparison,
    /// then, over only the segments the query actually touches, errors if any is
    /// unindexable (never a short answer), then concatenates their ascending per-segment
    /// outputs. The materialized order is already globally ascending because the segments
    /// are disjoint and ordered.
    pub fn search_all(
        &self,
        query: &Query,
        after: Position,
    ) -> Result<std::vec::IntoIter<Position>, IndexError> {
        // Prune, then check: collect the segments this query touches, erroring up front if
        // any touched segment is unindexable, before any positions are produced.
        let mut plan: Vec<Touched<'_>> = Vec::new();
        for entry in &self.sealed {
            let Some(max) = entry.max_position() else {
                continue;
            };
            if max <= after.get() {
                continue; // whole range is at or before `after`
            }
            match &entry.seg {
                Some(seg) => plan.push(Touched::Seg(seg.as_ref())),
                None => {
                    return Err(IndexError::Unindexable {
                        range: PositionRange {
                            first: entry.base,
                            last: Position::new(max),
                        },
                    });
                }
            }
        }
        if self.active_span > 0 {
            let max = self.active_base.get() + self.active_span - 1;
            if max > after.get() {
                if self.active_unindexable {
                    return Err(IndexError::Unindexable {
                        range: PositionRange {
                            first: self.active_base,
                            last: Position::new(max),
                        },
                    });
                }
                plan.push(Touched::Active(self.active.as_ref()));
            }
        }

        let mut out = Vec::new();
        for touched in plan {
            match touched {
                Touched::Seg(seg) => out.extend(search(seg, query, after)),
                // The active tail is queried through a full (unbounded) view: `search_all`
                // runs on the writer thread, so there is no watermark to clamp to.
                Touched::Active(tail) => out.extend(search(&tail.view_full(), query, after)),
            }
        }
        Ok(out.into_iter())
    }
}

/// A segment a query touches: either a sealed on-disk segment or the active tail. Kept as
/// a plan so the unindexable check runs before any positions are produced.
enum Touched<'a> {
    Seg(&'a IndexSegment),
    Active(&'a ActiveTail),
}

impl std::fmt::Debug for IndexSet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IndexSet")
            .field("dir", &self.dir)
            .field("sealed", &self.sealed.len())
            .field("active_base", &self.active_base)
            .field("active_span", &self.active_span)
            .field("active_unindexable", &self.active_unindexable)
            .finish()
    }
}

/// Loads `path` if it is a valid index segment whose base and event count match the log
/// segment. Returns `None` (rebuild it) for a missing, corrupt, or mismatched file.
fn load_valid(path: &Path, base: Position, count: u64) -> Result<Option<IndexSegment>, IndexError> {
    let bytes = match std::fs::read(path) {
        Ok(bytes) => bytes,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(IndexError::io(path, err)),
    };
    match IndexSegment::from_bytes(Arc::from(bytes)) {
        Ok(seg) if seg.header().base_position == base && seg.header().event_count == count => {
            Ok(Some(seg))
        }
        Ok(_) => {
            #[cfg(feature = "tracing")]
            tracing::warn!("index segment {path:?} disagrees with the log segment; rebuilding");
            Ok(None)
        }
        Err(_err) => {
            #[cfg(feature = "tracing")]
            tracing::warn!("index segment {path:?} is corrupt ({_err}); rebuilding from the log");
            Ok(None)
        }
    }
}

/// Rebuilds one sealed segment from the log and seals a fresh `.idx`, or records it
/// unindexable if its log holds more distinct types than the type column can address.
fn build_and_seal(
    set: &SegmentSet,
    dir: &Path,
    base: Position,
    count: u64,
) -> Result<SealedIndex, IndexError> {
    let rebuilt = rebuild_range(set, base, count)?;
    if rebuilt.unindexable {
        #[cfg(feature = "tracing")]
        tracing::error!(
            "sealed segment at base {base} exceeds the per-segment type limit; queries over it will error"
        );
        return Ok(SealedIndex {
            base,
            count,
            seg: None,
        });
    }
    let data: Arc<[u8]> = Arc::from(IndexSegment::encode(&rebuilt.index));
    let path = dir.join(index_file_name(base));
    if let Err(err) = write_segment_file(&path, &data) {
        #[cfg(feature = "tracing")]
        tracing::error!("failed to persist rebuilt index segment {path:?}: {err}");
        let _ = err;
    }
    let seg = IndexSegment::from_bytes(Arc::clone(&data))
        .expect("a just-encoded index segment must validate");
    Ok(SealedIndex {
        base,
        count,
        seg: Some(Arc::new(seg)),
    })
}

/// Scans `count` records from `base` and rebuilds a tail index. Drives the pure
/// [`Rebuilder`] against the log's lending scan (which cannot be a plain iterator).
fn rebuild_range(
    set: &SegmentSet,
    base: Position,
    count: u64,
) -> Result<super::recovery::Rebuilt, IndexError> {
    let mut builder = Rebuilder::new(base);
    let mut scan = set.scan_from(base);
    let mut seen = 0u64;
    while seen < count {
        match scan.next() {
            Some(item) => {
                let record = item.map_err(|source| IndexError::Log(Arc::new(source)))?;
                let event = EventRef::from_bytes(record.data).map_err(IndexError::Corrupt)?;
                builder.feed(record.position, event);
                seen += 1;
            }
            None => break,
        }
    }
    Ok(builder.finish())
}

/// `{base_position:020}.idx`, zero-padded so lexicographic order equals numeric order,
/// mirroring the log's `{base:020}.log`.
fn index_file_name(base: Position) -> String {
    format!("{:0width$}.idx", base.get(), width = NAME_DIGITS)
}

fn sync_dir(dir: &Path) -> io::Result<()> {
    File::open(dir)?.sync_all()
}

/// An index-layer error. Every variant is recoverable at the store level: an I/O or
/// rebuild failure is surfaced, and an [`Unindexable`](IndexError::Unindexable) query
/// range is a signal to scan the log for that range, never a wrong answer.
#[derive(Debug, Error)]
pub enum IndexError {
    #[error("index i/o error at {path}: {source}")]
    Io { path: PathBuf, source: io::Error },
    #[error("log error while rebuilding the index: {0}")]
    Log(Arc<LogError>),
    #[error("corrupt event while rebuilding the index: {0}")]
    Corrupt(DecodeError),
    #[error(
        "positions {}..={} cannot be answered from the index (a segment exceeds the per-segment type limit); scan the log for this range",
        range.first,
        range.last
    )]
    Unindexable { range: PositionRange },
}

impl IndexError {
    fn io(path: &Path, source: io::Error) -> Self {
        IndexError::Io {
            path: path.to_path_buf(),
            source,
        }
    }
}

// Lock in that the set is `Send` (it moves into the writer thread) and `Sync`: as of
// phase 6b the active tail is a shared `Arc<ActiveTail>` that reader threads query
// lock-free, so `&IndexSet` may cross threads. A future change reintroducing a
// non-`Sync` field would fail this build.
const _: fn() = || {
    fn is_send<T: Send>() {}
    fn is_sync<T: Sync>() {}
    is_send::<IndexSet>();
    is_sync::<IndexSet>();
};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::{Event, EventType, Tag, Tags};
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

    /// The event templates cycled through the fixtures: a mix of types and tag shapes so
    /// queries have varied answers.
    fn templates() -> Vec<Event> {
        vec![
            event("Registered", &["course:c1"]),
            event("Enrolled", &["course:c1", "student:s1"]),
            event("Renamed", &["student:s1"]),
            event("Registered", &["course:c2"]),
            event("Enrolled", &["course:c2", "student:s2"]),
            event("Renamed", &["student:s2"]),
            event("Registered", &["course:c1"]),
            event("Enrolled", &["course:c1", "student:s2"]),
        ]
    }

    /// Appends 40 events one per batch to a small-segment log so several segments seal.
    fn build_log(dir: &Path) -> SegmentSet {
        // A tiny segment forces frequent rollovers, so the index spans several segments.
        let mut set = SegmentSet::open(dir, SegmentConfig::new(512)).unwrap();
        let templates = templates();
        for i in 0..40 {
            set.append_batch(&[templates[i % templates.len()].as_bytes()])
                .unwrap();
        }
        set
    }

    fn scan_baseline(set: &SegmentSet, query: &Query, after: Position) -> Vec<Position> {
        let mut out = Vec::new();
        let mut scan = set.scan_after(after);
        while let Some(item) = scan.next() {
            let record = item.unwrap();
            let event = EventRef::from_bytes(record.data).unwrap();
            if query.matches(event) {
                out.push(record.position);
            }
        }
        out
    }

    #[test]
    fn open_rebuilds_and_answers_across_multiple_segments() {
        let dir = TempDir::new().unwrap();
        let set = build_log(dir.path());
        assert!(
            set.sealed_len() >= 1,
            "small segments should have sealed some"
        );
        let index = IndexSet::open(&set).unwrap();

        let queries = [
            Query::all(),
            Query::item(QueryItem::with_tags(tags(&["course:c1"]))),
            Query::item(QueryItem::with_tags(tags(&["student:s2"]))),
            Query::item(QueryItem::of_types(vec![
                EventType::new("Enrolled").unwrap(),
            ])),
            Query::items(vec![
                QueryItem::with_tags(tags(&["course:c1"])),
                QueryItem::with_tags(tags(&["student:s2"])),
            ]),
        ];
        let last = set.last_position().get();
        for query in &queries {
            for after in 0..=last {
                let from_index: Vec<Position> = index
                    .search_all(query, Position::new(after))
                    .unwrap()
                    .collect();
                let from_scan = scan_baseline(&set, query, Position::new(after));
                assert_eq!(from_index, from_scan, "query {query:?} after {after}");
            }
        }
    }

    #[test]
    fn reopen_uses_persisted_segments() {
        let dir = TempDir::new().unwrap();
        let set = build_log(dir.path());
        // First open seals fresh .idx files for every sealed segment.
        drop(IndexSet::open(&set).unwrap());
        // .idx files now exist for each sealed segment.
        let idx_dir = dir.path().join("index");
        let idx_count = std::fs::read_dir(&idx_dir).unwrap().count();
        assert_eq!(idx_count, set.sealed_len());
        // Second open loads them and still agrees with the scan.
        let index = IndexSet::open(&set).unwrap();
        let q = Query::item(QueryItem::with_tags(tags(&["course:c1"])));
        let from_index: Vec<Position> = index.search_all(&q, Position::ZERO).unwrap().collect();
        let from_scan = scan_baseline(&set, &q, Position::ZERO);
        assert_eq!(from_index, from_scan);
    }

    #[test]
    fn deleted_idx_is_rebuilt() {
        let dir = TempDir::new().unwrap();
        let set = build_log(dir.path());
        drop(IndexSet::open(&set).unwrap());
        // Delete one sealed .idx; the next open must rebuild it.
        let idx_dir = dir.path().join("index");
        let first = std::fs::read_dir(&idx_dir)
            .unwrap()
            .filter_map(Result::ok)
            .map(|e| e.path())
            .min()
            .unwrap();
        std::fs::remove_file(&first).unwrap();
        let index = IndexSet::open(&set).unwrap();
        let q = Query::all();
        let from_index: Vec<Position> = index.search_all(&q, Position::ZERO).unwrap().collect();
        let from_scan = scan_baseline(&set, &q, Position::ZERO);
        assert_eq!(from_index, from_scan);
    }

    #[test]
    fn corrupt_idx_body_is_rebuilt() {
        let dir = TempDir::new().unwrap();
        let set = build_log(dir.path());
        drop(IndexSet::open(&set).unwrap());
        // Corrupt one .idx body byte (leave the header CRC valid): open must rebuild it.
        let idx_dir = dir.path().join("index");
        let first = std::fs::read_dir(&idx_dir)
            .unwrap()
            .filter_map(Result::ok)
            .map(|e| e.path())
            .min()
            .unwrap();
        let mut bytes = std::fs::read(&first).unwrap();
        let last = bytes.len() - 1;
        bytes[last] ^= 0xFF;
        std::fs::write(&first, &bytes).unwrap();

        let index = IndexSet::open(&set).unwrap();
        let q = Query::all();
        let from_index: Vec<Position> = index.search_all(&q, Position::ZERO).unwrap().collect();
        let from_scan = scan_baseline(&set, &q, Position::ZERO);
        assert_eq!(from_index, from_scan);
    }

    #[test]
    fn unindexable_segment_errors_when_touched_but_not_when_pruned() {
        // A degraded segment must make a covering query error with its range, never return
        // a short answer; a query pruned away from it still succeeds. Built white-box
        // because a real >u16::MAX-types segment is not a practical fixture.
        let dir = TempDir::new().unwrap();
        let index = IndexSet {
            dir: dir.path().to_path_buf(),
            sealed: vec![SealedIndex {
                base: Position::new(1),
                count: 3,
                seg: None,
            }],
            active: Arc::new(super::super::ActiveTail::new(Position::new(4))),
            active_base: Position::new(4),
            active_span: 0,
            active_unindexable: false,
        };

        // `after = 0` touches the unindexable segment: error naming its exact range.
        match index.search_all(&Query::all(), Position::ZERO) {
            Err(IndexError::Unindexable { range }) => {
                assert_eq!(range.first, Position::new(1));
                assert_eq!(range.last, Position::new(3));
            }
            other => panic!("expected Unindexable error, got {other:?}"),
        }

        // `after = 3` prunes the whole segment away (max = 3 <= after), so the query
        // succeeds (and is empty, since nothing else is indexed).
        let got: Vec<Position> = index
            .search_all(&Query::all(), Position::new(3))
            .unwrap()
            .collect();
        assert!(got.is_empty());
    }

    #[test]
    fn live_feed_and_seal_agree_with_scan() {
        // Exercises the live push + seal_active path (what the coordinator drives).
        let dir = TempDir::new().unwrap();
        let mut set = SegmentSet::open(dir.path(), SegmentConfig::new(512)).unwrap();
        let mut index = IndexSet::open(&set).unwrap();

        let templates = templates();
        for i in 0..40 {
            let ev = &templates[i % templates.len()];
            // Mirror the coordinator's seam ordering: detect rollover, seal, then feed.
            let sealed_before = set.sealed_len();
            let range = set.append_batch(&[ev.as_bytes()]).unwrap();
            if set.sealed_len() > sealed_before {
                index.seal_active(range.first);
            }
            index.push(range.first, ev.as_ref());
        }
        assert!(set.sealed_len() >= 1, "should have rolled over");

        let last = set.last_position().get();
        let q = Query::item(QueryItem::with_tags(tags(&["course:c1"])));
        for after in 0..=last {
            let from_index: Vec<Position> = index
                .search_all(&q, Position::new(after))
                .unwrap()
                .collect();
            let from_scan = scan_baseline(&set, &q, Position::new(after));
            assert_eq!(from_index, from_scan, "after {after}");
        }
    }
}
