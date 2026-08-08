//! The active tail: the in-memory index over the one *active* (still-growing) segment,
//! shared lock-free between the writer thread that feeds it and any reader thread that
//! queries it, bounded on read by the published watermark (CLAUDE.md 9).
//!
//! Two structures, split on cardinality (CLAUDE.md 3, 7):
//!
//! - **Tags (high cardinality) -> an inverted index.** Each distinct tag interns to a
//!   [`TermId`](super::TermId) (via a concurrent [`DashMap`]) whose [`PostingSlot`] holds the
//!   *local* positions (global minus [`base`](ActiveTail::base)) of every event carrying it,
//!   ascending by construction because events are fed in position order.
//! - **Types (low cardinality) -> a dense column.** `type_column[local]` is the event's
//!   [`TypeId`](super::TypeId), an `AtomicU16` per local position.
//!
//! ## Concurrency
//!
//! Feeding is single-producer (the writer thread) via [`push`](ActiveTail::push); reading is
//! multi-consumer through a watermark-bounded [`ActiveView`]. Soundness without `unsafe`:
//!
//! - The columns are append-only [`ChunkedVec`]s of atomic slots. The writer only writes
//!   slots at or above the current length; a reader only reads slots below its pinned
//!   watermark; the two never touch the same slot, and per-slot atomics mean no aliasing
//!   `&mut`. Value visibility rides the watermark's release/acquire (type column) or the
//!   posting slot's own length (postings).
//! - The two interners are `DashMap`s: the writer inserts under one shard lock, a reader
//!   probes under one shard lock. A single-probe hold satisfies "no lock across query
//!   evaluation" (CLAUDE.md 9). The reverse (id -> string) maps the sealer needs are
//!   reconstructed from the `DashMap`s at seal time, so nothing reader-facing is duplicated.
//!
//! This is the in-memory counterpart of the on-disk index segment built in 5b; the two share
//! one evaluator ([`search`](super::search)) via the [`SegmentIndex`] trait, [`ActiveView`]
//! being the tail's implementation of it.

use std::borrow::Cow;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU16, Ordering};

use dashmap::DashMap;
use thiserror::Error;

use crate::Position;
use crate::event::EventRef;

use super::append::{ChunkedVec, PostingSlot, Snap};
use super::{SegmentIndex, TermId, TypeId};

/// An index over the events of one segment, fed strictly in position order and shared for
/// reading behind an `Arc<ActiveTail>`.
pub struct ActiveTail {
    /// The first global position covered: `global = base + local`.
    base: Position,
    /// Local position to [`TypeId`] value (`AtomicU16`), one slot per event.
    type_column: ChunkedVec<AtomicU16>,
    /// Per-[`TermId`] posting lists, indexed densely by term id.
    postings: ChunkedVec<PostingSlot>,
    /// Tag string to its dense [`TermId`]. Shared so an [`ActiveView`] probes the same map.
    tags: Arc<DashMap<Arc<str>, TermId>>,
    /// Event-type string to its dense [`TypeId`]. A `DashMap` (not a copy-on-insert map) so a
    /// segment holding many distinct types stays `O(n)`, not `O(n^2)`, to build.
    types: Arc<DashMap<Arc<str>, TypeId>>,
    /// Set once this segment exceeds the `u16` type limit: feeding then stops while positions
    /// keep arriving, so the columns are truncated relative to the watermark. A reader must
    /// see this **live** (the latch fires mid-segment, with no snapshot republish), which is
    /// why it lives on the shared tail rather than in the writer-only [`IndexSet`]. Readers
    /// then scan the log for the active range instead of trusting the short columns.
    unindexable: AtomicBool,
}

impl std::fmt::Debug for ActiveTail {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ActiveTail")
            .field("base", &self.base)
            .field("len", &self.len())
            .field("terms", &self.postings.len())
            .field("types", &self.types.len())
            .finish()
    }
}

impl ActiveTail {
    /// A fresh index whose first event will sit at global position `base`.
    pub fn new(base: Position) -> Self {
        ActiveTail {
            base,
            type_column: ChunkedVec::new(),
            postings: ChunkedVec::new(),
            tags: Arc::new(DashMap::new()),
            types: Arc::new(DashMap::new()),
            unindexable: AtomicBool::new(false),
        }
    }

    /// Indexes `event`, assigned global position `position`.
    ///
    /// `position` must be exactly the next in sequence (`base + len`). This is a real
    /// `assert`, not `debug_assert`: it is O(1), the writer's dense monotonic positions
    /// guarantee it, and feeding out of order would silently produce unsorted posting lists
    /// that break every downstream intersection.
    ///
    /// Single-producer: only the writer thread calls this. On [`TooManyTypes`] nothing is
    /// mutated (the fallible type intern runs before any column write), so a rejected event
    /// is a clean no-op.
    pub fn push(&self, position: Position, event: EventRef<'_>) -> Result<(), TooManyTypes> {
        let len = self.type_column.len();
        let expected = Position::new(self.base.get() + len as u64);
        assert_eq!(
            position, expected,
            "tail index fed out of order: expected {expected}, got {position}"
        );

        // Type intern first: it is the only fallible step, so a rejected push mutates nothing.
        let type_id = self.intern_type(event.event_type())?;

        let local = len;
        for tag in event.tags() {
            let term = self.intern_tag(tag);
            self.postings
                .with(term.get(), |slot| slot.push_local(local));
        }
        // Column store last: it carries `local`'s type and bumps the length. Both this and
        // the postings above become visible to a reader only once the watermark passes
        // `local`, which the writer publishes after this returns.
        self.type_column
            .push_with(|slot| slot.store(type_id.get(), Ordering::Relaxed));
        Ok(())
    }

    /// Interns a tag, creating its (empty) posting slot **before** publishing the mapping, so
    /// a reader that resolves the mapping always finds the slot. Writer-only.
    fn intern_tag(&self, tag: &str) -> TermId {
        if let Some(id) = self.tags.get(tag).map(|r| *r) {
            return id;
        }
        let id = TermId(self.postings.push_with(|_| {}));
        self.tags.insert(Arc::from(tag), id);
        id
    }

    /// Interns an event type, rejecting the `u16::MAX + 1`-th distinct one (the width of the
    /// dense type column) with the offending string in hand. Writer-only.
    fn intern_type(&self, name: &str) -> Result<TypeId, TooManyTypes> {
        if let Some(id) = self.types.get(name).map(|r| *r) {
            return Ok(id);
        }
        let next = self.types.len();
        // Valid ids are 0..=u16::MAX, so a new id fits only while len <= u16::MAX.
        if next > u16::MAX as usize {
            return Err(TooManyTypes {
                max: u16::MAX as usize + 1,
            });
        }
        let id = TypeId(next as u16);
        self.types.insert(Arc::from(name), id);
        Ok(id)
    }

    /// The first global position covered by this index.
    pub fn base(&self) -> Position {
        self.base
    }

    /// The number of events indexed.
    pub fn len(&self) -> u32 {
        self.type_column.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Latches this segment unindexable (writer, on a too-many-types rejection). Release, so
    /// a reader that observes the watermark published afterward also observes this flag.
    pub(crate) fn mark_unindexable(&self) {
        self.unindexable.store(true, Ordering::Release);
    }

    /// Whether this segment is unindexable: its columns are truncated relative to the
    /// watermark, so a reader must scan the log for its range rather than query the tail.
    /// Acquire, paired with [`mark_unindexable`](Self::mark_unindexable) and the watermark.
    pub fn is_unindexable(&self) -> bool {
        self.unindexable.load(Ordering::Acquire)
    }

    /// A reader view bounded by `watermark` (a global position): it exposes exactly the
    /// events at or before `watermark`. Clone the backbones **after** the watermark has been
    /// loaded (as the read path does), so they cover every visible local.
    pub fn view(&self, watermark: Position) -> ActiveView {
        self.make_view(self.visible_len(watermark))
    }

    /// A reader view over the whole tail as fed so far. Used on the writer thread (where
    /// there is no watermark), e.g. by [`IndexSet::search_all`](super::IndexSet).
    pub fn view_full(&self) -> ActiveView {
        self.make_view(self.len())
    }

    /// Number of visible locals for a `watermark`: locals `0..upto` are those whose global
    /// position (`base + local`) is at or before `watermark`.
    fn visible_len(&self, watermark: Position) -> u32 {
        if watermark.get() >= self.base.get() {
            (watermark.get() - self.base.get() + 1).min(u32::MAX as u64) as u32
        } else {
            0
        }
    }

    fn make_view(&self, upto_len: u32) -> ActiveView {
        let type_column = self.type_column.snapshot();
        let postings = self.postings.snapshot();
        // Clamp to what the cloned type column actually covers, so `type_at` is always in
        // range regardless of the watermark/backbone timing argument (CLAUDE.md 9).
        let upto_len = upto_len.min(type_column.covered());
        ActiveView {
            base: self.base,
            upto_len,
            type_column,
            postings,
            tags: Arc::clone(&self.tags),
            types: Arc::clone(&self.types),
        }
    }

    // --- sealer accessors (crate-internal): the frozen inputs [`super::segment`] needs to
    // encode this tail into an on-disk index segment. Called on the writer thread once the
    // segment has stopped being fed. ---

    /// The dense type column, one [`TypeId`] value per local position, in order.
    pub(crate) fn type_column(&self) -> Vec<u16> {
        let snap = self.type_column.snapshot();
        (0..self.len())
            .map(|i| snap.get(i).map(|s| s.load(Ordering::Relaxed)).unwrap_or(0))
            .collect()
    }

    /// The type strings in [`TypeId`] order, written as the type dictionary.
    pub(crate) fn type_names(&self) -> Vec<Arc<str>> {
        let mut names: Vec<Arc<str>> = vec![Arc::from(""); self.types.len()];
        for entry in self.types.iter() {
            names[entry.value().get() as usize] = Arc::clone(entry.key());
        }
        names
    }

    /// Every tag and its ascending postings, **sorted by the (unique) tag string** so the
    /// sealer feeds the FST in lexicographic key order. The sort is total and independent of
    /// [`TermId`] order, so the on-disk layout does not depend on `DashMap` iteration order.
    pub(crate) fn terms_sorted_with_postings(&self) -> Vec<(Arc<str>, Vec<u32>)> {
        let snap = self.postings.snapshot();
        let upto = self.len();
        let mut terms: Vec<(Arc<str>, Vec<u32>)> = self
            .tags
            .iter()
            .map(|entry| {
                let mut postings = Vec::new();
                if let Some(slot) = snap.get(entry.value().get()) {
                    slot.collect_below(upto, &mut postings);
                }
                (Arc::clone(entry.key()), postings)
            })
            .collect();
        terms.sort_by(|a, b| a.0.cmp(&b.0));
        terms
    }
}

/// A watermark-bounded, lock-free reader view over an [`ActiveTail`]. It owns cloned backbone
/// snapshots (taken after the watermark load) and shared `DashMap` handles, so evaluating a
/// query over it touches only atomics and single-shard probes, never a lock held across the
/// evaluation (CLAUDE.md 9).
pub struct ActiveView {
    base: Position,
    /// Visible local count: only locals `0..upto_len` are exposed.
    upto_len: u32,
    type_column: Snap<AtomicU16>,
    postings: Snap<PostingSlot>,
    tags: Arc<DashMap<Arc<str>, TermId>>,
    types: Arc<DashMap<Arc<str>, TypeId>>,
}

/// The active tail is the in-memory arm of [`SegmentIndex`], surfaced through a bounded
/// [`ActiveView`]. Unlike the zero-copy on-disk borrow, `term_postings` materializes an owned
/// vector: the postings are chunked (not one contiguous slice) and are truncated to the
/// watermark, so a copy is unavoidable here. It is bounded by one segment and off the hot
/// sealed path; 6c measures whether it warrants a lending-iterator contract change.
impl SegmentIndex for ActiveView {
    fn base(&self) -> Position {
        self.base
    }

    fn len(&self) -> u32 {
        self.upto_len
    }

    fn term_postings(&self, tag: &str) -> Option<Cow<'_, [u32]>> {
        let term = self.tags.get(tag).map(|r| *r)?;
        // A term id beyond the cloned backbone was interned after this view: its events sit
        // past the watermark, so it has no visible postings.
        let slot = self.postings.get(term.get())?;
        let mut out = Vec::new();
        slot.collect_below(self.upto_len, &mut out);
        Some(Cow::Owned(out))
    }

    fn type_id(&self, name: &str) -> Option<u16> {
        self.types.get(name).map(|r| r.get())
    }

    fn type_at(&self, local: u32) -> u16 {
        // Only ever called for `local < upto_len`, which the clamp guarantees is covered.
        self.type_column
            .get(local)
            .map(|s| s.load(Ordering::Relaxed))
            .unwrap_or(0)
    }
}

/// A segment held more distinct event types than the dense `u16` type column can address.
#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
#[error("too many distinct event types in one segment (maximum {max})")]
pub struct TooManyTypes {
    pub max: usize,
}

// The active tail is shared across threads from 6b on: the writer feeds it while readers
// query it lock-free. Lock in both bounds so a future change that reintroduces a non-atomic
// field fails the build rather than silently regressing.
const _: fn() = || {
    fn is_send<T: Send>() {}
    fn is_sync<T: Sync>() {}
    is_send::<ActiveTail>();
    is_sync::<ActiveTail>();
    is_send::<ActiveView>();
    is_sync::<ActiveView>();
};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::{Event, EventType, Tag, Tags};
    use crate::index::search;
    use crate::query::{Query, QueryItem};
    use smallvec::SmallVec;

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
        Event::new(&EventType::new(ty).unwrap(), &tags(tag_strs), b"").unwrap()
    }

    /// Feeds events starting at `base`, one position each, in order.
    fn build(base: u64, events: &[Event]) -> ActiveTail {
        let index = ActiveTail::new(Position::new(base));
        for (i, ev) in events.iter().enumerate() {
            index
                .push(Position::new(base + i as u64), ev.as_ref())
                .unwrap();
        }
        index
    }

    /// The ascending postings for `tag` in the full view, or `None` if the tag is absent.
    fn postings(index: &ActiveTail, tag: &str) -> Option<Vec<u32>> {
        index.view_full().term_postings(tag).map(|c| c.into_owned())
    }

    #[test]
    fn postings_are_ascending_and_correct() {
        // Positions 1..=4, tag "a" on 1 and 3, tag "b" on 2 and 3.
        let events = [
            event("E", &["a"]),
            event("E", &["b"]),
            event("E", &["a", "b"]),
            event("E", &["c"]),
        ];
        let index = build(1, &events);
        assert_eq!(index.len(), 4);

        // Local positions (global - base): a at 0 and 2, b at 1 and 2.
        assert_eq!(postings(&index, "a"), Some(vec![0, 2]));
        assert_eq!(postings(&index, "b"), Some(vec![1, 2]));
        assert_eq!(postings(&index, "absent"), None);
    }

    #[test]
    fn type_column_tracks_each_event() {
        let events = [
            event("Registered", &["a"]),
            event("Enrolled", &["b"]),
            event("Registered", &["c"]),
        ];
        let index = build(1, &events);
        let view = index.view_full();

        let registered = view.type_id("Registered").unwrap();
        let enrolled = view.type_id("Enrolled").unwrap();
        assert_ne!(registered, enrolled);
        assert_eq!(view.type_at(0), registered);
        assert_eq!(view.type_at(1), enrolled);
        assert_eq!(view.type_at(2), registered);
        assert_eq!(view.type_id("Missing"), None);
    }

    #[test]
    fn base_offsets_local_positions() {
        // A non-1 base: local 0 is global 100.
        let index = build(100, &[event("E", &["a"])]);
        assert_eq!(index.base(), Position::new(100));
        assert_eq!(postings(&index, "a"), Some(vec![0]));
    }

    #[test]
    fn view_is_bounded_by_the_watermark() {
        // Five events at globals 1..=5 all carry "a"; a view pinned at watermark 3 exposes
        // only locals 0..=2 (globals 1..=3).
        let events: Vec<Event> = (0..5).map(|_| event("E", &["a"])).collect();
        let index = build(1, &events);

        let view = index.view(Position::new(3));
        assert_eq!(view.len(), 3);
        assert_eq!(view.term_postings("a").unwrap().into_owned(), vec![0, 1, 2]);

        // A `search` over the bounded view stops at the watermark.
        let q = Query::item(QueryItem::with_tags(tags(&["a"])));
        let got: Vec<u64> = search(&view, &q, Position::ZERO).map(|p| p.get()).collect();
        assert_eq!(got, vec![1, 2, 3]);
    }

    #[test]
    #[should_panic(expected = "fed out of order")]
    fn out_of_order_push_panics() {
        let index = ActiveTail::new(Position::new(1));
        index
            .push(Position::new(1), event("E", &["a"]).as_ref())
            .unwrap();
        // Skips position 2: must trip the feed-order assert.
        index
            .push(Position::new(3), event("E", &["b"]).as_ref())
            .unwrap();
    }
}
