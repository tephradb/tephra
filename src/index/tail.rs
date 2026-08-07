//! The tail index: an in-memory index over one contiguous run of events (a segment).
//!
//! Two structures, split on cardinality (CLAUDE.md 3, 7):
//!
//! - **Tags (high cardinality) -> an inverted index.** Each distinct tag interns to a
//!   [`TermId`](super::TermId) whose posting list holds the *local* positions (global
//!   minus [`base`](TailIndex::base)) of every event carrying it, ascending by
//!   construction because events are fed in position order.
//! - **Types (low cardinality) -> a dense column.** `type_column[local]` is the event's
//!   [`TypeId`](super::TypeId), two bytes each, indexed directly by local position.
//!
//! This is the in-memory counterpart of the on-disk index segment built in 5b. Its
//! per-tag postings also *subsume* phase 4's `TagTips` (a tag's max position is just the
//! last element of its posting list); the two are kept separate for now and revisited
//! when the condition fallthrough is wired to the index in phase 6.

use crate::Position;
use crate::event::EventRef;

use super::interner::{TermInterner, TooManyTypes, TypeInterner};
use super::{TermId, TypeId};

/// An index over the events of one segment, fed strictly in position order.
#[derive(Debug)]
pub struct TailIndex {
    /// The first global position covered: `global = base + local`.
    base: Position,
    terms: TermInterner,
    /// Per-term posting lists, indexed by [`TermId`]. `postings[id]` is ascending local
    /// positions. Dense: a fresh id is always exactly `postings.len()`.
    postings: Vec<Vec<u32>>,
    types: TypeInterner,
    /// Local position to [`TypeId`] value. `len()` equals the number of events indexed.
    type_column: Vec<u16>,
}

impl TailIndex {
    /// A fresh index whose first event will sit at global position `base`.
    pub fn new(base: Position) -> Self {
        TailIndex {
            base,
            terms: TermInterner::new(),
            postings: Vec::new(),
            types: TypeInterner::new(),
            type_column: Vec::new(),
        }
    }

    /// Indexes `event`, assigned global position `position`.
    ///
    /// `position` must be exactly the next in sequence (`base + len`). This is a real
    /// `assert`, not `debug_assert`: it is O(1), the writer's dense monotonic positions
    /// guarantee it, and feeding out of order would silently produce unsorted posting
    /// lists that break every downstream intersection, so it must survive a release build.
    ///
    /// On [`TooManyTypes`] the index is left unchanged (the fallible type intern runs
    /// before any posting or column mutation), so the caller may treat a rejected event
    /// as a no-op.
    pub fn push(&mut self, position: Position, event: EventRef<'_>) -> Result<(), TooManyTypes> {
        let expected = Position::new(self.base.get() + self.type_column.len() as u64);
        assert_eq!(
            position, expected,
            "tail index fed out of order: expected {expected}, got {position}"
        );

        // Intern the type first: it is the only fallible step, so doing it before any
        // mutation keeps a rejected push a clean no-op.
        let type_id = self.types.intern(event.event_type())?;

        let local = self.type_column.len() as u32;
        for tag in event.tags() {
            let term = self.terms.intern(tag);
            let idx = term.0 as usize;
            // Ids are dense and assigned in order, so a new one is always the next slot.
            if idx == self.postings.len() {
                self.postings.push(Vec::new());
            }
            self.postings[idx].push(local);
        }
        self.type_column.push(type_id.0);
        Ok(())
    }

    /// The first global position covered by this index.
    pub fn base(&self) -> Position {
        self.base
    }

    /// The number of events indexed.
    pub fn len(&self) -> usize {
        self.type_column.len()
    }

    pub fn is_empty(&self) -> bool {
        self.type_column.is_empty()
    }

    /// The id for `tag` if any indexed event carries it.
    pub fn term_id(&self, tag: &str) -> Option<TermId> {
        self.terms.get(tag)
    }

    /// The ascending local positions of every event carrying `id`.
    pub fn postings(&self, id: TermId) -> &[u32] {
        &self.postings[id.0 as usize]
    }

    /// The id for `event_type` if any indexed event has it.
    pub fn type_id(&self, event_type: &str) -> Option<TypeId> {
        self.types.get(event_type)
    }

    /// The [`TypeId`] value of the event at local position `local`.
    pub fn type_at(&self, local: u32) -> u16 {
        self.type_column[local as usize]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::{Event, EventType, Tag, Tags};
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
    fn build(base: u64, events: &[Event]) -> TailIndex {
        let mut index = TailIndex::new(Position::new(base));
        for (i, ev) in events.iter().enumerate() {
            index
                .push(Position::new(base + i as u64), ev.as_ref())
                .unwrap();
        }
        index
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

        let a = index.term_id("a").unwrap();
        let b = index.term_id("b").unwrap();
        // Local positions (global - base): a at 0 and 2, b at 1 and 2.
        assert_eq!(index.postings(a), &[0, 2]);
        assert_eq!(index.postings(b), &[1, 2]);
        assert_eq!(index.term_id("absent"), None);
    }

    #[test]
    fn type_column_tracks_each_event() {
        let events = [
            event("Registered", &["a"]),
            event("Enrolled", &["b"]),
            event("Registered", &["c"]),
        ];
        let index = build(1, &events);

        let registered = index.type_id("Registered").unwrap();
        let enrolled = index.type_id("Enrolled").unwrap();
        assert_ne!(registered, enrolled);
        assert_eq!(index.type_at(0), registered.0);
        assert_eq!(index.type_at(1), enrolled.0);
        assert_eq!(index.type_at(2), registered.0);
        assert_eq!(index.type_id("Missing"), None);
    }

    #[test]
    fn base_offsets_local_positions() {
        // A non-1 base: local 0 is global 100.
        let index = build(100, &[event("E", &["a"])]);
        assert_eq!(index.base(), Position::new(100));
        let a = index.term_id("a").unwrap();
        assert_eq!(index.postings(a), &[0]);
    }

    #[test]
    #[should_panic(expected = "fed out of order")]
    fn out_of_order_push_panics() {
        let mut index = TailIndex::new(Position::new(1));
        index.push(Position::new(1), event("E", &["a"]).as_ref()).unwrap();
        // Skips position 2: must trip the feed-order assert.
        index.push(Position::new(3), event("E", &["b"]).as_ref()).unwrap();
    }
}
