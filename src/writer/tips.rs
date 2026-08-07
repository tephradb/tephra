//! Tag tips: the fast-reject for the append condition.
//!
//! Two types live here, and the reason they are two is load-bearing. They differ only
//! in what an *absent* tag means:
//!
//! - [`TagTips`] is the durable main map, holding lossy recent-window knowledge. An
//!   absent tag means "not seen recently, and possibly present below the window floor",
//!   so it yields [`Verdict::Unknown`] unless the floor proves the tag cannot appear
//!   after the queried position.
//! - [`StagedTips`] is a batch-local map with *complete* knowledge of one drain window.
//!   An absent tag means "definitely not staged", full stop.
//!
//! Collapsing them into one type with a flag is exactly the subtlety that reads as
//! correct and is not: a shared map with `window_floor = tip` would return `Unknown`
//! for every `after < tip` and reject the first conditional request in a drain against
//! nothing.
//!
//! Keys are the tag strings (`Box<str>`), not a hash. A `HashMap<Box<str>, _>` looks up
//! by `&str` for free, so recording from `EventRef::tags()` and querying from a
//! `QueryItem`'s `Tag::as_str()` both avoid allocation on the read side. A hash would
//! throw away diagnosability in the one component whose failure mode (a false negative
//! that silently accepts a conflicting write) is invisible by design: a `u64` cannot
//! answer "which tag" when a spurious rejection or a property-test disagreement is
//! investigated. Phase 5 replaces this with an exact `TermId` interner, which discards a
//! hash rather than building on one. A window-bounded map of string keys costs a few MB
//! at realistic write rates; revisit only if profiling says it matters.

use std::collections::HashMap;

use crate::Position;
use crate::query::Query;

/// Default cap on the number of distinct tags [`TagTips`] retains before it evicts the
/// oldest and raises its window floor. Bounding is memory-only: correctness never
/// depends on the map's contents, only memory does (a smaller map yields more `Unknown`s
/// and more scans), which is what makes crude eviction acceptable.
const DEFAULT_MAX_ENTRIES: usize = 1 << 20;

/// The fast-reject verdict for a query against the durable tips.
///
/// Never a bool: a false negative is silent, so every call site must handle the
/// `Unknown` fallthrough explicitly rather than treating a missing "true" as "no".
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Verdict {
    /// No event matching the query can exist after the queried position. Skip the scan.
    DefinitelyNoMatch,
    /// The tips cannot rule it out. Fall through to the scan oracle.
    Unknown,
}

/// Durable, lossy recent-window knowledge: highest position seen per tag, bounded to a
/// recent window. The fast-reject in front of the condition scan.
#[derive(Debug)]
pub struct TagTips {
    max_pos: HashMap<Box<str>, Position>,
    /// A tag absent from `max_pos` has its highest position treated as this floor, an
    /// upper bound on where an unrecorded tag could sit. Monotonic non-decreasing, so a
    /// tag whose only event predates construction stays below it forever and can never
    /// produce a false negative.
    window_floor: Position,
    window: u64,
    max_entries: usize,
}

impl TagTips {
    /// A fresh map that knows nothing below `window_floor` (pass the log's
    /// `next_position`). While cold it returns `Unknown` for everything, so every early
    /// append scans: correct and intended warm-up, not a bug.
    pub fn new(window_floor: Position, window: u64) -> Self {
        TagTips {
            max_pos: HashMap::new(),
            window_floor,
            window,
            max_entries: DEFAULT_MAX_ENTRIES,
        }
    }

    /// Records that `tag` was seen at `position`, keeping the max. Production warms the
    /// map through [`absorb`](Self::absorb) after a durable commit; this direct form is
    /// currently used only by tests.
    #[cfg(test)]
    fn record(&mut self, tag: &str, position: Position) {
        self.max_pos
            .entry(Box::from(tag))
            .and_modify(|p| {
                if position > *p {
                    *p = position;
                }
            })
            .or_insert(position);
    }

    /// The highest position recorded for `tag`, or the window floor if absent (an upper
    /// bound: an unrecorded tag's true max is strictly below the floor).
    fn max_for(&self, tag: &str) -> Position {
        self.max_pos
            .get(tag)
            .copied()
            .unwrap_or(self.window_floor)
    }

    /// The current window floor.
    #[cfg(test)]
    fn window_floor(&self) -> Position {
        self.window_floor
    }

    /// The fast-reject verdict for `query` against events strictly after `after`.
    ///
    /// An item is provably unable to match when *any* of its tags cannot appear after
    /// `after` (its recorded max is `<= after`, or it is absent and the floor rules it
    /// out). The whole query is `DefinitelyNoMatch` only when *every* item is; a single
    /// item the tips cannot rule out makes the query `Unknown`.
    pub fn may_match(&self, query: &Query, after: Position) -> Verdict {
        match query {
            // `All` matches any event; only a scan can decide whether one exists.
            Query::All => Verdict::Unknown,
            Query::Items(items) => {
                for item in items {
                    if self.item_unknown(item, after) {
                        return Verdict::Unknown;
                    }
                }
                Verdict::DefinitelyNoMatch
            }
        }
    }

    /// Whether the tips cannot rule this item out. An item with no tags can never be
    /// fast-rejected (there is no tag to prove absent). Otherwise the item is ruled out
    /// (returns false) as soon as one tag has `max_for(tag) <= after`.
    fn item_unknown(&self, item: &crate::query::QueryItem, after: Position) -> bool {
        if item.tags.is_empty() {
            return true;
        }
        item.tags.iter().all(|t| self.max_for(t.as_str()) > after)
    }

    /// Merges a completed drain window's staged tags in (call only after the batch is
    /// durable), then evicts if the map has grown past its cap.
    pub fn absorb(&mut self, staged: StagedTips, next_position: Position) {
        for (tag, position) in staged.tag_positions {
            // `tag` is owned; keep the max without a second allocation.
            self.max_pos
                .entry(tag)
                .and_modify(|p| {
                    if position > *p {
                        *p = position;
                    }
                })
                .or_insert(position);
        }
        self.evict_if_needed(next_position);
    }

    /// Bounds memory. Raises the floor to `next_position - window` (never lowering it, so
    /// the no-false-negative invariant holds) and drops entries at or below it.
    fn evict_if_needed(&mut self, next_position: Position) {
        if self.max_pos.len() <= self.max_entries {
            return;
        }
        let target = Position::new(next_position.get().saturating_sub(self.window));
        let new_floor = self.window_floor.max(target);
        self.window_floor = new_floor;
        self.max_pos.retain(|_, &mut pos| pos > new_floor);
    }

    #[cfg(test)]
    fn with_max_entries(window_floor: Position, window: u64, max_entries: usize) -> Self {
        TagTips {
            max_pos: HashMap::new(),
            window_floor,
            window,
            max_entries,
        }
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.max_pos.len()
    }
}

/// Complete knowledge of one drain window: every tag staged so far, at its assigned
/// position. Created empty per drain, then either merged into [`TagTips`] once the batch
/// is durable or dropped if the append failed.
#[derive(Debug, Default)]
pub struct StagedTips {
    tag_positions: HashMap<Box<str>, Position>,
}

impl StagedTips {
    pub fn new() -> Self {
        StagedTips::default()
    }

    /// Records a staged event's tag at its (not-yet-durable) assigned position.
    pub fn record(&mut self, tag: &str, position: Position) {
        self.tag_positions
            .entry(Box::from(tag))
            .and_modify(|p| {
                if position > *p {
                    *p = position;
                }
            })
            .or_insert(position);
    }

    pub fn is_empty(&self) -> bool {
        self.tag_positions.is_empty()
    }

    fn contains(&self, tag: &str) -> bool {
        self.tag_positions.contains_key(tag)
    }

    /// Whether some staged event might satisfy `query`. Conservative and tag-only: it
    /// ignores the query's type constraint (the staged map has no types), so two
    /// same-window requests sharing an item's full tag set conflict even if a precise
    /// type check would clear them. Safe (never accepts a true conflict) and
    /// self-correcting (the loser retries and gets the precise durable verdict).
    ///
    /// Every staged event has a position strictly above the durable tip, hence above any
    /// valid `after` (asserted `after <= tip`), so mere presence of a required tag set is
    /// enough: no position comparison is needed here.
    pub fn may_conflict(&self, query: &Query) -> bool {
        if self.is_empty() {
            return false;
        }
        match query {
            // Any staged event is an event after `after`, so `All` conflicts.
            Query::All => true,
            // An item conflicts when all of its tags are staged. An item with no tags
            // matches any event, so it conflicts as soon as anything is staged (already
            // guaranteed by the `is_empty` guard above): `all` over no tags is true.
            Query::Items(items) => items
                .iter()
                .any(|item| item.tags.iter().all(|t| self.contains(t.as_str()))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::QueryItem;

    fn ty(s: &str) -> crate::event::EventType {
        crate::event::EventType::new(s).unwrap()
    }

    fn tags(items: &[&str]) -> crate::event::Tags {
        use smallvec::SmallVec;
        crate::event::Tags::new(
            items
                .iter()
                .map(|s| crate::event::Tag::new(s).unwrap())
                .collect::<SmallVec<[crate::event::Tag; 4]>>(),
        )
        .unwrap()
    }

    fn pos(n: u64) -> Position {
        Position::new(n)
    }

    // --- TagTips::may_match window boundary ---

    #[test]
    fn cold_map_is_all_unknown() {
        // Floor at next_position (100): nothing recorded, every query falls through.
        let tips = TagTips::new(pos(100), 1_000);
        let q = Query::item(QueryItem::with_tags(tags(&["course:c1"])));
        assert_eq!(tips.may_match(&q, pos(50)), Verdict::Unknown);
        assert_eq!(tips.may_match(&q, pos(99)), Verdict::Unknown);
    }

    #[test]
    fn recorded_tag_after_the_bound_is_unknown() {
        let mut tips = TagTips::new(pos(1), 1_000);
        tips.record("course:c1", pos(10));
        let q = Query::item(QueryItem::with_tags(tags(&["course:c1"])));
        // Recorded max 10 > after 5: cannot rule it out.
        assert_eq!(tips.may_match(&q, pos(5)), Verdict::Unknown);
    }

    #[test]
    fn recorded_tag_at_or_before_the_bound_is_no_match() {
        let mut tips = TagTips::new(pos(1), 1_000);
        tips.record("course:c1", pos(10));
        let q = Query::item(QueryItem::with_tags(tags(&["course:c1"])));
        // after == max: an event "after" 10 cannot be this tag (its max is 10).
        assert_eq!(tips.may_match(&q, pos(10)), Verdict::DefinitelyNoMatch);
        assert_eq!(tips.may_match(&q, pos(11)), Verdict::DefinitelyNoMatch);
    }

    #[test]
    fn absent_tag_ruled_out_only_at_or_above_floor() {
        // Floor 100: an absent tag's max is treated as 100.
        let tips = TagTips::new(pos(100), 1_000);
        let q = Query::item(QueryItem::with_tags(tags(&["ghost:x"])));
        // after >= floor: absent tag cannot appear after `after`.
        assert_eq!(tips.may_match(&q, pos(100)), Verdict::DefinitelyNoMatch);
        assert_eq!(tips.may_match(&q, pos(150)), Verdict::DefinitelyNoMatch);
        // after < floor: cannot conclude.
        assert_eq!(tips.may_match(&q, pos(99)), Verdict::Unknown);
    }

    #[test]
    fn and_within_item_rejects_if_any_tag_ruled_out() {
        let mut tips = TagTips::new(pos(1), 1_000);
        tips.record("course:c1", pos(10)); // present, > after
        // student:s1 absent, floor 1 <= after 5 -> ruled out -> item DefinitelyNoMatch.
        let q = Query::item(QueryItem::with_tags(tags(&["course:c1", "student:s1"])));
        assert_eq!(tips.may_match(&q, pos(5)), Verdict::DefinitelyNoMatch);
    }

    #[test]
    fn or_across_items_is_unknown_if_any_item_unknown() {
        let mut tips = TagTips::new(pos(1), 1_000);
        tips.record("course:c1", pos(10));
        let q = Query::items(vec![
            // ruled out (student absent, floor rules it out)
            QueryItem::with_tags(tags(&["student:s1"])),
            // unknown (course present after the bound)
            QueryItem::with_tags(tags(&["course:c1"])),
        ]);
        assert_eq!(tips.may_match(&q, pos(5)), Verdict::Unknown);
    }

    #[test]
    fn empty_tags_item_is_always_unknown() {
        let tips = TagTips::new(pos(1), 1_000);
        let q = Query::item(QueryItem::of_types(vec![ty("Registered")]));
        assert_eq!(tips.may_match(&q, pos(5)), Verdict::Unknown);
    }

    #[test]
    fn all_query_is_unknown() {
        let tips = TagTips::new(pos(1), 1_000);
        assert_eq!(tips.may_match(&Query::all(), pos(5)), Verdict::Unknown);
    }

    // --- eviction / floor monotonicity ---

    #[test]
    fn eviction_raises_floor_and_drops_old_entries() {
        let mut tips = TagTips::with_max_entries(pos(1), 10, 2);
        tips.record("a", pos(100));
        tips.record("b", pos(101));
        tips.record("c", pos(102)); // now 3 > cap 2
        let staged = StagedTips::new();
        tips.absorb(staged, pos(103)); // triggers eviction: floor -> 103 - 10 = 93
        assert_eq!(tips.window_floor(), pos(93));
        // Entries at/below 93 dropped; 100/101/102 all survive here.
        assert_eq!(tips.len(), 3);
    }

    #[test]
    fn floor_never_lowers() {
        let mut tips = TagTips::with_max_entries(pos(500), 10, 0);
        // next_position - window = 100 - 10 = 90, below the floor of 500: must not lower.
        tips.record("a", pos(600));
        tips.absorb(StagedTips::new(), pos(100));
        assert_eq!(tips.window_floor(), pos(500));
    }

    // --- StagedTips ---

    #[test]
    fn empty_staged_never_conflicts() {
        let staged = StagedTips::new();
        let q = Query::item(QueryItem::with_tags(tags(&["course:c1"])));
        assert!(!staged.may_conflict(&q));
        assert!(!staged.may_conflict(&Query::all()));
    }

    #[test]
    fn staged_conflicts_on_full_tag_set() {
        let mut staged = StagedTips::new();
        staged.record("course:c1", pos(10));
        staged.record("student:s1", pos(10));
        // All tags present -> conflict.
        let q = Query::item(QueryItem::with_tags(tags(&["course:c1", "student:s1"])));
        assert!(staged.may_conflict(&q));
    }

    #[test]
    fn staged_no_conflict_on_partial_tag_set() {
        let mut staged = StagedTips::new();
        staged.record("course:c1", pos(10));
        // Requires student:s1 too, which is not staged.
        let q = Query::item(QueryItem::with_tags(tags(&["course:c1", "student:s1"])));
        assert!(!staged.may_conflict(&q));
    }

    #[test]
    fn staged_conflicts_ignore_type() {
        // Conservative: type is ignored, so a shared tag set conflicts regardless.
        let mut staged = StagedTips::new();
        staged.record("course:c1", pos(10));
        let q = Query::item(QueryItem::new(vec![ty("SomeType")], tags(&["course:c1"])));
        assert!(staged.may_conflict(&q));
    }

    #[test]
    fn staged_all_query_conflicts_when_non_empty() {
        let mut staged = StagedTips::new();
        staged.record("x:1", pos(10));
        assert!(staged.may_conflict(&Query::all()));
    }
}
