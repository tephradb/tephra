//! The index-driven query evaluator: the counterpart to the scan oracle.
//!
//! [`search`] answers a [`Query`] against a [`SegmentIndex`] with the same semantics the
//! scan uses (`Query::matches` over `scan_after`), driven by postings instead of a linear
//! decode. The differential test pins the two together: they must return the identical
//! ascending position set for every query. This evaluator is deliberately simple (no cost
//! model, no planning); the planner and its streaming API live in the read paths.
//!
//! Output is an **ascending, deduped** iterator of global positions strictly after
//! `after`. The ascending-per-segment contract is load-bearing for cross-segment
//! composition: a query spans
//! several per-segment indexes, and because the segments are position-disjoint and
//! ordered, the cross-segment combine is ordered concatenation of disjoint ascending runs,
//! not a k-way merge (see [`IndexSet::search_all`](super::IndexSet)).

use std::borrow::Cow;

use crate::Position;
use crate::query::{Query, QueryItem};

use super::SegmentIndex;

/// Positions of events matching `query`, ascending, deduped, strictly after `after`.
///
/// Generic over [`SegmentIndex`] so the identical evaluator serves the in-memory active tail
/// (through an [`ActiveView`](super::ActiveView)) and the on-disk
/// [`IndexSegment`](super::IndexSegment): there is one definition of the query semantics,
/// pinned to the spec by the tests below and differential-tested against the scan oracle.
///
/// Empty `Query::Items` matches nothing (OR over zero items); `Query::All` matches every
/// event. Within an item it is AND over tags and "one of" over types, an empty tag list
/// constraining only on type and an empty type list matching any type, exactly as
/// `Query::matches` and the DCB spec define.
pub fn search<'a, I: SegmentIndex>(
    index: &'a I,
    query: &Query,
    after: Position,
) -> impl Iterator<Item = Position> + 'a {
    let base = index.base().get();
    local_matches(index, query)
        .into_iter()
        .map(move |local| Position::new(base + local as u64))
        .filter(move |global| *global > after)
}

/// Positions of events matching `query`, **descending**, deduped, strictly before `before`.
///
/// The reverse counterpart to [`search`]: the identical match set, but yielded high-to-low and
/// bounded above rather than below. `before` is the exclusive **upper** bound (the dual of
/// `search`'s exclusive lower `after`), so `search_back(index, query, Position::MAX)` walks the
/// whole segment newest-first. Used by the backwards read path; the descending-per-segment
/// contract composes across segments by concatenating them high-to-low (the mirror of
/// [`search`]'s ascending concatenation).
pub fn search_back<'a, I: SegmentIndex>(
    index: &'a I,
    query: &Query,
    before: Position,
) -> impl Iterator<Item = Position> + 'a {
    let base = index.base().get();
    // `local_matches` is ascending; reversing it makes the globals descending.
    local_matches(index, query)
        .into_iter()
        .rev()
        .map(move |local| Position::new(base + local as u64))
        .filter(move |global| *global < before)
}

/// The ascending, deduped local positions matching `query` in this segment. The single
/// definition of the query semantics, shared by [`search`] and [`search_back`] so the forward
/// and backward evaluators can never disagree on *which* events match.
fn local_matches<I: SegmentIndex>(index: &I, query: &Query) -> Vec<u32> {
    match query {
        Query::All => (0..index.len()).collect(),
        Query::Items(items) => {
            // Union across items, then sort + dedup so the output is ascending with no
            // duplicate where items overlap. Per-item local sets are already ascending;
            // the query-level merge is materialized here (small), and the cross-segment
            // combine in `IndexSet::search_all` is ordered concatenation over disjoint
            // segments, so nothing re-sorts at the segment boundary.
            let mut locals = Vec::new();
            for item in items {
                item_locals(index, item, &mut locals);
            }
            locals.sort_unstable();
            locals.dedup();
            locals
        }
    }
}

/// Appends the local positions matching `item` (ascending) to `out`.
fn item_locals<I: SegmentIndex>(index: &I, item: &QueryItem, out: &mut Vec<u32>) {
    // Resolve the type constraint up front. `None` means no type filter (any type). If the
    // item lists types but none are indexed, it matches nothing.
    let type_ids: Option<Vec<u16>> = if item.types.is_empty() {
        None
    } else {
        let ids: Vec<u16> = item
            .types
            .iter()
            .filter_map(|t| index.type_id(t.as_str()))
            .collect();
        if ids.is_empty() {
            return;
        }
        Some(ids)
    };
    let keep = |local: u32| match &type_ids {
        None => true,
        Some(ids) => ids.contains(&index.type_at(local)),
    };

    if item.tags.is_empty() {
        // No tag constraint: walk the dense type column directly and keep matches, rather
        // than materializing a full-length candidate vector just to filter it. This is the
        // type-only path the column exists to make cheap.
        out.extend((0..index.len()).filter(|&local| keep(local)));
    } else {
        // Tag AND: intersect the posting lists of every required tag (bounded by the
        // smallest list), then apply the type filter.
        let mut lists: Vec<Cow<'_, [u32]>> = Vec::with_capacity(item.tags.len());
        for tag in item.tags.iter() {
            match index.term_postings(tag.as_str()) {
                Some(list) => lists.push(list),
                // A required tag no indexed event carries: the item matches nothing.
                None => return,
            }
        }
        out.extend(intersect(lists).into_iter().filter(|&local| keep(local)));
    }
}

/// Intersects ascending posting lists, preserving ascending order. `lists` is non-empty.
///
/// Walks the shortest list and keeps only elements present in all others (binary search,
/// since each list is ascending), so the cost is the smallest list times a log factor.
/// Takes [`Cow`] because the on-disk segment yields owned decoded lists while the tail
/// index borrows its slices; both are read the same way through the deref.
fn intersect(mut lists: Vec<Cow<'_, [u32]>>) -> Vec<u32> {
    lists.sort_by_key(|list| list.len());
    let (shortest, rest) = lists.split_first().expect("intersect called with no lists");
    shortest
        .iter()
        .copied()
        .filter(|x| rest.iter().all(|list| list.binary_search(x).is_ok()))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::{Event, EventType, Tag, Tags};
    use crate::index::ActiveTail;
    use crate::query::QueryItem;
    use smallvec::SmallVec;

    fn ty(s: &str) -> EventType {
        EventType::new(s).unwrap()
    }

    fn tags(items: &[&str]) -> Tags {
        Tags::new(
            items
                .iter()
                .map(|s| Tag::new(*s).unwrap())
                .collect::<SmallVec<[Tag; 4]>>(),
        )
        .unwrap()
    }

    fn event(type_str: &str, tag_strs: &[&str]) -> Event {
        Event::new(&ty(type_str), &tags(tag_strs), b"").unwrap()
    }

    /// The five-event fixture, positions 1..=5 (base 1). Chosen so the tricky cases below
    /// have hand-derived answers straight from the DCB spec, not from either implementation:
    ///
    /// | pos | type       | tags                       |
    /// |-----|------------|----------------------------|
    /// |  1  | Registered | (none)                     |
    /// |  2  | Enrolled   | course:c1                  |
    /// |  3  | Enrolled   | course:c1, student:s1      |
    /// |  4  | Renamed    | student:s1                 |
    /// |  5  | Registered | course:c1                  |
    fn fixture() -> ActiveTail {
        let events = [
            event("Registered", &[]),
            event("Enrolled", &["course:c1"]),
            event("Enrolled", &["course:c1", "student:s1"]),
            event("Renamed", &["student:s1"]),
            event("Registered", &["course:c1"]),
        ];
        let index = ActiveTail::new(Position::new(1));
        for (i, ev) in events.iter().enumerate() {
            index
                .push(Position::new(1 + i as u64), ev.as_ref())
                .unwrap();
        }
        index
    }

    fn run(index: &ActiveTail, query: &Query, after: u64) -> Vec<u64> {
        search(&index.view_full(), query, Position::new(after))
            .map(|p| p.get())
            .collect()
    }

    fn run_back(index: &ActiveTail, query: &Query, before: u64) -> Vec<u64> {
        search_back(&index.view_full(), query, Position::new(before))
            .map(|p| p.get())
            .collect()
    }

    // --- spec-anchored expectations (answers derived from the DCB spec, not the code) ---

    #[test]
    fn spec_empty_types_matches_any_type() {
        // A tag-only item matches every type carrying that tag: 2, 3, 5.
        let index = fixture();
        let q = Query::item(QueryItem::with_tags(tags(&["course:c1"])));
        assert_eq!(run(&index, &q, 0), vec![2, 3, 5]);
    }

    #[test]
    fn spec_empty_tags_constrains_only_on_type() {
        // A type-only item matches every event of that type regardless of tags: 1, 5.
        let index = fixture();
        let q = Query::item(QueryItem::of_types(vec![ty("Registered")]));
        assert_eq!(run(&index, &q, 0), vec![1, 5]);
    }

    #[test]
    fn spec_empty_item_matches_everything() {
        // No type and no tag constraint: every event.
        let index = fixture();
        let q = Query::item(QueryItem::default());
        assert_eq!(run(&index, &q, 0), vec![1, 2, 3, 4, 5]);
    }

    #[test]
    fn spec_empty_items_matches_nothing() {
        // OR over zero alternatives.
        let index = fixture();
        let q = Query::items(Vec::new());
        assert_eq!(run(&index, &q, 0), Vec::<u64>::new());
    }

    #[test]
    fn spec_all_matches_everything_and_after_is_whole_log() {
        let index = fixture();
        assert_eq!(run(&index, &Query::all(), 0), vec![1, 2, 3, 4, 5]);
    }

    // --- behavior ---

    #[test]
    fn and_within_item_tags() {
        // Both tags required: only position 3 carries course:c1 AND student:s1.
        let index = fixture();
        let q = Query::item(QueryItem::with_tags(tags(&["course:c1", "student:s1"])));
        assert_eq!(run(&index, &q, 0), vec![3]);
    }

    #[test]
    fn or_across_items() {
        // Renamed (4) OR anything with course:c1 (2, 3, 5), deduped and ascending.
        let index = fixture();
        let q = Query::items(vec![
            QueryItem::of_types(vec![ty("Renamed")]),
            QueryItem::with_tags(tags(&["course:c1"])),
        ]);
        assert_eq!(run(&index, &q, 0), vec![2, 3, 4, 5]);
    }

    #[test]
    fn type_and_tag_together() {
        // Enrolled AND course:c1: positions 2 and 3 (5 is Registered, 4 is Renamed).
        let index = fixture();
        let q = Query::item(QueryItem::new(vec![ty("Enrolled")], tags(&["course:c1"])));
        assert_eq!(run(&index, &q, 0), vec![2, 3]);
    }

    #[test]
    fn absent_tag_matches_nothing() {
        let index = fixture();
        let q = Query::item(QueryItem::with_tags(tags(&["ghost:x"])));
        assert_eq!(run(&index, &q, 0), Vec::<u64>::new());
    }

    #[test]
    fn absent_type_matches_nothing() {
        let index = fixture();
        let q = Query::item(QueryItem::of_types(vec![ty("Ghost")]));
        assert_eq!(run(&index, &q, 0), Vec::<u64>::new());
    }

    #[test]
    fn partly_absent_types_keep_the_known_ones() {
        // Registered exists (1, 5), Ghost does not: the item is Registered's events.
        let index = fixture();
        let q = Query::item(QueryItem::of_types(vec![ty("Registered"), ty("Ghost")]));
        assert_eq!(run(&index, &q, 0), vec![1, 5]);
    }

    #[test]
    fn after_is_exclusive() {
        let index = fixture();
        let q = Query::item(QueryItem::with_tags(tags(&["course:c1"])));
        // course:c1 is at 2, 3, 5. after excludes at-or-before.
        assert_eq!(run(&index, &q, 2), vec![3, 5]);
        assert_eq!(run(&index, &q, 3), vec![5]);
        assert_eq!(run(&index, &q, 5), Vec::<u64>::new());
        // All with after mid-log.
        assert_eq!(run(&index, &Query::all(), 2), vec![3, 4, 5]);
    }

    #[test]
    fn output_is_ascending_and_deduped_across_overlapping_items() {
        // Two items both matching position 3 must yield it once.
        let index = fixture();
        let q = Query::items(vec![
            QueryItem::with_tags(tags(&["course:c1"])),  // 2, 3, 5
            QueryItem::with_tags(tags(&["student:s1"])), // 3, 4
        ]);
        assert_eq!(run(&index, &q, 0), vec![2, 3, 4, 5]);
    }

    // --- backwards search ---

    /// The reverse evaluator returns exactly the forward match set, descending. Checked across
    /// every query shape above so `search_back` can never disagree with `search` on *which*
    /// events match, only on order and bound.
    #[test]
    fn search_back_is_search_reversed() {
        let index = fixture();
        let queries = [
            Query::all(),
            Query::item(QueryItem::default()),
            Query::item(QueryItem::with_tags(tags(&["course:c1"]))),
            Query::item(QueryItem::with_tags(tags(&["course:c1", "student:s1"]))),
            Query::item(QueryItem::of_types(vec![ty("Registered")])),
            Query::item(QueryItem::new(vec![ty("Enrolled")], tags(&["course:c1"]))),
            Query::items(vec![
                QueryItem::of_types(vec![ty("Renamed")]),
                QueryItem::with_tags(tags(&["course:c1"])),
            ]),
            Query::items(Vec::new()),
        ];
        for q in &queries {
            let mut forward = run(&index, q, 0);
            forward.reverse();
            assert_eq!(run_back(&index, q, u64::MAX), forward, "query {q:?}");
        }
    }

    #[test]
    fn before_is_exclusive_upper_bound() {
        let index = fixture();
        let q = Query::item(QueryItem::with_tags(tags(&["course:c1"]))); // at 2, 3, 5
        // `before` excludes at-or-after, descending.
        assert_eq!(run_back(&index, &q, u64::MAX), vec![5, 3, 2]);
        assert_eq!(run_back(&index, &q, 5), vec![3, 2]);
        assert_eq!(run_back(&index, &q, 3), vec![2]);
        assert_eq!(run_back(&index, &q, 2), Vec::<u64>::new());
        // All, bounded above mid-log.
        assert_eq!(run_back(&index, &Query::all(), 4), vec![3, 2, 1]);
    }
}
