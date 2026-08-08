//! Layer 4: the query planner's cost model.
//!
//! The read path answers a selective query through the index (plan the matching positions,
//! then fetch each event by a random read) and a broad query by a single sequential log
//! scan. The index wins only when the result is a small fraction of the range: one random
//! fetch per matching event beats a sequential scan at disk bandwidth only while there are
//! few of them (CLAUDE.md 8). This module is the estimator that decides which.
//!
//! It is pure and cheap: [`estimate_matches`] bounds the result size from **exact posting
//! lengths** ([`SegmentIndex::term_len`], free from the term dictionary, no statistics and
//! no estimation error), and [`choose`] compares that against the range width. The estimate
//! is only ever an **upper bound**, so a wrong estimate can only pick a slower correct path,
//! never a wrong answer: both execution modes return the identical positions the scan oracle
//! does.
//!
//! Granularity: 6c makes one **whole-query** decision (aggregated across the touched
//! segments in [`crate::read`]). The ROADMAP's per-*item* planner (index the narrow items,
//! scan the broad ones, in one read) is the future refinement; the per-item estimates
//! [`estimate_item`] exposes, logged by the read path, are the data that says whether it
//! would pay.

use crate::query::{Query, QueryItem};

use super::SegmentIndex;

/// Which execution mode the planner picks for a read.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Access {
    /// Plan the matching positions from the index, fetch each event on demand. Cheapest
    /// when the result is a small fraction of the range.
    Index,
    /// Sequentially scan the range once and filter. Cheapest when the result is a large
    /// fraction of the range (and the path `Query::all` always takes).
    Scan,
}

/// An upper bound on the positions in one segment matching `query`, within `width` (that
/// segment's post-pruning range width, the count of positions the query could touch there).
///
/// Per item the estimate is the tightest exact bound available (the shortest posting list
/// for an AND of tags); across items it is their sum (the union is at most the sum), capped
/// at `width`. It never underestimates, so [`choose`] only ever errs toward scanning.
pub fn estimate_matches<I: SegmentIndex>(index: &I, query: &Query, width: u64) -> u64 {
    match query {
        // Every position matches: maximally broad, always routes to a scan.
        Query::All => width,
        Query::Items(items) => {
            let mut total: u64 = 0;
            for item in items {
                total = total.saturating_add(estimate_item(index, item, width));
                if total >= width {
                    return width; // already at least as broad as a full scan
                }
            }
            total.min(width)
        }
    }
}

/// An upper bound on the positions in one segment matching a single `item`, within `width`.
///
/// - **Tags present** (AND): the intersection cannot exceed the shortest posting list, so
///   the bound is `min(term_len(tag))`. A tag no event carries makes the item empty (`0`).
///   The type filter can only shrink the result further, so this stays an upper bound.
/// - **No tags** (type-only, or the empty item that matches everything): `width`. The type
///   column walk is `O(width)` and there is no stored per-type count, so it is treated as
///   broad; this is what routes type-only queries and `Query::all` to the scan.
pub fn estimate_item<I: SegmentIndex>(index: &I, item: &QueryItem, width: u64) -> u64 {
    if item.tags.is_empty() {
        return width;
    }
    let mut smallest = u64::MAX;
    for tag in item.tags.iter() {
        match index.term_len(tag.as_str()) {
            None => return 0,
            Some(len) => smallest = smallest.min(u64::from(len)),
        }
    }
    smallest.min(width)
}

/// The index-vs-scan verdict: [`Access::Index`] iff the range is at least `scan_bias` times
/// the estimated result count, else [`Access::Scan`].
///
/// `scan_bias` is the planner's `K`: it biases toward scanning at the margin (larger scans
/// more; `1` means "index whenever the estimate does not exceed the range"). The
/// multiplication saturates, so a huge `scan_bias` cleanly forces a scan for any non-empty
/// estimate.
pub fn choose(estimate: u64, width: u64, scan_bias: u32) -> Access {
    if estimate.saturating_mul(u64::from(scan_bias)) <= width {
        Access::Index
    } else {
        Access::Scan
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Position;
    use crate::event::{Event, EventType, Tag, Tags};
    use crate::index::ActiveTail;
    use smallvec::SmallVec;

    fn ty(s: &str) -> EventType {
        EventType::new(s).unwrap()
    }

    fn tags(items: &[&str]) -> Tags {
        Tags::new(
            items
                .iter()
                .map(|s| Tag::new(s).unwrap())
                .collect::<SmallVec<[Tag; 4]>>(),
        )
        .unwrap()
    }

    fn event(type_str: &str, tag_strs: &[&str]) -> Event {
        Event::new(&ty(type_str), &tags(tag_strs), b"").unwrap()
    }

    /// Positions 1..=5 (base 1), matching search.rs's fixture:
    ///
    /// | pos | type       | tags                  |
    /// |-----|------------|-----------------------|
    /// |  1  | Registered | (none)                |
    /// |  2  | Enrolled   | course:c1             |
    /// |  3  | Enrolled   | course:c1, student:s1 |
    /// |  4  | Renamed    | student:s1            |
    /// |  5  | Registered | course:c1             |
    ///
    /// So `course:c1` has 3 postings, `student:s1` has 2, and their AND has 1 (position 3).
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

    const WIDTH: u64 = 5;

    fn estimate(query: &Query) -> u64 {
        estimate_matches(&fixture().view_full(), query, WIDTH)
    }

    #[test]
    fn single_tag_estimates_its_posting_length() {
        let q = Query::item(QueryItem::with_tags(tags(&["course:c1"])));
        assert_eq!(estimate(&q), 3);
        let q = Query::item(QueryItem::with_tags(tags(&["student:s1"])));
        assert_eq!(estimate(&q), 2);
    }

    #[test]
    fn tag_and_estimates_the_shortest_list() {
        // The intersection cannot exceed the shorter of the two lists (2), even though the
        // true answer is 1. An upper bound is all the planner needs.
        let q = Query::item(QueryItem::with_tags(tags(&["course:c1", "student:s1"])));
        assert_eq!(estimate(&q), 2);
    }

    #[test]
    fn absent_tag_makes_the_item_empty() {
        let q = Query::item(QueryItem::with_tags(tags(&["ghost:x"])));
        assert_eq!(estimate(&q), 0);
        // Absent tag AND a present one: still empty, the item cannot match.
        let q = Query::item(QueryItem::with_tags(tags(&["course:c1", "ghost:x"])));
        assert_eq!(estimate(&q), 0);
    }

    #[test]
    fn type_only_and_empty_item_are_broad() {
        // No stored per-type count, so a type-only item estimates the whole width.
        let q = Query::item(QueryItem::of_types(vec![ty("Registered")]));
        assert_eq!(estimate(&q), WIDTH);
        // The empty item matches everything: also width.
        let q = Query::item(QueryItem::default());
        assert_eq!(estimate(&q), WIDTH);
    }

    #[test]
    fn all_is_full_width_and_empty_items_is_zero() {
        assert_eq!(estimate(&Query::all()), WIDTH);
        assert_eq!(estimate(&Query::items(Vec::new())), 0);
    }

    #[test]
    fn or_sums_items_and_caps_at_width() {
        // course:c1 (3) OR student:s1 (2) sums to 5, capped at width 5 (the true union is 4).
        let q = Query::items(vec![
            QueryItem::with_tags(tags(&["course:c1"])),
            QueryItem::with_tags(tags(&["student:s1"])),
        ]);
        assert_eq!(estimate(&q), WIDTH);
        // A single narrow item stays narrow.
        let q = Query::items(vec![QueryItem::with_tags(tags(&["student:s1"]))]);
        assert_eq!(estimate(&q), 2);
    }

    #[test]
    fn choose_biases_toward_scanning_at_the_margin() {
        // scan_bias 1: index whenever the estimate does not exceed the range.
        assert_eq!(choose(3, 5, 1), Access::Index);
        assert_eq!(choose(5, 5, 1), Access::Index);
        assert_eq!(choose(6, 5, 1), Access::Scan);
        // The exact boundary at estimate * K == width is Index; one past it is Scan.
        assert_eq!(choose(2, 8, 4), Access::Index); // 2 * 4 == 8
        assert_eq!(choose(3, 8, 4), Access::Scan); // 3 * 4 == 12 > 8
        // A huge bias forces a scan for any non-empty estimate over a realistic range, but an
        // empty estimate (0 * K == 0 <= width) still indexes (cheapest for a no-match query).
        assert_eq!(choose(1, 5, u32::MAX), Access::Scan);
        assert_eq!(choose(0, 5, u32::MAX), Access::Index);
    }
}
