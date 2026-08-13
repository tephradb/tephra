//! Query match predicate over an encoded event.
//!
//! The [`Query`] data model ([`Query`], [`QueryItem`], [`AppendCondition`]) lives in
//! `tephra-types` and is re-exported here. This module owns the single definition of "does
//! this event match", evaluated over the zero-copy [`EventRef`].
//!
//! # One predicate
//!
//! [`Matches::matches`] is the single definition of "does this event match". The
//! condition evaluator (layer 2) and the index (layer 3) must agree with it exactly; the
//! index is differential-tested against this predicate rather than reimplementing the
//! semantics. Tag containment is a linear merge over two sorted sequences: the item's tags
//! are sorted (a [`Tags`] invariant) and an event's tags decode in sorted order, so
//! neither side needs sorting at match time.

use crate::event::{EventRef, Tags};

pub use tephra_types::{AppendCondition, Query, QueryItem};

/// The match predicate over an encoded event.
///
/// This is an extension trait because [`Query`] and [`QueryItem`] are foreign types (they
/// live in `tephra-types`, so a client can build queries without the engine), while the
/// predicate needs the engine's zero-copy [`EventRef`]. Keeping it here keeps "one
/// predicate, one definition" true while letting the data types be shared.
pub trait Matches {
    /// Whether `event` satisfies this query or item.
    fn matches(&self, event: EventRef<'_>) -> bool;
}

impl Matches for QueryItem {
    /// Type in the list (or the list is empty) AND every one of the item's tags present.
    fn matches(&self, event: EventRef<'_>) -> bool {
        let type_ok =
            self.types.is_empty() || self.types.iter().any(|t| t.as_str() == event.event_type());
        type_ok && tags_contained(&self.tags, event)
    }
}

impl Matches for Query {
    /// Always for [`Query::All`], otherwise true if any item matches (logical OR).
    fn matches(&self, event: EventRef<'_>) -> bool {
        match self {
            Query::All => true,
            Query::Items(items) => items.iter().any(|item| item.matches(event)),
        }
    }
}

/// Returns true if every tag in `required` (sorted, from a [`Tags`]) is present on
/// `event`, whose tags decode in sorted order. A linear merge over the two sorted
/// sequences: O(required + event tags), no allocation, no per-tag lookup.
fn tags_contained(required: &Tags, event: EventRef<'_>) -> bool {
    let mut event_tags = event.tags();
    let mut current = event_tags.next();
    for req in required.iter() {
        let req = req.as_str();
        loop {
            match current {
                // Event tag precedes the required one: it cannot help, skip it.
                Some(t) if t < req => current = event_tags.next(),
                // Found it. Advance past it; the next required tag is strictly greater.
                Some(t) if t == req => {
                    current = event_tags.next();
                    break;
                }
                // Event tag overshot the required one, or the tags ran out: the
                // required tag is absent, so the event fails the AND.
                _ => return false,
            }
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::{Event, EventType, Tag};

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

    /// Builds an owned event; call `.as_ref()` on the returned value to match against
    /// it (the `EventRef` borrows the returned `Event`, so it must outlive the match).
    fn event(type_str: &str, tag_strs: &[&str]) -> Event {
        Event::new(&ty(type_str), &tags(tag_strs), b"").unwrap()
    }

    // --- QueryItem: type constraint ---

    #[test]
    fn item_matches_one_of_listed_types() {
        let item = QueryItem::of_types(vec![ty("Registered"), ty("Deregistered")]);
        assert!(item.matches(event("Registered", &[]).as_ref()));
        assert!(item.matches(event("Deregistered", &[]).as_ref()));
        assert!(!item.matches(event("Renamed", &[]).as_ref()));
    }

    #[test]
    fn empty_types_matches_any_type() {
        // An item with no types constrains only on tags.
        let item = QueryItem::with_tags(tags(&["course:c1"]));
        assert!(item.matches(event("Anything", &["course:c1"]).as_ref()));
        assert!(item.matches(event("Whatever", &["course:c1"]).as_ref()));
        // The tag constraint still applies.
        assert!(!item.matches(event("Anything", &["course:c2"]).as_ref()));
    }

    #[test]
    fn empty_item_matches_everything() {
        // No types (any type) and no tags (any tags): matches every event.
        let item = QueryItem::default();
        assert!(item.matches(event("A", &[]).as_ref()));
        assert!(item.matches(event("B", &["x:1", "y:2"]).as_ref()));
    }

    // --- QueryItem: AND within tags ---

    #[test]
    fn item_requires_all_tags_present() {
        let item = QueryItem::new(vec![ty("Enrolled")], tags(&["course:c1", "student:s1"]));
        // Superset matches.
        assert!(item.matches(event("Enrolled", &["course:c1", "student:s1"]).as_ref()));
        assert!(item.matches(event("Enrolled", &["course:c1", "extra:e", "student:s1"]).as_ref()));
        // Missing one of the required tags fails.
        assert!(!item.matches(event("Enrolled", &["course:c1"]).as_ref()));
        assert!(!item.matches(event("Enrolled", &["student:s1"]).as_ref()));
        // Right tags but wrong type fails.
        assert!(!item.matches(event("Other", &["course:c1", "student:s1"]).as_ref()));
    }

    #[test]
    fn tag_containment_boundary_cases() {
        let none = QueryItem::with_tags(Tags::empty());
        // No required tags: any event's tags satisfy it.
        assert!(none.matches(event("T", &[]).as_ref()));
        assert!(none.matches(event("T", &["a:1"]).as_ref()));

        // Required tag smaller than every event tag (merge overshoots immediately).
        let low = QueryItem::with_tags(tags(&["a:0"]));
        assert!(!low.matches(event("T", &["b:1", "c:2"]).as_ref()));

        // Required tag larger than every event tag (merge exhausts the event tags).
        let high = QueryItem::with_tags(tags(&["z:9"]));
        assert!(!high.matches(event("T", &["a:1", "b:2"]).as_ref()));

        // Required tag against an event with no tags.
        assert!(!high.matches(event("T", &[]).as_ref()));
    }

    // --- Query: OR across items ---

    #[test]
    fn query_ors_across_items() {
        let q = Query::items(vec![
            QueryItem::of_types(vec![ty("Registered")]),
            QueryItem::with_tags(tags(&["course:c1"])),
        ]);
        // First item matches (by type).
        assert!(q.matches(event("Registered", &[]).as_ref()));
        // Second item matches (by tag).
        assert!(q.matches(event("Enrolled", &["course:c1"]).as_ref()));
        // Both match.
        assert!(q.matches(event("Registered", &["course:c1"]).as_ref()));
        // Neither matches.
        assert!(!q.matches(event("Enrolled", &["course:c2"]).as_ref()));
    }

    #[test]
    fn empty_items_query_matches_nothing() {
        let q = Query::items(Vec::new());
        assert!(!q.matches(event("Anything", &["x:1"]).as_ref()));
    }

    #[test]
    fn all_query_matches_everything() {
        let q = Query::all();
        assert!(q.matches(event("A", &[]).as_ref()));
        assert!(q.matches(event("B", &["x:1", "y:2"]).as_ref()));
        assert_eq!(q, Query::All);
    }

    #[test]
    fn single_item_query_helper() {
        let q = Query::item(QueryItem::of_types(vec![ty("Ping")]));
        assert!(q.matches(event("Ping", &[]).as_ref()));
        assert!(!q.matches(event("Pong", &[]).as_ref()));
    }
}
