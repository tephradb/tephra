//! Query model and the match predicate.
//!
//! Pure logic, no I/O. A [`Query`] describes which events a decision depends on, and
//! the same query goes into the [`AppendCondition`] that guards the append. That reuse
//! is what makes the consistency boundary dynamic: it covers exactly the events the
//! decision read, nothing more.
//!
//! # Semantics
//!
//! A query is a set of [`QueryItem`]s OR'd together, plus a separate [`Query::all`]
//! variant that matches everything. Within an item:
//!
//! - the event **type** must match *one* of the listed types (an empty type list
//!   matches *any* type), and
//! - the event **tags** must contain *all* of the item's tags.
//!
//! So the shape is **OR across items, AND within an item's tags**. An item with no
//! tags constrains only on type; an empty [`Query::Items`] set matches nothing (an OR
//! over zero alternatives).
//!
//! # One predicate
//!
//! [`Query::matches`] is the single definition of "does this event match". The
//! condition evaluator (layer 2) and, later, the index (layer 3) must agree with it
//! exactly; the index is differential-tested against this predicate rather than
//! reimplementing the semantics. Tag containment is a linear merge over two sorted
//! sequences: the item's tags are sorted (a [`Tags`] invariant) and an event's tags
//! decode in sorted order, so neither side needs sorting at match time.

use crate::Position;
use crate::event::{EventRef, EventType, Tags};

/// One alternative in a [`Query`]: a type constraint AND a tag constraint.
///
/// An event matches the item when its type is one of [`types`](Self::types) (or the
/// list is empty, matching any type) and its tags are a superset of
/// [`tags`](Self::tags).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct QueryItem {
    /// The types this item accepts. Empty means "any type". Not sorted: the list is
    /// tiny (types are low cardinality) so membership is a linear scan.
    pub types: Vec<EventType>,
    /// Tags the event must contain *all* of. Sorted and duplicate-free by construction.
    pub tags: Tags,
}

impl QueryItem {
    /// An item constraining on both a set of types and a set of tags.
    pub fn new(types: Vec<EventType>, tags: Tags) -> Self {
        QueryItem { types, tags }
    }

    /// An item constraining only on type (matches any tags).
    pub fn of_types(types: Vec<EventType>) -> Self {
        QueryItem {
            types,
            tags: Tags::empty(),
        }
    }

    /// An item constraining only on tags (matches any type).
    pub fn with_tags(tags: Tags) -> Self {
        QueryItem {
            types: Vec::new(),
            tags,
        }
    }

    /// Whether `event` satisfies this item: type in the list (or the list is empty)
    /// AND every one of the item's tags is present on the event.
    pub fn matches(&self, event: EventRef<'_>) -> bool {
        let type_ok = self.types.is_empty()
            || self
                .types
                .iter()
                .any(|t| t.as_str() == event.event_type());
        type_ok && tags_contained(&self.tags, event)
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

/// A query: a set of [`QueryItem`]s OR'd together, or the catch-all [`Query::All`].
///
/// [`All`](Query::All) is a distinct variant rather than "an item with no
/// constraints" so the read and condition paths can recognise a full scan and bypass
/// the index entirely (see layer 4). An empty [`Items`](Query::Items) set is the
/// opposite: it matches nothing.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Query {
    /// Matches every event. Full scans and broad projection catch-up use this and skip
    /// the index.
    All,
    /// Matches an event if *any* contained item matches (logical OR). Empty matches
    /// nothing.
    Items(Vec<QueryItem>),
}

impl Query {
    /// The catch-all query, matching every event.
    pub fn all() -> Self {
        Query::All
    }

    /// A query over a set of items, OR'd together.
    pub fn items(items: impl Into<Vec<QueryItem>>) -> Self {
        Query::Items(items.into())
    }

    /// A query with a single item.
    pub fn item(item: QueryItem) -> Self {
        Query::Items(vec![item])
    }

    /// Whether `event` matches this query: always for [`All`](Query::All), otherwise
    /// true if any item matches.
    pub fn matches(&self, event: EventRef<'_>) -> bool {
        match self {
            Query::All => true,
            Query::Items(items) => items.iter().any(|item| item.matches(event)),
        }
    }
}

/// The guard on an [`append`](crate) call.
///
/// The store ignores everything at or before [`after`](Self::after) and rejects the
/// append if anything matching [`fail_if_events_match`](Self::fail_if_events_match)
/// landed since. `after` is the highest position the client observed while building
/// its decision model, which may be higher than the last matching event's position.
///
/// Positions are 1-based, so [`after`](Self::after) `= Position::ZERO` (the default)
/// means "consider the whole log": the spec's "omit `after`" case, i.e. fail if *any*
/// event matches (the uniqueness-guard pattern).
///
/// This type only *holds* the condition. Evaluating it (position filtering plus the
/// match predicate over the durable suffix) is the condition evaluator's job in
/// layer 2.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AppendCondition {
    /// Reject the append if any event after [`after`](Self::after) matches this query.
    pub fail_if_events_match: Query,
    /// Ignore everything at or before this position. `Position::ZERO` means the whole
    /// log.
    pub after: Position,
}

impl AppendCondition {
    /// A condition checking the whole log (`after = Position::ZERO`): fail if any event
    /// matches.
    pub fn new(fail_if_events_match: Query) -> Self {
        AppendCondition {
            fail_if_events_match,
            after: Position::ZERO,
        }
    }

    /// Sets the exclusive lower bound: only events strictly after `after` are checked.
    pub fn after(mut self, after: Position) -> Self {
        self.after = after;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::{Event, Tag};
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
        assert!(item.matches(
            event("Enrolled", &["course:c1", "extra:e", "student:s1"]).as_ref()
        ));
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

    // --- AppendCondition ---

    #[test]
    fn condition_defaults_to_position_zero() {
        // Omitting `after` means "check the whole log", which under 1-based positions
        // is `after: 0`.
        let cond = AppendCondition::new(Query::all());
        assert_eq!(cond.after, Position::ZERO);
        assert_eq!(cond.fail_if_events_match, Query::All);
    }

    #[test]
    fn condition_after_sets_bound() {
        let cond = AppendCondition::new(Query::item(QueryItem::with_tags(tags(&["course:c1"]))))
            .after(Position::new(42));
        assert_eq!(cond.after, Position::new(42));
    }
}
