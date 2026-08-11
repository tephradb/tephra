//! Query model.
//!
//! Pure data, no I/O and no match logic (the match predicate lives in the engine, where
//! it decodes an event). A [`Query`] describes which events a decision depends on, and the
//! same query goes into the [`AppendCondition`] that guards the append. That reuse is what
//! makes the consistency boundary dynamic: it covers exactly the events the decision read,
//! nothing more.
//!
//! # Semantics
//!
//! A query is a set of [`QueryItem`]s OR'd together, plus a separate [`Query::All`]
//! variant that matches everything. Within an item:
//!
//! - the event **type** must match *one* of the listed types (an empty type list
//!   matches *any* type), and
//! - the event **tags** must contain *all* of the item's tags.
//!
//! So the shape is **OR across items, AND within an item's tags**. An item with no
//! tags constrains only on type; an empty [`Query::Items`] set matches nothing (an OR
//! over zero alternatives).

use crate::name::{EventType, Tags};
use crate::position::Position;

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
}

/// A query: a set of [`QueryItem`]s OR'd together, or the catch-all [`Query::All`].
///
/// [`All`](Query::All) is a distinct variant rather than "an item with no
/// constraints" so the read and condition paths can recognise a full scan and bypass
/// the index entirely. An empty [`Items`](Query::Items) set is the opposite: it matches
/// nothing.
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
}

/// The guard on an `append` call.
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
/// match predicate over the durable suffix) is the engine's job.
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

    #[test]
    fn item_constructors() {
        assert!(QueryItem::of_types(Vec::new()).tags.is_empty());
        assert!(QueryItem::with_tags(Tags::empty()).types.is_empty());
        assert_eq!(
            QueryItem::default(),
            QueryItem::new(Vec::new(), Tags::empty())
        );
    }

    #[test]
    fn query_constructors() {
        assert_eq!(Query::all(), Query::All);
        assert_eq!(Query::items(Vec::new()), Query::Items(Vec::new()));
        assert_eq!(
            Query::item(QueryItem::default()),
            Query::Items(vec![QueryItem::default()])
        );
    }

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
        let cond = AppendCondition::new(Query::all()).after(Position::new(42));
        assert_eq!(cond.after, Position::new(42));
    }
}
