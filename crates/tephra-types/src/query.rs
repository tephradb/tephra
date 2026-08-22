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

/// The guard on an `append` call: two independent checks, OR'd, so the append is rejected
/// if either fires.
///
/// The **boundary check** ignores everything at or before [`after`](Self::after) and rejects
/// the append if anything matching [`fail_if_events_match`](Self::fail_if_events_match) landed
/// since. `after` is the highest position the client observed while building its decision
/// model, which may be higher than the last matching event's position. Positions are 1-based,
/// so [`after`](Self::after) `= Position::ZERO` (the default) means "consider the whole log":
/// the spec's "omit `after`" case, i.e. fail if *any* event matches (the uniqueness-guard
/// pattern).
///
/// The optional **existence check** ([`fail_if_exists`](Self::fail_if_exists)) rejects the
/// append if any event *anywhere* matches its query, independent of `after` (an implicit
/// `after = 0`). It is the idempotency/dedupe primitive: assert a key is globally absent even
/// when the boundary legitimately advanced past events the decision read, which a single
/// `after` cannot express. A conflict from this clause is reported distinctly from a boundary
/// conflict (the engine's `ConflictClause`), so a client can treat "already applied"
/// differently from "boundary moved, rebuild and retry".
///
/// This type only *holds* the condition. Evaluating it (position filtering plus the match
/// predicate over the durable suffix) is the engine's job.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AppendCondition {
    /// Boundary check: reject the append if any event after [`after`](Self::after) matches
    /// this query.
    pub fail_if_events_match: Query,
    /// Ignore everything at or before this position for the boundary check. `Position::ZERO`
    /// means the whole log.
    pub after: Position,
    /// Optional existence check: reject the append if any event anywhere (implicit
    /// `after = 0`) matches this query. `None` disables it.
    pub fail_if_exists: Option<Query>,
}

impl AppendCondition {
    /// A condition checking the whole log (`after = Position::ZERO`): fail if any event
    /// matches.
    pub fn new(fail_if_events_match: Query) -> Self {
        AppendCondition {
            fail_if_events_match,
            after: Position::ZERO,
            fail_if_exists: None,
        }
    }

    /// A condition with no boundary check, only the existence clause: fail the append if any
    /// event anywhere matches `query`. The pure idempotency/dedupe guard, without a decision
    /// boundary. Equivalent to `AppendCondition::new(Query::items([])).fail_if_exists(query)`
    /// (an empty boundary matches nothing), without the empty-boundary boilerplate.
    pub fn exists_only(query: Query) -> Self {
        AppendCondition {
            fail_if_events_match: Query::items(Vec::new()),
            after: Position::ZERO,
            fail_if_exists: Some(query),
        }
    }

    /// Sets the exclusive lower bound for the boundary check: only events strictly after
    /// `after` are checked.
    pub fn after(mut self, after: Position) -> Self {
        self.after = after;
        self
    }

    /// Adds the existence check: fail the append if any event anywhere (implicit `after = 0`)
    /// matches `query`, independent of the boundary. The idempotency/dedupe guard.
    pub fn fail_if_exists(mut self, query: Query) -> Self {
        self.fail_if_exists = Some(query);
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
        assert_eq!(cond.fail_if_exists, None);
    }

    #[test]
    fn condition_after_sets_bound() {
        let cond = AppendCondition::new(Query::all()).after(Position::new(42));
        assert_eq!(cond.after, Position::new(42));
    }

    #[test]
    fn condition_fail_if_exists_sets_the_clause() {
        let dedupe = Query::item(QueryItem::with_tags(Tags::empty()));
        let cond = AppendCondition::new(Query::all()).fail_if_exists(dedupe.clone());
        assert_eq!(cond.fail_if_exists, Some(dedupe));
        // Independent of the boundary bound.
        assert_eq!(cond.after, Position::ZERO);
    }

    #[test]
    fn exists_only_has_an_empty_boundary_and_the_existence_clause() {
        let dedupe = Query::item(QueryItem::with_tags(Tags::empty()));
        let cond = AppendCondition::exists_only(dedupe.clone());
        // Empty boundary matches nothing, so only the existence clause can fire.
        assert_eq!(cond.fail_if_events_match, Query::items(Vec::new()));
        assert_eq!(cond.after, Position::ZERO);
        assert_eq!(cond.fail_if_exists, Some(dedupe));
    }
}
