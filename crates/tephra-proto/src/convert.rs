//! Conversions between the wire protobuf types and the shared `tephra-types` vocabulary.
//!
//! Both the client and the server map the same `Query`/`QueryItem`/`AppendCondition`
//! between the wire and the vocabulary, so that mapping lives here once. Name and tag
//! validation goes through the `tephra-types` constructors, so there is a single definition
//! of the rules. This module never depends on the storage engine: it converts to and from
//! the vocabulary types, not the engine's packed event codec (that stays engine-side).

use std::{error, fmt};

use tephra_types::{AppendCondition, EventType, NameError, Position, Query, QueryItem, Tag, Tags};

use crate::tephra as pb;

/// Why a wire message could not be turned into valid vocabulary input. Every variant is a
/// client error (the caller sent something malformed), never an internal failure.
#[derive(Debug)]
pub enum ConvertError {
    /// A string field was not valid UTF-8 (proto3 does not validate every string on parse).
    InvalidUtf8,
    /// An event type or tag was empty or too long.
    Name(NameError),
    /// A tag set contained a duplicate.
    Tags(tephra_types::TagsError),
}

impl fmt::Display for ConvertError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConvertError::InvalidUtf8 => write!(f, "string field is not valid utf-8"),
            ConvertError::Name(err) => write!(f, "invalid name: {err}"),
            ConvertError::Tags(err) => write!(f, "invalid tags: {err}"),
        }
    }
}

impl error::Error for ConvertError {}

/// Reads a protobuf string view as `&str`, rejecting invalid UTF-8.
pub fn as_str(view: &protobuf::ProtoStr) -> Result<&str, ConvertError> {
    view.to_str().map_err(|_| ConvertError::InvalidUtf8)
}

/// Collects a wire repeated-string field of tags into a validated [`Tags`] set.
pub fn tags_from_pb<'a>(
    tags: impl IntoIterator<Item = &'a protobuf::ProtoStr>,
) -> Result<Tags, ConvertError> {
    let mut out: Vec<Tag> = Vec::new();
    for tag in tags {
        out.push(Tag::new(as_str(tag)?).map_err(ConvertError::Name)?);
    }
    Tags::new(out).map_err(ConvertError::Tags)
}

/// Collects a wire repeated-string field of types into validated [`EventType`]s.
pub fn types_from_pb<'a>(
    types: impl IntoIterator<Item = &'a protobuf::ProtoStr>,
) -> Result<Vec<EventType>, ConvertError> {
    let mut out: Vec<EventType> = Vec::new();
    for ty in types {
        out.push(EventType::new(as_str(ty)?).map_err(ConvertError::Name)?);
    }
    Ok(out)
}

/// Builds a [`Query`] from a wire query view. `all` maps to [`Query::All`]; otherwise the
/// items are OR'd (an empty item set matches nothing, per the spec).
pub fn query_from_pb(query: pb::QueryView<'_>) -> Result<Query, ConvertError> {
    if query.all() {
        return Ok(Query::All);
    }
    let mut items = Vec::with_capacity(query.items().len());
    for item in query.items().iter() {
        let types = types_from_pb(item.types().iter())?;
        let tags = tags_from_pb(item.tags().iter())?;
        items.push(QueryItem::new(types, tags));
    }
    Ok(Query::items(items))
}

/// Builds a wire [`Query`](pb::Query) from a vocabulary [`Query`].
pub fn query_to_pb(query: &Query) -> pb::Query {
    let mut out = pb::Query::new();
    match query {
        Query::All => out.set_all(true),
        Query::Items(items) => {
            for item in items {
                let mut pb_item = pb::QueryItem::new();
                for ty in &item.types {
                    pb_item.types_mut().push(ty.as_str());
                }
                for tag in item.tags.iter() {
                    pb_item.tags_mut().push(tag.as_str());
                }
                out.items_mut().push(pb_item);
            }
        }
    }
    out
}

/// Builds an [`AppendCondition`] from a wire condition view.
pub fn condition_from_pb(
    condition: pb::AppendConditionView<'_>,
) -> Result<AppendCondition, ConvertError> {
    let query = query_from_pb(condition.fail_if_events_match())?;
    Ok(AppendCondition::new(query).after(Position::new(condition.after())))
}

/// Builds a wire [`AppendCondition`](pb::AppendCondition) from a vocabulary condition.
pub fn condition_to_pb(condition: &AppendCondition) -> pb::AppendCondition {
    let mut out = pb::AppendCondition::new();
    out.set_fail_if_events_match(query_to_pb(&condition.fail_if_events_match));
    out.set_after(condition.after.get());
    out
}

/// A protocol error code, decoupled from the generated [`pb::ErrorCode`] so callers do not
/// depend on the wire enum. [`ErrorCode::Unknown`] absorbs the unspecified value and any
/// future code this build does not recognise.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ErrorCode {
    Conflict,
    AfterBeyondTip,
    Empty,
    TooLarge,
    BadRequest,
    Internal,
    Shutdown,
    Unauthenticated,
    Unknown,
}

impl From<pb::ErrorCode> for ErrorCode {
    fn from(code: pb::ErrorCode) -> Self {
        match code {
            pb::ErrorCode::Conflict => ErrorCode::Conflict,
            pb::ErrorCode::AfterBeyondTip => ErrorCode::AfterBeyondTip,
            pb::ErrorCode::Empty => ErrorCode::Empty,
            pb::ErrorCode::TooLarge => ErrorCode::TooLarge,
            pb::ErrorCode::BadRequest => ErrorCode::BadRequest,
            pb::ErrorCode::Internal => ErrorCode::Internal,
            pb::ErrorCode::Shutdown => ErrorCode::Shutdown,
            pb::ErrorCode::Unauthenticated => ErrorCode::Unauthenticated,
            // The unspecified value and any code this build does not recognise (the wire
            // enum is an open i32) both decode to Unknown.
            _ => ErrorCode::Unknown,
        }
    }
}

impl ErrorCode {
    /// The wire encoding of this code.
    pub fn to_pb(self) -> pb::ErrorCode {
        match self {
            ErrorCode::Conflict => pb::ErrorCode::Conflict,
            ErrorCode::AfterBeyondTip => pb::ErrorCode::AfterBeyondTip,
            ErrorCode::Empty => pb::ErrorCode::Empty,
            ErrorCode::TooLarge => pb::ErrorCode::TooLarge,
            ErrorCode::BadRequest => pb::ErrorCode::BadRequest,
            ErrorCode::Internal => pb::ErrorCode::Internal,
            ErrorCode::Shutdown => pb::ErrorCode::Shutdown,
            ErrorCode::Unauthenticated => pb::ErrorCode::Unauthenticated,
            ErrorCode::Unknown => pb::ErrorCode::Unspecified,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn query_all_round_trips() {
        let pb = query_to_pb(&Query::All);
        assert!(pb.all());
        assert_eq!(query_from_pb(pb.as_view()).unwrap(), Query::All);
    }

    #[test]
    fn query_items_round_trip() {
        let query = Query::items(vec![
            QueryItem::of_types(vec![EventType::new("Registered").unwrap()]),
            QueryItem::new(
                vec![EventType::new("Enrolled").unwrap()],
                Tags::new(vec![
                    Tag::new("course:c1").unwrap(),
                    Tag::new("student:s1").unwrap(),
                ])
                .unwrap(),
            ),
        ]);
        let pb = query_to_pb(&query);
        assert_eq!(query_from_pb(pb.as_view()).unwrap(), query);
    }

    #[test]
    fn empty_items_query_round_trips_as_items_not_all() {
        // An empty items set matches nothing; it must not collapse to `all`.
        let pb = query_to_pb(&Query::items(Vec::new()));
        assert!(!pb.all());
        assert_eq!(
            query_from_pb(pb.as_view()).unwrap(),
            Query::items(Vec::new())
        );
    }

    #[test]
    fn condition_round_trips() {
        let cond = AppendCondition::new(Query::item(QueryItem::with_tags(
            Tags::new(vec![Tag::new("course:c1").unwrap()]).unwrap(),
        )))
        .after(Position::new(42));
        let pb = condition_to_pb(&cond);
        assert_eq!(condition_from_pb(pb.as_view()).unwrap(), cond);
    }

    #[test]
    fn empty_tag_is_rejected_through_core_validation() {
        // An empty tag is invalid, and the rejection comes from the core constructor, so
        // there is one definition of the rule.
        let mut item = pb::QueryItem::new();
        item.tags_mut().push("");
        let mut query = pb::Query::new();
        query.items_mut().push(item);
        assert!(matches!(
            query_from_pb(query.as_view()),
            Err(ConvertError::Name(NameError::Empty { .. }))
        ));
    }

    #[test]
    fn error_code_round_trips() {
        for code in [
            ErrorCode::Conflict,
            ErrorCode::AfterBeyondTip,
            ErrorCode::Empty,
            ErrorCode::TooLarge,
            ErrorCode::BadRequest,
            ErrorCode::Internal,
            ErrorCode::Shutdown,
            ErrorCode::Unauthenticated,
            ErrorCode::Unknown,
        ] {
            assert_eq!(ErrorCode::from(code.to_pb()), code);
        }
        // The unspecified wire value decodes to Unknown.
        assert_eq!(
            ErrorCode::from(pb::ErrorCode::Unspecified),
            ErrorCode::Unknown
        );
    }
}
