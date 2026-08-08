//! Translation between the wire protobuf types ([`dcbdb_proto`]) and the in-process dcbdb
//! types. This is the only place that touches both sides, so the proto crate stays free of
//! any dcbdb dependency.

use std::{error, fmt};

use dcbdb::Position;
use dcbdb::event::{EncodeError, Event, EventRef, EventType, NameError, Tag, Tags, TagsError};
use dcbdb::query::{AppendCondition, Query, QueryItem};
use dcbdb::writer::{AppendError, ConflictSite};

use dcbdb_proto::dcbdb as pb;

/// Why a client's request could not be turned into valid dcbdb input. Every variant is a
/// client error (mapped to `BAD_REQUEST`), never an internal failure.
#[derive(Debug)]
pub enum ConvertError {
    /// A string field was not valid UTF-8 (possible since the kernel does not validate it
    /// on parse for every field).
    InvalidUtf8,
    /// An event type or tag was empty or too long.
    Name(NameError),
    /// A tag set contained a duplicate.
    Tags(TagsError),
    /// The event could not be encoded (too many tags, or too large).
    Encode(EncodeError),
}

impl fmt::Display for ConvertError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConvertError::InvalidUtf8 => write!(f, "string field is not valid utf-8"),
            ConvertError::Name(err) => write!(f, "invalid name: {err}"),
            ConvertError::Tags(err) => write!(f, "invalid tags: {err}"),
            ConvertError::Encode(err) => write!(f, "invalid event: {err}"),
        }
    }
}

impl error::Error for ConvertError {}

/// Reads a protobuf string view as `&str`, rejecting invalid UTF-8.
fn as_str(view: &protobuf::ProtoStr) -> Result<&str, ConvertError> {
    view.to_str().map_err(|_| ConvertError::InvalidUtf8)
}

/// Builds a dcbdb [`Event`] from a wire event view, validating type, tags, and size through
/// dcbdb's own constructors (no validation is re-implemented here).
pub fn event_from_proto(ev: pb::EventView<'_>) -> Result<Event, ConvertError> {
    let ty = EventType::new(as_str(ev.r#type())?).map_err(ConvertError::Name)?;
    let mut tags: Vec<Tag> = Vec::with_capacity(ev.tags().len());
    for tag in ev.tags().iter() {
        tags.push(Tag::new(as_str(tag)?).map_err(ConvertError::Name)?);
    }
    let tags = Tags::new(tags).map_err(ConvertError::Tags)?;
    Event::new(&ty, &tags, ev.payload()).map_err(ConvertError::Encode)
}

/// Collects the events of an append request into dcbdb [`Event`]s.
pub fn events_from_proto(append: pb::AppendRequestView<'_>) -> Result<Vec<Event>, ConvertError> {
    let mut events = Vec::with_capacity(append.events().len());
    for ev in append.events().iter() {
        events.push(event_from_proto(ev)?);
    }
    Ok(events)
}

/// Builds a dcbdb [`Query`] from a wire query view. `all` maps to [`Query::All`]; otherwise
/// the items are OR'd (an empty item set matches nothing, per the spec).
pub fn query_from_proto(query: pb::QueryView<'_>) -> Result<Query, ConvertError> {
    if query.all() {
        return Ok(Query::All);
    }
    let mut items = Vec::with_capacity(query.items().len());
    for item in query.items().iter() {
        let mut types: Vec<EventType> = Vec::with_capacity(item.types().len());
        for ty in item.types().iter() {
            types.push(EventType::new(as_str(ty)?).map_err(ConvertError::Name)?);
        }
        let mut tags: Vec<Tag> = Vec::with_capacity(item.tags().len());
        for tag in item.tags().iter() {
            tags.push(Tag::new(as_str(tag)?).map_err(ConvertError::Name)?);
        }
        let tags = Tags::new(tags).map_err(ConvertError::Tags)?;
        items.push(QueryItem::new(types, tags));
    }
    Ok(Query::items(items))
}

/// Builds an [`AppendCondition`] from a wire condition view.
pub fn condition_from_proto(
    condition: pb::AppendConditionView<'_>,
) -> Result<AppendCondition, ConvertError> {
    let query = query_from_proto(condition.fail_if_events_match())?;
    Ok(AppendCondition::new(query).after(Position::new(condition.after())))
}

/// Builds a wire [`SequencedEvent`](pb::SequencedEvent) from a dcbdb position and event view.
pub fn sequenced_to_proto(position: Position, event: EventRef<'_>) -> pb::SequencedEvent {
    let mut out = pb::SequencedEvent::new();
    out.set_position(position.get());
    let mut ev = out.event_mut();
    ev.set_type(event.event_type());
    for tag in event.tags() {
        ev.tags_mut().push(tag);
    }
    ev.set_payload(event.data());
    out
}

/// Maps an [`AppendError`] to a wire [`ErrorResponse`](pb::ErrorResponse), preserving the
/// durable-vs-retryable conflict distinction the coordinator draws.
pub fn append_error_to_proto(err: &AppendError) -> pb::ErrorResponse {
    let mut resp = pb::ErrorResponse::new();
    resp.set_message(err.to_string());
    match err {
        AppendError::Conflict { at } => {
            resp.set_code(pb::ErrorCode::Conflict);
            match at {
                ConflictSite::Durable(position) => {
                    resp.set_conflict_position(position.get());
                    resp.set_retryable(false);
                }
                ConflictSite::SameBatch => {
                    resp.set_retryable(true);
                }
            }
        }
        AppendError::AfterBeyondTip { .. } => resp.set_code(pb::ErrorCode::AfterBeyondTip),
        AppendError::Empty => resp.set_code(pb::ErrorCode::Empty),
        AppendError::TooLarge { .. } => resp.set_code(pb::ErrorCode::TooLarge),
        AppendError::Log(_) | AppendError::Corrupt(_) => resp.set_code(pb::ErrorCode::Internal),
        AppendError::Shutdown => resp.set_code(pb::ErrorCode::Shutdown),
    }
    resp
}

/// Builds a `BAD_REQUEST` error response for a malformed request.
pub fn bad_request(message: impl fmt::Display) -> pb::ErrorResponse {
    let mut resp = pb::ErrorResponse::new();
    resp.set_code(pb::ErrorCode::BadRequest);
    resp.set_message(message.to_string());
    resp
}

/// Builds a `TOO_LARGE` error response for an oversized inbound frame.
pub fn too_large(message: impl fmt::Display) -> pb::ErrorResponse {
    let mut resp = pb::ErrorResponse::new();
    resp.set_code(pb::ErrorCode::TooLarge);
    resp.set_message(message.to_string());
    resp
}

/// Builds an `INTERNAL` error response for a read failure mid-stream.
pub fn internal_error(message: impl fmt::Display) -> pb::ErrorResponse {
    let mut resp = pb::ErrorResponse::new();
    resp.set_code(pb::ErrorCode::Internal);
    resp.set_message(message.to_string());
    resp
}
