//! Translation between the wire protobuf types ([`tephra_proto`]) and the in-process tephra
//! types. The shared vocabulary conversions ([`Query`]/[`AppendCondition`]) are delegated to
//! [`tephra_proto::convert`]; this module adds the engine-specific pieces: building the packed
//! [`Event`] codec from the wire, projecting an [`EventRef`] back to the wire, and mapping the
//! engine's [`AppendError`] to a wire error response.

use std::{error, fmt};

use tephra::Position;
use tephra::event::{EncodeError, Event, EventRef, EventType};
use tephra::query::{AppendCondition, Query};
use tephra::writer::{AppendError, ConflictSite};

use tephra_proto::convert as wire;
use tephra_proto::tephra as pb;

/// Why a client's request could not be turned into valid tephra input. Every variant is a
/// client error (mapped to `BAD_REQUEST`), never an internal failure.
#[derive(Debug)]
pub enum ConvertError {
    /// A vocabulary field (query, tag, type, condition) was malformed.
    Wire(wire::ConvertError),
    /// The event could not be encoded into the packed codec (too many tags, or too large).
    Encode(EncodeError),
}

impl fmt::Display for ConvertError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConvertError::Wire(err) => write!(f, "{err}"),
            ConvertError::Encode(err) => write!(f, "invalid event: {err}"),
        }
    }
}

impl error::Error for ConvertError {}

impl From<wire::ConvertError> for ConvertError {
    fn from(err: wire::ConvertError) -> Self {
        ConvertError::Wire(err)
    }
}

/// Builds a tephra packed [`Event`] from a wire event view, validating type, tags, and size
/// through tephra's own constructors (no validation is re-implemented here).
pub fn event_from_proto(ev: pb::EventView<'_>) -> Result<Event, ConvertError> {
    let ty = EventType::new(wire::as_str(ev.r#type())?)
        .map_err(|err| ConvertError::Wire(wire::ConvertError::Name(err)))?;
    let tags = wire::tags_from_pb(ev.tags().iter())?;
    Event::new(&ty, &tags, ev.payload()).map_err(ConvertError::Encode)
}

/// Collects the events of an append request into tephra [`Event`]s.
pub fn events_from_proto(append: pb::AppendRequestView<'_>) -> Result<Vec<Event>, ConvertError> {
    let mut events = Vec::with_capacity(append.events().len());
    for ev in append.events().iter() {
        events.push(event_from_proto(ev)?);
    }
    Ok(events)
}

/// Builds a tephra [`Query`] from a wire query view.
pub fn query_from_proto(query: pb::QueryView<'_>) -> Result<Query, ConvertError> {
    Ok(wire::query_from_pb(query)?)
}

/// Builds an [`AppendCondition`] from a wire condition view.
pub fn condition_from_proto(
    condition: pb::AppendConditionView<'_>,
) -> Result<AppendCondition, ConvertError> {
    Ok(wire::condition_from_pb(condition)?)
}

/// Builds a wire [`SequencedEvent`](pb::SequencedEvent) from a tephra position and event view.
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
