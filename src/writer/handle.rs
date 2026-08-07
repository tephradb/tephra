//! The caller-facing handle.

use std::sync::mpsc::{self, SyncSender};

use crate::event::Event;
use crate::log::set::PositionRange;
use crate::query::AppendCondition;

use super::{AppendError, Message, Request};

/// A cloneable, `Send` handle to the write coordinator. Every clone feeds the same
/// single writer; dropping the last one (and the owning [`WriteCoordinator`]) shuts the
/// coordinator down.
#[derive(Clone)]
pub struct WriteHandle {
    pub(super) tx: SyncSender<Message>,
}

impl WriteHandle {
    /// Appends `events` as one atomic unit, blocking until the batch is durable or the
    /// condition fails.
    ///
    /// `events` are already-encoded [`Event`]s (encoding and validation happen on the
    /// caller thread, off the writer). Pass `None` for an unconditional append, or a
    /// condition to guard the write against concurrent conflicting events. On success
    /// the returned [`PositionRange`] covers the assigned positions, dense and in order.
    ///
    /// Blocks if the request queue is full (backpressure). A [`ConflictSite::SameBatch`]
    /// conflict is retryable; see [`AppendError`] and
    /// [`ConflictSite`](super::ConflictSite).
    pub fn append(
        &self,
        events: Vec<Event>,
        condition: Option<AppendCondition>,
    ) -> Result<PositionRange, AppendError> {
        if events.is_empty() {
            return Err(AppendError::Empty);
        }
        let (reply, response) = mpsc::channel();
        let request = Request {
            events,
            condition,
            reply,
        };
        // `send` on a full bounded channel blocks; an error means the coordinator is
        // gone (channel disconnected).
        self.tx
            .send(Message::Append(request))
            .map_err(|_| AppendError::Shutdown)?;
        // A dropped reply sender (coordinator died mid-flight) surfaces as shutdown.
        response.recv().map_err(|_| AppendError::Shutdown)?
    }
}
