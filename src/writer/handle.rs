//! The caller-facing handle.

use flume::{self as channel, Sender};

use crate::Position;
use crate::event::Event;
use crate::log::set::PositionRange;
use crate::query::{AppendCondition, Query};
use crate::read::{ReadHandle, Reads};

use super::{AppendError, Message, Request};

/// A cloneable, `Send` handle to the write coordinator. Every clone feeds the same
/// single writer; dropping the last one (and the owning
/// [`WriteCoordinator`](super::WriteCoordinator)) shuts the coordinator down.
///
/// Also carries a [`ReadHandle`] so appends and reads share one handle, but the two are
/// independent: [`read`](Self::read) runs on the caller's thread over the published
/// snapshot and never touches the writer.
#[derive(Clone)]
pub struct WriteHandle {
    pub(super) tx: Sender<Message>,
    pub(super) reader: ReadHandle,
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
    /// Blocks if the request queue is full (backpressure). A
    /// [`ConflictSite::SameBatch`](super::ConflictSite::SameBatch)
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
        let (reply, response) = channel::unbounded();
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

    /// Reads events matching `query`, ascending, strictly after `after`, up to the
    /// watermark pinned now. Runs on the **caller's own thread** over the published read
    /// snapshot: it never touches the writer thread, and read-your-writes still holds (the
    /// writer publishes the watermark before replying to an append). See
    /// [`ReadHandle::read`] and [`Reads`].
    pub fn read(&self, query: Query, after: Position) -> Reads {
        self.reader.read(query, after)
    }

    /// A standalone [`ReadHandle`] for pure readers, sharing this handle's published read
    /// state without the ability to append.
    pub fn reader(&self) -> ReadHandle {
        self.reader.clone()
    }

    /// The `async` counterpart of [`append`](Self::append): identical semantics, but it
    /// yields to the executor instead of blocking the thread while the request queue is
    /// full (backpressure) and while awaiting the durable reply.
    #[cfg(feature = "async")]
    pub async fn append_async(
        &self,
        events: Vec<Event>,
        condition: Option<AppendCondition>,
    ) -> Result<PositionRange, AppendError> {
        if events.is_empty() {
            return Err(AppendError::Empty);
        }
        let (reply, response) = channel::unbounded();
        let request = Request {
            events,
            condition,
            reply,
        };
        // A full request queue awaits rather than blocks; an error means the coordinator
        // is gone (channel disconnected).
        self.tx
            .send_async(Message::Append(request))
            .await
            .map_err(|_| AppendError::Shutdown)?;
        // A dropped reply sender (coordinator died mid-flight) surfaces as shutdown.
        response
            .recv_async()
            .await
            .map_err(|_| AppendError::Shutdown)?
    }
}
