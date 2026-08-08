//! The caller-facing handle.

use flume::{self as channel, Sender};

use crate::Position;
use crate::event::Event;
use crate::index::IndexError;
use crate::log::set::PositionRange;
use crate::query::{AppendCondition, Query};

use super::{AppendError, Message, Request};

/// A cloneable, `Send` handle to the write coordinator. Every clone feeds the same
/// single writer; dropping the last one (and the owning [`WriteCoordinator`]) shuts the
/// coordinator down.
#[derive(Clone)]
pub struct WriteHandle {
    pub(super) tx: Sender<Message>,
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

    /// Runs `query` against the index, returning the matching positions ascending and
    /// strictly after `after`.
    ///
    /// Serviced on the writer thread in submission order, so a query issued after an
    /// `append` on the same handle sees that append (read-your-writes). Errs with
    /// [`IndexError::Unindexable`] if the query touches a segment that could not be
    /// indexed; the caller should then scan the log for that range. A shutdown coordinator
    /// surfaces as an [`IndexError::Io`]-free channel drop, mapped to
    /// [`SearchError::Shutdown`].
    pub fn search(&self, query: Query, after: Position) -> Result<Vec<Position>, SearchError> {
        let (reply, response) = channel::unbounded();
        self.tx
            .send(Message::Search {
                query,
                after,
                reply,
            })
            .map_err(|_| SearchError::Shutdown)?;
        response
            .recv()
            .map_err(|_| SearchError::Shutdown)?
            .map_err(SearchError::Index)
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

    /// The `async` counterpart of [`search`](Self::search): identical semantics, but it
    /// yields to the executor instead of blocking the thread while the request queue is
    /// full and while awaiting the reply. Read-your-writes still holds against an
    /// `append_async` awaited earlier on the same handle.
    #[cfg(feature = "async")]
    pub async fn search_async(
        &self,
        query: Query,
        after: Position,
    ) -> Result<Vec<Position>, SearchError> {
        let (reply, response) = channel::unbounded();
        self.tx
            .send_async(Message::Search {
                query,
                after,
                reply,
            })
            .await
            .map_err(|_| SearchError::Shutdown)?;
        response
            .recv_async()
            .await
            .map_err(|_| SearchError::Shutdown)?
            .map_err(SearchError::Index)
    }
}

/// Why an index query did not return positions.
#[derive(Debug, thiserror::Error)]
pub enum SearchError {
    /// The index could not answer the query (an unindexable range, or an I/O or rebuild
    /// failure). See [`IndexError`].
    #[error(transparent)]
    Index(IndexError),
    /// The write coordinator has shut down and will not service the query.
    #[error("write coordinator has shut down")]
    Shutdown,
}
