//! Subscriptions: catch-up from a position, then live tailing off the published watermark,
//! with no gap and no duplicate at the boundary.
//!
//! The whole design is one idea: **catch-up and live-tail are the same operation repeated.**
//! A [`Subscription`] holds a `cursor` (the last-delivered position, an exclusive lower
//! bound). Each round it reads `(cursor, watermark]` through the ordinary
//! [`Reads`](super::Reads) path and advances the cursor to the pinned watermark; between
//! rounds it blocks on [`ReadCore`](super::ReadCore)'s condvar until the watermark advances.
//! Because every read is `after`-exclusive and the cursor advances to the *pinned* watermark,
//! no event is ever skipped (gap) or re-delivered (duplicate). There is no separate handoff
//! step to get wrong, which is where subscriptions usually break.
//!
//! Two layers of API:
//!
//! - [`poll_batch`](Subscription::poll_batch) (non-blocking) plus
//!   [`wait`](Subscription::wait) / [`wait_timeout`](Subscription::wait_timeout): the
//!   primitive the server drives, so it can interleave the wait with its own shutdown checks.
//! - [`next_batch`](Subscription::next_batch): the ergonomic blocking form for in-process
//!   consumers and tests.

use std::sync::Arc;
use std::time::Duration;

use crate::Position;
use crate::event::Event;
use crate::query::Query;

use super::{ReadConfig, ReadCore, ReadError, Reads, WaitOutcome};

/// Default cap on the number of owned events one [`poll_batch`](Subscription::poll_batch)
/// returns, bounding memory during a large catch-up. Tunable per subscription via
/// [`with_max_batch_events`](Subscription::with_max_batch_events).
pub const DEFAULT_MAX_BATCH_EVENTS: usize = 1024;

/// A live subscription over a [`Query`], resuming strictly after a start position.
///
/// Constructed by [`ReadHandle::subscribe`](super::ReadHandle::subscribe). Owns a `cursor`
/// that only ever moves forward, so the subscription is a single long-lived object; it is not
/// meant to be cloned or resumed from a stale cursor.
pub struct Subscription {
    core: Arc<ReadCore>,
    config: ReadConfig,
    query: Query,
    /// Exclusive lower bound: everything at or before `cursor` has been delivered (or scanned
    /// and found non-matching). The next read covers `(cursor, watermark]`.
    cursor: Position,
    max_batch_events: usize,
}

impl Subscription {
    pub(super) fn new(
        core: Arc<ReadCore>,
        config: ReadConfig,
        query: Query,
        after: Position,
    ) -> Subscription {
        // Register before the first watermark read (the `poll_batch`/`wait` below). This
        // ordering is load-bearing for the wakeup gate; see `ReadCore::wake`.
        core.register_subscriber();
        Subscription {
            core,
            config,
            query,
            cursor: after,
            max_batch_events: DEFAULT_MAX_BATCH_EVENTS,
        }
    }

    /// Overrides the per-batch event cap (default [`DEFAULT_MAX_BATCH_EVENTS`]).
    pub fn with_max_batch_events(mut self, max_batch_events: usize) -> Subscription {
        self.max_batch_events = max_batch_events.max(1);
        self
    }

    /// The current resume position: the exclusive lower bound of the next read. Everything at
    /// or before it has been delivered. This is the value to persist for a durable subscriber
    /// that wants to resume across restarts.
    pub fn position(&self) -> Position {
        self.cursor
    }

    /// Reads the matching events available **now** in `(cursor, watermark]`, ascending, as an
    /// owned batch bounded by the event cap. Does not block. An empty result means the
    /// subscription has reached the live edge (caught up); call [`wait`](Self::wait) before
    /// polling again.
    ///
    /// The cursor advances only on genuine exhaustion: when the underlying [`Reads`] yields
    /// `None` (it reached its pinned watermark) the cursor jumps to that watermark, past any
    /// non-matching tail, so a selective query never re-scans it. When the batch cap is hit
    /// instead, the cursor advances only to the last delivered position and the remainder
    /// surfaces on the next call. It is never inferred from the batch size.
    pub fn poll_batch(&mut self) -> Result<Vec<(Position, Event)>, ReadError> {
        let (watermark, snapshot) = self.core.load();
        if self.cursor >= watermark {
            return Ok(Vec::new());
        }

        // No `limit` is passed to the read: `None` from `reads.next()` therefore means the
        // pinned `(cursor, watermark]` range is genuinely drained, never an early stop. If
        // `Reads` ever gains an early-termination mode, gate the watermark advance below on an
        // explicit `Reads::is_exhausted()` instead of on observing `None`.
        //
        // `plan` borrows the query, so a poll that lands on the index or full-scan path (the
        // common cases, including every idle tick) allocates nothing; only a broad *filtered*
        // plan clones it, and only once for the resulting scan.
        let mut reads = Reads::plan(snapshot, &self.query, self.cursor, watermark, &self.config);
        let mut out = Vec::new();
        loop {
            match reads.next() {
                Some(item) => {
                    let seq = item?;
                    out.push((seq.position, seq.event.to_owned()));
                    if out.len() >= self.max_batch_events {
                        // Cap hit: more may remain in the pinned range. Advance only to the
                        // last delivered position.
                        self.cursor = out.last().unwrap().0;
                        return Ok(out);
                    }
                }
                None => {
                    // Genuine exhaustion of the pinned range.
                    self.cursor = watermark;
                    return Ok(out);
                }
            }
        }
    }

    /// Blocks until the watermark advances past the cursor, returning `true`, or the store
    /// shuts down, returning `false`. No timeout: use [`wait_timeout`](Self::wait_timeout)
    /// when the caller must also observe an external signal (for example a server shutting
    /// down while no events flow).
    pub fn wait(&self) -> bool {
        matches!(
            self.core.wait_past(self.cursor, None),
            WaitOutcome::Advanced
        )
    }

    /// Like [`wait`](Self::wait) but bounded, so the caller regains control on `TimedOut` to
    /// check its own state (the server uses this to notice shutdown on an idle subscription).
    pub fn wait_timeout(&self, timeout: Duration) -> WaitOutcome {
        self.core.wait_past(self.cursor, Some(timeout))
    }

    /// Blocks until at least one matching event is available after the cursor, then returns it
    /// as an owned ascending batch (bounded by the event cap). Returns `None` when the store
    /// has shut down. Watermark advances that matched nothing are skipped internally (the
    /// cursor still advances, so they are never re-scanned).
    ///
    /// The ergonomic blocking form of [`poll_batch`](Self::poll_batch) + [`wait`](Self::wait),
    /// for in-process consumers that do not need to interleave their own shutdown checks.
    pub fn next_batch(&mut self) -> Option<Result<Vec<(Position, Event)>, ReadError>> {
        loop {
            match self.poll_batch() {
                Ok(batch) if !batch.is_empty() => return Some(Ok(batch)),
                Ok(_) => {
                    // Caught up to the live edge: block for the next advance.
                    if !self.wait() {
                        return None;
                    }
                }
                Err(err) => return Some(Err(err)),
            }
        }
    }
}

impl Drop for Subscription {
    fn drop(&mut self) {
        self.core.deregister_subscriber();
    }
}
