//! Batch accumulation: staging accepted requests into one group-committed unit and
//! assigning their positions.
//!
//! A [`Batch`] borrows the encoded event bytes straight out of the drained requests (no
//! copy) and remembers, per request, the reply channel and the dense position range it
//! was assigned. On a durable commit it absorbs the staged tags into the main tips and
//! replies success; on failure it replies the same error to every staged request
//! (all-or-nothing: partial success is not representable).

use crate::Position;
use crate::event::Event;
use crate::log::set::{BATCH_OVERHEAD, PositionRange, RECORD_OVERHEAD};

use super::tips::{StagedTips, TagTips};
use super::{AppendError, Reply};

/// One pass over a request's events, returning `(total_framed_bytes, largest_framed_
/// event)`. The total (each record pays [`RECORD_OVERHEAD`]) drives batch budgeting; it
/// excludes the once-per-batch commit marker, which the coordinator accounts separately.
/// The largest single event drives the per-record limit check (an over-limit record
/// fails only its own solo batch, never a shared one). Computed together so the drain
/// hot path walks each request's events exactly once.
pub(super) fn measure(events: &[Event]) -> (usize, usize) {
    let mut total = 0;
    let mut largest = 0;
    for event in events {
        let framed = RECORD_OVERHEAD + event.as_bytes().len();
        total += framed;
        if framed > largest {
            largest = framed;
        }
    }
    (total, largest)
}

/// The fixed cost a batch pays once for its commit marker.
pub(super) const MARKER_BYTES: usize = BATCH_OVERHEAD;

struct Staged<'a> {
    reply: &'a Reply,
    range: PositionRange,
    token: u64,
}

/// An in-flight batch borrowing from the drained requests.
pub(super) struct Batch<'a> {
    records: Vec<&'a [u8]>,
    staged: Vec<Staged<'a>>,
    tips: StagedTips,
    first: Position,
    next: Position,
}

impl<'a> Batch<'a> {
    /// A batch that will assign positions starting at `next` (the log's next position).
    pub(super) fn new(next: Position) -> Self {
        Batch {
            records: Vec::new(),
            staged: Vec::new(),
            tips: StagedTips::new(),
            first: next,
            next,
        }
    }

    /// The staged tips so far, so the coordinator can evaluate a later request's
    /// condition against records already accepted in this drain window.
    pub(super) fn staged_tips(&self) -> &StagedTips {
        &self.tips
    }

    pub(super) fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    /// The encoded records to hand to `append_batch`.
    pub(super) fn records(&self) -> &[&'a [u8]] {
        &self.records
    }

    /// Each committed record paired with its assigned global position, in order. The
    /// coordinator feeds these into the index before replying (read-your-writes). One
    /// record is one event, so positions run densely from the batch's first position.
    pub(super) fn committed_records(&self) -> impl Iterator<Item = (Position, &'a [u8])> + '_ {
        let first = self.first.get();
        self.records
            .iter()
            .enumerate()
            .map(move |(i, &bytes)| (Position::new(first + i as u64), bytes))
    }

    /// Stages `events`, assigning them a dense position range, recording their tags into
    /// the staged tips, and remembering `reply` for the commit. The request must already
    /// have passed its condition check.
    pub(super) fn stage(&mut self, events: &'a [Event], token: u64, reply: &'a Reply) {
        let first = self.next;
        let mut p = first;
        for event in events {
            self.records.push(event.as_bytes());
            for tag in event.as_ref().tags() {
                self.tips.record(tag, p);
            }
            p = p.next();
        }
        let range = PositionRange {
            first,
            last: Position::new(p.get() - 1),
        };
        self.next = p;
        self.staged.push(Staged {
            reply,
            range,
            token,
        });
    }

    /// Called after `append_batch` succeeded. Absorbs the staged tags into `main` (now
    /// durable) and replies each staged request its assigned range.
    pub(super) fn commit_ok(
        self,
        committed: PositionRange,
        main: &mut TagTips,
        next_position: Position,
    ) {
        debug_assert_eq!(committed.first, self.first, "batch first position mismatch");
        debug_assert_eq!(
            committed.last,
            Position::new(self.next.get() - 1),
            "batch last position mismatch",
        );
        main.absorb(self.tips, next_position);
        for s in self.staged {
            // Crash point: some but not all clients in a durable batch have been acked. The
            // unacked ones are still durable, so they must be present after recovery too.
            crash_points::crash_point!("partial_ack");
            // A dropped receiver (caller gave up) is fine; the write is durable either way.
            let _ = s.reply.send((s.token, Ok(s.range)));
        }
    }

    /// Called after `append_batch` failed. Replies the same error to every staged
    /// request; nothing was committed and the staged tips are dropped.
    pub(super) fn commit_err(self, err: AppendError) {
        for s in self.staged {
            let _ = s.reply.send((s.token, Err(err.clone())));
        }
    }
}

#[cfg(test)]
mod tests {
    use flume::{self as channel, Receiver};

    use crate::event::{Event, EventType, Tags};
    use crate::writer::WriterConfig;

    use super::super::AppendError;
    use super::*;

    fn event(payload: &[u8]) -> Event {
        Event::new(&EventType::new("E").unwrap(), &Tags::empty(), payload).unwrap()
    }

    type ReplyRx = Receiver<(u64, Result<PositionRange, AppendError>)>;

    fn reply() -> (Reply, ReplyRx) {
        channel::unbounded()
    }

    #[test]
    fn commit_err_replies_every_staged_request() {
        // Two staged requests; a batch failure must reach both, each carrying its token.
        let e1 = vec![event(b"a")];
        let e2 = vec![event(b"b"), event(b"c")];
        let (tx1, rx1) = reply();
        let (tx2, rx2) = reply();

        let mut batch = Batch::new(Position::new(1));
        batch.stage(&e1, 10, &tx1);
        batch.stage(&e2, 20, &tx2);
        batch.commit_err(AppendError::TooLarge { size: 99 });

        assert!(matches!(
            rx1.try_recv(),
            Ok((10, Err(AppendError::TooLarge { size: 99 })))
        ));
        assert!(matches!(
            rx2.try_recv(),
            Ok((20, Err(AppendError::TooLarge { size: 99 })))
        ));
    }

    #[test]
    fn commit_ok_assigns_dense_ranges_per_request() {
        let e1 = vec![event(b"a"), event(b"b")]; // positions 1..2
        let e2 = vec![event(b"c")]; // position 3
        let (tx1, rx1) = reply();
        let (tx2, rx2) = reply();

        let mut batch = Batch::new(Position::new(1));
        batch.stage(&e1, 10, &tx1);
        batch.stage(&e2, 20, &tx2);

        let cfg = WriterConfig::default();
        let mut main = TagTips::new(Position::new(1), cfg.tips_window);
        batch.commit_ok(
            PositionRange {
                first: Position::new(1),
                last: Position::new(3),
            },
            &mut main,
            Position::new(4),
        );

        let (token1, res1) = rx1.try_recv().unwrap();
        assert_eq!(token1, 10);
        assert_eq!(
            res1.unwrap(),
            PositionRange {
                first: Position::new(1),
                last: Position::new(2)
            }
        );
        let (token2, res2) = rx2.try_recv().unwrap();
        assert_eq!(token2, 20);
        assert_eq!(
            res2.unwrap(),
            PositionRange {
                first: Position::new(3),
                last: Position::new(3)
            }
        );
    }
}
