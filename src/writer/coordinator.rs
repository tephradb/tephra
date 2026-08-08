//! The writer thread: draining, condition evaluation, staging, and group commit.

use std::sync::Arc;
use std::sync::mpsc::{self, Receiver, SyncSender, TryRecvError};
use std::thread::{self, JoinHandle};

use crate::Position;
use crate::event::EventRef;
use crate::index::{IndexError, IndexSet};
use crate::log::set::{LogError, SegmentSet};
use crate::query::Query;

use super::batch::{Batch, MARKER_BYTES, measure};
use super::handle::WriteHandle;
use super::tips::TagTips;
use super::{AppendError, Message, Request, SearchReply, WriterConfig, condition};

/// Owns the writer thread. Holds the join handle so shutdown is deterministic and the
/// `SegmentSet` can be recovered for inspection or reopen.
pub struct WriteCoordinator {
    shutdown: SyncSender<Message>,
    join: Option<JoinHandle<SegmentSet>>,
}

impl WriteCoordinator {
    /// Spawns the writer thread, taking ownership of `set`. Returns the owner and a
    /// cloneable handle. Panics if `cfg` is inconsistent with `set` (a programming
    /// error, checked once at start).
    ///
    /// Opens the derived index for `set` first, rebuilding anything missing, corrupt, or
    /// never persisted from the log. This is the one fallible step: an I/O failure here is
    /// surfaced rather than run in a degraded, index-less mode.
    pub fn start(
        set: SegmentSet,
        cfg: WriterConfig,
    ) -> Result<(WriteCoordinator, WriteHandle), IndexError> {
        assert!(cfg.queue_capacity >= 1, "queue_capacity must be at least 1");
        assert!(
            cfg.max_batch_records >= 1,
            "max_batch_records must be at least 1"
        );
        assert!(
            cfg.max_batch_bytes <= set.segment_capacity(),
            "max_batch_bytes ({}) must not exceed segment capacity ({})",
            cfg.max_batch_bytes,
            set.segment_capacity(),
        );

        let index = IndexSet::open(&set)?;
        let (tx, rx) = mpsc::sync_channel::<Message>(cfg.queue_capacity);
        let tips = TagTips::new(set.next_position(), cfg.tips_window);
        let worker = Worker {
            set,
            index,
            tips,
            cfg,
            rx,
            pushback: None,
            shutdown: false,
        };
        let shutdown = tx.clone();
        let join = thread::Builder::new()
            .name("dcbdb-writer".to_string())
            .spawn(move || worker.run())
            .expect("spawn writer thread");
        Ok((
            WriteCoordinator {
                shutdown,
                join: Some(join),
            },
            WriteHandle { tx },
        ))
    }

    /// Signals shutdown, joins the writer thread, and returns the `SegmentSet`. Requests
    /// already queued ahead of the signal are serviced first; any queued after are
    /// answered with [`AppendError::Shutdown`].
    pub fn shutdown(mut self) -> SegmentSet {
        let _ = self.shutdown.send(Message::Shutdown);
        self.join
            .take()
            .expect("join handle present until shutdown")
            .join()
            .expect("writer thread panicked")
    }
}

impl Drop for WriteCoordinator {
    fn drop(&mut self) {
        if let Some(join) = self.join.take() {
            // The loop keeps draining, so this send cannot deadlock even if the queue is
            // momentarily full.
            let _ = self.shutdown.send(Message::Shutdown);
            let _ = join.join();
        }
    }
}

/// The state owned by the writer thread.
struct Worker {
    set: SegmentSet,
    /// The derived index, fed inline at the commit seam and queried on this thread. Owned
    /// here (not shared) in phase 5b: it is `!Sync`, so it cannot escape to a reader
    /// thread until phase 6 gives the active tail a published watermark.
    index: IndexSet,
    tips: TagTips,
    cfg: WriterConfig,
    rx: Receiver<Message>,
    /// A request received during a drain but deferred to the next one (over budget, or
    /// an oversize request that must start its own batch). `try_recv` hands over a
    /// request before it can be measured, so a one-slot buffer is required.
    pushback: Option<Request>,
    shutdown: bool,
}

impl Worker {
    fn run(mut self) -> SegmentSet {
        while let Some(reqs) = self.collect() {
            self.process(&reqs);
            if self.shutdown {
                break;
            }
        }
        self.set
    }

    /// Blocks for at least one request, then drains more up to the record/byte budget.
    /// Returns `None` only when the coordinator should exit (channel closed or an
    /// explicit shutdown arrived with nothing to process first).
    fn collect(&mut self) -> Option<Vec<Request>> {
        let first = match self.pushback.take() {
            Some(request) => request,
            None => loop {
                match self.rx.recv() {
                    Ok(Message::Append(request)) => break request,
                    // A query serviced between batches: answer it inline and keep waiting
                    // for the first append of the next batch.
                    Ok(Message::Search {
                        query,
                        after,
                        reply,
                    }) => self.answer_search(&query, after, reply),
                    Ok(Message::Shutdown) => {
                        self.shutdown = true;
                        return None;
                    }
                    Err(_) => {
                        self.shutdown = true;
                        return None;
                    }
                }
            },
        };

        let (first_sum, first_max) = measure(&first.events);
        let mut bytes = MARKER_BYTES + first_sum;
        let first_is_solo = self.is_oversized(first_sum, first_max);
        let mut reqs = vec![first];
        // An oversize first request takes a solo batch; `append_batch` validates the
        // exact capacity and any failure hits only this one request.
        if first_is_solo {
            return Some(reqs);
        }

        while reqs.len() < self.cfg.max_batch_records {
            match self.rx.try_recv() {
                Ok(Message::Append(request)) => {
                    let (sum, max) = measure(&request.events);
                    if self.is_oversized(sum, max) || bytes + sum > self.cfg.max_batch_bytes {
                        // Defer: an oversize request needs its own batch, and a request
                        // that would overflow the budget starts the next one.
                        self.pushback = Some(request);
                        break;
                    }
                    bytes += sum;
                    reqs.push(request);
                }
                // A query mid-drain: answer it against the index as it stands (the
                // in-flight batch is not yet durable, so it is correctly not visible) and
                // keep draining.
                Ok(Message::Search {
                    query,
                    after,
                    reply,
                }) => self.answer_search(&query, after, reply),
                Ok(Message::Shutdown) => {
                    self.shutdown = true;
                    break;
                }
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => {
                    self.shutdown = true;
                    break;
                }
            }
        }
        Some(reqs)
    }

    /// A request that cannot share a batch: its records plus the marker exceed the batch
    /// byte budget, or one of its events exceeds the per-record limit. Takes the measured
    /// sizes so the drain loop walks each request's events only once.
    fn is_oversized(&self, record_bytes: usize, largest_event: usize) -> bool {
        MARKER_BYTES + record_bytes > self.cfg.max_batch_bytes
            || largest_event > self.set.max_record_len()
    }

    /// Evaluates each request's condition, stages the accepted ones, and group-commits
    /// them under one fsync. Rejected requests are replied to immediately and never
    /// staged.
    fn process(&mut self, reqs: &[Request]) {
        let tip = self.set.last_position();
        let verify = self.cfg.verify_tips;
        let mut batch = Batch::new(self.set.next_position());

        for req in reqs {
            if let Some(cond) = &req.condition {
                // The invariant that makes staged records unskippable by the `after`
                // filter: a client cannot have observed a position past the durable tip.
                if cond.after > tip {
                    let _ = req.reply.send(Err(AppendError::AfterBeyondTip {
                        after: cond.after,
                        tip,
                    }));
                    continue;
                }
                match condition::evaluate(cond, &self.tips, batch.staged_tips(), &self.set, verify)
                {
                    Ok(Some(at)) => {
                        let _ = req.reply.send(Err(AppendError::Conflict { at }));
                        continue;
                    }
                    Ok(None) => {}
                    Err(err) => {
                        let _ = req.reply.send(Err(err));
                        continue;
                    }
                }
            }
            batch.stage(&req.events, &req.reply);
        }

        if batch.is_empty() {
            return;
        }

        // A rollover, if one happens, occurs inside `append_batch` before the records
        // land, so it grows `sealed_len`; capture the count first to detect it.
        let sealed_before = self.set.sealed_len();
        match self.set.append_batch(batch.records()) {
            Ok(range) => {
                // Feed the index before replying, so a caller that reads right after its
                // append sees its own write (read-your-writes). The write is already
                // durable here, so nothing below can turn it into a failure.
                if self.set.sealed_len() > sealed_before {
                    // The batch rolled to a new segment: seal the tail that covers the
                    // just-completed segment, then start the new one at this batch's base.
                    self.index.seal_active(range.first);
                }
                for (position, bytes) in batch.committed_records() {
                    // These bytes were validated when the caller encoded the event and
                    // again by `append_batch`, so a decode failure here is an integrity
                    // bug, not a normal outcome.
                    let event = EventRef::from_bytes(bytes)
                        .expect("committed record bytes decode; validated on append");
                    self.index.push(position, event);
                }
                let next = self.set.next_position();
                batch.commit_ok(range, &mut self.tips, next);
            }
            Err(err) => batch.commit_err(classify(err)),
        }
    }

    /// Answers an index query on the writer thread and replies. A dropped receiver (the
    /// caller gave up) is fine.
    fn answer_search(&self, query: &Query, after: Position, reply: SearchReply) {
        let result = self
            .index
            .search_all(query, after)
            .map(|positions| positions.collect());
        let _ = reply.send(result);
    }
}

/// Maps a log write failure to the caller-facing error. The size limits can only trip on
/// a solo oversize batch (a shared batch is budgeted to fit), so they name exactly one
/// request; everything else is an I/O-class failure of the whole batch.
fn classify(err: LogError) -> AppendError {
    match err {
        LogError::BatchTooLarge { size, .. } | LogError::RecordTooLarge { size, .. } => {
            AppendError::TooLarge { size }
        }
        other => AppendError::Log(Arc::new(other)),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::mpsc::Receiver;

    use smallvec::SmallVec;
    use tempfile::TempDir;

    use crate::Position;
    use crate::event::{Event, EventType, Tag, Tags};
    use crate::log::set::{PositionRange, SegmentConfig};
    use crate::query::{AppendCondition, Query, QueryItem};
    use crate::writer::ConflictSite;

    use super::*;

    type ReplyRx = Receiver<Result<PositionRange, AppendError>>;

    const SEG_SIZE: usize = 1 << 16;

    fn new_set(dir: &TempDir) -> SegmentSet {
        SegmentSet::open(dir.path(), SegmentConfig::new(SEG_SIZE)).unwrap()
    }

    fn cfg() -> WriterConfig {
        WriterConfig {
            queue_capacity: 64,
            max_batch_records: 64,
            max_batch_bytes: SEG_SIZE / 2,
            tips_window: 1_000_000,
            verify_tips: true,
        }
    }

    fn worker(dir: &TempDir, cfg: WriterConfig) -> (Worker, SyncSender<Message>) {
        let set = new_set(dir);
        let index = IndexSet::open(&set).unwrap();
        let (tx, rx) = mpsc::sync_channel(cfg.queue_capacity);
        let tips = TagTips::new(set.next_position(), cfg.tips_window);
        (
            Worker {
                set,
                index,
                tips,
                cfg,
                rx,
                pushback: None,
                shutdown: false,
            },
            tx,
        )
    }

    fn tags(items: &[&str]) -> Tags {
        Tags::new(
            items
                .iter()
                .map(|s| Tag::new(s).unwrap())
                .collect::<SmallVec<[Tag; 4]>>(),
        )
        .unwrap()
    }

    fn event(ty: &str, tag_strs: &[&str]) -> Event {
        Event::new(&EventType::new(ty).unwrap(), &tags(tag_strs), b"payload").unwrap()
    }

    /// A request whose events carry the given (type, tags), optionally guarded.
    fn request(
        specs: &[(&str, &[&str])],
        condition: Option<AppendCondition>,
    ) -> (Request, ReplyRx) {
        let events = specs.iter().map(|(ty, t)| event(ty, t)).collect();
        let (reply, rx) = mpsc::channel();
        (
            Request {
                events,
                condition,
                reply,
            },
            rx,
        )
    }

    /// A uniqueness guard: fail if any event with all `tag_strs` exists after `after`.
    fn guard(tag_strs: &[&str], after: Position) -> AppendCondition {
        AppendCondition::new(Query::item(QueryItem::with_tags(tags(tag_strs)))).after(after)
    }

    fn assert_ok(rx: &ReplyRx) -> PositionRange {
        match rx.try_recv() {
            Ok(Ok(range)) => range,
            other => panic!("expected Ok(range), got {other:?}"),
        }
    }

    fn assert_err(rx: &ReplyRx) -> AppendError {
        match rx.try_recv() {
            Ok(Err(err)) => err,
            other => panic!("expected Err, got {other:?}"),
        }
    }

    // --- intra-batch conflict (the correctness core) ---

    #[test]
    fn same_batch_uniqueness_conflict() {
        let dir = TempDir::new().unwrap();
        let (mut w, _tx) = worker(&dir, cfg());

        // Two decisions in one drain window, both guarding uniqueness of unique:x.
        let (r1, rx1) = request(
            &[("Reserved", &["unique:x"])],
            Some(guard(&["unique:x"], Position::ZERO)),
        );
        let (r2, rx2) = request(
            &[("Reserved", &["unique:x"])],
            Some(guard(&["unique:x"], Position::ZERO)),
        );
        w.process(&[r1, r2]);

        // Exactly one wins; the loser is a retryable same-batch conflict.
        assert_eq!(
            assert_ok(&rx1),
            PositionRange {
                first: Position::new(1),
                last: Position::new(1)
            }
        );
        assert!(matches!(
            assert_err(&rx2),
            AppendError::Conflict {
                at: ConflictSite::SameBatch
            }
        ));
        // Only the winner's event is durable.
        assert_eq!(w.set.last_position(), Position::new(1));
    }

    #[test]
    fn same_batch_distinct_tags_both_win() {
        let dir = TempDir::new().unwrap();
        let (mut w, _tx) = worker(&dir, cfg());
        let (r1, rx1) = request(
            &[("Reserved", &["unique:a"])],
            Some(guard(&["unique:a"], Position::ZERO)),
        );
        let (r2, rx2) = request(
            &[("Reserved", &["unique:b"])],
            Some(guard(&["unique:b"], Position::ZERO)),
        );
        w.process(&[r1, r2]);
        assert_ok(&rx1);
        assert_ok(&rx2);
        assert_eq!(w.set.last_position(), Position::new(2));
    }

    // --- durable conflict ---

    #[test]
    fn durable_conflict_after_commit() {
        let dir = TempDir::new().unwrap();
        let (mut w, _tx) = worker(&dir, cfg());

        // First batch commits an event with unique:x at position 1.
        let (r1, rx1) = request(&[("Reserved", &["unique:x"])], None);
        w.process(&[r1]);
        assert_ok(&rx1);

        // A later guarded append sees the durable event and loses.
        let (r2, rx2) = request(
            &[("Reserved", &["unique:x"])],
            Some(guard(&["unique:x"], Position::ZERO)),
        );
        w.process(&[r2]);
        assert!(matches!(
            assert_err(&rx2),
            AppendError::Conflict {
                at: ConflictSite::Durable(p)
            } if p == Position::new(1)
        ));
    }

    #[test]
    fn no_conflict_when_after_excludes_the_match() {
        let dir = TempDir::new().unwrap();
        let (mut w, _tx) = worker(&dir, cfg());
        let (r1, rx1) = request(&[("Reserved", &["unique:x"])], None);
        w.process(&[r1]);
        let first = assert_ok(&rx1).first; // position 1

        // Guard with after = 1 ignores the event at position 1, so no conflict.
        let (r2, rx2) = request(
            &[("Reserved", &["unique:x"])],
            Some(guard(&["unique:x"], first)),
        );
        w.process(&[r2]);
        assert_ok(&rx2);
    }

    // --- after == tip boundary with a staged record at tip + 1 ---

    #[test]
    fn after_equals_tip_with_staged_at_tip_plus_one() {
        let dir = TempDir::new().unwrap();
        let (mut w, _tx) = worker(&dir, cfg());

        // Grow the log to tip = 2.
        let (seed, rxs) = request(&[("Seed", &["k:1"]), ("Seed", &["k:2"])], None);
        w.process(&[seed]);
        assert_ok(&rxs);
        let tip = w.set.last_position();
        assert_eq!(tip, Position::new(2));

        // In one drain: r1 stages "u" at position tip+1 = 3; r2 guards "u" with after = tip.
        let (r1, rx1) = request(&[("Reserved", &["u"])], None);
        let (r2, rx2) = request(&[("Reserved", &["u"])], Some(guard(&["u"], tip)));
        w.process(&[r1, r2]);

        assert_eq!(
            assert_ok(&rx1),
            PositionRange {
                first: Position::new(3),
                last: Position::new(3)
            }
        );
        assert!(matches!(
            assert_err(&rx2),
            AppendError::Conflict {
                at: ConflictSite::SameBatch
            }
        ));
    }

    // --- input validation ---

    #[test]
    fn after_beyond_tip_is_rejected() {
        let dir = TempDir::new().unwrap();
        let (mut w, _tx) = worker(&dir, cfg());
        // Empty log, tip = 0; guard with after = 5 is beyond it.
        let (r, rx) = request(&[("X", &["a"])], Some(guard(&["a"], Position::new(5))));
        w.process(&[r]);
        assert!(matches!(
            assert_err(&rx),
            AppendError::AfterBeyondTip { after, tip }
                if after == Position::new(5) && tip == Position::ZERO
        ));
        // Nothing was appended.
        assert_eq!(w.set.last_position(), Position::ZERO);
    }

    #[test]
    fn verify_tips_definitely_no_match_path() {
        // Exercises the paranoid arm: a tag recorded low, queried with after above it,
        // yields DefinitelyNoMatch and the scan must agree (no panic).
        let dir = TempDir::new().unwrap();
        let (mut w, _tx) = worker(&dir, cfg());
        let (r1, rx1) = request(&[("A", &["u"]), ("B", &["v"]), ("C", &["w"])], None);
        w.process(&[r1]);
        assert_ok(&rx1); // positions 1..3, tip = 3

        // after = 2 (<= tip 3), guard on "u" whose only event is at position 1 <= after.
        let (r2, rx2) = request(&[("D", &["z"])], Some(guard(&["u"], Position::new(2))));
        w.process(&[r2]);
        assert_ok(&rx2); // DefinitelyNoMatch, scan agrees, append proceeds
    }

    // --- oversize routing and isolation ---

    #[test]
    fn oversize_request_replies_too_large_without_blocking_others() {
        let dir = TempDir::new().unwrap();
        let (mut w, _tx) = worker(&dir, cfg());

        // One event larger than a segment can hold: unappendable.
        let huge = Event::new(
            &EventType::new("Big").unwrap(),
            &Tags::empty(),
            &vec![0u8; SEG_SIZE],
        )
        .unwrap();
        let (reply, rx_big) = mpsc::channel();
        let big = Request {
            events: vec![huge],
            condition: None,
            reply,
        };
        w.process(&[big]);
        assert!(matches!(assert_err(&rx_big), AppendError::TooLarge { .. }));
        assert_eq!(w.set.last_position(), Position::ZERO);

        // A normal request still commits afterwards.
        let (r, rx) = request(&[("Ok", &["a"])], None);
        w.process(&[r]);
        assert_ok(&rx);
    }

    // --- paranoid property test: tips never disagree with the scan oracle ---

    struct Rng(u64);
    impl Rng {
        fn next(&mut self) -> u64 {
            self.0 = self
                .0
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            self.0 >> 17
        }
        fn below(&mut self, n: u64) -> u64 {
            self.next() % n
        }
    }

    #[test]
    fn verify_tips_agrees_with_scan_over_random_conditions() {
        // With verify_tips on, every conditional append cross-checks the tips fast-reject
        // against the scan oracle and panics on disagreement. Driving a random workload
        // through it is the property test: a false negative or off-by-one in the tips
        // would surface as a panic here. It also checks positions stay dense.
        let dir = TempDir::new().unwrap();
        let (mut w, _tx) = worker(&dir, cfg());

        let types = ["A", "B", "C"];
        let universe = ["t0", "t1", "t2", "t3", "t4", "t5"];
        let mut rng = Rng(0x9E3779B97F4A7C15);
        let mut expected_last = 0u64;

        for _ in 0..600 {
            // Random event: 1..=2 distinct tags from the universe.
            let ty = types[rng.below(types.len() as u64) as usize];
            let start = rng.below(universe.len() as u64) as usize;
            let ntags = 1 + rng.below(2) as usize;
            let picked: Vec<&str> = (0..ntags)
                .map(|i| universe[(start + i) % universe.len()])
                .collect();
            let ev = event(ty, &picked);

            let condition = if rng.below(2) == 0 {
                // Guard on a random 1..=2 tag subset with a random valid `after`.
                let gstart = rng.below(universe.len() as u64) as usize;
                let gn = 1 + rng.below(2) as usize;
                let gtags: Vec<&str> = (0..gn)
                    .map(|i| universe[(gstart + i) % universe.len()])
                    .collect();
                let tip = expected_last;
                let after = if tip == 0 { 0 } else { rng.below(tip + 1) };
                Some(guard(&gtags, Position::new(after)))
            } else {
                None
            };

            let (reply, rx) = mpsc::channel();
            let req = Request {
                events: vec![ev],
                condition,
                reply,
            };
            w.process(&[req]);

            match rx.try_recv().unwrap() {
                Ok(range) => {
                    assert_eq!(range.first.get(), expected_last + 1);
                    expected_last = range.last.get();
                }
                Err(AppendError::Conflict { .. }) => {} // no event committed
                Err(other) => panic!("unexpected error in property test: {other:?}"),
            }
            assert_eq!(w.set.last_position(), Position::new(expected_last));
        }
    }

    #[test]
    fn collect_defers_oversize_behind_a_normal_request() {
        let dir = TempDir::new().unwrap();
        let cfg = WriterConfig {
            max_batch_bytes: 4096,
            ..cfg()
        };
        let (mut w, tx) = worker(&dir, cfg);

        // A normal request, then an oversize one (bigger than max_batch_bytes).
        let (r_small, _rx_small) = request(&[("Small", &["a"])], None);
        let big_event = Event::new(
            &EventType::new("Big").unwrap(),
            &Tags::empty(),
            &vec![0u8; 8192],
        )
        .unwrap();
        let (reply, _rx_big) = mpsc::channel();
        let r_big = Request {
            events: vec![big_event],
            condition: None,
            reply,
        };
        tx.send(Message::Append(r_small)).unwrap();
        tx.send(Message::Append(r_big)).unwrap();

        // First collect takes only the small one; the oversize one is pushed back.
        let batch1 = w.collect().unwrap();
        assert_eq!(batch1.len(), 1);
        assert!(w.pushback.is_some());

        // Second collect starts with the deferred oversize request, solo.
        let batch2 = w.collect().unwrap();
        assert_eq!(batch2.len(), 1);
        assert!(w.pushback.is_none());
    }
}
