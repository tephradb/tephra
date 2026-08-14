//! An async read pool for embedding tephra inside an async application.
//!
//! Reads are a blocking, CPU-bound scan over a lock-free snapshot that run on the caller's
//! own thread (see [`ReadHandle::read`]). Calling one directly from an async task would
//! stall the executor, so this pool offloads reads onto a fixed set of worker threads, each
//! holding its own cloned [`ReadHandle`], and streams the matched events back over a
//! `flume` async channel. It is runtime-agnostic: it drives on tokio, smol, or async-std
//! with no runtime dependency of its own.
//!
//! ```no_run
//! # #[cfg(feature = "async")]
//! # async fn demo(handle: tephra::ReadHandle) -> Result<(), tephra::ReadError> {
//! use tephra::{Position, Query};
//! use tephra::read::pool::ReadPool;
//!
//! // One worker per read-parallelism slot; clone the pool freely (wrap in `Arc`).
//! let pool = ReadPool::new(handle, 4);
//!
//! // Collect the whole result:
//! let events = pool.read_all(Query::all(), Position::ZERO, None).await?;
//!
//! // Or stream owned batches with backpressure:
//! use futures_core::Stream;
//! let mut stream = pool.read(Query::all(), Position::ZERO, None);
//! # let _ = (events, &mut stream);
//! # Ok(())
//! # }
//! ```

use std::future::Future;
use std::panic::{self, AssertUnwindSafe};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll};
use std::time::Duration;
use std::{mem, thread};

use flume::SendTimeoutError;
use futures_core::Stream;

use crate::Position;
use crate::event::Event;
use crate::query::Query;
use crate::read::{DEFAULT_MAX_BATCH_EVENTS, ReadError, ReadHandle};

/// One batch of matched events, or the read error that ended the scan.
type ReadItem = Result<Vec<(Position, Event)>, ReadError>;

/// How long a backpressured worker parks on a send before re-checking for shutdown. Bounds
/// the time [`ReadPool::shutdown`] (and drop) can wait on a worker stalled by a slow consumer.
const SHUTDOWN_POLL: Duration = Duration::from_millis(50);

/// Tuning for a [`ReadPool`]. `Default` mirrors the server's read defaults.
#[derive(Debug, Clone, Copy)]
pub struct ReadPoolConfig {
    /// Number of worker threads (read-parallelism). Clamped to at least 1.
    pub workers: usize,
    /// Flush a batch once it holds this many events. Clamped to at least 1.
    pub batch_events: usize,
    /// Flush a batch once its event bytes reach this. Clamped to at least 1.
    pub batch_bytes: usize,
    /// In-flight batches allowed per read before a slow consumer applies backpressure to its
    /// worker. Clamped to at least 1.
    pub channel_depth: usize,
    /// Upper bound on reads submitted but not yet picked up by a worker. Submitting past this
    /// awaits a free slot (backpressure) rather than growing without bound. Clamped to at
    /// least 1.
    pub queue_capacity: usize,
}

impl Default for ReadPoolConfig {
    fn default() -> ReadPoolConfig {
        ReadPoolConfig {
            workers: 1,
            batch_events: DEFAULT_MAX_BATCH_EVENTS,
            batch_bytes: 512 * 1024,
            channel_depth: 8,
            queue_capacity: 1024,
        }
    }
}

/// A job handed to a worker: one read to run and the channel to stream its batches over.
struct ReadJob {
    query: Query,
    /// The pagination cursor: an exclusive lower bound (`after`) for a forward read, or an
    /// exclusive upper bound (`before`) for a backward one. Which it means is set by `reverse`.
    cursor: Position,
    limit: Option<u64>,
    /// Run the read backwards (descending from `cursor`) rather than forwards.
    reverse: bool,
    out: flume::Sender<ReadItem>,
    batch_events: usize,
    batch_bytes: usize,
    /// Test-only hook: when set, the worker panics before scanning so the abort-reporting
    /// path can be exercised. Per-job so parallel tests never interfere. Compiled only for
    /// this crate's own tests, so it is absent from every normal build and from downstream
    /// dependents.
    #[cfg(test)]
    inject_panic: bool,
}

/// A pool of worker threads that run blocking reads off the async caller's thread.
///
/// Each worker owns a cloned [`ReadHandle`], so reads run concurrently over the shared,
/// lock-free read snapshot while a writer appends. Cloneable and `Send + Sync` via wrapping
/// in an `Arc`; dropping the pool closes the job queue and joins every worker.
pub struct ReadPool {
    // `Option` so `Drop` can close the job channel (drop the sole sender) before joining.
    tx: Option<flume::Sender<ReadJob>>,
    workers: Vec<thread::JoinHandle<()>>,
    // Set on teardown so a worker parked on a backpressured send abandons its read rather
    // than pinning `join` forever behind a consumer that never drains.
    shutdown: Arc<AtomicBool>,
    batch_events: usize,
    batch_bytes: usize,
    channel_depth: usize,
}

impl ReadPool {
    /// A pool of `workers` threads reading through clones of `handle`, with default batch
    /// sizing. See [`with_config`](Self::with_config) to tune batching and backpressure.
    pub fn new(handle: ReadHandle, workers: usize) -> ReadPool {
        ReadPool::with_config(
            handle,
            ReadPoolConfig {
                workers,
                ..ReadPoolConfig::default()
            },
        )
    }

    /// A pool built from an explicit [`ReadPoolConfig`].
    pub fn with_config(handle: ReadHandle, config: ReadPoolConfig) -> ReadPool {
        let worker_count = config.workers.max(1);
        let batch_events = config.batch_events.max(1);
        let batch_bytes = config.batch_bytes.max(1);
        let channel_depth = config.channel_depth.max(1);
        let queue_capacity = config.queue_capacity.max(1);

        let (tx, rx) = flume::bounded::<ReadJob>(queue_capacity);
        let shutdown = Arc::new(AtomicBool::new(false));
        let mut workers = Vec::with_capacity(worker_count);
        for _ in 0..worker_count {
            let rx = rx.clone();
            let handle = handle.clone();
            let shutdown = Arc::clone(&shutdown);
            let worker = thread::Builder::new()
                .name("tephra-read-pool".to_string())
                .spawn(move || worker_loop(handle, rx, shutdown))
                .expect("spawn tephra read pool worker");
            workers.push(worker);
        }

        ReadPool {
            tx: Some(tx),
            workers,
            shutdown,
            batch_events,
            batch_bytes,
            channel_depth,
        }
    }

    /// Runs `query` on a worker and streams the matched events back as owned batches,
    /// ascending by position, strictly after `after`, up to `limit` (`None` = unlimited).
    ///
    /// The read is lazy: it is submitted the first time the stream is polled, so this call
    /// never blocks and the submission (which awaits a free job slot when the queue is full)
    /// applies backpressure through the stream rather than on the caller's thread. The stream
    /// ends when the read completes; a read error, including [`ReadError::Aborted`] if the
    /// worker panicked, is delivered as the stream's final item. A slow consumer applies
    /// backpressure: once `channel_depth` batches are buffered the worker blocks on its send,
    /// holding its pool slot until the consumer drains. Dropping the stream lets the worker
    /// observe the closed channel and stop early. If the pool has been shut down the stream is
    /// empty.
    pub fn read(&self, query: Query, after: Position, limit: Option<u64>) -> ReadStream {
        self.stream(query, after, false, limit)
    }

    /// The newest-first dual of [`read`](Self::read): streams the matched events **descending**
    /// by position, strictly before `before`, up to `limit`. `before` is an exclusive upper
    /// bound, so `read_back(query, Position::MAX, limit)` streams from the tip. Same laziness,
    /// backpressure, and shutdown behavior as [`read`](Self::read). See
    /// [`ReadHandle::read_back`].
    pub fn read_back(&self, query: Query, before: Position, limit: Option<u64>) -> ReadStream {
        self.stream(query, before, true, limit)
    }

    /// Shared submission for [`read`](Self::read) and [`read_back`](Self::read_back): builds the
    /// job (forward or reverse) and returns a lazily-submitted stream.
    fn stream(
        &self,
        query: Query,
        cursor: Position,
        reverse: bool,
        limit: Option<u64>,
    ) -> ReadStream {
        let (out_tx, out_rx) = flume::bounded::<ReadItem>(self.channel_depth);
        let job = ReadJob {
            query,
            cursor,
            limit,
            reverse,
            out: out_tx,
            batch_events: self.batch_events,
            batch_bytes: self.batch_bytes,
            #[cfg(test)]
            inject_panic: false,
        };
        match &self.tx {
            Some(tx) => {
                let tx = tx.clone();
                // Submit lazily on first poll. If the pool is gone the send fails and the job
                // (with its result sender) drops, ending the stream. Awaiting a bounded queue
                // yields instead of blocking, so this is safe even on a single-threaded runtime.
                let submit: Pin<Box<dyn Future<Output = ()> + Send>> = Box::pin(async move {
                    let _ = tx.send_async(job).await;
                });
                ReadStream(StreamState::Submitting {
                    submit,
                    out: out_rx,
                })
            }
            // Pool shut down: the job (and `out_tx`) drop here, so the stream is empty.
            None => ReadStream(StreamState::Done),
        }
    }

    /// Runs `query` on a worker and collects every matched event into one `Vec`, ascending by
    /// position. The async counterpart of draining [`read`](Self::read); prefer that when the
    /// result may be large, since this buffers the whole result in memory. Submission awaits a
    /// free job slot when the queue is full rather than blocking.
    pub async fn read_all(
        &self,
        query: Query,
        after: Position,
        limit: Option<u64>,
    ) -> Result<Vec<(Position, Event)>, ReadError> {
        self.collect(query, after, false, limit).await
    }

    /// The newest-first dual of [`read_all`](Self::read_all): collects every matched event
    /// **descending** by position, strictly before `before`, into one `Vec`. Prefer
    /// [`read_back`](Self::read_back) when the result may be large. See
    /// [`ReadHandle::read_back`].
    pub async fn read_all_back(
        &self,
        query: Query,
        before: Position,
        limit: Option<u64>,
    ) -> Result<Vec<(Position, Event)>, ReadError> {
        self.collect(query, before, true, limit).await
    }

    /// Shared collection for [`read_all`](Self::read_all) and
    /// [`read_all_back`](Self::read_all_back).
    async fn collect(
        &self,
        query: Query,
        cursor: Position,
        reverse: bool,
        limit: Option<u64>,
    ) -> Result<Vec<(Position, Event)>, ReadError> {
        let (out_tx, out_rx) = flume::bounded::<ReadItem>(self.channel_depth);
        let job = ReadJob {
            query,
            cursor,
            limit,
            reverse,
            out: out_tx,
            batch_events: self.batch_events,
            batch_bytes: self.batch_bytes,
            #[cfg(test)]
            inject_panic: false,
        };
        if let Some(tx) = &self.tx {
            // A closed queue (pool shut down) drops the job and its sender, so the loop below
            // sees an immediate disconnect and returns what it has.
            let _ = tx.send_async(job).await;
        }
        let mut out = Vec::new();
        loop {
            match out_rx.recv_async().await {
                Ok(Ok(mut batch)) => out.append(&mut batch),
                Ok(Err(err)) => return Err(err),
                // Every sender dropped: the read finished (or the pool is gone).
                Err(_) => return Ok(out),
            }
        }
    }

    /// Closes the job queue and joins every worker. Equivalent to dropping the pool, but
    /// explicit and blocking until the workers have exited. Bounded even if a consumer is
    /// holding an undrained stream: a parked worker rechecks for shutdown on a fixed poll
    /// interval and abandons its read.
    pub fn shutdown(self) {
        // `Drop` does the work.
    }

    /// Test-only: submit a read that panics inside its worker, returning the raw result
    /// channel so a test can assert the abort is reported rather than silently swallowed.
    #[cfg(test)]
    fn submit_panicking(&self, query: Query) -> flume::Receiver<ReadItem> {
        let (out_tx, out_rx) = flume::bounded::<ReadItem>(self.channel_depth);
        let job = ReadJob {
            query,
            cursor: Position::ZERO,
            limit: None,
            reverse: false,
            out: out_tx,
            batch_events: self.batch_events,
            batch_bytes: self.batch_bytes,
            inject_panic: true,
        };
        if let Some(tx) = &self.tx {
            tx.send(job).expect("submit panicking read");
        }
        out_rx
    }
}

impl Drop for ReadPool {
    fn drop(&mut self) {
        // Signal first, so a worker parked on a backpressured send wakes and gives up instead
        // of blocking `join` behind a consumer that never drains its stream.
        self.shutdown.store(true, Ordering::Release);
        // Drop the sole job sender so idle workers see the channel close and leave their loop.
        drop(self.tx.take());
        for worker in self.workers.drain(..) {
            let _ = worker.join();
        }
    }
}

/// A worker pulls jobs until the pool's sender is dropped, isolating each read's panic so one
/// bad read cannot shrink the pool. A caught panic is reported to the consumer as
/// [`ReadError::Aborted`] so a truncated result is never mistaken for a completed one.
fn worker_loop(handle: ReadHandle, rx: flume::Receiver<ReadJob>, shutdown: Arc<AtomicBool>) {
    while let Ok(job) = rx.recv() {
        // Keep a result sender alive past the read: on a panic, `run_job`'s own sender drops
        // during the unwind, which would otherwise close the channel and look like a clean
        // finish. This retained clone lets us send the abort marker instead.
        let out = job.out.clone();
        let outcome = panic::catch_unwind(AssertUnwindSafe(|| run_job(&handle, job, &shutdown)));
        if outcome.is_err() {
            let _ = out.send(ReadItem::Err(ReadError::Aborted));
        }
    }
}

/// Runs one read to completion, flushing owned batches over the job's channel. Returns early
/// if the consumer drops its receiver or the pool is shutting down.
fn run_job(handle: &ReadHandle, job: ReadJob, shutdown: &AtomicBool) {
    let ReadJob {
        query,
        cursor,
        limit,
        reverse,
        out,
        batch_events,
        batch_bytes,
        #[cfg(test)]
        inject_panic,
    } = job;

    // Test-only: exercise the worker's abort-reporting path (see `worker_loop`). The retained
    // sender there turns this panic into a `ReadError::Aborted` rather than a silent close.
    #[cfg(test)]
    if inject_panic {
        panic!("injected read panic (test hook)");
    }

    let mut reads = if reverse {
        handle.read_back(&query, cursor, limit)
    } else {
        handle.read(&query, cursor, limit)
    };
    let mut batch: Vec<(Position, Event)> = Vec::new();
    let mut batch_bytes_seen = 0usize;

    while let Some(item) = reads.next() {
        match item {
            Ok(seq) => {
                batch_bytes_seen += seq.event.as_bytes().len();
                batch.push((seq.position, seq.event.to_owned()));
                if batch.len() >= batch_events || batch_bytes_seen >= batch_bytes {
                    if !send_item(&out, Ok(mem::take(&mut batch)), shutdown) {
                        return;
                    }
                    batch_bytes_seen = 0;
                }
            }
            Err(err) => {
                let _ = send_item(&out, Err(err), shutdown);
                return;
            }
        }
    }

    if !batch.is_empty() {
        let _ = send_item(&out, Ok(batch), shutdown);
    }
    // Dropping `out` here ends the stream.
}

/// Sends one item, parking on a full channel but waking every [`SHUTDOWN_POLL`] to observe
/// shutdown. Returns `false` (stop the read) if the pool is shutting down or the consumer is
/// gone.
fn send_item(out: &flume::Sender<ReadItem>, mut item: ReadItem, shutdown: &AtomicBool) -> bool {
    loop {
        if shutdown.load(Ordering::Acquire) {
            return false;
        }
        match out.send_timeout(item, SHUTDOWN_POLL) {
            Ok(()) => return true,
            Err(SendTimeoutError::Timeout(returned)) => item = returned,
            Err(SendTimeoutError::Disconnected(_)) => return false,
        }
    }
}

/// The state a [`ReadStream`] moves through: awaiting job submission, then draining results.
enum StreamState {
    Submitting {
        submit: Pin<Box<dyn Future<Output = ()> + Send>>,
        out: flume::Receiver<ReadItem>,
    },
    Streaming(flume::r#async::RecvStream<'static, ReadItem>),
    Done,
}

/// An async stream of owned `(Position, Event)` batches from a [`ReadPool::read`]. The read
/// error that ended a scan, if any, arrives as the final item.
pub struct ReadStream(StreamState);

impl Stream for ReadStream {
    type Item = ReadItem;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        loop {
            // Take ownership of the state so we can move `out` into a stream on transition;
            // restore it on every path that does not fall through to the next state.
            match mem::replace(&mut this.0, StreamState::Done) {
                StreamState::Submitting { mut submit, out } => match submit.as_mut().poll(cx) {
                    Poll::Ready(()) => {
                        this.0 = StreamState::Streaming(out.into_stream());
                    }
                    Poll::Pending => {
                        this.0 = StreamState::Submitting { submit, out };
                        return Poll::Pending;
                    }
                },
                StreamState::Streaming(mut stream) => {
                    let polled = Pin::new(&mut stream).poll_next(cx);
                    this.0 = StreamState::Streaming(stream);
                    return polled;
                }
                StreamState::Done => return Poll::Ready(None),
            }
        }
    }
}

#[cfg(all(test, feature = "async"))]
mod tests {
    use std::future;
    use std::time::Instant;

    use super::*;

    use tempfile::TempDir;

    use crate::event::{Event, EventType, Tag, Tags};
    use crate::log::set::{SegmentConfig, SegmentSet};
    use crate::query::{Query, QueryItem};
    use crate::writer::{WriteCoordinator, WriteHandle, WriterConfig};

    /// Executor-free `block_on` (mirrors `tests/writer.rs`): a `flume` async op wakes us from
    /// its own thread, so parking the current thread is enough to drive a read to completion.
    fn block_on<F: future::Future>(fut: F) -> F::Output {
        use std::pin::pin;
        use std::sync::Arc;
        use std::task::{Context, Poll, Wake, Waker};
        use std::thread::{self, Thread};

        struct ThreadWaker(Thread);
        impl Wake for ThreadWaker {
            fn wake(self: Arc<Self>) {
                self.0.unpark();
            }
            fn wake_by_ref(self: &Arc<Self>) {
                self.0.unpark();
            }
        }

        let waker = Waker::from(Arc::new(ThreadWaker(thread::current())));
        let mut cx = Context::from_waker(&waker);
        let mut fut = pin!(fut);
        loop {
            match fut.as_mut().poll(&mut cx) {
                Poll::Ready(out) => return out,
                Poll::Pending => thread::park(),
            }
        }
    }

    fn tags(items: &[&str]) -> Tags {
        Tags::new(items.iter().map(|s| Tag::new(*s).unwrap())).unwrap()
    }

    fn event(ty: &str, tag_strs: &[&str]) -> Event {
        Event::new(&EventType::new(ty).unwrap(), &tags(tag_strs), b"data").unwrap()
    }

    fn store() -> (TempDir, WriteCoordinator, WriteHandle) {
        let dir = TempDir::new().unwrap();
        let set = SegmentSet::open(dir.path(), SegmentConfig::new(64 << 20)).unwrap();
        let (coord, handle) = WriteCoordinator::start(set, WriterConfig::default()).unwrap();
        (dir, coord, handle)
    }

    /// The positions from a direct blocking read, for cross-checking the pool.
    fn direct(handle: &WriteHandle, query: &Query) -> Vec<Position> {
        let mut reads = handle.read(query, Position::ZERO, None);
        let mut out = Vec::new();
        while let Some(item) = reads.next() {
            out.push(item.expect("read failed").position);
        }
        out
    }

    /// Drives a [`ReadStream`] to the end, collecting positions and counting batches.
    fn drain(mut stream: ReadStream) -> (Vec<Position>, usize) {
        block_on(async {
            let mut positions = Vec::new();
            let mut batches = 0;
            future::poll_fn(|cx| {
                loop {
                    match Pin::new(&mut stream).poll_next(cx) {
                        Poll::Ready(Some(item)) => {
                            let batch = item.expect("read failed");
                            batches += 1;
                            positions.extend(batch.into_iter().map(|(p, _)| p));
                        }
                        Poll::Ready(None) => return Poll::Ready((positions.clone(), batches)),
                        Poll::Pending => return Poll::Pending,
                    }
                }
            })
            .await
        })
    }

    #[test]
    fn read_all_returns_every_event_in_position_order() {
        let (_dir, _coord, handle) = store();
        for i in 0..50 {
            handle
                .append(
                    vec![event("Enrolled", &[&format!("course:c{}", i % 5)])],
                    None,
                )
                .unwrap();
        }
        let pool = ReadPool::new(handle.reader(), 4);

        let got = block_on(pool.read_all(Query::all(), Position::ZERO, None)).unwrap();
        let positions: Vec<Position> = got.iter().map(|(p, _)| *p).collect();

        assert_eq!(positions, direct(&handle, &Query::all()));
        assert!(positions.windows(2).all(|w| w[0] < w[1]));
    }

    #[test]
    fn read_back_streams_newest_first_and_matches_read_all_reversed() {
        let (_dir, _coord, handle) = store();
        for i in 0..50 {
            handle
                .append(
                    vec![event("Enrolled", &[&format!("course:c{}", i % 5)])],
                    None,
                )
                .unwrap();
        }
        let pool = ReadPool::new(handle.reader(), 4);

        // read_all_back over the whole log equals read_all reversed.
        let forward = block_on(pool.read_all(Query::all(), Position::ZERO, None)).unwrap();
        let back = block_on(pool.read_all_back(Query::all(), Position::MAX, None)).unwrap();
        let want: Vec<Position> = forward.iter().rev().map(|(p, _)| *p).collect();
        let got: Vec<Position> = back.iter().map(|(p, _)| *p).collect();
        assert_eq!(got, want);
        assert!(got.windows(2).all(|w| w[0] > w[1]), "descending");

        // The streaming form agrees with the collected form, and a limit takes the newest N.
        let (streamed, _) = drain(pool.read_back(Query::all(), Position::MAX, None));
        assert_eq!(streamed, got);
        let capped = block_on(pool.read_all_back(Query::all(), Position::MAX, Some(5))).unwrap();
        let capped: Vec<Position> = capped.iter().map(|(p, _)| *p).collect();
        assert_eq!(capped, want[..5]);
    }

    #[test]
    fn streamed_batches_concatenate_to_read_all() {
        let (_dir, _coord, handle) = store();
        for _ in 0..30 {
            handle
                .append(vec![event("Enrolled", &["course:c1"])], None)
                .unwrap();
        }
        // Tiny batches so a 30-event read spans several of them.
        let pool = ReadPool::with_config(
            handle.reader(),
            ReadPoolConfig {
                workers: 2,
                batch_events: 4,
                channel_depth: 2,
                ..ReadPoolConfig::default()
            },
        );

        let (positions, batches) = drain(pool.read(Query::all(), Position::ZERO, None));
        assert_eq!(positions, direct(&handle, &Query::all()));
        assert!(batches >= 2, "expected several batches, got {batches}");
    }

    #[test]
    fn query_and_limit_are_honored() {
        let (_dir, _coord, handle) = store();
        for i in 0..20 {
            let course = if i % 2 == 0 {
                "course:even"
            } else {
                "course:odd"
            };
            handle
                .append(vec![event("Enrolled", &[course])], None)
                .unwrap();
        }
        let pool = ReadPool::new(handle.reader(), 2);

        let query = Query::item(QueryItem::with_tags(tags(&["course:even"])));
        let limited = block_on(pool.read_all(query.clone(), Position::ZERO, Some(3))).unwrap();
        assert_eq!(limited.len(), 3);

        let all_even = block_on(pool.read_all(query.clone(), Position::ZERO, None)).unwrap();
        assert_eq!(all_even.len(), 10);
        assert_eq!(
            all_even.iter().map(|(p, _)| *p).collect::<Vec<_>>(),
            direct(&handle, &query),
        );
    }

    #[test]
    fn concurrent_reads_all_complete_while_a_writer_appends() {
        use std::sync::Arc;

        let (_dir, _coord, handle) = store();
        for _ in 0..40 {
            handle
                .append(vec![event("Enrolled", &["course:c1"])], None)
                .unwrap();
        }
        let pool = Arc::new(ReadPool::new(handle.reader(), 4));

        // A writer keeps appending during the reads.
        let writer = {
            let handle = handle.clone();
            thread::spawn(move || {
                for _ in 0..40 {
                    handle
                        .append(vec![event("Enrolled", &["course:c2"])], None)
                        .unwrap();
                }
            })
        };

        let readers: Vec<_> = (0..8)
            .map(|_| {
                let pool = Arc::clone(&pool);
                thread::spawn(move || {
                    let got = block_on(pool.read_all(Query::all(), Position::ZERO, None)).unwrap();
                    // Every read sees a dense prefix (at least the 40 pre-seeded events).
                    assert!(got.len() >= 40);
                    let positions: Vec<Position> = got.iter().map(|(p, _)| *p).collect();
                    assert!(positions.windows(2).all(|w| w[0] < w[1]));
                })
            })
            .collect();

        writer.join().unwrap();
        for reader in readers {
            reader.join().unwrap();
        }
    }

    #[test]
    fn shutdown_joins_workers_and_a_later_read_is_empty() {
        let (_dir, _coord, handle) = store();
        handle
            .append(vec![event("Enrolled", &["course:c1"])], None)
            .unwrap();

        let pool = ReadPool::new(handle.reader(), 2);
        let before = block_on(pool.read_all(Query::all(), Position::ZERO, None)).unwrap();
        assert_eq!(before.len(), 1);

        // Explicit shutdown blocks until the workers exit.
        pool.shutdown();
    }

    #[test]
    fn a_tiny_bounded_queue_still_reads_correctly() {
        let (_dir, _coord, handle) = store();
        for _ in 0..25 {
            handle
                .append(vec![event("Enrolled", &["course:c1"])], None)
                .unwrap();
        }
        // queue_capacity 1 forces submissions to serialize through one slot.
        let pool = ReadPool::with_config(
            handle.reader(),
            ReadPoolConfig {
                workers: 1,
                queue_capacity: 1,
                channel_depth: 1,
                batch_events: 3,
                ..ReadPoolConfig::default()
            },
        );

        let a = block_on(pool.read_all(Query::all(), Position::ZERO, None)).unwrap();
        let b = block_on(pool.read_all(Query::all(), Position::ZERO, None)).unwrap();
        assert_eq!(a.len(), 25);
        assert_eq!(
            a.iter().map(|(p, _)| *p).collect::<Vec<_>>(),
            direct(&handle, &Query::all())
        );
        assert_eq!(a.len(), b.len());
    }

    #[test]
    fn a_panicking_read_surfaces_as_aborted_not_a_clean_end() {
        let (_dir, _coord, handle) = store();
        for _ in 0..5 {
            handle
                .append(vec![event("Enrolled", &["course:c1"])], None)
                .unwrap();
        }
        let pool = ReadPool::new(handle.reader(), 1);

        // The worker catches the injected panic and reports it. Without that, the result
        // channel would just close, which a consumer cannot tell apart from a completed read.
        // (An "injected read panic" line on stderr is expected here; the catch is deliberate.)
        let rx = pool.submit_panicking(Query::all());
        let first = block_on(rx.recv_async()).expect("channel closed without reporting an item");
        match first {
            Err(ReadError::Aborted) => {}
            // `ReadItem` is not `Debug` (events are not), so report via `Display` / a literal.
            Err(other) => panic!("expected ReadError::Aborted, got a different error: {other}"),
            Ok(_) => panic!("expected ReadError::Aborted, got a successful batch"),
        }
        // Nothing follows the abort: the stream ends.
        assert!(
            block_on(rx.recv_async()).is_err(),
            "expected the channel to close after the abort",
        );
    }

    #[test]
    fn dropping_the_pool_with_an_undrained_stream_does_not_hang() {
        let (_dir, _coord, handle) = store();
        for _ in 0..50 {
            handle
                .append(vec![event("Enrolled", &["course:c1"])], None)
                .unwrap();
        }
        // One worker, depth-1 channel, one-event batches: after the consumer takes a single
        // batch and stops, the worker fills the channel and parks on its next send.
        let pool = ReadPool::with_config(
            handle.reader(),
            ReadPoolConfig {
                workers: 1,
                channel_depth: 1,
                batch_events: 1,
                ..ReadPoolConfig::default()
            },
        );

        let mut stream = pool.read(Query::all(), Position::ZERO, None);
        // Poll once: this submits the read and pulls a single batch, then leaves the stream
        // undrained so the worker backpressures and parks.
        let first = block_on(async {
            future::poll_fn(|cx| match Pin::new(&mut stream).poll_next(cx) {
                Poll::Ready(item) => Poll::Ready(item),
                Poll::Pending => Poll::Pending,
            })
            .await
        });
        assert!(first.is_some());
        // Let the worker reach its parked send.
        thread::sleep(Duration::from_millis(50));

        // Dropping the pool while the worker is parked must not deadlock; it is bounded by
        // SHUTDOWN_POLL. Hold the undrained stream across the drop.
        let start = Instant::now();
        drop(pool);
        assert!(
            start.elapsed() < Duration::from_secs(5),
            "drop hung on a parked worker",
        );
        drop(stream);
    }
}
