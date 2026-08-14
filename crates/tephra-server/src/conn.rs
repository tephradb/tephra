//! Per-connection request handling.
//!
//! A connection is served concurrently: its requests are read on one thread but processed on
//! others, so responses for different requests can interleave and complete out of order (each
//! frame carries its `request_id`, so the client demultiplexes). Per connection there are two
//! fixed threads, a **reader** (this thread) and a **writer**, plus an append **completion
//! pump** and one dedicated thread per subscription. Reads run on a shared, server-wide pool of
//! reusable worker threads ([`ReadPool`]) rather than a thread spawned per request.
//!
//! - **Append** does no work here: [`WriteHandle::append_submit`] hands it to the single write
//!   coordinator, and the pump turns the durable `(request_id, result)` reply into a frame. No
//!   thread waits on an append.
//! - **Read** is queued to the server-wide [`ReadPool`] (admission bounded by the
//!   per-connection in-flight semaphore) and runs on a pool worker over a lock-free snapshot,
//!   streaming `ReadEvents` frames then a `ReadEnd`.
//! - **Subscribe** runs on its own dedicated thread until cancelled or the connection ends, so
//!   it no longer monopolizes the connection.
//!
//! All producers push built [`pb::Response`]s into one bounded channel drained by the writer,
//! so frames never tear and a slow client applies backpressure. A [`pb::CancelRequest`] flips a
//! per-request flag that the read/subscribe loops observe.

use std::collections::HashMap;
use std::io::{BufReader, BufWriter, Write};
use std::mem;
use std::net::{Shutdown, SocketAddr, TcpStream};
use std::panic::{self, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;

use flume::{Receiver, Sender};

use tephra::log::set::PositionRange;
use tephra::query::Query;
use tephra::read::WaitOutcome;
use tephra::writer::{AppendError, WriteHandle};
use tephra::{Event, Position};

use tephra_proto::tephra as pb;
use tephra_proto::{FrameError, read_frame, write_frame};

use crate::ServerConfig;
use crate::convert;

/// The reply payload the coordinator sends back for one append, tagged with its `request_id`.
type AppendReply = (u64, Result<PositionRange, AppendError>);

/// Serves one connection until the client disconnects, the socket is shut down, or a transport
/// error occurs. Spawns the writer, the append pump, and per-request workers, then reads and
/// dispatches requests until the stream ends, and finally tears everything down and joins.
pub(crate) fn serve_connection(
    stream: TcpStream,
    handle: WriteHandle,
    config: ServerConfig,
    running: Arc<AtomicBool>,
    read_pool: Sender<ReadJob>,
) {
    let peer = stream.peer_addr().ok();
    if let Err(err) = stream.set_nodelay(true) {
        tracing::warn!(?peer, %err, "failed to set TCP_NODELAY");
    }

    let read_half = match stream.try_clone() {
        Ok(clone) => clone,
        Err(err) => {
            tracing::warn!(?peer, %err, "failed to clone connection stream");
            return;
        }
    };
    let write_half = match stream.try_clone() {
        Ok(clone) => clone,
        Err(err) => {
            tracing::warn!(?peer, %err, "failed to clone connection stream");
            return;
        }
    };

    let alive = Arc::new(AtomicBool::new(true));
    let (out_tx, out_rx) = flume::bounded::<pb::Response>(config.frame_queue_depth);
    let (reply_tx, reply_rx) = flume::unbounded::<AppendReply>();
    let cancels: Arc<Mutex<HashMap<u64, Arc<AtomicBool>>>> = Arc::default();
    // Appends and reads share one blocking budget; subscriptions get a separate rejecting one, so a
    // long-lived subscription never holds a permit only a cancel could free (see `spawn_subscribe`).
    let inflight = Arc::new(Semaphore::new(config.max_inflight_requests_per_conn));
    let subscriptions = Arc::new(Semaphore::new(config.max_concurrent_subscriptions));
    let workers = WaitGroup::default();

    let writer_thread = {
        let alive = Arc::clone(&alive);
        let shutdown = stream.try_clone().ok();
        let max = config.max_frame_len;
        thread::Builder::new()
            .name("tephra-conn-writer".to_string())
            .spawn(move || writer_loop(write_half, out_rx, shutdown, alive, max, peer))
            .expect("spawn connection writer thread")
    };

    let pump_thread = {
        let out_tx = out_tx.clone();
        let inflight = Arc::clone(&inflight);
        thread::Builder::new()
            .name("tephra-conn-pump".to_string())
            .spawn(move || pump_loop(reply_rx, out_tx, inflight))
            .expect("spawn connection pump thread")
    };

    let conn = ConnCtx {
        handle,
        config,
        running,
        alive: Arc::clone(&alive),
        out_tx: out_tx.clone(),
        cancels: Arc::clone(&cancels),
        workers: workers.clone(),
        inflight,
        subscriptions,
        read_pool,
    };

    let mut reader = BufReader::new(read_half);
    loop {
        let request = match read_frame::<pb::Request, _>(&mut reader, config.max_frame_len) {
            Ok(Some(request)) => request,
            Ok(None) => {
                // Clean close at a frame boundary (the peer closed between frames).
                tracing::debug!(?peer, "connection closed by peer at a frame boundary");
                break;
            }
            Err(err) => {
                // Name the failure to the client when the frame boundary allows it. The frame
                // never decoded, so its id is unknown and reported as 0.
                let error = match &err {
                    FrameError::TooLarge { .. } => Some(convert::too_large(err.to_string())),
                    FrameError::Parse(_) => Some(convert::bad_request(err.to_string())),
                    FrameError::Io(_) | FrameError::Serialize(_) => None,
                };
                if let Some(error) = error {
                    let _ = out_tx.send(make_response(0, ResponseKind::Error(error)));
                }
                // A transport error (reset, broken pipe, torn frame) is not an orderly close;
                // surface it so a load-induced drop is not silent. A plain end-of-stream still
                // arrives as `Ok(None)` above, so this only fires on a genuine failure.
                if alive.load(Ordering::Acquire) {
                    tracing::warn!(?peer, %err, "closing connection: reader failed");
                } else {
                    // The writer already marked the connection dead (its own transport error, or
                    // teardown), so this read error is just the reader observing that.
                    tracing::debug!(?peer, %err, "reader ended after the connection was closed");
                }
                break;
            }
        };

        dispatch(&request, &conn, &reply_tx);
    }

    // Drain before closing so a queued response (e.g. a frame-error reply) still reaches the
    // client: mark dead, drop this thread's channel handles, wait for workers, then join and close.
    // The writer flushes the remainder on its own exit; a dead socket fails writes fast so the
    // joins don't hang (a slow-but-alive client is bounded by TCP keepalive / server shutdown).
    alive.store(false, Ordering::Release);
    drop(conn);
    drop(out_tx);
    drop(reply_tx);
    workers.wait();
    let _ = pump_thread.join();
    let _ = writer_thread.join();
    let _ = stream.shutdown(Shutdown::Both);
}

/// The shared per-connection context handed to workers. Cheap to clone (handles and `Arc`s).
#[derive(Clone)]
struct ConnCtx {
    handle: WriteHandle,
    config: ServerConfig,
    /// Server-wide shutdown signal (the accept loop clears it).
    running: Arc<AtomicBool>,
    /// This connection is being torn down (a transport failure on either half).
    alive: Arc<AtomicBool>,
    out_tx: Sender<pb::Response>,
    cancels: Arc<Mutex<HashMap<u64, Arc<AtomicBool>>>>,
    workers: WaitGroup,
    /// Blocking budget shared by in-flight appends and reads.
    inflight: Arc<Semaphore>,
    /// Rejecting budget for concurrent subscriptions.
    subscriptions: Arc<Semaphore>,
    /// Sender into the shared, server-wide read-worker pool. Cloned per read job.
    read_pool: Sender<ReadJob>,
}

impl ConnCtx {
    /// Whether a streaming worker should keep producing: the server is up, the connection is
    /// alive, and this request has not been cancelled.
    fn should_continue(&self, cancel: &AtomicBool) -> bool {
        self.running.load(Ordering::Acquire)
            && self.alive.load(Ordering::Acquire)
            && !cancel.load(Ordering::Acquire)
    }
}

/// Routes one request. Appends submit and return; reads and subscribes spawn workers; a cancel
/// flips the target's flag; anything unrecognized is a bad request.
fn dispatch(request: &pb::Request, conn: &ConnCtx, reply_tx: &Sender<AppendReply>) {
    let request_id = request.request_id();
    match request.kind() {
        pb::request::KindOneof::Append(append) => handle_append(request_id, append, conn, reply_tx),
        pb::request::KindOneof::Read(read) => spawn_read(request_id, read, conn),
        pb::request::KindOneof::Subscribe(subscribe) => {
            spawn_subscribe(request_id, subscribe, conn)
        }
        pb::request::KindOneof::Cancel(cancel) => {
            if let Some(flag) = conn.cancels.lock().unwrap().get(&cancel.target()) {
                flag.store(true, Ordering::Release);
            }
        }
        // No kind set, or a future kind this server does not understand.
        _ => {
            let error =
                convert::bad_request("request has no append, read, subscribe, or cancel set");
            let _ = conn
                .out_tx
                .send(make_response(request_id, ResponseKind::Error(error)));
        }
    }
}

/// Submits an append to the coordinator without blocking; the pump delivers its reply. Input
/// errors (bad events or condition, empty, or a shut-down coordinator) reply immediately.
///
/// A permit from the in-flight budget is acquired before submitting (blocking the reader when the
/// connection is saturated, which bounds the reply backlog) and released by the pump when the
/// reply is forwarded, or here if the submit itself fails, since no reply will follow.
fn handle_append(
    request_id: u64,
    append: pb::AppendRequestView<'_>,
    conn: &ConnCtx,
    reply_tx: &Sender<AppendReply>,
) {
    let events = match convert::events_from_proto(append) {
        Ok(events) => events,
        Err(err) => {
            let _ = conn.out_tx.send(make_response(
                request_id,
                ResponseKind::Error(convert::bad_request(err)),
            ));
            return;
        }
    };
    let condition = match append.condition_opt() {
        Some(condition) => match convert::condition_from_proto(condition) {
            Ok(condition) => Some(condition),
            Err(err) => {
                let _ = conn.out_tx.send(make_response(
                    request_id,
                    ResponseKind::Error(convert::bad_request(err)),
                ));
                return;
            }
        },
        None => None,
    };

    conn.inflight.acquire();
    if let Err(err) = conn
        .handle
        .append_submit(events, condition, request_id, reply_tx.clone())
    {
        conn.inflight.release();
        let _ = conn.out_tx.send(make_response(
            request_id,
            ResponseKind::Error(convert::append_error_to_proto(&err)),
        ));
    }
}

/// Validates the query, then queues the read onto the shared read pool. Acquiring an in-flight
/// permit here (shared with appends) bounds a connection's outstanding reads and applies
/// backpressure to the reader before the job is enqueued.
fn spawn_read(request_id: u64, read: pb::ReadRequestView<'_>, conn: &ConnCtx) {
    let query = match convert::query_from_proto(read.query()) {
        Ok(query) => query,
        Err(err) => {
            let _ = conn.out_tx.send(make_response(
                request_id,
                ResponseKind::Error(convert::bad_request(err)),
            ));
            return;
        }
    };
    let after = Position::new(read.after());
    // Explicit presence: absent means unlimited, present (even 0) is a real cap.
    let limit = read.limit_opt();

    let cancel = register_cancel(conn, request_id);
    conn.inflight.acquire();
    conn.workers.add();
    let cleanup = WorkerCleanup {
        cancels: Arc::clone(&conn.cancels),
        sem: Some(Arc::clone(&conn.inflight)),
        workers: conn.workers.clone(),
        request_id,
    };
    let job = ReadJob {
        request_id,
        query,
        after,
        limit,
        conn: conn.clone(),
        cancel,
        cleanup,
    };
    if conn.read_pool.send(job).is_err() {
        // The pool is gone, which only happens once the server is shutting down. The job (and
        // with it `WorkerCleanup`) is dropped by the failed send, releasing its permit/worker/
        // cancel. No error frame: teardown is already underway.
        tracing::debug!(request_id, "read pool closed; dropping read");
    }
}

/// Validates the query, then spawns a dedicated thread for the subscription. Subscriptions do not
/// share the appends/reads budget (they are long-lived); instead a full subscription budget
/// *rejects* the request, so the reader never blocks waiting on a permit only a cancel could free.
fn spawn_subscribe(request_id: u64, subscribe: pb::SubscribeRequestView<'_>, conn: &ConnCtx) {
    let query = match convert::query_from_proto(subscribe.query()) {
        Ok(query) => query,
        Err(err) => {
            let _ = conn.out_tx.send(make_response(
                request_id,
                ResponseKind::Error(convert::bad_request(err)),
            ));
            return;
        }
    };
    let after = Position::new(subscribe.after());

    if !conn.subscriptions.try_acquire() {
        let _ = conn.out_tx.send(make_response(
            request_id,
            ResponseKind::Error(convert::bad_request(
                "too many concurrent subscriptions on this connection",
            )),
        ));
        return;
    }
    let cancel = register_cancel(conn, request_id);
    conn.workers.add();
    let cleanup = WorkerCleanup {
        cancels: Arc::clone(&conn.cancels),
        sem: Some(Arc::clone(&conn.subscriptions)),
        workers: conn.workers.clone(),
        request_id,
    };
    let conn_owned = conn.clone();
    if let Err(err) = thread::Builder::new()
        .name("tephra-conn-subscribe".to_string())
        .spawn(move || {
            let _cleanup = cleanup;
            run_subscribe(request_id, query, after, &conn_owned, &cancel);
        })
    {
        tracing::warn!(%err, "failed to spawn subscribe worker");
        let _ = conn.out_tx.send(make_response(
            request_id,
            ResponseKind::Error(convert::bad_request(
                "server could not start the subscription",
            )),
        ));
    }
}

/// Registers a fresh cancel flag for `request_id` so a later `CancelRequest` can find it.
fn register_cancel(conn: &ConnCtx, request_id: u64) -> Arc<AtomicBool> {
    let cancel = Arc::new(AtomicBool::new(false));
    conn.cancels
        .lock()
        .unwrap()
        .insert(request_id, Arc::clone(&cancel));
    cancel
}

/// Streams one read: `ReadEvents` batches then a terminating `ReadEnd`, framed on the same
/// event-count / byte thresholds a subscription uses. Stops quietly if cancelled or the
/// connection dies mid-stream. A log-integrity failure terminates with a single error frame.
fn run_read(
    request_id: u64,
    query: &Query,
    after: Position,
    limit: Option<u64>,
    conn: &ConnCtx,
    cancel: &AtomicBool,
) {
    let mut reads = conn.handle.read(query, after, limit);
    let watermark = reads.watermark();

    let mut batch = pb::ReadEvents::new();
    let mut batch_bytes = 0usize;

    while let Some(item) = reads.next() {
        if !conn.should_continue(cancel) {
            return;
        }
        let sequenced = match item {
            Ok(sequenced) => sequenced,
            Err(err) => {
                let _ = conn.out_tx.send(make_response(
                    request_id,
                    ResponseKind::Error(convert::internal_error(err)),
                ));
                return;
            }
        };
        batch_bytes += sequenced.event.as_bytes().len();
        batch.events_mut().push(convert::sequenced_to_proto(
            sequenced.position,
            sequenced.event,
        ));

        if batch.events().len() >= conn.config.read_batch_events
            || batch_bytes >= conn.config.read_batch_bytes
        {
            let full = mem::replace(&mut batch, pb::ReadEvents::new());
            if conn
                .out_tx
                .send(make_response(request_id, ResponseKind::ReadEvents(full)))
                .is_err()
            {
                return;
            }
            batch_bytes = 0;
        }
    }

    if !batch.events().is_empty()
        && conn
            .out_tx
            .send(make_response(request_id, ResponseKind::ReadEvents(batch)))
            .is_err()
    {
        return;
    }

    let mut end = pb::ReadEnd::new();
    end.set_watermark(watermark.get());
    let _ = conn
        .out_tx
        .send(make_response(request_id, ResponseKind::ReadEnd(end)));
}

/// Serves a live subscription: catch up on matching events after `after`, then tail new ones,
/// framing events like a read but ending only on cancel, connection death, or store shutdown
/// (never a `ReadEnd`). A `SubscribeCaughtUp` marker fires once per live-edge (re-armed).
fn run_subscribe(
    request_id: u64,
    query: Query,
    after: Position,
    conn: &ConnCtx,
    cancel: &AtomicBool,
) {
    let mut sub = conn.handle.subscribe(query, after);
    let mut announced = false;

    loop {
        if !conn.should_continue(cancel) {
            return;
        }
        let batch = match sub.poll_batch() {
            Ok(batch) => batch,
            Err(err) => {
                let _ = conn.out_tx.send(make_response(
                    request_id,
                    ResponseKind::Error(convert::internal_error(err)),
                ));
                return;
            }
        };

        if batch.is_empty() {
            // Reached the live edge: announce caught-up once for this edge, then block for the
            // next commit. A bounded wait keeps the subscription responsive to shutdown/cancel.
            if !announced {
                let mut caught_up = pb::SubscribeCaughtUp::new();
                caught_up.set_watermark(sub.position().get());
                if conn
                    .out_tx
                    .send(make_response(request_id, ResponseKind::CaughtUp(caught_up)))
                    .is_err()
                {
                    return;
                }
                announced = true;
            }
            match sub.wait_timeout(conn.config.subscribe_wait_tick) {
                WaitOutcome::Advanced | WaitOutcome::TimedOut => {}
                WaitOutcome::Closed => return,
            }
        } else {
            if send_event_batch(request_id, &batch, conn).is_err() {
                return;
            }
            announced = false;
        }
    }
}

/// Frames a batch of subscription events into one or more `ReadEvents` responses on the same
/// thresholds a streamed read uses. Returns `Err` if the frame channel closed (writer gone).
fn send_event_batch(
    request_id: u64,
    events: &[(Position, Event)],
    conn: &ConnCtx,
) -> Result<(), ()> {
    let mut batch = pb::ReadEvents::new();
    let mut batch_bytes = 0usize;
    for (position, event) in events {
        batch_bytes += event.as_bytes().len();
        batch
            .events_mut()
            .push(convert::sequenced_to_proto(*position, event.as_ref()));
        if batch.events().len() >= conn.config.read_batch_events
            || batch_bytes >= conn.config.read_batch_bytes
        {
            let full = mem::replace(&mut batch, pb::ReadEvents::new());
            conn.out_tx
                .send(make_response(request_id, ResponseKind::ReadEvents(full)))
                .map_err(|_| ())?;
            batch_bytes = 0;
        }
    }
    if !batch.events().is_empty() {
        conn.out_tx
            .send(make_response(request_id, ResponseKind::ReadEvents(batch)))
            .map_err(|_| ())?;
    }
    Ok(())
}

/// The writer thread: drains built responses and writes them, flushing once per burst so a
/// streamed read is delivered promptly without a syscall per frame. On a transport failure it
/// marks the connection dead and shuts the socket, unblocking the reader and any parked worker.
fn writer_loop(
    write_half: TcpStream,
    out_rx: Receiver<pb::Response>,
    shutdown: Option<TcpStream>,
    alive: Arc<AtomicBool>,
    max_frame_len: u32,
    peer: Option<SocketAddr>,
) {
    let mut writer = BufWriter::new(write_half);
    let outcome = 'outer: loop {
        let Ok(response) = out_rx.recv() else {
            // The channel closed: every producer is gone (orderly teardown). Not a failure.
            break Ok(());
        };
        if let Err(err) = write_frame(&mut writer, &response, max_frame_len) {
            break Err(err);
        }
        // Write anything already queued before paying for a flush.
        while let Ok(response) = out_rx.try_recv() {
            if let Err(err) = write_frame(&mut writer, &response, max_frame_len) {
                break 'outer Err(err);
            }
        }
        if let Err(err) = writer.flush() {
            break Err(FrameError::Io(err));
        }
    };
    // A write failure closes the connection under the client; name it so the drop is not silent.
    // `alive` was still set here means the writer is the half that observed the failure first.
    if let Err(err) = outcome {
        if alive.load(Ordering::Acquire) {
            tracing::warn!(?peer, %err, "closing connection: writer failed");
        } else {
            tracing::debug!(?peer, %err, "writer ended after the connection was closed");
        }
    }
    // Any exit means the output side is done: mark dead and wake the rest of the connection.
    alive.store(false, Ordering::Release);
    if let Some(stream) = shutdown {
        let _ = stream.shutdown(Shutdown::Both);
    }
}

/// The append completion pump: turns each durable reply into a response frame and releases the
/// append's in-flight permit. Exits when the reply channel closes (all appends done and the
/// reader gone) or the writer has gone away.
fn pump_loop(
    reply_rx: Receiver<AppendReply>,
    out_tx: Sender<pb::Response>,
    inflight: Arc<Semaphore>,
) {
    while let Ok((request_id, result)) = reply_rx.recv() {
        let response = match result {
            Ok(range) => {
                let mut ok = pb::AppendResponse::new();
                ok.set_first(range.first.get());
                ok.set_last(range.last.get());
                make_response(request_id, ResponseKind::Append(ok))
            }
            Err(err) => make_response(
                request_id,
                ResponseKind::Error(convert::append_error_to_proto(&err)),
            ),
        };
        // Send before releasing, so a full frame channel keeps the in-flight bound tight.
        let sent = out_tx.send(response);
        inflight.release();
        if sent.is_err() {
            break;
        }
    }
}

/// The payload of one response frame.
enum ResponseKind {
    Append(pb::AppendResponse),
    ReadEvents(pb::ReadEvents),
    ReadEnd(pb::ReadEnd),
    CaughtUp(pb::SubscribeCaughtUp),
    Error(pb::ErrorResponse),
}

/// Builds one `Response` with its `request_id` echoed.
fn make_response(request_id: u64, kind: ResponseKind) -> pb::Response {
    let mut response = pb::Response::new();
    response.set_request_id(request_id);
    match kind {
        ResponseKind::Append(append) => response.set_append(append),
        ResponseKind::ReadEvents(events) => response.set_read_events(events),
        ResponseKind::ReadEnd(end) => response.set_read_end(end),
        ResponseKind::CaughtUp(caught_up) => response.set_caught_up(caught_up),
        ResponseKind::Error(error) => response.set_error(error),
    }
    response
}

// ---------------------------------------------------------------------------
// Read-worker pool
// ---------------------------------------------------------------------------

/// One queued read, carrying everything [`run_read`] needs so a pool worker can run it without
/// borrowing the connection. The `WorkerCleanup` guard rides along and drops when the job
/// finishes (or if a worker unwinds), releasing the in-flight permit, the cancel entry, and the
/// worker count exactly once.
pub(crate) struct ReadJob {
    request_id: u64,
    query: Query,
    after: Position,
    limit: Option<u64>,
    conn: ConnCtx,
    cancel: Arc<AtomicBool>,
    cleanup: WorkerCleanup,
}

impl ReadJob {
    fn run(self) {
        let ReadJob {
            request_id,
            query,
            after,
            limit,
            conn,
            cancel,
            cleanup,
        } = self;
        run_read(request_id, &query, after, limit, &conn, &cancel);
        // Release the permit/cancel/worker only once the read's frames are all queued.
        drop(cleanup);
    }
}

/// A shared, server-wide pool of reusable worker threads that stream reads. Created once at
/// startup, so a read pays no per-request thread-creation cost. One unbounded MPMC channel feeds
/// all workers: a sent [`ReadJob`] wakes whichever worker is idle, which runs it to completion
/// and returns for the next (the reads themselves, not the queue, are the bottleneck; the
/// per-connection in-flight budget already bounds how many jobs can be outstanding). Subscriptions
/// keep their own dedicated threads and never use this pool.
pub(crate) struct ReadPool {
    tx: Sender<ReadJob>,
    workers: Vec<thread::JoinHandle<()>>,
}

impl ReadPool {
    /// Spawns `size` (at least one) reusable worker threads reading jobs off a shared queue.
    pub(crate) fn new(size: usize) -> ReadPool {
        let size = size.max(1);
        let (tx, rx) = flume::unbounded::<ReadJob>();
        let mut workers = Vec::with_capacity(size);
        for _ in 0..size {
            let rx = rx.clone();
            let worker = thread::Builder::new()
                .name("tephra-read-worker".to_string())
                .spawn(move || {
                    while let Ok(job) = rx.recv() {
                        // Isolate a panic in one read so a single bad request cannot tear down a
                        // permanent worker and shrink the pool. `WorkerCleanup` lives inside the
                        // job, so it still drops during the unwind.
                        let _ = panic::catch_unwind(AssertUnwindSafe(move || job.run()));
                    }
                })
                .expect("spawn read worker thread");
            workers.push(worker);
        }
        ReadPool { tx, workers }
    }

    /// A sender a connection clones into its [`ConnCtx`] to enqueue reads.
    pub(crate) fn sender(&self) -> Sender<ReadJob> {
        self.tx.clone()
    }

    /// Drops the pool's own sender and joins the workers. Once every connection (and so every
    /// cloned sender) is gone, the channel disconnects and each worker's `recv` returns `Err`,
    /// ending its loop.
    pub(crate) fn shutdown(self) {
        drop(self.tx);
        for worker in self.workers {
            let _ = worker.join();
        }
    }
}

// ---------------------------------------------------------------------------
// Small concurrency primitives
// ---------------------------------------------------------------------------

/// Releases a worker's resources when its thread ends (including on panic or a spawn failure):
/// deregisters the cancel flag, returns the read permit (if any), and marks the worker done.
struct WorkerCleanup {
    cancels: Arc<Mutex<HashMap<u64, Arc<AtomicBool>>>>,
    sem: Option<Arc<Semaphore>>,
    workers: WaitGroup,
    request_id: u64,
}

impl Drop for WorkerCleanup {
    fn drop(&mut self) {
        self.cancels.lock().unwrap().remove(&self.request_id);
        if let Some(sem) = &self.sem {
            sem.release();
        }
        self.workers.done();
    }
}

/// A counting semaphore bounding a per-connection budget. Used two ways: `acquire` (blocking) for
/// the appends/reads budget, and `try_acquire` (rejecting) for the subscriptions budget.
struct Semaphore {
    permits: Mutex<usize>,
    available: Condvar,
}

impl Semaphore {
    fn new(permits: usize) -> Semaphore {
        Semaphore {
            // At least one, so a misconfigured zero cannot wedge the connection.
            permits: Mutex::new(permits.max(1)),
            available: Condvar::new(),
        }
    }

    fn acquire(&self) {
        let mut permits = self.permits.lock().unwrap();
        while *permits == 0 {
            permits = self.available.wait(permits).unwrap();
        }
        *permits -= 1;
    }

    /// Takes a permit if one is free, without blocking. Returns whether it was taken.
    fn try_acquire(&self) -> bool {
        let mut permits = self.permits.lock().unwrap();
        if *permits == 0 {
            false
        } else {
            *permits -= 1;
            true
        }
    }

    fn release(&self) {
        *self.permits.lock().unwrap() += 1;
        self.available.notify_one();
    }
}

/// Tracks outstanding worker threads so teardown can wait for them without holding join
/// handles (workers are detached; a long-lived subscription is joined via `wait` once it
/// observes the connection is dead).
#[derive(Clone, Default)]
struct WaitGroup {
    inner: Arc<(Mutex<usize>, Condvar)>,
}

impl WaitGroup {
    fn add(&self) {
        *self.inner.0.lock().unwrap() += 1;
    }

    fn done(&self) {
        let mut count = self.inner.0.lock().unwrap();
        *count -= 1;
        if *count == 0 {
            self.inner.1.notify_all();
        }
    }

    fn wait(&self) {
        let mut count = self.inner.0.lock().unwrap();
        while *count > 0 {
            count = self.inner.1.wait(count).unwrap();
        }
    }
}
