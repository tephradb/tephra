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
//! Producers push built [`pb::Response`]s onto one of two bounded channels drained by the writer:
//! a **control** lane (append acks, stats, standalone errors) and a **bulk** lane (read and
//! subscription event frames). The writer prioritizes control so a small ack never queues behind
//! megabytes of read response, with a bounded run so a sustained control stream cannot starve the
//! bulk lane. Each lane is independently bounded, so a slow client applies backpressure per lane
//! and frames never tear. A [`pb::CancelRequest`] flips a per-request flag that the read/subscribe
//! loops observe.

use std::collections::HashMap;
use std::fmt;
use std::io::{BufReader, BufWriter, Read, Write};
use std::mem;
use std::net::{Shutdown, SocketAddr, TcpStream};
use std::panic::{self, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use flume::{Receiver, Selector, Sender, TryRecvError};

use tephra::log::set::PositionRange;
use tephra::query::Query;
use tephra::read::WaitOutcome;
use tephra::writer::{AppendError, WriteHandle};
use tephra::{Event, Position};

#[cfg(feature = "tls")]
use rustls::{Connection, ServerConnection};
#[cfg(feature = "tls")]
use std::io::ErrorKind;
#[cfg(feature = "tls")]
use tephra_proto::TlsConn;
use tephra_proto::tephra as pb;
use tephra_proto::{FrameError, FramePoll, FrameReader, write_frame};

use crate::convert;
use crate::stats;
use crate::{ServerConfig, SharedStats};

/// The reply payload the coordinator sends back for one append, tagged with its `request_id`.
type AppendReply = (u64, Result<PositionRange, AppendError>);

/// Serves one connection until the client disconnects, the socket is shut down, or a transport
/// error occurs. Splits the socket into a read and a write half, arms the read-side timeout, and
/// hands both to [`serve_over`], which owns the reader/writer threads and the request loop. With a
/// TLS acceptor, the handshake and split happen in [`serve_tls`] first.
pub(crate) fn serve_connection(
    stream: TcpStream,
    handle: WriteHandle,
    config: ServerConfig,
    running: Arc<AtomicBool>,
    read_pool: Sender<ReadJob>,
    stats: &Arc<SharedStats>,
    #[cfg(feature = "tls")] tls: Option<Arc<rustls::ServerConfig>>,
) {
    let peer = stream.peer_addr().ok();
    if let Err(err) = stream.set_nodelay(true) {
        tracing::warn!(?peer, %err, "failed to set TCP_NODELAY");
    }

    #[cfg(feature = "tls")]
    if let Some(tls_config) = tls {
        serve_tls(
            stream, tls_config, handle, config, running, read_pool, stats,
        );
        return;
    }

    let conn_start = Instant::now();
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
    let writer_shutdown = stream.try_clone().ok();

    // Arm the read side with a poll interval when any connection timeout is enabled, so the reader
    // wakes periodically to enforce its deadlines. With all timeouts disabled it blocks normally and
    // the poll simply never yields a would-block.
    if let Some(timeout) = request_read_timeout(&config)
        && let Err(err) = read_half.set_read_timeout(Some(timeout))
    {
        tracing::warn!(?peer, %err, "failed to arm read timeout; this connection will not be reaped");
    }

    serve_over(
        Transport {
            reader: read_half,
            writer: write_half,
            shutdown: writer_shutdown,
            conn_start,
        },
        handle,
        config,
        running,
        read_pool,
        stats,
        peer,
    );
    let _ = stream.shutdown(Shutdown::Both);
}

/// The two halves of a split connection plus a raw-socket handle for shutdown. Bundled so
/// [`serve_over`] stays generic over the transport (plaintext or TLS) behind one parameter.
struct Transport<R, W> {
    reader: R,
    writer: W,
    /// A handle to the raw socket, used to `shutdown(Both)` on a transport failure.
    shutdown: Option<TcpStream>,
    /// When the connection was accepted, so the handshake deadline spans setup through the first
    /// frame as one budget (on TLS the handshake counts against it, matching the plaintext path).
    conn_start: Instant,
}

/// Runs the connection over an already-split transport: spawns the writer and append-pump threads,
/// then reads and dispatches request frames on this thread until the stream ends, and finally tears
/// everything down and joins. Generic over the read and write halves so the same logic serves a
/// plaintext `TcpStream` and a TLS-wrapped stream identically.
fn serve_over<R: Read, W: Write + Send + 'static>(
    transport: Transport<R, W>,
    handle: WriteHandle,
    config: ServerConfig,
    running: Arc<AtomicBool>,
    read_pool: Sender<ReadJob>,
    stats: &Arc<SharedStats>,
    peer: Option<SocketAddr>,
) {
    let Transport {
        reader: reader_half,
        writer: write_half,
        shutdown: writer_shutdown,
        conn_start,
    } = transport;

    let alive = Arc::new(AtomicBool::new(true));
    // Two egress lanes: a control lane the writer drains first (append acks, stats, errors) and the
    // deep bulk lane for read/subscription frames, each independently bounded. Control is sized to
    // hold the whole append-ack backlog, so a healthy client never blocks the pump there; it fills
    // only when a client stops reading its socket, where a blocking send is ordinary backpressure.
    let control_depth = config.max_inflight_requests_per_conn.max(CONTROL_QUEUE_MIN);
    let (control_tx, control_rx) = flume::bounded::<pb::Response>(control_depth);
    let (bulk_tx, bulk_rx) = flume::bounded::<pb::Response>(config.frame_queue_depth);
    let (reply_tx, reply_rx) = flume::unbounded::<AppendReply>();
    let cancels: Arc<Mutex<HashMap<u64, Arc<AtomicBool>>>> = Arc::default();
    // Appends and reads have separate budgets so an append never blocks the reader behind reads
    // (which would strand a cancel for those reads). Appends block on `append_inflight` (bounding the
    // reply backlog); reads take `read_inflight` without ever blocking the reader, and `read_overflow`
    // bounds reads that could not take a permit up front (they acquire it on a worker). Subscriptions
    // get a separate rejecting budget (see `spawn_subscribe`).
    let append_inflight = Arc::new(Semaphore::new(config.max_inflight_requests_per_conn));
    let read_inflight = Arc::new(Semaphore::new(config.max_inflight_requests_per_conn));
    let read_overflow = Arc::new(Semaphore::new(config.max_inflight_requests_per_conn));
    let subscriptions = Arc::new(Semaphore::new(config.max_concurrent_subscriptions));
    let workers = WaitGroup::default();

    let writer_thread = {
        let alive = Arc::clone(&alive);
        let shutdown = writer_shutdown;
        let max = config.max_frame_len;
        thread::Builder::new()
            .name("tephra-conn-writer".to_string())
            .spawn(move || writer_loop(write_half, control_rx, bulk_rx, shutdown, alive, max, peer))
            .expect("spawn connection writer thread")
    };

    let pump_thread = {
        let control_tx = control_tx.clone();
        let append_inflight = Arc::clone(&append_inflight);
        thread::Builder::new()
            .name("tephra-conn-pump".to_string())
            .spawn(move || pump_loop(reply_rx, control_tx, append_inflight))
            .expect("spawn connection pump thread")
    };

    let conn = ConnCtx {
        handle,
        config,
        running,
        alive: Arc::clone(&alive),
        control_tx: control_tx.clone(),
        bulk_tx: bulk_tx.clone(),
        cancels: Arc::clone(&cancels),
        workers: workers.clone(),
        append_inflight,
        read_inflight,
        read_overflow,
        subscriptions,
        stats: Arc::clone(stats),
        read_pool,
    };

    let mut reader = BufReader::new(reader_half);
    let mut frames = FrameReader::new();
    let mut established = false;
    let mut last_frame_at = conn_start;
    // When a partial frame is mid-flight, the instant it first stalled (for the incomplete-frame
    // deadline). Cleared whenever the connection is not mid-frame.
    let mut frame_started_at: Option<Instant> = None;
    loop {
        match frames.poll::<pb::Request, _>(&mut reader, config.max_frame_len) {
            Ok(FramePoll::Frame(request)) => {
                established = true;
                last_frame_at = Instant::now();
                frame_started_at = None;
                dispatch(&request, &conn, &reply_tx);
            }
            Ok(FramePoll::Eof) => {
                // Clean close at a frame boundary (the peer closed between frames).
                tracing::debug!(?peer, "connection closed by peer at a frame boundary");
                break;
            }
            Ok(FramePoll::Progress) => {
                // A byte of the current frame arrived: activity. Advance the idle clock so a
                // *progressing* frame is never idle-reaped, then bound the frame's completion time.
                if !conn.running.load(Ordering::Acquire) || !alive.load(Ordering::Acquire) {
                    break;
                }
                last_frame_at = Instant::now();
                if let Some(reason) = mid_frame_reason(
                    &config,
                    established,
                    conn_start,
                    last_frame_at,
                    &mut frame_started_at,
                ) {
                    reap_connection(&conn, peer, reason);
                    break;
                }
            }
            Ok(FramePoll::WouldBlock { in_progress: true }) => {
                // A partial frame is stalled (no byte within the poll interval). Bound it by the
                // incomplete-frame and (first-frame) handshake deadlines, plus idle_timeout: unlike
                // Progress this does not advance last_frame_at, so a stall of idle_timeout is reaped
                // even with the incomplete-frame timeout disabled. Never exempt: a stalled partial
                // frame is never legitimate, regardless of any concurrent subscription.
                if !conn.running.load(Ordering::Acquire) || !alive.load(Ordering::Acquire) {
                    break;
                }
                if let Some(reason) = mid_frame_reason(
                    &config,
                    established,
                    conn_start,
                    last_frame_at,
                    &mut frame_started_at,
                ) {
                    reap_connection(&conn, peer, reason);
                    break;
                }
            }
            Ok(FramePoll::WouldBlock { in_progress: false }) => {
                // Idle at a frame boundary. Compute the cheap deadline first; only consult
                // has_activity (which locks the permit budgets) once a deadline is breached, so the
                // common idle poll does no lock traffic. The handshake deadline covers the pre-first-
                // frame window; the idle deadline covers every silent gap after (and, since
                // last_frame_at starts at accept, a silent connection that never sends).
                if !conn.running.load(Ordering::Acquire) || !alive.load(Ordering::Acquire) {
                    break;
                }
                frame_started_at = None;
                let breached = if !established
                    && !config.handshake_timeout.is_zero()
                    && conn_start.elapsed() > config.handshake_timeout
                {
                    Some("handshake timeout")
                } else if !config.idle_timeout.is_zero()
                    && last_frame_at.elapsed() > config.idle_timeout
                {
                    Some("idle timeout")
                } else {
                    None
                };
                let reason = match breached {
                    // In-flight work or a live subscription counts as activity: not idle. Refresh
                    // the idle clock so we neither reap it nor re-lock every tick until the deadline
                    // lapses again. Activity only begins after a frame dispatches (which sets
                    // `established`), so this only ever exempts an idle breach on a busy connection,
                    // never a handshake breach.
                    Some(_) if conn.has_activity() => {
                        debug_assert!(
                            established,
                            "activity implies a frame already established the connection"
                        );
                        last_frame_at = Instant::now();
                        None
                    }
                    other => other,
                };
                if let Some(reason) = reason {
                    reap_connection(&conn, peer, reason);
                    break;
                }
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
                    // Best-effort on the control lane; the loop is breaking regardless.
                    let _ = control_tx.try_send(make_response(0, ResponseKind::Error(error)));
                }
                // A transport error (reset, broken pipe, torn frame) is not an orderly close;
                // surface it so a load-induced drop is not silent. A plain end-of-stream still
                // arrives as `Eof` above, so this only fires on a genuine failure.
                if alive.load(Ordering::Acquire) {
                    tracing::warn!(?peer, %err, "closing connection: reader failed");
                } else {
                    // The writer already marked the connection dead (its own transport error, or
                    // teardown), so this read error is just the reader observing that.
                    tracing::debug!(?peer, %err, "reader ended after the connection was closed");
                }
                break;
            }
        }
    }

    // Drain before closing so a queued response (e.g. a frame-error reply) still reaches the
    // client: mark dead, drop this thread's channel handles, wait for workers, then join and close.
    // The writer flushes the remainder on its own exit; a dead socket fails writes fast so the
    // joins don't hang (a slow-but-alive client is bounded by TCP keepalive / server shutdown).
    alive.store(false, Ordering::Release);
    drop(conn);
    drop(control_tx);
    drop(bulk_tx);
    drop(reply_tx);
    workers.wait();
    let _ = pump_thread.join();
    let _ = writer_thread.join();
}

/// Runs a connection over TLS: completes the handshake single-threaded on this thread (bounded by
/// the handshake deadline), then splits the session into a read and write half over independent
/// socket handles and hands them to [`serve_over`], exactly as the plaintext path does.
#[cfg(feature = "tls")]
fn serve_tls(
    mut stream: TcpStream,
    tls_config: Arc<rustls::ServerConfig>,
    handle: WriteHandle,
    config: ServerConfig,
    running: Arc<AtomicBool>,
    read_pool: Sender<ReadJob>,
    stats: &Arc<SharedStats>,
) {
    let peer = stream.peer_addr().ok();
    // From here (≈ accept), so the handshake deadline and the first-frame deadline are one budget,
    // matching the plaintext path's accept-to-first-frame `handshake_timeout`.
    let conn_start = Instant::now();
    let session = match ServerConnection::new(tls_config) {
        Ok(session) => session,
        Err(err) => {
            tracing::warn!(?peer, %err, "failed to start tls session");
            return;
        }
    };
    let mut conn: Connection = session.into();

    // Bound both directions of the handshake so a client that connects but never finishes it cannot
    // pin a connection thread: reads bound a stalled or slow-drip client, writes bound a peer that
    // stops reading our flight. The deadline spans from accept (`conn_start`) so the TLS handshake
    // counts against the same budget as the first frame does on the plaintext path.
    let deadline = handshake_deadline(&config);
    if let Err(err) = stream.set_read_timeout(Some(TIMEOUT_POLL_INTERVAL)) {
        tracing::warn!(?peer, %err, "failed to arm tls handshake read timeout");
    }
    if let Err(err) = stream.set_write_timeout(Some(TIMEOUT_POLL_INTERVAL)) {
        tracing::warn!(?peer, %err, "failed to arm tls handshake write timeout");
    }
    // The shared driver steps one socket syscall per iteration, re-checking `deadline` before each,
    // so neither a slow-drip reader nor a peer that stops reading our flight can run past it.
    if let Err(err) =
        tephra_proto::tls::drive_handshake(&mut conn, &mut stream, deadline, conn_start)
    {
        if err.kind() == ErrorKind::TimedOut {
            stats.connections_reaped.fetch_add(1, Ordering::Relaxed);
            tracing::debug!(?peer, "reaping connection on timeout: tls handshake");
        } else {
            tracing::debug!(?peer, %err, "tls handshake failed");
        }
        return;
    }

    // Handshake done. Clear the write timeout so the request-phase writer blocks on backpressure
    // rather than failing a slow client; socket options are shared across the clones below.
    if let Err(err) = stream.set_write_timeout(None) {
        tracing::warn!(?peer, %err, "failed to clear tls handshake write timeout");
    }

    // Independent socket handles for the reader and writer, plus a shutdown handle, so the blocking
    // reads and writes stay independent at the kernel level as they are on the plaintext path.
    let read_sock = match stream.try_clone() {
        Ok(clone) => clone,
        Err(err) => {
            tracing::warn!(?peer, %err, "failed to clone connection stream");
            return;
        }
    };
    let write_sock = match stream.try_clone() {
        Ok(clone) => clone,
        Err(err) => {
            tracing::warn!(?peer, %err, "failed to clone connection stream");
            return;
        }
    };
    let writer_shutdown = stream.try_clone().ok();

    // Arm the request-loop read timeout on the reader's own handle (a blocking read when no timeout
    // is enabled), matching the plaintext path.
    if let Err(err) = read_sock.set_read_timeout(request_read_timeout(&config)) {
        tracing::warn!(?peer, %err, "failed to arm read timeout; this connection will not be reaped");
    }

    let session = TlsConn::new(conn);
    let (reader, writer) = session.split(read_sock, write_sock);
    serve_over(
        Transport {
            reader,
            writer,
            shutdown: writer_shutdown,
            conn_start,
        },
        handle,
        config,
        running,
        read_pool,
        stats,
        peer,
    );
    let _ = stream.shutdown(Shutdown::Both);
}

/// The read-poll interval to arm on a connection's read side, or `None` (a blocking read) when no
/// connection timeout is enabled.
fn request_read_timeout(config: &ServerConfig) -> Option<Duration> {
    let enabled = !config.incomplete_frame_timeout.is_zero()
        || !config.handshake_timeout.is_zero()
        || !config.idle_timeout.is_zero();
    enabled.then_some(TIMEOUT_POLL_INTERVAL)
}

/// The wall-clock deadline for completing the TLS handshake, measured from accept. Prefers the
/// handshake timeout, then the incomplete-frame timeout (on by default), then the idle timeout, so
/// a silent or slow client is bounded whenever any of the three is configured (matching which
/// connections the plaintext reader reaps before a first frame). `None` only when all are disabled.
#[cfg(feature = "tls")]
fn handshake_deadline(config: &ServerConfig) -> Option<Duration> {
    [
        config.handshake_timeout,
        config.incomplete_frame_timeout,
        config.idle_timeout,
    ]
    .into_iter()
    .find(|timeout| !timeout.is_zero())
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
    /// The priority egress lane: append acks, stats, and errors not tied to an active stream.
    control_tx: Sender<pb::Response>,
    /// The bulk egress lane: read and subscription event frames.
    bulk_tx: Sender<pb::Response>,
    cancels: Arc<Mutex<HashMap<u64, Arc<AtomicBool>>>>,
    workers: WaitGroup,
    /// Blocking budget for in-flight appends (bounds the reply backlog). Separate from reads so an
    /// append never parks the reader behind reads holding permits.
    append_inflight: Arc<Semaphore>,
    /// Non-blocking budget for running reads: taken at admission, or deferred via `read_overflow`.
    read_inflight: Arc<Semaphore>,
    /// Rejecting budget bounding reads that could not take a `read_inflight` permit up front.
    read_overflow: Arc<Semaphore>,
    /// Rejecting budget for concurrent subscriptions.
    subscriptions: Arc<Semaphore>,
    /// Server-wide gauges and data-directory location, read by the stats op.
    stats: Arc<SharedStats>,
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

    /// Whether the connection is doing work that counts as activity for the idle reaper: an append
    /// or read in flight, or a live subscription. A pooled connection idling between uses has none
    /// of these, so it is a reaping candidate once its idle deadline passes.
    fn has_activity(&self) -> bool {
        !self.append_inflight.is_idle()
            || !self.read_inflight.is_idle()
            || !self.read_overflow.is_idle()
            || !self.subscriptions.is_idle()
    }

    /// Sends a control-lane frame. Blocking, like the append pump: a full control lane means the
    /// client has stopped reading its socket, so blocking here is ordinary backpressure (the lane is
    /// sized to hold the whole ack backlog, so a healthy client never fills it), not a disconnect.
    /// Independent of the bulk lane, so this never queues behind a large read.
    fn send_control(&self, response: pb::Response) {
        let _ = self.control_tx.send(response);
    }

    /// Sends a bad-request error on the control lane (the common shape across the reader-thread
    /// validation and admission-rejection paths).
    fn send_error(&self, request_id: u64, message: impl fmt::Display) {
        self.send_control(make_response(
            request_id,
            ResponseKind::Error(convert::bad_request(message)),
        ));
    }

    /// Sends a bulk-lane frame from a worker or subscription thread, blocking on a slow client
    /// (backpressure). Returns whether the frame was queued; `false` means the writer is gone.
    fn send_bulk(&self, response: pb::Response) -> bool {
        self.bulk_tx.send(response).is_ok()
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
        pb::request::KindOneof::Stats(_) => handle_stats(request_id, conn),
        // No kind set, or a future kind this server does not understand.
        _ => conn.send_error(
            request_id,
            "request has no append, read, subscribe, or cancel set",
        ),
    }
}

/// Answers a stats request inline on the reader thread: atomic gauges plus one stat of the data
/// directory, cheap enough not to warrant a worker.
fn handle_stats(request_id: u64, conn: &ConnCtx) {
    let snap = stats::gather(&conn.stats, &conn.handle);
    let mut stats = pb::StatsResponse::new();
    stats.set_event_count(snap.event_count);
    stats.set_segment_count(snap.segment_count);
    stats.set_disk_bytes(snap.disk_bytes);
    stats.set_uptime_seconds(snap.uptime_seconds);
    stats.set_active_connections(snap.active_connections);
    stats.set_active_subscriptions(snap.active_subscriptions);
    stats.set_connections_refused(snap.connections_refused);
    stats.set_connections_reaped(snap.connections_reaped);
    stats.set_max_connections(snap.max_connections);
    stats.set_version(snap.version.to_string());
    conn.send_control(make_response(request_id, ResponseKind::Stats(stats)));
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
            conn.send_error(request_id, err);
            return;
        }
    };
    let condition = match append.condition_opt() {
        Some(condition) => match convert::condition_from_proto(condition) {
            Ok(condition) => Some(condition),
            Err(err) => {
                conn.send_error(request_id, err);
                return;
            }
        },
        None => None,
    };

    conn.append_inflight.acquire();
    if let Err(err) = conn
        .handle
        .append_submit(events, condition, request_id, reply_tx.clone())
    {
        conn.append_inflight.release();
        conn.send_control(make_response(
            request_id,
            ResponseKind::Error(convert::append_error_to_proto(&err)),
        ));
    }
}

/// Validates the query, admits the read without blocking the reader, then queues it onto the shared
/// read pool. A read takes a `read_inflight` permit up front when one is free; otherwise it takes an
/// `read_overflow` slot and acquires its permit later on a pool worker, so the reader is never
/// parked on read admission (a read never strands a cancel behind it). A read past both budgets is
/// rejected. Appends and a non-draining client still backpressure the reader through their own paths.
fn spawn_read(request_id: u64, read: pb::ReadRequestView<'_>, conn: &ConnCtx) {
    let query = match convert::query_from_proto(read.query()) {
        Ok(query) => query,
        Err(err) => {
            conn.send_error(request_id, err);
            return;
        }
    };
    let reverse = read.reverse();
    // The cursor, taken verbatim: an exclusive lower bound (`after`) for a forward read, an
    // exclusive upper bound (`before`) for a backward one. The client sends the real position in
    // both directions (a "from the tip" backward read sends `Position::MAX.get()`), so there is
    // no sentinel to remap here: `0` means the same as it does embedded (from the start forward,
    // nothing backward), keeping the wire and embedded paths in lockstep.
    let cursor = Position::new(read.after());
    // Explicit presence: absent means unlimited, present (even 0) is a real cap.
    let limit = read.limit_opt();

    // Admit without blocking the reader: a free permit runs immediately, otherwise an overflow slot
    // defers the permit acquire to the worker, and a read past both budgets is rejected.
    let admission = match conn.read_inflight.try_acquire_guard() {
        Some(permit) => Admission::Permitted(permit),
        None => match conn.read_overflow.try_acquire_guard() {
            Some(slot) => Admission::Overflow { slot },
            None => {
                conn.send_error(request_id, "too many in-flight reads on this connection");
                return;
            }
        },
    };

    let cancel = register_cancel(conn, request_id);
    conn.workers.add();
    let cleanup = WorkerCleanup {
        cancels: Arc::clone(&conn.cancels),
        sem: None,
        workers: conn.workers.clone(),
        request_id,
    };
    let job = ReadJob {
        request_id,
        query,
        cursor,
        reverse,
        limit,
        conn: conn.clone(),
        cancel,
        cleanup,
        admission,
    };
    if conn.read_pool.send(job).is_err() {
        // The pool is gone, which only happens once the server is shutting down. The job (and with
        // it `WorkerCleanup` and the admission guard) is dropped by the failed send, releasing its
        // permit/slot/worker/cancel. No error frame: teardown is already underway.
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
            conn.send_error(request_id, err);
            return;
        }
    };
    let after = Position::new(subscribe.after());

    if !conn.subscriptions.try_acquire() {
        conn.send_error(
            request_id,
            "too many concurrent subscriptions on this connection",
        );
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
    // Tracks the live-subscription gauge for the whole life of the worker (including an unwind
    // or a failed spawn, where the guard drops without ever running).
    let gauge = SubGauge::new(Arc::clone(&conn.stats));
    let conn_owned = conn.clone();
    if let Err(err) = thread::Builder::new()
        .name("tephra-conn-subscribe".to_string())
        .spawn(move || {
            let _cleanup = cleanup;
            let _gauge = gauge;
            run_subscribe(request_id, query, after, &conn_owned, &cancel);
        })
    {
        tracing::warn!(%err, "failed to spawn subscribe worker");
        conn.send_error(request_id, "server could not start the subscription");
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
    cursor: Position,
    reverse: bool,
    limit: Option<u64>,
    conn: &ConnCtx,
    cancel: &AtomicBool,
) {
    let mut reads = if reverse {
        conn.handle.read_back(query, cursor, limit)
    } else {
        conn.handle.read(query, cursor, limit)
    };
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
                conn.send_bulk(make_response(
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
            if !conn.send_bulk(make_response(request_id, ResponseKind::ReadEvents(full))) {
                return;
            }
            batch_bytes = 0;
        }
    }

    if !batch.events().is_empty()
        && !conn.send_bulk(make_response(request_id, ResponseKind::ReadEvents(batch)))
    {
        return;
    }

    let mut end = pb::ReadEnd::new();
    end.set_watermark(watermark.get());
    conn.send_bulk(make_response(request_id, ResponseKind::ReadEnd(end)));
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
                conn.send_bulk(make_response(
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
                if !conn.send_bulk(make_response(request_id, ResponseKind::CaughtUp(caught_up))) {
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
            if !conn.send_bulk(make_response(request_id, ResponseKind::ReadEvents(full))) {
                return Err(());
            }
            batch_bytes = 0;
        }
    }
    if !batch.events().is_empty()
        && !conn.send_bulk(make_response(request_id, ResponseKind::ReadEvents(batch)))
    {
        return Err(());
    }
    Ok(())
}

/// Floor for the control egress lane depth. The lane is sized to `max(append budget, this)` so it
/// can hold the whole append-ack backlog without blocking the pump on a healthy client, while still
/// giving small deployments a reasonable buffer for reader-thread acks and errors.
const CONTROL_QUEUE_MIN: usize = 64;

/// Most consecutive control frames the writer emits while bulk is pending before forcing one bulk
/// frame. Bounds how long a sustained ack stream can starve a read sharing the connection while
/// keeping ack latency effectively unchanged.
const MAX_CONTROL_RUN: usize = 64;

/// How often an overflow read parked on a permit re-checks its cancel flag (see
/// [`Semaphore::acquire_guard_or_cancel`]). Bounds the delay before a cancelled, permit-starved read
/// gives its worker back.
const CANCEL_POLL_INTERVAL: Duration = Duration::from_millis(25);

/// How often the connection reader wakes from a blocked read to re-check its timeout deadlines.
/// Reaping is enforced within one interval of the configured deadline.
const TIMEOUT_POLL_INTERVAL: Duration = Duration::from_secs(1);

/// The reap reason for a connection that is mid-frame (a byte just arrived, or a partial frame is
/// stalled). Bounds the frame's completion time (`incomplete_frame_timeout`, from its first byte),
/// the first frame against `handshake_timeout`, and a stall against `idle_timeout` (the caller does
/// not advance `last_frame_at` while stalled, so an idle-length stall trips this even with the
/// incomplete-frame timeout disabled). No activity exemption: a stalled partial frame is never
/// legitimate. Stamps `frame_started_at` at the first byte of the frame.
fn mid_frame_reason(
    config: &ServerConfig,
    established: bool,
    conn_start: Instant,
    last_frame_at: Instant,
    frame_started_at: &mut Option<Instant>,
) -> Option<&'static str> {
    let started = *frame_started_at.get_or_insert_with(Instant::now);
    if !config.incomplete_frame_timeout.is_zero()
        && started.elapsed() > config.incomplete_frame_timeout
    {
        return Some("incomplete frame");
    }
    if !established
        && !config.handshake_timeout.is_zero()
        && conn_start.elapsed() > config.handshake_timeout
    {
        return Some("handshake timeout");
    }
    if !config.idle_timeout.is_zero() && last_frame_at.elapsed() > config.idle_timeout {
        return Some("idle timeout");
    }
    None
}

/// Counts and logs a connection reaped for exceeding a timeout.
fn reap_connection(conn: &ConnCtx, peer: Option<SocketAddr>, reason: &'static str) {
    conn.stats
        .connections_reaped
        .fetch_add(1, Ordering::Relaxed);
    tracing::debug!(?peer, reason, "reaping connection on timeout");
}

/// The writer thread: drains the two egress lanes and writes them, prioritizing control so a small
/// ack never queues behind a large read. On a transport failure it marks the connection dead and
/// shuts the socket, unblocking the reader and any parked worker.
fn writer_loop<W: Write>(
    write_half: W,
    control_rx: Receiver<pb::Response>,
    bulk_rx: Receiver<pb::Response>,
    shutdown: Option<TcpStream>,
    alive: Arc<AtomicBool>,
    max_frame_len: u32,
    peer: Option<SocketAddr>,
) {
    let mut writer = BufWriter::new(write_half);
    let outcome = drive_writer(&mut writer, &control_rx, &bulk_rx, max_frame_len);
    // A write failure closes the connection under the client; name it so the drop is not silent.
    // `alive` still set here means the writer is the half that observed the failure first.
    match &outcome {
        // A clean exit (both lanes drained and disconnected): flush so a TLS transport drains any
        // session output the read half produced (a fatal alert on a rejected record) before the
        // socket is shut down. On plaintext this is a spent no-op. Skip it after a write failure:
        // the socket is already broken, and re-flushing would re-encrypt bytes a TLS write half had
        // partially consumed into the session, duplicating records on the wire.
        Ok(()) => {
            let _ = writer.flush();
        }
        Err(err) if alive.load(Ordering::Acquire) => {
            tracing::warn!(?peer, %err, "closing connection: writer failed");
        }
        Err(err) => {
            tracing::debug!(?peer, %err, "writer ended after the connection was closed");
        }
    }
    // Any exit means the output side is done: mark dead and wake the rest of the connection.
    alive.store(false, Ordering::Release);
    if let Some(stream) = shutdown {
        let _ = stream.shutdown(Shutdown::Both);
    }
}

/// Drains the control and bulk lanes onto `writer`, control-first with a bulk-liveness escape
/// valve. Generic over the sink so it can be exercised against an in-memory buffer in tests.
/// Returns `Ok(())` when both lanes have disconnected and drained, or the first write error.
fn drive_writer<W: Write>(
    writer: &mut W,
    control_rx: &Receiver<pb::Response>,
    bulk_rx: &Receiver<pb::Response>,
    max_frame_len: u32,
) -> Result<(), FrameError> {
    loop {
        // Control first, but cap the run at MAX_CONTROL_RUN while bulk is pending, so a client
        // appending at full rate cannot starve a read sharing the connection.
        let mut control_written = 0;
        let mut wrote_control = false;
        loop {
            if control_written >= MAX_CONTROL_RUN && !bulk_rx.is_empty() {
                break;
            }
            match control_rx.try_recv() {
                Ok(response) => {
                    write_frame(writer, &response, max_frame_len)?;
                    wrote_control = true;
                    control_written += 1;
                }
                Err(_) => break,
            }
        }
        if wrote_control {
            writer.flush().map_err(FrameError::Io)?;
        }

        // One bulk frame: normal priority, or the forced frame after a full control run.
        match bulk_rx.try_recv() {
            Ok(response) => {
                write_frame(writer, &response, max_frame_len)?;
                // Coalesce: flush only when nothing else is immediately queued.
                if bulk_rx.is_empty() && control_rx.is_empty() {
                    writer.flush().map_err(FrameError::Io)?;
                }
                continue;
            }
            Err(TryRecvError::Empty) | Err(TryRecvError::Disconnected) => {}
        }
        if wrote_control {
            continue;
        }

        // Both lanes idle: flush and block on whichever is still live.
        writer.flush().map_err(FrameError::Io)?;
        let next = match (!control_rx.is_disconnected(), !bulk_rx.is_disconnected()) {
            (true, true) => Selector::new()
                .recv(control_rx, |r| r.ok())
                .recv(bulk_rx, |r| r.ok())
                .wait(),
            (true, false) => control_rx.recv().ok(),
            (false, true) => bulk_rx.recv().ok(),
            (false, false) => return Ok(()),
        };
        match next {
            Some(response) => write_frame(writer, &response, max_frame_len)?,
            None => continue,
        }
    }
}

/// The append completion pump: turns each durable reply into a response frame and releases the
/// append's in-flight permit. Exits when the reply channel closes (all appends done and the
/// reader gone) or the writer has gone away.
fn pump_loop(
    reply_rx: Receiver<AppendReply>,
    control_tx: Sender<pb::Response>,
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
        // A blocking send off the reader thread: a full control lane backpressures the pump (and so
        // the in-flight budget) without ever parking the reader. Send before releasing, so the bound
        // stays tight.
        let sent = control_tx.send(response);
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
    Stats(pb::StatsResponse),
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
        ResponseKind::Stats(stats) => response.set_stats(stats),
        ResponseKind::Error(error) => response.set_error(error),
    }
    response
}

// ---------------------------------------------------------------------------
// Read-worker pool
// ---------------------------------------------------------------------------

/// One queued read, carrying everything [`run_read`] needs so a pool worker can run it without
/// borrowing the connection. `admission` owns the in-flight permit (or the overflow slot pending
/// one); `cleanup` rides along and drops when the job finishes (or a worker unwinds), releasing the
/// cancel entry and the worker count exactly once.
pub(crate) struct ReadJob {
    request_id: u64,
    query: Query,
    /// The pagination cursor: an exclusive lower bound (`after`) forward, an exclusive upper
    /// bound (`before`) backward. Its meaning is set by `reverse`.
    cursor: Position,
    reverse: bool,
    limit: Option<u64>,
    conn: ConnCtx,
    cancel: Arc<AtomicBool>,
    cleanup: WorkerCleanup,
    admission: Admission,
}

/// A read's admission to run: either it took a `read_inflight` permit up front, or it holds an
/// overflow slot and must acquire its permit on the worker before running.
enum Admission {
    Permitted(Permit),
    Overflow { slot: Permit },
}

impl ReadJob {
    fn run(self) {
        let ReadJob {
            request_id,
            query,
            cursor,
            reverse,
            limit,
            conn,
            cancel,
            cleanup,
            admission,
        } = self;
        // Acquire the permit if it was deferred (blocking here on the worker, never the reader),
        // watching the cancel flag so a cancelled read waiting on a permit gives its worker back
        // promptly instead of parking until an unrelated read finishes. Acquiring frees the overflow
        // slot: the read now counts against the in-flight budget instead.
        //
        // This parks a shared pool worker while it waits. Harmless at the default
        // `max_inflight_requests_per_conn` (>= the worker count): the FIFO drains all of a
        // connection's permit-holders before any of its overflow reads is pulled, so a worker never
        // blocks here with a permit available. Only a deployment configuring the budget below the
        // pool size could tie up workers this way.
        let _permit = match admission {
            Admission::Permitted(permit) => permit,
            Admission::Overflow { slot } => {
                match conn.read_inflight.acquire_guard_or_cancel(&cancel) {
                    Some(permit) => {
                        drop(slot);
                        permit
                    }
                    // Cancelled while waiting: drop the slot and cleanup, run nothing.
                    None => return,
                }
            }
        };
        // A cancel that landed while the job waited for a permit stops it before any work.
        if cancel.load(Ordering::Acquire) {
            return;
        }
        run_read(request_id, &query, cursor, reverse, limit, &conn, &cancel);
        // Release the cancel/worker only once the read's frames are all queued; `_permit` drops here.
        drop(cleanup);
    }
}

/// A shared, server-wide pool of reusable worker threads that stream reads. Created once at
/// startup, so a read pays no per-request thread-creation cost. One unbounded MPMC channel feeds
/// all workers: a sent [`ReadJob`] wakes whichever worker is idle, which runs it to completion
/// and returns for the next (the reads themselves, not the queue, are the bottleneck; the
/// per-connection in-flight plus overflow budgets already bound how many jobs can be outstanding).
/// Subscriptions keep their own dedicated threads and never use this pool.
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
/// deregisters the cancel flag, returns its budget permit (subscriptions only; a read's permit is
/// owned by its [`Admission`]), and marks the worker done.
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

/// Holds the server-wide live-subscription gauge up for one subscription: increments on
/// construction and decrements on drop, so the count is right across an unwind or a failed spawn.
struct SubGauge(Arc<SharedStats>);

impl SubGauge {
    fn new(stats: Arc<SharedStats>) -> SubGauge {
        stats.active_subscriptions.fetch_add(1, Ordering::Relaxed);
        SubGauge(stats)
    }
}

impl Drop for SubGauge {
    fn drop(&mut self) {
        self.0.active_subscriptions.fetch_sub(1, Ordering::Relaxed);
    }
}

/// A counting semaphore bounding a per-connection budget. Used three ways: `acquire` (blocking) and
/// `try_acquire` (rejecting) for the raw count, and the guard variants that tie a permit's release
/// to a [`Permit`]'s drop.
struct Semaphore {
    permits: Mutex<usize>,
    available: Condvar,
    capacity: usize,
}

impl Semaphore {
    fn new(permits: usize) -> Semaphore {
        // At least one, so a misconfigured zero cannot wedge the connection.
        let capacity = permits.max(1);
        Semaphore {
            permits: Mutex::new(capacity),
            available: Condvar::new(),
            capacity,
        }
    }

    /// Whether every permit is free, i.e. nothing is currently using this budget. Used by the
    /// idle-connection reaper to treat in-flight work as activity.
    fn is_idle(&self) -> bool {
        *self.permits.lock().unwrap() == self.capacity
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

    /// Non-blocking [`try_acquire`](Self::try_acquire) returning an RAII [`Permit`] on success.
    fn try_acquire_guard(self: &Arc<Self>) -> Option<Permit> {
        self.try_acquire().then(|| Permit(Arc::clone(self)))
    }

    /// Blocks for a permit, returning an RAII [`Permit`], but gives up (returns `None`) if `cancel`
    /// is set. Re-checks `cancel` on a fixed poll so a cancel that lands while parked is observed
    /// even with no permit release to wake it, so a cancelled overflow read yields its worker
    /// promptly rather than parking until some unrelated read finishes.
    fn acquire_guard_or_cancel(self: &Arc<Self>, cancel: &AtomicBool) -> Option<Permit> {
        let mut permits = self.permits.lock().unwrap();
        loop {
            if cancel.load(Ordering::Acquire) {
                // If a permit is available we are declining, hand it to another waiter rather than
                // strand it (this waiter may have consumed the release's `notify_one`).
                if *permits > 0 {
                    self.available.notify_one();
                }
                return None;
            }
            if *permits > 0 {
                *permits -= 1;
                return Some(Permit(Arc::clone(self)));
            }
            let (guard, _timeout) = self
                .available
                .wait_timeout(permits, CANCEL_POLL_INTERVAL)
                .unwrap();
            permits = guard;
        }
    }
}

/// An RAII permit from a [`Semaphore`]: releases its permit when dropped. Used for the read
/// in-flight budget and the read-overflow budget, so a dropped read job (cancel, pool shutdown)
/// returns exactly what it held with no leak or over-release.
struct Permit(Arc<Semaphore>);

impl Drop for Permit {
    fn drop(&mut self) {
        self.0.release();
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

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use tephra_proto::read_frame;

    use super::*;

    /// Control-lane frame carrying `id` (an append ack stands in for any small control response).
    fn control(id: u64) -> pb::Response {
        make_response(id, ResponseKind::Append(pb::AppendResponse::new()))
    }

    /// Bulk-lane frame carrying `id` (an empty `ReadEvents` stands in for a read batch).
    fn bulk(id: u64) -> pb::Response {
        make_response(id, ResponseKind::ReadEvents(pb::ReadEvents::new()))
    }

    /// Decodes the request ids of every frame written by [`drive_writer`], in order.
    fn decode_ids(bytes: &[u8]) -> Vec<u64> {
        let mut cursor = Cursor::new(bytes);
        let mut ids = Vec::new();
        while let Some(resp) = read_frame::<pb::Response, _>(&mut cursor, 1 << 20).unwrap() {
            ids.push(resp.request_id());
        }
        ids
    }

    /// Drives the writer over two pre-loaded, then disconnected, lanes so it drains and returns.
    /// Control ids are tagged `>= 1000`, bulk ids `< 1000`.
    fn run(control_frames: &[u64], bulk_frames: &[u64]) -> Vec<u64> {
        let (control_tx, control_rx) = flume::bounded::<pb::Response>(control_frames.len().max(1));
        let (bulk_tx, bulk_rx) = flume::bounded::<pb::Response>(bulk_frames.len().max(1));
        for id in bulk_frames {
            bulk_tx.send(bulk(*id)).unwrap();
        }
        for id in control_frames {
            control_tx.send(control(1000 + *id)).unwrap();
        }
        drop(control_tx);
        drop(bulk_tx);
        let mut out = Vec::new();
        drive_writer(&mut out, &control_rx, &bulk_rx, 1 << 20).unwrap();
        decode_ids(&out)
    }

    #[test]
    fn control_frames_are_written_before_queued_bulk() {
        // Three bulk frames queued first, then three control: control still egresses first, so a
        // small ack never waits behind queued read frames.
        let ids = run(&[0, 1, 2], &[0, 1, 2]);
        assert_eq!(ids.len(), 6);
        let first_bulk = ids.iter().position(|id| *id < 1000).unwrap();
        assert_eq!(first_bulk, 3, "all queued control drains before any bulk");
        assert!(ids[first_bulk..].iter().all(|id| *id < 1000));
    }

    #[test]
    fn a_sustained_control_stream_cannot_starve_bulk() {
        // Far more than MAX_CONTROL_RUN control frames with bulk pending: the escape valve forces a
        // bulk frame out within MAX_CONTROL_RUN + 1 writes rather than starving the read.
        let control_frames: Vec<u64> = vec![0; MAX_CONTROL_RUN * 3];
        let ids = run(&control_frames, &[0, 1]);
        let first_bulk = ids
            .iter()
            .position(|id| *id < 1000)
            .expect("a bulk frame must be written");
        assert!(
            first_bulk <= MAX_CONTROL_RUN,
            "bulk starved: first bulk at {first_bulk}, cap {MAX_CONTROL_RUN}",
        );
    }

    #[test]
    fn a_cancelled_overflow_acquire_gives_up_without_a_permit() {
        // An overflow read parked on an exhausted budget must return promptly once cancelled, so its
        // pool worker is freed instead of waiting for an unrelated read to release a permit.
        let sem = Arc::new(Semaphore::new(1));
        let _held = sem.try_acquire_guard().expect("the sole permit");
        let cancel = Arc::new(AtomicBool::new(false));
        let waiter = {
            let sem = Arc::clone(&sem);
            let cancel = Arc::clone(&cancel);
            // Time how long the waiter takes to observe the cancel and return.
            thread::spawn(move || {
                let started = std::time::Instant::now();
                let gave_up = sem.acquire_guard_or_cancel(&cancel).is_none();
                (gave_up, started.elapsed())
            })
        };
        thread::sleep(Duration::from_millis(10));
        cancel.store(true, Ordering::Release);
        let (gave_up, waited) = waiter.join().unwrap();
        assert!(gave_up, "a cancelled waiter returns None, not a permit");
        // Bounded by the poll interval (plus slack), so a regression removing the timeout or making
        // it seconds-long fails here rather than silently passing.
        assert!(
            waited < CANCEL_POLL_INTERVAL * 8,
            "cancel took {waited:?}, expected within a few poll intervals",
        );
    }

    #[test]
    fn an_overflow_acquire_takes_a_released_permit() {
        // The uncancelled path still blocks for and takes a permit once one is freed.
        let sem = Arc::new(Semaphore::new(1));
        let held = sem.try_acquire_guard().expect("the sole permit");
        let cancel = Arc::new(AtomicBool::new(false));
        let waiter = {
            let sem = Arc::clone(&sem);
            let cancel = Arc::clone(&cancel);
            thread::spawn(move || sem.acquire_guard_or_cancel(&cancel).is_some())
        };
        thread::sleep(Duration::from_millis(10));
        drop(held);
        assert!(waiter.join().unwrap(), "a waiter takes the released permit",);
    }
}
