//! A synchronous, thread-per-connection TCP server exposing a [`tephra`] event store over the
//! length-prefixed protobuf protocol defined in [`tephra_proto`].
//!
//! The model mirrors the database: tephra is single-writer and synchronous
//! ([`WriteHandle::append`](tephra::writer::WriteHandle::append) blocks until durable, reads
//! run on the caller's own thread over a lock-free snapshot), so each connection is served on
//! its own OS thread that blocks on the socket and calls straight into the shared handles. No
//! async runtime, no thread pool.
//!
//! # Usage
//!
//! ```no_run
//! use std::net::TcpListener;
//! use tephra::log::set::{SegmentConfig, SegmentSet};
//! use tephra::writer::{WriteCoordinator, WriterConfig};
//! use tephra_server::{Server, ServerConfig};
//!
//! let set = SegmentSet::open("data", SegmentConfig::new(16 * 1024 * 1024)).unwrap();
//! let (coordinator, handle) = WriteCoordinator::start(set, WriterConfig::default()).unwrap();
//! let server = Server::bind("127.0.0.1:9000", handle, ServerConfig::default()).unwrap();
//! let shutdown = server.shutdown_handle();
//! // ... install a signal handler that calls `shutdown.shutdown()` ...
//! server.run().unwrap();
//! coordinator.shutdown();
//! ```

mod conn;
mod convert;
#[cfg(feature = "metrics")]
mod metrics;
mod stats;

use std::collections::HashMap;
use std::io;
use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream, ToSocketAddrs};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use socket2::{SockRef, TcpKeepalive};

use tephra::writer::WriteHandle;
use tephra_proto::DEFAULT_MAX_FRAME_LEN;

pub use convert::ConvertError;

/// Tuning for the server.
#[derive(Clone, Copy, Debug)]
pub struct ServerConfig {
    /// Largest single frame accepted or produced, in bytes. Bounds per-frame memory and
    /// rejects a hostile length before allocating for it.
    pub max_frame_len: u32,
    /// A streamed read (or subscription) is flushed as a frame once it holds this many events.
    pub read_batch_events: usize,
    /// A streamed read (or subscription) is flushed as a frame once its buffered events reach
    /// this many bytes.
    pub read_batch_bytes: usize,
    /// How often an idle subscription's blocking wait wakes to re-check server shutdown. Keeps
    /// a subscription with no events flowing responsive to `shutdown` without a heartbeat
    /// frame.
    pub subscribe_wait_tick: Duration,
    /// Per-connection in-flight budget, applied separately to appends and reads. Appends: this many
    /// may be awaiting their durable reply before the reader blocks (backpressure, bounding the
    /// reply backlog). Reads: this many may run concurrently, plus this many more may queue for a
    /// slot without ever blocking the reader, before a further read is rejected (so a read never
    /// strands a cancel behind it; an append at its own cap, or a client that has stopped reading,
    /// still backpressures the reader). Subscriptions are budgeted separately.
    pub max_inflight_requests_per_conn: usize,
    /// Most live subscriptions a single connection may hold at once. A subscription over the
    /// limit is rejected with an error (rather than blocking the reader, since a long-lived
    /// subscription's permit could only be freed by a cancel the blocked reader could not read).
    pub max_concurrent_subscriptions: usize,
    /// Number of reusable worker threads in the shared, server-wide pool that streams reads.
    /// `0` means auto: one worker per logical CPU ([`std::thread::available_parallelism`]).
    /// Warm reads are short and CPU-bound, so one worker per core reaches the read-parallelism
    /// ceiling without oversubscription; raise it for deployments dominated by slow-client
    /// streaming reads, where workers can park on a backpressured send.
    pub read_worker_threads: usize,
    /// Depth of a connection's outbound **bulk** frame queue (read and subscription frames).
    /// Bounds buffered response frames, so a slow client applies backpressure to the workers
    /// producing them. Small control frames (append acks, stats, errors) use a separate, priority
    /// lane so they never queue behind a large read.
    pub frame_queue_depth: usize,
    /// TCP keepalive idle time before the first probe on an accepted connection. The OS
    /// default (~2h on Linux) is too long to reap a silently-dead subscription promptly, so it
    /// is set explicitly.
    pub keepalive_idle: Duration,
    /// Interval between TCP keepalive probes once they start.
    pub keepalive_interval: Duration,
    /// Most connections served at once, across all clients. Each connection costs several OS
    /// threads (reader, writer, append pump, plus one per live subscription), so an unbounded
    /// client fleet would otherwise exhaust threads, file descriptors, and memory. A connection
    /// accepted over this cap is closed immediately, before any request is read. `0` means
    /// unlimited (an explicit operator opt-out; the per-connection budgets still apply).
    pub max_connections: usize,
}

impl Default for ServerConfig {
    fn default() -> Self {
        ServerConfig {
            max_frame_len: DEFAULT_MAX_FRAME_LEN,
            read_batch_events: 1024,
            read_batch_bytes: 512 * 1024,
            subscribe_wait_tick: Duration::from_millis(250),
            max_inflight_requests_per_conn: 256,
            max_concurrent_subscriptions: 64,
            read_worker_threads: 0,
            frame_queue_depth: 256,
            keepalive_idle: Duration::from_secs(60),
            keepalive_interval: Duration::from_secs(15),
            max_connections: 1024,
        }
    }
}

/// A registry of live connection sockets, so a shutdown can unblock threads parked on a
/// blocking read. Each connection removes itself on exit, so the map does not grow unbounded.
///
/// Registration and shutdown share one lock and a sticky `shutting_down` flag, so there is no
/// window in which a connection is served but missed by [`shutdown_all`](Connections::shutdown_all):
/// a connection registered before shutdown is woken by it, and one that arrives after is
/// refused.
#[derive(Clone, Default)]
struct Connections {
    inner: Arc<Mutex<ConnectionsInner>>,
}

#[derive(Default)]
struct ConnectionsInner {
    shutting_down: bool,
    streams: HashMap<u64, TcpStream>,
}

impl Connections {
    /// Registers a live connection so shutdown can wake it. Returns `false` if shutdown has
    /// already begun, in which case the stream is shut down here and the caller must not serve
    /// it.
    fn register(&self, id: u64, stream: TcpStream) -> bool {
        let mut inner = self.inner.lock().unwrap();
        if inner.shutting_down {
            let _ = stream.shutdown(Shutdown::Both);
            return false;
        }
        inner.streams.insert(id, stream);
        true
    }

    fn remove(&self, id: u64) {
        self.inner.lock().unwrap().streams.remove(&id);
    }

    /// Marks shutdown and wakes every registered connection. After this, [`register`](Connections::register)
    /// refuses new connections, so none can be served without being woken.
    fn shutdown_all(&self) {
        let mut inner = self.inner.lock().unwrap();
        inner.shutting_down = true;
        for stream in inner.streams.values() {
            // Either half erroring is fine; the goal is only to wake a parked read.
            let _ = stream.shutdown(Shutdown::Both);
        }
    }
}

// Server-wide state sampled by the stats op: the data directory (stat'd for segment count and
// disk usage), the start instant for uptime, the live connection/subscription gauges, the
// running total of refused connections, and the configured connection cap. Shared behind an
// `Arc` and read with plain atomic loads.
pub(crate) struct SharedStats {
    data_dir: Option<PathBuf>,
    start_time: Instant,
    active_connections: AtomicU64,
    active_subscriptions: AtomicU64,
    connections_refused: AtomicU64,
    max_connections: u64,
}

impl SharedStats {
    fn new(data_dir: Option<PathBuf>, max_connections: u64) -> SharedStats {
        SharedStats {
            data_dir,
            start_time: Instant::now(),
            active_connections: AtomicU64::new(0),
            active_subscriptions: AtomicU64::new(0),
            connections_refused: AtomicU64::new(0),
            max_connections,
        }
    }
}

// Holds a connection slot for the life of one connection: the `active_connections` gauge is the
// cap counter, so acquiring both enforces `max_connections` and drives the gauge. Increments on
// a successful acquire and decrements on drop, so an unwind out of `serve_connection` (or a
// failed thread spawn) cannot inflate the gauge or leak a slot.
struct ConnPermit(Arc<SharedStats>);

impl ConnPermit {
    /// Takes a connection slot if the server is under `max_connections`, returning `None` when at
    /// capacity (`0` means unlimited). The accept loop is the sole incrementer, so the
    /// load-then-add cannot overshoot the cap: concurrent drops only lower the count, and no other
    /// thread raises it.
    fn acquire(stats: &Arc<SharedStats>) -> Option<ConnPermit> {
        let max = stats.max_connections;
        if max != 0 && stats.active_connections.load(Ordering::Relaxed) >= max {
            return None;
        }
        stats.active_connections.fetch_add(1, Ordering::Relaxed);
        Some(ConnPermit(Arc::clone(stats)))
    }
}

impl Drop for ConnPermit {
    fn drop(&mut self) {
        self.0.active_connections.fetch_sub(1, Ordering::Relaxed);
    }
}

/// A bound server, ready to [`run`](Server::run).
pub struct Server {
    listener: TcpListener,
    handle: WriteHandle,
    config: ServerConfig,
    local_addr: SocketAddr,
    running: Arc<AtomicBool>,
    connections: Connections,
    data_dir: Option<PathBuf>,
    #[cfg(feature = "metrics")]
    metrics_listener: Option<TcpListener>,
    #[cfg(feature = "metrics")]
    metrics_addr: Option<SocketAddr>,
}

impl Server {
    /// Binds a listener on `addr` and prepares to serve `handle`. Does not start accepting
    /// until [`run`](Server::run) is called.
    pub fn bind(
        addr: impl ToSocketAddrs,
        handle: WriteHandle,
        config: ServerConfig,
    ) -> io::Result<Server> {
        let listener = TcpListener::bind(addr)?;
        let local_addr = listener.local_addr()?;
        Ok(Server {
            listener,
            handle,
            config,
            local_addr,
            running: Arc::new(AtomicBool::new(true)),
            connections: Connections::default(),
            data_dir: None,
            #[cfg(feature = "metrics")]
            metrics_listener: None,
            #[cfg(feature = "metrics")]
            metrics_addr: None,
        })
    }

    /// Records the data directory the store lives in, so the stats op can report the on-disk
    /// segment count and byte usage. Without it, those fields report zero (the store still
    /// serves normally). The standalone binary always sets this.
    pub fn with_data_dir(mut self, data_dir: impl Into<PathBuf>) -> Server {
        self.data_dir = Some(data_dir.into());
        self
    }

    /// Binds a Prometheus `/metrics` HTTP endpoint on `addr`, served on its own thread and port
    /// (separate from the data protocol). Bound eagerly so [`metrics_local_addr`](Self::metrics_local_addr)
    /// is known before [`run`](Self::run); the endpoint starts serving when `run` is called.
    #[cfg(feature = "metrics")]
    pub fn with_metrics_addr(mut self, addr: impl ToSocketAddrs) -> io::Result<Server> {
        let listener = TcpListener::bind(addr)?;
        self.metrics_addr = Some(listener.local_addr()?);
        self.metrics_listener = Some(listener);
        Ok(self)
    }

    /// The address the metrics endpoint is bound to, if one was configured (useful when binding
    /// to port 0).
    #[cfg(feature = "metrics")]
    pub fn metrics_local_addr(&self) -> Option<SocketAddr> {
        self.metrics_addr
    }

    /// The address the listener is bound to (useful when binding to port 0).
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// A handle that stops the accept loop and unblocks in-flight connections. It can be
    /// moved to another thread (for example a signal handler).
    pub fn shutdown_handle(&self) -> ShutdownHandle {
        ShutdownHandle {
            running: Arc::clone(&self.running),
            local_addr: self.local_addr,
            connections: self.connections.clone(),
        }
    }

    /// Runs the accept loop until shutdown, spawning one thread per connection. Returns once
    /// the loop has stopped and every connection thread has joined.
    pub fn run(self) -> io::Result<()> {
        tracing::info!(addr = %self.local_addr, "tephra server listening");
        let next_id = AtomicU64::new(0);
        let mut threads = Vec::new();
        let stats = Arc::new(SharedStats::new(
            self.data_dir.clone(),
            self.config.max_connections as u64,
        ));

        // Optional Prometheus endpoint on its own thread and port. Absent unless a metrics
        // address was bound, so a deployment that does not want it runs nothing extra.
        #[cfg(feature = "metrics")]
        let metrics_thread = match self.metrics_listener {
            Some(listener) => {
                if let Some(addr) = self.metrics_addr {
                    tracing::info!(%addr, "metrics endpoint listening");
                }
                let handle = self.handle.clone();
                let stats = Arc::clone(&stats);
                let running = Arc::clone(&self.running);
                Some(
                    thread::Builder::new()
                        .name("tephra-metrics".to_string())
                        .spawn(move || metrics::serve(listener, handle, stats, running))
                        .expect("spawn metrics thread"),
                )
            }
            None => None,
        };

        // One shared, server-wide pool of reusable worker threads streams every connection's
        // reads, so a read pays no per-request thread-creation cost. `0` means one worker per
        // logical CPU.
        let read_workers = if self.config.read_worker_threads != 0 {
            self.config.read_worker_threads
        } else {
            thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(1)
        };
        let read_pool = conn::ReadPool::new(read_workers);
        tracing::info!(read_workers, "read worker pool started");

        // Edge-triggered so a sustained connection flood does not itself flood the log: the warn
        // fires once when the cap is first hit and re-arms only after a connection is admitted
        // again. The `connections_refused` counter still records every refusal for metrics.
        let mut at_cap = false;
        for stream in self.listener.incoming() {
            if !self.running.load(Ordering::Acquire) {
                break;
            }
            let stream = match stream {
                Ok(stream) => stream,
                Err(err) => {
                    tracing::warn!(%err, "accept failed");
                    continue;
                }
            };

            // Enforce the global connection cap before doing any per-connection setup. Over the
            // cap the connection is closed immediately, before a request is ever read, so a
            // client fleet cannot exhaust threads and file descriptors. The permit rides into the
            // connection thread and releases the slot (and the gauge) when the connection ends.
            let permit = match ConnPermit::acquire(&stats) {
                Some(permit) => {
                    at_cap = false;
                    permit
                }
                None => {
                    let refused = stats.connections_refused.fetch_add(1, Ordering::Relaxed) + 1;
                    if !at_cap {
                        at_cap = true;
                        tracing::warn!(
                            max = self.config.max_connections,
                            refused_total = refused,
                            "at max_connections; refusing new connections until one frees"
                        );
                    }
                    let _ = stream.shutdown(Shutdown::Both);
                    continue;
                }
            };

            // Reap the handles of connections that have already finished so the vector stays
            // bounded by the live (capped) connection count rather than every connection ever
            // served. The remainder is joined after the accept loop ends.
            threads.retain(|thread: &thread::JoinHandle<()>| !thread.is_finished());

            // Explicit TCP keepalive so a silently-dead connection (for example a subscriber
            // whose client vanished) is eventually reaped by the OS, rather than after the
            // multi-hour default idle. Best-effort: a failure only forgoes early reaping.
            let keepalive = TcpKeepalive::new()
                .with_time(self.config.keepalive_idle)
                .with_interval(self.config.keepalive_interval);
            if let Err(err) = SockRef::from(&stream).set_tcp_keepalive(&keepalive) {
                tracing::warn!(%err, "failed to set TCP keepalive");
            }

            // A registry clone lets shutdown wake this connection if it parks on a read. If we
            // cannot clone it, we cannot guarantee that, so refuse the connection rather than
            // risk a thread that shutdown can never reach.
            let id = next_id.fetch_add(1, Ordering::Relaxed);
            let registry_handle = match stream.try_clone() {
                Ok(clone) => clone,
                Err(err) => {
                    tracing::warn!(%err, "failed to clone connection for shutdown; dropping it");
                    continue;
                }
            };
            // Register before serving. If shutdown has already begun, do not serve: the stream
            // is shut down by `register`, and no more connections are accepted.
            if !self.connections.register(id, registry_handle) {
                let _ = stream.shutdown(Shutdown::Both);
                break;
            }

            let handle = self.handle.clone();
            let config = self.config;
            let connections = self.connections.clone();
            // The accept-loop flag doubles as the subscription shutdown signal: an idle
            // subscription's bounded wait re-checks it each tick, so shutdown ends the stream
            // even when no events flow and the socket is idle.
            let running = Arc::clone(&self.running);
            let read_pool = read_pool.sender();
            let thread = thread::Builder::new()
                .name("tephra-conn".to_string())
                .spawn(move || {
                    // The permit (an RAII guard, not a bare fetch_sub) holds the connection slot
                    // and the gauge for the whole connection, so an unwind out of
                    // serve_connection cannot leak a slot or inflate the gauge.
                    conn::serve_connection(stream, handle, config, running, read_pool, &permit.0);
                    drop(permit);
                    connections.remove(id);
                })?;
            threads.push(thread);
        }

        for thread in threads {
            let _ = thread.join();
        }
        // Every connection (and so every cloned sender) is gone; drain and join the pool workers.
        read_pool.shutdown();
        // The metrics thread polls `running` (now cleared), so it exits within a poll tick.
        #[cfg(feature = "metrics")]
        if let Some(metrics_thread) = metrics_thread {
            let _ = metrics_thread.join();
        }
        tracing::info!("tephra server stopped");
        Ok(())
    }
}

/// Stops a running [`Server`]. Cloneable and `Send`, so it can drive shutdown from a signal
/// handler or another thread.
#[derive(Clone)]
pub struct ShutdownHandle {
    running: Arc<AtomicBool>,
    local_addr: SocketAddr,
    connections: Connections,
}

impl ShutdownHandle {
    /// Signals the accept loop to stop and unblocks any connection parked on a read. Idempotent.
    pub fn shutdown(&self) {
        self.running.store(false, Ordering::Release);
        // Wake the blocking `accept()` with a throwaway loopback connection, so the loop
        // observes the cleared flag and breaks.
        let _ = TcpStream::connect(self.local_addr);
        // Shut down live connections so threads parked on a blocking read return promptly.
        self.connections.shutdown_all();
    }
}
