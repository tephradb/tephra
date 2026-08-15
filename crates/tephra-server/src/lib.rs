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
    /// Most appends + reads a single connection may have in flight at once. Once reached, the
    /// connection's reader blocks (backpressure) until one finishes, bounding both read worker
    /// threads and the buffered append-reply backlog. Subscriptions are budgeted separately.
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
    /// Depth of a connection's outbound frame queue. Bounds buffered response frames, so a slow
    /// client applies backpressure to the workers producing them.
    pub frame_queue_depth: usize,
    /// TCP keepalive idle time before the first probe on an accepted connection. The OS
    /// default (~2h on Linux) is too long to reap a silently-dead subscription promptly, so it
    /// is set explicitly.
    pub keepalive_idle: Duration,
    /// Interval between TCP keepalive probes once they start.
    pub keepalive_interval: Duration,
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
// disk usage), the start instant for uptime, and the live connection/subscription gauges.
// Shared behind an `Arc` and read with plain atomic loads.
pub(crate) struct SharedStats {
    data_dir: Option<PathBuf>,
    start_time: Instant,
    active_connections: AtomicU64,
    active_subscriptions: AtomicU64,
}

impl SharedStats {
    fn new(data_dir: Option<PathBuf>) -> SharedStats {
        SharedStats {
            data_dir,
            start_time: Instant::now(),
            active_connections: AtomicU64::new(0),
            active_subscriptions: AtomicU64::new(0),
        }
    }
}

// Holds the live-connection gauge up for one connection: increments on construction and
// decrements on drop, so an unwind out of `serve_connection` cannot inflate it.
struct ConnGuard(Arc<SharedStats>);

impl ConnGuard {
    fn new(stats: Arc<SharedStats>) -> ConnGuard {
        stats.active_connections.fetch_add(1, Ordering::Relaxed);
        ConnGuard(stats)
    }
}

impl Drop for ConnGuard {
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
        })
    }

    /// Records the data directory the store lives in, so the stats op can report the on-disk
    /// segment count and byte usage. Without it, those fields report zero (the store still
    /// serves normally). The standalone binary always sets this.
    pub fn with_data_dir(mut self, data_dir: impl Into<PathBuf>) -> Server {
        self.data_dir = Some(data_dir.into());
        self
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
        let stats = Arc::new(SharedStats::new(self.data_dir.clone()));

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
            let stats = Arc::clone(&stats);
            let thread = thread::Builder::new()
                .name("tephra-conn".to_string())
                .spawn(move || {
                    // A guard, not a bare fetch_sub, so an unwind out of serve_connection (e.g. a
                    // failed thread spawn under load) cannot leave the gauge inflated.
                    let guard = ConnGuard::new(stats);
                    conn::serve_connection(stream, handle, config, running, read_pool, &guard.0);
                    connections.remove(id);
                })?;
            threads.push(thread);
        }

        for thread in threads {
            let _ = thread.join();
        }
        // Every connection (and so every cloned sender) is gone; drain and join the pool workers.
        read_pool.shutdown();
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
