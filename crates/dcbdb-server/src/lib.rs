//! A synchronous, thread-per-connection TCP server exposing a [`dcbdb`] event store over the
//! length-prefixed protobuf protocol defined in [`dcbdb_proto`].
//!
//! The model mirrors the database: dcbdb is single-writer and synchronous
//! ([`WriteHandle::append`](dcbdb::writer::WriteHandle::append) blocks until durable, reads
//! run on the caller's own thread over a lock-free snapshot), so each connection is served on
//! its own OS thread that blocks on the socket and calls straight into the shared handles. No
//! async runtime, no thread pool.
//!
//! # Usage
//!
//! ```no_run
//! use std::net::TcpListener;
//! use dcbdb::log::set::{SegmentConfig, SegmentSet};
//! use dcbdb::writer::{WriteCoordinator, WriterConfig};
//! use dcbdb_server::{Server, ServerConfig};
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
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use socket2::{SockRef, TcpKeepalive};

use dcbdb::writer::WriteHandle;
use dcbdb_proto::DEFAULT_MAX_FRAME_LEN;

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

/// A bound server, ready to [`run`](Server::run).
pub struct Server {
    listener: TcpListener,
    handle: WriteHandle,
    config: ServerConfig,
    local_addr: SocketAddr,
    running: Arc<AtomicBool>,
    connections: Connections,
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
        })
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
        tracing::info!(addr = %self.local_addr, "dcbdb server listening");
        let next_id = AtomicU64::new(0);
        let mut threads = Vec::new();

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
            let thread = thread::Builder::new()
                .name("dcbdb-conn".to_string())
                .spawn(move || {
                    conn::serve_connection(stream, handle, config, running);
                    connections.remove(id);
                })?;
            threads.push(thread);
        }

        for thread in threads {
            let _ = thread.join();
        }
        tracing::info!("dcbdb server stopped");
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
