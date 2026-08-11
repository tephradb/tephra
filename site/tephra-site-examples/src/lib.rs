//! Compiled, tested code examples for the Tephra documentation site.
//!
//! Every code sample on the site is a test in this crate, so a signature change breaks the
//! build rather than rotting the docs. The client-path tests run against a real Tephra server
//! started in-process on an ephemeral port with a temporary data directory, so `cargo test`
//! is self-contained and needs no external process.
//!
//! The pages import the region between `// ANCHOR: name` and `// ANCHOR_END: name` markers
//! from the test files, so the shipped snippet is the exact code that compiled here.

use std::net::SocketAddr;
use std::thread::{self, JoinHandle};

use tempfile::TempDir;
use tephra::log::set::{SegmentConfig, SegmentSet};
use tephra::writer::{WriteCoordinator, WriterConfig};
use tephra_server::{Server, ServerConfig, ShutdownHandle};

/// A Tephra server running in-process for the lifetime of a test.
///
/// Bound to an ephemeral loopback port over a fresh temporary directory. Dropping it shuts the
/// server down, joins its accept thread, and joins the writer thread, so a test leaves nothing
/// behind.
pub struct TestServer {
    addr: SocketAddr,
    shutdown: ShutdownHandle,
    server_thread: Option<JoinHandle<()>>,
    coordinator: Option<WriteCoordinator>,
    _dir: TempDir,
}

impl TestServer {
    /// Starts a server on a fresh tempdir, bound to `127.0.0.1:0` (an ephemeral port).
    pub fn start() -> TestServer {
        let dir = TempDir::new().expect("create temp data dir");
        let set = SegmentSet::open(dir.path(), SegmentConfig::new(16 * 1024 * 1024))
            .expect("open segment set");
        let (coordinator, handle) =
            WriteCoordinator::start(set, WriterConfig::default()).expect("start coordinator");
        let server = Server::bind("127.0.0.1:0", handle, ServerConfig::default()).expect("bind");
        let addr = server.local_addr();
        let shutdown = server.shutdown_handle();
        let server_thread = thread::spawn(move || {
            let _ = server.run();
        });
        TestServer {
            addr,
            shutdown,
            server_thread: Some(server_thread),
            coordinator: Some(coordinator),
            _dir: dir,
        }
    }

    /// The `host:port` string a client connects to.
    pub fn addr(&self) -> String {
        self.addr.to_string()
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        self.shutdown.shutdown();
        if let Some(thread) = self.server_thread.take() {
            let _ = thread.join();
        }
        if let Some(coordinator) = self.coordinator.take() {
            coordinator.shutdown();
        }
    }
}
