//! The `dcbdb-server` binary: opens an event store on disk and serves it over TCP.
//!
//! Usage:
//!
//! ```text
//! dcbdb-server [BIND_ADDR] [DATA_DIR]
//! ```
//!
//! Defaults: `BIND_ADDR=127.0.0.1:9000`, `DATA_DIR=./dcbdb-data`. The segment size and
//! maximum frame length can be overridden with the `DCBDB_SEGMENT_SIZE` and
//! `DCBDB_MAX_FRAME_LEN` environment variables. Logging is controlled with `RUST_LOG`.

use std::env;
use std::error::Error;
use std::process::ExitCode;

use dcbdb::log::set::{SegmentConfig, SegmentSet};
use dcbdb::writer::{WriteCoordinator, WriterConfig};
use dcbdb_server::{Server, ServerConfig};
use tracing_subscriber::EnvFilter;

const DEFAULT_ADDR: &str = "127.0.0.1:9000";
const DEFAULT_DIR: &str = "dcbdb-data";
const DEFAULT_SEGMENT_SIZE: usize = 16 * 1024 * 1024;

fn main() -> ExitCode {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            tracing::error!(%err, "server exited with error");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<(), Box<dyn Error>> {
    let mut args = env::args().skip(1);
    let addr = args.next().unwrap_or_else(|| DEFAULT_ADDR.to_string());
    let dir = args.next().unwrap_or_else(|| DEFAULT_DIR.to_string());

    let segment_size = env_usize("DCBDB_SEGMENT_SIZE", DEFAULT_SEGMENT_SIZE)?;
    let mut server_config = ServerConfig::default();
    if let Some(max_frame_len) = env_opt_u32("DCBDB_MAX_FRAME_LEN")? {
        server_config.max_frame_len = max_frame_len;
    }

    let set = SegmentSet::open(&dir, SegmentConfig::new(segment_size))?;
    let (coordinator, handle) = WriteCoordinator::start(set, WriterConfig::default())?;
    tracing::info!(dir, segment_size, "opened event store");

    let server = Server::bind(&addr, handle, server_config)?;
    let shutdown = server.shutdown_handle();

    // Ctrl-C triggers a graceful shutdown: the accept loop stops and in-flight connections
    // are unblocked, so `run` returns and the writer is joined below.
    ctrlc::set_handler(move || {
        tracing::info!("received interrupt, shutting down");
        shutdown.shutdown();
    })?;

    let run_result = server.run();
    // Join the writer so the log is flushed and closed cleanly, regardless of run's result.
    coordinator.shutdown();
    run_result?;
    Ok(())
}

fn env_usize(key: &str, default: usize) -> Result<usize, Box<dyn Error>> {
    match env::var(key) {
        Ok(value) => Ok(value.parse().map_err(|err| format!("{key}: {err}"))?),
        Err(_) => Ok(default),
    }
}

fn env_opt_u32(key: &str) -> Result<Option<u32>, Box<dyn Error>> {
    match env::var(key) {
        Ok(value) => Ok(Some(value.parse().map_err(|err| format!("{key}: {err}"))?)),
        Err(_) => Ok(None),
    }
}
