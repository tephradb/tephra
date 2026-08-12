//! The `tephra-server` binary: opens an event store on disk and serves it over TCP.
//!
//! Configuration is layered (later sources win): built-in defaults, then a TOML file passed
//! with `--config`, then `TEPHRA__*` environment variables, then the command-line flags. The
//! command line carries only the launch essentials:
//!
//! ```text
//! tephra-server [--config PATH] [--bind ADDR] [--data-dir DIR] [--log FILTER]
//! ```
//!
//! Everything else (segment size, group-commit sizing, tips window, planner bias, frame and
//! read-batch limits) is set in the config file or the environment. See `tephra.example.toml`.

mod settings;

use std::error::Error;
use std::process::{self, ExitCode};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use tephra::log::set::SegmentSet;
use tephra::writer::WriteCoordinator;
use tephra_server::Server;
use tracing_subscriber::EnvFilter;

use settings::{Args, Settings};

/// Exit code used when a second shutdown signal forces an immediate exit, bypassing the
/// graceful path. Matches the conventional 128 + SIGTERM(15) for a termination by signal.
const EXIT_FORCED: i32 = 143;

fn main() -> ExitCode {
    let args: Args = argh::from_env();
    let settings = match settings::load(&args) {
        Ok(settings) => settings,
        Err(err) => {
            // Tracing is not up yet, so report the config failure on stderr directly.
            eprintln!("configuration error: {err}");
            return ExitCode::FAILURE;
        }
    };
    init_tracing(&settings);

    match run(settings) {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            tracing::error!(%err, "server exited with error");
            ExitCode::FAILURE
        }
    }
}

/// Initialises tracing. An explicit `log` setting (from `--log`, the file, or the env) wins;
/// otherwise `RUST_LOG` is honoured, falling back to `info`.
fn init_tracing(settings: &Settings) {
    let filter = match &settings.log {
        Some(filter) => EnvFilter::new(filter.clone()),
        None => EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
    };
    tracing_subscriber::fmt().with_env_filter(filter).init();
}

fn run(settings: Settings) -> Result<(), Box<dyn Error>> {
    let set = SegmentSet::open(&settings.data_dir, settings.segment_config())?;

    // segment.size is operator-settable while the coordinator asserts
    // max_batch_bytes <= segment capacity, so a small segment size against the default batch
    // budget would otherwise panic at startup. Clamp with a warning instead.
    let capacity = set.segment_capacity();
    let mut writer_config = settings.writer_config();
    if writer_config.max_batch_bytes > capacity {
        tracing::warn!(
            requested = writer_config.max_batch_bytes,
            capacity,
            "max_batch_bytes exceeds segment capacity; clamping to capacity"
        );
        writer_config.max_batch_bytes = capacity;
    }

    let (coordinator, handle) = WriteCoordinator::start(set, writer_config)?;
    tracing::info!(
        data_dir = %settings.data_dir,
        segment_size = settings.segment.size,
        "opened event store"
    );

    let server = Server::bind(&settings.bind, handle, settings.server_config())?;
    let shutdown = server.shutdown_handle();

    // SIGINT (Ctrl-C) and SIGTERM (the signal `docker stop`, systemd, and Kubernetes send)
    // both trigger a graceful shutdown: the accept loop stops and in-flight connections are
    // unblocked, so `run` returns and the writer is joined below, flushing and closing the log
    // cleanly. A second signal means an operator gave up waiting on a stuck shutdown, so it
    // force-exits immediately rather than being swallowed like the process default would.
    let signalled = Arc::new(AtomicBool::new(false));
    ctrlc::set_handler(move || {
        if signalled.swap(true, Ordering::SeqCst) {
            tracing::warn!("received second signal, exiting immediately");
            process::exit(EXIT_FORCED);
        }
        tracing::info!("received shutdown signal, shutting down");
        shutdown.shutdown();
    })?;

    let run_result = server.run();
    // Join the writer so the log is flushed and closed cleanly, regardless of run's result.
    coordinator.shutdown();
    run_result?;
    Ok(())
}
