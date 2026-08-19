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

mod healthcheck;
mod settings;

use std::env;
use std::error::Error;
#[cfg(feature = "tls")]
use std::path::Path;
use std::process::{self, ExitCode};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use tephra::log::set::SegmentSet;
use tephra::writer::WriteCoordinator;
use tephra_server::Server;
use tracing_subscriber::filter::{LevelFilter, Targets};
use tracing_subscriber::layer::SubscriberExt as _;
use tracing_subscriber::util::SubscriberInitExt as _;

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

    // `--healthcheck` runs as a client, not a server: probe the configured bind address and
    // exit, without opening the store or standing up the listener.
    if args.healthcheck {
        return match healthcheck::probe(&settings.bind) {
            Ok(()) => ExitCode::SUCCESS,
            Err(err) => {
                eprintln!("healthcheck failed: {err}");
                ExitCode::FAILURE
            }
        };
    }

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
/// otherwise `RUST_LOG` is honoured, falling back to `info`. Uses `Targets` rather than
/// `EnvFilter` so the binary carries no regex engine; the `target=level` directive syntax is the
/// same.
fn init_tracing(settings: &Settings) {
    let directives = settings
        .log
        .clone()
        .or_else(|| env::var("RUST_LOG").ok())
        .unwrap_or_else(|| "info".to_string());
    let targets = directives
        .parse::<Targets>()
        .unwrap_or_else(|_| Targets::new().with_default(LevelFilter::INFO));
    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer())
        .with(targets)
        .init();
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

    let server = Server::bind(&settings.bind, handle, settings.server_config())?
        .with_data_dir(&settings.data_dir);
    #[cfg(feature = "metrics")]
    let server = match &settings.metrics.bind {
        Some(addr) => server.with_metrics_addr(addr)?,
        None => server,
    };
    #[cfg(feature = "tls")]
    let server = match (&settings.tls.cert, &settings.tls.key) {
        (Some(cert), Some(key)) => {
            let tls = tephra_server::tls::build_server_config(Path::new(cert), Path::new(key))?;
            tracing::info!("serving over tls");
            server.with_tls(tls)
        }
        _ => server,
    };
    #[cfg(not(feature = "tls"))]
    if settings.tls.cert.is_some() {
        tracing::warn!("tls is configured but this binary was built without the tls feature");
    }
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
