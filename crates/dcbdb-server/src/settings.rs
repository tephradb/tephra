//! Layered server configuration.
//!
//! Sources are merged in ascending precedence: built-in defaults, then an optional TOML
//! config file (`--config`), then `DCBDB__*` environment variables, then a small set of
//! command-line flags. A later source overrides an earlier one.
//!
//! The command line intentionally carries only the launch essentials (`--bind`,
//! `--data-dir`, `--log`, plus `--config`): where the server runs and how to reach it, the
//! things typed per invocation. The full performance and memory tuning surface lives in the
//! config file and environment so it stays declarative and reviewable rather than a wall of
//! flags. Deliberately internal knobs (the paranoid tips cross-check, record-framing sizes)
//! are not exposed at any tier.

use std::error::Error;

use argh::FromArgs;
use config::{Config, Environment, File, FileFormat};
use dcbdb::log::set::SegmentConfig;
use dcbdb::read::ReadConfig;
use dcbdb::writer::WriterConfig;
use dcbdb_proto::DEFAULT_MAX_FRAME_LEN;
use dcbdb_server::ServerConfig;
use serde::Deserialize;

/// dcbdb event store server: opens a store on disk and serves it over TCP.
#[derive(Debug, FromArgs)]
pub struct Args {
    /// path to a TOML config file (all tuning lives here or in DCBDB__* env vars)
    #[argh(option, short = 'c')]
    pub config: Option<String>,

    /// address to bind, e.g. 127.0.0.1:9000
    #[argh(option, short = 'b')]
    pub bind: Option<String>,

    /// data directory for the event store
    #[argh(option, short = 'd')]
    pub data_dir: Option<String>,

    /// tracing filter, overriding RUST_LOG (e.g. "info" or "dcbdb=debug")
    #[argh(option, short = 'l')]
    pub log: Option<String>,
}

/// The fully-resolved server configuration.
///
/// Field names double as config-file keys and (upper-cased, `__`-joined) environment
/// variable names, so `writer.max_batch_bytes` is `DCBDB__WRITER__MAX_BATCH_BYTES`.
#[derive(Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Settings {
    /// Address the TCP listener binds.
    pub bind: String,
    /// Directory holding the log (and index) segment files.
    pub data_dir: String,
    /// Tracing filter. `None` falls back to `RUST_LOG`, then to `info`.
    pub log: Option<String>,
    pub segment: SegmentSettings,
    pub writer: WriterSettings,
    pub read: ReadSettings,
    pub server: ServerSettings,
}

impl Default for Settings {
    fn default() -> Self {
        Settings {
            bind: "127.0.0.1:9000".to_string(),
            data_dir: "dcbdb-data".to_string(),
            log: None,
            segment: SegmentSettings::default(),
            writer: WriterSettings::default(),
            read: ReadSettings::default(),
            server: ServerSettings::default(),
        }
    }
}

/// Log-segment sizing options.
#[derive(Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct SegmentSettings {
    /// Total size of each segment file in bytes, including its header.
    pub size: usize,
}

impl Default for SegmentSettings {
    fn default() -> Self {
        SegmentSettings {
            size: 16 * 1024 * 1024,
        }
    }
}

/// Write-coordinator tuning: backpressure, group-commit sizing, and the tips memory bound.
#[derive(Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct WriterSettings {
    /// Bounded request-queue depth. When full, an append blocks (backpressure).
    pub queue_capacity: usize,
    /// Most requests folded into one group-committed batch.
    pub max_batch_records: usize,
    /// Byte budget for one batch. Clamped down to the segment capacity at startup so it can
    /// never exceed a shrunk `segment.size`.
    pub max_batch_bytes: usize,
    /// Recent-position window width for the durable tips map (a memory bound only).
    pub tips_window: u64,
    /// Resolve the append-condition durable arm with the log scan instead of the index
    /// existence check. An operational escape hatch: the log is the source of truth, so the
    /// scan is always safe, just slower.
    pub condition_force_scan: bool,
}

impl Default for WriterSettings {
    fn default() -> Self {
        WriterSettings {
            queue_capacity: 1024,
            max_batch_records: 1024,
            max_batch_bytes: 8 * 1024 * 1024,
            tips_window: 1_000_000,
            condition_force_scan: false,
        }
    }
}

/// Read-path planner tuning.
#[derive(Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ReadSettings {
    /// The planner's `K`: the index is chosen only when the post-pruning range is at least
    /// `scan_bias` times the estimated result count, so larger values bias toward scanning at
    /// the margin. Changes only which correct path runs, never the answer.
    pub scan_bias: u32,
}

impl Default for ReadSettings {
    fn default() -> Self {
        ReadSettings { scan_bias: 4 }
    }
}

/// TCP server tuning: frame limit and streamed-read flush thresholds.
#[derive(Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ServerSettings {
    /// Largest single frame accepted or produced, in bytes.
    pub max_frame_len: u32,
    /// A streamed read is flushed as a frame once it holds this many events.
    pub read_batch_events: usize,
    /// A streamed read is flushed as a frame once its buffered events reach this many bytes.
    pub read_batch_bytes: usize,
}

impl Default for ServerSettings {
    fn default() -> Self {
        ServerSettings {
            max_frame_len: DEFAULT_MAX_FRAME_LEN,
            read_batch_events: 1024,
            read_batch_bytes: 512 * 1024,
        }
    }
}

impl Settings {
    /// The segment config for opening the store.
    pub fn segment_config(&self) -> SegmentConfig {
        SegmentConfig::new(self.segment.size)
    }

    /// The write-coordinator config. `verify_tips` is deliberately never operator-settable, so
    /// it stays `false` here.
    pub fn writer_config(&self) -> WriterConfig {
        WriterConfig {
            queue_capacity: self.writer.queue_capacity,
            max_batch_records: self.writer.max_batch_records,
            max_batch_bytes: self.writer.max_batch_bytes,
            tips_window: self.writer.tips_window,
            verify_tips: false,
            condition_force_scan: self.writer.condition_force_scan,
            read: ReadConfig {
                scan_bias: self.read.scan_bias,
            },
        }
    }

    /// The TCP server config.
    pub fn server_config(&self) -> ServerConfig {
        ServerConfig {
            max_frame_len: self.server.max_frame_len,
            read_batch_events: self.server.read_batch_events,
            read_batch_bytes: self.server.read_batch_bytes,
        }
    }

    /// Rejects values the write coordinator would otherwise assert on at startup, so a config
    /// typo is a graceful error rather than a panic. A count of zero is never meaningful, so it
    /// is rejected outright (unlike `max_batch_bytes`, whose valid default can exceed a shrunk
    /// `segment.size` and so is clamped, not rejected, once the capacity is known).
    fn validate(&self) -> Result<(), String> {
        if self.writer.queue_capacity == 0 {
            return Err("writer.queue_capacity must be at least 1".to_string());
        }
        if self.writer.max_batch_records == 0 {
            return Err("writer.max_batch_records must be at least 1".to_string());
        }
        Ok(())
    }
}

/// Builds the effective settings from defaults, the optional config file, the `DCBDB__*`
/// environment, and finally the command-line overrides.
pub fn load(args: &Args) -> Result<Settings, Box<dyn Error>> {
    let mut builder = Config::builder();
    if let Some(path) = &args.config {
        // Explicit path: a missing or malformed file is an error, not a silent skip.
        builder = builder.add_source(File::new(path, FileFormat::Toml).required(true));
    }
    builder = builder.add_source(
        Environment::with_prefix("DCBDB")
            .prefix_separator("__")
            .separator("__")
            .try_parsing(true),
    );

    let mut settings: Settings = builder.build()?.try_deserialize()?;

    // The command line wins over the file and the environment.
    if let Some(bind) = &args.bind {
        settings.bind = bind.clone();
    }
    if let Some(data_dir) = &args.data_dir {
        settings.data_dir = data_dir.clone();
    }
    if args.log.is_some() {
        settings.log = args.log.clone();
    }

    settings.validate()?;
    Ok(settings)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn no_args() -> Args {
        Args {
            config: None,
            bind: None,
            data_dir: None,
            log: None,
        }
    }

    #[test]
    fn defaults_match_the_library_defaults() {
        // No file, no matching env: every field falls through to its serde default, which must
        // mirror the library's own `Default` impls so behaviour is identical to the old binary.
        let settings = load(&no_args()).unwrap();
        let writer = settings.writer_config();
        let library_default = WriterConfig::default();
        assert_eq!(writer.queue_capacity, library_default.queue_capacity);
        assert_eq!(writer.max_batch_records, library_default.max_batch_records);
        assert_eq!(writer.max_batch_bytes, library_default.max_batch_bytes);
        assert_eq!(writer.tips_window, library_default.tips_window);
        assert!(!writer.verify_tips);
        assert_eq!(writer.read.scan_bias, ReadConfig::default().scan_bias);

        let server = settings.server_config();
        let server_default = ServerConfig::default();
        assert_eq!(server.max_frame_len, server_default.max_frame_len);
        assert_eq!(server.read_batch_events, server_default.read_batch_events);
        assert_eq!(server.read_batch_bytes, server_default.read_batch_bytes);

        assert_eq!(settings.bind, "127.0.0.1:9000");
        assert_eq!(settings.data_dir, "dcbdb-data");
    }

    #[test]
    fn cli_overrides_win() {
        let args = Args {
            config: None,
            bind: Some("0.0.0.0:7000".to_string()),
            data_dir: Some("/var/lib/dcbdb".to_string()),
            log: Some("dcbdb=debug".to_string()),
        };
        let settings = load(&args).unwrap();
        assert_eq!(settings.bind, "0.0.0.0:7000");
        assert_eq!(settings.data_dir, "/var/lib/dcbdb");
        assert_eq!(settings.log.as_deref(), Some("dcbdb=debug"));
    }

    #[test]
    fn zero_writer_counts_are_rejected_not_panicked() {
        // The coordinator asserts these are at least 1; validation must turn a config typo into a
        // graceful error rather than let the assert abort the process.
        let mut settings = Settings::default();
        settings.writer.queue_capacity = 0;
        assert!(settings.validate().is_err());

        let mut settings = Settings::default();
        settings.writer.max_batch_records = 0;
        assert!(settings.validate().is_err());
    }
}
