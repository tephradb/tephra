//! Layered server configuration.
//!
//! Sources are merged in ascending precedence: built-in defaults, then an optional TOML
//! config file (`--config`), then `TEPHRA__*` environment variables, then a small set of
//! command-line flags. A later source overrides an earlier one.
//!
//! The command line intentionally carries only the launch essentials (`--bind`,
//! `--data-dir`, `--log`, plus `--config`): where the server runs and how to reach it, the
//! things typed per invocation. The full performance and memory tuning surface lives in the
//! config file and environment so it stays declarative and reviewable rather than a wall of
//! flags. Deliberately internal knobs (the paranoid tips cross-check, record-framing sizes)
//! are not exposed at any tier.

use std::error::Error;
use std::time::Duration;

use argh::FromArgs;
use config::{Config, Environment, File, FileFormat};
use serde::Deserialize;
use tephra::log::set::SegmentConfig;
use tephra::read::ReadConfig;
use tephra::writer::WriterConfig;
use tephra_proto::DEFAULT_MAX_FRAME_LEN;
use tephra_server::ServerConfig;

/// tephra event store server: opens a store on disk and serves it over TCP.
#[derive(Debug, FromArgs)]
pub struct Args {
    /// path to a TOML config file (all tuning lives here or in TEPHRA__* env vars)
    #[argh(option, short = 'c')]
    pub config: Option<String>,

    /// address to bind, e.g. 127.0.0.1:9000
    #[argh(option, short = 'b')]
    pub bind: Option<String>,

    /// data directory for the event store
    #[argh(option, short = 'd')]
    pub data_dir: Option<String>,

    /// tracing filter, overriding RUST_LOG (e.g. "info" or "tephra=debug")
    #[argh(option, short = 'l')]
    pub log: Option<String>,

    /// probe a running server at the configured bind address, then exit 0 if healthy or 1 if not
    #[argh(switch)]
    pub healthcheck: bool,
}

/// The fully-resolved server configuration.
///
/// Field names double as config-file keys and (upper-cased, `__`-joined) environment
/// variable names, so `writer.max_batch_bytes` is `TEPHRA__WRITER__MAX_BATCH_BYTES`.
#[derive(Debug, PartialEq, Deserialize)]
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
    pub metrics: MetricsSettings,
    pub tls: TlsSettings,
    pub auth: AuthSettings,
}

impl Default for Settings {
    fn default() -> Self {
        Settings {
            bind: "127.0.0.1:9000".to_string(),
            data_dir: "tephra-data".to_string(),
            log: None,
            segment: SegmentSettings::default(),
            writer: WriterSettings::default(),
            read: ReadSettings::default(),
            server: ServerSettings::default(),
            metrics: MetricsSettings::default(),
            tls: TlsSettings::default(),
            auth: AuthSettings::default(),
        }
    }
}

/// Prometheus `/metrics` endpoint. Served on its own port, separate from `bind`.
#[derive(Debug, Default, PartialEq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct MetricsSettings {
    /// Address for the `/metrics` HTTP endpoint, e.g. `127.0.0.1:9100`. `None` disables it.
    pub bind: Option<String>,
}

/// TLS transport. A certificate and key together enable TLS; both absent leaves the server
/// plaintext. Exactly one set is a configuration error.
#[derive(Debug, Default, PartialEq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct TlsSettings {
    /// Path to the PEM certificate chain. Set with `key` to serve TLS.
    pub cert: Option<String>,
    /// Path to the PEM private key.
    pub key: Option<String>,
}

/// Bearer-token authentication. Any configured token in a connection's opening Hello is accepted;
/// an empty `tokens` list leaves the server open (no authentication). Tokens are secrets, so they
/// require TLS unless `allow_insecure` is set (see [`Settings::validate`]).
#[derive(Debug, Default, PartialEq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct AuthSettings {
    /// The accepted tokens, each a table so scopes can be added later without a config-format
    /// change. Multiple tokens allow zero-downtime rotation: add the new one, roll clients over,
    /// then drop the old.
    pub tokens: Vec<TokenSettings>,
    /// Permit tokens over a plaintext listener, for a deployment that terminates TLS at a proxy or
    /// mesh before tephra. Off by default: a bearer secret should not cross an unencrypted hop.
    pub allow_insecure: bool,
}

/// One accepted token. A table (rather than a bare string) so an `access` scope, a name, or tag
/// restrictions can be added additively in a later step.
#[derive(Debug, Default, PartialEq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct TokenSettings {
    /// The bearer token a client presents in its Hello.
    pub token: String,
}

/// Log-segment sizing options.
#[derive(Debug, PartialEq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct SegmentSettings {
    /// Total size of each segment file in bytes, including its header.
    pub size: usize,
}

impl Default for SegmentSettings {
    fn default() -> Self {
        SegmentSettings {
            size: 256 * 1024 * 1024,
        }
    }
}

/// Write-coordinator tuning: backpressure, group-commit sizing, and the tips memory bound.
#[derive(Debug, PartialEq, Deserialize)]
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
            queue_capacity: 16384,
            max_batch_records: 2048,
            max_batch_bytes: 8 * 1024 * 1024,
            tips_window: 1_000_000,
            condition_force_scan: false,
        }
    }
}

/// Read-path planner tuning.
#[derive(Debug, PartialEq, Deserialize)]
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

/// TCP server tuning: frame limit, streamed-read flush thresholds, subscription pacing, and
/// TCP keepalive. Durations are expressed as integers with an explicit unit suffix so they
/// stay natural TOML/env scalars (there is no bare `Duration` on the wire).
#[derive(Debug, PartialEq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ServerSettings {
    /// Largest single frame accepted or produced, in bytes.
    pub max_frame_len: u32,
    /// A streamed read (or subscription) is flushed as a frame once it holds this many events.
    pub read_batch_events: usize,
    /// A streamed read (or subscription) is flushed as a frame once its buffered events reach
    /// this many bytes.
    pub read_batch_bytes: usize,
    /// How often an idle subscription's blocking wait wakes to re-check server shutdown, in
    /// milliseconds. Keeps a subscription with no events flowing responsive to shutdown
    /// without a heartbeat frame.
    pub subscribe_wait_tick_ms: u64,
    /// Per-connection in-flight budget, applied separately to appends and reads: this many appends
    /// awaiting a reply (then the reader backpressures), and this many concurrent reads plus this
    /// many queued for a slot (then a further read is rejected, never blocking the reader).
    pub max_inflight_requests_per_conn: usize,
    /// Most live subscriptions a single connection may hold at once; one over the limit is
    /// rejected.
    pub max_concurrent_subscriptions: usize,
    /// Number of reusable worker threads in the shared read pool. 0 means auto: one per logical
    /// CPU. Warm reads are short and CPU-bound, so one per core reaches the read-parallelism
    /// ceiling; raise it for slow-client streaming-read workloads.
    pub read_worker_threads: usize,
    /// Depth of a connection's outbound bulk frame queue: read and subscription frames buffered
    /// before a slow client applies backpressure. Small control frames use a separate priority lane.
    pub frame_queue_depth: usize,
    /// TCP keepalive idle time before the first probe on an accepted connection, in seconds.
    /// The OS default (~2h on Linux) is too long to reap a silently-dead subscription
    /// promptly.
    pub keepalive_idle_secs: u64,
    /// Interval between TCP keepalive probes once they start, in seconds.
    pub keepalive_interval_secs: u64,
    /// Most connections served at once, across all clients. A connection over the cap is closed
    /// immediately, before any request is read. `0` means unlimited (an explicit opt-out).
    pub max_connections: usize,
    /// Seconds a partial request frame may take to finish once its first byte has arrived, before
    /// the connection is reaped (slow-loris trickle defense). `0` disables it.
    pub incomplete_frame_timeout_secs: u64,
    /// Seconds a freshly accepted connection may take to send its first complete frame before being
    /// reaped. `0` disables it (the default): a pooling client may hold a connection open before
    /// its first request, so enable this only where clients do not.
    pub handshake_timeout_secs: u64,
    /// Seconds a connection with no request in flight and no live subscription may sit idle before
    /// being reaped. `0` disables it (the default), for the same pooling reason as
    /// `handshake_timeout_secs`.
    pub idle_timeout_secs: u64,
}

impl Default for ServerSettings {
    fn default() -> Self {
        ServerSettings {
            max_frame_len: DEFAULT_MAX_FRAME_LEN,
            read_batch_events: 1024,
            read_batch_bytes: 512 * 1024,
            subscribe_wait_tick_ms: 250,
            max_inflight_requests_per_conn: 256,
            max_concurrent_subscriptions: 64,
            read_worker_threads: 0,
            frame_queue_depth: 256,
            keepalive_idle_secs: 60,
            keepalive_interval_secs: 15,
            max_connections: 1024,
            incomplete_frame_timeout_secs: 30,
            handshake_timeout_secs: 0,
            idle_timeout_secs: 0,
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
            subscribe_wait_tick: Duration::from_millis(self.server.subscribe_wait_tick_ms),
            max_inflight_requests_per_conn: self.server.max_inflight_requests_per_conn,
            max_concurrent_subscriptions: self.server.max_concurrent_subscriptions,
            read_worker_threads: self.server.read_worker_threads,
            frame_queue_depth: self.server.frame_queue_depth,
            keepalive_idle: Duration::from_secs(self.server.keepalive_idle_secs),
            keepalive_interval: Duration::from_secs(self.server.keepalive_interval_secs),
            max_connections: self.server.max_connections,
            incomplete_frame_timeout: Duration::from_secs(
                self.server.incomplete_frame_timeout_secs,
            ),
            handshake_timeout: Duration::from_secs(self.server.handshake_timeout_secs),
            idle_timeout: Duration::from_secs(self.server.idle_timeout_secs),
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
        // A zero wait tick would make the subscription's bounded wait return immediately and
        // busy-spin; zero keepalive timers are meaningless. Reject rather than let either
        // degrade silently.
        if self.server.subscribe_wait_tick_ms == 0 {
            return Err("server.subscribe_wait_tick_ms must be at least 1".to_string());
        }
        // A zero budget would wedge the connection (no request could ever acquire a permit); a
        // zero frame queue is a rendezvous channel, not the intended bound. Reject both.
        if self.server.max_inflight_requests_per_conn == 0 {
            return Err("server.max_inflight_requests_per_conn must be at least 1".to_string());
        }
        if self.server.max_concurrent_subscriptions == 0 {
            return Err("server.max_concurrent_subscriptions must be at least 1".to_string());
        }
        if self.server.frame_queue_depth == 0 {
            return Err("server.frame_queue_depth must be at least 1".to_string());
        }
        if self.server.keepalive_idle_secs == 0 {
            return Err("server.keepalive_idle_secs must be at least 1".to_string());
        }
        if self.server.keepalive_interval_secs == 0 {
            return Err("server.keepalive_interval_secs must be at least 1".to_string());
        }
        // A certificate without a key (or the reverse) cannot serve TLS and is almost certainly a
        // mistake; reject it rather than silently fall back to plaintext.
        if self.tls.cert.is_some() != self.tls.key.is_some() {
            return Err("tls.cert and tls.key must be set together".to_string());
        }
        // An empty token is never a valid secret and would silently accept unauthenticated peers.
        if self.auth.tokens.iter().any(|t| t.token.is_empty()) {
            return Err("auth.tokens entries must have a non-empty token".to_string());
        }
        // Tokens are bearer secrets: refuse to serve them over plaintext unless the operator has
        // explicitly opted in (TLS terminated at a proxy/mesh in front of tephra).
        let tls_enabled = self.tls.cert.is_some() && self.tls.key.is_some();
        if !self.auth.tokens.is_empty() && !tls_enabled && !self.auth.allow_insecure {
            return Err(
                "auth.tokens require tls; set tls.cert and tls.key, or auth.allow_insecure = true"
                    .to_string(),
            );
        }
        Ok(())
    }

    /// The configured bearer tokens, if any. `None` leaves the server open (no authentication).
    /// Owned because the sole consumer, `AuthConfig::new`, hashes and keeps them.
    pub fn auth_tokens(&self) -> Option<Vec<String>> {
        if self.auth.tokens.is_empty() {
            return None;
        }
        Some(self.auth.tokens.iter().map(|t| t.token.clone()).collect())
    }

    /// The first configured token, borrowed, for the healthcheck probe (which needs one token, not
    /// the whole set). `None` when no tokens are configured.
    pub fn first_auth_token(&self) -> Option<&str> {
        self.auth.tokens.first().map(|t| t.token.as_str())
    }
}

/// Builds the effective settings from defaults, the optional config file, the `TEPHRA__*`
/// environment, and finally the command-line overrides.
pub fn load(args: &Args) -> Result<Settings, Box<dyn Error>> {
    let mut builder = Config::builder();
    if let Some(path) = &args.config {
        // Explicit path: a missing or malformed file is an error, not a silent skip.
        builder = builder.add_source(File::new(path, FileFormat::Toml).required(true));
    }
    builder = builder.add_source(
        Environment::with_prefix("TEPHRA")
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
            healthcheck: false,
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
        assert_eq!(
            server.subscribe_wait_tick,
            server_default.subscribe_wait_tick
        );
        assert_eq!(
            server.max_inflight_requests_per_conn,
            server_default.max_inflight_requests_per_conn
        );
        assert_eq!(
            server.max_concurrent_subscriptions,
            server_default.max_concurrent_subscriptions
        );
        assert_eq!(
            server.read_worker_threads,
            server_default.read_worker_threads
        );
        assert_eq!(server.frame_queue_depth, server_default.frame_queue_depth);
        assert_eq!(server.keepalive_idle, server_default.keepalive_idle);
        assert_eq!(server.keepalive_interval, server_default.keepalive_interval);
        assert_eq!(server.max_connections, server_default.max_connections);
        assert_eq!(
            server.incomplete_frame_timeout,
            server_default.incomplete_frame_timeout
        );
        assert_eq!(server.handshake_timeout, server_default.handshake_timeout);
        assert_eq!(server.idle_timeout, server_default.idle_timeout);

        assert_eq!(settings.bind, "127.0.0.1:9000");
        assert_eq!(settings.data_dir, "tephra-data");
    }

    #[test]
    fn example_toml_mirrors_the_defaults() {
        // `tephra.example.toml` documents itself as "every value shown is the built-in default",
        // and nothing else pins that promise. Deserialize the file on its own (no env, no CLI) and
        // assert it round-trips to `Settings::default()`, so any drift between the file and a
        // changed default fails here. `deny_unknown_fields` also catches a renamed or stray key.
        let path = concat!(env!("CARGO_MANIFEST_DIR"), "/tephra.example.toml");
        let settings: Settings = Config::builder()
            .add_source(File::new(path, FileFormat::Toml).required(true))
            .build()
            .unwrap()
            .try_deserialize()
            .unwrap();
        assert_eq!(settings, Settings::default());
    }

    #[test]
    fn cli_overrides_win() {
        let args = Args {
            config: None,
            bind: Some("0.0.0.0:7000".to_string()),
            data_dir: Some("/var/lib/tephra".to_string()),
            log: Some("tephra=debug".to_string()),
            healthcheck: false,
        };
        let settings = load(&args).unwrap();
        assert_eq!(settings.bind, "0.0.0.0:7000");
        assert_eq!(settings.data_dir, "/var/lib/tephra");
        assert_eq!(settings.log.as_deref(), Some("tephra=debug"));
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

    #[test]
    fn zero_server_durations_are_rejected() {
        // A zero wait tick would busy-spin the subscription loop; zero keepalive timers are
        // meaningless. Each must be rejected at load time.
        let mut settings = Settings::default();
        settings.server.subscribe_wait_tick_ms = 0;
        assert!(settings.validate().is_err());

        let mut settings = Settings::default();
        settings.server.keepalive_idle_secs = 0;
        assert!(settings.validate().is_err());

        let mut settings = Settings::default();
        settings.server.keepalive_interval_secs = 0;
        assert!(settings.validate().is_err());
    }

    #[test]
    fn zero_server_concurrency_counts_are_rejected() {
        // A zero budget would wedge a connection; a zero frame queue changes channel semantics.
        // Each must be rejected at load time rather than degrade at runtime.
        let mut settings = Settings::default();
        settings.server.max_inflight_requests_per_conn = 0;
        assert!(settings.validate().is_err());

        let mut settings = Settings::default();
        settings.server.max_concurrent_subscriptions = 0;
        assert!(settings.validate().is_err());

        let mut settings = Settings::default();
        settings.server.frame_queue_depth = 0;
        assert!(settings.validate().is_err());
    }

    fn with_tls(mut settings: Settings) -> Settings {
        settings.tls.cert = Some("server.crt".to_string());
        settings.tls.key = Some("server.key".to_string());
        settings
    }

    fn token(value: &str) -> TokenSettings {
        TokenSettings {
            token: value.to_string(),
        }
    }

    #[test]
    fn auth_tokens_require_tls_unless_allow_insecure() {
        // Tokens over plaintext are rejected by default...
        let mut settings = Settings::default();
        settings.auth.tokens = vec![token("secret")];
        assert!(settings.validate().is_err());

        // ...accepted with TLS...
        let mut settings = with_tls(Settings::default());
        settings.auth.tokens = vec![token("secret")];
        assert!(settings.validate().is_ok());

        // ...and accepted over plaintext only with the explicit opt-out.
        let mut settings = Settings::default();
        settings.auth.tokens = vec![token("secret")];
        settings.auth.allow_insecure = true;
        assert!(settings.validate().is_ok());
    }

    #[test]
    fn empty_auth_token_is_rejected() {
        let mut settings = with_tls(Settings::default());
        settings.auth.tokens = vec![token("")];
        assert!(settings.validate().is_err());
    }

    #[test]
    fn no_auth_tokens_is_open_and_valid() {
        // The default (empty token list) is valid over plaintext and yields no auth config.
        let settings = Settings::default();
        assert!(settings.validate().is_ok());
        assert!(settings.auth_tokens().is_none());
    }

    #[test]
    fn auth_tokens_collects_configured_tokens() {
        let mut settings = with_tls(Settings::default());
        settings.auth.tokens = vec![token("alpha"), token("beta")];
        assert_eq!(
            settings.auth_tokens(),
            Some(vec!["alpha".to_string(), "beta".to_string()])
        );
    }

    #[test]
    fn tls_cert_and_key_must_be_set_together() {
        // Both unset (plaintext) and both set (TLS) are valid; exactly one is a misconfiguration.
        let mut settings = Settings::default();
        settings.tls.cert = Some("server.crt".to_string());
        assert!(settings.validate().is_err());

        let mut settings = Settings::default();
        settings.tls.key = Some("server.key".to_string());
        assert!(settings.validate().is_err());

        let mut settings = Settings::default();
        settings.tls.cert = Some("server.crt".to_string());
        settings.tls.key = Some("server.key".to_string());
        assert!(settings.validate().is_ok());
    }
}
