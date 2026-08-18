//! One source of truth for the server's operational numbers, shared by the protobuf `Stats` op
//! ([`conn`](crate::conn)) and the Prometheus `/metrics` endpoint ([`metrics`](crate::metrics)),
//! so the two can never drift.

use std::fs;
use std::path::Path;
use std::sync::atomic::Ordering;

use tephra::writer::WriteHandle;

use crate::SharedStats;

/// A point-in-time reading of the server's operational state.
pub(crate) struct StatsSnapshot {
    pub event_count: u64,
    pub segment_count: u64,
    pub disk_bytes: u64,
    pub uptime_seconds: u64,
    pub active_connections: u64,
    pub active_subscriptions: u64,
    /// Connections refused because the server was at `max_connections`. Monotonic.
    pub connections_refused: u64,
    /// Connections reaped for exceeding a connection timeout (handshake, idle, or incomplete
    /// frame). Monotonic.
    pub connections_reaped: u64,
    /// The configured connection cap, or `0` when unlimited.
    pub max_connections: u64,
    pub version: &'static str,
}

/// Samples the gauges, the durable tip, and (if known) the data directory. Cheap: atomic loads
/// plus one directory stat.
pub(crate) fn gather(stats: &SharedStats, handle: &WriteHandle) -> StatsSnapshot {
    let (segment_count, disk_bytes) = match &stats.data_dir {
        Some(dir) => scan_data_dir(dir),
        None => (0, 0),
    };
    StatsSnapshot {
        event_count: handle.head().get(),
        segment_count,
        disk_bytes,
        uptime_seconds: stats.start_time.elapsed().as_secs(),
        active_connections: stats.active_connections.load(Ordering::Relaxed),
        active_subscriptions: stats.active_subscriptions.load(Ordering::Relaxed),
        connections_refused: stats.connections_refused.load(Ordering::Relaxed),
        connections_reaped: stats.connections_reaped.load(Ordering::Relaxed),
        max_connections: stats.max_connections,
        version: env!("CARGO_PKG_VERSION"),
    }
}

/// Sums the file sizes in `dir` and counts the log segments among them. A directory that cannot
/// be read reports zero rather than failing the caller.
fn scan_data_dir(dir: &Path) -> (u64, u64) {
    let Ok(entries) = fs::read_dir(dir) else {
        return (0, 0);
    };
    let mut segment_count = 0;
    let mut disk_bytes = 0;
    for entry in entries.flatten() {
        let Ok(metadata) = entry.metadata() else {
            continue;
        };
        if !metadata.is_file() {
            continue;
        }
        disk_bytes += metadata.len();
        if entry.path().extension().is_some_and(|ext| ext == "log") {
            segment_count += 1;
        }
    }
    (segment_count, disk_bytes)
}
