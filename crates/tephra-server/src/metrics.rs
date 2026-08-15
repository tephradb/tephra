//! An optional Prometheus `/metrics` endpoint, served over plain HTTP/1.1 on its own port.
//!
//! Std-only: no async runtime and no HTTP or metrics crate. One dedicated thread runs a blocking
//! accept loop; a scrape is answered by rendering the [`StatsSnapshot`](crate::stats::StatsSnapshot)
//! as text exposition. The endpoint exists only when a metrics bind address is configured, so a
//! deployment that does not want it opens no extra port.

use std::io::{self, BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::Duration;

use tephra::writer::WriteHandle;

use crate::SharedStats;
use crate::stats::{self, StatsSnapshot};

/// How often the accept loop wakes to re-check shutdown while no scrape is arriving.
const POLL_INTERVAL: Duration = Duration::from_millis(250);
/// Per-scrape read/write deadline, so a slow or stuck client cannot pin the metrics thread.
const IO_TIMEOUT: Duration = Duration::from_secs(5);
/// Cap on the request head we read, so a client cannot make us buffer unboundedly.
const MAX_REQUEST_BYTES: u64 = 8 * 1024;

/// Runs the accept loop until `running` clears. The listener is polled non-blocking so shutdown is
/// observed within [`POLL_INTERVAL`]; scrapes are infrequent, so the poll cost is negligible.
pub(crate) fn serve(
    listener: TcpListener,
    handle: WriteHandle,
    stats: Arc<SharedStats>,
    running: Arc<AtomicBool>,
) {
    if let Err(err) = listener.set_nonblocking(true) {
        tracing::warn!(%err, "metrics listener could not be set non-blocking; shutdown may lag");
    }
    while running.load(Ordering::Acquire) {
        match listener.accept() {
            Ok((stream, _peer)) => handle_scrape(stream, &handle, &stats),
            Err(err) if err.kind() == io::ErrorKind::WouldBlock => thread::sleep(POLL_INTERVAL),
            Err(err) => {
                tracing::warn!(%err, "metrics accept failed");
                thread::sleep(POLL_INTERVAL);
            }
        }
    }
}

/// Answers one scrape: `GET /metrics` renders the exposition, anything else is a 404.
fn handle_scrape(mut stream: TcpStream, handle: &WriteHandle, stats: &SharedStats) {
    // The accepted socket inherits the listener's non-blocking flag on some platforms; force
    // blocking with deadlines so the read/write below behave predictably.
    let _ = stream.set_nonblocking(false);
    let _ = stream.set_read_timeout(Some(IO_TIMEOUT));
    let _ = stream.set_write_timeout(Some(IO_TIMEOUT));

    let Some(target) = read_request_target(&mut stream) else {
        return;
    };
    if target == "/metrics" {
        let body = render(&stats::gather(stats, handle));
        let _ = write_response(
            &mut stream,
            "200 OK",
            "text/plain; version=0.0.4; charset=utf-8",
            &body,
        );
    } else {
        let _ = write_response(
            &mut stream,
            "404 Not Found",
            "text/plain; charset=utf-8",
            "",
        );
    }
}

/// Reads the request line and returns its target (the path). Only the first line is needed; the
/// read is capped so a header flood cannot exhaust memory.
fn read_request_target(stream: &mut TcpStream) -> Option<String> {
    let mut reader = BufReader::new(stream).take(MAX_REQUEST_BYTES);
    let mut line = String::new();
    reader.read_line(&mut line).ok()?;
    // "GET /metrics HTTP/1.1" -> method, target, version.
    let mut parts = line.split_whitespace();
    let _method = parts.next()?;
    Some(parts.next()?.to_string())
}

/// Writes one HTTP/1.1 response and closes (no keep-alive, one scrape per connection).
fn write_response(
    stream: &mut TcpStream,
    status: &str,
    content_type: &str,
    body: &str,
) -> io::Result<()> {
    let response = format!(
        "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {len}\r\nConnection: close\r\n\r\n{body}",
        len = body.len(),
    );
    stream.write_all(response.as_bytes())
}

/// Renders a snapshot as Prometheus text exposition (format version 0.0.4).
fn render(snap: &StatsSnapshot) -> String {
    let mut out = String::with_capacity(768);
    metric(
        &mut out,
        "tephra_events_total",
        "counter",
        "Total durable events.",
        snap.event_count,
    );
    metric(
        &mut out,
        "tephra_segments",
        "gauge",
        "On-disk log segments in the data directory.",
        snap.segment_count,
    );
    metric(
        &mut out,
        "tephra_disk_bytes",
        "gauge",
        "Total bytes on disk in the data directory.",
        snap.disk_bytes,
    );
    metric(
        &mut out,
        "tephra_uptime_seconds",
        "gauge",
        "Seconds since the server started serving.",
        snap.uptime_seconds,
    );
    metric(
        &mut out,
        "tephra_active_connections",
        "gauge",
        "Connections currently being served.",
        snap.active_connections,
    );
    metric(
        &mut out,
        "tephra_active_subscriptions",
        "gauge",
        "Live subscriptions across all connections.",
        snap.active_subscriptions,
    );
    let version = snap.version;
    out.push_str("# HELP tephra_build_info Build metadata; the value is always 1.\n");
    out.push_str("# TYPE tephra_build_info gauge\n");
    out.push_str(&format!("tephra_build_info{{version=\"{version}\"}} 1\n"));
    out
}

/// Appends the `# HELP` / `# TYPE` / sample lines for one scalar metric.
fn metric(out: &mut String, name: &str, kind: &str, help: &str, value: u64) {
    out.push_str(&format!(
        "# HELP {name} {help}\n# TYPE {name} {kind}\n{name} {value}\n"
    ));
}

#[cfg(test)]
mod tests {
    use super::render;
    use crate::stats::StatsSnapshot;

    fn snapshot() -> StatsSnapshot {
        StatsSnapshot {
            event_count: 5,
            segment_count: 2,
            disk_bytes: 4096,
            uptime_seconds: 12,
            active_connections: 3,
            active_subscriptions: 1,
            version: "9.9.9",
        }
    }

    #[test]
    fn renders_typed_metrics_and_values() {
        let out = render(&snapshot());
        assert!(out.contains("# TYPE tephra_events_total counter\n"));
        assert!(out.contains("\ntephra_events_total 5\n"));
        assert!(out.contains("# TYPE tephra_active_subscriptions gauge\n"));
        assert!(out.contains("\ntephra_active_subscriptions 1\n"));
        assert!(out.contains("tephra_build_info{version=\"9.9.9\"} 1\n"));
    }

    #[test]
    fn every_sample_line_has_a_type_line() {
        let out = render(&snapshot());
        for line in out.lines() {
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            // A sample line: its metric name (before the first space or brace) must have a TYPE.
            let name = line.split([' ', '{']).next().unwrap();
            assert!(
                out.contains(&format!("# TYPE {name} ")),
                "sample {name} has no # TYPE line"
            );
        }
    }
}
