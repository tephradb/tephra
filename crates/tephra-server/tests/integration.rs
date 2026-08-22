//! End-to-end tests over a real socket: a `tephra-server` bound to an ephemeral port, driven
//! by the blocking `tephra-client`.

use std::collections::{BTreeSet, HashMap};
use std::io::{BufReader, BufWriter, ErrorKind, Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::sync::Arc;
use std::sync::mpsc;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use tephra::log::set::{SegmentConfig, SegmentSet};
use tephra::writer::{WriteCoordinator, WriterConfig};

use tempfile::TempDir;
use tephra_client::{
    AppendCondition, AsyncClient, AsyncClientConfig, Client, ClientError, ErrorCode, Event,
    Position, Query, QueryItem, SequencedEvent, SubEvent, Tag, Tags,
};
use tephra_proto::{
    DEFAULT_MAX_FRAME_LEN, PROTOCOL_VERSION, read_frame, tephra as pb, write_frame,
};
use tephra_server::auth::AuthConfig;
use tephra_server::{Server, ServerConfig, ShutdownHandle};
use tokio_stream::StreamExt as _;

/// A server running on its own thread over a temp-dir store, torn down on drop.
struct TestServer {
    addr: SocketAddr,
    shutdown: ShutdownHandle,
    server_thread: Option<JoinHandle<()>>,
    coordinator: Option<WriteCoordinator>,
    _dir: TempDir,
}

impl TestServer {
    fn start() -> TestServer {
        TestServer::start_with(
            ServerConfig::default(),
            16 * 1024 * 1024,
            WriterConfig::default(),
        )
    }

    fn start_with(
        server_config: ServerConfig,
        segment_size: usize,
        writer_config: WriterConfig,
    ) -> TestServer {
        let dir = TempDir::new().unwrap();
        let set = SegmentSet::open(dir.path(), SegmentConfig::new(segment_size)).unwrap();
        let (coordinator, handle) = WriteCoordinator::start(set, writer_config).unwrap();
        let server = Server::bind("127.0.0.1:0", handle, server_config)
            .unwrap()
            .with_data_dir(dir.path());
        let addr = server.local_addr();
        let shutdown = server.shutdown_handle();
        let server_thread = thread::spawn(move || server.run().expect("server run"));
        TestServer {
            addr,
            shutdown,
            server_thread: Some(server_thread),
            coordinator: Some(coordinator),
            _dir: dir,
        }
    }

    /// A plaintext server that requires one of `tokens` in each connection's opening Hello. Auth is
    /// transport-agnostic, so a plaintext server exercises the same handshake enforcement as TLS
    /// without the certificate setup.
    fn start_with_auth(tokens: Vec<String>) -> TestServer {
        let dir = TempDir::new().unwrap();
        let set = SegmentSet::open(dir.path(), SegmentConfig::new(16 * 1024 * 1024)).unwrap();
        let (coordinator, handle) = WriteCoordinator::start(set, WriterConfig::default()).unwrap();
        let server = Server::bind("127.0.0.1:0", handle, ServerConfig::default())
            .unwrap()
            .with_data_dir(dir.path())
            .with_auth(Arc::new(AuthConfig::new(tokens)));
        let addr = server.local_addr();
        let shutdown = server.shutdown_handle();
        let server_thread = thread::spawn(move || server.run().expect("server run"));
        TestServer {
            addr,
            shutdown,
            server_thread: Some(server_thread),
            coordinator: Some(coordinator),
            _dir: dir,
        }
    }

    fn client(&self) -> Client {
        Client::connect(self.addr).unwrap()
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

// --- test helpers (build clean-typed values) ---

/// Builds a validated event.
fn ev(ty: &str, tags: &[&str], payload: &[u8]) -> Event {
    Event::new(ty, tags.iter().copied(), payload).unwrap()
}

/// A validated tag set.
fn tag_set(tags: &[&str]) -> Tags {
    Tags::new(
        tags.iter()
            .map(|tag| Tag::new(*tag).unwrap())
            .collect::<Vec<_>>(),
    )
    .unwrap()
}

/// A query matching events that carry all of `tags` (any type).
fn tag_query(tags: &[&str]) -> Query {
    Query::item(QueryItem::with_tags(tag_set(tags)))
}

/// The uniqueness guard over `tags`: fail if any event already carries all of them.
fn tag_condition(tags: &[&str]) -> AppendCondition {
    AppendCondition::new(tag_query(tags))
}

/// Seeds `batches * per_batch` events of `payload_len` bytes each, one atomic append (one fsync) per
/// batch, so a multi-megabyte corpus is written quickly.
fn seed_bulk(client: &mut Client, batches: usize, per_batch: usize, payload_len: usize) {
    let payload = vec![b'x'; payload_len];
    for _ in 0..batches {
        let events: Vec<Event> = (0..per_batch)
            .map(|_| ev("E", &["hot"], &payload))
            .collect();
        client.append(events, None).unwrap();
    }
}

/// The positions of a batch of sequenced events as plain `u64`s.
fn positions(events: &[SequencedEvent]) -> Vec<u64> {
    events.iter().map(|e| e.position().get()).collect()
}

/// Collects a sequenced event's fields into owned, comparable values.
fn fields(sequenced: &SequencedEvent) -> (u64, String, Vec<String>, Vec<u8>) {
    let ev = sequenced.event();
    (
        sequenced.position().get(),
        ev.event_type().to_string(),
        ev.tags().map(str::to_string).collect(),
        ev.payload().to_vec(),
    )
}

#[test]
fn append_then_read_round_trips_events() {
    let ts = TestServer::start();
    let mut client = ts.client();

    let range = client
        .append(
            [ev("Enrolled", &["course:c1", "student:s1"], b"payload-1")],
            None,
        )
        .unwrap();
    assert_eq!(range.first.get(), 1);
    assert_eq!(range.last.get(), 1);

    client
        .append([ev("Renamed", &["course:c1"], b"payload-2")], None)
        .unwrap();

    let (events, watermark) = client.read_all(Query::all(), Position::ZERO, None).unwrap();
    assert_eq!(watermark.get(), 2);
    assert_eq!(events.len(), 2);

    let (pos, ty, tags, payload) = fields(&events[0]);
    assert_eq!(pos, 1);
    assert_eq!(ty, "Enrolled");
    assert_eq!(tags, vec!["course:c1", "student:s1"]);
    assert_eq!(payload, b"payload-1");

    let (pos, ty, _, payload) = fields(&events[1]);
    assert_eq!(pos, 2);
    assert_eq!(ty, "Renamed");
    assert_eq!(payload, b"payload-2");
}

#[test]
fn stats_reports_event_count_and_disk_usage() {
    let ts = TestServer::start();
    let mut client = ts.client();

    // Empty store: no events, but the first segment is already on disk.
    let empty = client.stats().unwrap();
    assert_eq!(empty.event_count, 0);
    assert!(empty.segment_count >= 1, "the first segment file exists");
    assert!(empty.disk_bytes > 0, "the segment is allocated on disk");
    assert!(!empty.version.is_empty());
    assert!(empty.active_connections >= 1, "this connection is counted");
    assert_eq!(empty.active_subscriptions, 0);

    for i in 0..5 {
        client
            .append([ev("E", &[&format!("k:{i}")], b"payload")], None)
            .unwrap();
    }

    let after = client.stats().unwrap();
    assert_eq!(after.event_count, 5, "positions are dense and 1-based");
    assert!(after.disk_bytes >= empty.disk_bytes);
}

#[test]
fn stats_counts_active_subscriptions() {
    let ts = TestServer::start();
    let mut appender = ts.client();
    appender.append([ev("E", &["k:1"], b"")], None).unwrap();

    let (item_tx, item_rx) = mpsc::channel();
    let (cancel_tx, cancel_rx) = mpsc::channel();
    let subscriber = spawn_subscriber(
        ts.client(),
        Query::all(),
        Position::ZERO,
        item_tx,
        cancel_tx,
    );
    let cancel = cancel_rx.recv().unwrap();
    // The first delivered event proves the subscription is registered server-side.
    match item_rx.recv().unwrap() {
        Ok(SubEvent::Event(_)) => {}
        other => panic!("expected the first event, got {other:?}"),
    }

    let mut stats_client = ts.client();
    assert_eq!(poll_active_subscriptions(&mut stats_client, 1), 1);

    // Cancelling tears the subscription down, and the gauge returns to zero.
    cancel.cancel();
    subscriber.join().unwrap();
    assert_eq!(poll_active_subscriptions(&mut stats_client, 0), 0);
}

/// Polls the active-subscription gauge until it reaches `expected` (the server decrements it on
/// its own worker thread, off the client's join), returning the last value seen.
fn poll_active_subscriptions(client: &mut Client, expected: u64) -> u64 {
    let mut seen = client.stats().unwrap().active_subscriptions;
    for _ in 0..100 {
        if seen == expected {
            break;
        }
        thread::sleep(Duration::from_millis(10));
        seen = client.stats().unwrap().active_subscriptions;
    }
    seen
}

#[test]
fn max_connections_refuses_over_the_cap() {
    // A one-connection server: the first client holds the only slot, and a second connection is
    // closed before a request is served, so a client fleet cannot exhaust server threads.
    let server_config = ServerConfig {
        max_connections: 1,
        ..ServerConfig::default()
    };
    let ts = TestServer::start_with(server_config, 16 * 1024 * 1024, WriterConfig::default());

    // The first client takes the sole slot; a round-trip proves the server is serving it, so the
    // slot is held before the second connection is attempted.
    let mut first = ts.client();
    let stats = first.stats().unwrap();
    assert_eq!(stats.active_connections, 1);
    assert_eq!(stats.max_connections, 1);
    assert_eq!(stats.connections_refused, 0);

    // A second connection completes at the TCP layer but is refused (closed) by the server before
    // its opening Hello can complete, so connecting over the cap fails.
    let second = Client::connect(ts.addr);
    assert!(
        second.is_err(),
        "a connection over the cap must be refused, not served"
    );

    // The refusal is counted and the live gauge never exceeded the cap (the refused connection
    // never took a slot). The counter is bumped on the accept thread, so poll for visibility.
    let stats = poll_stats(&mut first, |s| s.connections_refused, 1);
    assert!(
        stats.connections_refused >= 1,
        "the refusal must be counted"
    );
    assert_eq!(stats.active_connections, 1, "the cap was never exceeded");
}

/// Polls the stats (over the given client's own connection) until `get` reads at least `expected`,
/// returning the last snapshot. Server-side gauges are bumped on other threads, so a counter may
/// lag the observable effect briefly.
fn poll_stats(
    client: &mut Client,
    get: impl Fn(&tephra_client::Stats) -> u64,
    expected: u64,
) -> tephra_client::Stats {
    let mut stats = client.stats().unwrap();
    for _ in 0..300 {
        if get(&stats) >= expected {
            break;
        }
        thread::sleep(Duration::from_millis(20));
        stats = client.stats().unwrap();
    }
    stats
}

/// Connects a raw socket, sends `prelude`, then waits for the server to close it (via the reaper),
/// asserting the close is observed within `within`. The socket read timeout is set to `within * 3`,
/// so the bound has teeth: a never-reaped connection blocks the full read timeout instead and fails
/// the `elapsed < within` assertion rather than passing on the client's own timeout.
fn expect_reaped_within(addr: SocketAddr, prelude: &[u8], within: Duration) {
    let mut sock = TcpStream::connect(addr).unwrap();
    if !prelude.is_empty() {
        sock.write_all(prelude).unwrap();
        sock.flush().unwrap();
    }
    sock.set_read_timeout(Some(within * 3)).unwrap();
    let start = Instant::now();
    let mut buf = [0u8; 1];
    let read = sock.read(&mut buf);
    let elapsed = start.elapsed();
    assert!(
        matches!(read, Ok(0)) || read.is_err(),
        "the reaper must close the connection, got {read:?}"
    );
    assert!(
        elapsed < within,
        "reaping took {elapsed:?}, expected under {within:?}"
    );
}

#[test]
fn a_stalled_partial_frame_is_reaped() {
    // A short incomplete-frame timeout: a client that sends a length prefix then stalls mid-frame
    // is reaped promptly, so a slow-loris cannot pin a connection slot.
    let server_config = ServerConfig {
        incomplete_frame_timeout: Duration::from_secs(1),
        ..ServerConfig::default()
    };
    let ts = TestServer::start_with(server_config, 16 * 1024 * 1024, WriterConfig::default());

    // A 4-byte length prefix promising a 1 KiB body, then nothing. Reaped within ~2s (deadline plus
    // one poll interval); the 5s bound fails if reaping never happens.
    expect_reaped_within(ts.addr, &1024u32.to_be_bytes(), Duration::from_secs(5));

    let mut client = ts.client();
    let stats = poll_stats(&mut client, |s| s.connections_reaped, 1);
    assert!(stats.connections_reaped >= 1, "the reap must be counted");
}

#[test]
fn a_slow_loris_trickle_is_reaped() {
    // The core defense: a body trickled one byte at a time, each delivered within the per-read
    // socket timeout, must still be reaped by the wall-clock incomplete-frame deadline. A per-read
    // timeout alone never fires here, since every byte resets it.
    let server_config = ServerConfig {
        incomplete_frame_timeout: Duration::from_secs(2),
        ..ServerConfig::default()
    };
    let ts = TestServer::start_with(server_config, 16 * 1024 * 1024, WriterConfig::default());

    let mut sock = TcpStream::connect(ts.addr).unwrap();
    // Announce a large body, then trickle it a byte every 300ms, well under the 1s poll interval.
    sock.write_all(&4096u32.to_be_bytes()).unwrap();
    sock.flush().unwrap();
    sock.set_read_timeout(Some(Duration::from_secs(15)))
        .unwrap();

    let mut trickler = sock.try_clone().unwrap();
    let trickle = thread::spawn(move || {
        for _ in 0..4096 {
            if trickler.write_all(&[0u8]).is_err() || trickler.flush().is_err() {
                break;
            }
            thread::sleep(Duration::from_millis(300));
        }
    });

    let start = Instant::now();
    let mut buf = [0u8; 1];
    let read = sock.read(&mut buf);
    let elapsed = start.elapsed();
    assert!(
        matches!(read, Ok(0)) || read.is_err(),
        "the trickle must be reaped, got {read:?}"
    );
    // Reaped by the 2s wall-clock deadline (plus a poll interval and slack), not the ~20 minutes it
    // would take to trickle 4096 bytes at 300ms each.
    assert!(
        elapsed < Duration::from_secs(8),
        "trickle reaping took {elapsed:?}, deadline was 2s"
    );
    let _ = trickle.join();

    let mut client = ts.client();
    assert!(poll_stats(&mut client, |s| s.connections_reaped, 1).connections_reaped >= 1);
}

#[test]
fn a_partial_first_frame_is_reaped_by_the_handshake_timeout() {
    // handshake_timeout with the incomplete-frame timeout disabled: a connection that sends a single
    // byte (a partial length prefix) then stalls must still be reaped, so a partial first frame
    // cannot bypass the handshake deadline.
    let server_config = ServerConfig {
        handshake_timeout: Duration::from_secs(1),
        incomplete_frame_timeout: Duration::ZERO,
        ..ServerConfig::default()
    };
    let ts = TestServer::start_with(server_config, 16 * 1024 * 1024, WriterConfig::default());

    expect_reaped_within(ts.addr, &[0u8], Duration::from_secs(5));

    let mut client = ts.client();
    assert!(poll_stats(&mut client, |s| s.connections_reaped, 1).connections_reaped >= 1);
}

#[test]
fn idle_timeout_reaps_a_silent_unestablished_connection() {
    // idle_timeout alone (handshake and incomplete disabled): a connection that connects and sends
    // nothing is still reaped, since the idle clock starts at accept rather than at the first frame.
    let server_config = ServerConfig {
        idle_timeout: Duration::from_secs(1),
        incomplete_frame_timeout: Duration::ZERO,
        ..ServerConfig::default()
    };
    let ts = TestServer::start_with(server_config, 16 * 1024 * 1024, WriterConfig::default());

    expect_reaped_within(ts.addr, &[], Duration::from_secs(5));

    let mut client = ts.client();
    assert!(poll_stats(&mut client, |s| s.connections_reaped, 1).connections_reaped >= 1);
}

#[test]
fn idle_timeout_reaps_a_stalled_mid_frame_when_incomplete_is_disabled() {
    // With the incomplete-frame timeout disabled and idle_timeout set, a connection that sends part
    // of a frame then stalls mid-frame must still be reaped by idle_timeout (a stall is "no complete
    // frame"), rather than being pinned indefinitely because idle was only checked at a boundary.
    let server_config = ServerConfig {
        incomplete_frame_timeout: Duration::ZERO,
        idle_timeout: Duration::from_secs(1),
        ..ServerConfig::default()
    };
    let ts = TestServer::start_with(server_config, 16 * 1024 * 1024, WriterConfig::default());

    // A length prefix promising a body, then a stall: mid-frame (in progress), not a boundary.
    expect_reaped_within(ts.addr, &1024u32.to_be_bytes(), Duration::from_secs(5));

    let mut client = ts.client();
    assert!(poll_stats(&mut client, |s| s.connections_reaped, 1).connections_reaped >= 1);
}

#[test]
fn an_idle_connection_is_reaped_but_a_subscription_is_exempt() {
    // A short idle timeout: a connection sitting idle with no work is reaped, but a live
    // subscription counts as activity and is left alone.
    let server_config = ServerConfig {
        idle_timeout: Duration::from_secs(1),
        ..ServerConfig::default()
    };
    let ts = TestServer::start_with(server_config, 16 * 1024 * 1024, WriterConfig::default());

    let mut appender = ts.client();
    appender.append([ev("E", &["k:1"], b"x")], None).unwrap();

    // A subscription must survive well past the idle timeout (it is activity, so it is exempt).
    let (item_tx, item_rx) = mpsc::channel();
    let (cancel_tx, cancel_rx) = mpsc::channel();
    let subscriber = spawn_subscriber(
        ts.client(),
        Query::all(),
        Position::ZERO,
        item_tx,
        cancel_tx,
    );
    let cancel = cancel_rx.recv().unwrap();
    match item_rx.recv().unwrap() {
        Ok(SubEvent::Event(_)) => {}
        other => panic!("expected the seeded event, got {other:?}"),
    }

    // A plain connection that goes idle (no request in flight, no subscription) is reaped.
    let mut idle = Client::connect(ts.addr).unwrap();
    idle.stats().unwrap();
    thread::sleep(Duration::from_secs(3));
    assert!(
        idle.stats().is_err(),
        "an idle connection past the idle timeout must be reaped"
    );

    // The subscription is still live: a new append is delivered to it (proving it was not reaped).
    // Skip the caught-up live-edge marker emitted while the stream was idle at the tip.
    let mut appender2 = ts.client();
    appender2.append([ev("E", &["k:2"], b"y")], None).unwrap();
    loop {
        match item_rx.recv().unwrap() {
            Ok(SubEvent::Event(_)) => break,
            Ok(SubEvent::CaughtUp(_)) => continue,
            other => panic!("the subscription was reaped despite being active: {other:?}"),
        }
    }

    cancel.cancel();
    subscriber.join().unwrap();
}

#[cfg(feature = "metrics")]
#[test]
fn metrics_endpoint_serves_prometheus_exposition() {
    let dir = TempDir::new().unwrap();
    let set = SegmentSet::open(dir.path(), SegmentConfig::new(16 * 1024 * 1024)).unwrap();
    let (coordinator, handle) = WriteCoordinator::start(set, WriterConfig::default()).unwrap();
    let server = Server::bind("127.0.0.1:0", handle, ServerConfig::default())
        .unwrap()
        .with_data_dir(dir.path())
        .with_metrics_addr("127.0.0.1:0")
        .unwrap();
    let data_addr = server.local_addr();
    let metrics_addr = server.metrics_local_addr().unwrap();
    let shutdown = server.shutdown_handle();
    let server_thread = thread::spawn(move || server.run().expect("server run"));

    // Two appends, so the exposition reflects a non-zero, exact event count.
    let mut client = Client::connect(data_addr).unwrap();
    client.append([ev("E", &["k:1"], b"x")], None).unwrap();
    client.append([ev("E", &["k:2"], b"y")], None).unwrap();

    let ok = http_get(metrics_addr, "/metrics");
    assert!(ok.starts_with("HTTP/1.1 200"), "expected 200, got: {ok}");
    assert!(ok.contains("# TYPE tephra_events_total counter"));
    assert!(ok.contains("\ntephra_events_total 2\n"), "body: {ok}");
    assert!(ok.contains("tephra_active_connections"));

    // Any other path is a 404.
    let missing = http_get(metrics_addr, "/nope");
    assert!(
        missing.starts_with("HTTP/1.1 404"),
        "expected 404, got: {missing}"
    );

    shutdown.shutdown();
    server_thread.join().unwrap();
    coordinator.shutdown();
}

/// Sends one HTTP/1.1 `GET path` and returns the full response (the server closes after replying).
#[cfg(feature = "metrics")]
fn http_get(addr: SocketAddr, path: &str) -> String {
    use std::io::Read as _;

    let mut stream = TcpStream::connect(addr).unwrap();
    let request = format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n");
    stream.write_all(request.as_bytes()).unwrap();
    let mut response = String::new();
    stream.read_to_string(&mut response).unwrap();
    response
}

#[test]
fn tag_query_filters_the_read() {
    let ts = TestServer::start();
    let mut client = ts.client();
    client.append([ev("A", &["course:c1"], b"")], None).unwrap();
    client.append([ev("B", &["course:c2"], b"")], None).unwrap();
    client.append([ev("C", &["course:c1"], b"")], None).unwrap();

    let (events, _) = client
        .read_all(tag_query(&["course:c1"]), Position::ZERO, None)
        .unwrap();
    assert_eq!(positions(&events), vec![1, 3]);
}

#[test]
fn read_after_skips_the_prefix() {
    let ts = TestServer::start();
    let mut client = ts.client();
    for i in 0..5 {
        client
            .append([ev("E", &[&format!("k:{i}")], b"")], None)
            .unwrap();
    }

    let (events, watermark) = client
        .read_all(Query::all(), Position::new(2), None)
        .unwrap();
    assert_eq!(positions(&events), vec![3, 4, 5]);
    assert_eq!(watermark.get(), 5);
}

#[test]
fn empty_read_yields_only_a_watermark() {
    let ts = TestServer::start();
    let mut client = ts.client();
    client.append([ev("E", &["k:1"], b"")], None).unwrap();

    // Nothing after the tip.
    let (events, watermark) = client
        .read_all(Query::all(), Position::new(1), None)
        .unwrap();
    assert!(events.is_empty());
    assert_eq!(watermark.get(), 1);
}

#[test]
fn read_limit_caps_the_result_and_paginates() {
    let ts = TestServer::start();
    let mut client = ts.client();

    // One selective entity with a known history, interleaved with noise the query must skip,
    // so the limit and pagination exercise a real filter rather than a dense prefix.
    let total = 25u64;
    for i in 0..total {
        client
            .append(
                [ev("Enrolled", &["student:s0"], format!("e{i}").as_bytes())],
                None,
            )
            .unwrap();
        client
            .append([ev("Enrolled", &["student:s1"], b"noise")], None)
            .unwrap();
    }
    let query = || tag_query(&["student:s0"]);

    // A limit returns exactly its cap, the leading prefix of the full selective history.
    let (all, _) = client.read_all(query(), Position::ZERO, None).unwrap();
    assert_eq!(all.len() as u64, total);
    let (page, _) = client.read_all(query(), Position::ZERO, Some(10)).unwrap();
    assert_eq!(positions(&page), positions(&all)[..10].to_vec());

    // A cap above the history returns all of it, no more.
    let (over, _) = client
        .read_all(query(), Position::ZERO, Some(total + 100))
        .unwrap();
    assert_eq!(positions(&over), positions(&all));

    // Paginate the whole selective history with `after` + `limit`: the concatenation equals
    // the unlimited read exactly, with no gap and no duplicate at any seam.
    let page_size = 7;
    let mut after = Position::ZERO;
    let mut tiled: Vec<u64> = Vec::new();
    loop {
        let (chunk, _) = client.read_all(query(), after, Some(page_size)).unwrap();
        if chunk.is_empty() {
            break;
        }
        after = chunk.last().unwrap().position();
        tiled.extend(positions(&chunk));
    }
    assert_eq!(tiled, positions(&all));

    // A zero limit yields nothing but still terminates cleanly with the pinned watermark.
    let (none, watermark) = client.read_all(query(), Position::ZERO, Some(0)).unwrap();
    assert!(none.is_empty());
    assert_eq!(watermark.get(), 2 * total);
}

#[test]
fn read_back_streams_newest_first_and_paginates() {
    let ts = TestServer::start();
    let mut client = ts.client();

    // A selective history interleaved with noise, so a reverse read exercises the filter, not
    // just a dense suffix.
    let total = 25u64;
    for i in 0..total {
        client
            .append(
                [ev("Enrolled", &["student:s0"], format!("e{i}").as_bytes())],
                None,
            )
            .unwrap();
        client
            .append([ev("Enrolled", &["student:s1"], b"noise")], None)
            .unwrap();
    }
    let query = || tag_query(&["student:s0"]);

    // read_all_back over the whole history equals read_all reversed, strictly descending.
    let (forward, _) = client.read_all(query(), Position::ZERO, None).unwrap();
    let mut want = positions(&forward);
    want.reverse();
    let (back, _) = client.read_all_back(query(), Position::MAX, None).unwrap();
    assert_eq!(positions(&back), want);
    assert!(
        positions(&back).windows(2).all(|w| w[0] > w[1]),
        "descending by position"
    );

    // A limit takes the newest N.
    let (page, _) = client
        .read_all_back(query(), Position::MAX, Some(10))
        .unwrap();
    assert_eq!(positions(&page), want[..10].to_vec());

    // Paginate newest-first with `before` + `limit`: the concatenation equals the full reverse,
    // with no gap and no duplicate at any seam.
    let page_size = 7;
    let mut before = Position::MAX;
    let mut tiled: Vec<u64> = Vec::new();
    loop {
        let (chunk, _) = client
            .read_all_back(query(), before, Some(page_size))
            .unwrap();
        if chunk.is_empty() {
            break;
        }
        before = chunk.last().unwrap().position(); // the oldest position in this page
        tiled.extend(positions(&chunk));
    }
    assert_eq!(tiled, want);

    // `before = Position::ZERO` is the exclusive-upper "before everything" bound, so it returns
    // nothing over the wire, exactly as the embedded `read_back(Position::ZERO)` does. This pins
    // that the transport does not reinterpret a zero cursor as "from the tip".
    let (empty, watermark) = client.read_all_back(query(), Position::ZERO, None).unwrap();
    assert!(empty.is_empty());
    assert_eq!(watermark.get(), 2 * total);
}

#[test]
fn durable_append_conflict_is_reported_and_not_retryable() {
    let ts = TestServer::start();
    let mut client = ts.client();

    // Reserve a unique username, guarded so a second identical reservation fails.
    client
        .append(
            [ev("Reserved", &["username:alice"], b"{}")],
            Some(tag_condition(&["username:alice"])),
        )
        .unwrap();

    let err = client
        .append(
            [ev("Reserved", &["username:alice"], b"{}")],
            Some(tag_condition(&["username:alice"])),
        )
        .unwrap_err();
    match err {
        ClientError::Server {
            code,
            retryable,
            conflict_position,
            ..
        } => {
            assert_eq!(code, ErrorCode::Conflict);
            assert!(!retryable, "a durable conflict is terminal");
            assert_eq!(conflict_position, Some(Position::new(1)));
        }
        other => panic!("expected a server conflict, got {other:?}"),
    }
}

#[test]
fn existence_clause_conflict_is_reported_as_already_exists() {
    let ts = TestServer::start();
    let mut client = ts.client();

    // Commit a command carrying its dedupe key.
    client
        .append([ev("OrderPlaced", &["cmd:abc"], b"{}")], None)
        .unwrap();

    // A re-application guarded by `fail_if_exists` on the dedupe key is rejected with
    // AlreadyExists (distinct from a boundary Conflict), terminal, at the original position.
    let condition = AppendCondition::exists_only(tag_query(&["cmd:abc"]));
    let err = client
        .append([ev("OrderPlaced", &["cmd:abc"], b"{}")], Some(condition))
        .unwrap_err();
    match err {
        ClientError::Server {
            code,
            retryable,
            conflict_position,
            ..
        } => {
            assert_eq!(code, ErrorCode::AlreadyExists);
            assert!(!retryable, "a durable duplicate is terminal");
            assert_eq!(conflict_position, Some(Position::new(1)));
        }
        other => panic!("expected an AlreadyExists conflict, got {other:?}"),
    }
}

#[test]
fn malformed_wire_event_maps_to_bad_request() {
    // The clean client validates before sending, so a malformed event cannot go through
    // `append`. A hand-built wire frame with an empty event type exercises the server's
    // BAD_REQUEST path directly (defense in depth for other, non-validating clients).
    let ts = TestServer::start();

    let mut event = pb::Event::new();
    event.set_type(""); // empty type: rejected by tephra's own constructor server-side.
    event.tags_mut().push("k:1");
    let mut append = pb::AppendRequest::new();
    append.events_mut().push(event);
    let mut request = pb::Request::new();
    request.set_request_id(1);
    request.set_append(append);

    let response = send_raw_request(ts.addr, &request);
    match response.kind() {
        pb::response::KindOneof::Error(err) => assert_eq!(err.code(), pb::ErrorCode::BadRequest),
        other => panic!("expected a bad-request error, got {other:?}"),
    }
}

#[test]
fn empty_append_maps_to_empty() {
    let ts = TestServer::start();
    let mut client = ts.client();

    // An append with no events -> EMPTY.
    let err = client.append(Vec::new(), None).unwrap_err();
    match err {
        ClientError::Server { code, .. } => assert_eq!(code, ErrorCode::Empty),
        other => panic!("expected empty, got {other:?}"),
    }
}

#[test]
fn large_streamed_read_returns_every_event_in_order() {
    // Tiny segments (several sealed indexes), a small writer batch to fit them, and a small
    // server read batch so the result spans many `read_events` frames.
    let server_config = ServerConfig {
        read_batch_events: 7,
        read_batch_bytes: 64,
        ..ServerConfig::default()
    };
    let writer_config = WriterConfig {
        queue_capacity: 64,
        max_batch_records: 64,
        max_batch_bytes: 256,
        ..WriterConfig::default()
    };
    let ts = TestServer::start_with(server_config, 512, writer_config);
    let mut client = ts.client();

    let total = 200u64;
    for i in 0..total {
        client
            .append([ev("E", &[&format!("n:{i}")], b"x")], None)
            .unwrap();
    }

    let (events, watermark) = client.read_all(Query::all(), Position::ZERO, None).unwrap();
    assert_eq!(watermark.get(), total);
    let expected: Vec<u64> = (1..=total).collect();
    assert_eq!(positions(&events), expected);
}

#[test]
fn streaming_read_iterator_yields_incrementally() {
    let ts = TestServer::start();
    let mut client = ts.client();
    for i in 0..10 {
        client
            .append([ev("E", &[&format!("k:{i}")], b"")], None)
            .unwrap();
    }

    let mut stream = client.read(Query::all(), Position::ZERO, None).unwrap();
    let mut count = 0;
    for item in stream.by_ref() {
        item.unwrap();
        count += 1;
    }
    assert_eq!(count, 10);
    assert_eq!(stream.watermark(), Some(Position::new(10)));
}

#[test]
fn dropping_a_read_early_keeps_the_connection_usable() {
    // One event per frame, so stopping after the first leaves many unread frames in the
    // socket: the client's drain-on-drop must consume them, or the next read on the same
    // connection would return this read's leftovers.
    let server_config = ServerConfig {
        read_batch_events: 1,
        read_batch_bytes: 1,
        ..ServerConfig::default()
    };
    let ts = TestServer::start_with(server_config, 16 * 1024 * 1024, WriterConfig::default());
    let mut client = ts.client();
    for i in 0..20 {
        client
            .append([ev("E", &[&format!("k:{i}")], b"")], None)
            .unwrap();
    }

    {
        let mut stream = client.read(Query::all(), Position::ZERO, None).unwrap();
        let first = stream.next().unwrap().unwrap();
        assert_eq!(first.position().get(), 1);
        // `stream` is dropped here, mid-read, with 19 events plus the terminator unread.
    }

    // The next read on the same connection returns complete, correct results.
    let (events, watermark) = client.read_all(Query::all(), Position::ZERO, None).unwrap();
    assert_eq!(watermark.get(), 20);
    assert_eq!(positions(&events), (1..=20).collect::<Vec<_>>());
}

#[test]
fn oversized_request_gets_a_too_large_error_not_a_disconnect() {
    // A server with a tiny frame cap; the client keeps its large default, so it happily
    // writes a frame the server must reject.
    let server_config = ServerConfig {
        max_frame_len: 64,
        ..ServerConfig::default()
    };
    let ts = TestServer::start_with(server_config, 16 * 1024 * 1024, WriterConfig::default());
    let mut client = ts.client();

    let big = vec![b'x'; 256];
    let err = client.append([ev("E", &["k:1"], &big)], None).unwrap_err();
    match err {
        ClientError::Server { code, .. } => assert_eq!(code, ErrorCode::TooLarge),
        other => panic!("expected a TooLarge server error, got {other:?}"),
    }
}

#[test]
fn concurrent_clients_stay_consistent() {
    let ts = TestServer::start();
    let threads = 4u64;
    let per_thread = 50u64;

    let handles: Vec<_> = (0..threads)
        .map(|t| {
            let addr = ts.addr;
            thread::spawn(move || {
                let mut client = Client::connect(addr).unwrap();
                for i in 0..per_thread {
                    client
                        .append(
                            [ev("Appended", &[&format!("t:{t}"), &format!("i:{i}")], b"")],
                            None,
                        )
                        .unwrap();
                }
            })
        })
        .collect();
    for handle in handles {
        handle.join().unwrap();
    }

    let mut client = ts.client();
    let (events, watermark) = client.read_all(Query::all(), Position::ZERO, None).unwrap();
    let total = threads * per_thread;
    assert_eq!(watermark.get(), total);
    assert_eq!(events.len() as u64, total);
    // Positions are dense 1..=total regardless of interleaving.
    let mut sorted = positions(&events);
    sorted.sort_unstable();
    assert_eq!(sorted, (1..=total).collect::<Vec<_>>());
}

#[test]
fn graceful_shutdown_stops_accepting_and_returns() {
    let ts = TestServer::start();
    let addr = ts.addr;
    let mut client = ts.client();
    client.append([ev("E", &["k:1"], b"")], None).unwrap();

    // Signal shutdown; the accept loop stops and `run` returns (joined by Drop), which drops
    // the listener and closes the port.
    ts.shutdown.shutdown();
    thread::sleep(Duration::from_millis(50));

    // A new connection is refused, or if it slips in before the port closes, its request
    // fails: either way the server no longer accepts work.
    let refused = match Client::connect(addr) {
        Err(_) => true,
        Ok(mut client) => client.append([ev("E", &["k:2"], b"")], None).is_err(),
    };
    assert!(refused, "server should refuse work after shutdown");
}

// ------------------------------- subscriptions -------------------------------

/// Spawns a subscriber on its own thread (the stream borrows its client, so both live there).
/// It forwards every item over `items` and hands back a `SubscribeCancel` on `cancel` so the
/// test can stop it. Returns the join handle carrying nothing (results flow over `items`).
fn spawn_subscriber(
    mut client: Client,
    query: Query,
    after: Position,
    items: mpsc::Sender<Result<SubEvent, String>>,
    cancel: mpsc::Sender<tephra_client::SubscribeCancel>,
) -> JoinHandle<()> {
    thread::spawn(move || {
        let (stream, canceller) = client.subscribe(query, after).unwrap();
        cancel.send(canceller).unwrap();
        for item in stream {
            match item {
                Ok(event) => {
                    if items.send(Ok(event)).is_err() {
                        break;
                    }
                }
                Err(err) => {
                    let _ = items.send(Err(err.to_string()));
                    break;
                }
            }
        }
    })
}

#[test]
fn subscribe_streams_catch_up_then_live() {
    let ts = TestServer::start();

    // Two events already durable before the subscription starts.
    let mut appender = ts.client();
    appender.append([ev("E", &["k:1"], b"a")], None).unwrap();
    appender.append([ev("E", &["k:1"], b"b")], None).unwrap();

    let (item_tx, item_rx) = mpsc::channel();
    let (cancel_tx, cancel_rx) = mpsc::channel();
    let subscriber = spawn_subscriber(
        ts.client(),
        Query::all(),
        Position::ZERO,
        item_tx,
        cancel_tx,
    );
    let cancel = cancel_rx.recv().unwrap();

    let mut positions = Vec::new();
    let mut caught_up = Vec::new();

    // Catch-up phase: the two pre-appended events arrive in order, then a caught-up marker once
    // the subscription drains to the live edge. Wait for that marker before appending the live
    // events below, so those appends cannot race ahead of the live-edge signal and starve it.
    while positions.len() < 2 || caught_up.is_empty() {
        match item_rx.recv().unwrap() {
            Ok(SubEvent::Event(ev)) => positions.push(ev.position().get()),
            Ok(SubEvent::CaughtUp(w)) => caught_up.push(w),
            Err(err) => panic!("subscription error: {err}"),
        }
    }

    // Live phase: two more appended after the subscription is running.
    appender.append([ev("E", &["k:1"], b"c")], None).unwrap();
    appender.append([ev("E", &["k:1"], b"d")], None).unwrap();

    while positions.len() < 4 {
        match item_rx.recv().unwrap() {
            Ok(SubEvent::Event(ev)) => positions.push(ev.position().get()),
            Ok(SubEvent::CaughtUp(w)) => caught_up.push(w),
            Err(err) => panic!("subscription error: {err}"),
        }
    }

    assert_eq!(
        positions,
        vec![1, 2, 3, 4],
        "no gap or duplicate across the catch-up/live boundary"
    );
    assert!(
        !caught_up.is_empty(),
        "expected at least one caught-up marker at the live edge"
    );
    assert!(
        caught_up.windows(2).all(|w| w[0] <= w[1]),
        "caught-up watermarks are non-decreasing"
    );

    cancel.cancel();
    subscriber.join().unwrap();
}

#[test]
fn subscribe_from_mid_position_skips_the_prefix() {
    let ts = TestServer::start();
    let mut appender = ts.client();
    for i in 0..4 {
        appender
            .append([ev("E", &[&format!("k:{i}")], b"")], None)
            .unwrap();
    }

    let (item_tx, item_rx) = mpsc::channel();
    let (cancel_tx, cancel_rx) = mpsc::channel();
    // Resume after position 2: only 3 and 4 should arrive.
    let subscriber = spawn_subscriber(
        ts.client(),
        Query::all(),
        Position::new(2),
        item_tx,
        cancel_tx,
    );
    let cancel = cancel_rx.recv().unwrap();

    let mut positions = Vec::new();
    while positions.len() < 2 {
        match item_rx.recv().unwrap() {
            Ok(SubEvent::Event(ev)) => positions.push(ev.position().get()),
            Ok(SubEvent::CaughtUp(_)) => {}
            Err(err) => panic!("subscription error: {err}"),
        }
    }
    assert_eq!(positions, vec![3, 4]);

    cancel.cancel();
    subscriber.join().unwrap();
}

#[test]
fn cancel_ends_a_live_subscription() {
    let ts = TestServer::start();
    let mut appender = ts.client();
    appender.append([ev("E", &["k:1"], b"")], None).unwrap();

    let (item_tx, item_rx) = mpsc::channel();
    let (cancel_tx, cancel_rx) = mpsc::channel();
    let subscriber = spawn_subscriber(
        ts.client(),
        Query::all(),
        Position::ZERO,
        item_tx,
        cancel_tx,
    );
    let cancel = cancel_rx.recv().unwrap();

    // Receive the one durable event.
    match item_rx.recv().unwrap() {
        Ok(SubEvent::Event(ev)) => assert_eq!(ev.position().get(), 1),
        other => panic!("expected the first event, got {other:?}"),
    }

    // Cancel from this (other) thread: the subscriber, blocked at the live edge, unblocks and
    // the stream ends. The thread must join without hanging.
    cancel.cancel();
    subscriber.join().unwrap();
}

#[test]
fn idle_subscription_does_not_flood_caught_up_frames() {
    // A short wait tick so several ticks elapse within the sleep below. A per-tick (rather than
    // per-live-edge) caught-up would turn the bounded wait into a heartbeat and be caught here.
    let server_config = ServerConfig {
        subscribe_wait_tick: Duration::from_millis(20),
        ..ServerConfig::default()
    };
    let ts = TestServer::start_with(server_config, 16 * 1024 * 1024, WriterConfig::default());

    let (item_tx, item_rx) = mpsc::channel();
    let (cancel_tx, cancel_rx) = mpsc::channel();
    // Empty store: the subscription is immediately caught up.
    let subscriber = spawn_subscriber(
        ts.client(),
        Query::all(),
        Position::ZERO,
        item_tx,
        cancel_tx,
    );
    let cancel = cancel_rx.recv().unwrap();

    match item_rx.recv().unwrap() {
        Ok(SubEvent::CaughtUp(w)) => assert_eq!(w.get(), 0),
        other => panic!("expected a caught-up marker, got {other:?}"),
    }

    // Let many wait ticks elapse with no writes: exactly one caught-up should have been sent.
    thread::sleep(Duration::from_millis(200));
    match item_rx.try_recv() {
        Err(mpsc::TryRecvError::Empty) => {}
        other => panic!("idle subscription sent an unexpected extra frame: {other:?}"),
    }

    // Still live: an append is delivered, followed by exactly one re-armed caught-up marker.
    let mut appender = ts.client();
    appender.append([ev("E", &["k:1"], b"")], None).unwrap();
    let mut saw_event = false;
    let mut saw_caught_up = false;
    for _ in 0..2 {
        match item_rx.recv().unwrap() {
            Ok(SubEvent::Event(ev)) => {
                assert_eq!(ev.position().get(), 1);
                saw_event = true;
            }
            Ok(SubEvent::CaughtUp(w)) => {
                assert_eq!(w.get(), 1);
                saw_caught_up = true;
            }
            Err(err) => panic!("subscription error: {err}"),
        }
    }
    assert!(
        saw_event && saw_caught_up,
        "expected the event and one re-armed caught-up marker"
    );

    cancel.cancel();
    subscriber.join().unwrap();
}

#[test]
fn server_shutdown_ends_an_idle_subscription() {
    let ts = TestServer::start();
    let mut appender = ts.client();
    appender.append([ev("E", &["k:1"], b"")], None).unwrap();

    let (item_tx, item_rx) = mpsc::channel();
    let (cancel_tx, cancel_rx) = mpsc::channel();
    let subscriber = spawn_subscriber(
        ts.client(),
        Query::all(),
        Position::ZERO,
        item_tx,
        cancel_tx,
    );
    let _cancel = cancel_rx.recv().unwrap();

    // Drain the one event so the subscription is parked at the live edge (idle).
    match item_rx.recv().unwrap() {
        Ok(SubEvent::Event(ev)) => assert_eq!(ev.position().get(), 1),
        other => panic!("expected the first event, got {other:?}"),
    }

    // Tear the server down (drop runs shutdown + coordinator shutdown). The idle subscription
    // must end promptly rather than hang the connection thread.
    drop(ts);
    subscriber.join().unwrap();
}

/// Completes the mandatory opening Hello on a raw connection and consumes the `HelloAck`, so a
/// raw-socket test can then send request frames directly. The ack is read unbuffered, leaving the
/// stream byte-aligned for a subsequent `BufReader`.
fn raw_hello_stream(stream: &TcpStream) {
    let mut io = stream;
    let request = tephra_proto::hello_request(1, None);
    write_frame(&mut io, &request, DEFAULT_MAX_FRAME_LEN).unwrap();
    io.flush().unwrap();
    let ack = read_frame::<pb::Response, _>(&mut io, DEFAULT_MAX_FRAME_LEN)
        .unwrap()
        .expect("server acknowledges the hello");
    assert!(
        matches!(ack.kind(), pb::response::KindOneof::HelloAck(_)),
        "expected a hello ack, got {:?}",
        ack.kind()
    );
}

/// Sends one hand-built wire request over a fresh connection and returns the first response,
/// bypassing the clean client so a test can exercise the server's rejection of malformed input.
fn send_raw_request(addr: SocketAddr, request: &pb::Request) -> pb::Response {
    let stream = TcpStream::connect(addr).unwrap();
    stream.set_nodelay(true).unwrap();
    raw_hello_stream(&stream);
    let mut writer = BufWriter::new(stream.try_clone().unwrap());
    write_frame(&mut writer, request, DEFAULT_MAX_FRAME_LEN).unwrap();
    writer.flush().unwrap();
    let mut reader = BufReader::new(stream);
    read_frame::<pb::Response, _>(&mut reader, DEFAULT_MAX_FRAME_LEN)
        .unwrap()
        .expect("server closed without responding")
}

// --- server-side concurrency (one connection, multiple in-flight requests) ---

/// Builds an append request frame carrying one tagged event.
fn append_frame(request_id: u64, ty: &str, tag: &str) -> pb::Request {
    let mut event = pb::Event::new();
    event.set_type(ty);
    event.tags_mut().push(tag.to_string());
    event.set_payload(b"p".to_vec());
    let mut append = pb::AppendRequest::new();
    append.events_mut().push(event);
    let mut request = pb::Request::new();
    request.set_request_id(request_id);
    request.set_append(append);
    request
}

/// Builds a catch-all read request frame resuming after `after`.
fn read_all_frame(request_id: u64, after: u64) -> pb::Request {
    let mut query = pb::Query::new();
    query.set_all(true);
    let mut read = pb::ReadRequest::new();
    read.set_query(query);
    read.set_after(after);
    let mut request = pb::Request::new();
    request.set_request_id(request_id);
    request.set_read(read);
    request
}

/// Builds a catch-all subscribe request frame resuming after `after`.
fn subscribe_all_frame(request_id: u64, after: u64) -> pb::Request {
    let mut query = pb::Query::new();
    query.set_all(true);
    let mut subscribe = pb::SubscribeRequest::new();
    subscribe.set_query(query);
    subscribe.set_after(after);
    let mut request = pb::Request::new();
    request.set_request_id(request_id);
    request.set_subscribe(subscribe);
    request
}

#[test]
fn pipelined_appends_all_succeed_with_dense_positions() {
    // Fire several appends back-to-back on one connection without waiting, then read the
    // responses. The server processes the pipeline and tags each response with its request id.
    let ts = TestServer::start();
    let stream = TcpStream::connect(ts.addr).unwrap();
    stream.set_nodelay(true).unwrap();
    raw_hello_stream(&stream);
    let mut writer = BufWriter::new(stream.try_clone().unwrap());
    let mut reader = BufReader::new(stream);

    let n = 8u64;
    for id in 1..=n {
        write_frame(
            &mut writer,
            &append_frame(id, "E", "k:1"),
            DEFAULT_MAX_FRAME_LEN,
        )
        .unwrap();
    }
    writer.flush().unwrap();

    // Collect one AppendResponse per request id.
    let mut positions = HashMap::new();
    for _ in 0..n {
        let resp = read_frame::<pb::Response, _>(&mut reader, DEFAULT_MAX_FRAME_LEN)
            .unwrap()
            .expect("a response per pipelined append");
        match resp.kind() {
            pb::response::KindOneof::Append(append) => {
                positions.insert(resp.request_id(), (append.first(), append.last()));
            }
            other => panic!("expected an append response, got {other:?}"),
        }
    }

    // Every id answered; single-connection appends serialize at the coordinator in submission
    // order, so id k lands at position k, dense and unique.
    for id in 1..=n {
        assert_eq!(positions.get(&id), Some(&(id, id)), "append {id} position");
    }
}

#[test]
fn teardown_with_reads_in_flight_keeps_the_pool_healthy() {
    // A connection dying mid-read must stop its pooled read (not wedge a shared worker) and leave
    // other connections' reads correct. Droppers on raw sockets start a full streamed read, read
    // a few frames, then hard-close mid-stream; concurrent blocking clients read the whole corpus
    // and must always get complete, gap-free results. One event per frame keeps a read genuinely
    // in flight after only a few frames are consumed.
    let server_config = ServerConfig {
        read_batch_events: 1,
        read_batch_bytes: 1,
        ..ServerConfig::default()
    };
    let ts = TestServer::start_with(server_config, 16 * 1024 * 1024, WriterConfig::default());
    let corpus = 300u64;
    {
        let mut client = ts.client();
        for i in 0..corpus {
            client
                .append([ev("E", &[&format!("k:{i}")], b"p")], None)
                .unwrap();
        }
    }
    let addr = ts.addr;
    let mut handles = Vec::new();

    // Droppers: open a raw read, consume a few frames, then drop the socket (hard close) mid-stream.
    for _ in 0..8 {
        handles.push(thread::spawn(move || {
            for _ in 0..10 {
                let stream = TcpStream::connect(addr).unwrap();
                stream.set_nodelay(true).unwrap();
                raw_hello_stream(&stream);
                let mut writer = BufWriter::new(stream.try_clone().unwrap());
                write_frame(&mut writer, &read_all_frame(1, 0), DEFAULT_MAX_FRAME_LEN).unwrap();
                writer.flush().unwrap();
                let mut reader = BufReader::new(stream);
                for _ in 0..3 {
                    let _ = read_frame::<pb::Response, _>(&mut reader, DEFAULT_MAX_FRAME_LEN);
                }
                // `reader` and `writer` drop here, hard-closing the socket with the read in flight.
            }
        }));
    }

    // Survivors: full reads that must always return the complete, gap-free corpus.
    for _ in 0..8 {
        handles.push(thread::spawn(move || {
            let mut client = Client::connect(addr).unwrap();
            for _ in 0..10 {
                let (events, watermark) =
                    client.read_all(Query::all(), Position::ZERO, None).unwrap();
                assert_eq!(watermark.get(), corpus);
                assert_eq!(positions(&events), (1..=corpus).collect::<Vec<_>>());
            }
        }));
    }

    for handle in handles {
        handle.join().unwrap();
    }
}

#[test]
fn concurrent_reads_across_connections_return_correct_results() {
    // Many connections hammering small reads through the shared pool must all get correct results,
    // a functional guard that pooling never reorders or drops a request's frames. Mirrors the
    // warm, small, high-concurrency read shape the pool is meant to serve.
    let ts = TestServer::start();
    let corpus = 100u64;
    {
        let mut client = ts.client();
        for i in 0..corpus {
            client
                .append([ev("E", &["k:same"], format!("e{i}").as_bytes())], None)
                .unwrap();
        }
    }
    let addr = ts.addr;
    let mut handles = Vec::new();
    for _ in 0..16 {
        handles.push(thread::spawn(move || {
            let mut client = Client::connect(addr).unwrap();
            for _ in 0..50 {
                // First page, then the next, both exact and gap-free.
                let (page, _) = client
                    .read_all(tag_query(&["k:same"]), Position::ZERO, Some(10))
                    .unwrap();
                assert_eq!(positions(&page), (1..=10).collect::<Vec<_>>());
                let after = page.last().unwrap().position();
                let (rest, watermark) = client
                    .read_all(tag_query(&["k:same"]), after, Some(10))
                    .unwrap();
                assert_eq!(positions(&rest), (11..=20).collect::<Vec<_>>());
                assert_eq!(watermark.get(), corpus);
            }
        }));
    }
    for handle in handles {
        handle.join().unwrap();
    }
}

#[test]
fn a_subscription_does_not_block_a_concurrent_append() {
    // The old server dedicated a connection to a subscription forever. Now a subscribe and an
    // append share one connection: the append is answered while the subscription stays live,
    // and the subscription then delivers the just-appended event.
    let ts = TestServer::start();
    let stream = TcpStream::connect(ts.addr).unwrap();
    stream.set_nodelay(true).unwrap();
    raw_hello_stream(&stream);
    let mut writer = BufWriter::new(stream.try_clone().unwrap());
    let mut reader = BufReader::new(stream);

    // Open the subscription (id 1) over the empty store; it reaches the live edge immediately.
    write_frame(
        &mut writer,
        &subscribe_all_frame(1, 0),
        DEFAULT_MAX_FRAME_LEN,
    )
    .unwrap();
    writer.flush().unwrap();
    let first = read_frame::<pb::Response, _>(&mut reader, DEFAULT_MAX_FRAME_LEN)
        .unwrap()
        .expect("subscription responds");
    assert_eq!(first.request_id(), 1);
    assert!(matches!(first.kind(), pb::response::KindOneof::CaughtUp(_)));

    // With the subscription still live, append on the same connection (id 2).
    write_frame(
        &mut writer,
        &append_frame(2, "E", "k:1"),
        DEFAULT_MAX_FRAME_LEN,
    )
    .unwrap();
    writer.flush().unwrap();

    // We must see both the append's own response (id 2) and the subscription (id 1) delivering
    // the new event, proof the two ran concurrently over one connection.
    let mut saw_append = false;
    let mut saw_sub_event = false;
    for _ in 0..8 {
        if saw_append && saw_sub_event {
            break;
        }
        let resp = read_frame::<pb::Response, _>(&mut reader, DEFAULT_MAX_FRAME_LEN)
            .unwrap()
            .expect("more frames follow");
        match (resp.request_id(), resp.kind()) {
            (2, pb::response::KindOneof::Append(append)) => {
                assert_eq!((append.first(), append.last()), (1, 1));
                saw_append = true;
            }
            (1, pb::response::KindOneof::ReadEvents(events)) => {
                assert_eq!(events.events().len(), 1);
                assert_eq!(events.events().get(0).unwrap().position(), 1);
                saw_sub_event = true;
            }
            (1, pb::response::KindOneof::CaughtUp(_)) => {} // a re-armed edge marker
            other => panic!("unexpected frame: {other:?}"),
        }
    }
    assert!(saw_append, "the append was answered while subscribed");
    assert!(
        saw_sub_event,
        "the subscription delivered the appended event"
    );
}

#[test]
fn cancel_stops_a_subscription_and_frees_the_connection() {
    // A multiplexed client cancels one request by id without closing the socket. After the
    // cancel, the connection still serves an append (the subscription worker has stopped).
    let ts = TestServer::start();
    let stream = TcpStream::connect(ts.addr).unwrap();
    stream.set_nodelay(true).unwrap();
    raw_hello_stream(&stream);
    let mut writer = BufWriter::new(stream.try_clone().unwrap());
    let mut reader = BufReader::new(stream);

    write_frame(
        &mut writer,
        &subscribe_all_frame(1, 0),
        DEFAULT_MAX_FRAME_LEN,
    )
    .unwrap();
    writer.flush().unwrap();
    let caught = read_frame::<pb::Response, _>(&mut reader, DEFAULT_MAX_FRAME_LEN)
        .unwrap()
        .expect("subscription responds");
    assert!(matches!(
        caught.kind(),
        pb::response::KindOneof::CaughtUp(_)
    ));

    // Cancel the subscription (id 1).
    let mut cancel = pb::CancelRequest::new();
    cancel.set_target(1);
    let mut cancel_req = pb::Request::new();
    cancel_req.set_request_id(99);
    cancel_req.set_cancel(cancel);
    write_frame(&mut writer, &cancel_req, DEFAULT_MAX_FRAME_LEN).unwrap();
    writer.flush().unwrap();

    // The connection is still usable: an append is answered normally.
    write_frame(
        &mut writer,
        &append_frame(2, "E", "k:1"),
        DEFAULT_MAX_FRAME_LEN,
    )
    .unwrap();
    writer.flush().unwrap();

    // The next append response (id 2) arrives; the cancelled subscription may deliver the event
    // once if it was mid-flight, but must not keep the connection from answering the append.
    let mut saw_append = false;
    for _ in 0..8 {
        let resp = read_frame::<pb::Response, _>(&mut reader, DEFAULT_MAX_FRAME_LEN)
            .unwrap()
            .expect("append is answered after a cancel");
        if resp.request_id() == 2 {
            assert!(matches!(resp.kind(), pb::response::KindOneof::Append(_)));
            saw_append = true;
            break;
        }
    }
    assert!(
        saw_append,
        "append answered after the subscription was cancelled"
    );
}

// --- async client: multiplexing over one connection ---

#[tokio::test]
async fn async_client_appends_and_reads_round_trip() {
    let ts = TestServer::start();
    let client = AsyncClient::connect(ts.addr).await.unwrap();

    client
        .append([ev("Enrolled", &["course:c1"], b"one")], None)
        .await
        .unwrap();
    client
        .append([ev("Enrolled", &["course:c2"], b"two")], None)
        .await
        .unwrap();

    let (events, watermark) = client
        .read_all(Query::all(), Position::ZERO, None)
        .await
        .unwrap();
    assert_eq!(events.len(), 2);
    assert_eq!(events[0].position(), Position::new(1));
    assert_eq!(events[1].position(), Position::new(2));
    assert_eq!(watermark, Position::new(2));
}

#[tokio::test]
async fn async_read_limit_caps_the_result_and_paginates() {
    let ts = TestServer::start();
    let client = AsyncClient::connect(ts.addr).await.unwrap();
    for i in 0..12u64 {
        client
            .append(
                [ev("Enrolled", &["student:s0"], format!("e{i}").as_bytes())],
                None,
            )
            .await
            .unwrap();
    }
    let query = || tag_query(&["student:s0"]);

    // Exact cap, and a page that resumes after the last position tiles the rest with no gap.
    let (page, _) = client
        .read_all(query(), Position::ZERO, Some(5))
        .await
        .unwrap();
    assert_eq!(positions(&page), vec![1, 2, 3, 4, 5]);
    let after = page.last().unwrap().position();
    let (rest, _) = client.read_all(query(), after, Some(100)).await.unwrap();
    assert_eq!(positions(&rest), (6..=12).collect::<Vec<_>>());
}

#[tokio::test]
async fn async_client_pipelines_concurrent_appends() {
    let ts = TestServer::start();
    let client = AsyncClient::connect(ts.addr).await.unwrap();

    // Fire many appends concurrently through clones of the one client (one connection).
    let n = 16u64;
    let mut set = tokio::task::JoinSet::new();
    for i in 0..n {
        let client = client.clone();
        set.spawn(async move {
            client
                .append([ev("E", &[&format!("k:{i}")], b"p")], None)
                .await
                .unwrap()
        });
    }

    let mut firsts = BTreeSet::new();
    while let Some(joined) = set.join_next().await {
        let range = joined.unwrap();
        assert_eq!(range.first, range.last, "each append is a single event");
        firsts.insert(range.first.get());
    }

    // All succeeded, and the assigned positions are exactly 1..=n, dense and unique.
    let expected: BTreeSet<u64> = (1..=n).collect();
    assert_eq!(firsts, expected);
}

#[tokio::test]
async fn async_client_subscribe_coexists_with_append() {
    let ts = TestServer::start();
    let client = AsyncClient::connect(ts.addr).await.unwrap();

    // Subscribe over the empty store; the first item is the caught-up marker.
    let mut sub = client.subscribe(Query::all(), Position::ZERO).await;
    match sub.next().await.unwrap().unwrap() {
        SubEvent::CaughtUp(_) => {}
        other => panic!("expected a caught-up marker first, got {other:?}"),
    }

    // Append on the same client while subscribed: the append resolves, and the subscription
    // then delivers the new event, both multiplexed over one connection.
    let range = client
        .append([ev("E", &["k:1"], b"p")], None)
        .await
        .unwrap();
    assert_eq!(range.first, Position::new(1));

    loop {
        match sub.next().await.unwrap().unwrap() {
            SubEvent::Event(event) => {
                assert_eq!(event.position(), Position::new(1));
                break;
            }
            SubEvent::CaughtUp(_) => {}
        }
    }
}

#[tokio::test]
async fn async_cancelling_an_in_flight_read_frees_the_connection() {
    // One event per frame keeps a large read in flight on a pool worker after the client pulls a
    // single event. Dropping the stream sends a CancelRequest; the pooled read must stop promptly
    // and the multiplexed connection must keep serving.
    let server_config = ServerConfig {
        read_batch_events: 1,
        read_batch_bytes: 1,
        ..ServerConfig::default()
    };
    let ts = TestServer::start_with(server_config, 16 * 1024 * 1024, WriterConfig::default());
    let client = AsyncClient::connect(ts.addr).await.unwrap();
    for i in 0..200u64 {
        client
            .append([ev("E", &[&format!("k:{i}")], b"p")], None)
            .await
            .unwrap();
    }

    {
        let mut stream = client.read(Query::all(), Position::ZERO, None).await;
        let first = stream.next().await.unwrap().unwrap();
        assert_eq!(first.position(), Position::new(1));
        // `stream` drops here mid-read, cancelling the in-flight read on its pool worker.
    }

    // The connection still serves a fresh append and a full read.
    let range = client
        .append([ev("E", &["k:done"], b"p")], None)
        .await
        .unwrap();
    assert_eq!(range.first, Position::new(201));
    let (events, watermark) = client
        .read_all(Query::all(), Position::ZERO, None)
        .await
        .unwrap();
    assert_eq!(watermark, Position::new(201));
    assert_eq!(events.len(), 201);
}

#[tokio::test]
async fn async_client_dropping_a_subscription_cancels_and_frees_the_connection() {
    let ts = TestServer::start();
    let client = AsyncClient::connect(ts.addr).await.unwrap();

    {
        let mut sub = client.subscribe(Query::all(), Position::ZERO).await;
        match sub.next().await.unwrap().unwrap() {
            SubEvent::CaughtUp(_) => {}
            other => panic!("expected a caught-up marker, got {other:?}"),
        }
        // Dropping `sub` here sends a cancel; the shared connection stays usable.
    }

    let range = client
        .append([ev("E", &["k:1"], b"p")], None)
        .await
        .unwrap();
    assert_eq!(range.first, Position::new(1));
}

#[test]
fn subscription_budget_rejects_excess_subscriptions() {
    // With room for two subscriptions, a third on the same connection is rejected (not blocked),
    // and the connection keeps working.
    let config = ServerConfig {
        max_concurrent_subscriptions: 2,
        ..ServerConfig::default()
    };
    let ts = TestServer::start_with(config, 16 * 1024 * 1024, WriterConfig::default());
    let stream = TcpStream::connect(ts.addr).unwrap();
    stream.set_nodelay(true).unwrap();
    raw_hello_stream(&stream);
    let mut writer = BufWriter::new(stream.try_clone().unwrap());
    let mut reader = BufReader::new(stream);

    // Two subscriptions fit (each acquires a permit in the reader before spawning), so by the
    // time the third is read both permits are held and it is rejected deterministically.
    for id in 1..=3u64 {
        write_frame(
            &mut writer,
            &subscribe_all_frame(id, 0),
            DEFAULT_MAX_FRAME_LEN,
        )
        .unwrap();
    }
    writer.flush().unwrap();

    let mut caught_up = 0;
    let mut rejected = 0;
    for _ in 0..3 {
        let resp = read_frame::<pb::Response, _>(&mut reader, DEFAULT_MAX_FRAME_LEN)
            .unwrap()
            .expect("three responses");
        match resp.kind() {
            pb::response::KindOneof::CaughtUp(_) => caught_up += 1,
            pb::response::KindOneof::Error(_) => {
                assert_eq!(
                    resp.request_id(),
                    3,
                    "the third subscription is the one rejected"
                );
                rejected += 1;
            }
            other => panic!("unexpected frame: {other:?}"),
        }
    }
    assert_eq!(caught_up, 2, "two subscriptions were accepted");
    assert_eq!(rejected, 1, "the third was rejected");
}

// A raw client that pipelines a flood of batch-1 append requests over each socket while draining
// the responses concurrently on a second thread (like a real pipelining client). This is the
// reporter's scenario without the AsyncClient's built-in flow control: many concurrent in-flight
// appends per socket, across many sockets, at maximum rate. The server must backpressure via the
// socket (blocking the writer when its queue is full), never close the connection. A write error
// or a short read of responses means the server dropped the connection, which is the bug.
#[test]
fn raw_pipelined_append_flood_is_backpressured_not_dropped() {
    let ts = TestServer::start();

    const CONNS: usize = 16;
    const APPENDS_PER_CONN: usize = 20_000;

    // One prebuilt append frame reused for every request.
    let frame = {
        let mut append = pb::AppendRequest::new();
        let mut pe = pb::Event::new();
        pe.set_type("E".to_string());
        pe.tags_mut().push("k:1".to_string());
        pe.set_payload(b"payload".to_vec());
        append.events_mut().push(pe);
        let mut request = pb::Request::new();
        request.set_request_id(1);
        request.set_append(append);
        let mut buf = Vec::new();
        write_frame(&mut buf, &request, DEFAULT_MAX_FRAME_LEN).unwrap();
        buf
    };

    let addr = ts.addr;
    let mut conns = Vec::new();
    for _ in 0..CONNS {
        let frame = frame.clone();
        conns.push(thread::spawn(move || -> Result<(), String> {
            let stream = TcpStream::connect(addr).map_err(|err| format!("connect: {err}"))?;
            stream.set_nodelay(true).ok();
            raw_hello_stream(&stream);
            let reader_stream = stream.try_clone().map_err(|err| format!("clone: {err}"))?;

            // Drain responses concurrently so the socket never wedges: a well-behaved pipelining
            // client always reads. Count the append acknowledgements it sees.
            let reader = thread::spawn(move || -> Result<usize, String> {
                let mut reader = BufReader::new(reader_stream);
                let mut acked = 0usize;
                while acked < APPENDS_PER_CONN {
                    match read_frame::<pb::Response, _>(&mut reader, DEFAULT_MAX_FRAME_LEN) {
                        Ok(Some(resp)) => match resp.kind() {
                            pb::response::KindOneof::Append(_) => acked += 1,
                            pb::response::KindOneof::Error(err) => {
                                return Err(format!("server error frame: {}", err.message()));
                            }
                            other => return Err(format!("unexpected frame: {other:?}")),
                        },
                        Ok(None) => {
                            return Err(format!("server closed after {acked} acks"));
                        }
                        Err(err) => return Err(format!("read failed after {acked} acks: {err}")),
                    }
                }
                Ok(acked)
            });

            let mut writer = BufWriter::new(stream);
            for _ in 0..APPENDS_PER_CONN {
                writer
                    .write_all(&frame)
                    .map_err(|err| format!("write: {err}"))?;
            }
            writer.flush().map_err(|err| format!("flush: {err}"))?;

            let acked = reader.join().unwrap()?;
            if acked != APPENDS_PER_CONN {
                return Err(format!("only {acked} of {APPENDS_PER_CONN} appends acked"));
            }
            Ok(())
        }));
    }

    let mut drops = Vec::new();
    for conn in conns {
        if let Err(err) = conn.join().unwrap() {
            drops.push(err);
        }
    }
    assert!(
        drops.is_empty(),
        "the server dropped {} of {CONNS} pipelining connections instead of backpressuring: {drops:?}",
        drops.len()
    );
}

// The failure mode the in-flight bound fixes: N workers each keeping K appends outstanding over
// a pool of AsyncClients, flooding batch-1 appends as fast as they can. Each client is given a
// deliberately small `max_inflight_requests` so the client's own outstanding-request budget (not
// the socket) is the binding constraint: a worker wanting K > that budget makes the client
// backpressure (await a free permit) rather than pile up an unbounded unacked backlog. The whole
// flood must complete with zero connection drops, and every append must be acked exactly once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bounded_inflight_flood_backpressures_without_dropping() {
    let ts = TestServer::start();

    const CONNS: usize = 16;
    const WORKERS: usize = 32;
    const OUTSTANDING: usize = 64; // appends each worker keeps in flight at once
    const APPENDS_PER_WORKER: usize = 2000;

    // A small per-connection in-flight bound, far below what the workers try to keep outstanding,
    // so the client's semaphore is exercised as backpressure under the flood.
    let config = AsyncClientConfig {
        max_inflight_requests: 16,
        ..AsyncClientConfig::default()
    };
    let mut pool = Vec::with_capacity(CONNS);
    for _ in 0..CONNS {
        pool.push(
            AsyncClient::connect_with(ts.addr, config.clone())
                .await
                .unwrap(),
        );
    }
    let pool = Arc::new(pool);

    let mut workers = tokio::task::JoinSet::new();
    for w in 0..WORKERS {
        let pool = Arc::clone(&pool);
        workers.spawn(async move {
            // Keep OUTSTANDING appends in flight at all times until the quota is met.
            let mut inflight = tokio::task::JoinSet::new();
            let mut launched = 0usize;
            let mut failures = 0usize;
            let mut sample_err = None;
            while launched < APPENDS_PER_WORKER || !inflight.is_empty() {
                while launched < APPENDS_PER_WORKER && inflight.len() < OUTSTANDING {
                    let client = pool[(w + launched) % CONNS].clone();
                    inflight.spawn(async move {
                        client
                            .append([ev("E", &["k:1"], b"p")], None)
                            .await
                            .map(|_| ())
                            .map_err(|err| err.to_string())
                    });
                    launched += 1;
                }
                if let Some(joined) = inflight.join_next().await
                    && let Err(err) = joined.unwrap()
                {
                    failures += 1;
                    if sample_err.is_none() {
                        sample_err = Some(err);
                    }
                }
            }
            (failures, sample_err)
        });
    }

    let mut total_failures = 0usize;
    let mut sample_err = None;
    while let Some(joined) = workers.join_next().await {
        let (failures, err) = joined.unwrap();
        total_failures += failures;
        if sample_err.is_none() {
            sample_err = err;
        }
    }
    assert_eq!(
        total_failures, 0,
        "the bounded flood dropped {total_failures} appends; sample error: {sample_err:?}"
    );
}

// Reproduces the high-concurrency write flood: a pool of connections with worker appends
// round-robined across them, each socket carrying many concurrent pipelined batch-1 appends
// with no in-flight limit. Under the bug the server drops connections instead of applying
// backpressure, so every append that lands on a dropped socket fails. This asserts zero
// failures across the whole flood.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn write_flood_over_a_connection_pool_never_drops() {
    // Tiny per-connection queues so any path that errors-instead-of-blocks trips fast, and few
    // runtime threads so the client's own reader task must contend with the flooding workers.
    let server_config = ServerConfig {
        max_inflight_requests_per_conn: 8,
        frame_queue_depth: 8,
        ..ServerConfig::default()
    };
    let ts = TestServer::start_with(server_config, 16 * 1024 * 1024, WriterConfig::default());

    const CONNS: usize = 16;
    const TOTAL_APPENDS: usize = 200_000;

    let mut pool = Vec::with_capacity(CONNS);
    for _ in 0..CONNS {
        pool.push(AsyncClient::connect(ts.addr).await.unwrap());
    }
    let pool = Arc::new(pool);

    // No in-flight limit: fire every append as its own task, round-robined across the pool, so
    // each socket carries a huge number of concurrent pipelined appends at once.
    let mut set = tokio::task::JoinSet::new();
    for i in 0..TOTAL_APPENDS {
        let pool = Arc::clone(&pool);
        set.spawn(async move {
            let client = &pool[i % CONNS];
            client
                .append([ev("E", &["k:1"], b"p")], None)
                .await
                .map(|_| ())
                .map_err(|err| err.to_string())
        });
    }

    let mut total_failures = 0usize;
    let mut sample_err = None;
    while let Some(joined) = set.join_next().await {
        if let Err(err) = joined.unwrap() {
            total_failures += 1;
            if sample_err.is_none() {
                sample_err = Some(err);
            }
        }
    }

    assert_eq!(
        total_failures, 0,
        "the write flood dropped {total_failures} appends; sample error: {sample_err:?}"
    );
}

// --- head-of-line blocking: egress fairness and the control/bulk socket split ---

#[tokio::test]
async fn async_client_defaults_to_a_control_and_bulk_pool() {
    let ts = TestServer::start();
    let client = AsyncClient::connect(ts.addr).await.unwrap();
    // A default client opens five sockets: one control plus a pool of four bulk.
    assert_eq!(wait_active_connections(&client, 5).await, 5);
}

#[tokio::test]
async fn async_client_single_socket_mode_uses_one_connection() {
    let ts = TestServer::start();
    let config = AsyncClientConfig {
        bulk_connections: 0,
        ..AsyncClientConfig::default()
    };
    let client = AsyncClient::connect_with(ts.addr, config).await.unwrap();
    // With no bulk sockets, reads share the single control connection (legacy mode).
    assert_eq!(wait_active_connections(&client, 1).await, 1);
    let (events, _) = client
        .read_all(Query::all(), Position::ZERO, None)
        .await
        .unwrap();
    assert!(events.is_empty());
}

/// Polls the server's connection gauge until it reaches `want` (the bulk socket may register a
/// beat after `connect` returns), returning the last value seen.
async fn wait_active_connections(client: &AsyncClient, want: u64) -> u64 {
    let mut last = 0;
    for _ in 0..200 {
        last = client.stats().await.unwrap().active_connections;
        if last == want {
            return last;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    last
}

#[test]
fn a_small_response_interleaves_with_a_large_read_on_one_socket() {
    // A multi-megabyte read and a small append pipelined on one socket: the append ack must
    // interleave ahead of the read's completion, not queue behind the whole read (the head-of-line
    // defect). Small frames plus a small client receive buffer hold the read in hard TCP
    // backpressure so it stays in flight, and a brief pause lets the append commit; the ack then
    // rides the priority control lane and arrives before the read's terminating ReadEnd.
    let server_config = ServerConfig {
        read_batch_bytes: 8 * 1024,
        ..ServerConfig::default()
    };
    let ts = TestServer::start_with(server_config, 16 * 1024 * 1024, WriterConfig::default());
    // ~3 MiB => hundreds of 8 KiB frames, far more than the receive buffer and bulk lane hold, so
    // the read is firmly in flight when the append commits.
    seed_bulk(&mut ts.client(), 24, 16, 8 * 1024);

    let stream = TcpStream::connect(ts.addr).unwrap();
    stream.set_nodelay(true).unwrap();
    socket2::SockRef::from(&stream)
        .set_recv_buffer_size(32 * 1024)
        .unwrap();
    raw_hello_stream(&stream);
    let mut writer = BufWriter::new(stream.try_clone().unwrap());
    write_frame(&mut writer, &read_all_frame(1, 0), DEFAULT_MAX_FRAME_LEN).unwrap();
    write_frame(
        &mut writer,
        &append_frame(2, "E", "k:new"),
        DEFAULT_MAX_FRAME_LEN,
    )
    .unwrap();
    writer.flush().unwrap();
    // Give the append time to commit while the read stays backpressured (nothing drained yet), so
    // its ack is queued on the control lane before we start reading.
    thread::sleep(Duration::from_millis(50));

    // Read frames until either the ack or the read's ReadEnd arrives. The ack riding the priority
    // control lane must beat the read's terminating frame; a regression that queued it behind the
    // read on one lane would surface ReadEnd first. This invariant is timing-robust: however many
    // read frames the kernel pre-buffers ahead of the ack, the ack still precedes ReadEnd, which the
    // ~3 MiB read cannot reach while backpressured through the small receive buffer.
    let mut reader = BufReader::new(stream);
    loop {
        let resp = read_frame::<pb::Response, _>(&mut reader, DEFAULT_MAX_FRAME_LEN)
            .unwrap()
            .expect("server closed before the append ack arrived");
        match resp.kind() {
            pb::response::KindOneof::Append(_) if resp.request_id() == 2 => {
                return; // interleaved ahead of the read's completion: no head-of-line block
            }
            pb::response::KindOneof::ReadEnd(_) if resp.request_id() == 1 => {
                panic!(
                    "append ack arrived only after the read fully drained (head-of-line blocked)"
                );
            }
            _ => {}
        }
    }
}

#[test]
fn a_read_past_the_inflight_and_overflow_budgets_is_rejected() {
    // With one in-flight permit and one overflow slot, the third concurrent read on a connection is
    // rejected rather than parking the reader. That the rejection arrives at all proves the reader
    // never blocked on read admission (the old blocking acquire would have wedged on the second).
    //
    // Determinism: a read's permit is released only when its worker *finishes*, so the first read
    // must stay in flight while reads 2 and 3 are admitted. Small frames, a shallow bulk lane, and a
    // small client receive buffer put the first read's worker into hard TCP backpressure (blocked
    // mid-stream, permit held) rather than letting it complete in microseconds and free the permit.
    let server_config = ServerConfig {
        max_inflight_requests_per_conn: 1,
        read_batch_bytes: 8 * 1024,
        frame_queue_depth: 8,
        ..ServerConfig::default()
    };
    let ts = TestServer::start_with(server_config, 16 * 1024 * 1024, WriterConfig::default());
    // Far more than the bulk lane + receive buffer can hold, so read 1's worker blocks mid-stream.
    seed_bulk(&mut ts.client(), 16, 16, 8 * 1024);

    // A raw socket with a small receive buffer we deliberately do not drain, so read 1 cannot finish
    // (its permit stays held) and read 2 keeps its overflow slot.
    let stream = TcpStream::connect(ts.addr).unwrap();
    stream.set_nodelay(true).unwrap();
    socket2::SockRef::from(&stream)
        .set_recv_buffer_size(32 * 1024)
        .unwrap();
    raw_hello_stream(&stream);
    let mut writer = BufWriter::new(stream.try_clone().unwrap());
    write_frame(&mut writer, &read_all_frame(1, 0), DEFAULT_MAX_FRAME_LEN).unwrap();
    write_frame(&mut writer, &read_all_frame(2, 0), DEFAULT_MAX_FRAME_LEN).unwrap();
    write_frame(&mut writer, &read_all_frame(3, 0), DEFAULT_MAX_FRAME_LEN).unwrap();
    writer.flush().unwrap();

    // Read 3's rejection rides the priority control lane, so it arrives ahead of reads 1/2's bulk.
    let mut reader = BufReader::new(stream);
    loop {
        let resp = read_frame::<pb::Response, _>(&mut reader, DEFAULT_MAX_FRAME_LEN)
            .unwrap()
            .expect("server closed without rejecting the third read");
        if resp.request_id() == 3 {
            match resp.kind() {
                pb::response::KindOneof::Error(error) => {
                    let message = error.message().to_str().unwrap_or("");
                    assert!(
                        message.contains("too many in-flight reads"),
                        "unexpected rejection message: {message}",
                    );
                    return;
                }
                other => panic!("expected the third read to be rejected, got {other:?}"),
            }
        }
    }
}

// --- bearer-token authentication (Hello handshake) ---

#[test]
fn a_valid_token_authenticates_and_serves() {
    // With a token configured, a client presenting it in its Hello connects and serves normally.
    let ts = TestServer::start_with_auth(vec!["s3cret".to_string()]);
    let mut client = Client::connect_with(ts.addr, Some("s3cret")).unwrap();
    client.append([ev("E", &["k:1"], b"p")], None).unwrap();
    let (events, _) = client.read_all(Query::all(), Position::ZERO, None).unwrap();
    assert_eq!(positions(&events), vec![1]);
}

#[test]
fn a_wrong_token_is_rejected_at_connect() {
    // A bad token fails the Hello, so `connect_with` returns PermissionDenied before any request.
    let ts = TestServer::start_with_auth(vec!["s3cret".to_string()]);
    let err = Client::connect_with(ts.addr, Some("nope")).err().unwrap();
    assert_eq!(err.kind(), ErrorKind::PermissionDenied, "got {err:?}");
}

#[test]
fn a_missing_token_is_rejected_at_connect() {
    // No token against an auth server is also rejected at the Hello.
    let ts = TestServer::start_with_auth(vec!["s3cret".to_string()]);
    let err = Client::connect(ts.addr).err().unwrap();
    assert_eq!(err.kind(), ErrorKind::PermissionDenied, "got {err:?}");
}

#[test]
fn either_rotation_token_authenticates() {
    // Multiple accepted tokens (the zero-downtime rotation window): a client using either connects.
    let ts = TestServer::start_with_auth(vec!["old".to_string(), "new".to_string()]);
    assert!(Client::connect_with(ts.addr, Some("old")).is_ok());
    assert!(Client::connect_with(ts.addr, Some("new")).is_ok());
    assert!(Client::connect_with(ts.addr, Some("other")).is_err());
}

#[test]
fn an_open_server_accepts_with_or_without_a_token() {
    // No tokens configured: the Hello still runs (version negotiation) and any client connects,
    // whether or not it presents a token.
    let ts = TestServer::start();
    assert!(Client::connect(ts.addr).is_ok());
    assert!(Client::connect_with(ts.addr, Some("ignored")).is_ok());
}

#[test]
fn a_wrong_protocol_version_is_rejected() {
    // A Hello announcing an unsupported protocol version is rejected with a bad-request error,
    // proving the version is the explicit compatibility gate.
    let ts = TestServer::start();
    let stream = TcpStream::connect(ts.addr).unwrap();
    stream.set_nodelay(true).unwrap();
    let mut writer = BufWriter::new(stream.try_clone().unwrap());
    let mut reader = BufReader::new(stream);

    let mut hello = pb::Hello::new();
    hello.set_protocol_version(PROTOCOL_VERSION + 1);
    let mut request = pb::Request::new();
    request.set_request_id(1);
    request.set_hello(hello);
    write_frame(&mut writer, &request, DEFAULT_MAX_FRAME_LEN).unwrap();
    writer.flush().unwrap();

    let response = read_frame::<pb::Response, _>(&mut reader, DEFAULT_MAX_FRAME_LEN)
        .unwrap()
        .expect("the server answers the hello");
    match response.kind() {
        pb::response::KindOneof::Error(error) => {
            assert_eq!(error.code(), pb::ErrorCode::BadRequest);
        }
        other => panic!("expected a version-mismatch error, got {other:?}"),
    }
}

#[test]
fn a_non_hello_first_frame_is_rejected() {
    // The first frame must be a Hello: a client that opens with a request is unauthenticated.
    let ts = TestServer::start_with_auth(vec!["s3cret".to_string()]);
    let response = send_raw_first_frame(ts.addr, &append_frame(1, "E", "k:1"));
    match response.kind() {
        pb::response::KindOneof::Error(error) => {
            assert_eq!(error.code(), pb::ErrorCode::Unauthenticated);
        }
        other => panic!("expected an unauthenticated error, got {other:?}"),
    }
}

/// Sends one frame as the very first frame on a fresh connection (no preceding Hello) and returns
/// the server's reply, for the "first frame must be a hello" path.
fn send_raw_first_frame(addr: SocketAddr, request: &pb::Request) -> pb::Response {
    let stream = TcpStream::connect(addr).unwrap();
    stream.set_nodelay(true).unwrap();
    let mut writer = BufWriter::new(stream.try_clone().unwrap());
    write_frame(&mut writer, request, DEFAULT_MAX_FRAME_LEN).unwrap();
    writer.flush().unwrap();
    let mut reader = BufReader::new(stream);
    read_frame::<pb::Response, _>(&mut reader, DEFAULT_MAX_FRAME_LEN)
        .unwrap()
        .expect("server replies before closing")
}

#[tokio::test]
async fn the_async_client_authenticates_control_and_bulk_sockets() {
    // Every physical socket (control + the bulk pool) authenticates independently, so an append
    // (control) and a read (a bulk socket) both succeed only if each socket sent a valid Hello.
    let ts = TestServer::start_with_auth(vec!["s3cret".to_string()]);
    let config = AsyncClientConfig {
        auth_token: Some("s3cret".to_string()),
        bulk_connections: 2,
        ..AsyncClientConfig::default()
    };
    let client = AsyncClient::connect_with(ts.addr, config).await.unwrap();
    client
        .append([ev("E", &["k:1"], b"p")], None)
        .await
        .unwrap();
    let (events, _) = client
        .read_all(Query::all(), Position::ZERO, None)
        .await
        .unwrap();
    assert_eq!(positions(&events), vec![1]);
}

#[tokio::test]
async fn the_async_client_fails_to_connect_without_a_token() {
    // A missing token fails the control socket's Hello, so `connect_with` fails.
    let ts = TestServer::start_with_auth(vec!["s3cret".to_string()]);
    let result = AsyncClient::connect_with(ts.addr, AsyncClientConfig::default()).await;
    assert!(
        result.is_err(),
        "an unauthenticated async connect must fail"
    );
}
