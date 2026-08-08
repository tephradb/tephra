//! End-to-end tests over a real socket: a `dcbdb-server` bound to an ephemeral port, driven
//! by the blocking `dcbdb-client`.

use std::net::SocketAddr;
use std::thread::{self, JoinHandle};

use dcbdb::log::set::{SegmentConfig, SegmentSet};
use dcbdb::writer::{WriteCoordinator, WriterConfig};

use dcbdb_client::{
    Client, ClientError, condition, event, proto, query_all, query_item, query_items,
};
use dcbdb_server::{Server, ServerConfig, ShutdownHandle};
use tempfile::TempDir;

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
        let server = Server::bind("127.0.0.1:0", handle, server_config).unwrap();
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

/// Collects a sequenced event's fields into owned, comparable values.
fn fields(sequenced: &proto::SequencedEvent) -> (u64, String, Vec<String>, Vec<u8>) {
    let ev = sequenced.event();
    (
        sequenced.position(),
        ev.r#type().to_str().unwrap().to_string(),
        ev.tags()
            .iter()
            .map(|t| t.to_str().unwrap().to_string())
            .collect(),
        ev.payload().to_vec(),
    )
}

#[test]
fn append_then_read_round_trips_events() {
    let ts = TestServer::start();
    let mut client = ts.client();

    let range = client
        .append(
            vec![event(
                "Enrolled",
                &["course:c1", "student:s1"],
                b"payload-1",
            )],
            None,
        )
        .unwrap();
    assert_eq!(range.first(), 1);
    assert_eq!(range.last(), 1);

    client
        .append(vec![event("Renamed", &["course:c1"], b"payload-2")], None)
        .unwrap();

    let (events, watermark) = client.read_all(query_all(), 0).unwrap();
    assert_eq!(watermark, 2);
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
fn tag_query_filters_the_read() {
    let ts = TestServer::start();
    let mut client = ts.client();
    client
        .append(vec![event("A", &["course:c1"], b"")], None)
        .unwrap();
    client
        .append(vec![event("B", &["course:c2"], b"")], None)
        .unwrap();
    client
        .append(vec![event("C", &["course:c1"], b"")], None)
        .unwrap();

    let (events, _) = client
        .read_all(query_items(vec![query_item(&[], &["course:c1"])]), 0)
        .unwrap();
    let positions: Vec<u64> = events.iter().map(|e| e.position()).collect();
    assert_eq!(positions, vec![1, 3]);
}

#[test]
fn read_after_skips_the_prefix() {
    let ts = TestServer::start();
    let mut client = ts.client();
    for i in 0..5 {
        client
            .append(vec![event("E", &[&format!("k:{i}")], b"")], None)
            .unwrap();
    }

    let (events, watermark) = client.read_all(query_all(), 2).unwrap();
    let positions: Vec<u64> = events.iter().map(|e| e.position()).collect();
    assert_eq!(positions, vec![3, 4, 5]);
    assert_eq!(watermark, 5);
}

#[test]
fn empty_read_yields_only_a_watermark() {
    let ts = TestServer::start();
    let mut client = ts.client();
    client
        .append(vec![event("E", &["k:1"], b"")], None)
        .unwrap();

    // Nothing after the tip.
    let (events, watermark) = client.read_all(query_all(), 1).unwrap();
    assert!(events.is_empty());
    assert_eq!(watermark, 1);
}

#[test]
fn durable_append_conflict_is_reported_and_not_retryable() {
    let ts = TestServer::start();
    let mut client = ts.client();

    // Reserve a unique username, guarded so a second identical reservation fails.
    let guard = condition(query_items(vec![query_item(&[], &["username:alice"])]), 0);
    client
        .append(
            vec![event("Reserved", &["username:alice"], b"{}")],
            Some(guard),
        )
        .unwrap();

    let guard = condition(query_items(vec![query_item(&[], &["username:alice"])]), 0);
    let err = client
        .append(
            vec![event("Reserved", &["username:alice"], b"{}")],
            Some(guard),
        )
        .unwrap_err();
    match err {
        ClientError::Server {
            code,
            retryable,
            conflict_position,
            ..
        } => {
            assert_eq!(code, proto::ErrorCode::Conflict);
            assert!(!retryable, "a durable conflict is terminal");
            assert_eq!(conflict_position, Some(1));
        }
        other => panic!("expected a server conflict, got {other:?}"),
    }
}

#[test]
fn invalid_request_maps_to_bad_request_and_empty_maps_to_empty() {
    let ts = TestServer::start();
    let mut client = ts.client();

    // An empty event type is rejected by dcbdb's constructor -> BAD_REQUEST.
    let err = client
        .append(vec![event("", &["k:1"], b"")], None)
        .unwrap_err();
    match err {
        ClientError::Server { code, .. } => assert_eq!(code, proto::ErrorCode::BadRequest),
        other => panic!("expected bad request, got {other:?}"),
    }

    // An append with no events -> EMPTY.
    let err = client.append(vec![], None).unwrap_err();
    match err {
        ClientError::Server { code, .. } => assert_eq!(code, proto::ErrorCode::Empty),
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
            .append(vec![event("E", &[&format!("n:{i}")], b"x")], None)
            .unwrap();
    }

    let (events, watermark) = client.read_all(query_all(), 0).unwrap();
    assert_eq!(watermark, total);
    let positions: Vec<u64> = events.iter().map(|e| e.position()).collect();
    let expected: Vec<u64> = (1..=total).collect();
    assert_eq!(positions, expected);
}

#[test]
fn streaming_read_iterator_yields_incrementally() {
    let ts = TestServer::start();
    let mut client = ts.client();
    for i in 0..10 {
        client
            .append(vec![event("E", &[&format!("k:{i}")], b"")], None)
            .unwrap();
    }

    let mut stream = client.read(query_all(), 0).unwrap();
    let mut count = 0;
    for item in stream.by_ref() {
        item.unwrap();
        count += 1;
    }
    assert_eq!(count, 10);
    assert_eq!(stream.watermark(), Some(10));
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
            .append(vec![event("E", &[&format!("k:{i}")], b"")], None)
            .unwrap();
    }

    {
        let mut stream = client.read(query_all(), 0).unwrap();
        let first = stream.next().unwrap().unwrap();
        assert_eq!(first.position(), 1);
        // `stream` is dropped here, mid-read, with 19 events plus the terminator unread.
    }

    // The next read on the same connection returns complete, correct results.
    let (events, watermark) = client.read_all(query_all(), 0).unwrap();
    assert_eq!(watermark, 20);
    let positions: Vec<u64> = events.iter().map(|e| e.position()).collect();
    assert_eq!(positions, (1..=20).collect::<Vec<_>>());
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
    let err = client
        .append(vec![event("E", &["k:1"], &big)], None)
        .unwrap_err();
    match err {
        ClientError::Server { code, .. } => assert_eq!(code, proto::ErrorCode::TooLarge),
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
                            vec![event(
                                "Appended",
                                &[&format!("t:{t}"), &format!("i:{i}")],
                                b"",
                            )],
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
    let (events, watermark) = client.read_all(query_all(), 0).unwrap();
    let total = threads * per_thread;
    assert_eq!(watermark, total);
    assert_eq!(events.len() as u64, total);
    // Positions are dense 1..=total regardless of interleaving.
    let mut positions: Vec<u64> = events.iter().map(|e| e.position()).collect();
    positions.sort_unstable();
    assert_eq!(positions, (1..=total).collect::<Vec<_>>());
}

#[test]
fn graceful_shutdown_stops_accepting_and_returns() {
    let ts = TestServer::start();
    let addr = ts.addr;
    let mut client = ts.client();
    client
        .append(vec![event("E", &["k:1"], b"")], None)
        .unwrap();

    // Signal shutdown; the accept loop stops and `run` returns (joined by Drop), which drops
    // the listener and closes the port.
    ts.shutdown.shutdown();
    thread::sleep(std::time::Duration::from_millis(50));

    // A new connection is refused, or if it slips in before the port closes, its request
    // fails: either way the server no longer accepts work.
    let refused = match Client::connect(addr) {
        Err(_) => true,
        Ok(mut client) => client
            .append(vec![event("E", &["k:2"], b"")], None)
            .is_err(),
    };
    assert!(refused, "server should refuse work after shutdown");
}
