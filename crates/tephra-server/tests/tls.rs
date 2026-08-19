//! End-to-end TLS tests: a tephra-server serving over rustls, driven by the blocking client's
//! `connect_tls`, plus a raw TLS client for the pipelining interleave guard.
#![cfg(feature = "tls")]

use std::io::{BufReader, BufWriter, Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::path::Path;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use tephra::log::set::{SegmentConfig, SegmentSet};
use tephra::writer::{WriteCoordinator, WriterConfig};

use tempfile::{NamedTempFile, TempDir};
use tephra_client::{AsyncClient, Client, Event, Position, Query, SubEvent};
use tephra_proto::{DEFAULT_MAX_FRAME_LEN, TlsConn, read_frame, tephra as pb, write_frame};
use tephra_server::tls::build_server_config;
use tephra_server::{Server, ServerConfig, ShutdownHandle};

fn ev(ty: &str, tags: &[&str], payload: &[u8]) -> Event {
    Event::new(ty, tags.iter().copied(), payload).unwrap()
}

/// A self-signed certificate and key for `localhost`, written to temp files so the server can load
/// them by path and the client can trust the certificate directly.
struct TestCerts {
    cert: NamedTempFile,
    key: NamedTempFile,
}

impl TestCerts {
    fn generate() -> TestCerts {
        let generated = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
        let mut cert = NamedTempFile::new().unwrap();
        cert.write_all(generated.cert.pem().as_bytes()).unwrap();
        cert.flush().unwrap();
        let mut key = NamedTempFile::new().unwrap();
        key.write_all(generated.signing_key.serialize_pem().as_bytes())
            .unwrap();
        key.flush().unwrap();
        TestCerts { cert, key }
    }
}

/// A TLS server on its own thread over a temp-dir store, torn down on drop.
struct TlsTestServer {
    addr: SocketAddr,
    shutdown: ShutdownHandle,
    server_thread: Option<JoinHandle<()>>,
    coordinator: Option<WriteCoordinator>,
    certs: TestCerts,
    _dir: TempDir,
}

impl TlsTestServer {
    fn start() -> TlsTestServer {
        TlsTestServer::start_with(ServerConfig::default())
    }

    fn start_with(config: ServerConfig) -> TlsTestServer {
        let certs = TestCerts::generate();
        let dir = TempDir::new().unwrap();
        let set = SegmentSet::open(dir.path(), SegmentConfig::new(16 * 1024 * 1024)).unwrap();
        let (coordinator, handle) = WriteCoordinator::start(set, WriterConfig::default()).unwrap();
        let tls = build_server_config(certs.cert.path(), certs.key.path()).unwrap();
        let server = Server::bind("127.0.0.1:0", handle, config)
            .unwrap()
            .with_data_dir(dir.path())
            .with_tls(tls);
        let addr = server.local_addr();
        let shutdown = server.shutdown_handle();
        let server_thread = thread::spawn(move || server.run().expect("server run"));
        TlsTestServer {
            addr,
            shutdown,
            server_thread: Some(server_thread),
            coordinator: Some(coordinator),
            certs,
            _dir: dir,
        }
    }

    fn client(&self) -> Client {
        let config = tephra_client::tls::config_with_custom_ca(self.certs.cert.path()).unwrap();
        Client::connect_tls(self.addr, "localhost", config).unwrap()
    }
}

impl Drop for TlsTestServer {
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

#[test]
fn tls_append_and_read_round_trip() {
    let ts = TlsTestServer::start();
    let mut client = ts.client();
    client
        .append(
            [
                ev("Enrolled", &["course:c1"], b"one"),
                ev("Enrolled", &["course:c2"], b"two"),
            ],
            None,
        )
        .unwrap();

    let (events, _watermark) = client.read_all(Query::all(), Position::ZERO, None).unwrap();
    let payloads: Vec<&[u8]> = events.iter().map(|e| e.event().payload()).collect();
    assert_eq!(payloads, vec![b"one".as_slice(), b"two".as_slice()]);
}

#[test]
fn tls_survives_a_large_read_over_the_encrypted_stream() {
    // Many KB-scale events across several batches: the read streams far more than one TLS record,
    // exercising the read half's record reassembly and the write half's interleaved draining.
    let ts = TlsTestServer::start();
    let mut client = ts.client();
    let payload = vec![b'z'; 4 * 1024];
    for _ in 0..32 {
        let batch: Vec<Event> = (0..8).map(|_| ev("E", &["hot"], &payload)).collect();
        client.append(batch, None).unwrap();
    }
    let (events, _watermark) = client.read_all(Query::all(), Position::ZERO, None).unwrap();
    assert_eq!(events.len(), 256);
    assert!(events.iter().all(|e| e.event().payload().len() == 4 * 1024));
}

#[tokio::test]
async fn async_client_round_trips_over_tls() {
    // The multiplexing async client (tokio-rustls) over the same TLS server: append on the control
    // socket, then drain a read over the bulk pool, all encrypted.
    let ts = TlsTestServer::start();
    let config = tephra_client::tls::config_with_custom_ca(ts.certs.cert.path()).unwrap();
    let client = AsyncClient::connect_tls(ts.addr, "localhost", config)
        .await
        .unwrap();
    client
        .append([ev("E", &["t"], b"async-tls")], None)
        .await
        .unwrap();
    let (events, _watermark) = client
        .read_all(Query::all(), Position::ZERO, None)
        .await
        .unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].event().payload(), b"async-tls");
}

#[test]
fn tls_subscribe_streams_catch_up_events() {
    let ts = TlsTestServer::start();
    ts.client()
        .append([ev("E", &["t"], b"first")], None)
        .unwrap();

    let mut subscriber = ts.client();
    let (mut stream, cancel) = subscriber.subscribe(Query::all(), Position::ZERO).unwrap();
    let first = loop {
        match stream.next().unwrap().unwrap() {
            SubEvent::Event(event) => break event,
            SubEvent::CaughtUp(_) => continue,
        }
    };
    assert_eq!(first.event().payload(), b"first");
    cancel.cancel();
}

#[test]
fn a_plaintext_client_is_rejected_by_a_tls_server() {
    let ts = TlsTestServer::start();
    // A plaintext client completes the TCP connect, but its first frame is not a TLS ClientHello,
    // so the server aborts the handshake and drops the connection. The request must error, never
    // hang or succeed.
    let mut client = Client::connect(ts.addr).unwrap();
    let result = client.append([ev("E", &["t"], b"x")], None);
    assert!(
        result.is_err(),
        "a plaintext request to a TLS server must fail"
    );
}

#[test]
fn a_tls_client_is_rejected_by_a_plaintext_server() {
    // The mirror: a TLS handshake against a plaintext server cannot complete, so connect_tls fails
    // rather than returning a half-open client.
    let dir = TempDir::new().unwrap();
    let set = SegmentSet::open(dir.path(), SegmentConfig::new(16 * 1024 * 1024)).unwrap();
    let (coordinator, handle) = WriteCoordinator::start(set, WriterConfig::default()).unwrap();
    let server = Server::bind("127.0.0.1:0", handle, ServerConfig::default()).unwrap();
    let addr = server.local_addr();
    let shutdown = server.shutdown_handle();
    let server_thread = thread::spawn(move || server.run().expect("server run"));

    let config = tephra_client::tls::config_with_native_roots()
        .unwrap_or_else(|_| unreachable!("native roots present on CI"));
    let result = Client::connect_tls(addr, "localhost", config);
    assert!(
        result.is_err(),
        "a TLS handshake against a plaintext server must fail"
    );

    shutdown.shutdown();
    let _ = server_thread.join();
    coordinator.shutdown();
}

#[test]
fn an_unfinished_tls_handshake_is_reaped() {
    // A client that connects but never sends a ClientHello must be reaped by the handshake
    // deadline, not pin a connection thread forever. The server closes it, so a blocking read on
    // the raw socket returns EOF well within the deadline plus a margin.
    let config = ServerConfig {
        handshake_timeout: Duration::from_secs(1),
        ..ServerConfig::default()
    };
    let ts = TlsTestServer::start_with(config);
    let mut raw = TcpStream::connect(ts.addr).unwrap();
    raw.set_read_timeout(Some(Duration::from_secs(6))).unwrap();
    let started = Instant::now();
    let mut buf = [0u8; 1];
    let read = raw.read(&mut buf);
    assert!(
        matches!(read, Ok(0)),
        "server should close the stalled handshake, got {read:?}"
    );
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "handshake reap took {:?}",
        started.elapsed()
    );
}

#[test]
fn a_partial_tls_handshake_is_reaped() {
    // A client that sends part of a ClientHello then stalls must be reaped by the handshake
    // deadline, not pinned. rustls `complete_io` would loop internally over reads while bytes keep
    // arriving; the server drives the handshake one read at a time so the wall-clock deadline is
    // enforced even with a record in flight.
    let config = ServerConfig {
        handshake_timeout: Duration::from_secs(1),
        ..ServerConfig::default()
    };
    let ts = TlsTestServer::start_with(config);
    let mut raw = TcpStream::connect(ts.addr).unwrap();
    raw.set_read_timeout(Some(Duration::from_secs(6))).unwrap();
    // A TLS handshake record header claiming a 512-byte body, then only a couple of body bytes: the
    // server can never assemble a complete ClientHello from it.
    raw.write_all(&[0x16, 0x03, 0x01, 0x02, 0x00, 0x01, 0x00])
        .unwrap();
    raw.flush().unwrap();

    let started = Instant::now();
    let mut buf = [0u8; 1];
    let read = raw.read(&mut buf);
    assert!(
        matches!(read, Ok(0)),
        "server should reap the stalled handshake, got {read:?}"
    );
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "partial handshake reap took {:?}",
        started.elapsed()
    );
}

#[test]
fn an_idle_timeout_bounds_the_tls_handshake() {
    // With only idle_timeout configured (handshake and incomplete-frame off), a silent client must
    // still be reaped during the handshake: the deadline folds in idle_timeout, matching the
    // plaintext reader, which bounds an accept-to-first-frame gap via idle_timeout too.
    let config = ServerConfig {
        incomplete_frame_timeout: Duration::ZERO,
        idle_timeout: Duration::from_secs(1),
        ..ServerConfig::default()
    };
    let ts = TlsTestServer::start_with(config);
    let mut raw = TcpStream::connect(ts.addr).unwrap();
    raw.set_read_timeout(Some(Duration::from_secs(6))).unwrap();

    let started = Instant::now();
    let mut buf = [0u8; 1];
    let read = raw.read(&mut buf);
    assert!(
        matches!(read, Ok(0)),
        "server should reap the idle handshake, got {read:?}"
    );
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "idle handshake reap took {:?}",
        started.elapsed()
    );
}

/// Opens a raw TLS client connection, returning framing-ready read and write halves plus a handle
/// kept alive for the connection's lifetime. `recv_buffer` bounds the client's kernel receive
/// buffer so a large read stays in TCP backpressure.
fn raw_tls_client(
    addr: SocketAddr,
    ca: &Path,
    recv_buffer: usize,
) -> (
    BufReader<tephra_proto::TlsReadHalf>,
    BufWriter<tephra_proto::TlsWriteHalf>,
) {
    let stream = TcpStream::connect(addr).unwrap();
    stream.set_nodelay(true).unwrap();
    socket2::SockRef::from(&stream)
        .set_recv_buffer_size(recv_buffer)
        .unwrap();
    let config = tephra_client::tls::config_with_custom_ca(ca).unwrap();
    let name = rustls::pki_types::ServerName::try_from("localhost".to_string()).unwrap();
    let mut session = rustls::ClientConnection::new(config, name).unwrap();
    let mut handshake = stream.try_clone().unwrap();
    while session.is_handshaking() {
        session.complete_io(&mut handshake).unwrap();
    }
    let read_sock = stream.try_clone().unwrap();
    let write_sock = stream.try_clone().unwrap();
    let session = TlsConn::new(session);
    let (read_half, write_half) = session.split(read_sock, write_sock);
    (BufReader::new(read_half), BufWriter::new(write_half))
}

#[test]
fn a_small_response_interleaves_with_a_large_read_over_tls() {
    // The TLS twin of the plaintext interleave guard: a multi-megabyte read and a small append
    // pipelined on one encrypted connection. The append ack rides the priority control lane and
    // must arrive before the read's terminating ReadEnd, proving the TLS transport did not
    // serialise the two directions (the Option-B head-of-line regression).
    let config = ServerConfig {
        read_batch_bytes: 8 * 1024,
        ..ServerConfig::default()
    };
    let ts = TlsTestServer::start_with(config);

    // ~3 MiB of small events, so the read is firmly in flight when the append commits.
    let mut seed = ts.client();
    let payload = vec![b'x'; 8 * 1024];
    for _ in 0..24 {
        let batch: Vec<Event> = (0..16).map(|_| ev("E", &["hot"], &payload)).collect();
        seed.append(batch, None).unwrap();
    }

    let (mut reader, mut writer) = raw_tls_client(ts.addr, ts.certs.cert.path(), 32 * 1024);
    write_frame(&mut writer, &read_all_frame(1, 0), DEFAULT_MAX_FRAME_LEN).unwrap();
    write_frame(
        &mut writer,
        &append_frame(2, "E", "k:new"),
        DEFAULT_MAX_FRAME_LEN,
    )
    .unwrap();
    writer.flush().unwrap();
    // Let the append commit while the read stays backpressured, so its ack is queued on the control
    // lane before we start reading.
    thread::sleep(Duration::from_millis(50));

    loop {
        let resp = read_frame::<pb::Response, _>(&mut reader, DEFAULT_MAX_FRAME_LEN)
            .unwrap()
            .expect("server closed before the append ack arrived");
        match resp.kind() {
            pb::response::KindOneof::Append(_) if resp.request_id() == 2 => return,
            pb::response::KindOneof::ReadEnd(_) if resp.request_id() == 1 => {
                panic!(
                    "append ack arrived only after the read fully drained (head-of-line blocked)"
                )
            }
            _ => {}
        }
    }
}

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
