//! An async, multiplexing client for a tephra event store, built on tokio.
//!
//! Unlike the blocking [`Client`](super::Client), which runs one request at a time per
//! connection, an [`AsyncClient`] pipelines many requests. It is a cheap `Clone` handle over one or
//! more connection actors, each a socket with a background **reader task** that demultiplexes
//! response frames by `request_id` and a **writer task** that serializes outbound frames. Reads and
//! subscriptions are returned as [`Stream`]s; dropping one cancels it server-side (a
//! `CancelRequest`) without disturbing other requests.
//!
//! By default the client opens a **control** socket for appends, stats, and cancels plus a pool of
//! four **bulk** sockets for streaming reads and subscriptions. Splitting the control socket off
//! keeps a multi-megabyte read response from delaying a small append ack behind it on one byte
//! stream (head-of-line blocking), and the bulk pool keeps concurrent reads from serializing behind
//! one another, which otherwise caps read throughput sharply under load. Set
//! [`AsyncClientConfig::bulk_connections`] to `0` to fold everything onto a single socket (the
//! legacy behavior, which reintroduces both hazards); raise it for more read fan-out at the cost of
//! more sockets per client on the server.
//!
//! ```no_run
//! use tephra_client::{AsyncClient, Event, Position, Query};
//! use tokio_stream::StreamExt;
//!
//! # async fn run() -> Result<(), Box<dyn std::error::Error>> {
//! let client = AsyncClient::connect("127.0.0.1:9000").await?;
//! client
//!     .append([Event::new("Enrolled", ["course:c1"], b"{}")?], None)
//!     .await?;
//!
//! let mut stream = client.read(Query::all(), Position::ZERO, None).await;
//! while let Some(sequenced) = stream.next().await {
//!     let sequenced = sequenced?;
//!     println!("{}: {}", sequenced.position(), sequenced.event().event_type());
//! }
//! # Ok(())
//! # }
//! ```

use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use futures_core::Stream;
#[cfg(feature = "async-tls")]
use rustls::pki_types::ServerName;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt, BufWriter};
use tokio::net::{TcpStream, ToSocketAddrs, lookup_host};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot};
use tokio::task::JoinSet;
#[cfg(feature = "async-tls")]
use tokio_rustls::TlsConnector;
use tokio_stream::StreamExt;

use tephra_proto::convert as wire;
use tephra_proto::tephra as pb;
use tephra_proto::{DEFAULT_MAX_FRAME_LEN, read_frame_async, write_frame_async};

use super::{
    AppendCondition, AppendResult, ClientError, Event, Position, Query, SequencedEvent, Stats,
    SubEvent, UNATTRIBUTED_REQUEST_ID, event_to_pb, hello_error, sequenced_from_pb, server_error,
    stats_from_pb, verify_hello_ack,
};

/// Tuning for an [`AsyncClient`].
#[derive(Clone, Debug)]
pub struct AsyncClientConfig {
    /// Largest single frame accepted or produced (default [`DEFAULT_MAX_FRAME_LEN`]). Must match
    /// or exceed the server's limit, or a large `ReadEvents` batch is rejected as over-limit and
    /// fails the connection.
    pub max_frame_len: u32,
    /// Depth of the outbound request queue. Once full, `append`/`read`/`subscribe` await room to
    /// send (backpressure), bounding how far a fast producer can outrun a slow socket.
    pub request_queue_depth: usize,
    /// Most requests that may be outstanding (sent but not yet fully answered) at once on this
    /// connection. A permit is taken before a request goes on the wire and released when its
    /// reply arrives, the stream ends, or it is cancelled; when the limit is reached,
    /// `append`/`read`/`subscribe` await a free permit (backpressure). This caps the unacked
    /// backlog a fast producer can build up, so a flood applies backpressure end to end instead
    /// of outrunning the connection. Many requests still run concurrently; only the total in
    /// flight is bounded.
    pub max_inflight_requests: usize,
    /// Number of dedicated **bulk** sockets carrying streaming reads and subscriptions, separate
    /// from the control socket that carries appends, stats, and cancels. `4` (the default) fans
    /// reads across a small pool so they do not serialize behind one another on one byte stream,
    /// which otherwise caps read throughput sharply under concurrent load; four saturates a
    /// per-core server read pool while keeping per-client sockets low, since total server
    /// connections scale with this value times the client count. `1` is a control/bulk split with
    /// no read fan-out; `0` folds reads onto the control socket (the legacy head-of-line hazard).
    /// The outstanding-read budget is per socket, so a larger pool also raises how many of this
    /// client's reads the server admits at once.
    pub bulk_connections: usize,
    /// The bearer token presented in each socket's opening Hello, for a server that requires
    /// authentication. `None` (the default) connects unauthenticated. Every socket (control and
    /// bulk) authenticates with it independently.
    pub auth_token: Option<String>,
}

impl Default for AsyncClientConfig {
    fn default() -> Self {
        AsyncClientConfig {
            max_frame_len: DEFAULT_MAX_FRAME_LEN,
            request_queue_depth: 256,
            max_inflight_requests: 1024,
            bulk_connections: 4,
            auth_token: None,
        }
    }
}

/// The connection actor's shared state: the id allocator, the outstanding-request budget, and
/// the registry mapping each in-flight `request_id` to the sink awaiting its response.
struct Shared {
    next_id: AtomicU64,
    requests: Mutex<HashMap<u64, Registered>>,
    /// Bounds outstanding (sent-but-unanswered) requests. A permit lives inside each
    /// [`Registered`] entry, so it is released exactly when the request is finalized.
    inflight: Arc<Semaphore>,
    max_frame_len: u32,
}

impl Shared {
    fn next_id(&self) -> u64 {
        self.next_id.fetch_add(1, Ordering::Relaxed)
    }
}

/// A registered in-flight request: the sink awaiting its response, plus the permit from the
/// outstanding-request budget. The permit is released when this entry is dropped, which happens
/// exactly once per request: when its terminal response is routed, when its stream is dropped or
/// cancelled (the stream's `Drop` removes the entry), or when the connection fails and
/// [`fail_all`] drains the registry. A streaming read/subscribe re-registers the same entry
/// while it continues, so it holds its single permit for its whole life.
struct Registered {
    sink: Sink,
    _permit: OwnedSemaphorePermit,
}

/// Where the reader task delivers a response for a given request.
enum Sink {
    /// One append: a single result.
    Append(oneshot::Sender<Result<AppendResult, ClientError>>),
    /// One stats request: a single result.
    Stats(oneshot::Sender<Result<Stats, ClientError>>),
    /// A streamed read: events, then a terminating watermark or error.
    Read(mpsc::UnboundedSender<ReadItem>),
    /// A live subscription: events and caught-up markers until an error or cancel.
    Subscribe(mpsc::UnboundedSender<Result<SubEvent, ClientError>>),
}

/// One item delivered to a [`ReadStream`]'s channel.
enum ReadItem {
    Event(SequencedEvent),
    End(Position),
    Err(ClientError),
}

/// One multiplexed socket: a shared request registry plus its reader and writer tasks. An
/// [`AsyncClient`] composes one or more of these.
struct Conn {
    shared: Arc<Shared>,
    out_tx: mpsc::Sender<pb::Request>,
}

impl Conn {
    /// Connects one socket to the first working address, sets `TCP_NODELAY`, authenticates with the
    /// opening Hello, and spawns its reader and writer tasks.
    async fn connect(addrs: &[SocketAddr], config: AsyncClientConfig) -> io::Result<Conn> {
        let stream = connect_any(addrs).await?;
        stream.set_nodelay(true)?;
        let (mut read_half, mut write_half) = stream.into_split();
        send_hello(&mut read_half, &mut write_half, &config)
            .await
            .map_err(hello_error)?;
        Ok(Conn::spawn(read_half, write_half, config))
    }

    /// Connects one socket, completes a TLS handshake, and authenticates with the opening Hello
    /// before spawning its tasks.
    #[cfg(feature = "async-tls")]
    async fn connect_tls(
        addrs: &[SocketAddr],
        server_name: ServerName<'static>,
        connector: TlsConnector,
        config: AsyncClientConfig,
    ) -> io::Result<Conn> {
        let stream = connect_any(addrs).await?;
        stream.set_nodelay(true)?;
        // Bound the handshake so a server that accepts the TCP connection but stalls cannot leave
        // the connect future pending forever.
        let tls = tokio::time::timeout(
            crate::TLS_HANDSHAKE_TIMEOUT,
            connector.connect(server_name, stream),
        )
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "tls handshake timed out"))??;
        let (mut read_half, mut write_half) = tokio::io::split(tls);
        send_hello(&mut read_half, &mut write_half, &config)
            .await
            .map_err(hello_error)?;
        Ok(Conn::spawn(read_half, write_half, config))
    }

    /// Spawns the reader and writer tasks over an already-connected transport (plaintext or TLS) and
    /// returns the multiplexed connection.
    fn spawn<R, W>(read_half: R, write_half: W, config: AsyncClientConfig) -> Conn
    where
        R: AsyncRead + Unpin + Send + 'static,
        W: AsyncWrite + Unpin + Send + 'static,
    {
        let (out_tx, out_rx) = mpsc::channel(config.request_queue_depth.max(1));
        let shared = Arc::new(Shared {
            // Ids start at 1 so 0 stays reserved as the unattributed-error sentinel.
            next_id: AtomicU64::new(1),
            requests: Mutex::new(HashMap::new()),
            inflight: Arc::new(Semaphore::new(config.max_inflight_requests.max(1))),
            max_frame_len: config.max_frame_len,
        });

        tokio::spawn(reader_task(read_half, Arc::clone(&shared)));
        tokio::spawn(writer_task(write_half, out_rx, shared.max_frame_len));

        Conn { shared, out_tx }
    }

    /// Acquires a permit from this connection's outstanding-request budget, awaiting at the
    /// in-flight limit (backpressure). The permit is held inside the request's [`Registered`] entry
    /// for its whole life and released when that entry is dropped.
    async fn acquire_inflight(&self) -> OwnedSemaphorePermit {
        Arc::clone(&self.shared.inflight)
            .acquire_owned()
            .await
            // The semaphore is never closed, so acquisition cannot fail.
            .expect("inflight semaphore is never closed")
    }
}

/// Tries each address in turn, returning the first connected socket or the last error.
async fn connect_any(addrs: &[SocketAddr]) -> io::Result<TcpStream> {
    let mut last_err = None;
    for addr in addrs {
        match TcpStream::connect(addr).await {
            Ok(stream) => return Ok(stream),
            Err(err) => last_err = Some(err),
        }
    }
    Err(last_err.unwrap_or_else(|| {
        io::Error::new(io::ErrorKind::InvalidInput, "no addresses to connect to")
    }))
}

/// Sends the mandatory opening Hello on a freshly connected socket and awaits its `HelloAck`, so a
/// version mismatch or a rejected token fails the connect. Runs directly on the split halves before
/// the reader/writer actor tasks are spawned, so authentication never touches the multiplexing
/// machinery and every socket is authenticated before it carries a request.
async fn send_hello<R, W>(
    read_half: &mut R,
    write_half: &mut W,
    config: &AsyncClientConfig,
) -> Result<(), ClientError>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    // Ids start at 1, so the Hello borrows the first id; its reply is consumed here and never
    // reaches the actor's registry.
    let request = tephra_proto::hello_request(1, config.auth_token.as_deref());
    write_frame_async(write_half, &request, config.max_frame_len).await?;
    // Flush the first record to the socket, matching every other Hello sender rather than relying on
    // the transport draining a small first write.
    write_half.flush().await?;

    let response = read_frame_async::<pb::Response, _>(read_half, config.max_frame_len)
        .await?
        .ok_or(ClientError::UnexpectedEof)?;
    verify_hello_ack(&response)
}

/// A cheap, cloneable handle to a tephra server over a control socket plus a pool of bulk sockets
/// (see the module docs and [`AsyncClientConfig::bulk_connections`]). Every clone shares the same
/// sockets and background tasks; requests issued through any clone run concurrently.
#[derive(Clone)]
pub struct AsyncClient {
    control: Arc<Conn>,
    bulk: Arc<Vec<Conn>>,
    next_bulk: Arc<AtomicUsize>,
}

impl AsyncClient {
    /// Connects to a server with the default [`AsyncClientConfig`].
    pub async fn connect(addr: impl ToSocketAddrs) -> io::Result<AsyncClient> {
        AsyncClient::connect_with(addr, AsyncClientConfig::default()).await
    }

    /// Connects to a server with an explicit [`AsyncClientConfig`], opening one control socket plus
    /// [`bulk_connections`](AsyncClientConfig::bulk_connections) bulk sockets. Each sets
    /// `TCP_NODELAY` and spawns its own reader and writer tasks. The returned handle can be cloned.
    pub async fn connect_with(
        addr: impl ToSocketAddrs,
        config: AsyncClientConfig,
    ) -> io::Result<AsyncClient> {
        let addrs: Vec<SocketAddr> = lookup_host(addr).await?.collect();
        // The control socket must succeed; dial the bulk pool concurrently so a pool over a high-RTT
        // link pays one round-trip, not N. If any bulk socket fails the whole connect fails (the
        // caller asked for this many sockets); the JoinSet aborts the rest on drop.
        let control = Arc::new(Conn::connect(&addrs, config.clone()).await?);
        let mut dialing = JoinSet::new();
        for _ in 0..config.bulk_connections {
            let addrs = addrs.clone();
            let config = config.clone();
            dialing.spawn(async move { Conn::connect(&addrs, config).await });
        }
        let mut bulk = Vec::with_capacity(config.bulk_connections);
        while let Some(joined) = dialing.join_next().await {
            bulk.push(joined.map_err(io::Error::other)??);
        }
        Ok(AsyncClient {
            control,
            bulk: Arc::new(bulk),
            next_bulk: Arc::new(AtomicUsize::new(0)),
        })
    }

    /// Connects to a server over TLS with the default [`AsyncClientConfig`], verifying its
    /// certificate against `tls_config` (build one with [`crate::tls::config_with_native_roots`] or
    /// [`crate::tls::config_with_custom_ca`]). `server_name` is the name the certificate must match.
    #[cfg(feature = "async-tls")]
    pub async fn connect_tls(
        addr: impl ToSocketAddrs,
        server_name: &str,
        tls_config: Arc<rustls::ClientConfig>,
    ) -> io::Result<AsyncClient> {
        AsyncClient::connect_tls_with(addr, server_name, tls_config, AsyncClientConfig::default())
            .await
    }

    /// [`connect_tls`](Self::connect_tls) with an explicit [`AsyncClientConfig`]. Each of the control
    /// and bulk sockets completes its own TLS handshake, dialed concurrently.
    #[cfg(feature = "async-tls")]
    pub async fn connect_tls_with(
        addr: impl ToSocketAddrs,
        server_name: &str,
        tls_config: Arc<rustls::ClientConfig>,
        config: AsyncClientConfig,
    ) -> io::Result<AsyncClient> {
        let addrs: Vec<SocketAddr> = lookup_host(addr).await?.collect();
        let name = ServerName::try_from(server_name.to_owned()).map_err(|err| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("tls: invalid server name: {err}"),
            )
        })?;
        let connector = TlsConnector::from(tls_config);
        let control = Arc::new(
            Conn::connect_tls(&addrs, name.clone(), connector.clone(), config.clone()).await?,
        );
        let mut dialing = JoinSet::new();
        for _ in 0..config.bulk_connections {
            let addrs = addrs.clone();
            let name = name.clone();
            let connector = connector.clone();
            let config = config.clone();
            dialing.spawn(async move { Conn::connect_tls(&addrs, name, connector, config).await });
        }
        let mut bulk = Vec::with_capacity(config.bulk_connections);
        while let Some(joined) = dialing.join_next().await {
            bulk.push(joined.map_err(io::Error::other)??);
        }
        Ok(AsyncClient {
            control,
            bulk: Arc::new(bulk),
            next_bulk: Arc::new(AtomicUsize::new(0)),
        })
    }

    /// The connection a streaming read or subscription runs on: round-robin across the bulk pool,
    /// or the control connection when no bulk sockets are configured (single-socket mode).
    fn bulk(&self) -> &Conn {
        if self.bulk.is_empty() {
            self.control.as_ref()
        } else {
            let i = self.next_bulk.fetch_add(1, Ordering::Relaxed) % self.bulk.len();
            &self.bulk[i]
        }
    }

    /// Appends `events` as one atomic batch, optionally guarded by `condition`, resolving to the
    /// position range the batch was assigned. Many appends may be awaited concurrently, bounded
    /// by [`AsyncClientConfig::max_inflight_requests`].
    pub async fn append(
        &self,
        events: impl IntoIterator<Item = Event>,
        condition: Option<AppendCondition>,
    ) -> Result<AppendResult, ClientError> {
        // Appends ride the control connection, off the bulk lane carrying large read responses.
        let conn = self.control.as_ref();
        let id = conn.shared.next_id();
        let mut append = pb::AppendRequest::new();
        for event in events {
            append.events_mut().push(event_to_pb(&event));
        }
        if let Some(condition) = condition {
            append.set_condition(wire::condition_to_pb(&condition));
        }
        let mut request = pb::Request::new();
        request.set_request_id(id);
        request.set_append(append);

        // Take an outstanding-request permit before the request goes on the wire (backpressure at
        // the in-flight limit); it rides in the registry entry and frees when the reply is routed.
        let _permit = conn.acquire_inflight().await;
        let (tx, rx) = oneshot::channel();
        conn.shared.requests.lock().unwrap().insert(
            id,
            Registered {
                sink: Sink::Append(tx),
                _permit,
            },
        );
        // Await room in the outbound queue (backpressure). An error means the writer task is gone.
        if conn.out_tx.send(request).await.is_err() {
            conn.shared.requests.lock().unwrap().remove(&id);
            return Err(ClientError::UnexpectedEof);
        }

        // A dropped sender means the reader task ended (the connection closed) before replying.
        rx.await.unwrap_or(Err(ClientError::UnexpectedEof))
    }

    /// Fetches a snapshot of the server's operational state (event count, on-disk size, uptime,
    /// and live connection/subscription gauges).
    pub async fn stats(&self) -> Result<Stats, ClientError> {
        let conn = self.control.as_ref();
        let id = conn.shared.next_id();
        let mut request = pb::Request::new();
        request.set_request_id(id);
        request.set_stats(pb::StatsRequest::new());

        let _permit = conn.acquire_inflight().await;
        let (tx, rx) = oneshot::channel();
        conn.shared.requests.lock().unwrap().insert(
            id,
            Registered {
                sink: Sink::Stats(tx),
                _permit,
            },
        );
        if conn.out_tx.send(request).await.is_err() {
            conn.shared.requests.lock().unwrap().remove(&id);
            return Err(ClientError::UnexpectedEof);
        }

        rx.await.unwrap_or(Err(ClientError::UnexpectedEof))
    }

    /// Starts a read, returning a [`Stream`] over the matching events in ascending position
    /// order. Awaits room in the outbound queue (backpressure) before returning.
    /// [`watermark`](ReadStream::watermark) is available once the stream ends; dropping the stream
    /// early cancels the read server-side.
    ///
    /// `limit` caps the number of matched events returned (`None` = unlimited), applied
    /// server-side during planning. With `after` it forms a stateless pagination cursor: read
    /// a page, then read again with `after` set to the last position, with no gap or duplicate.
    pub async fn read(&self, query: Query, after: Position, limit: Option<u64>) -> ReadStream {
        self.start_read(query, after, false, limit).await
    }

    /// The newest-first dual of [`read`](Self::read): a [`Stream`] over the matching events in
    /// **descending** position order, strictly before `before`. `before` is an exclusive upper
    /// bound, so `read_back(query, Position::MAX, limit)` streams from the tip. `limit` caps the
    /// events from the tip down; with `before` it paginates newest-first.
    pub async fn read_back(
        &self,
        query: Query,
        before: Position,
        limit: Option<u64>,
    ) -> ReadStream {
        self.start_read(query, before, true, limit).await
    }

    /// Shared submission for [`read`](Self::read) and [`read_back`](Self::read_back). `cursor` is
    /// the exclusive lower bound forward, the exclusive upper bound backward.
    async fn start_read(
        &self,
        query: Query,
        cursor: Position,
        reverse: bool,
        limit: Option<u64>,
    ) -> ReadStream {
        // Reads ride a bulk connection so their large responses never delay control-lane acks.
        let conn = self.bulk();
        let id = conn.shared.next_id();
        let mut read = pb::ReadRequest::new();
        read.set_query(wire::query_to_pb(&query));
        read.set_after(cursor.get());
        if reverse {
            read.set_reverse(true);
        }
        if let Some(limit) = limit {
            read.set_limit(limit);
        }
        let mut request = pb::Request::new();
        request.set_request_id(id);
        request.set_read(read);

        let _permit = conn.acquire_inflight().await;
        let (tx, rx) = mpsc::unbounded_channel();
        conn.shared.requests.lock().unwrap().insert(
            id,
            Registered {
                sink: Sink::Read(tx),
                _permit,
            },
        );
        if conn.out_tx.send(request).await.is_err() {
            if let Some(Registered {
                sink: Sink::Read(tx),
                ..
            }) = conn.shared.requests.lock().unwrap().remove(&id)
            {
                let _ = tx.send(ReadItem::Err(ClientError::UnexpectedEof));
            }
        }

        ReadStream {
            shared: Arc::clone(&conn.shared),
            out_tx: conn.out_tx.clone(),
            id,
            rx,
            watermark: None,
            done: false,
        }
    }

    /// Convenience: drains a read fully, returning the events and the watermark it was pinned to.
    /// See [`read`](Self::read) for `limit` semantics.
    pub async fn read_all(
        &self,
        query: Query,
        after: Position,
        limit: Option<u64>,
    ) -> Result<(Vec<SequencedEvent>, Position), ClientError> {
        drain_read(self.read(query, after, limit).await).await
    }

    /// The newest-first dual of [`read_all`](Self::read_all): drains a backward read, returning
    /// the events (descending by position) and the watermark it was pinned to. See
    /// [`read_back`](Self::read_back).
    pub async fn read_all_back(
        &self,
        query: Query,
        before: Position,
        limit: Option<u64>,
    ) -> Result<(Vec<SequencedEvent>, Position), ClientError> {
        drain_read(self.read_back(query, before, limit).await).await
    }

    /// Opens a live subscription over `query`, resuming strictly after `after`: matching durable
    /// events first, then new ones as they commit, with a [`SubEvent::CaughtUp`] marker at each
    /// live edge. Awaits room in the outbound queue before returning; dropping the returned
    /// [`Stream`] cancels the subscription server-side.
    pub async fn subscribe(&self, query: Query, after: Position) -> SubscribeStream {
        // Subscriptions ride a bulk connection alongside reads, off the control lane.
        let conn = self.bulk();
        let id = conn.shared.next_id();
        let mut subscribe = pb::SubscribeRequest::new();
        subscribe.set_query(wire::query_to_pb(&query));
        subscribe.set_after(after.get());
        let mut request = pb::Request::new();
        request.set_request_id(id);
        request.set_subscribe(subscribe);

        let _permit = conn.acquire_inflight().await;
        let (tx, rx) = mpsc::unbounded_channel();
        conn.shared.requests.lock().unwrap().insert(
            id,
            Registered {
                sink: Sink::Subscribe(tx),
                _permit,
            },
        );
        if conn.out_tx.send(request).await.is_err() {
            if let Some(Registered {
                sink: Sink::Subscribe(tx),
                ..
            }) = conn.shared.requests.lock().unwrap().remove(&id)
            {
                let _ = tx.send(Err(ClientError::UnexpectedEof));
            }
        }

        SubscribeStream {
            shared: Arc::clone(&conn.shared),
            out_tx: conn.out_tx.clone(),
            id,
            rx,
            done: false,
        }
    }
}

// ---------------------------------------------------------------------------
// Background tasks
// ---------------------------------------------------------------------------

/// The writer task: drains outbound requests and writes them as frames, flushing once per burst
/// so a pipeline of requests costs one syscall rather than one per frame.
async fn writer_task<W: AsyncWrite + Unpin>(
    write_half: W,
    mut out_rx: mpsc::Receiver<pb::Request>,
    max_frame_len: u32,
) {
    let mut writer = BufWriter::new(write_half);
    while let Some(request) = out_rx.recv().await {
        if write_frame_async(&mut writer, &request, max_frame_len)
            .await
            .is_err()
        {
            break;
        }
        // Write anything already queued before paying for a flush.
        while let Ok(request) = out_rx.try_recv() {
            if write_frame_async(&mut writer, &request, max_frame_len)
                .await
                .is_err()
            {
                return;
            }
        }
        if writer.flush().await.is_err() {
            break;
        }
    }
}

/// The reader task: reads response frames and routes each by `request_id`. On EOF or a transport
/// error it fails every still-waiting request so no caller hangs.
async fn reader_task<R: AsyncRead + Unpin>(mut read_half: R, shared: Arc<Shared>) {
    let max = shared.max_frame_len;
    let mut last_error: Option<String> = None;
    loop {
        match read_frame_async::<pb::Response, _>(&mut read_half, max).await {
            Ok(Some(response)) => {
                // A frame error the server could not attribute carries id 0 and precedes a
                // close. Remember its message so pending requests learn why.
                if response.request_id() == UNATTRIBUTED_REQUEST_ID {
                    if let pb::response::KindOneof::Error(error) = response.kind() {
                        last_error = Some(
                            error
                                .message()
                                .to_str()
                                .unwrap_or("server error")
                                .to_string(),
                        );
                    }
                    continue;
                }
                route(response, &shared);
            }
            Ok(None) => break,
            Err(err) => {
                last_error = Some(format!("connection error: {err}"));
                break;
            }
        }
    }
    let reason = last_error.unwrap_or_else(|| "server closed the connection".to_string());
    fail_all(&shared, &reason);
}

/// Delivers one response to its waiting request, looked up by `request_id`. A streaming request
/// is re-registered while it continues; a terminal frame leaves it removed. An unknown id (a
/// late frame after cancellation or completion) is ignored.
fn route(response: pb::Response, shared: &Shared) {
    let id = response.request_id();
    let mut map = shared.requests.lock().unwrap();
    let Some(Registered { sink, _permit }) = map.remove(&id) else {
        return;
    };
    match sink {
        Sink::Append(tx) => {
            let result = match response.kind() {
                pb::response::KindOneof::Append(append) => Ok(AppendResult {
                    first: Position::new(append.first()),
                    last: Position::new(append.last()),
                }),
                pb::response::KindOneof::Error(error) => Err(server_error(error)),
                other => Err(ClientError::Protocol(format!(
                    "unexpected response to append: {other:?}"
                ))),
            };
            let _ = tx.send(result);
            // `_permit` drops here: the append is answered.
        }
        Sink::Stats(tx) => {
            let result = match response.kind() {
                pb::response::KindOneof::Stats(stats) => Ok(stats_from_pb(stats)),
                pb::response::KindOneof::Error(error) => Err(server_error(error)),
                other => Err(ClientError::Protocol(format!(
                    "unexpected response to stats: {other:?}"
                ))),
            };
            let _ = tx.send(result);
        }
        Sink::Read(tx) => {
            // Re-register (carrying the same permit) while the read continues; a terminal frame
            // leaves it removed, dropping the permit.
            if deliver_read(&tx, response) {
                map.insert(
                    id,
                    Registered {
                        sink: Sink::Read(tx),
                        _permit,
                    },
                );
            }
        }
        Sink::Subscribe(tx) => {
            if deliver_subscribe(&tx, response) {
                map.insert(
                    id,
                    Registered {
                        sink: Sink::Subscribe(tx),
                        _permit,
                    },
                );
            }
        }
    }
}

/// Pushes a read response into its channel. Returns whether the read continues (a batch keeps
/// it open; a `ReadEnd`, error, or dropped receiver ends it).
fn deliver_read(tx: &mpsc::UnboundedSender<ReadItem>, response: pb::Response) -> bool {
    match response.kind() {
        pb::response::KindOneof::ReadEvents(events) => {
            for view in events.events().iter() {
                match sequenced_from_pb(view) {
                    Ok(event) => {
                        if tx.send(ReadItem::Event(event)).is_err() {
                            return false;
                        }
                    }
                    Err(err) => {
                        let _ = tx.send(ReadItem::Err(err));
                        return false;
                    }
                }
            }
            true
        }
        pb::response::KindOneof::ReadEnd(end) => {
            let _ = tx.send(ReadItem::End(Position::new(end.watermark())));
            false
        }
        pb::response::KindOneof::Error(error) => {
            let _ = tx.send(ReadItem::Err(server_error(error)));
            false
        }
        other => {
            let _ = tx.send(ReadItem::Err(ClientError::Protocol(format!(
                "unexpected response during read: {other:?}"
            ))));
            false
        }
    }
}

/// Pushes a subscription response into its channel. Returns whether the subscription continues.
fn deliver_subscribe(
    tx: &mpsc::UnboundedSender<Result<SubEvent, ClientError>>,
    response: pb::Response,
) -> bool {
    match response.kind() {
        pb::response::KindOneof::ReadEvents(events) => {
            for view in events.events().iter() {
                match sequenced_from_pb(view) {
                    Ok(event) => {
                        if tx.send(Ok(SubEvent::Event(event))).is_err() {
                            return false;
                        }
                    }
                    Err(err) => {
                        let _ = tx.send(Err(err));
                        return false;
                    }
                }
            }
            true
        }
        pb::response::KindOneof::CaughtUp(caught_up) => tx
            .send(Ok(SubEvent::CaughtUp(Position::new(caught_up.watermark()))))
            .is_ok(),
        pb::response::KindOneof::Error(error) => {
            let _ = tx.send(Err(server_error(error)));
            false
        }
        other => {
            let _ = tx.send(Err(ClientError::Protocol(format!(
                "unexpected response during subscribe: {other:?}"
            ))));
            false
        }
    }
}

/// Fails every still-registered request with `reason`, so callers awaiting a closed connection
/// return an error rather than hang.
fn fail_all(shared: &Shared, reason: &str) {
    let mut map = shared.requests.lock().unwrap();
    // Draining drops each entry's permit as it is handled, freeing the whole in-flight budget.
    for (_id, Registered { sink, _permit }) in map.drain() {
        match sink {
            Sink::Append(tx) => {
                let _ = tx.send(Err(ClientError::Protocol(reason.to_string())));
            }
            Sink::Stats(tx) => {
                let _ = tx.send(Err(ClientError::Protocol(reason.to_string())));
            }
            Sink::Read(tx) => {
                let _ = tx.send(ReadItem::Err(ClientError::Protocol(reason.to_string())));
            }
            Sink::Subscribe(tx) => {
                let _ = tx.send(Err(ClientError::Protocol(reason.to_string())));
            }
        }
    }
}

/// Sends a fire-and-forget cancel for `target` so the server stops streaming it. Best-effort: a
/// cancel from a stream's (synchronous) `Drop` cannot await a full queue, so it is dropped in that
/// rare case; the server then reaps the request when the connection closes.
fn send_cancel(out_tx: &mpsc::Sender<pb::Request>, shared: &Shared, target: u64) {
    let mut cancel = pb::CancelRequest::new();
    cancel.set_target(target);
    let mut request = pb::Request::new();
    request.set_request_id(shared.next_id());
    request.set_cancel(cancel);
    let _ = out_tx.try_send(request);
}

// ---------------------------------------------------------------------------
// Streams
// ---------------------------------------------------------------------------

/// A [`Stream`] over the events of one read, in ascending position order. After it ends,
/// [`watermark`](ReadStream::watermark) returns the position the read was pinned to. Dropping it
/// before the end cancels the read on the server.
pub struct ReadStream {
    shared: Arc<Shared>,
    out_tx: mpsc::Sender<pb::Request>,
    id: u64,
    rx: mpsc::UnboundedReceiver<ReadItem>,
    watermark: Option<Position>,
    done: bool,
}

/// Drains a [`ReadStream`] fully, returning the events and the watermark the read was pinned
/// to. Shared by [`Client::read_all`] and [`Client::read_all_back`].
async fn drain_read(
    mut stream: ReadStream,
) -> Result<(Vec<SequencedEvent>, Position), ClientError> {
    let mut events = Vec::new();
    while let Some(item) = stream.next().await {
        events.push(item?);
    }
    let watermark = stream
        .watermark()
        .ok_or_else(|| ClientError::Protocol("read ended without a watermark".to_string()))?;
    Ok((events, watermark))
}

impl ReadStream {
    /// The watermark this read was pinned to, once the stream has reached its end.
    pub fn watermark(&self) -> Option<Position> {
        self.watermark
    }
}

impl Stream for ReadStream {
    type Item = Result<SequencedEvent, ClientError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.done {
            return Poll::Ready(None);
        }
        match this.rx.poll_recv(cx) {
            Poll::Ready(Some(ReadItem::Event(event))) => Poll::Ready(Some(Ok(event))),
            Poll::Ready(Some(ReadItem::End(watermark))) => {
                this.watermark = Some(watermark);
                this.done = true;
                Poll::Ready(None)
            }
            Poll::Ready(Some(ReadItem::Err(err))) => {
                this.done = true;
                Poll::Ready(Some(Err(err)))
            }
            // The channel closed without a terminator (connection gone): end the stream.
            Poll::Ready(None) => {
                this.done = true;
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl Drop for ReadStream {
    fn drop(&mut self) {
        if !self.done {
            send_cancel(&self.out_tx, &self.shared, self.id);
        }
        self.shared.requests.lock().unwrap().remove(&self.id);
    }
}

/// A [`Stream`] over a live subscription, yielding [`SubEvent`]s indefinitely until the
/// connection closes, an error arrives, or the stream is dropped (which cancels it server-side).
pub struct SubscribeStream {
    shared: Arc<Shared>,
    out_tx: mpsc::Sender<pb::Request>,
    id: u64,
    rx: mpsc::UnboundedReceiver<Result<SubEvent, ClientError>>,
    done: bool,
}

impl Stream for SubscribeStream {
    type Item = Result<SubEvent, ClientError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.done {
            return Poll::Ready(None);
        }
        match this.rx.poll_recv(cx) {
            Poll::Ready(Some(Ok(event))) => Poll::Ready(Some(Ok(event))),
            Poll::Ready(Some(Err(err))) => {
                this.done = true;
                Poll::Ready(Some(Err(err)))
            }
            Poll::Ready(None) => {
                this.done = true;
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl Drop for SubscribeStream {
    fn drop(&mut self) {
        if !self.done {
            send_cancel(&self.out_tx, &self.shared, self.id);
        }
        self.shared.requests.lock().unwrap().remove(&self.id);
    }
}
