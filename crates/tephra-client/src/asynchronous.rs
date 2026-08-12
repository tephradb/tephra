//! An async, multiplexing client for a tephra event store, built on tokio.
//!
//! Unlike the blocking [`Client`](super::Client), which runs one request at a time per
//! connection, an [`AsyncClient`] pipelines many requests over a single socket. It is a cheap
//! `Clone` handle to a shared connection actor: a background **reader task** demultiplexes each
//! response frame by its `request_id` into the waiting caller, and a **writer task** serializes
//! outbound frames. Reads and subscriptions are returned as [`Stream`]s; dropping one cancels it
//! server-side (a `CancelRequest`) without disturbing the other requests sharing the socket.
//!
//! ```no_run
//! use tephra_client::{AsyncClient, Event, Position, Query};
//! use tokio_stream::StreamExt;
//!
//! # async fn run() -> Result<(), Box<dyn std::error::Error>> {
//! let client = AsyncClient::connect("127.0.0.1:9000").await?;
//! client
//!     .append([Event::new("Enrolled", &["course:c1"], b"{}")?], None)
//!     .await?;
//!
//! let mut stream = client.read(Query::all(), Position::ZERO).await;
//! while let Some(sequenced) = stream.next().await {
//!     let sequenced = sequenced?;
//!     println!("{}: {}", sequenced.position(), sequenced.event().event_type());
//! }
//! # Ok(())
//! # }
//! ```

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use futures_core::Stream;
use tokio::io::{AsyncWriteExt, BufWriter};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpStream, ToSocketAddrs};
use tokio::sync::{mpsc, oneshot};
use tokio_stream::StreamExt;

use tephra_proto::convert as wire;
use tephra_proto::tephra as pb;
use tephra_proto::{DEFAULT_MAX_FRAME_LEN, read_frame_async, write_frame_async};

use super::{
    AppendCondition, AppendResult, ClientError, Event, Position, Query, SequencedEvent, SubEvent,
    UNATTRIBUTED_REQUEST_ID, event_to_pb, sequenced_from_pb, server_error,
};

/// Tuning for an [`AsyncClient`].
#[derive(Clone, Copy, Debug)]
pub struct AsyncClientConfig {
    /// Largest single frame accepted or produced (default [`DEFAULT_MAX_FRAME_LEN`]). Must match
    /// or exceed the server's limit, or a large `ReadEvents` batch is rejected as over-limit and
    /// fails the connection.
    pub max_frame_len: u32,
    /// Depth of the outbound request queue. Once full, `append`/`read`/`subscribe` await room to
    /// send (backpressure), bounding how far a fast producer can outrun a slow socket.
    pub request_queue_depth: usize,
}

impl Default for AsyncClientConfig {
    fn default() -> Self {
        AsyncClientConfig {
            max_frame_len: DEFAULT_MAX_FRAME_LEN,
            request_queue_depth: 256,
        }
    }
}

/// The connection actor's shared state: the id allocator and the registry mapping each in-flight
/// `request_id` to the sink awaiting its response.
struct Shared {
    next_id: AtomicU64,
    requests: Mutex<HashMap<u64, Sink>>,
    max_frame_len: u32,
}

impl Shared {
    fn next_id(&self) -> u64 {
        self.next_id.fetch_add(1, Ordering::Relaxed)
    }
}

/// Where the reader task delivers a response for a given request.
enum Sink {
    /// One append: a single result.
    Append(oneshot::Sender<Result<AppendResult, ClientError>>),
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

/// A cheap, cloneable handle to a multiplexed connection. Every clone shares one socket and one
/// set of background tasks; requests issued through any clone run concurrently.
#[derive(Clone)]
pub struct AsyncClient {
    shared: Arc<Shared>,
    out_tx: mpsc::Sender<pb::Request>,
}

impl AsyncClient {
    /// Connects to a server with the default [`AsyncClientConfig`].
    pub async fn connect(addr: impl ToSocketAddrs) -> std::io::Result<AsyncClient> {
        AsyncClient::connect_with(addr, AsyncClientConfig::default()).await
    }

    /// Connects to a server with an explicit [`AsyncClientConfig`], setting `TCP_NODELAY` and
    /// spawning the reader and writer tasks. The returned handle can be cloned and shared.
    pub async fn connect_with(
        addr: impl ToSocketAddrs,
        config: AsyncClientConfig,
    ) -> std::io::Result<AsyncClient> {
        let stream = TcpStream::connect(addr).await?;
        stream.set_nodelay(true)?;
        let (read_half, write_half) = stream.into_split();

        let (out_tx, out_rx) = mpsc::channel(config.request_queue_depth.max(1));
        let shared = Arc::new(Shared {
            // Ids start at 1 so 0 stays reserved as the unattributed-error sentinel.
            next_id: AtomicU64::new(1),
            requests: Mutex::new(HashMap::new()),
            max_frame_len: config.max_frame_len,
        });

        tokio::spawn(reader_task(read_half, Arc::clone(&shared)));
        tokio::spawn(writer_task(write_half, out_rx, shared.max_frame_len));

        Ok(AsyncClient { shared, out_tx })
    }

    /// Appends `events` as one atomic batch, optionally guarded by `condition`, resolving to the
    /// position range the batch was assigned. Many appends may be awaited concurrently.
    pub async fn append(
        &self,
        events: impl IntoIterator<Item = Event>,
        condition: Option<AppendCondition>,
    ) -> Result<AppendResult, ClientError> {
        let id = self.shared.next_id();
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

        let (tx, rx) = oneshot::channel();
        self.shared
            .requests
            .lock()
            .unwrap()
            .insert(id, Sink::Append(tx));
        // Await room in the outbound queue (backpressure). An error means the writer task is gone.
        if self.out_tx.send(request).await.is_err() {
            self.shared.requests.lock().unwrap().remove(&id);
            return Err(ClientError::UnexpectedEof);
        }

        // A dropped sender means the reader task ended (the connection closed) before replying.
        rx.await.unwrap_or(Err(ClientError::UnexpectedEof))
    }

    /// Starts a read, returning a [`Stream`] over the matching events in ascending position
    /// order. Awaits room in the outbound queue (backpressure) before returning.
    /// [`watermark`](ReadStream::watermark) is available once the stream ends; dropping the stream
    /// early cancels the read server-side.
    pub async fn read(&self, query: Query, after: Position) -> ReadStream {
        let id = self.shared.next_id();
        let mut read = pb::ReadRequest::new();
        read.set_query(wire::query_to_pb(&query));
        read.set_after(after.get());
        let mut request = pb::Request::new();
        request.set_request_id(id);
        request.set_read(read);

        let (tx, rx) = mpsc::unbounded_channel();
        self.shared
            .requests
            .lock()
            .unwrap()
            .insert(id, Sink::Read(tx));
        if self.out_tx.send(request).await.is_err() {
            if let Some(Sink::Read(tx)) = self.shared.requests.lock().unwrap().remove(&id) {
                let _ = tx.send(ReadItem::Err(ClientError::UnexpectedEof));
            }
        }

        ReadStream {
            shared: Arc::clone(&self.shared),
            out_tx: self.out_tx.clone(),
            id,
            rx,
            watermark: None,
            done: false,
        }
    }

    /// Convenience: drains a read fully, returning the events and the watermark it was pinned to.
    pub async fn read_all(
        &self,
        query: Query,
        after: Position,
    ) -> Result<(Vec<SequencedEvent>, Position), ClientError> {
        let mut stream = self.read(query, after).await;
        let mut events = Vec::new();
        while let Some(item) = stream.next().await {
            events.push(item?);
        }
        let watermark = stream
            .watermark()
            .ok_or_else(|| ClientError::Protocol("read ended without a watermark".to_string()))?;
        Ok((events, watermark))
    }

    /// Opens a live subscription over `query`, resuming strictly after `after`: matching durable
    /// events first, then new ones as they commit, with a [`SubEvent::CaughtUp`] marker at each
    /// live edge. Awaits room in the outbound queue before returning; dropping the returned
    /// [`Stream`] cancels the subscription server-side.
    pub async fn subscribe(&self, query: Query, after: Position) -> SubscribeStream {
        let id = self.shared.next_id();
        let mut subscribe = pb::SubscribeRequest::new();
        subscribe.set_query(wire::query_to_pb(&query));
        subscribe.set_after(after.get());
        let mut request = pb::Request::new();
        request.set_request_id(id);
        request.set_subscribe(subscribe);

        let (tx, rx) = mpsc::unbounded_channel();
        self.shared
            .requests
            .lock()
            .unwrap()
            .insert(id, Sink::Subscribe(tx));
        if self.out_tx.send(request).await.is_err() {
            if let Some(Sink::Subscribe(tx)) = self.shared.requests.lock().unwrap().remove(&id) {
                let _ = tx.send(Err(ClientError::UnexpectedEof));
            }
        }

        SubscribeStream {
            shared: Arc::clone(&self.shared),
            out_tx: self.out_tx.clone(),
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
async fn writer_task(
    write_half: OwnedWriteHalf,
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
async fn reader_task(mut read_half: OwnedReadHalf, shared: Arc<Shared>) {
    let max = shared.max_frame_len;
    let mut last_error: Option<String> = None;
    loop {
        match read_frame_async::<pb::Response, _>(&mut read_half, max).await {
            Ok(Some(response)) => {
                // A frame error the server could not attribute carries id 0 and precedes a
                // close. Remember its message so pending requests learn why.
                if response.request_id() == UNATTRIBUTED_REQUEST_ID {
                    if let pb::response::KindOneof::Error(error) = response.kind() {
                        last_error =
                            Some(error.message().to_str().unwrap_or("server error").to_string());
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
    let Some(sink) = map.remove(&id) else {
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
        }
        Sink::Read(tx) => {
            if deliver_read(&tx, response) {
                map.insert(id, Sink::Read(tx));
            }
        }
        Sink::Subscribe(tx) => {
            if deliver_subscribe(&tx, response) {
                map.insert(id, Sink::Subscribe(tx));
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
    for (_id, sink) in map.drain() {
        match sink {
            Sink::Append(tx) => {
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
