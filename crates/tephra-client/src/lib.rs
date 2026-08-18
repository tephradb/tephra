//! A synchronous, blocking TCP client for a tephra event store.
//!
//! The client speaks clean Rust types: the shared vocabulary from
//! [`tephra-types`](tephra_types) ([`Query`], [`QueryItem`], [`AppendCondition`], [`Position`],
//! [`EventType`], [`Tag`], [`Tags`]) plus a friendly owned [`Event`] and [`SequencedEvent`].
//! The wire protobuf types are an implementation detail hidden behind these; the raw wire
//! module is still available as [`proto`] for low-level use, but it is not the API.
//!
//! One request is in flight at a time per [`Client`] (the server answers a connection's
//! requests sequentially); open more clients for concurrency.
//!
//! ```no_run
//! use tephra_client::{Client, Event, Position, Query};
//!
//! fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let mut client = Client::connect("127.0.0.1:9000")?;
//!     client.append([Event::new("Enrolled", ["course:c1"], b"{}")?], None)?;
//!
//!     let (events, _watermark) = client.read_all(Query::all(), Position::ZERO, None)?;
//!     for sequenced in events {
//!         println!("{}: {}", sequenced.position(), sequenced.event().event_type());
//!     }
//!     Ok(())
//! }
//! ```

use std::collections::VecDeque;
use std::io::{self, BufReader, BufWriter, Write};
use std::net::{Shutdown, TcpStream, ToSocketAddrs};
use std::{error, fmt};

use tephra_proto::convert as wire;
use tephra_proto::tephra as pb;
use tephra_proto::{DEFAULT_MAX_FRAME_LEN, FrameError, read_frame, write_frame};

pub use tephra_proto::convert::ErrorCode;
/// The shared vocabulary types, re-exported so the client's public API is one coherent set.
pub use tephra_types::{
    AppendCondition, EventType, NameError, Position, Query, QueryItem, Tag, Tags, TagsError,
};

/// The raw wire protobuf types, re-exported for low-level use. This is an escape hatch, not
/// the primary API: prefer the clean types above ([`Event`], [`Query`], and friends).
pub use tephra_proto::tephra as proto;

/// The async, multiplexing client (behind the `async` feature). Unlike the blocking [`Client`],
/// a single [`AsyncClient`] runs many requests concurrently over one connection.
#[cfg(feature = "async")]
mod asynchronous;
#[cfg(feature = "async")]
pub use asynchronous::{
    AsyncClient, AsyncClientConfig, ReadStream as AsyncReadStream,
    SubscribeStream as AsyncSubscribeStream,
};

/// The request id the server uses for an error it cannot attribute to a specific request (an
/// oversized or unparseable frame it rejected before decoding). Client request ids start at 1,
/// so this sentinel never collides with a real one, and such an error is accepted as applying
/// to the request currently in flight.
const UNATTRIBUTED_REQUEST_ID: u64 = 0;

// ---------------------------------------------------------------------------
// Value types
// ---------------------------------------------------------------------------

/// A friendly owned event: a type, a set of tags, and an opaque payload.
///
/// Construct with [`Event::new`], which validates the type and tags through the shared
/// `tephra-types` constructors (the same rules the server enforces).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Event {
    event_type: EventType,
    tags: Tags,
    payload: Vec<u8>,
}

impl Event {
    /// Builds an event, validating the type and tags (non-empty, within the length limit,
    /// no duplicate tags).
    pub fn new<T>(
        event_type: impl Into<Box<str>>,
        tags: impl IntoIterator<Item = T>,
        payload: impl Into<Vec<u8>>,
    ) -> Result<Event, BuildError>
    where
        T: Into<Box<str>>,
    {
        let event_type = EventType::new(event_type).map_err(BuildError::Name)?;
        let tags = tags.into_iter();
        let mut collected: Vec<Tag> = Vec::with_capacity(tags.size_hint().0);
        for tag in tags {
            collected.push(Tag::new(tag).map_err(BuildError::Name)?);
        }
        let tags = Tags::new(collected).map_err(BuildError::Tags)?;
        Ok(Event {
            event_type,
            tags,
            payload: payload.into(),
        })
    }

    /// The event type.
    pub fn event_type(&self) -> &str {
        self.event_type.as_str()
    }

    /// The tags, in sorted order.
    pub fn tags(&self) -> impl ExactSizeIterator<Item = &str> {
        self.tags.iter().map(|tag| tag.as_str())
    }

    /// The opaque payload bytes.
    pub fn payload(&self) -> &[u8] {
        &self.payload
    }
}

/// An event together with the position it was assigned in the global order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SequencedEvent {
    position: Position,
    event: Event,
}

impl SequencedEvent {
    /// The global position of this event (1-based).
    pub fn position(&self) -> Position {
        self.position
    }

    /// The event.
    pub fn event(&self) -> &Event {
        &self.event
    }

    /// Consumes this into its owned [`Event`], dropping the position.
    pub fn into_event(self) -> Event {
        self.event
    }
}

/// The outcome of a successful append: the position range the batch was assigned.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AppendResult {
    /// The position of the first event in the batch.
    pub first: Position,
    /// The position of the last event in the batch.
    pub last: Position,
}

/// A snapshot of a server's operational state, returned by [`Client::stats`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Stats {
    /// Total durable events, which (positions being dense and 1-based) is also the tip position.
    pub event_count: u64,
    /// On-disk log segments in the data directory.
    pub segment_count: u64,
    /// Total bytes on disk in the data directory (log segments plus index sidecars).
    pub disk_bytes: u64,
    /// Seconds since the server began accepting connections.
    pub uptime_seconds: u64,
    /// Connections currently being served, including this one.
    pub active_connections: u64,
    /// Live subscriptions across all connections.
    pub active_subscriptions: u64,
    /// Connections refused because the server was at its connection cap. Monotonic.
    pub connections_refused: u64,
    /// Connections reaped for exceeding a handshake, idle, or incomplete-frame timeout. Monotonic.
    pub connections_reaped: u64,
    /// The server's configured maximum concurrent connections, or `0` when unlimited.
    pub max_connections: u64,
    /// The server's crate version.
    pub version: String,
}

/// One item from a [`SubscribeStream`]: a matching event, or a live-edge marker.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SubEvent {
    /// A matching event, in ascending position order.
    Event(SequencedEvent),
    /// The subscription drained everything up to this watermark and is now tailing live.
    /// Re-armed: delivered again after each subsequent catch-up burst, watermark
    /// non-decreasing.
    CaughtUp(Position),
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Why building an [`Event`] failed.
#[derive(Debug, PartialEq, Eq)]
pub enum BuildError {
    /// An event type or tag was empty or too long.
    Name(NameError),
    /// The tag set contained a duplicate.
    Tags(TagsError),
}

impl fmt::Display for BuildError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BuildError::Name(err) => write!(f, "{err}"),
            BuildError::Tags(err) => write!(f, "{err}"),
        }
    }
}

impl error::Error for BuildError {}

/// Why a client operation failed.
#[derive(Debug)]
pub enum ClientError {
    /// A framing or transport failure (I/O, decode, or an over-limit frame).
    Frame(FrameError),
    /// The server closed the connection where a response was expected.
    UnexpectedEof,
    /// The server sent a response that does not fit the protocol (for example the wrong
    /// frame kind for the request, or an event that fails to decode).
    Protocol(String),
    /// The server returned an error response. `retryable` distinguishes an advisory
    /// same-batch append conflict (retry) from a terminal durable one.
    Server {
        code: ErrorCode,
        message: String,
        retryable: bool,
        conflict_position: Option<Position>,
    },
}

impl fmt::Display for ClientError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ClientError::Frame(err) => write!(f, "{err}"),
            ClientError::UnexpectedEof => write!(f, "server closed the connection unexpectedly"),
            ClientError::Protocol(msg) => write!(f, "protocol error: {msg}"),
            ClientError::Server {
                code,
                message,
                retryable,
                ..
            } => write!(
                f,
                "server error ({code:?}, retryable={retryable}): {message}"
            ),
        }
    }
}

impl error::Error for ClientError {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        match self {
            ClientError::Frame(err) => Some(err),
            _ => None,
        }
    }
}

impl From<FrameError> for ClientError {
    fn from(err: FrameError) -> Self {
        ClientError::Frame(err)
    }
}

impl From<io::Error> for ClientError {
    fn from(err: io::Error) -> Self {
        ClientError::Frame(FrameError::Io(err))
    }
}

// ---------------------------------------------------------------------------
// Client
// ---------------------------------------------------------------------------

/// A connection to a tephra server.
pub struct Client {
    reader: BufReader<TcpStream>,
    writer: BufWriter<TcpStream>,
    next_id: u64,
    max_frame_len: u32,
}

impl Client {
    /// Connects to a server, setting `TCP_NODELAY` for request/response latency.
    pub fn connect(addr: impl ToSocketAddrs) -> io::Result<Client> {
        let stream = TcpStream::connect(addr)?;
        stream.set_nodelay(true)?;
        let reader = BufReader::new(stream.try_clone()?);
        let writer = BufWriter::new(stream);
        Ok(Client {
            reader,
            writer,
            // Ids start at 1 so 0 stays reserved as the unattributed-error sentinel.
            next_id: 1,
            max_frame_len: DEFAULT_MAX_FRAME_LEN,
        })
    }

    /// Overrides the maximum frame length (default [`DEFAULT_MAX_FRAME_LEN`]). Must match or
    /// exceed the server's limit for large reads.
    pub fn set_max_frame_len(&mut self, max_frame_len: u32) {
        self.max_frame_len = max_frame_len;
    }

    fn next_id(&mut self) -> u64 {
        let id = self.next_id;
        self.next_id += 1;
        id
    }

    fn send(&mut self, request: &pb::Request) -> Result<(), ClientError> {
        write_frame(&mut self.writer, request, self.max_frame_len)?;
        self.writer.flush()?;
        Ok(())
    }

    /// Appends `events` as one atomic batch, optionally guarded by `condition`. Blocks until
    /// the server replies, returning the position range the batch was assigned.
    pub fn append(
        &mut self,
        events: impl IntoIterator<Item = Event>,
        condition: Option<AppendCondition>,
    ) -> Result<AppendResult, ClientError> {
        let id = self.next_id();
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
        self.send(&request)?;

        let response = self.recv()?;
        check_response_id(&response, id)?;
        match response.kind() {
            pb::response::KindOneof::Append(append) => Ok(AppendResult {
                first: Position::new(append.first()),
                last: Position::new(append.last()),
            }),
            pb::response::KindOneof::Error(error) => Err(server_error(error)),
            other => Err(ClientError::Protocol(format!(
                "unexpected response to append: {other:?}"
            ))),
        }
    }

    /// Fetches a snapshot of the server's operational state (event count, on-disk size, uptime,
    /// and live connection/subscription gauges). Blocks until the server replies.
    pub fn stats(&mut self) -> Result<Stats, ClientError> {
        let id = self.next_id();
        let mut request = pb::Request::new();
        request.set_request_id(id);
        request.set_stats(pb::StatsRequest::new());
        self.send(&request)?;

        let response = self.recv()?;
        check_response_id(&response, id)?;
        match response.kind() {
            pb::response::KindOneof::Stats(stats) => Ok(stats_from_pb(stats)),
            pb::response::KindOneof::Error(error) => Err(server_error(error)),
            other => Err(ClientError::Protocol(format!(
                "unexpected response to stats: {other:?}"
            ))),
        }
    }

    /// Starts a read, returning a streaming iterator over the matching events in ascending
    /// position order. The stream borrows the client until it is dropped.
    ///
    /// `limit` caps the number of matched events returned (`None` = unlimited). The cap is
    /// applied server-side during planning, so a selective read does work proportional to
    /// `limit` rather than to the query's full result. Combined with `after` (an exclusive
    /// lower bound) it forms a stateless pagination cursor: read a page, then read again with
    /// `after` set to the last position, with no gap and no duplicate at the seam.
    pub fn read(
        &mut self,
        query: Query,
        after: Position,
        limit: Option<u64>,
    ) -> Result<ReadStream<'_>, ClientError> {
        self.start_read(query, after, false, limit)
    }

    /// The newest-first dual of [`read`](Self::read): a streaming iterator over the matching
    /// events in **descending** position order, strictly before `before`. `before` is an
    /// exclusive upper bound, so `read_back(query, Position::MAX, limit)` streams from the tip.
    /// `limit` caps the events from the tip down; combined with `before` it paginates
    /// newest-first (read a page, then read again with `before` set to the oldest position
    /// returned).
    pub fn read_back(
        &mut self,
        query: Query,
        before: Position,
        limit: Option<u64>,
    ) -> Result<ReadStream<'_>, ClientError> {
        self.start_read(query, before, true, limit)
    }

    /// Shared submission for [`read`](Self::read) and [`read_back`](Self::read_back). `cursor`
    /// is the exclusive lower bound forward, the exclusive upper bound backward.
    fn start_read(
        &mut self,
        query: Query,
        cursor: Position,
        reverse: bool,
        limit: Option<u64>,
    ) -> Result<ReadStream<'_>, ClientError> {
        let id = self.next_id();
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
        self.send(&request)?;

        Ok(ReadStream {
            reader: &mut self.reader,
            max_frame_len: self.max_frame_len,
            request_id: id,
            buffered: VecDeque::new(),
            watermark: None,
            done: false,
        })
    }

    /// Convenience: drains a read fully into a vector, returning the events and the watermark
    /// the read was pinned to. See [`read`](Self::read) for `limit` semantics.
    pub fn read_all(
        &mut self,
        query: Query,
        after: Position,
        limit: Option<u64>,
    ) -> Result<(Vec<SequencedEvent>, Position), ClientError> {
        drain_read(self.read(query, after, limit)?)
    }

    /// The newest-first dual of [`read_all`](Self::read_all): drains a backward read into a
    /// vector (descending by position). See [`read_back`](Self::read_back).
    pub fn read_all_back(
        &mut self,
        query: Query,
        before: Position,
        limit: Option<u64>,
    ) -> Result<(Vec<SequencedEvent>, Position), ClientError> {
        drain_read(self.read_back(query, before, limit)?)
    }

    /// Opens a live subscription over `query`, resuming strictly after `after`: it streams all
    /// matching events already durable, then tails new ones as they are committed, with no gap
    /// and no duplicate at the boundary. A [`SubEvent::CaughtUp`] marker is delivered each time
    /// the stream reaches the live edge.
    ///
    /// Returns the [`SubscribeStream`] and a [`SubscribeCancel`]. The stream borrows the client
    /// for its lifetime, so this connection is dedicated to the subscription (it cannot serve
    /// other requests while subscribed). To stop from another thread, hold the `Send`
    /// [`SubscribeCancel`] and call [`cancel`](SubscribeCancel::cancel): it shuts the socket
    /// down, ending the stream.
    pub fn subscribe(
        &mut self,
        query: Query,
        after: Position,
    ) -> Result<(SubscribeStream<'_>, SubscribeCancel), ClientError> {
        let id = self.next_id();
        let mut subscribe = pb::SubscribeRequest::new();
        subscribe.set_query(wire::query_to_pb(&query));
        subscribe.set_after(after.get());
        let mut request = pb::Request::new();
        request.set_request_id(id);
        request.set_subscribe(subscribe);
        self.send(&request)?;

        // A clone of the socket for out-of-band cancellation: shutting it down unblocks the
        // stream's in-flight `read_frame`. Taken before borrowing the reader for the stream.
        let cancel = SubscribeCancel {
            stream: self.reader.get_ref().try_clone()?,
        };
        let stream = SubscribeStream {
            reader: &mut self.reader,
            max_frame_len: self.max_frame_len,
            request_id: id,
            buffered: VecDeque::new(),
            done: false,
        };
        Ok((stream, cancel))
    }

    fn recv(&mut self) -> Result<pb::Response, ClientError> {
        read_frame(&mut self.reader, self.max_frame_len)?.ok_or(ClientError::UnexpectedEof)
    }
}

// ---------------------------------------------------------------------------
// Subscription stream
// ---------------------------------------------------------------------------

/// A `Send` handle that stops a [`SubscribeStream`] from another thread by shutting down the
/// connection. A subscription otherwise never ends on its own, and the stream borrows the
/// client, so this is the way to cancel a long-lived subscriber cleanly.
pub struct SubscribeCancel {
    stream: TcpStream,
}

impl SubscribeCancel {
    /// Shuts the subscription's connection down, unblocking the stream and ending it. The
    /// stream then yields a terminating error or `None`.
    pub fn cancel(self) {
        let _ = self.stream.shutdown(Shutdown::Both);
    }
}

/// A streaming iterator over a subscription. Yields [`SubEvent`]s indefinitely (events and
/// re-armed caught-up markers) until the connection closes, the store shuts down, an error
/// frame arrives, or a [`SubscribeCancel`] stops it.
pub struct SubscribeStream<'a> {
    reader: &'a mut BufReader<TcpStream>,
    max_frame_len: u32,
    request_id: u64,
    buffered: VecDeque<SubEvent>,
    done: bool,
}

impl SubscribeStream<'_> {
    /// Reads response frames until the next event batch or caught-up marker arrives, or the
    /// stream ends (clean close sets `done` with nothing buffered).
    fn fill(&mut self) -> Result<(), ClientError> {
        loop {
            let response = match read_frame::<pb::Response, _>(self.reader, self.max_frame_len)? {
                Some(response) => response,
                // A clean close at a frame boundary (for example after a cancel): end quietly.
                None => {
                    self.done = true;
                    return Ok(());
                }
            };
            let got = response.request_id();
            if got != self.request_id && got != UNATTRIBUTED_REQUEST_ID {
                self.done = true;
                return Err(ClientError::Protocol(format!(
                    "response for request {got} does not match subscribe request {}",
                    self.request_id
                )));
            }
            match response.kind() {
                pb::response::KindOneof::ReadEvents(events) => {
                    for sequenced in events.events().iter() {
                        self.buffered
                            .push_back(SubEvent::Event(sequenced_from_pb(sequenced)?));
                    }
                    if !self.buffered.is_empty() {
                        return Ok(());
                    }
                    // An empty batch is unusual but not an error: keep reading.
                }
                pb::response::KindOneof::CaughtUp(caught_up) => {
                    self.buffered
                        .push_back(SubEvent::CaughtUp(Position::new(caught_up.watermark())));
                    return Ok(());
                }
                pb::response::KindOneof::Error(error) => {
                    self.done = true;
                    return Err(server_error(error));
                }
                other => {
                    self.done = true;
                    return Err(ClientError::Protocol(format!(
                        "unexpected response during subscribe: {other:?}"
                    )));
                }
            }
        }
    }
}

impl Iterator for SubscribeStream<'_> {
    type Item = Result<SubEvent, ClientError>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(event) = self.buffered.pop_front() {
            return Some(Ok(event));
        }
        if self.done {
            return None;
        }
        match self.fill() {
            Ok(()) => self.buffered.pop_front().map(Ok),
            Err(err) => {
                self.done = true;
                Some(Err(err))
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Read stream
// ---------------------------------------------------------------------------

/// A streaming iterator over the events of one read. Yields events in ascending position
/// order, then ends; [`watermark`](ReadStream::watermark) is available once the stream has
/// finished (it is carried on the terminating frame).
pub struct ReadStream<'a> {
    reader: &'a mut BufReader<TcpStream>,
    max_frame_len: u32,
    request_id: u64,
    buffered: VecDeque<SequencedEvent>,
    watermark: Option<Position>,
    done: bool,
}

/// Drains a [`ReadStream`] fully into a vector, returning the events and the watermark the read
/// was pinned to. Shared by [`Client::read_all`] and [`Client::read_all_back`].
fn drain_read(mut stream: ReadStream<'_>) -> Result<(Vec<SequencedEvent>, Position), ClientError> {
    let mut events = Vec::new();
    for item in stream.by_ref() {
        events.push(item?);
    }
    let watermark = stream
        .watermark()
        .ok_or_else(|| ClientError::Protocol("read ended without a watermark".to_string()))?;
    Ok((events, watermark))
}

impl ReadStream<'_> {
    /// The watermark this read was pinned to, once the stream has reached its end.
    pub fn watermark(&self) -> Option<Position> {
        self.watermark
    }

    /// Reads response frames until the next batch of events arrives, or the read ends.
    fn fill(&mut self) -> Result<(), ClientError> {
        loop {
            let response = read_frame::<pb::Response, _>(self.reader, self.max_frame_len)?
                .ok_or(ClientError::UnexpectedEof)?;
            let got = response.request_id();
            if got != self.request_id && got != UNATTRIBUTED_REQUEST_ID {
                self.done = true;
                return Err(ClientError::Protocol(format!(
                    "response for request {got} does not match read request {}",
                    self.request_id
                )));
            }
            match response.kind() {
                pb::response::KindOneof::ReadEvents(events) => {
                    for sequenced in events.events().iter() {
                        self.buffered.push_back(sequenced_from_pb(sequenced)?);
                    }
                    if !self.buffered.is_empty() {
                        return Ok(());
                    }
                    // An empty batch is unusual but not an error: keep reading.
                }
                pb::response::KindOneof::ReadEnd(end) => {
                    self.watermark = Some(Position::new(end.watermark()));
                    self.done = true;
                    return Ok(());
                }
                pb::response::KindOneof::Error(error) => {
                    self.done = true;
                    return Err(server_error(error));
                }
                other => {
                    self.done = true;
                    return Err(ClientError::Protocol(format!(
                        "unexpected response during read: {other:?}"
                    )));
                }
            }
        }
    }

    /// Consumes and discards any remaining response frames through the read's terminator, so
    /// a partially-consumed read does not leave frames in the socket for the next request to
    /// mistake as its own. Best-effort: a transport error simply ends the drain.
    fn drain(&mut self) {
        self.done = true;
        loop {
            match read_frame::<pb::Response, _>(self.reader, self.max_frame_len) {
                Ok(Some(response)) => match response.kind() {
                    // The read's single terminator: nothing follows it for this request.
                    pb::response::KindOneof::ReadEnd(_) | pb::response::KindOneof::Error(_) => {
                        return;
                    }
                    // A batch (or an unexpected kind): discard and keep draining.
                    _ => {}
                },
                // EOF or a transport error: nothing left to drain.
                Ok(None) | Err(_) => return,
            }
        }
    }
}

impl Iterator for ReadStream<'_> {
    type Item = Result<SequencedEvent, ClientError>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(sequenced) = self.buffered.pop_front() {
            return Some(Ok(sequenced));
        }
        if self.done {
            return None;
        }
        match self.fill() {
            Ok(()) => self.buffered.pop_front().map(Ok),
            Err(err) => {
                self.done = true;
                Some(Err(err))
            }
        }
    }
}

impl Drop for ReadStream<'_> {
    fn drop(&mut self) {
        // If the caller stopped early (before the terminator), drain the rest so the
        // connection stays frame-aligned and reusable for the next request.
        if !self.done {
            self.drain();
        }
    }
}

// ---------------------------------------------------------------------------
// Conversions and helpers
// ---------------------------------------------------------------------------

/// Builds a wire [`Event`](pb::Event) from a friendly [`Event`].
fn event_to_pb(event: &Event) -> pb::Event {
    let mut out = pb::Event::new();
    out.set_type(event.event_type.as_str());
    for tag in event.tags.iter() {
        out.tags_mut().push(tag.as_str());
    }
    out.set_payload(&event.payload);
    out
}

/// Builds a friendly [`SequencedEvent`] from a wire view, validating the event through the
/// shared constructors. A malformed event from the server is a protocol error.
fn sequenced_from_pb(view: pb::SequencedEventView<'_>) -> Result<SequencedEvent, ClientError> {
    let position = Position::new(view.position());
    let ev = view.event();
    let event_type =
        EventType::new(wire::as_str(ev.r#type()).map_err(protocol)?).map_err(|err| {
            ClientError::Protocol(format!("server sent an invalid event type: {err}"))
        })?;
    let tags = wire::tags_from_pb(ev.tags().iter()).map_err(protocol)?;
    let event = Event {
        event_type,
        tags,
        payload: ev.payload().to_vec(),
    };
    Ok(SequencedEvent { position, event })
}

/// Maps a wire conversion failure (invalid UTF-8, name, or tags) to a protocol error.
fn protocol(err: wire::ConvertError) -> ClientError {
    ClientError::Protocol(format!("server sent a malformed event: {err}"))
}

/// Verifies a response echoes the request's id, catching a desynced connection loudly rather
/// than returning another request's data.
fn check_response_id(response: &pb::Response, expected: u64) -> Result<(), ClientError> {
    let got = response.request_id();
    if got != expected && got != UNATTRIBUTED_REQUEST_ID {
        return Err(ClientError::Protocol(format!(
            "response for request {got} does not match request {expected}"
        )));
    }
    Ok(())
}

fn server_error(error: pb::ErrorResponseView<'_>) -> ClientError {
    ClientError::Server {
        code: ErrorCode::from(error.code()),
        message: error.message().to_str().unwrap_or_default().to_string(),
        retryable: error.retryable(),
        conflict_position: error
            .has_conflict_position()
            .then(|| Position::new(error.conflict_position())),
    }
}

fn stats_from_pb(stats: pb::StatsResponseView<'_>) -> Stats {
    Stats {
        event_count: stats.event_count(),
        segment_count: stats.segment_count(),
        disk_bytes: stats.disk_bytes(),
        uptime_seconds: stats.uptime_seconds(),
        active_connections: stats.active_connections(),
        active_subscriptions: stats.active_subscriptions(),
        connections_refused: stats.connections_refused(),
        connections_reaped: stats.connections_reaped(),
        max_connections: stats.max_connections(),
        version: stats.version().to_str().unwrap_or_default().to_string(),
    }
}

/// The README, compiled as a doctest so its code samples cannot drift from the API. Present
/// only during doctest builds (`cfg(doctest)`), so it never appears in the published docs.
#[cfg(doctest)]
#[doc = include_str!("../README.md")]
struct CrateReadme;
