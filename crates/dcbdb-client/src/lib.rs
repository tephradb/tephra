//! A synchronous, blocking TCP client for a dcbdb event store.
//!
//! One request is in flight at a time per [`Client`] (the server answers a connection's
//! requests sequentially); open more clients for concurrency. The client speaks the wire
//! protobuf types directly ([`proto`]) and never links the storage engine.
//!
//! ```no_run
//! use dcbdb_client::{Client, event, query_all};
//!
//! let mut client = Client::connect("127.0.0.1:9000").unwrap();
//! client.append(vec![event("Enrolled", &["course:c1"], b"{}")], None).unwrap();
//! let (events, _watermark) = client.read_all(query_all(), 0).unwrap();
//! for sequenced in events {
//!     println!("{}: {}", sequenced.position(), sequenced.event().r#type().to_str().unwrap());
//! }
//! ```

use std::collections::VecDeque;
use std::io::{self, BufReader, BufWriter, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::{error, fmt};

use dcbdb_proto::dcbdb as pb;
use dcbdb_proto::{DEFAULT_MAX_FRAME_LEN, FrameError, read_frame, write_frame};

/// The wire protobuf types, re-exported so callers can build and inspect messages.
pub use dcbdb_proto::dcbdb as proto;

/// The request id the server uses for an error it cannot attribute to a specific request (an
/// oversized or unparseable frame it rejected before decoding). Client request ids start at 1,
/// so this sentinel never collides with a real one, and such an error is accepted as applying
/// to the request currently in flight.
const UNATTRIBUTED_REQUEST_ID: u64 = 0;

/// Why a client operation failed.
#[derive(Debug)]
pub enum ClientError {
    /// A framing or transport failure (I/O, decode, or an over-limit frame).
    Frame(FrameError),
    /// The server closed the connection where a response was expected.
    UnexpectedEof,
    /// The server sent a response that does not fit the protocol (for example the wrong
    /// frame kind for the request).
    Protocol(String),
    /// The server returned an error response. `retryable` distinguishes an advisory
    /// same-batch append conflict (retry) from a terminal durable one.
    Server {
        code: pb::ErrorCode,
        message: String,
        retryable: bool,
        conflict_position: Option<u64>,
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

/// A connection to a dcbdb server.
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
    /// the server replies.
    pub fn append(
        &mut self,
        events: Vec<pb::Event>,
        condition: Option<pb::AppendCondition>,
    ) -> Result<pb::AppendResponse, ClientError> {
        let id = self.next_id();
        let mut append = pb::AppendRequest::new();
        for event in events {
            append.events_mut().push(event);
        }
        if let Some(condition) = condition {
            append.set_condition(condition);
        }
        let mut request = pb::Request::new();
        request.set_request_id(id);
        request.set_append(append);
        self.send(&request)?;

        let response = self.recv()?;
        check_response_id(&response, id)?;
        match response.kind() {
            pb::response::KindOneof::Append(append) => Ok(append.to_owned()),
            pb::response::KindOneof::Error(error) => Err(server_error(error)),
            other => Err(ClientError::Protocol(format!(
                "unexpected response to append: {other:?}"
            ))),
        }
    }

    /// Starts a read, returning a streaming iterator over the matching events in ascending
    /// position order. The stream borrows the client until it is dropped.
    pub fn read(&mut self, query: pb::Query, after: u64) -> Result<ReadStream<'_>, ClientError> {
        let id = self.next_id();
        let mut read = pb::ReadRequest::new();
        read.set_query(query);
        read.set_after(after);
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
    /// the read was pinned to.
    pub fn read_all(
        &mut self,
        query: pb::Query,
        after: u64,
    ) -> Result<(Vec<pb::SequencedEvent>, u64), ClientError> {
        let mut stream = self.read(query, after)?;
        let mut events = Vec::new();
        for item in stream.by_ref() {
            events.push(item?);
        }
        let watermark = stream
            .watermark()
            .ok_or_else(|| ClientError::Protocol("read ended without a watermark".to_string()))?;
        Ok((events, watermark))
    }

    fn recv(&mut self) -> Result<pb::Response, ClientError> {
        read_frame(&mut self.reader, self.max_frame_len)?.ok_or(ClientError::UnexpectedEof)
    }
}

/// A streaming iterator over the events of one read. Yields events in ascending position
/// order, then ends; [`watermark`](ReadStream::watermark) is available once the stream has
/// finished (it is carried on the terminating frame).
pub struct ReadStream<'a> {
    reader: &'a mut BufReader<TcpStream>,
    max_frame_len: u32,
    request_id: u64,
    buffered: VecDeque<pb::SequencedEvent>,
    watermark: Option<u64>,
    done: bool,
}

impl ReadStream<'_> {
    /// The watermark this read was pinned to, once the stream has reached its end.
    pub fn watermark(&self) -> Option<u64> {
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
                        self.buffered.push_back(sequenced.to_owned());
                    }
                    if !self.buffered.is_empty() {
                        return Ok(());
                    }
                    // An empty batch is unusual but not an error: keep reading.
                }
                pb::response::KindOneof::ReadEnd(end) => {
                    self.watermark = Some(end.watermark());
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
}

impl ReadStream<'_> {
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
    type Item = Result<pb::SequencedEvent, ClientError>;

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
        code: error.code(),
        message: error.message().to_str().unwrap_or_default().to_string(),
        retryable: error.retryable(),
        conflict_position: error
            .has_conflict_position()
            .then(|| error.conflict_position()),
    }
}

// --- ergonomic builders ---

/// Builds a wire [`Event`](pb::Event) from primitives.
pub fn event(ty: &str, tags: &[&str], payload: &[u8]) -> pb::Event {
    let mut event = pb::Event::new();
    event.set_type(ty);
    for tag in tags {
        event.tags_mut().push(*tag);
    }
    event.set_payload(payload);
    event
}

/// The catch-all query, matching every event.
pub fn query_all() -> pb::Query {
    let mut query = pb::Query::new();
    query.set_all(true);
    query
}

/// A query over a set of items, OR'd together.
pub fn query_items(items: Vec<pb::QueryItem>) -> pb::Query {
    let mut query = pb::Query::new();
    for item in items {
        query.items_mut().push(item);
    }
    query
}

/// A single query item: an event matches if its type is one of `types` (empty means any)
/// and its tags contain all of `tags`.
pub fn query_item(types: &[&str], tags: &[&str]) -> pb::QueryItem {
    let mut item = pb::QueryItem::new();
    for ty in types {
        item.types_mut().push(*ty);
    }
    for tag in tags {
        item.tags_mut().push(*tag);
    }
    item
}

/// An append condition: reject if any event after `after` matches `query` (`after` = 0 means
/// the whole log).
pub fn condition(query: pb::Query, after: u64) -> pb::AppendCondition {
    let mut condition = pb::AppendCondition::new();
    condition.set_fail_if_events_match(query);
    condition.set_after(after);
    condition
}
