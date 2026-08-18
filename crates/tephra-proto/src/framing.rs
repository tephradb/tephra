//! Length-prefixed framing: a 4-byte big-endian `u32` length followed by that many bytes
//! of a serialized protobuf message. The server and client share exactly one framing
//! implementation so the two halves cannot drift.

use std::io::{self, Read, Write};
use std::{error, fmt};

use protobuf::{Message, ParseError, SerializeError};

/// Default cap on a single frame's body length (16 MiB). Bounds per-frame memory and
/// rejects a hostile or corrupt length before allocating for it.
pub const DEFAULT_MAX_FRAME_LEN: u32 = 16 * 1024 * 1024;

/// Body-buffer capacity retained across frames (64 KiB). The buffer is reused to keep the
/// common small-frame path allocation-free, but a connection that once received a large frame
/// would otherwise pin that capacity (up to `max_frame_len`) for its whole lifetime. Shrinking
/// back to this watermark after an oversized frame bounds per-connection idle memory while still
/// covering a typical request without a realloc.
const MAX_RETAINED_BODY: usize = 64 * 1024;

/// Why reading or writing a frame failed.
#[derive(Debug)]
pub enum FrameError {
    /// Underlying transport I/O failure (includes an unexpected EOF partway through a frame).
    Io(io::Error),
    /// The frame body was not a valid protobuf message of the expected type.
    Parse(ParseError),
    /// The message could not be serialized.
    Serialize(SerializeError),
    /// A frame length exceeded the configured maximum.
    TooLarge { len: u32, max: u32 },
}

impl fmt::Display for FrameError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FrameError::Io(err) => write!(f, "frame i/o error: {err}"),
            FrameError::Parse(err) => write!(f, "frame decode error: {err}"),
            FrameError::Serialize(err) => write!(f, "frame encode error: {err}"),
            FrameError::TooLarge { len, max } => {
                write!(f, "frame length {len} exceeds the maximum of {max}")
            }
        }
    }
}

impl error::Error for FrameError {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        match self {
            FrameError::Io(err) => Some(err),
            _ => None,
        }
    }
}

impl From<io::Error> for FrameError {
    fn from(err: io::Error) -> Self {
        FrameError::Io(err)
    }
}

/// Serializes `msg` and writes it as one length-prefixed frame. Does not flush; the caller
/// flushes at a response boundary.
pub fn write_frame<M: Message, W: Write>(
    writer: &mut W,
    msg: &M,
    max_frame_len: u32,
) -> Result<(), FrameError> {
    let body = msg.serialize().map_err(FrameError::Serialize)?;
    if body.len() as u64 > u64::from(max_frame_len) {
        return Err(FrameError::TooLarge {
            len: body.len().min(u32::MAX as usize) as u32,
            max: max_frame_len,
        });
    }
    writer.write_all(&(body.len() as u32).to_be_bytes())?;
    writer.write_all(&body)?;
    Ok(())
}

/// Reads one length-prefixed frame and parses it as `M`. Returns `Ok(None)` on a clean EOF
/// at a frame boundary (the peer closed between frames), distinguishing an orderly
/// disconnect from a frame torn partway through (which is an `Io` error).
///
/// This is the blocking convenience over [`FrameReader`]: it drives a fresh reader to completion,
/// so the two share exactly one framing implementation.
pub fn read_frame<M: Message, R: Read>(
    reader: &mut R,
    max_frame_len: u32,
) -> Result<Option<M>, FrameError> {
    let mut frames = FrameReader::new();
    loop {
        match frames.poll::<M, R>(reader, max_frame_len)? {
            FramePoll::Frame(msg) => return Ok(Some(msg)),
            FramePoll::Eof => return Ok(None),
            // A partial read: loop to read the rest.
            FramePoll::Progress => {}
            // A read that would block. For a plain blocking reader this never happens; for a reader
            // with a read timeout it means the timeout fired, which for this blocking convenience is
            // a failure (a hung peer), so surface it rather than spin. This preserves the pre-
            // `FrameReader` contract that a read-timeout socket makes `read_frame` return an error.
            FramePoll::WouldBlock { .. } => {
                return Err(FrameError::Io(io::Error::new(
                    io::ErrorKind::WouldBlock,
                    "read timed out before a complete frame",
                )));
            }
        }
    }
}

/// The outcome of one [`FrameReader::poll`].
#[derive(Debug)]
pub enum FramePoll<M> {
    /// A complete frame was read and parsed.
    Frame(M),
    /// A clean EOF at a frame boundary (the peer closed between frames).
    Eof,
    /// A read advanced the current frame without completing it. Poll again; more bytes may be
    /// ready immediately. A plain blocking reader (no read timeout) only ever yields this,
    /// `Frame`, or `Eof`.
    Progress,
    /// The read would block: a non-blocking or read-timeout socket with no bytes ready.
    /// `in_progress` is `true` once any byte of the current frame has been buffered, `false` at a
    /// frame boundary. A caller with a read timeout treats this as "no data within the timeout".
    /// Partial state is retained, so polling again never desynchronizes the stream.
    WouldBlock { in_progress: bool },
}

/// A resumable, timeout-aware reader for length-prefixed frames.
///
/// Each [`poll`](Self::poll) issues at most one `read` and returns [`FramePoll::Progress`] (a
/// partial read) or [`FramePoll::WouldBlock`] until a whole frame is buffered. Returning after
/// every read (rather than looping internally until the reader blocks) is deliberate and
/// load-bearing: it lets a caller enforce a wall-clock deadline *between* reads, so a slow trickle
/// that keeps a per-read socket timeout from ever firing (each byte resets it) is still bounded.
/// Partial state is retained across polls, so a non-blocking or read-timeout socket never
/// desynchronizes the stream.
#[derive(Default)]
pub struct FrameReader {
    len_buf: [u8; 4],
    // Bytes of the 4-byte length prefix read so far; `== 4` means the body phase is active. This
    // doubles as the "have length" flag, so there is no second field to keep in sync.
    len_filled: usize,
    // Reused across frames (cleared, not dropped) to keep the allocation warm.
    body: Vec<u8>,
    body_filled: usize,
}

/// The result of a single `read` into a buffer.
enum ReadStep {
    /// Some bytes were read (progress toward filling the buffer).
    Progress,
    /// The read would block (a non-blocking socket, or its read timeout elapsed).
    WouldBlock,
    /// The reader returned EOF (`Ok(0)`).
    Eof,
}

impl FrameReader {
    /// A fresh reader positioned at a frame boundary.
    pub fn new() -> FrameReader {
        FrameReader::default()
    }

    /// Reads once toward the next frame: [`FramePoll::Frame`] when a whole frame is parsed,
    /// [`FramePoll::Eof`] on a clean boundary close, or [`FramePoll::Progress`] /
    /// [`FramePoll::WouldBlock`] otherwise (poll again). At most one `read` is issued per call, so
    /// the caller regains control to check its own deadlines even while bytes are still trickling in.
    pub fn poll<M: Message, R: Read>(
        &mut self,
        reader: &mut R,
        max_frame_len: u32,
    ) -> Result<FramePoll<M>, FrameError> {
        // Phase 1: the 4-byte length prefix. `len_filled` reaches 4 exactly once, then phase 2 runs.
        if self.len_filled < 4 {
            match read_once(reader, &mut self.len_buf, &mut self.len_filled)? {
                ReadStep::Eof => {
                    return if self.len_filled == 0 {
                        Ok(FramePoll::Eof)
                    } else {
                        Err(torn("eof partway through a frame length prefix"))
                    };
                }
                ReadStep::WouldBlock => {
                    return Ok(FramePoll::WouldBlock {
                        in_progress: self.len_filled > 0,
                    });
                }
                ReadStep::Progress => {
                    if self.len_filled < 4 {
                        return Ok(FramePoll::Progress);
                    }
                    let len = u32::from_be_bytes(self.len_buf);
                    if len > max_frame_len {
                        return Err(FrameError::TooLarge {
                            len,
                            max: max_frame_len,
                        });
                    }
                    // Reuse the buffer across frames: `clear` keeps the allocation, `resize` sizes
                    // it. A zero-length body is already complete.
                    self.body.clear();
                    self.body.resize(len as usize, 0);
                    self.body_filled = 0;
                    if len == 0 {
                        return self.finish::<M>();
                    }
                    return Ok(FramePoll::Progress);
                }
            }
        }

        // Phase 2: the body.
        match read_once(reader, &mut self.body, &mut self.body_filled)? {
            ReadStep::Eof => Err(torn("eof partway through a frame body")),
            ReadStep::WouldBlock => Ok(FramePoll::WouldBlock { in_progress: true }),
            ReadStep::Progress => {
                if self.body_filled < self.body.len() {
                    Ok(FramePoll::Progress)
                } else {
                    self.finish::<M>()
                }
            }
        }
    }

    /// Parses the fully-buffered body, then resets to a frame boundary, retaining the buffer up to
    /// [`MAX_RETAINED_BODY`] so a one-off large frame does not pin its capacity for the connection's
    /// lifetime.
    fn finish<M: Message>(&mut self) -> Result<FramePoll<M>, FrameError> {
        let msg = M::parse(&self.body).map_err(FrameError::Parse)?;
        self.len_filled = 0;
        self.body.clear();
        if self.body.capacity() > MAX_RETAINED_BODY {
            self.body.shrink_to(MAX_RETAINED_BODY);
        }
        self.body_filled = 0;
        Ok(FramePoll::Frame(msg))
    }
}

/// A torn-frame error: a length prefix promised bytes that a clean EOF never delivered.
fn torn(msg: &'static str) -> FrameError {
    FrameError::Io(io::Error::new(io::ErrorKind::UnexpectedEof, msg))
}

/// Issues a single `read` into `buf` from `filled` onward, tracking progress so a would-block can
/// be resumed. `Interrupted` is retried within the call; any real progress returns immediately, so
/// the caller regains control to re-check its deadlines between reads.
fn read_once<R: Read>(
    reader: &mut R,
    buf: &mut [u8],
    filled: &mut usize,
) -> Result<ReadStep, FrameError> {
    loop {
        match reader.read(&mut buf[*filled..]) {
            Ok(0) => return Ok(ReadStep::Eof),
            Ok(n) => {
                *filled += n;
                return Ok(ReadStep::Progress);
            }
            Err(ref err) if err.kind() == io::ErrorKind::Interrupted => {}
            Err(ref err)
                if err.kind() == io::ErrorKind::WouldBlock
                    || err.kind() == io::ErrorKind::TimedOut =>
            {
                return Ok(ReadStep::WouldBlock);
            }
            Err(err) => return Err(FrameError::Io(err)),
        }
    }
}

// ---------------------------------------------------------------------------
// Async framing (tokio)
// ---------------------------------------------------------------------------

/// The async counterpart of [`write_frame`]: serializes `msg` and writes it as one
/// length-prefixed frame over an [`AsyncWrite`](tokio::io::AsyncWrite). Does not flush; the caller flushes at a
/// response boundary.
#[cfg(feature = "tokio")]
pub async fn write_frame_async<M, W>(
    writer: &mut W,
    msg: &M,
    max_frame_len: u32,
) -> Result<(), FrameError>
where
    M: Message,
    W: tokio::io::AsyncWrite + Unpin,
{
    use tokio::io::AsyncWriteExt;

    let body = msg.serialize().map_err(FrameError::Serialize)?;
    if body.len() as u64 > u64::from(max_frame_len) {
        return Err(FrameError::TooLarge {
            len: body.len().min(u32::MAX as usize) as u32,
            max: max_frame_len,
        });
    }
    writer.write_all(&(body.len() as u32).to_be_bytes()).await?;
    writer.write_all(&body).await?;
    Ok(())
}

/// The async counterpart of [`read_frame`]: reads one length-prefixed frame from an
/// [`AsyncRead`](tokio::io::AsyncRead) and parses it as `M`. Returns `Ok(None)` on a clean EOF at a frame boundary
/// (the peer closed between frames), mirroring the synchronous version.
#[cfg(feature = "tokio")]
pub async fn read_frame_async<M, R>(
    reader: &mut R,
    max_frame_len: u32,
) -> Result<Option<M>, FrameError>
where
    M: Message,
    R: tokio::io::AsyncRead + Unpin,
{
    let mut len_buf = [0u8; 4];
    if !read_full_or_eof_async(reader, &mut len_buf).await? {
        return Ok(None);
    }
    let len = u32::from_be_bytes(len_buf);
    if len > max_frame_len {
        return Err(FrameError::TooLarge {
            len,
            max: max_frame_len,
        });
    }
    let mut body = vec![0u8; len as usize];
    read_exact_async(reader, &mut body).await?;
    let msg = M::parse(&body).map_err(FrameError::Parse)?;
    Ok(Some(msg))
}

/// Async analog of [`read_full_or_eof`]: `Ok(false)` on a clean EOF before the first byte,
/// `Ok(true)` once full, `UnexpectedEof` on a torn read. `tokio`'s own `read_exact` cannot
/// distinguish a clean boundary from a torn one, so the length prefix is read here.
#[cfg(feature = "tokio")]
async fn read_full_or_eof_async<R>(reader: &mut R, buf: &mut [u8]) -> io::Result<bool>
where
    R: tokio::io::AsyncRead + Unpin,
{
    use tokio::io::AsyncReadExt;

    let mut filled = 0;
    while filled < buf.len() {
        match reader.read(&mut buf[filled..]).await {
            Ok(0) => {
                if filled == 0 {
                    return Ok(false);
                }
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "eof partway through a frame length prefix",
                ));
            }
            Ok(n) => filled += n,
            Err(err) => return Err(err),
        }
    }
    Ok(true)
}

/// Fills `buf` completely from an [`AsyncRead`](tokio::io::AsyncRead); an early EOF (a torn frame body) is an
/// `UnexpectedEof` error, since a length prefix already promised these bytes.
#[cfg(feature = "tokio")]
async fn read_exact_async<R>(reader: &mut R, buf: &mut [u8]) -> io::Result<()>
where
    R: tokio::io::AsyncRead + Unpin,
{
    use tokio::io::AsyncReadExt;

    let mut filled = 0;
    while filled < buf.len() {
        match reader.read(&mut buf[filled..]).await {
            Ok(0) => {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "eof partway through a frame body",
                ));
            }
            Ok(n) => filled += n,
            Err(err) => return Err(err),
        }
    }
    Ok(())
}

#[cfg(test)]
mod frame_reader_tests {
    use std::collections::VecDeque;

    use protobuf::Serialize;

    use super::*;
    use crate::tephra::Request;

    /// A scripted reader: it hands out `Data` chunks (across as many reads as the caller needs),
    /// returns `WouldBlock` once per `Block` marker, and reports EOF (`Ok(0)`) once the script is
    /// exhausted.
    struct MockReader {
        events: VecDeque<Event>,
        buf: Vec<u8>,
        pos: usize,
    }

    enum Event {
        Data(Vec<u8>),
        Block,
    }

    impl MockReader {
        fn new(events: Vec<Event>) -> MockReader {
            MockReader {
                events: events.into(),
                buf: Vec::new(),
                pos: 0,
            }
        }
    }

    impl Read for MockReader {
        fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
            if self.pos < self.buf.len() {
                let n = (self.buf.len() - self.pos).min(out.len());
                out[..n].copy_from_slice(&self.buf[self.pos..self.pos + n]);
                self.pos += n;
                return Ok(n);
            }
            match self.events.pop_front() {
                Some(Event::Data(data)) => {
                    self.buf = data;
                    self.pos = 0;
                    self.read(out)
                }
                Some(Event::Block) => Err(io::Error::from(io::ErrorKind::WouldBlock)),
                None => Ok(0),
            }
        }
    }

    /// A length-prefixed frame carrying a `Request` with `request_id`.
    fn framed(request_id: u64) -> Vec<u8> {
        let mut request = Request::new();
        request.set_request_id(request_id);
        let body = request.serialize().unwrap();
        let mut out = (body.len() as u32).to_be_bytes().to_vec();
        out.extend_from_slice(&body);
        out
    }

    /// A length-prefixed frame whose body is at least `payload_len` bytes, built from an append
    /// request carrying a single event with a payload of that size.
    fn framed_large(request_id: u64, payload_len: usize) -> Vec<u8> {
        let mut event = crate::tephra::Event::new();
        event.set_payload(vec![0u8; payload_len]);
        let mut append = crate::tephra::AppendRequest::new();
        append.events_mut().push(event);
        let mut request = Request::new();
        request.set_request_id(request_id);
        request.set_append(append);
        let body = request.serialize().unwrap();
        let mut out = (body.len() as u32).to_be_bytes().to_vec();
        out.extend_from_slice(&body);
        out
    }

    /// Drives `poll` until it yields a frame (or panics on EOF), returning the request id.
    fn drive_to_frame(reader: &mut FrameReader, mock: &mut MockReader) -> u64 {
        loop {
            match reader
                .poll::<Request, _>(mock, DEFAULT_MAX_FRAME_LEN)
                .unwrap()
            {
                FramePoll::Frame(req) => return req.request_id(),
                FramePoll::Progress | FramePoll::WouldBlock { .. } => {}
                FramePoll::Eof => panic!("unexpected eof before a frame"),
            }
        }
    }

    #[test]
    fn reads_a_whole_frame() {
        let mut mock = MockReader::new(vec![Event::Data(framed(7))]);
        let mut reader = FrameReader::new();
        assert_eq!(drive_to_frame(&mut reader, &mut mock), 7);
    }

    #[test]
    fn poll_yields_after_each_read_so_a_trickle_is_observable() {
        // Deliver the frame one byte per read, with no would-block between bytes. poll must still
        // return Progress after each read rather than looping internally to consume the whole
        // trickle, so a caller can check a wall-clock deadline between bytes. This is the property
        // that makes the incomplete-frame timeout actually fire against a sub-timeout trickle; its
        // absence was the headline bug.
        let frame = framed(21);
        let total = frame.len();
        let events = frame.iter().map(|b| Event::Data(vec![*b])).collect();
        let mut mock = MockReader::new(events);
        let mut reader = FrameReader::new();
        let mut progress = 0;
        loop {
            match reader
                .poll::<Request, _>(&mut mock, DEFAULT_MAX_FRAME_LEN)
                .unwrap()
            {
                FramePoll::Frame(req) => {
                    assert_eq!(req.request_id(), 21);
                    break;
                }
                // Every read delivers a byte, so each poll is progress, never a would-block.
                FramePoll::Progress => progress += 1,
                other => panic!("expected progress, got {other:?}"),
            }
        }
        // One Progress per byte except the final read that completes the frame: proof that no
        // internal loop hid the trickle from the caller.
        assert_eq!(
            progress,
            total - 1,
            "expected one poll per byte for {total} bytes"
        );
    }

    #[test]
    fn would_block_is_idle_before_the_first_byte() {
        // A would-block before any byte of the next frame: idle at a boundary, not in progress.
        let mut mock = MockReader::new(vec![Event::Block, Event::Data(framed(9))]);
        let mut reader = FrameReader::new();
        match reader
            .poll::<Request, _>(&mut mock, DEFAULT_MAX_FRAME_LEN)
            .unwrap()
        {
            FramePoll::WouldBlock { in_progress } => assert!(!in_progress, "idle at a boundary"),
            other => panic!("expected a boundary would-block, got {other:?}"),
        }
        assert_eq!(drive_to_frame(&mut reader, &mut mock), 9);
    }

    #[test]
    fn resumes_across_a_would_block_mid_length_prefix() {
        let frame = framed(11);
        let (head, tail) = frame.split_at(2);
        let mut mock = MockReader::new(vec![
            Event::Data(head.to_vec()),
            Event::Block,
            Event::Data(tail.to_vec()),
        ]);
        let mut reader = FrameReader::new();
        // The 2-byte read makes progress; the following would-block is in progress.
        assert!(matches!(
            reader.poll::<Request, _>(&mut mock, DEFAULT_MAX_FRAME_LEN),
            Ok(FramePoll::Progress)
        ));
        match reader
            .poll::<Request, _>(&mut mock, DEFAULT_MAX_FRAME_LEN)
            .unwrap()
        {
            FramePoll::WouldBlock { in_progress } => assert!(in_progress, "partial length prefix"),
            other => panic!("expected an in-progress would-block, got {other:?}"),
        }
        assert_eq!(drive_to_frame(&mut reader, &mut mock), 11);
    }

    #[test]
    fn resumes_across_a_would_block_mid_body() {
        let frame = framed(13);
        // The length prefix plus one body byte, then a stall, then the rest.
        let (head, tail) = frame.split_at(5);
        let mut mock = MockReader::new(vec![
            Event::Data(head.to_vec()),
            Event::Block,
            Event::Data(tail.to_vec()),
        ]);
        let mut reader = FrameReader::new();
        // Read the length prefix (Progress), read one body byte (Progress), then hit the block.
        for _ in 0..2 {
            assert!(matches!(
                reader.poll::<Request, _>(&mut mock, DEFAULT_MAX_FRAME_LEN),
                Ok(FramePoll::Progress)
            ));
        }
        match reader
            .poll::<Request, _>(&mut mock, DEFAULT_MAX_FRAME_LEN)
            .unwrap()
        {
            FramePoll::WouldBlock { in_progress } => assert!(in_progress, "partial body"),
            other => panic!("expected an in-progress would-block, got {other:?}"),
        }
        assert_eq!(drive_to_frame(&mut reader, &mut mock), 13);
    }

    #[test]
    fn shrinks_the_body_buffer_after_an_oversized_frame() {
        // A one-off large frame must not pin its capacity for the connection's lifetime: after it is
        // parsed, the reused body buffer shrinks back toward the watermark. Without the shrink the
        // capacity would stay at least the large frame's size.
        let large = 512 * 1024;
        let mut mock = MockReader::new(vec![Event::Data(framed_large(1, large))]);
        let mut reader = FrameReader::new();
        assert_eq!(drive_to_frame(&mut reader, &mut mock), 1);
        assert!(
            reader.body.capacity() < large,
            "body buffer retained {} bytes after a {large}-byte frame",
            reader.body.capacity(),
        );
    }

    #[test]
    fn reuses_its_buffer_across_frames() {
        // Two frames back to back through one reader: the second must parse correctly after the
        // first's reset (which clears, not drops, the body buffer).
        let mut bytes = framed(1);
        bytes.extend_from_slice(&framed(2));
        let mut mock = MockReader::new(vec![Event::Data(bytes)]);
        let mut reader = FrameReader::new();
        assert_eq!(drive_to_frame(&mut reader, &mut mock), 1);
        assert_eq!(drive_to_frame(&mut reader, &mut mock), 2);
    }

    #[test]
    fn clean_eof_at_a_boundary() {
        let mut mock = MockReader::new(vec![]);
        let mut reader = FrameReader::new();
        assert!(matches!(
            reader.poll::<Request, _>(&mut mock, DEFAULT_MAX_FRAME_LEN),
            Ok(FramePoll::Eof)
        ));
    }

    #[test]
    fn a_torn_frame_is_an_error() {
        // A length prefix promising a body, then EOF before the body: not a clean boundary.
        let frame = framed(15);
        let len_prefix = frame[..4].to_vec();
        let mut mock = MockReader::new(vec![Event::Data(len_prefix)]);
        let mut reader = FrameReader::new();
        // First poll reads the length prefix (Progress), second hits EOF mid-body (torn).
        let mut err = None;
        for _ in 0..3 {
            match reader.poll::<Request, _>(&mut mock, DEFAULT_MAX_FRAME_LEN) {
                Ok(FramePoll::Progress | FramePoll::WouldBlock { .. }) => {}
                Ok(other) => panic!("expected an error, got {other:?}"),
                Err(e) => {
                    err = Some(e);
                    break;
                }
            }
        }
        assert!(
            matches!(err, Some(FrameError::Io(_))),
            "torn frame is an i/o error, got {err:?}"
        );
    }

    #[test]
    fn an_oversized_length_is_rejected() {
        // A 4-byte length prefix claiming more than the max, with no body sent.
        let mut prefix = 100u32.to_be_bytes().to_vec();
        prefix.extend_from_slice(&[0u8; 4]);
        let mut mock = MockReader::new(vec![Event::Data(prefix)]);
        let mut reader = FrameReader::new();
        let err = reader.poll::<Request, _>(&mut mock, 16).unwrap_err();
        assert!(matches!(err, FrameError::TooLarge { len: 100, max: 16 }));
    }
}
