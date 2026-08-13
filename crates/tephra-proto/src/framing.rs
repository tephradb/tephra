//! Length-prefixed framing: a 4-byte big-endian `u32` length followed by that many bytes
//! of a serialized protobuf message. The server and client share exactly one framing
//! implementation so the two halves cannot drift.

use std::io::{self, Read, Write};
use std::{error, fmt};

use protobuf::{Message, ParseError, SerializeError};

/// Default cap on a single frame's body length (16 MiB). Bounds per-frame memory and
/// rejects a hostile or corrupt length before allocating for it.
pub const DEFAULT_MAX_FRAME_LEN: u32 = 16 * 1024 * 1024;

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
pub fn read_frame<M: Message, R: Read>(
    reader: &mut R,
    max_frame_len: u32,
) -> Result<Option<M>, FrameError> {
    let mut len_buf = [0u8; 4];
    if !read_full_or_eof(reader, &mut len_buf)? {
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
    reader.read_exact(&mut body)?;
    let msg = M::parse(&body).map_err(FrameError::Parse)?;
    Ok(Some(msg))
}

/// Fills `buf` completely. Returns `Ok(false)` if EOF is hit before the *first* byte (a
/// clean boundary), or `Ok(true)` once full. An EOF after some but not all bytes is an
/// `UnexpectedEof` error: a torn frame is not a clean close.
fn read_full_or_eof<R: Read>(reader: &mut R, buf: &mut [u8]) -> io::Result<bool> {
    let mut filled = 0;
    while filled < buf.len() {
        match reader.read(&mut buf[filled..]) {
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
            Err(ref err) if err.kind() == io::ErrorKind::Interrupted => {}
            Err(err) => return Err(err),
        }
    }
    Ok(true)
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
