//! TLS transport adapters shared by the server and client.
//!
//! A rustls session is a single, non-clonable object whose record operations all need `&mut`
//! access, so the `TcpStream::try_clone` split the plaintext transport relies on does not
//! translate. Instead the two independent socket handles are kept (one for the reader, one for the
//! writer), and only the in-memory session is shared, behind a [`Mutex`] held for the duration of a
//! record-layer step and never across a blocking socket syscall. The writer is the sole
//! socket-writer, so outbound TLS records reach the wire in the order they were encrypted.
//!
//! [`TlsReadHalf`] and [`TlsWriteHalf`] implement [`Read`] and [`Write`], so the generic framing
//! path ([`crate::FrameReader`], [`crate::write_frame`]) drives them exactly as it drives a raw
//! `TcpStream`, with no behavioural difference beyond the crypto.

use std::io::{self, ErrorKind, Read, Write};
use std::mem;
use std::net::TcpStream;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use rustls::Connection;

/// Ciphertext staged from one socket read. A single TLS record tops out near 16 KiB plus overhead,
/// so this comfortably holds one record per read while keeping the reader to one syscall per poll.
const RX_CHUNK: usize = 32 * 1024;

/// Plaintext fed into the session per interleaved write. Bounding the amount encrypted before it is
/// drained to the socket keeps a non-draining client blocking on the socket rather than ballooning
/// the session's unbounded outbound buffer.
const TX_CHUNK: usize = 16 * 1024;

/// One TLS session, shared by a read half and a write half. The mutex guards only in-memory record
/// processing; the blocking socket reads and writes happen on the halves' own handles, outside it.
pub struct TlsConn {
    conn: Mutex<Connection>,
}

impl TlsConn {
    /// Wraps a completed (post-handshake) rustls connection. Both `ServerConnection` and
    /// `ClientConnection` convert into [`Connection`], so one adapter serves both ends.
    pub fn new(conn: impl Into<Connection>) -> Arc<TlsConn> {
        Arc::new(TlsConn {
            conn: Mutex::new(conn.into()),
        })
    }

    /// Locks the session, recovering from a poisoned lock rather than propagating the panic. A
    /// panic in one half must not cascade into the other (which would kill both connection threads
    /// and skip the writer's teardown flush); the connection is being torn down regardless.
    fn lock(&self) -> MutexGuard<'_, Connection> {
        self.conn
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Splits into a read half and a write half. `read_sock` and `write_sock` must be independent
    /// clones of the same underlying socket (as `TcpStream::try_clone` yields), so the reader and
    /// writer never share a file descriptor.
    pub fn split(
        self: &Arc<TlsConn>,
        read_sock: TcpStream,
        write_sock: TcpStream,
    ) -> (TlsReadHalf, TlsWriteHalf) {
        (
            TlsReadHalf {
                conn: Arc::clone(self),
                sock: read_sock,
                rx: vec![0u8; RX_CHUNK],
                rx_pos: 0,
                rx_len: 0,
            },
            TlsWriteHalf {
                conn: Arc::clone(self),
                sock: write_sock,
                tx: Vec::with_capacity(TX_CHUNK),
            },
        )
    }
}

/// The read half of a split TLS connection. Reads ciphertext on its own socket handle and decrypts
/// it under the shared session lock; never writes to the socket (the single-writer rule is a
/// structural property, not a discipline: this type has no write handle).
pub struct TlsReadHalf {
    conn: Arc<TlsConn>,
    sock: TcpStream,
    /// Ciphertext staged from the last socket read; `rx[rx_pos..rx_len]` is not yet fed to the
    /// session. Retained across reads so a record split over socket reads is never dropped.
    rx: Vec<u8>,
    rx_pos: usize,
    rx_len: usize,
}

impl Read for TlsReadHalf {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        loop {
            // Serve any plaintext already decrypted, or a clean end of stream.
            if let Some(n) = self.serve_plaintext(out)? {
                return Ok(n);
            }
            // Feed any staged-but-unconsumed ciphertext into the session before reading more, so a
            // record split over socket reads (or a `read_tls` that stopped short of the buffer) is
            // never dropped. `read_tls` reads from an in-memory slice, so the lock is not held
            // across a syscall.
            if self.rx_pos < self.rx_len {
                let mut conn = self.conn.lock();
                let mut cursor: &[u8] = &self.rx[self.rx_pos..self.rx_len];
                self.rx_pos += conn.read_tls(&mut cursor)?;
                conn.process_new_packets()
                    .map_err(|err| io::Error::new(ErrorKind::InvalidData, err))?;
                continue;
            }
            // Staging buffer drained: one socket read, outside the lock. A record that carries no
            // application plaintext (a KeyUpdate, a session ticket) or one still arriving leaves no
            // plaintext, so we loop and read more: on a blocking socket that blocks until the record
            // completes (never a spurious end of stream), and on the server's timeout socket a
            // would-block surfaces so the poll loop can check its deadlines.
            match self.sock.read(&mut self.rx) {
                // TCP end of stream. A peer that closed with or without a `close_notify` is an EOF
                // here; a frame torn mid-body is already caught one layer up by the frame reader.
                Ok(0) => {
                    return self
                        .serve_plaintext(out)
                        .map(|plaintext| plaintext.unwrap_or(0));
                }
                Ok(n) => {
                    self.rx_pos = 0;
                    self.rx_len = n;
                }
                Err(err) if err.kind() == ErrorKind::Interrupted => continue,
                Err(err)
                    if err.kind() == ErrorKind::WouldBlock || err.kind() == ErrorKind::TimedOut =>
                {
                    return Err(io::Error::from(ErrorKind::WouldBlock));
                }
                Err(err) => return Err(err),
            }
        }
    }
}

impl TlsReadHalf {
    /// Drains buffered plaintext into `out`. `Ok(Some(0))` is a clean end of stream, `Ok(Some(n))`
    /// is `n` bytes of data, and `Ok(None)` means no plaintext is buffered yet (the connection is
    /// live). A peer that dropped without a `close_notify` is reported as a clean end of stream.
    fn serve_plaintext(&self, out: &mut [u8]) -> io::Result<Option<usize>> {
        let mut conn = self.conn.lock();
        match conn.reader().read(out) {
            Ok(n) => Ok(Some(n)),
            Err(err) if err.kind() == ErrorKind::WouldBlock => Ok(None),
            Err(err) if err.kind() == ErrorKind::UnexpectedEof => Ok(Some(0)),
            Err(err) => Err(err),
        }
    }

    /// A clone of the underlying socket, for shutting the connection down out of band (for example
    /// a subscription cancel from another thread). Shutting it down unblocks a read parked here.
    pub fn socket_handle(&self) -> io::Result<TcpStream> {
        self.sock.try_clone()
    }
}

/// The write half of a split TLS connection, and the connection's sole socket-writer. Encrypts
/// under the shared session lock and drains the ciphertext to its own socket handle outside it.
pub struct TlsWriteHalf {
    conn: Arc<TlsConn>,
    sock: TcpStream,
    tx: Vec<u8>,
}

impl Write for TlsWriteHalf {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        // Encrypt in bounded chunks, draining each to the socket before encrypting the next, so a
        // client that stops reading backs up on the socket rather than in the session's buffer.
        let mut offset = 0;
        while offset < buf.len() {
            let end = (offset + TX_CHUNK).min(buf.len());
            {
                let mut conn = self.conn.lock();
                conn.writer().write_all(&buf[offset..end])?;
            }
            self.drain()?;
            offset = end;
        }
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        // Also drains any session output produced by the read half (a KeyUpdate answer, or a fatal
        // alert queued when a bad record was rejected), since this is the only socket-writer.
        self.drain()?;
        self.sock.flush()
    }
}

impl TlsWriteHalf {
    /// Writes all pending session ciphertext to the socket, producing it under the lock and writing
    /// it outside the lock, one buffer at a time.
    fn drain(&mut self) -> io::Result<()> {
        loop {
            let mut cipher = mem::take(&mut self.tx);
            cipher.clear();
            let produced = {
                let mut conn = self.conn.lock();
                if !conn.wants_write() {
                    self.tx = cipher;
                    return Ok(());
                }
                conn.write_tls(&mut cipher)?
            };
            let write = if produced == 0 {
                Ok(())
            } else {
                self.sock.write_all(&cipher)
            };
            self.tx = cipher;
            write?;
            if produced == 0 {
                return Ok(());
            }
        }
    }
}

/// Drives a rustls handshake to completion on a blocking, timeout-bounded socket, one socket
/// syscall per loop iteration so the total `deadline` (measured from `started`) is re-checked
/// before every syscall.
///
/// This is deliberately not rustls's own `complete_io`: that helper loops internally, doing many
/// reads per call, so a deadline checked only around it is evadable by a client that dribbles one
/// byte per timeout to stretch the handshake without bound. Stepping one syscall at a time closes
/// that gap on both the read and the write side, since a peer that stops reading mid-handshake is
/// bounded here too.
///
/// `deadline` is the total budget (`None` means unbounded); `started` is when the connection was
/// accepted, so time already spent before the handshake counts against it. The socket is expected
/// to carry a read/write timeout, so a retryable error only surfaces after that timeout elapses,
/// never as a busy-spin.
pub fn drive_handshake(
    conn: &mut Connection,
    sock: &mut TcpStream,
    deadline: Option<Duration>,
    started: Instant,
) -> io::Result<()> {
    // Only the handshake proper is bound by the deadline. All of our mandatory flights are queued
    // while `is_handshaking()` is still true, so they flush inside this loop; the flight queued as
    // it clears (the peer is authenticated by then) is handled best-effort below.
    while conn.is_handshaking() {
        check_deadline(deadline, started)?;
        // Flush queued handshake output first, one write syscall at a time, so a peer that stops
        // reading mid-handshake cannot park us in an unbounded write.
        if conn.wants_write() {
            match conn.write_tls(sock) {
                Ok(_) => {}
                Err(err) if is_retryable(&err) => {}
                Err(err) => return Err(err),
            }
            continue;
        }
        // Nothing to send: read one batch of handshake input and process it.
        match conn.read_tls(sock) {
            Ok(0) => {
                return Err(io::Error::new(
                    ErrorKind::UnexpectedEof,
                    "peer closed during tls handshake",
                ));
            }
            Ok(_) => {
                conn.process_new_packets()
                    .map_err(|err| io::Error::new(ErrorKind::InvalidData, err))?;
            }
            Err(err) if is_retryable(&err) => {}
            Err(err) => return Err(err),
        }
    }
    flush_final_flight(conn, sock)
}

/// Pushes whatever the session queued as `is_handshaking()` cleared: the client's own Finished
/// (which the server is still blocked on), or on the server the optional NewSessionTicket flight.
/// Best-effort and not bound by the handshake deadline: the peer is authenticated now, so a slow
/// reader is handed to the request-phase writer rather than reaped, and any bytes left unsent here
/// flush there under ordinary backpressure. In practice this flight is a few hundred bytes and
/// clears in one write.
fn flush_final_flight(conn: &mut Connection, sock: &mut TcpStream) -> io::Result<()> {
    while conn.wants_write() {
        match conn.write_tls(sock) {
            Ok(0) => break,
            Ok(_) => {}
            Err(err) if is_retryable(&err) => break,
            Err(err) => return Err(err),
        }
    }
    Ok(())
}

/// Returns `TimedOut` once `started.elapsed()` reaches `deadline`; a `None` deadline never trips.
fn check_deadline(deadline: Option<Duration>, started: Instant) -> io::Result<()> {
    if let Some(deadline) = deadline {
        if started.elapsed() >= deadline {
            return Err(io::Error::new(
                ErrorKind::TimedOut,
                "tls handshake exceeded its deadline",
            ));
        }
    }
    Ok(())
}

/// Whether a socket error should be retried within the handshake loop rather than surfaced. A
/// would-block or timed-out read/write is retried so the loop re-checks the total deadline; an
/// interrupted syscall (EINTR) is always retried.
fn is_retryable(err: &io::Error) -> bool {
    matches!(
        err.kind(),
        ErrorKind::WouldBlock | ErrorKind::TimedOut | ErrorKind::Interrupted
    )
}
