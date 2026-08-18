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
