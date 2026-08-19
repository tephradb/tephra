//! Wire protocol for the tephra TCP server and client: the protobuf message types, the
//! length-prefixed framing, and the conversions between the wire types and the shared
//! `tephra-types` vocabulary (see [`convert`]). This crate depends on `tephra-types` but not on
//! the `tephra` storage engine, so a client links the wire types and the vocabulary without
//! pulling in the engine.
//!
//! The messages are defined in `proto/tephra.proto` and generated at build time by the
//! official `protobuf-codegen` (see `build.rs`). Framing is a 4-byte big-endian length
//! prefix in front of each serialized message; see the `framing` module.

pub mod convert;
mod framing;
#[cfg(feature = "tls")]
pub mod tls;

pub use framing::{
    DEFAULT_MAX_FRAME_LEN, FrameError, FramePoll, FrameReader, read_frame, write_frame,
};

#[cfg(feature = "tokio")]
pub use framing::{read_frame_async, write_frame_async};

#[cfg(feature = "tls")]
pub use tls::{TlsConn, TlsReadHalf, TlsWriteHalf};

/// The protocol version a client announces in its [`Hello`](tephra::Hello) and the server
/// answers in its [`HelloAck`](tephra::HelloAck). The single compatibility mechanism: a server
/// rejects a version it does not support rather than inferring compatibility from field presence.
/// Bumped on any breaking change to the wire protocol.
pub const PROTOCOL_VERSION: u32 = 1;

/// The generated protobuf message types (`package tephra.v1`).
#[allow(clippy::all, clippy::pedantic, clippy::nursery)]
pub mod tephra {
    include!(concat!(env!("OUT_DIR"), "/generated/generated.rs"));
}

/// Builds the opening [`Hello`](tephra::Hello) request frame: the current [`PROTOCOL_VERSION`] and,
/// when authenticating, the bearer token. Every client and the healthcheck probe open a connection
/// with this, so the protocol version is set in exactly one place.
pub fn hello_request(request_id: u64, auth_token: Option<&str>) -> tephra::Request {
    let mut hello = tephra::Hello::new();
    hello.set_protocol_version(PROTOCOL_VERSION);
    if let Some(token) = auth_token {
        hello.set_auth_token(token);
    }
    let mut request = tephra::Request::new();
    request.set_request_id(request_id);
    request.set_hello(hello);
    request
}
