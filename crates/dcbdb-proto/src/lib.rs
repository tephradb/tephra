//! Wire protocol for the dcbdb TCP server and client: the protobuf message types plus
//! length-prefixed framing. This crate has no dependency on `dcbdb` itself, so a client
//! links the wire types without pulling in the storage engine.
//!
//! The messages are defined in `proto/dcbdb.proto` and generated at build time by the
//! official `protobuf-codegen` (see `build.rs`). Framing is a 4-byte big-endian length
//! prefix in front of each serialized message; see the `framing` module.

mod framing;

pub use framing::{DEFAULT_MAX_FRAME_LEN, FrameError, read_frame, write_frame};

/// The generated protobuf message types (`package dcbdb.v1`).
#[allow(clippy::all, clippy::pedantic, clippy::nursery)]
pub mod dcbdb {
    include!(concat!(env!("OUT_DIR"), "/generated/generated.rs"));
}
