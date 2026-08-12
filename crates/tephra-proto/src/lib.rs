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

pub use framing::{DEFAULT_MAX_FRAME_LEN, FrameError, read_frame, write_frame};

#[cfg(feature = "tokio")]
pub use framing::{read_frame_async, write_frame_async};

/// The generated protobuf message types (`package tephra.v1`).
#[allow(clippy::all, clippy::pedantic, clippy::nursery)]
pub mod tephra {
    include!(concat!(env!("OUT_DIR"), "/generated/generated.rs"));
}
