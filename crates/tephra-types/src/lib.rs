//! Shared vocabulary for tephra: the concepts a client and the storage engine both speak.
//!
//! This crate is pure data and validation with no I/O and no storage machinery, so a
//! client can link it (and the wire protocol) without pulling in the engine. It holds the
//! [`Position`] type, the event [`EventType`]/[`Tag`] names and the sorted [`Tags`] set,
//! and the [`Query`] model ([`Query`], [`QueryItem`], [`AppendCondition`]).
//!
//! The engine re-exports these types, so `tephra::Query` and [`tephra_types::Query`](Query)
//! are the same type. Name and tag validation ([`EventType::new`], [`Tag::new`],
//! [`Tags::new`]) lives here as the single source of truth for both sides.

pub mod name;
pub mod position;
pub mod query;

pub use name::{EventType, MAX_NAME_LEN, NameError, Tag, Tags, TagsError};
pub use position::Position;
pub use query::{AppendCondition, Query, QueryItem};
