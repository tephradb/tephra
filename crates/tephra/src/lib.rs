//! A DCB-compliant, immutable event store with global ordering.
//!
//! Tephra is a Dynamic Consistency Boundary (DCB) event store. Instead of a static consistency
//! boundary baked into an aggregate, the boundary is derived per decision from a [`Query`].
//! Events carry an [`EventType`] plus a set of [`Tags`], so one event can belong to several
//! entities at once, and a decision reads exactly the events it depends on and guards exactly
//! those on append (an [`AppendCondition`]).
//!
//! This crate is the embedded engine: the durable log, the single writer, the index, and the
//! read paths. Use it directly in-process, or reach it over the network with the
//! [`tephra-server`](https://crates.io/crates/tephra-server) TCP server and the
//! [`tephra-client`](https://crates.io/crates/tephra-client) client.
//!
//! # Design
//!
//! The log is the source of truth and everything else is derived. Data is written once, never
//! updated and never deleted, keyed by a dense monotonic [`Position`] assigned by the single
//! writer. Indexes need no write-ahead log and no fsync on the write path, because they can be
//! rebuilt by replaying the log. The `ARCHITECTURE.md` document in the repository records the
//! full rationale and the alternatives that were rejected.
//!
//! # Example
//!
//! ```no_run
//! use tephra::{
//!     AppendCondition, Event, EventType, Position, Query, QueryItem, SegmentConfig,
//!     SegmentSet, Tag, Tags, WriteCoordinator, WriterConfig,
//! };
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! // Open (or create) a log directory and start the single-writer coordinator.
//! let set = SegmentSet::open("tephra-data", SegmentConfig::new(256 * 1024 * 1024))?;
//! let (coordinator, handle) = WriteCoordinator::start(set, WriterConfig::default())?;
//!
//! // Build a packed event, then append it guarded so it fails if course:c1 already exists.
//! let ty = EventType::new("CourseOpened")?;
//! let tags = Tags::new([Tag::new("course:c1")?])?;
//! let event = Event::new(&ty, &tags, br#"{"course":"c1","seats":30}"#)?;
//! let guard = AppendCondition::new(Query::item(QueryItem::with_tags(
//!     Tags::new([Tag::new("course:c1")?])?,
//! )));
//! handle.append(vec![event], Some(guard))?;
//!
//! // Reads run on the caller's thread over a snapshot published at each commit. `read` returns
//! // a lending iterator, so it is consumed with `while let`, not a `for` loop.
//! let query = Query::item(QueryItem::with_tags(Tags::new([Tag::new("course:c1")?])?));
//! let mut reads = handle.read(&query, Position::ZERO, None);
//! while let Some(item) = reads.next() {
//!     let seq = item?;
//!     println!("{} {}", seq.position, seq.event.event_type());
//! }
//!
//! // Shutdown joins the writer thread and flushes cleanly.
//! coordinator.shutdown();
//! # Ok(())
//! # }
//! ```

pub mod event;
pub mod index;
pub mod log;
pub mod query;
pub mod read;
pub mod writer;

pub use event::{Event, EventRef};
pub use log::set::{PositionRange, SegmentConfig, SegmentSet};
pub use query::Matches;
pub use read::{
    DEFAULT_MAX_BATCH_EVENTS, ReadConfig, ReadError, ReadHandle, Subscription, WaitOutcome,
};
#[cfg(feature = "async")]
pub use read::pool::{ReadPool, ReadPoolConfig, ReadStream};
pub use tephra_types::{
    AppendCondition, EventType, MAX_NAME_LEN, NameError, Position, Query, QueryItem, Tag, Tags,
    TagsError,
};
pub use writer::{AppendError, ConflictSite, WriteCoordinator, WriteHandle, WriterConfig};
