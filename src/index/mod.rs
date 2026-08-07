//! Layer 3: the derived index.
//!
//! The log is the source of truth; this is derived from it and rebuildable by replay.
//! Phase 5a is the in-memory, pure core:
//!
//! - [`TermInterner`] / [`TypeInterner`]: tag and event-type strings to dense ids.
//! - [`TailIndex`]: a per-segment index, per-tag postings plus a dense type column, fed
//!   in position order.
//! - [`search`]: the index-driven query evaluator, the counterpart to phase 4's scan
//!   oracle (`writer::condition`). It answers a [`Query`](crate::query::Query)
//!   identically to a scan, which the differential test pins down.
//!
//! 5a builds nothing on disk and changes no write path. The on-disk index segment format,
//! inline feeding at the commit seam, seal-on-rollover, tail recovery, and segment
//! pruning are phase 5b.

mod interner;
mod search;
mod tail;

pub use interner::{TermInterner, TooManyTypes, TypeInterner};
pub use search::search;
pub use tail::TailIndex;

/// A segment-local identifier for a tag, dense from 0.
///
/// Interned per [`TailIndex`] (Lucene-style), so ids never need to be stable across
/// segments and there is no global registry to persist.
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct TermId(u32);

/// A segment-local identifier for an event type, dense from 0.
///
/// A `u16` because event types are low cardinality (10s to 100s), so the dense type
/// column addresses them directly at two bytes each.
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct TypeId(u16);
