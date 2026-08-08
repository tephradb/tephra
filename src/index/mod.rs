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
//! Phase 5b adds the on-disk half: an `IndexSegment` format (CRC-locked header, FST
//! term dictionary, tiered postings, dense type column), an `IndexSet` that owns the
//! sealed segments plus the active tail and answers a [`Query`](crate::query::Query)
//! across all of them, inline feeding at the commit seam, seal-on-rollover, and
//! rebuild-from-log recovery. [`search`] is made generic over [`SegmentIndex`] so the
//! one spec-pinned evaluator serves both the in-memory [`TailIndex`] and the on-disk
//! `IndexSegment` unchanged.

use std::borrow::Cow;

use crate::Position;

mod header;
mod interner;
mod postings;
mod search;
mod segment;
mod set;
mod tail;

pub mod recovery;

pub use header::{INDEX_HEADER_SIZE, IndexHeaderError, IndexSegmentHeader};
pub use interner::{TermInterner, TooManyTypes, TypeInterner};
pub use search::search;
pub use segment::{IndexSegment, IndexSegmentError};
pub use set::{IndexError, IndexSet};
pub use tail::TailIndex;

/// A segment-local identifier for a tag, dense from 0.
///
/// Interned per [`TailIndex`] (Lucene-style), so ids never need to be stable across
/// segments and there is no global registry to persist.
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct TermId(u32);

impl TermId {
    /// The raw dense id. Crate-internal so the sealer and readers can address postings
    /// without the parent module's field being public.
    pub(crate) fn get(self) -> u32 {
        self.0
    }
}

/// A segment-local identifier for an event type, dense from 0.
///
/// A `u16` because event types are low cardinality (10s to 100s), so the dense type
/// column addresses them directly at two bytes each.
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct TypeId(u16);

impl TypeId {
    /// The raw dense id, the value stored in the on-disk type column.
    pub(crate) fn get(self) -> u16 {
        self.0
    }
}

/// One segment's worth of index that [`search`] can evaluate a query against, whether
/// it lives in memory ([`TailIndex`]) or on disk ([`IndexSegment`]).
///
/// The two share exactly one evaluator (CLAUDE.md 6, 7.0). The only shape difference is
/// [`term_postings`](SegmentIndex::term_postings): the in-memory tail borrows its posting
/// slice ([`Cow::Borrowed`], zero-copy), while the on-disk segment decodes varint deltas
/// into an owned vec ([`Cow::Owned`]). Positions are segment-local (`global - base`).
pub trait SegmentIndex {
    /// First global position this segment covers; `global = base + local`.
    fn base(&self) -> Position;

    /// Number of events indexed; local positions run `0..len`.
    fn len(&self) -> u32;

    /// Whether the segment indexes no events.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Ascending local positions of every event carrying `tag`, or `None` if no indexed
    /// event carries it.
    fn term_postings(&self, tag: &str) -> Option<Cow<'_, [u32]>>;

    /// The dense type id for `name`, or `None` if no indexed event has that type.
    fn type_id(&self, name: &str) -> Option<u16>;

    /// The dense type id of the event at local position `local`.
    fn type_at(&self, local: u32) -> u16;
}
