//! Layer 2: the write coordinator.
//!
//! One logical writer owns the [`SegmentSet`](crate::log::set::SegmentSet), assigns
//! positions, evaluates append conditions, and group-commits batches under a single
//! fsync. Single writer, not single threaded: the serialized work (position assignment,
//! the tips lookup, the condition verdict) is all in-memory hash work on one thread,
//! while callers block on a bounded channel that provides backpressure for free.
//!
//! The correctness core is the append condition under group commit: several independent
//! decisions can be drained into one batch, and two of them can conflict with each other
//! (the uniqueness-guard pattern). That case is handled by a batch-local
//! [`StagedTips`](tips::StagedTips) consulted alongside the durable
//! [`TagTips`](tips::TagTips); see [`condition`] and [`coordinator`].

mod batch;
mod condition;
mod coordinator;
mod handle;
mod tips;

use std::sync::Arc;

use flume::Sender;
use thiserror::Error;

use crate::Position;
use crate::event::{DecodeError, Event};
use crate::log::set::{LogError, PositionRange};
use crate::query::AppendCondition;
use crate::read::ReadConfig;

pub use coordinator::WriteCoordinator;
pub use handle::WriteHandle;

/// Configuration for the write coordinator.
#[derive(Clone, Copy, Debug)]
pub struct WriterConfig {
    /// Bounded request-queue capacity. When full, `append` blocks (backpressure).
    pub queue_capacity: usize,
    /// Most requests to fold into one group-committed batch.
    pub max_batch_records: usize,
    /// Byte budget for one batch. Must be `<= SegmentSet::segment_capacity()`, so a
    /// multi-request batch always fits a segment; a single request larger than this gets
    /// its own solo batch.
    pub max_batch_bytes: usize,
    /// Recent-position window width for the durable tips (memory bound only).
    pub tips_window: u64,
    /// Paranoid cross-check: scan even on `DefinitelyNoMatch` and assert agreement. A
    /// runtime flag (not a cargo feature) so the property test can flip it per case.
    pub verify_tips: bool,
    /// Force the append-condition durable arm to resolve `Verdict::Unknown` with the scan
    /// oracle instead of the index existence check ([`condition`], phase 6d). An operational
    /// escape hatch (the log is the source of truth, so the scan is always safe) and the A/B
    /// control the `condition_path` benchmark uses to measure index-vs-scan. `false` in
    /// production.
    pub condition_force_scan: bool,
    /// Read-path tuning for the handles this coordinator hands out (the index-vs-scan cost
    /// model, CLAUDE.md 8). Reads run off the writer thread, so this only configures the
    /// planner, never the append path.
    pub read: ReadConfig,
}

impl Default for WriterConfig {
    fn default() -> Self {
        WriterConfig {
            queue_capacity: 1024,
            max_batch_records: 1024,
            max_batch_bytes: 8 * 1024 * 1024,
            tips_window: 1_000_000,
            verify_tips: false,
            condition_force_scan: false,
            read: ReadConfig::default(),
        }
    }
}

/// Where a conflict was found.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConflictSite {
    /// A real, durable conflict at this position. Terminal: retrying hits it again until
    /// the client rebuilds its decision model against the new tail.
    Durable(Position),
    /// A conservative same-drain-window rejection. **Advisory and retryable**: the
    /// tag-only staged check cannot see event type, so there may be no true conflict. A
    /// retry re-reads the now-durable record and gets the precise verdict. A layer above
    /// the coordinator must treat this as retryable and must not collapse it into a bare
    /// position.
    SameBatch,
}

/// Why an append did not succeed.
#[derive(Clone, Debug, Error)]
pub enum AppendError {
    /// The condition matched at least one event after `after`.
    #[error("append condition conflict ({at:?})")]
    Conflict { at: ConflictSite },
    /// `after` referenced a position beyond the durable tip: a misbehaving client or a
    /// bug, since a client cannot have observed a position that is not yet durable.
    #[error("after {after} is beyond the durable tip {tip}")]
    AfterBeyondTip { after: Position, tip: Position },
    /// An append carrying no events.
    #[error("append with no events")]
    Empty,
    /// A single request's events cannot fit even an empty segment.
    #[error("event batch of {size} bytes exceeds the segment capacity")]
    TooLarge { size: usize },
    /// The log write failed; the whole drained batch was rejected. `Arc` so the one
    /// error can be reported to every request in the failed batch (`LogError` is not
    /// `Clone`).
    #[error("log write failed: {0}")]
    Log(Arc<LogError>),
    /// An event already on the log failed to decode during a condition scan: integrity
    /// failure, not a normal outcome.
    #[error("event on the log failed to decode: {0}")]
    Corrupt(DecodeError),
    /// The write coordinator has shut down and will not service the request.
    #[error("write coordinator has shut down")]
    Shutdown,
}

/// The reply channel for one request (a per-call oneshot).
type Reply = Sender<Result<PositionRange, AppendError>>;

/// One append request handed to the coordinator. Events are pre-encoded on the caller
/// thread, so the coordinator does no encoding.
struct Request {
    events: Vec<Event>,
    condition: Option<AppendCondition>,
    reply: Reply,
}

/// What flows over the coordinator's channel: an append or the explicit shutdown sentinel
/// (drop-based shutdown works too, via channel disconnect).
///
/// Reads no longer ride this channel: as of phase 6 they run on the caller's own thread
/// over a published read snapshot ([`crate::read`]), off the writer thread entirely.
enum Message {
    Append(Request),
    Shutdown,
}
