//! Append-condition evaluation: the one definition of "does a matching event exist
//! after `after`" that the index is differential-tested against.
//!
//! A condition composes a **boundary clause** ([`AppendCondition::fail_if_events_match`] with
//! its `after`) and an optional **existence clause** ([`AppendCondition::fail_if_exists`],
//! whole-log with `after = 0`), OR'd: the append fails if either matches. [`evaluate`] checks
//! the boundary first (cheap and range-pruned) and only then the existence clause, tagging the
//! conflict with its [`ConflictClause`]. Each clause runs through the same two arms
//! ([`evaluate_clause`]), and neither arm may ever produce a false negative (silently accepting
//! a conflicting write):
//!
//! 1. The staged arm settles same-drain-window conflicts against [`StagedTips`], which
//!    has complete knowledge of the batch. Conservative and tag-only; no scan is
//!    possible because staged records are not durable yet.
//! 2. The durable arm fast-rejects with [`TagTips`] and, on [`Verdict::Unknown`], resolves
//!    with an **early-terminating index existence check** ([`IndexSet::find_match`])
//!    rather than a linear log decode. The scan oracle ([`scan_for_match`]) stays as the
//!    fallback when a touched segment is unindexable, as the `verify` cross-check, and as the
//!    escape hatch `force_scan` forces.

use std::sync::Arc;

use crate::Position;
use crate::event::EventRef;
use crate::index::IndexSet;
use crate::log::set::SegmentSet;
use crate::query::{AppendCondition, Matches, Query};

use super::tips::{StagedTips, TagTips, Verdict};
use super::{AppendError, ConflictClause, ConflictSite};

/// The read-only state one clause evaluation reads: the durable tips, the batch-local staged
/// tips, the index, the durable log, and the verify/force-scan flags. Bundled so evaluating a
/// clause is `(ctx, query, after)` rather than an eight-argument call threaded to both sites.
struct EvalCtx<'a> {
    main: &'a TagTips,
    staged: &'a StagedTips,
    index: &'a IndexSet,
    set: &'a SegmentSet,
    verify: bool,
    force_scan: bool,
}

/// Evaluates `cond` and returns the first conflict found (its [`ConflictClause`] and site),
/// or `None` if the append may proceed. `Ok(Some(_))` is a conflict, not an error; `Err` is
/// an integrity failure (log I/O, or an event on the log that will not decode).
///
/// The boundary clause is checked first: it is range-pruned to recent segments, so rejecting
/// there skips the whole-log existence scan. Only if it passes is the optional existence
/// clause ([`AppendCondition::fail_if_exists`]) checked, against the whole log (`after = 0`).
pub fn evaluate(
    cond: &AppendCondition,
    main: &TagTips,
    staged: &StagedTips,
    index: &IndexSet,
    set: &SegmentSet,
    verify: bool,
    force_scan: bool,
) -> Result<Option<(ConflictClause, ConflictSite)>, AppendError> {
    let ctx = EvalCtx {
        main,
        staged,
        index,
        set,
        verify,
        force_scan,
    };
    // Clauses in precedence order: the boundary first (range-pruned, so a reject here skips the
    // whole-log existence scan), then the optional existence clause (whole-log, `after = 0`).
    let clauses = [
        Some((
            ConflictClause::Boundary,
            &cond.fail_if_events_match,
            cond.after,
        )),
        cond.fail_if_exists
            .as_ref()
            .map(|query| (ConflictClause::Existence, query, Position::ZERO)),
    ];
    for (clause, query, after) in clauses.into_iter().flatten() {
        if let Some(at) = evaluate_clause(&ctx, query, after)? {
            return Ok(Some((clause, at)));
        }
    }
    Ok(None)
}

/// Evaluates one clause: a `query` with its own exclusive lower bound `after`. Returns the
/// site of the first conflict, or `None`. The boundary and existence clauses share this body,
/// differing only in `after` (the boundary's is the client's cursor; the existence clause's is
/// [`Position::ZERO`]).
///
/// The durable arm resolves `Verdict::Unknown` through `ctx.index` (the writer's own
/// [`IndexSet`], fed up to the durable tip before this runs, so it reflects exactly the log
/// the scan oracle would). `ctx.verify` turns on the paranoid cross-check against the scan; a
/// disagreement is logged and the store degrades to the authoritative scan answer rather
/// than panicking the writer thread (it panics only in debug builds, so tests fail loudly).
/// `ctx.force_scan` bypasses the index and uses the scan oracle directly (an operational escape
/// hatch and the benchmark's A/B control).
fn evaluate_clause(
    ctx: &EvalCtx<'_>,
    query: &Query,
    after: Position,
) -> Result<Option<ConflictSite>, AppendError> {
    // 1. Same-batch: conservative, tag-only, no scan possible.
    if ctx.staged.may_conflict(query) {
        return Ok(Some(ConflictSite::SameBatch));
    }

    // 2. Durable: tips fast-reject, then the index existence check (scan on fallthrough).
    match ctx.main.may_match(query, after) {
        // Tips prove no match. In verify mode cross-check against the scan; otherwise trust
        // the tips (the floor-monotonic invariant guarantees no false negative).
        Verdict::DefinitelyNoMatch if ctx.verify => {
            Ok(verified_against_scan(None, ctx.set, query, after)?.map(ConflictSite::Durable))
        }
        Verdict::DefinitelyNoMatch => Ok(None),
        // The escape hatch: resolve the fallthrough with the scan oracle directly.
        Verdict::Unknown if ctx.force_scan => {
            Ok(scan_for_match(ctx.set, query, after)?.map(ConflictSite::Durable))
        }
        Verdict::Unknown => match ctx.index.find_match(query, after) {
            Ok(found) if ctx.verify => {
                Ok(verified_against_scan(found, ctx.set, query, after)?.map(ConflictSite::Durable))
            }
            Ok(found) => Ok(found.map(ConflictSite::Durable)),
            // A touched segment is unindexable (in practice the only error, since
            // `find_match` does no I/O). Fall back to the scan oracle over the durable log,
            // which is always authoritative. Warn: a silently degraded
            // segment forcing this repeatedly is worth noticing.
            Err(_err) => {
                #[cfg(feature = "tracing")]
                tracing::warn!(
                    "index existence check unavailable ({_err}); scanning the log for the condition range"
                );
                Ok(scan_for_match(ctx.set, query, after)?.map(ConflictSite::Durable))
            }
        },
    }
}

/// Cross-checks a fast-path answer against the scan oracle and returns the oracle's answer.
///
/// The scan is authoritative, so its result is returned either way. A disagreement means a
/// bug in the tips or the index existence check: it is logged at error with both verdicts
/// and panics in debug builds so tests fail loudly, but in release the writer thread
/// survives on the oracle's answer rather than poisoning every future append (the
/// "a degraded component errors, never answers short" rule applied to the verify path).
fn verified_against_scan(
    fast: Option<Position>,
    set: &SegmentSet,
    query: &Query,
    after: Position,
) -> Result<Option<Position>, AppendError> {
    let scanned = scan_for_match(set, query, after)?;
    if fast != scanned {
        #[cfg(feature = "tracing")]
        tracing::error!(
            "verify: fast-path {fast:?} disagreed with scan oracle {scanned:?} for query {query:?} after {after}"
        );
        debug_assert_eq!(
            fast, scanned,
            "verify: fast-path disagreed with the scan oracle"
        );
    }
    Ok(scanned)
}

/// The scan oracle: the first position strictly after `after` whose event matches `query`,
/// or `None`. Bounded work for the boundary clause (`after` is recent, so it touches only the
/// tail); the existence clause scans from the start, but reaches here only on the tips or
/// index fallthrough.
fn scan_for_match(
    set: &SegmentSet,
    query: &Query,
    after: Position,
) -> Result<Option<Position>, AppendError> {
    let mut scan = set.scan_after(after);
    while let Some(item) = scan.next() {
        let record = item.map_err(|err| AppendError::Log(Arc::new(err)))?;
        let event = EventRef::from_bytes(record.data).map_err(AppendError::Corrupt)?;
        if query.matches(event) {
            return Ok(Some(record.position));
        }
    }
    Ok(None)
}
