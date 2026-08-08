//! Append-condition evaluation: the one definition of "does a matching event exist
//! after `after`" that the index (phase 5) is differential-tested against.
//!
//! Two arms, and neither may ever produce a false negative (silently accepting a
//! conflicting write):
//!
//! 1. The staged arm settles same-drain-window conflicts against [`StagedTips`], which
//!    has complete knowledge of the batch. Conservative and tag-only; no scan is
//!    possible because staged records are not durable yet.
//! 2. The durable arm fast-rejects with [`TagTips`] and, on [`Verdict::Unknown`], resolves
//!    with an **early-terminating index existence check** ([`IndexSet::find_match`], phase
//!    6d) rather than a linear log decode. The scan oracle ([`scan_for_match`]) stays as the
//!    fallback when a touched segment is unindexable, as the `verify` cross-check, and as the
//!    escape hatch `force_scan` forces.

use std::sync::Arc;

use crate::Position;
use crate::event::EventRef;
use crate::index::IndexSet;
use crate::log::set::SegmentSet;
use crate::query::AppendCondition;

use super::tips::{StagedTips, TagTips, Verdict};
use super::{AppendError, ConflictSite};

/// Evaluates `cond` and returns the site of the first conflict found, or `None` if the
/// append may proceed. `Ok(Some(_))` is a conflict, not an error; `Err` is an integrity
/// failure (log I/O, or an event on the log that will not decode).
///
/// The durable arm resolves `Verdict::Unknown` through `index` (the writer's own
/// [`IndexSet`], fed up to the durable tip before this runs, so it reflects exactly the log
/// the scan oracle would). `verify` turns on the paranoid cross-check against the scan; a
/// disagreement is logged and the store degrades to the authoritative scan answer rather
/// than panicking the writer thread (it panics only in debug builds, so tests fail loudly).
/// `force_scan` bypasses the index and uses the scan oracle directly (an operational escape
/// hatch and the benchmark's A/B control).
pub fn evaluate(
    cond: &AppendCondition,
    main: &TagTips,
    staged: &StagedTips,
    index: &IndexSet,
    set: &SegmentSet,
    verify: bool,
    force_scan: bool,
) -> Result<Option<ConflictSite>, AppendError> {
    let query = &cond.fail_if_events_match;

    // 1. Same-batch: conservative, tag-only, no scan possible.
    if staged.may_conflict(query) {
        return Ok(Some(ConflictSite::SameBatch));
    }

    // 2. Durable: tips fast-reject, then the index existence check (scan on fallthrough).
    match main.may_match(query, cond.after) {
        // Tips prove no match. In verify mode cross-check against the scan; otherwise trust
        // the tips (the floor-monotonic invariant guarantees no false negative).
        Verdict::DefinitelyNoMatch if verify => {
            Ok(verified_against_scan(None, set, cond)?.map(ConflictSite::Durable))
        }
        Verdict::DefinitelyNoMatch => Ok(None),
        // The escape hatch: resolve the fallthrough with the scan oracle directly.
        Verdict::Unknown if force_scan => Ok(scan_for_match(set, cond)?.map(ConflictSite::Durable)),
        Verdict::Unknown => match index.find_match(query, cond.after) {
            Ok(found) if verify => {
                Ok(verified_against_scan(found, set, cond)?.map(ConflictSite::Durable))
            }
            Ok(found) => Ok(found.map(ConflictSite::Durable)),
            // A touched segment is unindexable (in practice the only error, since
            // `find_match` does no I/O). Fall back to the scan oracle over the durable log,
            // which is always authoritative (CLAUDE.md 2, 7). Warn: a silently degraded
            // segment forcing this repeatedly is worth noticing.
            Err(_err) => {
                #[cfg(feature = "tracing")]
                tracing::warn!(
                    "index existence check unavailable ({_err}); scanning the log for the condition range"
                );
                Ok(scan_for_match(set, cond)?.map(ConflictSite::Durable))
            }
        },
    }
}

/// Cross-checks a fast-path answer against the scan oracle and returns the oracle's answer.
///
/// The scan is authoritative, so its result is returned either way. A disagreement means a
/// bug in the tips or the index existence check: it is logged at error with both verdicts
/// and panics in debug builds so tests fail loudly, but in release the writer thread
/// survives on the oracle's answer rather than poisoning every future append (CLAUDE.md 7's
/// "a degraded component errors, never answers short" applied to the verify path).
fn verified_against_scan(
    fast: Option<Position>,
    set: &SegmentSet,
    cond: &AppendCondition,
) -> Result<Option<Position>, AppendError> {
    let scanned = scan_for_match(set, cond)?;
    if fast != scanned {
        #[cfg(feature = "tracing")]
        tracing::error!(
            "verify: fast-path {fast:?} disagreed with scan oracle {scanned:?} for query {:?} after {}",
            cond.fail_if_events_match,
            cond.after,
        );
        debug_assert_eq!(
            fast, scanned,
            "verify: fast-path disagreed with the scan oracle"
        );
    }
    Ok(scanned)
}

/// The scan oracle: the first position strictly after `cond.after` whose event matches
/// the query, or `None`. Bounded work: `after` is recent, so this touches only the tail.
fn scan_for_match(
    set: &SegmentSet,
    cond: &AppendCondition,
) -> Result<Option<Position>, AppendError> {
    let query = &cond.fail_if_events_match;
    let mut scan = set.scan_after(cond.after);
    while let Some(item) = scan.next() {
        let record = item.map_err(|err| AppendError::Log(Arc::new(err)))?;
        let event = EventRef::from_bytes(record.data).map_err(AppendError::Corrupt)?;
        if query.matches(event) {
            return Ok(Some(record.position));
        }
    }
    Ok(None)
}
