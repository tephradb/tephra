//! Append-condition evaluation: the one definition of "does a matching event exist
//! after `after`" that the index (phase 5) will be differential-tested against.
//!
//! Two arms, and neither may ever produce a false negative (silently accepting a
//! conflicting write):
//!
//! 1. The staged arm settles same-drain-window conflicts against [`StagedTips`], which
//!    has complete knowledge of the batch. Conservative and tag-only; no scan is
//!    possible because staged records are not durable yet.
//! 2. The durable arm fast-rejects with [`TagTips`] and, on [`Verdict::Unknown`], falls
//!    through to the scan oracle over the durable log.

use std::sync::Arc;

use crate::Position;
use crate::event::EventRef;
use crate::log::set::SegmentSet;
use crate::query::AppendCondition;

use super::tips::{StagedTips, TagTips, Verdict};
use super::{AppendError, ConflictSite};

/// Evaluates `cond` and returns the site of the first conflict found, or `None` if the
/// append may proceed. `Ok(Some(_))` is a conflict, not an error; `Err` is an integrity
/// failure (log I/O, or an event on the log that will not decode).
///
/// `verify` turns on the paranoid cross-check: when the tips say `DefinitelyNoMatch`, the
/// scan runs anyway and must agree, or the process panics. This is the whole value of
/// keeping both the tips and the oracle.
pub fn evaluate(
    cond: &AppendCondition,
    main: &TagTips,
    staged: &StagedTips,
    set: &SegmentSet,
    verify: bool,
) -> Result<Option<ConflictSite>, AppendError> {
    let query = &cond.fail_if_events_match;

    // 1. Same-batch: conservative, tag-only, no scan possible.
    if staged.may_conflict(query) {
        return Ok(Some(ConflictSite::SameBatch));
    }

    // 2. Durable: fast-reject, then the scan oracle.
    match main.may_match(query, cond.after) {
        Verdict::DefinitelyNoMatch => {
            if verify {
                let found = scan_for_match(set, cond)?;
                assert!(
                    found.is_none(),
                    "verify_tips: tips said DefinitelyNoMatch but the scan found a \
                     match at {found:?} for after {}",
                    cond.after,
                );
            }
            Ok(None)
        }
        Verdict::Unknown => Ok(scan_for_match(set, cond)?.map(ConflictSite::Durable)),
    }
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
