//! Rebuilding an index from the log, pure over the events it is fed.
//!
//! The log is the source of truth and the index is derived (CLAUDE.md 2), so any index
//! state that is missing, corrupt, or never persisted is reconstructed by replaying the
//! log segment it covers. This module holds the pure core of that reconstruction: a
//! [`Rebuilder`] that a caller drives one `(position, event)` at a time, and a free
//! [`rebuild`] over any iterator of them. Neither touches a file, so both are testable
//! against hand-built inputs, exactly like the log's own recovery rule.
//!
//! The file-scanning wrapper that feeds these from a [`SegmentSet`](crate::log::set::SegmentSet)
//! scan lives in [`super::set`], because a log scan is a lending iterator that cannot be
//! expressed as `Iterator<Item = (Position, EventRef)>`.

use crate::Position;
use crate::event::EventRef;

use super::ActiveTail;

/// The result of a rebuild: the reconstructed index, how many events it covers, and
/// whether the segment could be indexed at all.
///
/// `unindexable` is set only when a single segment holds more than `u16::MAX + 1`
/// distinct event types, which the dense type column cannot address. It is not a real
/// workload (event types are low cardinality by domain), but it must not be silently
/// mis-indexed: an unindexable range makes queries over it error loudly rather than
/// return a short answer (CLAUDE.md 7). `count` is always the true number of events in
/// the range, even when `unindexable`, so the range can still be named and pruned.
#[derive(Debug)]
pub struct Rebuilt {
    pub index: ActiveTail,
    pub count: u64,
    pub unindexable: bool,
}

/// Accumulates an [`ActiveTail`] from events fed in position order.
///
/// The one wrinkle over a bare loop of [`ActiveTail::push`] is the `unindexable` latch:
/// once a push is rejected for too many types, feeding must stop pushing (the tail's
/// length would otherwise disagree with the positions still arriving and trip the
/// feed-order assert), but the caller keeps feeding so `count` stays exact.
#[derive(Debug)]
pub struct Rebuilder {
    index: ActiveTail,
    count: u64,
    unindexable: bool,
}

impl Rebuilder {
    /// A fresh rebuilder whose first event sits at global position `base`.
    pub fn new(base: Position) -> Self {
        Rebuilder {
            index: ActiveTail::new(base),
            count: 0,
            unindexable: false,
        }
    }

    /// Feeds one event at its assigned `position`. Positions must arrive in order (the
    /// tail index asserts it). A too-many-types rejection latches `unindexable` and skips
    /// every subsequent push while still counting.
    pub fn feed(&mut self, position: Position, event: EventRef<'_>) {
        self.count += 1;
        if self.unindexable {
            return;
        }
        if self.index.push(position, event).is_err() {
            self.unindexable = true;
        }
    }

    pub fn finish(self) -> Rebuilt {
        Rebuilt {
            index: self.index,
            count: self.count,
            unindexable: self.unindexable,
        }
    }
}

/// Rebuilds an index by feeding every `(position, event)` from `records` in order.
/// The convenience form of [`Rebuilder`] for callers that already hold a plain iterator
/// (the file-scanning path uses the incremental [`Rebuilder`] against a lending scan).
pub fn rebuild<'a>(
    base: Position,
    records: impl Iterator<Item = (Position, EventRef<'a>)>,
) -> Rebuilt {
    let mut builder = Rebuilder::new(base);
    for (position, event) in records {
        builder.feed(position, event);
    }
    builder.finish()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::{Event, EventType, Tag, Tags};
    use crate::index::search;
    use crate::query::{Query, QueryItem};
    use smallvec::SmallVec;

    fn tags(items: &[&str]) -> Tags {
        Tags::new(
            items
                .iter()
                .map(|s| Tag::new(s).unwrap())
                .collect::<SmallVec<[Tag; 4]>>(),
        )
        .unwrap()
    }

    fn event(ty: &str, tag_strs: &[&str]) -> Event {
        Event::new(&EventType::new(ty).unwrap(), &tags(tag_strs), b"").unwrap()
    }

    #[test]
    fn rebuild_reconstructs_the_same_index() {
        let events = [
            event("Registered", &[]),
            event("Enrolled", &["course:c1"]),
            event("Enrolled", &["course:c1", "student:s1"]),
        ];
        let base = Position::new(1);
        let rebuilt = rebuild(
            base,
            events
                .iter()
                .enumerate()
                .map(|(i, ev)| (Position::new(1 + i as u64), ev.as_ref())),
        );
        assert_eq!(rebuilt.count, 3);
        assert!(!rebuilt.unindexable);

        // The rebuilt index answers a query exactly as a freshly fed one would.
        let q = Query::item(QueryItem::with_tags(tags(&["course:c1"])));
        let got: Vec<Position> = search(&rebuilt.index.view_full(), &q, Position::ZERO).collect();
        assert_eq!(got, vec![Position::new(2), Position::new(3)]);
    }

    #[test]
    fn rebuilder_counts_and_feeds_incrementally() {
        let mut builder = Rebuilder::new(Position::new(10));
        let a = event("E", &["a"]);
        let b = event("E", &["b"]);
        builder.feed(Position::new(10), a.as_ref());
        builder.feed(Position::new(11), b.as_ref());
        let rebuilt = builder.finish();
        assert_eq!(rebuilt.count, 2);
        assert_eq!(rebuilt.index.base(), Position::new(10));
        assert_eq!(rebuilt.index.len(), 2);
    }

    #[test]
    fn empty_range_rebuilds_to_empty() {
        let rebuilt = rebuild(Position::new(1), std::iter::empty());
        assert_eq!(rebuilt.count, 0);
        assert!(!rebuilt.unindexable);
        assert!(rebuilt.index.is_empty());
    }
}
