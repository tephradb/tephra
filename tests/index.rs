//! Differential test: the in-memory index must answer every query identically to the
//! phase-4 scan baseline.
//!
//! The scan baseline (`scan_after` + `Query::matches`) is the permanent oracle the whole
//! index is defined against. Here a random workload is appended to a real `SegmentSet`
//! and fed, in the same order, to a `TailIndex`; for a spread of random queries and
//! `after` bounds, `index::search` must return the exact same ascending positions the
//! scan does. A disagreement is a bug in the index, since the oracle is authoritative.

use dcbdb::Position;
use dcbdb::event::{Event, EventRef, EventType, Tag, Tags};
use dcbdb::index::{TailIndex, search};
use dcbdb::log::set::{SegmentConfig, SegmentSet};
use dcbdb::query::{Query, QueryItem};
use smallvec::SmallVec;
use tempfile::TempDir;

/// The same small LCG used in the coordinator tests: deterministic, dependency-free.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.0 >> 17
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

const TYPES: [&str; 3] = ["Registered", "Enrolled", "Renamed"];
const TAGS: [&str; 6] = ["course:c1", "course:c2", "student:s1", "student:s2", "team:t1", "team:t2"];

fn event_type(s: &str) -> EventType {
    EventType::new(s).unwrap()
}

/// `k` distinct tags starting at `start`, wrapping the universe. Consecutive indices mod
/// a universe of 6 are distinct for `k <= 6`, so this never trips `Tags`' duplicate check.
fn pick_tags(start: usize, k: usize) -> Tags {
    let picked: SmallVec<[Tag; 4]> = (0..k)
        .map(|i| Tag::new(TAGS[(start + i) % TAGS.len()]).unwrap())
        .collect();
    Tags::new(picked).unwrap()
}

/// A random event: a random type and 0..=3 distinct tags.
fn random_event(rng: &mut Rng) -> Event {
    let ty = event_type(TYPES[rng.below(TYPES.len() as u64) as usize]);
    let k = rng.below(4) as usize; // 0..=3 tags
    let start = rng.below(TAGS.len() as u64) as usize;
    Event::new(&ty, &pick_tags(start, k), b"payload").unwrap()
}

/// A random query: `All`, empty `Items`, or 1..=3 items each with 0..=2 types and 0..=3
/// tags. Covers type-only, tag-only, mixed, empty, and multi-item OR shapes.
fn random_query(rng: &mut Rng) -> Query {
    match rng.below(6) {
        0 => Query::all(),
        1 => Query::items(Vec::new()),
        _ => {
            let n_items = 1 + rng.below(3) as usize;
            let items = (0..n_items)
                .map(|_| {
                    let n_types = rng.below(3) as usize; // 0..=2
                    let type_start = rng.below(TYPES.len() as u64) as usize;
                    let types = (0..n_types)
                        .map(|i| event_type(TYPES[(type_start + i) % TYPES.len()]))
                        .collect();
                    let n_tags = rng.below(4) as usize; // 0..=3
                    let tag_start = rng.below(TAGS.len() as u64) as usize;
                    QueryItem::new(types, pick_tags(tag_start, n_tags))
                })
                .collect::<Vec<_>>();
            Query::items(items)
        }
    }
}

/// The oracle: positions strictly after `after` whose event matches `query`, by scanning.
fn scan_baseline(set: &SegmentSet, query: &Query, after: Position) -> Vec<Position> {
    let mut out = Vec::new();
    let mut scan = set.scan_after(after);
    while let Some(item) = scan.next() {
        let record = item.unwrap();
        let event = EventRef::from_bytes(record.data).unwrap();
        if query.matches(event) {
            out.push(record.position);
        }
    }
    out
}

#[test]
fn index_search_agrees_with_scan_over_random_workload() {
    let dir = TempDir::new().unwrap();
    // One roomy segment: the differential contract for the in-memory half is a single
    // logical index; multi-segment composition and pruning are phase 5b.
    let mut set = SegmentSet::open(dir.path(), SegmentConfig::new(1 << 20)).unwrap();
    let mut index = TailIndex::new(set.next_position());

    let mut rng = Rng(0x1234_5678_9ABC_DEF0);

    for _ in 0..400 {
        // Append one event as its own batch, then feed it to the index at the position
        // the log assigned, keeping the two in lockstep.
        let event = random_event(&mut rng);
        let range = set.append_batch(&[event.as_bytes()]).unwrap();
        index.push(range.first, event.as_ref()).unwrap();

        // A spread of random queries against the current log, each with a random valid
        // `after` (0..=last), asserting the index and the scan agree exactly.
        let last = set.last_position().get();
        for _ in 0..4 {
            let query = random_query(&mut rng);
            let after = Position::new(rng.below(last + 1));
            let from_index: Vec<Position> = search(&index, &query, after).collect();
            let from_scan = scan_baseline(&set, &query, after);
            assert_eq!(
                from_index, from_scan,
                "index disagreed with scan for query {query:?} after {after}"
            );
        }
    }
}
