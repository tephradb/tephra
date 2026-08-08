//! End-to-end tests of the off-thread read path (`ReadHandle::read` via `WriteHandle`).
//!
//! The scan baseline (`scan_after` + `Query::matches`) is the same permanent oracle the
//! index is defined against. Here a random workload is appended through the live
//! coordinator, then `read()` (running on this thread over the published snapshot) must
//! return exactly the positions, and the exact events, that a scan of the recovered log
//! does, for a spread of random queries and `after` bounds across many sealed segments.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;

use dcbdb::Position;
use dcbdb::event::{Event, EventRef, EventType, Tag, Tags};
use dcbdb::log::set::{SegmentConfig, SegmentSet};
use dcbdb::query::{Query, QueryItem};
use dcbdb::read::ReadError;
use dcbdb::writer::{WriteCoordinator, WriteHandle, WriterConfig};
use smallvec::SmallVec;
use tempfile::TempDir;

// ------------------------- deterministic random workload -------------------------

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
const TAGS: [&str; 6] = [
    "course:c1",
    "course:c2",
    "student:s1",
    "student:s2",
    "team:t1",
    "team:t2",
];

fn event_type(s: &str) -> EventType {
    EventType::new(s).unwrap()
}

fn pick_tags(start: usize, k: usize) -> Tags {
    let picked: SmallVec<[Tag; 4]> = (0..k)
        .map(|i| Tag::new(TAGS[(start + i) % TAGS.len()]).unwrap())
        .collect();
    Tags::new(picked).unwrap()
}

fn random_event(rng: &mut Rng) -> Event {
    let ty = event_type(TYPES[rng.below(TYPES.len() as u64) as usize]);
    let k = rng.below(4) as usize;
    let start = rng.below(TAGS.len() as u64) as usize;
    Event::new(&ty, &pick_tags(start, k), b"payload").unwrap()
}

fn random_query(rng: &mut Rng) -> Query {
    match rng.below(6) {
        0 => Query::all(),
        1 => Query::items(Vec::new()),
        _ => {
            let n_items = 1 + rng.below(3) as usize;
            let items = (0..n_items)
                .map(|_| {
                    let n_types = rng.below(3) as usize;
                    let type_start = rng.below(TYPES.len() as u64) as usize;
                    let types = (0..n_types)
                        .map(|i| event_type(TYPES[(type_start + i) % TYPES.len()]))
                        .collect();
                    let n_tags = rng.below(4) as usize;
                    let tag_start = rng.below(TAGS.len() as u64) as usize;
                    QueryItem::new(types, pick_tags(tag_start, n_tags))
                })
                .collect::<Vec<_>>();
            Query::items(items)
        }
    }
}

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

// ------------------------------- helpers -------------------------------

/// Small segments so a 400-event workload rolls over many times: the read path then spans
/// a dozen-plus sealed on-disk index segments plus the bounded active-range scan.
fn coordinator() -> (WriteCoordinator, WriteHandle, TempDir) {
    let dir = TempDir::new().unwrap();
    let set = SegmentSet::open(dir.path(), SegmentConfig::new(512)).unwrap();
    let cfg = WriterConfig {
        queue_capacity: 64,
        max_batch_records: 64,
        max_batch_bytes: 256,
        tips_window: 1_000_000,
        verify_tips: false,
    };
    let (coord, handle) = WriteCoordinator::start(set, cfg).unwrap();
    (coord, handle, dir)
}

fn read_owned(
    handle: &WriteHandle,
    query: Query,
    after: Position,
) -> Result<Vec<(Position, Event)>, ReadError> {
    handle.read(query, after).collect_owned()
}

fn tags(items: &[&str]) -> Tags {
    Tags::new(
        items
            .iter()
            .map(|s| Tag::new(s).unwrap())
            .collect::<SmallVec<[Tag; 4]>>(),
    )
    .unwrap()
}

fn tagged_event(ty: &str, tag_strs: &[&str]) -> Event {
    Event::new(&EventType::new(ty).unwrap(), &tags(tag_strs), b"payload").unwrap()
}

// ------------------------------- tests -------------------------------

#[test]
fn read_matches_scan_over_random_workload_across_segments() {
    let (coord, handle, _dir) = coordinator();

    let mut rng = Rng(0x1234_5678_9ABC_DEF0);
    for _ in 0..400 {
        let event = random_event(&mut rng);
        handle.append(vec![event], None).unwrap();
    }

    // Capture a spread of reads while the coordinator is live (read runs on this thread).
    // One case: the query, its `after` bound, and the `(position, event)` pairs read back.
    type ReadCase = (Query, Position, Vec<(Position, Event)>);
    let mut qrng = Rng(0xC0FF_EE00_1234_5678);
    let last = 400u64;
    let mut cases: Vec<ReadCase> = Vec::new();
    for _ in 0..2000 {
        let query = random_query(&mut qrng);
        let after = Position::new(qrng.below(last + 1));
        let got = read_owned(&handle, query.clone(), after).unwrap();
        cases.push((query, after, got));
    }

    // Shut down to recover the log, then check every captured read against the scan oracle:
    // identical positions, and each returned event equal to the one at that position.
    let set = coord.shutdown();
    for (query, after, got) in &cases {
        let positions: Vec<Position> = got.iter().map(|(p, _)| *p).collect();
        let baseline = scan_baseline(&set, query, *after);
        assert_eq!(
            positions, baseline,
            "read positions disagreed with scan for query {query:?} after {after}"
        );
        for (position, event) in got {
            let record = set.read_at(*position).unwrap();
            let expected = EventRef::from_bytes(&record.data).unwrap();
            assert_eq!(event.as_ref().event_type(), expected.event_type());
            assert_eq!(event.as_ref().data(), expected.data());
            let got_tags: Vec<&str> = event.as_ref().tags().collect();
            let want_tags: Vec<&str> = expected.tags().collect();
            assert_eq!(got_tags, want_tags);
        }
    }
}

#[test]
fn concurrent_reads_see_a_consistent_prefix_under_heavy_appends() {
    // Every event matches `Query::all`, so a read pinned at watermark W must return exactly
    // positions 1..=W: a dense, gap-free prefix. A torn snapshot (a segment set not covering
    // its watermark) or an event past the watermark would break that. Readers hammer while
    // the writer appends.
    let (coord, handle, _dir) = coordinator();
    let stop = Arc::new(AtomicBool::new(false));

    let readers: Vec<_> = (0..4)
        .map(|_| {
            let reader = handle.reader();
            let stop = Arc::clone(&stop);
            thread::spawn(move || {
                let mut observed = 0u64;
                while !stop.load(Ordering::Relaxed) {
                    let mut reads = reader.read(Query::all(), Position::ZERO);
                    let watermark = reads.watermark().get();
                    let mut positions = Vec::new();
                    while let Some(item) = reads.next() {
                        positions.push(item.expect("read failed").position.get());
                    }
                    let expected: Vec<u64> = (1..=watermark).collect();
                    assert_eq!(
                        positions, expected,
                        "read did not return the dense prefix 1..={watermark}"
                    );
                    observed = observed.max(watermark);
                }
                observed
            })
        })
        .collect();

    let mut rng = Rng(0xABCD_1234_5678_9F01);
    for _ in 0..600 {
        handle.append(vec![random_event(&mut rng)], None).unwrap();
    }
    stop.store(true, Ordering::Relaxed);

    let mut max_seen = 0u64;
    for r in readers {
        max_seen = max_seen.max(r.join().unwrap());
    }
    assert!(max_seen > 0, "readers should have observed some appends");
    coord.shutdown();
}

#[test]
fn concurrent_reads_of_the_active_index_see_a_consistent_prefix() {
    // Every event carries course:c1, so a *tag* query pinned at watermark W must return
    // exactly 1..=W. Unlike `Query::all` (which streams a bypass log scan), a tag query
    // routes through the index: sealed segments via their on-disk index, and the active range
    // via the shared, watermark-bounded `ActiveView` (phase 6b). Readers hammer the active
    // tail while the writer feeds it, so a torn view of the chunked postings/type column, a
    // posting past the watermark, or a backbone not yet covering a visible local would each
    // break the dense-prefix assert. Small segments force constant rollover, so there is
    // always a live active tail being read mid-growth.
    let (coord, handle, _dir) = coordinator();
    let stop = Arc::new(AtomicBool::new(false));

    let readers: Vec<_> = (0..4)
        .map(|_| {
            let reader = handle.reader();
            let stop = Arc::clone(&stop);
            thread::spawn(move || {
                let mut max_seen = 0u64;
                while !stop.load(Ordering::Relaxed) {
                    let query = Query::item(QueryItem::with_tags(tags(&["course:c1"])));
                    let mut reads = reader.read(query, Position::ZERO);
                    let watermark = reads.watermark().get();
                    let mut positions = Vec::new();
                    while let Some(item) = reads.next() {
                        positions.push(item.expect("read failed").position.get());
                    }
                    let expected: Vec<u64> = (1..=watermark).collect();
                    assert_eq!(
                        positions, expected,
                        "active-index read did not return the dense prefix 1..={watermark}"
                    );
                    max_seen = max_seen.max(watermark);
                }
                max_seen
            })
        })
        .collect();

    for _ in 0..600 {
        handle
            .append(vec![tagged_event("Enrolled", &["course:c1"])], None)
            .unwrap();
    }
    stop.store(true, Ordering::Relaxed);

    let mut max_seen = 0u64;
    for r in readers {
        max_seen = max_seen.max(r.join().unwrap());
    }
    assert!(max_seen > 0, "readers should have observed some appends");
    coord.shutdown();
}

#[test]
fn watermark_resume_has_no_gap_or_duplicate() {
    // A read pinned at W1 returns 1..=W1 and reports W1; after more appends, a read resumed
    // from W1 returns exactly W1+1..=W2 with no gap and no duplicate at the boundary. This
    // is the phase-7 catch-up seam.
    let (coord, handle, _dir) = coordinator();

    let mut rng = Rng(0x5EED_5EED_5EED_5EED);
    for _ in 0..50 {
        handle.append(vec![random_event(&mut rng)], None).unwrap();
    }

    let first = handle.read(Query::all(), Position::ZERO);
    let w1 = first.watermark();
    let prefix: Vec<u64> = collect_positions(first);
    assert_eq!(prefix, (1..=w1.get()).collect::<Vec<_>>());

    for _ in 0..50 {
        handle.append(vec![random_event(&mut rng)], None).unwrap();
    }

    let resumed = handle.read(Query::all(), w1);
    let w2 = resumed.watermark();
    let tail: Vec<u64> = collect_positions(resumed);
    assert!(w2 > w1, "watermark should advance after more appends");
    assert_eq!(tail, (w1.get() + 1..=w2.get()).collect::<Vec<_>>());

    coord.shutdown();
}

fn collect_positions(mut reads: dcbdb::read::Reads) -> Vec<u64> {
    let mut out = Vec::new();
    while let Some(item) = reads.next() {
        out.push(item.expect("read failed").position.get());
    }
    out
}
