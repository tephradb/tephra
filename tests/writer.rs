//! End-to-end tests of the write coordinator through its public API: concurrency,
//! conflict resolution, shutdown, backpressure, and durability across reopen.

use std::collections::HashSet;
use std::sync::Arc;
use std::thread;

use smallvec::SmallVec;
use tempfile::TempDir;

use dcbdb::Position;
use dcbdb::event::{Event, EventType, Tag, Tags};
use dcbdb::log::set::{SegmentConfig, SegmentSet};
use dcbdb::query::{AppendCondition, Query, QueryItem};
use dcbdb::writer::{AppendError, WriteCoordinator, WriteHandle, WriterConfig};

const SEG_SIZE: usize = 1 << 20;

fn config() -> WriterConfig {
    WriterConfig {
        queue_capacity: 256,
        max_batch_records: 256,
        max_batch_bytes: 256 * 1024,
        tips_window: 1_000_000,
        verify_tips: true,
    }
}

fn open(dir: &TempDir) -> SegmentSet {
    SegmentSet::open(dir.path(), SegmentConfig::new(SEG_SIZE)).unwrap()
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

fn event(ty: &str, tag_strs: &[&str]) -> Event {
    Event::new(&EventType::new(ty).unwrap(), &tags(tag_strs), b"data").unwrap()
}

fn unique_guard(tag: &str) -> AppendCondition {
    AppendCondition::new(Query::item(QueryItem::with_tags(tags(&[tag]))))
}

#[test]
fn concurrent_appends_are_dense_and_unique() {
    let dir = TempDir::new().unwrap();
    let (coord, handle) = WriteCoordinator::start(open(&dir), config()).unwrap();

    const THREADS: u64 = 8;
    const PER_THREAD: u64 = 250;

    let mut joins = Vec::new();
    for t in 0..THREADS {
        let h: WriteHandle = handle.clone();
        joins.push(thread::spawn(move || {
            let mut positions = Vec::new();
            for i in 0..PER_THREAD {
                let ev = event("Appended", &[&format!("thread:{t}"), &format!("seq:{i}")]);
                let range = h.append(vec![ev], None).unwrap();
                assert_eq!(range.first, range.last);
                positions.push(range.first.get());
            }
            positions
        }));
    }

    let mut all: Vec<u64> = joins.into_iter().flat_map(|j| j.join().unwrap()).collect();
    all.sort_unstable();

    let total = THREADS * PER_THREAD;
    // Dense 1..=total, no gaps, no duplicates.
    assert_eq!(all.len() as u64, total);
    assert_eq!(all.first().copied(), Some(1));
    assert_eq!(all.last().copied(), Some(total));
    assert_eq!(all.iter().collect::<HashSet<_>>().len() as u64, total);
    for (i, p) in all.iter().enumerate() {
        assert_eq!(*p, i as u64 + 1);
    }

    drop(handle);
    let set = coord.shutdown();
    assert_eq!(set.last_position(), Position::new(total));
}

#[test]
fn overlapping_uniqueness_guard_lets_exactly_one_win() {
    // Many threads race to append the same unique tag under a uniqueness guard. Whether
    // they land in the same drain (one SameBatch conflict) or different drains (one
    // Durable conflict), exactly one may win.
    let dir = TempDir::new().unwrap();
    let (coord, handle) = WriteCoordinator::start(open(&dir), config()).unwrap();

    const THREADS: usize = 16;
    let barrier = Arc::new(std::sync::Barrier::new(THREADS));
    let mut joins = Vec::new();
    for _ in 0..THREADS {
        let h = handle.clone();
        let b = barrier.clone();
        joins.push(thread::spawn(move || {
            b.wait();
            h.append(
                vec![event("Reserved", &["unique:x"])],
                Some(unique_guard("unique:x")),
            )
        }));
    }

    let mut wins = 0;
    let mut conflicts = 0;
    for j in joins {
        match j.join().unwrap() {
            Ok(_) => wins += 1,
            Err(AppendError::Conflict { .. }) => conflicts += 1,
            Err(other) => panic!("unexpected error: {other:?}"),
        }
    }
    assert_eq!(wins, 1, "exactly one writer should win");
    assert_eq!(conflicts, THREADS - 1);

    drop(handle);
    let set = coord.shutdown();
    assert_eq!(set.last_position(), Position::new(1));
}

#[test]
fn explicit_shutdown_returns_a_consistent_set() {
    let dir = TempDir::new().unwrap();
    let (coord, handle) = WriteCoordinator::start(open(&dir), config()).unwrap();

    for i in 0..10 {
        handle
            .append(vec![event("E", &[&format!("k:{i}")])], None)
            .unwrap();
    }
    drop(handle);
    let set = coord.shutdown();
    assert_eq!(set.last_position(), Position::new(10));
    drop(set);

    // Reopening the directory sees exactly the acknowledged writes.
    let reopened = open(&dir);
    assert_eq!(reopened.last_position(), Position::new(10));
}

#[test]
fn drop_based_shutdown_persists_acknowledged_writes() {
    let dir = TempDir::new().unwrap();
    {
        let (coord, handle) = WriteCoordinator::start(open(&dir), config()).unwrap();
        for i in 0..5 {
            handle
                .append(vec![event("E", &[&format!("k:{i}")])], None)
                .unwrap();
        }
        // Drop both: the coordinator's Drop signals shutdown and joins the thread.
        drop(handle);
        drop(coord);
    }
    let reopened = open(&dir);
    assert_eq!(reopened.last_position(), Position::new(5));
}

#[test]
fn tiny_queue_still_serves_all_writes() {
    // queue_capacity = 1 forces the bounded channel to block and unblock repeatedly;
    // every write must still complete, with dense positions.
    let dir = TempDir::new().unwrap();
    let cfg = WriterConfig {
        queue_capacity: 1,
        ..config()
    };
    let (coord, handle) = WriteCoordinator::start(open(&dir), cfg).unwrap();

    let mut joins = Vec::new();
    for t in 0..4 {
        let h = handle.clone();
        joins.push(thread::spawn(move || {
            for i in 0..100 {
                h.append(
                    vec![event("E", &[&format!("t:{t}"), &format!("i:{i}")])],
                    None,
                )
                .unwrap();
            }
        }));
    }
    for j in joins {
        j.join().unwrap();
    }

    drop(handle);
    let set = coord.shutdown();
    assert_eq!(set.last_position(), Position::new(400));
}

#[test]
fn index_search_sees_own_writes_across_rollovers() {
    // Read-your-writes through the index: a query issued right after an append must see
    // that append, including the batch that triggered a rollover (visible in the freshly
    // sealed segment) and everything before it. Tiny segments force many rollovers mid-run.
    let dir = TempDir::new().unwrap();
    let set = SegmentSet::open(dir.path(), SegmentConfig::new(512)).unwrap();
    let cfg = WriterConfig {
        queue_capacity: 64,
        max_batch_records: 64,
        max_batch_bytes: 256,
        tips_window: 1_000_000,
        verify_tips: true,
    };
    let (coord, handle) = WriteCoordinator::start(set, cfg).unwrap();

    // Every event carries course:c1, so after the i-th append the query must return
    // exactly positions 1..=i.
    let mut expected = Vec::new();
    for i in 0..80u64 {
        let ty = if i % 2 == 0 { "Enrolled" } else { "Renamed" };
        let range = handle
            .append(vec![event(ty, &["course:c1"])], None)
            .unwrap();
        expected.push(range.first);

        let got = handle
            .search(
                Query::item(QueryItem::with_tags(tags(&["course:c1"]))),
                Position::ZERO,
            )
            .unwrap();
        assert_eq!(got, expected, "read-your-writes after append {i}");
    }

    // A type filter and an `after` bound also compose across the sealed segments.
    let enrolled = handle
        .search(
            Query::item(QueryItem::of_types(vec![
                EventType::new("Enrolled").unwrap(),
            ])),
            Position::new(40),
        )
        .unwrap();
    assert!(enrolled.iter().all(|p| p.get() > 40));
    assert!(!enrolled.is_empty());

    drop(handle);
    coord.shutdown();
}

#[test]
fn append_with_no_events_is_rejected() {
    let dir = TempDir::new().unwrap();
    let (coord, handle) = WriteCoordinator::start(open(&dir), config()).unwrap();
    assert!(matches!(
        handle.append(vec![], None),
        Err(AppendError::Empty)
    ));
    drop(handle);
    coord.shutdown();
}
