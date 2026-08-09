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
use dcbdb::writer::{AppendError, ConflictSite, WriteCoordinator, WriteHandle, WriterConfig};

const SEG_SIZE: usize = 1 << 20;

fn config() -> WriterConfig {
    WriterConfig {
        queue_capacity: 256,
        max_batch_records: 256,
        max_batch_bytes: 256 * 1024,
        tips_window: 1_000_000,
        verify_tips: true,
        ..WriterConfig::default()
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

/// Reads a query through the handle and collects just the matching positions.
fn read_positions(handle: &WriteHandle, query: Query, after: Position) -> Vec<Position> {
    let mut reads = handle.read(query, after);
    let mut out = Vec::new();
    while let Some(item) = reads.next() {
        out.push(item.expect("read failed").position);
    }
    out
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
        ..WriterConfig::default()
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

        let got = read_positions(
            &handle,
            Query::item(QueryItem::with_tags(tags(&["course:c1"]))),
            Position::ZERO,
        );
        assert_eq!(got, expected, "read-your-writes after append {i}");
    }

    // A type filter and an `after` bound also compose across the sealed segments.
    let enrolled = read_positions(
        &handle,
        Query::item(QueryItem::of_types(vec![
            EventType::new("Enrolled").unwrap(),
        ])),
        Position::new(40),
    );
    assert!(enrolled.iter().all(|p| p.get() > 40));
    assert!(!enrolled.is_empty());

    drop(handle);
    coord.shutdown();
}

#[test]
fn durable_conflict_detected_through_the_index_across_sealed_segments() {
    // With verify off, the durable arm runs the index existence check alone (no scan
    // cross-check), so this exercises the real production path. Tiny segments
    // force seals, so the guarded tag lives in an early *sealed* segment: the check must
    // find it there, proving the index path works across sealed segments, not just the
    // active tail. This is the `after`-omitted uniqueness guard, the shape the index most improves.
    let dir = TempDir::new().unwrap();
    let set = SegmentSet::open(dir.path(), SegmentConfig::new(512)).unwrap();
    let cfg = WriterConfig {
        queue_capacity: 64,
        max_batch_records: 64,
        max_batch_bytes: 256,
        verify_tips: false,
        ..WriterConfig::default()
    };
    let (coord, handle) = WriteCoordinator::start(set, cfg).unwrap();

    // The guarded tag lands first, then enough filler to seal several later segments.
    handle
        .append(vec![event("Reserved", &["unique:early"])], None)
        .unwrap();
    for i in 0..80u64 {
        handle
            .append(vec![event("Filler", &[&format!("f:{i}")])], None)
            .unwrap();
    }

    // A uniqueness guard (after: 0) on the early tag must see the durable event and conflict.
    let err = handle
        .append(
            vec![event("Reserved", &["unique:early"])],
            Some(unique_guard("unique:early")),
        )
        .unwrap_err();
    assert!(
        matches!(
            err,
            AppendError::Conflict {
                at: ConflictSite::Durable(_)
            }
        ),
        "expected a durable conflict from a sealed segment, got {err:?}"
    );

    // A guard on a tag no event carries succeeds.
    handle
        .append(
            vec![event("Reserved", &["unique:fresh"])],
            Some(unique_guard("unique:fresh")),
        )
        .unwrap();

    drop(handle);
    let set = coord.shutdown();
    assert!(
        set.sealed_len() >= 1,
        "tiny segments should have sealed, so unique:early was in a sealed segment"
    );
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

/// Drives a future to completion on the calling thread with a park/unpark waker. The
/// worker replies from its own thread, so its `wake` unparks us. Keeps the async tests
/// free of an executor dependency.
#[cfg(feature = "async")]
fn block_on<F: std::future::Future>(fut: F) -> F::Output {
    use std::sync::Arc;
    use std::task::{Context, Poll, Wake, Waker};
    use std::thread::{self, Thread};

    struct ThreadWaker(Thread);
    impl Wake for ThreadWaker {
        fn wake(self: Arc<Self>) {
            self.0.unpark();
        }
        fn wake_by_ref(self: &Arc<Self>) {
            self.0.unpark();
        }
    }

    let waker = Waker::from(Arc::new(ThreadWaker(thread::current())));
    let mut cx = Context::from_waker(&waker);
    let mut fut = std::pin::pin!(fut);
    loop {
        match fut.as_mut().poll(&mut cx) {
            Poll::Ready(out) => return out,
            Poll::Pending => thread::park(),
        }
    }
}

#[cfg(feature = "async")]
#[test]
fn append_async_assigns_dense_positions_and_read_sees_own_writes() {
    let dir = TempDir::new().unwrap();
    let (coord, handle) = WriteCoordinator::start(open(&dir), config()).unwrap();

    block_on(async {
        // Every event carries course:c1, so after the i-th append the read must return
        // exactly positions 1..=i. `read` is synchronous (it runs on this thread over the
        // published snapshot); read-your-writes holds because `append_async` resolves only
        // after the writer has published the watermark.
        let mut expected = Vec::new();
        for i in 0..20u64 {
            let range = handle
                .append_async(vec![event("Enrolled", &["course:c1"])], None)
                .await
                .unwrap();
            assert_eq!(range.first, range.last, "one event, one position");
            assert_eq!(range.first, Position::new(i + 1), "dense positions");
            expected.push(range.first);

            let got = read_positions(
                &handle,
                Query::item(QueryItem::with_tags(tags(&["course:c1"]))),
                Position::ZERO,
            );
            assert_eq!(got, expected, "read-your-writes after async append {i}");
        }
    });

    drop(handle);
    let set = coord.shutdown();
    assert_eq!(set.last_position(), Position::new(20));
}

#[cfg(feature = "async")]
#[test]
fn append_async_honors_the_uniqueness_guard() {
    let dir = TempDir::new().unwrap();
    let (coord, handle) = WriteCoordinator::start(open(&dir), config()).unwrap();

    block_on(async {
        // Empty events are rejected before ever touching the channel.
        assert!(matches!(
            handle.append_async(vec![], None).await,
            Err(AppendError::Empty)
        ));

        // First guarded write wins; the second, guarded on the same tag, sees the durable
        // conflict.
        handle
            .append_async(
                vec![event("Reserved", &["unique:x"])],
                Some(unique_guard("unique:x")),
            )
            .await
            .unwrap();
        assert!(matches!(
            handle
                .append_async(
                    vec![event("Reserved", &["unique:x"])],
                    Some(unique_guard("unique:x")),
                )
                .await,
            Err(AppendError::Conflict { .. })
        ));
    });

    drop(handle);
    let set = coord.shutdown();
    assert_eq!(set.last_position(), Position::new(1));
}
