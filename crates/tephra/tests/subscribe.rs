//! End-to-end tests of subscriptions (`ReadHandle::subscribe` / `Subscription`).
//!
//! The crux is the catch-up/live boundary: a subscription must deliver every matching event
//! exactly once, in ascending order, with no gap and no duplicate where catch-up hands off to
//! live tailing. These tests hammer that boundary (append during catch-up, subscriber slower
//! than the writer, subscriber starting exactly at the watermark) and the wakeup/shutdown
//! plumbing.

use std::thread;
use std::time::Duration;

use smallvec::SmallVec;
use tempfile::TempDir;
use tephra::Position;
use tephra::event::{Event, EventType, Tag, Tags};
use tephra::log::set::{SegmentConfig, SegmentSet};
use tephra::query::{Query, QueryItem};
use tephra::writer::{WriteCoordinator, WriteHandle, WriterConfig};

// ------------------------------- helpers -------------------------------

/// Small segments so a modest workload rolls over many times, exercising the sealed-index and
/// active-tail halves of the read path a subscription drives.
fn coordinator() -> (WriteCoordinator, WriteHandle, TempDir) {
    let dir = TempDir::new().unwrap();
    let set = SegmentSet::open(dir.path(), SegmentConfig::new(512)).unwrap();
    let cfg = WriterConfig {
        queue_capacity: 64,
        max_batch_records: 64,
        max_batch_bytes: 256,
        tips_window: 1_000_000,
        verify_tips: false,
        ..WriterConfig::default()
    };
    let (coord, handle) = WriteCoordinator::start(set, cfg).unwrap();
    (coord, handle, dir)
}

fn tags(items: &[&str]) -> Tags {
    Tags::new(
        items
            .iter()
            .map(|s| Tag::new(*s).unwrap())
            .collect::<SmallVec<[Tag; 4]>>(),
    )
    .unwrap()
}

fn tagged_event(ty: &str, tag_strs: &[&str]) -> Event {
    Event::new(&EventType::new(ty).unwrap(), &tags(tag_strs), b"payload").unwrap()
}

fn append(handle: &WriteHandle, tag: &str) {
    handle
        .append(vec![tagged_event("E", &[tag])], None)
        .unwrap();
}

fn tag_query(tag: &str) -> Query {
    Query::item(QueryItem::with_tags(tags(&[tag])))
}

// ------------------------------- tests -------------------------------

/// The crux: catch up on a prefix, then keep receiving live appends, and the full delivered
/// sequence is exactly the dense positions with no gap and no duplicate at the boundary. Run
/// with the subscriber on its own thread while the writer appends concurrently, so the
/// catch-up/live handoff happens under real interleaving.
#[test]
fn catch_up_then_live_has_no_gap_or_duplicate() {
    let (coord, handle, _dir) = coordinator();
    let before = 60u64;
    let after = 60u64;
    let total = before + after;

    for _ in 0..before {
        append(&handle, "k:1");
    }

    let reader = handle.reader();
    let subscriber = thread::spawn(move || {
        let mut sub = reader.subscribe(Query::all(), Position::ZERO);
        let mut positions = Vec::new();
        while (positions.len() as u64) < total {
            match sub.next_batch() {
                Some(Ok(batch)) => positions.extend(batch.iter().map(|(p, _)| p.get())),
                Some(Err(err)) => panic!("subscription error: {err}"),
                None => break,
            }
        }
        positions
    });

    // Append the rest concurrently: some land while the subscriber is still catching up.
    for _ in 0..after {
        append(&handle, "k:1");
    }

    let positions = subscriber.join().unwrap();
    assert_eq!(
        positions,
        (1..=total).collect::<Vec<_>>(),
        "delivered positions must be dense with no gap or duplicate"
    );
    coord.shutdown();
}

/// A high-contention variant: the writer hammers appends while the subscriber consumes. A lost
/// wakeup would hang the subscriber (the test would deadlock), and a gap/duplicate would fail
/// the assertion.
#[test]
fn subscription_under_contention_is_complete() {
    let (coord, handle, _dir) = coordinator();
    let total = 1000u64;

    let reader = handle.reader();
    let subscriber = thread::spawn(move || {
        let mut sub = reader.subscribe(Query::all(), Position::ZERO);
        let mut next_expected = 1u64;
        while next_expected <= total {
            match sub.next_batch() {
                Some(Ok(batch)) => {
                    for (position, _) in batch {
                        assert_eq!(position.get(), next_expected, "gap or duplicate");
                        next_expected += 1;
                    }
                }
                Some(Err(err)) => panic!("subscription error: {err}"),
                None => break,
            }
        }
        next_expected - 1
    });

    for _ in 0..total {
        append(&handle, "k:1");
    }

    assert_eq!(subscriber.join().unwrap(), total);
    coord.shutdown();
}

/// Several concurrent subscribers each receive the complete stream: `wake` notifies all of
/// them, and the subscriber-count gate works past one.
#[test]
fn multiple_subscribers_each_receive_everything() {
    let (coord, handle, _dir) = coordinator();
    let total = 300u64;

    let subscribers: Vec<_> = (0..4)
        .map(|_| {
            let reader = handle.reader();
            thread::spawn(move || {
                let mut sub = reader.subscribe(Query::all(), Position::ZERO);
                let mut next_expected = 1u64;
                while next_expected <= total {
                    match sub.next_batch() {
                        Some(Ok(batch)) => {
                            for (position, _) in batch {
                                assert_eq!(position.get(), next_expected, "gap or duplicate");
                                next_expected += 1;
                            }
                        }
                        Some(Err(err)) => panic!("subscription error: {err}"),
                        None => break,
                    }
                }
                next_expected - 1
            })
        })
        .collect();

    for _ in 0..total {
        append(&handle, "k:1");
    }

    for subscriber in subscribers {
        assert_eq!(subscriber.join().unwrap(), total);
    }
    coord.shutdown();
}

/// A selective subscription delivers only matching events, and the cursor still advances past
/// a non-matching tail to the watermark (so it never re-scans it).
#[test]
fn selective_subscription_delivers_only_matches_and_advances_cursor() {
    let (coord, handle, _dir) = coordinator();
    // 20 events alternating hit/miss; the last (position 20) is a miss.
    for i in 0..20 {
        append(&handle, if i % 2 == 0 { "k:hit" } else { "k:miss" });
    }

    let mut sub = handle.subscribe(tag_query("k:hit"), Position::ZERO);
    let batch = sub.poll_batch().unwrap();
    let got: Vec<u64> = batch.iter().map(|(p, _)| p.get()).collect();
    // i even -> position i+1: 1, 3, 5, ... 19.
    let expected: Vec<u64> = (0u64..20).filter(|i| i % 2 == 0).map(|i| i + 1).collect();
    assert_eq!(got, expected);
    // Cursor advanced to the watermark (20), past the trailing non-matching event, not just to
    // the last delivered match (19).
    assert_eq!(sub.position(), Position::new(20));
    // Caught up now: a further poll is empty.
    assert!(sub.poll_batch().unwrap().is_empty());
    coord.shutdown();
}

/// Starting exactly at the watermark: the subscription is immediately caught up (empty poll),
/// then a later append advances it and is delivered with no duplicate.
#[test]
fn subscribe_at_watermark_then_receives_next_append() {
    let (coord, handle, _dir) = coordinator();
    for _ in 0..10 {
        append(&handle, "k:1");
    }
    let tip = handle.read(&Query::all(), Position::ZERO, None).watermark();
    assert_eq!(tip, Position::new(10));

    let mut sub = handle.subscribe(Query::all(), tip);
    // Caught up at the tip: nothing to deliver yet.
    assert!(sub.poll_batch().unwrap().is_empty());

    append(&handle, "k:1");
    let batch = sub.next_batch().expect("store live").expect("no error");
    assert_eq!(batch.len(), 1);
    assert_eq!(batch[0].0, Position::new(11));
    coord.shutdown();
}

/// A slow subscriber with a tiny batch cap receives bounded batches and still sees every event
/// in order.
#[test]
fn slow_subscriber_gets_bounded_batches_and_completeness() {
    let (coord, handle, _dir) = coordinator();
    for _ in 0..100 {
        append(&handle, "k:1");
    }

    let mut sub = handle
        .subscribe(Query::all(), Position::ZERO)
        .with_max_batch_events(10);
    let mut positions = Vec::new();
    loop {
        let batch = sub.poll_batch().unwrap();
        if batch.is_empty() {
            break;
        }
        assert!(batch.len() <= 10, "batch must respect the cap");
        positions.extend(batch.iter().map(|(p, _)| p.get()));
    }
    assert_eq!(positions, (1..=100).collect::<Vec<_>>());
    coord.shutdown();
}

/// Shutdown wakes a subscriber blocked at the live edge: its blocking `next_batch` returns
/// `None` rather than hanging. This is also the proof that `wait` really blocks (a subscriber
/// that never parked could not be woken by close).
#[test]
fn shutdown_wakes_a_blocked_subscriber() {
    let (coord, handle, _dir) = coordinator();
    append(&handle, "k:1");

    let reader = handle.reader();
    let subscriber = thread::spawn(move || {
        let mut sub = reader.subscribe(Query::all(), Position::ZERO);
        // Drain the one durable event, then block at the live edge.
        let first = sub.next_batch().expect("live").expect("no error");
        assert_eq!(first.len(), 1);
        // Caught up: this call blocks until the coordinator closes.
        sub.next_batch()
    });

    // Give the subscriber time to reach and park at the live edge, then shut down.
    thread::sleep(Duration::from_millis(100));
    coord.shutdown();

    assert!(
        subscriber.join().unwrap().is_none(),
        "a blocked subscriber must observe shutdown and end"
    );
}

/// `wait_timeout` returns `TimedOut` at the live edge with no new events, and `Advanced` once
/// an event arrives; the bounded form the server uses to stay shutdown-responsive.
#[test]
fn wait_timeout_ticks_then_advances() {
    use tephra::WaitOutcome;

    let (coord, handle, _dir) = coordinator();
    append(&handle, "k:1");
    let mut sub = handle.subscribe(Query::all(), Position::ZERO);
    // Drain to the live edge.
    assert_eq!(sub.poll_batch().unwrap().len(), 1);
    assert!(sub.poll_batch().unwrap().is_empty());

    // No new events: a short wait times out.
    assert_eq!(
        sub.wait_timeout(Duration::from_millis(20)),
        WaitOutcome::TimedOut
    );

    // After an append, the wait reports an advance.
    append(&handle, "k:1");
    assert_eq!(
        sub.wait_timeout(Duration::from_millis(500)),
        WaitOutcome::Advanced
    );
    coord.shutdown();
}
