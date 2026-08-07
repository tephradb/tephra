//! Minimal end-to-end usage of the event store: open a log, append events (one
//! unconditional, one guarded by an append condition), watch a conflicting guard fail,
//! then read everything back.
//!
//! Run with `cargo run`.

use std::{env, fs};

use dcbdb::event::{Event, EventType, Tag, Tags};
use dcbdb::log::set::{SegmentConfig, SegmentSet};
use dcbdb::query::{AppendCondition, Query, QueryItem};
use dcbdb::writer::{WriteCoordinator, WriterConfig};

fn event(ty: &str, tags: &[&str], payload: &[u8]) -> Event {
    let ty = EventType::new(ty).unwrap();
    let tags = Tags::new(
        tags.iter()
            .map(|t| Tag::new(t).unwrap())
            .collect::<Vec<_>>(),
    )
    .unwrap();
    Event::new(&ty, &tags, payload).unwrap()
}

fn main() {
    // A 16 MiB-per-segment log in a scratch directory (large enough for the default
    // writer batch budget).
    let dir = env::temp_dir().join("dcbdb-demo");
    let _ = fs::remove_dir_all(&dir);
    let set = SegmentSet::open(&dir, SegmentConfig::new(16 * 1024 * 1024)).unwrap();

    // The write coordinator owns the log; callers talk to it through a cloneable handle.
    let (coordinator, handle) = WriteCoordinator::start(set, WriterConfig::default());

    // Unconditional append: a student enrols on a course.
    let range = handle
        .append(
            vec![event("Enrolled", &["course:c1", "student:s1"], b"{}")],
            None,
        )
        .unwrap();
    println!("appended Enrolled at position {}", range.first);

    // Guarded append: reserve a unique username, failing if one already exists.
    let guard = AppendCondition::new(Query::item(QueryItem::with_tags(
        Tags::new(vec![Tag::new("username:alice").unwrap()]).unwrap(),
    )));
    let range = handle
        .append(
            vec![event("UsernameReserved", &["username:alice"], b"{}")],
            Some(guard.clone()),
        )
        .unwrap();
    println!("reserved username:alice at position {}", range.first);

    // The same guard now conflicts with the event just written.
    match handle.append(
        vec![event("UsernameReserved", &["username:alice"], b"{}")],
        Some(guard),
    ) {
        Ok(_) => println!("second reservation unexpectedly succeeded"),
        Err(err) => println!("second reservation rejected: {err}"),
    }

    // Shutting down returns the log so we can read it back.
    let set = coordinator.shutdown();
    println!("log holds {} events:", set.last_position());
    let mut scan = set.scan_from(dcbdb::Position::ZERO);
    while let Some(item) = scan.next() {
        let record = item.unwrap();
        let event = dcbdb::event::EventRef::from_bytes(record.data).unwrap();
        let tags: Vec<&str> = event.tags().collect();
        println!("  {} {} {:?}", record.position, event.event_type(), tags);
    }
}
