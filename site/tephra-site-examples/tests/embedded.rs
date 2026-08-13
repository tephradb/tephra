//! Embedding the engine directly, without the server: open a store, start the coordinator,
//! append with the packed engine `Event`, and read over the lending iterator.

use tempfile::TempDir;
use tephra::{
    AppendCondition, Event, EventType, Position, Query, QueryItem, SegmentConfig, SegmentSet, Tag,
    Tags, WriteCoordinator, WriterConfig,
};

#[test]
fn embed_the_engine_directly() {
    let dir = TempDir::new().expect("temp dir");

    // ANCHOR: open
    // Open (or create) a log directory and start the single-writer coordinator.
    // SegmentConfig and SegmentSet are re-exported at the crate root.
    let set =
        SegmentSet::open(dir.path(), SegmentConfig::new(256 * 1024 * 1024)).expect("open store");
    let (coordinator, handle) =
        WriteCoordinator::start(set, WriterConfig::default()).expect("start coordinator");
    // ANCHOR_END: open

    // ANCHOR: append
    // The engine's packed Event is built from &EventType, &Tags, and an opaque payload.
    let ty = EventType::new("CourseOpened").expect("type");
    let tags = Tags::new([Tag::new("course:c1").expect("tag")]).expect("tags");
    let event = Event::new(&ty, &tags, br#"{"course":"c1","seats":30}"#).expect("encode");

    // Guard the append so it fails if any event already carries course:c1 (a uniqueness guard).
    let guard = AppendCondition::new(Query::item(QueryItem::with_tags(
        Tags::new([Tag::new("course:c1").expect("tag")]).expect("tags"),
    )));
    let range = handle.append(vec![event], Some(guard)).expect("append");
    // ANCHOR_END: append

    // ANCHOR: read
    // Reads run on the caller's own thread over a snapshot published at each commit. Reads is a
    // lending iterator, so it is consumed with `while let`, not a `for` loop.
    let query = Query::item(QueryItem::with_tags(
        Tags::new([Tag::new("course:c1").expect("tag")]).expect("tags"),
    ));
    let mut reads = handle.read(&query, Position::ZERO, None);
    let mut count = 0usize;
    while let Some(item) = reads.next() {
        let seq = item.expect("decode");
        println!("{} {}", seq.position, seq.event.event_type());
        count += 1;
    }
    // ANCHOR_END: read

    assert_eq!(range.first, Position::new(1));
    assert_eq!(count, 1);

    // Shutdown joins the writer thread and returns the SegmentSet.
    coordinator.shutdown();
}
