# tephra

[![Crates.io](https://img.shields.io/crates/v/tephra.svg)](https://crates.io/crates/tephra)
[![Documentation](https://docs.rs/tephra/badge.svg)](https://docs.rs/tephra)
[![License](https://img.shields.io/crates/l/tephra.svg)](https://github.com/tephradb/tephra/blob/main/LICENSE)

A DCB-compliant, immutable event store with global ordering.

Tephra is a Dynamic Consistency Boundary (DCB) event store. Instead of a static consistency
boundary baked into an aggregate, the boundary is derived per decision from a query. Events
carry a type plus a set of tags (`course:c1`, `student:s1`), so one event can belong to several
entities at once, and a decision reads exactly the events it depends on and guards exactly those
on append.

This crate is the embedded engine: the durable log, the single writer, the index, and the read
paths. Use it directly in-process, or reach it over the network with the
[`tephra-server`](https://crates.io/crates/tephra-server) TCP server and the
[`tephra-client`](https://crates.io/crates/tephra-client) client.

## Design

The log is the source of truth and everything else is derived. Data is written once, never
updated and never deleted, keyed by a dense monotonic position assigned by the single writer.
Indexes need no write-ahead log and no fsync on the write path, because they can be rebuilt by
replaying the log. See
[`ARCHITECTURE.md`](https://github.com/tephradb/tephra/blob/main/ARCHITECTURE.md) for the full
rationale and the alternatives that were rejected.

## Example

```rust,no_run
use tephra::{
    AppendCondition, Event, EventType, Position, Query, QueryItem, SegmentConfig,
    SegmentSet, Tag, Tags, WriteCoordinator, WriterConfig,
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Open (or create) a log directory and start the single-writer coordinator.
    let set = SegmentSet::open("tephra-data", SegmentConfig::new(256 * 1024 * 1024))?;
    let (coordinator, handle) = WriteCoordinator::start(set, WriterConfig::default())?;

    // Build a packed event, then append it guarded so it fails if course:c1 already exists.
    let ty = EventType::new("CourseOpened")?;
    let tags = Tags::new([Tag::new("course:c1")?])?;
    let event = Event::new(&ty, &tags, br#"{"course":"c1","seats":30}"#)?;
    let guard = AppendCondition::new(Query::item(QueryItem::with_tags(
        Tags::new([Tag::new("course:c1")?])?,
    )));
    handle.append(vec![event], Some(guard))?;

    // Reads run on the caller's thread over a snapshot. `read` returns a lending iterator, so it
    // is consumed with `while let`, not a `for` loop.
    let query = Query::item(QueryItem::with_tags(Tags::new([Tag::new("course:c1")?])?));
    let mut reads = handle.read(&query, Position::ZERO, None);
    while let Some(item) = reads.next() {
        let seq = item?;
        println!("{} {}", seq.position, seq.event.event_type());
    }

    coordinator.shutdown();
    Ok(())
}
```

## Related crates

- [`tephra-types`](https://crates.io/crates/tephra-types): the shared vocabulary (positions, names, query model).
- [`tephra-server`](https://crates.io/crates/tephra-server): a TCP server exposing the engine.
- [`tephra-client`](https://crates.io/crates/tephra-client): a synchronous client for the server.
- [`seglog`](https://crates.io/crates/seglog): the low-level segmented record log underneath.

## License

Licensed under the [Apache License, Version 2.0](https://github.com/tephradb/tephra/blob/main/LICENSE).
