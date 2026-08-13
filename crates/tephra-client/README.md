# tephra-client

[![Crates.io](https://img.shields.io/crates/v/tephra-client.svg)](https://crates.io/crates/tephra-client)
[![Documentation](https://docs.rs/tephra-client/badge.svg)](https://docs.rs/tephra-client)
[![License](https://img.shields.io/crates/l/tephra-client.svg)](https://github.com/tqwewe/tephra/blob/main/LICENSE)

A synchronous, blocking TCP client for a [tephra](https://crates.io/crates/tephra) event store,
speaking the length-prefixed protobuf protocol.

The client speaks clean Rust types: the shared vocabulary from
[`tephra-types`](https://crates.io/crates/tephra-types) (`Query`, `QueryItem`,
`AppendCondition`, `Position`, `EventType`, `Tag`, `Tags`) plus a friendly owned `Event` and
`SequencedEvent`. The wire protobuf types are an implementation detail hidden behind these.

It exposes append, one-shot reads, and live subscriptions (catch-up followed by a live tail,
with no gap or duplicate at the boundary).

## Example

```rust,no_run
use tephra_client::{Client, Event, Position, Query, QueryItem, Tag, Tags};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = Client::connect("127.0.0.1:9000")?;

    // Append an event: type, tags, and an opaque payload. `None` means no append condition.
    let event = Event::new("CourseOpened", ["course:c1"], br#"{"course":"c1","seats":30}"#.to_vec())?;
    let result = client.append([event], None)?;
    println!("recorded positions {} to {}", result.first, result.last);

    // Read every event matching a query, from the beginning.
    let query = Query::item(QueryItem::with_tags(Tags::new([Tag::new("course:c1")?])?));
    let (events, _watermark) = client.read_all(query, Position::ZERO, None)?;
    for seq in &events {
        println!("{} {}", seq.position(), seq.event().event_type());
    }
    Ok(())
}
```

Subscriptions follow the same query model; see the [crate documentation](https://docs.rs/tephra-client)
for the `subscribe` API.

## Related crates

- [`tephra-server`](https://crates.io/crates/tephra-server): the server this client connects to.
- [`tephra-types`](https://crates.io/crates/tephra-types): the shared vocabulary.
- [`tephra`](https://crates.io/crates/tephra): the embedded engine, if you do not need the network.

## License

Licensed under the [Apache License, Version 2.0](https://github.com/tqwewe/tephra/blob/main/LICENSE).
