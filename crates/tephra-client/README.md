# tephra-client

[![Crates.io](https://img.shields.io/crates/v/tephra-client.svg)](https://crates.io/crates/tephra-client)
[![Documentation](https://docs.rs/tephra-client/badge.svg)](https://docs.rs/tephra-client)
[![License](https://img.shields.io/crates/l/tephra-client.svg)](https://github.com/tephradb/tephra/blob/main/LICENSE)

A TCP client for a [tephra](https://github.com/tephradb/tephra) event store, speaking its
length-prefixed protobuf-over-TCP protocol. It ships a blocking `Client` and, behind the `async`
feature, a concurrent `AsyncClient` that multiplexes many requests over a control socket plus a pool
of bulk read sockets.

The client speaks clean Rust types: the shared vocabulary from
[`tephra-types`](https://crates.io/crates/tephra-types) (`Query`, `QueryItem`, `AppendCondition`,
`Position`, `EventType`, `Tag`, `Tags`) plus a friendly owned `Event` and `SequencedEvent`. The wire
protobuf types stay an implementation detail behind them.

```sh
cargo add tephra-client
```

Requires a tephra server on 0.4 or above, which speaks the mandatory `Hello` handshake this client
opens with. Optional features:

- `async`: the multiplexing `AsyncClient`, on Tokio.
- `tls`: TLS 1.3 for the blocking client.
- `async-tls`: TLS for the async client.

## Quick start

The blocking `Client` opens one connection and carries a single request at a time, so give each
thread its own, or use the `AsyncClient` below.

```rust,no_run
use tephra_client::{Client, Event, Position, Query, QueryItem, Tag, Tags};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = Client::connect("127.0.0.1:9000")?;

    // Append an event: a type, a set of tags, and an opaque payload. `None` is no condition.
    let event = Event::new("CourseOpened", ["course:c1"], br#"{"seats":30}"#.to_vec())?;
    let result = client.append([event], None)?;
    println!("recorded positions {} to {}", result.first, result.last);

    // Read every event matching a query, from the start.
    let query = Query::item(QueryItem::with_tags(Tags::new([Tag::new("course:c1")?])?));
    let (events, _watermark) = client.read_all(query, Position::ZERO, None)?;
    for seq in &events {
        println!("{} {}", seq.position(), seq.event().event_type());
    }

    // A point-in-time snapshot of the server.
    let stats = client.stats()?;
    println!("{} events across {} segments", stats.event_count, stats.segment_count);
    Ok(())
}
```

## Concepts

- **Event**: a type, a set of tags, and an opaque payload, built with `Event::new`.
- **Position**: a dense, 1-based global order. `Position::ZERO` is the start cursor; `Position::MAX`
  is the "from the tip" cursor for a backward read.
- **Query**: `Query::all()` matches everything; `Query::items` OR's items, where each item AND's its
  tags and OR's its types.
- **AppendCondition**: a dynamic consistency boundary. Reject the append if any event after its
  `after` position matches the query; omit `after` for the uniqueness-guard pattern.

## Reads and pagination

`read` returns a lazy `ReadStream`; `read_all` drains one into a `Vec` and returns the watermark.
`read_back` and `read_all_back` are the newest-first duals, taking a `before` upper bound, so
`Position::MAX` starts at the tip.

`after` (exclusive) and a `limit` compose into a stateless pagination cursor:

```rust,ignore
let mut cursor = Position::ZERO;
loop {
    let (page, _watermark) = client.read_all(query.clone(), cursor, Some(100))?;
    let Some(last) = page.last() else { break };
    for seq in &page {
        handle(seq);
    }
    cursor = last.position(); // next page resumes here, no gap or duplicate
}
```

## Subscriptions

`subscribe` catches up on matching events, then tails new ones live, yielding a `CaughtUp` marker
each time it reaches the live edge.

```rust,ignore
use tephra_client::SubEvent;

let (mut stream, cancel) = client.subscribe(query, Position::ZERO)?;
for item in &mut stream {
    match item? {
        SubEvent::Event(seq) => handle(seq),
        SubEvent::CaughtUp(_) => { /* reached the live edge */ }
    }
}
cancel.cancel();
```

A subscription does not end on its own: drop the stream, or call `cancel` on the paired
`SubscribeCancel`.

## Async client

With the `async` feature, `AsyncClient` multiplexes many concurrent requests over one control socket
plus a pool of bulk read sockets. Its methods take `&self`, so a single client drives concurrent work
on a Tokio runtime.

```rust,ignore
use tephra_client::{AsyncClient, Event, Position, Query};

let client = AsyncClient::connect("127.0.0.1:9000").await?;

// Both futures borrow the same client; the requests are multiplexed on one connection.
let (a, b) = tokio::join!(
    client.append([Event::new("A", ["k:1"], b"{}".to_vec())?], None),
    client.append([Event::new("B", ["k:2"], b"{}".to_vec())?], None),
);
a?;
b?;

let (events, _watermark) = client.read_all(Query::all(), Position::ZERO, None).await?;
```

## TLS

With the `tls` feature, `Client::connect_tls` verifies the server certificate (TLS 1.3,
server-authenticated). Build the config from the system roots, or from a custom CA for a self-signed
certificate.

```rust,ignore
use tephra_client::{Client, tls};

// Verify against the system roots (a public CA):
let config = tls::config_with_native_roots()?;
let mut client = Client::connect_tls("tephra.example.com:9000", "tephra.example.com", config)?;

// Or trust a private CA for a self-signed certificate:
let config = tls::config_with_custom_ca("ca.pem".as_ref())?;
let mut client = Client::connect_tls("tephra.internal:9000", "tephra.internal", config)?;
```

## Authentication

When the server requires a bearer token, pass it to a `*_with` connect variant, so a rejected token
fails the connect rather than the first request. Pair it with TLS so the token does not cross an
unencrypted hop.

```rust,ignore
// Blocking, over TLS:
let config = tls::config_with_native_roots()?;
let mut client = Client::connect_tls_with(
    "tephra.example.com:9000",
    "tephra.example.com",
    config,
    Some("a-long-random-secret"),
)?;
```

The async client carries the token on `AsyncClientConfig::auth_token`, and every socket in its
control-plus-bulk pool authenticates independently.

## Errors

A call returns `ClientError` on failure. The `Server` variant carries the wire `code`, a `message`,
a `retryable` flag (set for an advisory same-batch append conflict), and a `conflict_position` for a
durable one; `Protocol`, `UnexpectedEof`, and `Frame` cover transport and framing failures. The
client does no automatic retries or reconnection.

## Related crates

- [`tephra-server`](https://crates.io/crates/tephra-server): the server this client connects to.
- [`tephra-types`](https://crates.io/crates/tephra-types): the shared vocabulary.
- [`tephra`](https://crates.io/crates/tephra): the embedded engine, if you do not need the network.

Clients for other languages: [tephra-go](https://github.com/tephradb/tephra-go) and, for JavaScript,
[@tephradb/client](https://github.com/tephradb/tephra-js).

## License

Licensed under the [Apache License, Version 2.0](https://github.com/tephradb/tephra/blob/main/LICENSE).
