# tephra

A DCB-compliant, immutable event store with global ordering.

`tephra` stores an append-only, position-addressed log of events. Data is written once and
never updated or deleted: the primary key is a dense, monotonic `u64` (a `Position`)
assigned by a single writer. Everything else (the tag and type indexes) is derived from
the log and rebuildable by replaying it, so there is no write-ahead log for indexes, no
compaction, and no page reuse.

## Dynamic Consistency Boundary (DCB)

Instead of a static consistency boundary baked into an aggregate (one stream per
aggregate), DCB derives the boundary per decision from a query.

- One event stream per bounded context.
- Each event carries a **type** plus a set of **tags** (for example `course:c1`,
  `student:s1`), so one event can belong to several entities at once.
- A **query** is a set of items OR'd together; within an item the type matches one of the
  listed types and the tags must all be present (OR across items, AND within an item).
- An **append condition** guards a write: `fail_if_events_match` plus an optional `after`
  position. The store rejects the append if any event matching the query landed after
  `after`. This is what makes the boundary dynamic: it covers exactly the events the
  decision depended on.

## Workspace crates

All crates live under `crates/`:

| Crate | Purpose |
|---|---|
| `tephra` | The core embedded event store: log, write coordinator, index, read paths. |
| `tephra-client` | A synchronous TCP client for the server. |
| `tephra-server` | A synchronous, thread-per-connection TCP server exposing the store over the wire protocol. |
| `tephra-types` | The shared vocabulary: positions, event type/tag names, and the query model. Pure data, no I/O. |
| `tephra-proto` | The wire protocol: protobuf message types plus length-prefixed framing. No dependency on the engine. |
| `seglog` | The low-level segmented record log (framing, CRCs, batch commit markers, recovery). |

## Running the server

Docker (listens on `0.0.0.0:9000`, data in the `/data` volume):

```sh
docker run -p 9000:9000 -v tephra-data:/data ghcr.io/tqwewe/tephra:latest
```

Each release publishes three image variants: the plain tags (`latest`, `0.1.0`) are a
distroless glibc image, `*-debug` adds a shell for inspection, and `*-static` is a `FROM
scratch` image built around a fully static musl binary (no libc, no loader).

Pre-built binaries (Linux `x86_64` and `aarch64`, glibc or fully static musl) are attached
to each [release](https://github.com/tqwewe/tephra/releases/latest):

```sh
VERSION=0.3.6
TARGET=x86_64-unknown-linux-musl   # fully static, runs on any Linux
BASE="https://github.com/tqwewe/tephra/releases/download/v${VERSION}"
curl -fsSLO "${BASE}/tephra-server-v${VERSION}-${TARGET}.tar.gz"
curl -fsSLO "${BASE}/tephra-server-v${VERSION}-${TARGET}.tar.gz.sha256"
sha256sum -c "tephra-server-v${VERSION}-${TARGET}.tar.gz.sha256"
tar -xzf "tephra-server-v${VERSION}-${TARGET}.tar.gz"
./tephra-server-v${VERSION}-${TARGET}/tephra-server --help
```

The `*-musl` archives are fully static (no libc needed); the `*-gnu` archives link glibc
dynamically. Every archive can be checked against the release's `SHA256SUMS`.

Or with [`cargo binstall`](https://github.com/cargo-bins/cargo-binstall), which pulls the
same prebuilt archive instead of compiling:

```sh
cargo binstall tephra-server
```

Nix:

```sh
nix run github:tqwewe/tephra
```

Cargo:

```sh
cargo install tephra-server
```

## Usage

```rust,no_run
use tephra::{
    AppendCondition, Event, EventType, Position, Query, QueryItem, Tag, Tags,
    WriteCoordinator, WriterConfig,
};
use tephra::log::set::{SegmentConfig, SegmentSet};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Open (or create) a log directory and start the single-writer coordinator.
    let set = SegmentSet::open("./data", SegmentConfig::new(1 << 26))?;
    let (coordinator, handle) = WriteCoordinator::start(set, WriterConfig::default())?;

    // Build an event: a type, a sorted set of tags, and an opaque payload.
    let ty = EventType::new("CourseRegistered")?;
    let tags = Tags::new(vec![Tag::new("course:c1")?, Tag::new("student:s1")?])?;
    let event = Event::new(&ty, &tags, b"{...payload...}")?;

    // Append, guarded so it fails if any event already carries course:c1.
    let guard = AppendCondition::new(Query::item(QueryItem::with_tags(
        Tags::new(vec![Tag::new("course:c1")?])?,
    )));
    let range = handle.append(vec![event], Some(guard))?;
    println!("appended at {}", range.first);

    // Read every event carrying course:c1, ascending, from the beginning.
    let query = Query::item(QueryItem::with_tags(Tags::new(vec![Tag::new("course:c1")?])?));
    let mut reads = handle.read(&query, Position::ZERO, None);
    while let Some(item) = reads.next() {
        let seq = item?;
        println!("{} {}", seq.position, seq.event.event_type());
    }

    // Shutdown joins the writer thread and returns the SegmentSet.
    coordinator.shutdown();
    Ok(())
}
```

Reads run on the caller's own thread over a snapshot the writer publishes at each commit,
so they never touch the writer thread, and a client sees its own writes immediately after
an append returns.

## Documentation

- `ARCHITECTURE.md` is the architecture document: what the system is, why each structural choice
  was made, and which alternatives were rejected.
- `ROADMAP.md` tracks what is done and what is next.

## AI Use

AI was used **heavily** in tephra's development, with careful code reviews. The underlying [seglog](https://crates.io/crates/seglog) crate was authored by [@tqwewe](https://github.com/tqwewe).
