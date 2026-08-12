# Tephra API inventory

An exact, source-verified inventory of the public API surface. Every signature, field,
default, and error variant below is copied verbatim from the Rust source. Paths are
absolute; line numbers are given where useful. Nothing here is inferred: where a thing
could not be found, it is called out in "Notes / awkward API surface".

Workspace crates (from `/home/ari/dev/tqwewe/dcbdb/Cargo.toml` and the tree):

- `crates/tephra-types` (the shared vocabulary)
- `crates/tephra` (the engine, package name `tephra`)
- `crates/tephra-proto` (wire protobuf + framing + conversions)
- `crates/tephra-client` (the sync TCP client)
- `crates/tephra-server` (the sync TCP server)
- `crates/seglog` (the low-level segmented record log)

---

## 1. Public types (vocabulary)

### `Position`
File: `/home/ari/dev/tqwewe/dcbdb/crates/tephra-types/src/position.rs`

```rust
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct Position(pub(crate) u64);      // line 3-4
```

Associated const and methods:

```rust
pub const ZERO: Position = Position(0);   // line 7
pub fn new(n: u64) -> Self                 // line 9
pub fn get(self) -> u64                    // line 13
pub fn next(self) -> Position              // line 17
pub fn offset_from(self, base: Position) -> u64   // line 21
```

Trait impls: `From<u64>`; `Add<Position>/Add<u64>` (both `Output = u64`),
`Add<Position> for u64`; `Sub<Position>/Sub<u64>` (both `Output = u64`),
`Sub<Position> for u64`; `Display`. (Note: `Position + Position` returns `u64`, not
`Position`; see notes.)

### `EventType`
File: `/home/ari/dev/tqwewe/dcbdb/crates/tephra-types/src/name.rs`

```rust
#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct EventType(Box<str>);            // line 55-56

pub fn new(s: impl AsRef<str>) -> Result<Self, NameError>   // line 60
pub fn as_str(&self) -> &str                                // line 66
```

Trait impls: `AsRef<str>`, `Display`. Deliberately no `Deref`.

### `Tag`
File: `/home/ari/dev/tqwewe/dcbdb/crates/tephra-types/src/name.rs`

```rust
#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct Tag(Box<str>);                  // line 86-87

pub fn new(s: impl AsRef<str>) -> Result<Self, NameError>   // line 91
pub fn as_str(&self) -> &str                                // line 97
```

Trait impls: `AsRef<str>`, `Display`. No `Deref`.

### `Tags`
File: `/home/ari/dev/tqwewe/dcbdb/crates/tephra-types/src/name.rs`

```rust
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Tags(SmallVec<[Tag; 4]>);       // line 136-137

pub fn new(tags: impl Into<SmallVec<[Tag; 4]>>) -> Result<Self, TagsError>   // line 141
pub fn empty() -> Self                     // line 156
pub fn as_slice(&self) -> &[Tag]           // line 160
pub fn len(&self) -> usize                 // line 164
pub fn is_empty(&self) -> bool             // line 168
pub fn iter(&self) -> std::slice::Iter<'_, Tag>   // line 172
```

### `MAX_NAME_LEN`
File: `/home/ari/dev/tqwewe/dcbdb/crates/tephra-types/src/name.rs`, line 16

```rust
pub const MAX_NAME_LEN: usize = u16::MAX as usize;
```

### `QueryItem`
File: `/home/ari/dev/tqwewe/dcbdb/crates/tephra-types/src/query.rs`

```rust
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct QueryItem {                     // line 30-37
    pub types: Vec<EventType>,             // empty means "any type"
    pub tags: Tags,                        // event must contain ALL of these
}

pub fn new(types: Vec<EventType>, tags: Tags) -> Self     // line 41
pub fn of_types(types: Vec<EventType>) -> Self            // line 46
pub fn with_tags(tags: Tags) -> Self                      // line 54
```

### `Query`
File: `/home/ari/dev/tqwewe/dcbdb/crates/tephra-types/src/query.rs`

```rust
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Query {                           // line 68-76
    All,                                   // matches every event
    Items(Vec<QueryItem>),                 // OR across items; empty matches nothing
}

pub fn all() -> Self                                          // line 80
pub fn items(items: impl Into<Vec<QueryItem>>) -> Self        // line 85
pub fn item(item: QueryItem) -> Self                          // line 90
```

### `AppendCondition`
File: `/home/ari/dev/tqwewe/dcbdb/crates/tephra-types/src/query.rs`

```rust
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AppendCondition {               // line 108-115
    pub fail_if_events_match: Query,
    pub after: Position,                   // Position::ZERO means "the whole log"
}

pub fn new(fail_if_events_match: Query) -> Self   // line 120 (after = Position::ZERO)
pub fn after(mut self, after: Position) -> Self   // line 128 (builder)
```

### `Event` (engine, the packed codec)
File: `/home/ari/dev/tqwewe/dcbdb/crates/tephra/src/event.rs`

```rust
#[derive(Clone, PartialEq, Eq)]
pub struct Event {                         // line 85-93 (fields private: buf, data_offset)
}

pub fn new(event_type: &EventType, tags: &Tags, payload: &[u8]) -> Result<Self, EncodeError>  // line 97
pub fn as_ref(&self) -> EventRef<'_>       // line 143
pub fn event_type(&self) -> &str           // line 150
pub fn tags(&self) -> TagsRef<'_>          // line 154
pub fn data(&self) -> &[u8]                // line 159  (the payload)
pub fn as_bytes(&self) -> &[u8]            // line 164  (the whole encoded record)
```

Trait impls: `Clone`, `PartialEq`, `Eq`, `Debug` (via `EventRef`).

### `EventRef<'a>` (engine, zero-copy borrowed event)
File: `/home/ari/dev/tqwewe/dcbdb/crates/tephra/src/event.rs`

```rust
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct EventRef<'a> {                  // line 180-184 (fields private: buf, data_offset)
}

pub fn from_bytes(buf: &'a [u8]) -> Result<Self, DecodeError>   // line 192
pub fn event_type(&self) -> &'a str        // line 253
pub fn tags(&self) -> TagsRef<'a>          // line 257
pub fn data(&self) -> &'a [u8]             // line 262  (the payload)
pub fn as_bytes(&self) -> &'a [u8]         // line 267
pub fn to_owned(&self) -> Event            // line 272
```

### `TagsRef<'a>` (engine, borrowed tag iterator)
File: `/home/ari/dev/tqwewe/dcbdb/crates/tephra/src/event.rs`, line 340-345

```rust
#[derive(Clone, Copy)]
pub struct TagsRef<'a> { /* private fields */ }
```

Impls `Iterator<Item = &'a str>` and `ExactSizeIterator`.

### `Matches` (engine, the match predicate)
File: `/home/ari/dev/tqwewe/dcbdb/crates/tephra/src/query.rs`, line 26-29

```rust
pub trait Matches {
    fn matches(&self, event: EventRef<'_>) -> bool;
}
```

Implemented for `QueryItem` (line 31) and `Query` (line 40).

### Engine re-exports
File: `/home/ari/dev/tqwewe/dcbdb/crates/tephra/src/lib.rs`, lines 8-16

```rust
pub use tephra_types::{
    AppendCondition, EventType, MAX_NAME_LEN, NameError, Position, Query, QueryItem, Tag, Tags,
    TagsError,
};
pub use event::{Event, EventRef};
pub use log::set::PositionRange;
pub use query::Matches;
pub use read::{ReadConfig, ReadError, ReadHandle, Subscription, WaitOutcome};
pub use writer::{AppendError, ConflictSite, WriteCoordinator, WriteHandle, WriterConfig};
```

Note: `EncodeError`, `DecodeError`, and `TagsRef` are public in `event.rs` but are NOT
re-exported at the crate root; reach them via `tephra::event::...`.

---

## 2. Method signatures (the API a user touches)

### `WriteCoordinator`
File: `/home/ari/dev/tqwewe/dcbdb/crates/tephra/src/writer/coordinator.rs`

```rust
pub struct WriteCoordinator { /* private */ }            // line 20-25

pub fn start(
    set: SegmentSet,
    cfg: WriterConfig,
) -> Result<(WriteCoordinator, WriteHandle), IndexError>  // line 35-38

pub fn read_handle(&self) -> ReadHandle                   // line 86
pub fn shutdown(mut self) -> SegmentSet                   // line 93
```

`Drop` for `WriteCoordinator` signals shutdown and joins (line 103).

### `WriteHandle`
File: `/home/ari/dev/tqwewe/dcbdb/crates/tephra/src/writer/handle.rs`

```rust
#[derive(Clone)]
pub struct WriteHandle { /* private */ }                  // line 20-24

pub fn append(
    &self,
    events: Vec<Event>,
    condition: Option<AppendCondition>,
) -> Result<PositionRange, AppendError>                   // line 39-43

pub fn read(&self, query: Query, after: Position, limit: Option<u64>) -> Reads             // line 67
pub fn subscribe(&self, query: Query, after: Position) -> Subscription // line 76
pub fn reader(&self) -> ReadHandle                                     // line 82

// Only under the `async` cargo feature:
pub async fn append_async(
    &self,
    events: Vec<Event>,
    condition: Option<AppendCondition>,
) -> Result<PositionRange, AppendError>                   // line 90-94 (#[cfg(feature = "async")])
```

`append` returns `Err(AppendError::Empty)` immediately if `events.is_empty()` (line 44).

### `ReadHandle`
File: `/home/ari/dev/tqwewe/dcbdb/crates/tephra/src/read/mod.rs`

```rust
#[derive(Clone)]
pub struct ReadHandle { /* private */ }                   // line 341-345

pub fn read(&self, query: Query, after: Position, limit: Option<u64>) -> Reads              // line 355
pub fn subscribe(&self, query: Query, after: Position) -> Subscription  // line 363
```

### `Reads` (lending iterator over a read)
File: `/home/ari/dev/tqwewe/dcbdb/crates/tephra/src/read/mod.rs`

```rust
pub struct Reads { /* private */ }                        // line 395-399

pub fn watermark(&self) -> Position                       // line 450
pub fn next(&mut self) -> Option<Result<Sequenced<'_>, ReadError>>   // line 538 (NOT std::Iterator)
pub fn collect_owned(mut self) -> Result<Vec<(Position, crate::event::Event)>, ReadError>  // line 638
```

### `Sequenced<'a>` (one yielded read item)
File: `/home/ari/dev/tqwewe/dcbdb/crates/tephra/src/read/mod.rs`, line 370-374

```rust
#[derive(Clone, Copy, Debug)]
pub struct Sequenced<'a> {
    pub position: Position,
    pub event: EventRef<'a>,
}
```

Note: `Sequenced` and `Reads` are public types but are NOT re-exported at the crate root
(only `ReadHandle`, `ReadConfig`, `ReadError`, `Subscription`, `WaitOutcome` are). Reach
them via `tephra::read::{Reads, Sequenced}`.

### `Subscription`
File: `/home/ari/dev/tqwewe/dcbdb/crates/tephra/src/read/subscribe.rs`

```rust
pub struct Subscription { /* private */ }                 // line 40-48

pub fn with_max_batch_events(mut self, max_batch_events: usize) -> Subscription  // line 70
pub fn position(&self) -> Position                                               // line 78
pub fn poll_batch(&mut self) -> Result<Vec<(Position, Event)>, ReadError>        // line 92 (non-blocking)
pub fn wait(&self) -> bool                                                        // line 133 (blocks; false = shut down)
pub fn wait_timeout(&self, timeout: Duration) -> WaitOutcome                      // line 142
pub fn next_batch(&mut self) -> Option<Result<Vec<(Position, Event)>, ReadError>>  // line 153 (blocking)
```

Related const:

```rust
pub const DEFAULT_MAX_BATCH_EVENTS: usize = 1024;   // subscribe.rs line 33
```

### `WaitOutcome`
File: `/home/ari/dev/tqwewe/dcbdb/crates/tephra/src/read/mod.rs`, line 188-196

```rust
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WaitOutcome {
    Advanced,
    TimedOut,
    Closed,
}
```

### Condition evaluation result
File: `/home/ari/dev/tqwewe/dcbdb/crates/tephra/src/writer/condition.rs`

```rust
pub fn evaluate(
    cond: &AppendCondition,
    main: &TagTips,
    staged: &StagedTips,
    index: &IndexSet,
    set: &SegmentSet,
    verify: bool,
    force_scan: bool,
) -> Result<Option<ConflictSite>, AppendError>    // line 38-46
```

`Ok(None)` means "no conflict, the append may proceed"; `Ok(Some(_))` is a conflict (not
an error); `Err(_)` is an integrity failure. This function is in the (private) `condition`
module, not re-exported. The user-visible surface is `AppendError::Conflict { at:
ConflictSite }` returned from `append`.

Internal verdict type (module `writer::tips`, not public API):
File: `/home/ari/dev/tqwewe/dcbdb/crates/tephra/src/writer/tips.rs`, line 44-48

```rust
pub enum Verdict {
    DefinitelyNoMatch,
    Unknown,
}
```

`may_match(&self, query: &Query, after: Position) -> Verdict` at tips.rs line 111.
(`Verdict`, `TagTips`, `StagedTips` are `pub` within the crate's private `writer::tips`
module but are not re-exported at the crate root, so they are not part of the external
API.)

### `SegmentSet` (layer 1, used to open a store before `WriteCoordinator::start`)
File: `/home/ari/dev/tqwewe/dcbdb/crates/tephra/src/log/set.rs`

```rust
pub fn open(dir: impl AsRef<Path>, config: SegmentConfig) -> Result<Self, LogError>  // line 320
pub fn append_batch(&mut self, records: &[&[u8]]) -> Result<PositionRange, LogError> // line 500
pub fn read_at(&self, pos: Position) -> Result<Record, LogError>                     // line 646
pub fn scan_from(&self, pos: Position) -> Scan<&SegmentSet>                          // line 682
pub fn scan_after(&self, pos: Position) -> Scan<&SegmentSet>                         // line 691
pub fn last_position(&self) -> Position                                             // line 703
pub fn next_position(&self) -> Position                                             // line 710
pub fn segment_capacity(&self) -> usize                                             // line 716
pub fn max_record_len(&self) -> usize                                               // line 721
pub fn sealed_len(&self) -> usize                                                   // line 726
pub fn sealed_arcs(&self) -> &[Arc<Segment>]                                        // line 732
pub fn active_arc(&self) -> Arc<Segment>                                            // line 739
pub fn dir(&self) -> &Path                                                          // line 745
pub fn active_base(&self) -> Position                                               // line 751
pub fn sealed_segments(&self) -> impl Iterator<Item = (Position, u64)> + '_         // line 758
pub fn segment_for(&self, pos: Position) -> Option<&Arc<Segment>>                   // line 767
```

Note: `SegmentSet` itself is NOT re-exported at the crate root; reach it via
`tephra::log::set::SegmentSet`. `WriteCoordinator::start` takes it by value.

### `PositionRange`
File: `/home/ari/dev/tqwewe/dcbdb/crates/tephra/src/log/set.rs`, line 283-294

```rust
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PositionRange {
    pub first: Position,
    pub last: Position,
}

pub fn count(&self) -> u64                 // line 291
```

---

## 3. The client (`tephra-client`)

File: `/home/ari/dev/tqwewe/dcbdb/crates/tephra-client/src/lib.rs`

### Friendly owned `Event` (client-side, distinct from the engine's packed `Event`)

```rust
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Event {                         // line 60-65 (fields private)
    // event_type: EventType, tags: Tags, payload: Vec<u8>
}

pub fn new(
    event_type: impl AsRef<str>,
    tags: &[&str],
    payload: impl Into<Vec<u8>>,
) -> Result<Event, BuildError>             // line 70-74

pub fn event_type(&self) -> &str                                    // line 89
pub fn tags(&self) -> impl ExactSizeIterator<Item = &str>           // line 94
pub fn payload(&self) -> &[u8]                                      // line 99
```

### `SequencedEvent`

```rust
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SequencedEvent { /* private */ }   // line 105-109

pub fn position(&self) -> Position         // line 113
pub fn event(&self) -> &Event              // line 118
pub fn into_event(self) -> Event           // line 123
```

### `AppendResult`

```rust
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AppendResult {                  // line 129-135
    pub first: Position,
    pub last: Position,
}
```

### `SubEvent`

```rust
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SubEvent {                        // line 138-146
    Event(SequencedEvent),
    CaughtUp(Position),
}
```

### `Client`

```rust
pub struct Client { /* private */ }        // line 236-242

pub fn connect(addr: impl ToSocketAddrs) -> io::Result<Client>              // line 246
pub fn set_max_frame_len(&mut self, max_frame_len: u32)                     // line 262
pub fn append(
    &mut self,
    events: impl IntoIterator<Item = Event>,
    condition: Option<AppendCondition>,
) -> Result<AppendResult, ClientError>                                      // line 280-284
pub fn read(&mut self, query: Query, after: Position, limit: Option<u64>) -> Result<ReadStream<'_>, ClientError>   // line 314
pub fn read_all(
    &mut self,
    query: Query,
    after: Position,
    limit: Option<u64>,
) -> Result<(Vec<SequencedEvent>, Position), ClientError>                   // line 336-340
pub fn subscribe(
    &mut self,
    query: Query,
    after: Position,
) -> Result<(SubscribeStream<'_>, SubscribeCancel), ClientError>            // line 362-366
```

### `ReadStream<'a>`

```rust
pub struct ReadStream<'a> { /* private */ }   // line 505-512

pub fn watermark(&self) -> Option<Position>   // line 516 (available once the stream ends)
// impl Iterator<Item = Result<SequencedEvent, ClientError>>   // line 584
```

### `SubscribeStream<'a>`

```rust
pub struct SubscribeStream<'a> { /* private */ }   // line 418-424
// impl Iterator<Item = Result<SubEvent, ClientError>>   // line 478
```

### `SubscribeCancel`

```rust
pub struct SubscribeCancel { /* private */ }   // line 403-405

pub fn cancel(self)                            // line 410 (shuts the socket down)
```

### Client re-exports (line 36-44)

```rust
pub use tephra_types::{
    AppendCondition, EventType, NameError, Position, Query, QueryItem, Tag, Tags, TagsError,
};
pub use tephra_proto::convert::ErrorCode;
pub use tephra_proto::tephra as proto;    // raw wire types, an escape hatch
```

---

## 4. Config structs, fields, and defaults

### `WriterConfig`
File: `/home/ari/dev/tqwewe/dcbdb/crates/tephra/src/writer/mod.rs`, line 36-61 (fields),
63-75 (defaults)

```rust
#[derive(Clone, Copy, Debug)]
pub struct WriterConfig {
    pub queue_capacity: usize,
    pub max_batch_records: usize,
    pub max_batch_bytes: usize,
    pub tips_window: u64,
    pub verify_tips: bool,
    pub condition_force_scan: bool,
    pub read: ReadConfig,
}

impl Default for WriterConfig {
    fn default() -> Self {
        WriterConfig {
            queue_capacity: 1024,
            max_batch_records: 1024,
            max_batch_bytes: 8 * 1024 * 1024,   // 8 MiB
            tips_window: 1_000_000,
            verify_tips: false,
            condition_force_scan: false,
            read: ReadConfig::default(),
        }
    }
}
```

### `ReadConfig`
File: `/home/ari/dev/tqwewe/dcbdb/crates/tephra/src/read/mod.rs`, line 70-78 (field),
80-87 (default)

```rust
#[derive(Clone, Copy, Debug)]
pub struct ReadConfig {
    pub scan_bias: u32,
}

impl Default for ReadConfig {
    fn default() -> Self {
        ReadConfig { scan_bias: 4 }
    }
}
```

### `SegmentConfig`
File: `/home/ari/dev/tqwewe/dcbdb/crates/tephra/src/log/set.rs`, line 58-66 (fields),
68-80 (`new` constructor)

```rust
#[derive(Clone, Copy, Debug)]
pub struct SegmentConfig {
    pub segment_size: usize,     // total size of each segment file in bytes (incl. header)
    pub max_record_len: usize,   // largest total on-disk record length
    pub header_size: usize,      // bytes reserved for the SegmentHeader
}

pub fn new(segment_size: usize) -> Self {
    SegmentConfig {
        segment_size,
        max_record_len: segment_size / 4,
        header_size: SEGMENT_HEADER_SIZE,        // = 64 (the segment header size)
    }
}

pub fn validate(&self) -> Result<(), LogError>   // line 86
```

Important: `SegmentConfig` has NO `Default` impl. There is no `SegmentConfig::segment_size`
default at the engine layer; the only defaulting of a segment size lives in the server
settings (below). `max_record_len` defaults to `segment_size / 4` and `header_size` to
`SEGMENT_HEADER_SIZE` (64), but only via `SegmentConfig::new`.

### Related public log constants
File: `/home/ari/dev/tqwewe/dcbdb/crates/tephra/src/log/set.rs`

```rust
pub const RECORD_OVERHEAD: usize = RECORD_HEAD_SIZE;                          // line 51
pub const BATCH_OVERHEAD: usize = RECORD_HEAD_SIZE + COMMIT_MARKER_PAYLOAD;   // line 55
```

### Server settings (where the 256 MiB default lives)
File: `/home/ari/dev/tqwewe/dcbdb/crates/tephra-server/src/settings.rs`

`Settings` default (line 65-77):

```rust
bind: "127.0.0.1:9000".to_string(),
data_dir: "tephra-data".to_string(),
log: None,
segment: SegmentSettings::default(),
writer: WriterSettings::default(),
read: ReadSettings::default(),
server: ServerSettings::default(),
```

`SegmentSettings` (line 80-93):

```rust
pub struct SegmentSettings {
    pub size: usize,
}
impl Default for SegmentSettings {
    fn default() -> Self {
        SegmentSettings {
            size: 256 * 1024 * 1024,     // 256 MiB, the committed default segment size
        }
    }
}
```

`WriterSettings` (line 96-124):

```rust
pub struct WriterSettings {
    pub queue_capacity: usize,
    pub max_batch_records: usize,
    pub max_batch_bytes: usize,
    pub tips_window: u64,
    pub condition_force_scan: bool,
}
impl Default for WriterSettings {
    fn default() -> Self {
        WriterSettings {
            queue_capacity: 1024,
            max_batch_records: 1024,
            max_batch_bytes: 8 * 1024 * 1024,   // 8 MiB
            tips_window: 1_000_000,
            condition_force_scan: false,
        }
    }
}
```

Note: `WriterSettings` deliberately does NOT expose `verify_tips` (comment at
settings.rs line 186-187: it is never operator-settable and stays `false`).

`ReadSettings` (line 127-140):

```rust
pub struct ReadSettings {
    pub scan_bias: u32,
}
impl Default for ReadSettings {
    fn default() -> Self {
        ReadSettings { scan_bias: 4 }
    }
}
```

`ServerSettings` (line 145-178):

```rust
pub struct ServerSettings {
    pub max_frame_len: u32,
    pub read_batch_events: usize,
    pub read_batch_bytes: usize,
    pub subscribe_wait_tick_ms: u64,
    pub max_inflight_requests_per_conn: usize,
    pub max_concurrent_subscriptions: usize,
    pub read_worker_threads: usize,
    pub frame_queue_depth: usize,
    pub keepalive_idle_secs: u64,
    pub keepalive_interval_secs: u64,
}
impl Default for ServerSettings {
    fn default() -> Self {
        ServerSettings {
            max_frame_len: DEFAULT_MAX_FRAME_LEN,   // = 16 * 1024 * 1024 (16 MiB)
            read_batch_events: 1024,
            read_batch_bytes: 512 * 1024,           // 512 KiB
            subscribe_wait_tick_ms: 250,
            max_inflight_requests_per_conn: 256,
            max_concurrent_subscriptions: 64,
            read_worker_threads: 0,                  // 0 = one worker per logical CPU
            frame_queue_depth: 256,
            keepalive_idle_secs: 60,
            keepalive_interval_secs: 15,
        }
    }
}
```

### `DEFAULT_MAX_FRAME_LEN`
File: `/home/ari/dev/tqwewe/dcbdb/crates/tephra-proto/src/framing.rs`, line 12

```rust
pub const DEFAULT_MAX_FRAME_LEN: u32 = 16 * 1024 * 1024;   // 16 MiB
```

The client also defaults its `max_frame_len` to `DEFAULT_MAX_FRAME_LEN`
(`tephra-client/src/lib.rs` line 256).

---

## 5. Error enums (every variant)

### `NameError`
File: `/home/ari/dev/tqwewe/dcbdb/crates/tephra-types/src/name.rs`, line 23-33

```rust
#[derive(Debug, Error, PartialEq, Eq)]
pub enum NameError {
    Empty { what: &'static str },
    TooLong { what: &'static str, len: usize, max: usize },
}
```

### `TagsError`
File: `/home/ari/dev/tqwewe/dcbdb/crates/tephra-types/src/name.rs`, line 119-123

```rust
#[derive(Debug, Error, PartialEq, Eq)]
pub enum TagsError {
    Duplicate { tag: Tag },
}
```

### `EncodeError` (engine codec)
File: `/home/ari/dev/tqwewe/dcbdb/crates/tephra/src/event.rs`, line 51-57

```rust
#[derive(Debug, Error, PartialEq, Eq)]
pub enum EncodeError {
    TooManyTags { count: usize, max: usize },
    TooLarge { size: u64, max: u64 },
}
```

### `DecodeError` (engine codec)
File: `/home/ari/dev/tqwewe/dcbdb/crates/tephra/src/event.rs`, line 60-74

```rust
#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum DecodeError {
    Truncated,
    TooLarge,
    EmptyType,
    EmptyTag,
    InvalidUtf8,
    TagsNotSorted,
}
```

### `ConflictSite`
File: `/home/ari/dev/tqwewe/dcbdb/crates/tephra/src/writer/mod.rs`, line 78-89

```rust
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConflictSite {
    Durable(Position),   // real, durable conflict; terminal until the client rebuilds
    SameBatch,           // conservative same-drain rejection; advisory and RETRYABLE
}
```

### `AppendError`
File: `/home/ari/dev/tqwewe/dcbdb/crates/tephra/src/writer/mod.rs`, line 92-119

```rust
#[derive(Clone, Debug, Error)]
pub enum AppendError {
    Conflict { at: ConflictSite },
    AfterBeyondTip { after: Position, tip: Position },
    Empty,
    TooLarge { size: usize },
    Log(Arc<LogError>),
    Corrupt(DecodeError),
    Shutdown,
}
```

### `LogError`
File: `/home/ari/dev/tqwewe/dcbdb/crates/tephra/src/log/set.rs`, line 1036-1098

```rust
#[derive(Debug, Error)]
pub enum LogError {
    InvalidConfig { reason: String },
    Io { path: PathBuf, source: io::Error },
    Header { path: PathBuf, source: HeaderError },
    BasePositionMismatch { path: PathBuf, header: Position, name: Position },
    UnwrittenNonLast { path: PathBuf },
    NonContiguous { path: PathBuf, found: Position, expected: Position },
    PositionMismatch { path: PathBuf, found: Position, expected: Position },
    RecordTooLarge { size: usize, max: usize },
    BatchTooLarge { size: usize, capacity: usize },
    EmptyBatch,
    EmptyRecord,
    NotFound { position: Position },
    Write { path: PathBuf, source: WriteError },
    Read { path: PathBuf, source: ReadError },
}
```

(`HeaderError`, `WriteError`, `ReadError` in the `Header`/`Write`/`Read` variants are the
`seglog` crate's types; `ReadError` here is `seglog::read::ReadError`, NOT the engine's
`read::ReadError` below.)

### `ReadError` (engine read path)
File: `/home/ari/dev/tqwewe/dcbdb/crates/tephra/src/read/mod.rs`, line 378-384

```rust
#[derive(Debug, Error)]
pub enum ReadError {
    Log(Arc<LogError>),
    Corrupt(DecodeError),
}
```

### `IndexError`
File: `/home/ari/dev/tqwewe/dcbdb/crates/tephra/src/index/set.rs`, line 443-457

```rust
#[derive(Debug, Error)]
pub enum IndexError {
    Io { path: PathBuf, source: io::Error },
    Log(Arc<LogError>),
    Corrupt(DecodeError),
    Unindexable { range: PositionRange },
}
```

`IndexError` is re-exported from `crate::index` (`index/mod.rs` line 39) but NOT at the
crate root. `WriteCoordinator::start` returns `Result<_, IndexError>`.

### `TooManyTypes` (index, degraded-segment marker)
File: `/home/ari/dev/tqwewe/dcbdb/crates/tephra/src/index/tail.rs`, line 326-330

```rust
#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
#[error("too many distinct event types in one segment (maximum {max})")]
pub struct TooManyTypes {
    pub max: usize,
}
```

Returned by `ActiveTail::push` (`tail.rs` line 102). Re-exported from `crate::index`
(`index/mod.rs` line 40), not at the crate root.

### `ErrorCode` (wire error taxonomy, used by the client)
File: `/home/ari/dev/tqwewe/dcbdb/crates/tephra-proto/src/convert.rs`, line 122-131

```rust
pub enum ErrorCode {
    Conflict,
    AfterBeyondTip,
    Empty,
    TooLarge,
    BadRequest,
    Internal,
    Shutdown,
    Unknown,      // absorbs the unspecified wire value and any unrecognised code
}
```

### `BuildError` (client)
File: `/home/ari/dev/tqwewe/dcbdb/crates/tephra-client/src/lib.rs`, line 153-159

```rust
#[derive(Debug, PartialEq, Eq)]
pub enum BuildError {
    Name(NameError),
    Tags(TagsError),
}
```

### `ClientError`
File: `/home/ari/dev/tqwewe/dcbdb/crates/tephra-client/src/lib.rs`, line 173-190

```rust
#[derive(Debug)]
pub enum ClientError {
    Frame(FrameError),
    UnexpectedEof,
    Protocol(String),
    Server {
        code: ErrorCode,
        message: String,
        retryable: bool,
        conflict_position: Option<Position>,
    },
}
```

(`FrameError` is `tephra_proto`'s framing error, re-exported through the proto crate.)

---

## 6. Benchmark figures

There are NO committed concrete benchmark numbers anywhere in the repository. The
benchmark files define Criterion harnesses only; they contain no recorded results, and
no results file (JSON, txt, or Criterion output) is checked into git. README.md and
ROADMAP.md contain no throughput or latency figures either.

Bench files (all under `/home/ari/dev/tqwewe/dcbdb/crates/tephra/benches/`):

- `write_path.rs`: groups `append_latency`, `batch_size`, `payload_size`, `group_commit`,
  `conditional_append`. Reported as appends/sec, bytes/sec, aggregate throughput.
- `read_path.rs`: measures the index-vs-scan crossover (both arms over the same store).
- `condition_path.rs`: measures the append-condition check cost (index existence check
  vs linear log decode), no append, no fsync.

Also `/home/ari/dev/tqwewe/dcbdb/crates/seglog/benches/seglog_bench.rs`.

Caveats stated in the bench source (attach these to any number a run produces):

- write_path (module doc, lines ~24-40): "The fsync caveat (read this before trusting a
  number)". A `TempDir` under `/tmp` is very often a tmpfs (RAM) mount where fsync is
  effectively a no-op, so latencies look 10x to 100x better than any real disk. tmpfs
  numbers measure the coordinator's CPU and coalescing, not the durability ceiling. Point
  `TEPHRA_BENCH_DIR` at real storage to measure the fsync ceiling.
- read_path (lines ~34-44): "The cache caveat". Default `TempDir` is usually tmpfs where a
  random read is as cheap as a sequential one, so the crossover looks far flatter than on
  disk, and runs are effectively hot (data stays resident across iterations). Use
  `TEPHRA_BENCH_DIR` for a fair picture.
- condition_path (lines ~33-40): "The cache caveat". On a warm tmpfs cache both arms are
  CPU-bound, so numbers measure decode/probe cost, useful for regressions but not a fair
  durability comparison.
- compare (lines ~26-42): tephra group-commits under one fsync and feeds the index inline;
  UmaDB commits inline with two fsyncs. The same fsync caveat applies: run on real storage
  via `TEPHRA_BENCH_DIR`, not a tmpfs, or the fsync ceiling is hidden. The single-threaded
  caller sees the coalescing win only under concurrency.

Every bench honours the `TEPHRA_BENCH_DIR` env var to run against real storage. No
hardware is named anywhere, because no results are recorded.

---

## Notes / awkward API surface

- Two distinct types named `Event`. The engine's `tephra::Event` is the packed on-disk
  codec (constructed with `&EventType`, `&Tags`, `&[u8]`, returns `EncodeError`). The
  client's `tephra_client::Event` is a separate friendly owned type (constructed with
  `impl AsRef<str>`, `&[&str]`, `impl Into<Vec<u8>>`, returns `BuildError`). They do not
  interconvert directly; the client type serialises to wire protobuf.

- Two distinct types named `ReadError`. `tephra::read::ReadError` (the engine read path,
  variants `Log` / `Corrupt`) and `seglog::read::ReadError` (surfaced inside
  `LogError::Read { source }`). Do not conflate them.

- `Reads::next` is NOT `std::iter::Iterator::next`: `Reads` is a lending iterator
  (`fn next(&mut self) -> Option<Result<Sequenced<'_>, ReadError>>`). Consume with
  `while let Some(item) = reads.next()`, not a `for` loop. `#[allow(clippy::should_implement_trait)]`
  is on it in source.

- Several public types are reachable only through submodules, not the crate root:
  `SegmentSet` (`tephra::log::set::SegmentSet`), `SegmentConfig`
  (`tephra::log::set::SegmentConfig`), `Reads` and `Sequenced` (`tephra::read::...`),
  `IndexError`, `IndexSet`, `TooManyTypes` (`tephra::index::...`), and the codec errors
  `EncodeError`/`DecodeError`/`TagsRef` (`tephra::event::...`). The crate root
  (`lib.rs`) re-exports only the subset in section 1. A docs example that opens a store
  needs `SegmentSet` and `SegmentConfig`, which are NOT at the root; this is easy to trip
  over.

- `SegmentConfig` has no `Default` impl, so there is no engine-level "default segment
  size". The 256 MiB figure is a server-settings default only
  (`SegmentSettings::size`). If the docs claim a default segment size for the library,
  that claim has no source in the engine crate; it must be attributed to the server.

- `Position` arithmetic is unusual by design: `Position + Position` and `Position - Position`
  both yield `u64`, not `Position`. Subtraction returning a `u64` count is intentional
  (documented in ARCHITECTURE.md), but `Add<Position> for Position -> u64` is a surprising shape
  worth flagging in prose.

- `ConflictSite::SameBatch` is advisory and retryable; `ConflictSite::Durable(pos)` is
  terminal. The client surfaces this split as `ClientError::Server { retryable,
  conflict_position, .. }`. A caller that collapses the two into a bare position loses the
  retry contract (ARCHITECTURE.md section 6 calls this out explicitly).

- `WriteHandle::append_async` exists only under the `async` cargo feature
  (`async = ["flume/async"]` in `crates/tephra/Cargo.toml`). Document it as feature-gated
  or omit it.

- `evaluate` (the condition core) returns `Result<Option<ConflictSite>, AppendError>`
  where `Ok(Some(_))` is a conflict, not an error, and `Ok(None)` means "may proceed".
  This tri-state is internal (module `writer::condition`, not exported) but is the one
  definition a docs author describing append semantics should mirror.

- No committed benchmark numbers. Any performance claim in the docs must either run the
  benches on named hardware (via `TEPHRA_BENCH_DIR` on real storage, not tmpfs) or state
  that figures are not yet measured. Do not quote a number, because none exists in-tree.
