# Architecture

A DCB-compliant, immutable event store with global ordering.

This document is foundational. It records what the system is, why each structural
choice was made, and which alternatives were rejected and for what reason. Read it
before proposing changes to storage, indexing, or the write path. Where it states a
rationale, that rationale is the thing to argue with, not the conclusion alone.

---

## Conventions

**No em dashes anywhere.** Not in code, doc comments, commit messages, `README`s, this
file, or `ROADMAP.md`. Rewrite with a colon, comma, parentheses, or two sentences
instead. This applies to everything written in this repository.

---

## 1. What DCB is

Dynamic Consistency Boundary. Instead of a static consistency boundary baked into an
aggregate (stream-per-aggregate), the boundary is derived per decision from a query.

- One event stream per bounded context.
- Events carry a **type** plus a set of **tags** (`course:c1`, `student:s1`).
- One event can legitimately belong to several entities at once.

This removes the classic "one fact, two events plus a saga" problem: a decision that
spans several entities reads exactly the events it depends on, and guards exactly
those on append.

### Spec surface

A minimally compliant store provides:

- `read(query, options?) -> SequencedEvents`
- `append(events, condition?)`, atomic, fails if any event matches the condition

### Query semantics

A `Query` is a set of `QueryItem`s OR'd together. Within an item, the event type
matches *one* of the listed types, and tags must contain *all* listed tags.
So: **OR across items, AND within an item's tags.** `Query.all()` is a separate
variant.

### Append condition

`failIfEventsMatch: Query` plus optional `after: Position`.

The store ignores everything at or before `after` and rejects if anything matching the
query landed since. `after` is the highest position the client observed while building
its decision model, which may be higher than the position of the last matching event.
Omitting `after` means "fail if *any* event matches" (the uniqueness-guard pattern).

### Decision model

Compose small projections, each with its own type/tag filter, fold them over one read.
The union of their filters **is** the query, and the same query goes into the append
condition. That is what makes the boundary dynamic: it covers exactly the events the
decision actually depended on, nothing more.

---

## 2. Core principles

These drive everything below. If a proposed change violates one, it needs a strong
argument.

**The log is the source of truth. Everything else is derived.**
Indexes need no WAL, no crash-consistent transactional update, and no fsync in the
write path, because they can be rebuilt by replaying the log. This is a large
complexity deletion that general-purpose engines cannot take.

**Data is written once, never updated, never deleted.**
The primary key is a dense monotonic u64 assigned by the writer. This eliminates the
entire reason B-trees and LSMs exist: both are machinery for reconciling mutation
against sorted order, and there is no mutation here.

**Split structures on cardinality, not on being "an index".**
High-cardinality fields (tags) get an inverted index. Low-cardinality fields (types)
get a dense column. Using the same structure for both is the mistake.

**Segments are position-disjoint.**
Both log segments and index segments cover contiguous, non-overlapping position ranges.
This is the structural advantage over every general-purpose engine: an `after: p`
restriction prunes whole segments by header comparison, with no probing.

**Merging is concatenation, not k-way merge.**
Because ranges are disjoint, merging two adjacent index segments is per-term
concatenation. Merge only to amortise term dictionary overhead and reduce open-file
count, never for correctness. This is the thing LSM compaction can never be.

---

## 3. Layer map

| Layer | Responsibility |
|---|---|
| 1. Log | Durable, position-addressed byte storage. `seglog` + `SegmentSet`. |
| 2. Write coordinator | Single writer, position assignment, append conditions, group commit. |
| 3. Index segments | Immutable inverted index (tags) + dense column (types). |
| 4. Query planner | Chooses index vs scan per query item using exact posting lengths. |
| 5. Read paths | Condition check, decision-model read, projection catch-up / subscriptions. |

Layers 1 and 2 are the current build target. 3 onward are designed but not built.

---

## 4. Layer 1: Log

### 4.1 `seglog` record format

```
+-------------+-------------+----------------+
| Length (4B) | CRC32 (4B)  | Data (N bytes) |
+-------------+-------------+----------------+
```

The 4-byte length field carries two flags in its high bits:

```rust
const COMPRESSION_FLAG: u32 = 0x8000_0000;  // bit 31
const CONTROL_FLAG:     u32 = 0x4000_0000;  // bit 30
const FLAG_MASK:        u32 = COMPRESSION_FLAG | CONTROL_FLAG;
const LENGTH_MASK:      u32 = 0x3FFF_FFFF;  // bits 0..=29, 1 GiB ceiling
```

Any bit outside `FLAG_MASK | LENGTH_MASK` is a hard corruption error, never ignored.
Every length extraction uses `raw & LENGTH_MASK`. Never `raw & !COMPRESSION_FLAG`.

**Checksums use CRC-32 (IEEE 802.3 polynomial), via `crc32fast`.** One algorithm across
the record format and the segment header. CRC32C was considered and is marginally
better on small buffers on x86-64, but `crc32fast` won on ubiquity and maintenance;
the difference is negligible against fsync.

**Compression is off by default for the event log.** It costs nothing on small events
and puts a decompress on the path of every random-access fetch after an index lookup.
It earns its keep only on cold archival segments.

### 4.2 Batch commit markers

CRC per record detects a torn *record*. It does not detect a torn *batch*: if records 1
and 2 of a 3-record batch land durably and record 3 does not, records 1 and 2 each
validate individually, and recovery would expose half a transaction as committed.

Fix, in **one** fsync (a header watermark or any write-then-flip scheme costs two, which
doubles commit latency on the operation that dominates throughput):

1. Append all data records.
2. Append a control record (`CONTROL_FLAG` set) whose payload is a kind byte
   (`BATCH_COMMIT = 0x01`) plus the u64 LE highest global position in the batch.
3. One `sync()`.
4. Advance `flushed_offset` past the marker.

Control records carry a kind discriminant from day one. Future kinds (segment seal,
position checkpoint, format version bump) are far harder to retrofit than to include.
Unknown kind bytes are a recoverable skip, not a hard error.

The marker does **not** mean "this was fsynced" (it is written before the fsync). It
means "this is the end of a batch submitted as a unit."

### 4.3 The recovery rule

> A batch is committed **iff** every record from the previous commit point onward
> validates by CRC **and** the run terminates in a valid commit marker.

Marker-present alone is insufficient. `fsync` provides no ordering guarantee *within* a
flush, so a trailing marker can be durable while an earlier page of the same batch is
torn. Validate the whole run, not just the terminator.

Recovery scans forward from the last known-good point, buffering the current run,
accepting on a valid marker, and discarding the trailing partial run.

Two invariants this depends on:

- **A batch never spans segments.** Roll over before writing, never split.
- **Segment files are never recycled.** `fallocate` zeroes, so trailing garbage cannot
  masquerade as a valid record, but only in a freshly created file.

Recovery logic must be **pure over a byte slice**, taking no file handles, so the rule
is directly testable against hand-built inputs. That rule is where the real risk lives.

### 4.4 Durability details that are easy to get wrong

- `Writer::create` must `file.sync_all()` (not `sync_data`: `fallocate` changes the
  inode) and then fsync the **parent directory**. Without the directory fsync the
  segment can vanish entirely after a crash. Order is file, then directory.
- Parent path needs a fallback: `Path::new("x.log").parent()` returns `Some("")`.
- Any change to the directory namespace (rename, delete during archival or retention)
  needs a directory fsync too.
- Oversized records are rejected, not split. Hard limit is
  `segment_size - header_size - (RECORD_HEAD_SIZE + COMMIT_MARKER_PAYLOAD)`; the
  default cap is `segment_size / 4` so one large record cannot thrash rollover.
- **Zero-length payloads are rejected.** A length field of 0 is exactly what the
  zero-filled `fallocate` tail looks like, so an empty record would terminate scans
  early.
- **Partial batch failure must rewind.** If record 4 of 5 fails to append, the writer's
  offset has already advanced; the *next* successful batch would place its commit
  marker after those orphans and recovery would adopt them as its own. Capture
  `write_offset` before the batch, `rewind_to` it on any error. This is the one bug
  class here that produces silent corruption.

### 4.5 Segment header (64 bytes, offset 0)

```
magic "EVTS" (4) | version (2) | created_at_nanos (8) | base_position (8)
| zero padding (38) | CRC32 (4)
```

Offsets are **derived from widths** (`OFF_VERSION = OFF_MAGIC + SZ_MAGIC`, ...), with
`OFF_CRC` anchored to the end of the header. Hand-written literals and
adjacency-derived ranges were both rejected: the first states each width twice, the
second breaks on the padding gap.

Validation order in `from_bytes` is **load-bearing**:

1. All-zero buffer -> `Unwritten`. This is a normal state (created but not yet
   header-written), not corruption.
2. **Checksum**, before any field. A torn write that happens to leave valid magic must
   report corruption, not a format complaint, or debugging leads the wrong way.
3. Magic, then `version > VERSION` (not `!=`, so older segments stay readable), then
   padding.

The header is the one structure that interprets every record in the file, so it is
CRC-protected like the records are. A silently wrong `base_position` would poison the
entire position space derived from that segment.

Encoding uses `copy_from_slice` into a stack buffer at named offsets; decoding uses
`try_into().unwrap()` on subslices of `&[u8; SEGMENT_HEADER_SIZE]`. No `unsafe`
transmute, no `#[repr(C)]` cast: zero measurable gain at 64 bytes written once per
segment.

Layout constraints are `const _: () = assert!(...)` at module scope, not runtime tests.
The golden byte-array tests are a **layout lock**: if one fails, the on-disk format
changed. Bump `VERSION`; do not regenerate the expected bytes. A golden test you
regenerate on failure is decoration.

### 4.6 Positions

**Positions are 1-based.** Position 0 is the "before everything" sentinel.

This is a DCB-specific requirement, not taste. `after` means "ignore everything at or
before this position", so a client that has read nothing needs to express "check from
the beginning". Under 1-based numbering that is `after: 0`, which falls out of
`u64::default()` for free and unifies with the spec's "omit `after`" case. Under 0-based
it needs an `Option` or a sentinel, and `last_position()` needs an `Option` to
distinguish an empty log from one event.

The offset sidecar stays 0-indexed internally (`pos - base_position`). Only the external
position space is 1-based.

`scan_from(0)` **clamps to the beginning**, it does not return empty. Returning empty
would be a silent wrong answer: a projection catch-up starting at 0 would build an empty
read model and report success. With `after` exclusive and `scan_from` inclusive,
`after: 0`, `scan_from(0)` and `scan_from(1)` all denote the same set.

`Position` is `Copy + Ord + Hash + Display`, with `next()`, `offset_from(base)`, and
`Sub<Position> = u64`. The subtraction yielding `u64` rather than `Position` makes
"difference between positions is a count, not a position" a type-level fact.

### 4.7 `SegmentSet`

Turns N independent `seglog` files into one logically continuous, position-addressed
log. Everything about *what* an event is stays above it; everything about record framing
stays below it.

Invariants:

1. Segments are position-disjoint and contiguous: segment N's `base_position` equals
   segment N-1's `base_position + event_count`.
2. Exactly one segment is active (writable); the rest are sealed and immutable.
3. A batch never spans segments.
4. Segment files are never recycled or reused.

Naming is `{base_position:020}.log`, zero-padded so lexicographic order equals numeric
order. The filename is the segment's identity, so `base_position` appears in both the
name and the header, and startup cross-checks them.

**Open path.** Create and fsync dir if absent; list, filter, sort numerically; read each
header. `Unwritten` is legal *only* on the last file (crash between create and header
write) and that file is deleted. Verify contiguity across the whole chain: a gap or
overlap is a hard error, never a repair. Run recovery on the **last** segment only;
earlier segments were sealed, so full verification belongs behind a flag. Cross-check
the recovered commit marker's highest position against the scanned event count.

Startup outcomes must be distinguishable: clean, recovered-with-rollback (log the
discarded byte and position range), and corrupt (refuse to open).

**Append path.** Reject oversized/empty records; compute total size including the
marker; roll over *first* if it does not fit; append, commit, extend the in-memory
offset sidecar, advance `next_position`. A batch that cannot fit an empty segment is an
error, not an infinite rollover loop.

**Offset sidecar.** Per segment, `Vec<u32>` of byte offsets indexed by
`position - base_position`. Not persisted: it is derivable by one sequential pass, so it
needs no durability of its own. `SegmentConfig` validates `segment_size <= u32::MAX`,
which makes the offset conversions provably infallible.

**Reader caching.** Each `Segment` caches one `Reader` behind a `Mutex`. Segments are
immutable, so the fd is reusable indefinitely. Opening a file per `read_at` was the
original mistake.

**Errors never look like end-of-stream.** A scan that fails to open a reader must yield
an error, not terminate. On the projection path, "done" means "you have seen
everything", and a silent short read produces a wrong read model that reports success.

---

## 5. Event model

### 5.1 Representation

`Event` owns a single buffer; strings are ranges into it.

```rust
pub struct Event {
    buf: Box<[u8]>,
    data_offset: u32,   // cached prefix sum locating the payload; decode invariant
}
```

Layout is a small length header (`type_len`, `tag_count`, then one `u16` per tag), then
the type, then tags in sorted order, then payload, all contiguous. Lengths rather than
ranges, because offsets are prefix sums and the encoded form needs no separate offset
table.

Only `data_offset` is cached, not `type_len` or the per-tag lengths: those are single
`u16` reads out of `buf`'s header, so caching them would just duplicate bytes already in
the buffer (and, for events with more than four tags, heap-allocate a side array).
`data_offset` alone is cached because it is the one value that costs an O(tags) prefix
sum to recompute. This keeps `EventRef` a `Copy` two-word view (`&[u8]` plus `u32`) that
allocates nothing, ever. `EventRef::from_bytes` and `Event::new` are the only places the
header is parsed and validated; the accessors read it back unchecked.

One allocation per event regardless of tag count, and **the in-memory layout is the
on-disk layout**, so decoding is parsing integers rather than copying.

The justification is not allocation count, it is **lazy decoding**. A projection scan
needs type and tags to filter, and the payload only for events that match. With owned
`String` fields you allocate and copy every payload before discovering you are
discarding it. With ranges you parse a handful of integers and never touch payload bytes
for non-matching events.

`EventRef<'a>` borrows from the reader's buffer directly: zero allocations on the
highest-volume read path. `Event` is the owned counterpart, obtained via `to_owned()`.
The borrowed form is the primitive; the owned form is the convenience, not the reverse.

**All accessors return borrows**: `&str` for the type, `&[u8]` for the payload, and an
iterator of `&str` for the tags. This is the one truly irreversible API decision; the
internal representation behind it is swappable, the accessor shape is not.

Tags are *not* returned as `&[Tag]`. A slice needs fixed-stride elements, but tags are
variable-length strings packed contiguously in the buffer, so a `&[Tag]` would require a
separate fat-pointer array or an allocation, which defeats the zero-copy read path this
whole section exists for. The borrowed iterator (`TagsRef`, yielding `&str` in sorted
order) is the zero-copy shape, and it is what AND-matching consumes as a linear merge
over two sorted sequences.

### 5.2 `EventType` and `Tag`

Both are `Box<str>` newtypes. Arbitrary opaque strings: the spec treats `course:c1` as a
string, so do not parse it into key/value.

`Box<str>` over `String` (16 bytes, one exact alloc, no capacity slack, data is
immutable) and over small-string crates: `smol_str`/`compact_str` inline windows are
~23 bytes, and the dominant real tag shape
(`student:550e8400-e29b-41d4-a716-446655440000`, 44 bytes) spills on every one, so the
inline optimisation mostly never fires.

**No `Deref`.** `Deref` on a newtype is a known antipattern outside smart pointers:
method resolution silently reaches through, so `tag.len()` compiles and means the
string's length, and adding an inherent `len()` later silently changes meaning under
callers. `AsRef<str>`, an explicit `as_str()`, `Display`, and `Ord` cover everything
needed, in about a dozen lines with no dependency.

Both validate on construction: non-empty, and under a max length (the length is encoded
in a fixed-width field, and tags become FST keys later).

In the buffer design, `Event` no longer *holds* these types; `buf` does. They are
construction input and index keys.

### 5.3 `Tags`

`Tags(SmallVec<[Tag; 4]>)`, sorted and deduped at construction, private behind an
accessor.

Not a `BTreeSet`: tag sets are 1 to 4 entries, and a sorted inline vec beats node
allocation and pointer chasing, and the encoder needs a slice anyway.

**Duplicates are rejected, not silently deduped.** A duplicate tag is a caller bug, and
swallowing it means an event that round-trips to something different from what was
submitted.

Sortedness does real work: the encoded form becomes canonical (identical tag sets
produce identical bytes), and AND-item matching becomes a linear merge over two sorted
sequences.

There is no `Tags` on the read path, and so no `from_sorted_unchecked`. The decode path
never rebuilds a `Tags`: `EventRef::tags()` yields the tags as borrowed `&str` straight
out of the buffer (already sorted, so iteration is sorted), which is strictly cheaper
than reconstructing an owned, per-tag-allocating `Tags` would be. An earlier design had
decode rebuild `Tags` and kept an unsafe `from_sorted_unchecked` to skip its re-sort;
the zero-copy `EventRef` made both unnecessary, so the unsafe constructor was removed
rather than left as unused, unverifiable code. Reintroduce it only alongside a real
caller that needs an owned `Tags` from already-sorted input.

### 5.4 Codec and scan shape

**The seglog record payload is the encoded event, verbatim.** No intermediate
`SegmentSet`-level framing. This is what lets `EventRef` borrow the readahead buffer.
`append_batch` keeps its `&[&[u8]]` signature: an encoded event *is* `&[u8]`, and
`event.as_bytes()` is the whole interface. Taking `&Event` there would leak the codec
into the framing layer for no gain.

**`Scan` is a lending iterator**, not `std::Iterator`:

```rust
fn next(&mut self) -> Option<Result<EventRef<'_>, LogError>>
```

`std::Iterator` cannot yield a borrow of `self` per item (the GAT / lending-iterator
wall), so an owned-yielding `Iterator` structurally forbids the zero-copy path.

A visitor (`fn scan(&self, pos, f: impl FnMut(EventRef<'_>) -> ControlFlow<..>)`) was
considered and rejected as a *second* shape. Two consumption shapes means two code paths
over the same bytes, two sets of segment-boundary handling, and two places for the
flushed-watermark check to be subtly wrong: the highest-risk logic in layer 1, which
should exist once. The lending iterator subsumes it (`while let` + `break`
short-circuits identically) and keeps control of the loop with the caller, which matters
when the condition evaluator interleaves scanning with other state. A visitor on top of
a lending iterator is five lines later; the reverse is not true.

Point reads (`read_at`) stay owned. Low volume, and one event is materialised anyway.

Decode signature is `&[u8] -> Result<EventRef<'_>, _>`. Start with checked decoding
(prefix-sum bounds checks) and measure; design so a `from_bytes_unchecked` justified by
the record CRC can be added without changing `EventRef`'s shape.

---

## 6. Layer 2: Write coordinator

**One logical writer. Single writer, not single threaded.**

Serialized: position assignment, the tag-tips lookup, the condition verdict. All
in-memory hash work, millions of ops/sec on one core. Not the bottleneck.

Off-thread: payload encoding, group-commit fsync, index flush and merge, all reads.

**Sync, not async.** The writer thread (`Worker`) owns the `SegmentSet` and a `Receiver`;
`WriteHandle` is a cloneable `SyncSender<Message>` where each request carries a reply
`Sender` (a per-call oneshot). The caller blocks on the reply, so the API is
`handle.append(events, condition) -> Result<PositionRange, AppendError>`. Events are
pre-encoded `Event`s: encoding is on the caller thread, off the writer. The bounded
channel gives backpressure for free (the caller blocks when it is full). `WriteCoordinator`
owns the join handle; `shutdown()` signals and joins, returning the `SegmentSet`, and a
drop of the owner does the same. A `Message::Shutdown` sentinel gives deterministic
shutdown even while handles are still live; channel disconnect handles the drop path.

The loop blocks on `recv()`, then drains with `try_recv()` up to a record/byte batch cap.
Group commit falls out naturally without a timer, and batch size grows automatically as
fsync latency rises, which is exactly the desired behaviour. A one-slot pushback buffer
holds a request received but deferred (over budget, or oversize and needing its own solo
batch): `try_recv` hands over a request before it can be measured, so the buffer is
required. An oversize request within segment capacity gets a solo batch; one beyond
capacity is answered `TooLarge` and never blocks anything.

**The condition check under group commit is the correctness core.** Several independent
decisions drain into one batch, and two can conflict with each other (the uniqueness
guard). Each accepted request is staged (its positions assigned, its tags recorded) before
the next request in the same drain is evaluated, so a later request sees the earlier one.
The whole batch is one `append_batch` (one fsync); on failure every staged request is
answered with an error and nothing commits (`append_batch` rewinds, `next_position` does
not advance). Partial success is not representable. The `after <= tip` invariant is
asserted per request: a client cannot have observed a position past the durable tip, so a
staged record (position strictly above the tip) can never be skipped by the `after`
filter. A request past the tip is a bug and is rejected `AfterBeyondTip`.

### Append-condition check: two arms

The condition evaluator (`condition::evaluate`) is the one definition of "does a matching
event exist after `after`" that the index (layer 3) will be differential-tested against.
It has two arms, and **neither may ever produce a false negative** (silently accepting a
conflicting write). `may_match` returns an enum (`DefinitelyNoMatch` / `Unknown`), never a
bool, so every call site handles the `Unknown` fallthrough explicitly.

**Durable arm: `TagTips` fast-reject, then the scan oracle.** `HashMap<tag, max_position>`
of the highest position per tag, **bounded to a recent position window**. For an AND-item,
if any tag has `max_position <= after`, the item cannot match: `DefinitelyNoMatch` with a
hash lookup and zero I/O. On `Unknown`, fall through to the scan (`scan_after`, decode
each `EventRef`, run `Query::matches`), which is the permanent oracle, not scaffolding.
The scan is bounded because `after` is recent, so it touches only the tail. This arm is an
optimisation over always scanning; correctness lives in the oracle.

The bound is what makes the map viable. `after` is recent by construction (the position
the client read at, milliseconds ago), so the map only needs tags touched within a recent
window, sized by write rate times window duration, not by total tag cardinality. At 50k
events/sec with a 60-second window that is a few million entries whether you have 100M
entities or 100 billion. An unbounded version at 100M entities would be 3 to 5 GB, which
is the correct objection to it.

Rule with a window: if the tag is absent and `window_floor <= after`, its max is below the
floor, which is at or below `after`, so it cannot match. `DefinitelyNoMatch`. If
`after < window_floor`, fall through to the scan. The floor is **monotonic
non-decreasing** (it starts at `next_position`, so a cold map is all-`Unknown` and every
early append scans: correct warm-up, not a bug) and eviction only ever raises it. A tag
whose only event predates the current floor therefore stays below it forever and can never
produce a false negative. Bounding is memory-only: correctness never depends on the map's
contents, only memory does. That is what makes crude eviction (raise the floor, drop what
falls below) acceptable.

**Staged arm: `StagedTips`, complete knowledge of one drain window.** Staging a record
updates a batch-local `StagedTips` with its not-yet-durable position, and a later request
in the same drain is rejected by the same kind of tag lookup. This is load-bearing for
batch correctness, not an optimisation: staged records are not durable, so no scan can see
them. `StagedTips` and `TagTips` are **two types, not one**, because they disagree on what
an absent tag means, and one type carrying both meanings is exactly the subtlety that
reads as correct and is not. Absent in `TagTips` means "maybe below the floor" (`Unknown`);
absent in `StagedTips` means "definitely not staged" (no conflict). A single flagged type
with `window_floor = tip` would return `Unknown` for every `after < tip` and reject the
first conditional request in a drain against nothing.

The staged check is **conservative and tag-only**: it ignores the query's type constraint
(the staged map has no types), so two same-window requests sharing an item's full tag set
conflict even if a precise type check would clear them. This is safe (never accepts a true
conflict) and self-correcting: the loser is answered `ConflictSite::SameBatch`, which is
**advisory and retryable**, and a retry re-reads the now-durable record for the precise
verdict. A durable conflict is `ConflictSite::Durable(pos)`, terminal until the client
rebuilds. A layer above the coordinator must honour that distinction and must not collapse
the two into a bare position. This conservatism is only safe if callers retry; the retry
contract is the future client layer's obligation.

**Keys are the tag strings, not a hash.** A `HashMap<Box<str>, _>` looks up by `&str` for
free, so recording and querying allocate nothing on the read side. A hash was rejected: it
throws away diagnosability in the one component whose failure mode is invisible by design
(a `u64` cannot answer "which tag" when a spurious rejection or a property-test
disagreement is investigated), and it is *not* a step toward layer 3, which interns tags to
an exact `TermId(u32)` and discards a hash rather than building on it. A window-bounded map
of string keys costs a few MB; revisit only if profiling says so. A fixed-size hashed array
with collisions remains an acceptable *memory-bounded* fallback if it ever matters: a
collided slot stores the max over several tags (`>=` the true max), so `stored <= after`
still implies the real max is, losing a fast-reject but never correctness. Its constraint
is that a collision may only lose a fast-reject, never invert one.

A paranoid `verify_tips` mode (a runtime flag, so a property test can flip it per case, not
a cargo feature) runs the scan even on `DefinitelyNoMatch` and asserts they agree. Test the
window boundary and the `after < window_floor` fallthrough hard, and the `after == tip`
case with a staged record at `tip + 1`: a false negative there silently accepts a
conflicting write.

---

## 7. Layer 3: Index segments

Immutable, position-disjoint, one index segment per sealed log segment (so disjointness
is inherited and segment lifecycle is one concept, not two).

### 7.0 Build split and the in-memory tail index

The layer is built in two parts. **5a** is the in-memory core: a term interner
(`TermInterner` -> `TermId(u32)`) and a type interner (`TypeInterner` -> `TypeId(u16)`,
rejecting more than `u16::MAX + 1` distinct types per segment at push time, where the
offending string is in hand), a per-segment `TailIndex` (per-tag postings plus the dense
type column), and `index::search`, the index-driven evaluator. **5b** is the on-disk index
segment format, feeding, sealing, recovery, and pruning (everything below this
subsection).

`index::search` is the counterpart to phase 4's scan oracle: it evaluates a `Query` from
postings instead of a linear decode, and is **differential-tested to return the identical
ascending positions the scan does**. That test is anchored by hand-derived, spec-sourced
answers for the tricky cases (empty types match any type, empty tags constrain only on
type, empty `Items` matches nothing, `after` is exclusive), so the evaluator and the
predicate are both pinned to the spec rather than only to each other.

`search` returns an **ascending, deduped `impl Iterator<Item = Position>`, not a `Vec`**.
The per-segment ascending-output contract is load-bearing for 5b: a query spans several
per-segment indexes and the cross-segment union is a k-merge of these streams, so a `Vec`
would force a materialize-then-sort per segment that 5b would rewrite. This is distinct
from the phase-4 (layer 4) planner's streaming, which is a later concern.

The `TailIndex` is fed **in position order** via `push(position, event)`, guarded by a
real (not `debug_assert`) contiguity assert: out-of-order feeding would silently produce
unsorted posting lists that break every intersection. Feeding is inline at the commit seam
(the `TagTips::absorb` point) in 5b; the append-condition hot path keeps the scan oracle
until phase 6 wires the `Unknown` fallthrough onto the index.

The tail index's per-tag postings **subsume** the layer-2 `TagTips`: a tag's max position
is just the last element of its posting list. They are kept as separate structures for now
and unified only when the condition fallthrough moves onto the index (phase 6), so phase
4's correctness core is not destabilized early.

**Tags (high cardinality) -> inverted index.** FST term dictionary (compresses `course:`
style prefixes well), tiered postings by term frequency: singletons inlined in the term
dictionary, small terms as varint deltas, dense terms as Roaring bitmaps. AND is
intersection, OR is union, both ascending by construction.

**Types (low cardinality) -> dense column.** A `u16` type_id array indexed by
`position - base_position`. Two bytes per event, mmap'd, cache-friendly.

- Type-only query: sequential scan of the column, roughly 100x less I/O than a log scan,
  and counting projections never touch the log.
- Type + tags: intersect tag postings first, then probe the column per candidate at
  O(1).

**There are no type posting lists.** Type cardinality is 10s to 100s and per-type event
counts are in the millions, so a type posting list is nearly dense and an index lookup
buys nothing over a scan while costing random I/O. Postgres declining to use such an
index is Postgres being right.

Type ids are **segment-local**, Lucene-style. No global registry, so no id-stability or
persistence problem.

Payloads live in a separate block-compressed blob region per segment, addressed
separately, so condition checks and count-style projections never decompress. This also
gives a place for crypto-shredding on erasure requests without touching positions or
indexes.

---

## 8. Layer 4: Query planner

Per query item, choose posting intersection vs column/log scan by comparing posting
length against the post-pruning position range width.

Exact posting lengths come free from the term dictionary, per segment. No statistics, no
`ANALYZE`, no estimation error: the planner is strictly easier here than in a
general-purpose database.

`Query.all()` and broad projection catch-up bypass the index entirely and scan the log
sequentially. Keep the index out of the projection rebuild path.

---

## 9. Layer 5: Read paths

Three distinct shapes, deliberately not unified:

1. **Condition check.** Hot, on the write path, index-only, usually resolved in the tail
   segment.
2. **Decision model read.** Selective, index-driven, small result set, then payload
   fetch by position with the random read hint.
3. **Projection catch-up and subscriptions.** Sequential log scan at disk bandwidth with
   the sequential read hint. Highest volume by far.

Subscriptions need catch-up followed by live-tail handoff with no gap and no duplicate at
the boundary. This is subtle and is where event stores usually have bugs.

Readers are lock-free over immutable segments plus an atomically published watermark.
Immutability means readers never block the writer.

---

## 10. Scale constraints

Global total order means **one logical writer per bounded context**. You cannot shard
your way out: the entire value proposition of DCB is queries spanning entities, so
tag-based partitioning breaks exactly the cross-entity conditions the system exists for.

A Corfu-style sequencer with sharded storage scales the data plane, but positions get
assigned before they are durable (inheriting hole-filling), and the condition check still
needs the fully ordered durable suffix, so the sequencer keeps the tips map anyway. Same
decision bottleneck, new distributed failure modes. Rejected.

**The ceiling is fsync, not ordering.** Batch aggressively and measure.

Scale by running more bounded contexts, not by splitting one.

Design rule: nothing outside the write coordinator may assign a position, so a sequencer
could be swapped in later if the numbers ever justify it.

---

## 11. What is deliberately absent

Each of these exists in general-purpose engines purely to reconcile mutation with sorted
order. Immutability deletes them:

- No compaction (nothing mutates). LSM compaction would also destroy `after` pruning by
  producing SSTs that span the whole position range.
- No free-list or page reuse (nothing is freed). This is the most subtle part of a COW
  B-tree design and it is a consequence of choosing COW pages, not a requirement.
- No WAL for indexes (all derived).
- No tombstones.
- No type index (see layer 3).
- No B-tree over positions. A dense monotonic key plus a sparse offset index already
  costs nothing, which is also why learned indexes have no problem to solve here.

---

## 12. Deferred, without design debt

Retention and archival, cold-segment recompression, index segment merging, replication,
persisted offset sidecars (or lazy per-segment construction) for large logs where
startup would otherwise read everything.

---

## 13. Module layout

```
src/
  lib.rs
  error.rs
  event.rs          // Event, EventRef, EventType, Tag, Tags, codec
  query.rs          // Query, QueryItem, AppendCondition, match predicate
  position.rs       // Position, PositionRange

  log/
    mod.rs          // Log is the layer-1 entry point
    segment.rs      // Segment: one seglog file + header + offset sidecar
    header.rs       // SegmentHeader encode/decode
    offsets.rs      // Offsets: position -> byte offset, built by scan
    set.rs          // SegmentSet: discovery, ordering, rollover, active segment
    scan.rs         // Scan: lending iterator over positions
    recovery.rs     // pure run validation over a byte slice

  writer/
    mod.rs
    coordinator.rs  // owns the writer thread loop
    handle.rs       // WriteHandle: cloneable, Send, blocking append()
    batch.rs        // accumulates records + assigns positions
    condition.rs    // evaluates AppendCondition (two arms: staged, then durable)
    tips.rs         // TagTips (durable, lossy) + StagedTips (batch, complete)

  index/
    mod.rs          // TermId, TypeId; re-exports
    interner.rs     // TermInterner (tags -> u32), TypeInterner (types -> u16, bounded)
    tail.rs         // TailIndex: per-tag postings + dense type column, fed in position order
    search.rs       // index-driven Query evaluator, ascending iterator (differential oracle)
```

Naming: `Log`, not `Store` or `EventLog` (it reads as `crate::log::Log`). `SegmentSet`,
not `SegmentManager`; nothing is called a manager. "Tip" is the standard word for the
latest entry on a branch; it is not a cache.

`Segment` is one type covering both the active writable segment and sealed ones,
distinguished by an `Option<Writer>`. A `Segment`/`SealedSegment` split looks cleaner and
then fights you when the active segment seals mid-scan.

---

## 14. Testing standards

- **Recovery is the highest-risk logic.** Test it pure over byte slices, table-driven
  over a dense range of truncation offsets, not a handful of hand-picked ones.
- **The case that matters most:** intact trailing commit marker with a corrupted record
  *earlier* in the same batch. The whole batch must be rejected. Overwriting from a
  cutoff to the end never exercises this, because it always destroys the marker too.
- Test physical truncation (short file) separately from garbage overwrite: different
  code paths.
- Every degenerate buffer: all-zero (must be `Unwritten`, a normal state) and all-`0xFF`
  (must be corruption).
- Exhaustive single-bit flips over fixed-size structures. 512 cases for a 64-byte header
  runs instantly and proves no single-bit corruption yields a trusted decode.
- Pin validation *ordering* explicitly, or someone reorders the checks for readability
  and error messages start pointing at the wrong cause.
- Constant-expression invariants belong in `const _: () = assert!(...)` at module scope,
  not runtime tests: a violation should fail the build.
- Golden byte arrays are layout locks. Never regenerate on failure.

---

## 15. Prior art

**UmaDB** is the closest comparable: LMDB specialised for DCB (single paged file,
copy-on-write B+trees, dual headers, TSN-based MVCC, free-list tree, mmap readers, one
writer). Worth reading; it reaches the same single-writer conclusion, and its tag-value
tiering (inline list for rare tags, promote to subtree when hot) is the same insight as
the tiered postings here, arrived at independently. Its dual-header COW gives crash
safety with no WAL and O(1) recovery, which is cleaner than a recovery scan.

Where this design diverges and why:

- **Sequential scan degrades under COW.** Projection rebuild and subscription catch-up
  are position-ordered scans and the dominant read workload. In an append-only log that
  is a sequential read at disk bandwidth. In a B+tree, after page reuse, logical order
  stops matching physical order and the scan becomes random I/O.
- **Payloads in leaf nodes** mean fewer positions per page and every COW leaf copy drags
  payload bytes along.
- **Per-commit write amplification:** k tags is k random leaves plus root-to-leaf path
  copies, plus free-list churn that recursively frees and reuses its own pages until it
  settles, plus two fsyncs.
- **Tag hashing** (64-bit hash as key) forbids prefix scans and needs collision
  post-filtering; an FST over raw strings costs less space given the shared prefixes.
