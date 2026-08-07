# Roadmap

Companion to `CLAUDE.md`. That document says *what* the system is and *why*; this one
tracks *what is done and what is next*.

Ordered by dependency. Do not skip ahead: each phase exists because the next one needs
it. Check items off as they land, and when a decision gets made that contradicts
`CLAUDE.md`, update `CLAUDE.md` in the same commit.

Status key: `[x]` done, `[~]` in progress, `[ ]` not started.

---

## Phase 1: Log (layer 1)

Durable, position-addressed byte storage.

### 1.1 `seglog` crate

- [x] Record format: length + flags + CRC32, `LENGTH_MASK` at 30 bits
- [x] `COMPRESSION_FLAG` (bit 31), `CONTROL_FLAG` (bit 30), strict unknown-flag rejection
- [x] Batch commit markers with kind discriminant (`BATCH_COMMIT = 0x01`)
- [x] Directory fsync in `Writer::create`, with relative-path fallback
- [x] `sync_all` after `fallocate` (inode metadata, not just data)
- [x] Oversized record cap and `WriteError` variant
- [x] `Writer::rewind_to(offset)` for partial-batch failure
- [x] `Reader::peek(offset) -> RecordKind` (header-only, no CRC, no allocation)
- [x] Sequential reads borrow the readahead buffer (invariant, tested)
- [x] Control records skipped by iteration, direct test
- [x] Verify `rewind_to` resets the `dirty` flag; assert `write_offset == flushed_offset`
      after a rewind to a clean point
- [x] Integration test for the `commit`-fails path specifically: N successful
      `append_data`, failed `commit`, next batch succeeds, recovery must not adopt the
      orphans
- [x] Audit `unreachable!` in the borrow path: message must name the invariant and the
      test that pins it, or handle the owned branch instead of asserting it away

### 1.2 Segment header

- [x] 64-byte fixed layout, magic / version / created_at_nanos / base_position / CRC
- [x] Width-derived offset constants, `const _: () = assert!(...)` layout locks
- [x] `to_bytes` / `from_bytes`, no bincode, no unsafe
- [x] Validation order: all-zero, then checksum, then magic, version, padding
- [x] Full rejection test suite including exhaustive single-bit flips
- [x] Golden byte arrays as layout locks

### 1.3 `SegmentSet`

- [x] Naming, discovery, numeric ordering, contiguity verification
- [x] Open path with `Unwritten` handling (last file only) and hard errors elsewhere
- [x] Recovery on the active segment, commit-position vs event-count cross-check
- [x] Append path with rollover-first, batch-never-spans-segments
- [x] `SegmentConfig::validate` (including `segment_size <= u32::MAX`)
- [x] Offset sidecar, in-memory, rebuilt by scan
- [x] Per-segment cached `Reader` behind a `Mutex`
- [x] `Scan` as a zero-copy lending iterator yielding `RecordRef<'a>`
- [x] Errors never presented as end-of-stream
- [x] Truncation tests, table-driven over dense cutoffs
- [x] Corrupted-record-with-intact-marker test
- [x] Physical truncation test, separate from garbage overwrite
- [x] 1-based positions: `next_position` starts at 1, first segment
      `base_position = 1`, `last_position() -> Position` drops the `Option`, empty-log
      checks compare against 1
- [x] `scan_from(0)` clamps to the beginning rather than returning empty
- [x] `Position` API: `Copy + Ord + Hash + Display`, `next()`, `offset_from(base)`,
      `Sub<Position> = u64`
- [x] Migrate `.0` call sites onto the `Position` methods, then drop `pub` on the inner
      field (field was already private; all production `.0` accesses now use
      `get()` / `next()` / `offset_from()` / `Sub` / `Ord` / `Position::new` / `Position::ZERO`)
- [x] Decide `scan_after(Position)` (exclusive) as the primitive, with `scan_from`
      as an inclusive wrapper: subscriptions hold "last processed", and everything
      above is exclusive
- [x] Replace remaining `u32::try_from(...).expect(...)` messages with ones naming the
      `SegmentConfig::validate` invariant that guarantees them

---

## Phase 2: Event model and codec

Pure logic, no I/O. Blocks everything in phase 3.

- [x] `EventType(Box<str>)`, `Tag(Box<str>)`: `AsRef<str>`, `as_str()`, `Display`,
      `Ord`, no `Deref`
- [x] Construction validation: non-empty, max length (fixed-width length field, FST keys
      later)
- [x] `Tags(SmallVec<[Tag; 4]>)`: sorted, duplicates **rejected** not deduped
- [x] ~~`Tags::from_sorted_unchecked` for the decode path~~ **Removed.** The zero-copy
      `EventRef` decode never rebuilds a `Tags` (it yields borrowed `&str`), so the
      unsafe constructor had no caller. Reconciled in CLAUDE.md 5.3
- [x] `Event { buf: Box<[u8]>, data_offset }` (dropped the cached `type_len` / `tag_lens`:
      derivable from `buf`'s header, so `EventRef` is a `Copy` allocation-free view;
      CLAUDE.md 5.1 updated)
- [x] `EventRef<'a>` borrowing a `&[u8]` (joined to `RecordRef::data` at the call site)
- [x] All accessors return borrows: `&str` (type), `&[u8]` (payload), and `TagsRef`, a
      borrowed iterator of `&str`, for tags. **Deviation from CLAUDE.md 5.1**: tags
      cannot be `&[Tag]` zero-copy (variable-length strings packed contiguously have no
      fixed stride); CLAUDE.md 5.1 updated to match
- [x] `Event::to_owned()` from `EventRef`; borrowed is the primitive, owned is the
      convenience
- [x] Codec: type, then sorted tags, then payload, contiguous, length-prefixed
- [x] `EventRef::from_bytes(&[u8]) -> Result<EventRef<'_>, _>`, checked decoding
- [x] Encoded form is canonical: identical tag sets produce identical bytes (test it)
- [x] Round-trip tests, plus malformed-input rejection (truncated, lying lengths,
      overflowing prefix sums)
- [x] Codec lives in `event.rs`; the join to `RecordRef` happens at the call site above
      both, not via a helper on the log side (`event.rs` has no `seglog` dependency)
- [x] Deferred: `from_bytes_unchecked` justified by the record CRC. Signature is designed
      for it: `from_bytes(&[u8]) -> Result<EventRef>` means a `from_bytes_unchecked(&[u8])
      -> EventRef` can be added later without changing `EventRef`'s shape

## Phase 3: Query model

Pure logic, no I/O.

- [x] `QueryItem { types: Vec<EventType>, tags: Tags }` (with `new` / `of_types` /
      `with_tags` / `default` constructors; `types` unsorted since it is low
      cardinality and membership is a linear `any`)
- [x] `Query` as a set of items, plus the `Query::all()` variant. Modelled as an enum
      (`All` vs `Items(Vec<QueryItem>)`) so the read/condition paths can recognise a
      full scan and bypass the index; empty `Items` matches nothing (OR over zero)
- [x] `AppendCondition { fail_if_events_match: Query, after: Position }` (`new` defaults
      `after` to `Position::ZERO`, the 1-based "whole log" bound; `.after(pos)` builder)
- [x] The match predicate: type in list (empty list matches any type) AND all tags
      present, as a linear merge over sorted sequences (`tags_contained`). The item's
      `Tags` is sorted by construction and `EventRef::tags()` decodes in sorted order,
      so neither side sorts at match time
- [x] One definition of the predicate: `Query::matches` / `QueryItem::matches`, to be
      shared by the condition evaluator (layer 2) and differential-tested against the
      index (layer 3)
- [x] Tests: OR-across-items, AND-within-item, empty-types (matches any type), empty
      item (matches everything), empty `Items` (matches nothing), tag-merge boundary
      cases (overshoot, exhaust, no tags), and `AppendCondition` default `after: 0` plus
      the `.after` bound

---

## Phase 4: Write coordinator (layer 2)

- [ ] `writer/tips.rs`: `TagTips`, bounded window map, `record()` and `may_match()`
- [ ] Test the window boundary and the `after < window_floor` fallthrough hard. A false
      negative silently accepts a conflicting write
- [ ] Optional: fixed-size hashed array fallback, with a test that collisions only ever
      lose a fast-reject, never correctness
- [ ] `writer/condition.rs`: tips fast-reject, then fall through to a scan from `after`
      to the end
- [ ] Slow path is a full scan in v1. Correct and slow, and it becomes the
      known-correct baseline the index is tested against later
- [ ] `writer/batch.rs`: accumulate records, assign positions
- [ ] `writer/handle.rs`: `WriteHandle`, bounded `SyncSender<Request>`, per-request reply
      `Sender`, blocking `append()`
- [ ] `writer/coordinator.rs`: thread loop, `recv()` then `try_recv()` drain to a batch
      cap, group commit without a timer
- [ ] Shutdown signal (`crossbeam-channel` select, or a sentinel request)
- [ ] Backpressure behaviour under a full queue: decide block vs reject, document it
- [ ] Concurrency tests: N threads appending, positions dense and unique, no gaps
- [ ] Conflict tests: two decisions on overlapping tags, exactly one wins

**Milestone: after phase 4, this is a correct, durable, DCB-compliant store.** It just
restarts slowly and answers selective queries by scanning. Everything below is
performance, and every bit of it can be differential-tested against this baseline.

---

## Phase 5: Index segments (layer 3)

- [ ] In-memory tail index: term -> growable position list, fed on append
- [ ] Term interning, `TermId(u32)`
- [ ] Index segment format: header with min/max position, FST term dictionary,
      postings region
- [ ] Tiered postings: singletons inlined in the term dict, small terms as varint
      deltas, dense terms as Roaring
- [ ] Dense type column: `u16` type_id indexed by `position - base_position`,
      segment-local ids
- [ ] Seal one index segment per sealed log segment
- [ ] Recovery: rebuild index state for any log tail that was durable but not indexed
- [ ] Differential test: every query answered identically by the index and by the
      phase-4 scan baseline
- [ ] Segment pruning by header comparison for `after: p`

## Phase 6: Query planner and read paths (layers 4 and 5)

- [ ] Compile `Query` into iterators: AND intersection, OR union, ascending merge across
      segments
- [ ] `after` restriction and segment pruning
- [ ] Cost model per item: posting length (exact, from the term dictionary) vs
      post-pruning position range width
- [ ] `read(query, options)` as a streaming API
- [ ] Bypass path: `Query.all()` and broad projections scan the log, never the index
- [ ] Wire `TagTips` fallthrough to the index instead of a full scan

## Phase 7: Subscriptions

- [ ] Catch-up from a position
- [ ] Live tailing off the published watermark
- [ ] Handoff with no gap and no duplicate at the boundary
- [ ] Tests that hammer the boundary specifically: append during catch-up, subscriber
      slower than the writer, subscriber starting exactly at the watermark

This is subtle and is where event stores usually have bugs. Budget accordingly.

---

## Phase 8: Deferred (no design debt)

Ordered by likely usefulness, not urgency. None of these are blocked by anything above,
and none block anything.

- [ ] Persisted offset sidecars per sealed segment, or lazy per-segment construction.
      Needed when startup would otherwise read the whole log
- [ ] Index segment merging (to reduce open-file count, never for correctness)
- [ ] Retention and archival
- [ ] Cold-segment recompression on seal
- [ ] Separate blob region for payloads, so condition checks never decompress
- [ ] Crypto-shredding for erasure requests
- [ ] Replication
- [ ] Benchmarks: fsync-bound throughput, group commit batch-size behaviour under load,
      condition-check latency with and without tips

---

## Standing rules

- Any decision that contradicts `CLAUDE.md` updates `CLAUDE.md` in the same commit.
- Recovery logic stays pure over byte slices, testable without file handles.
- Nothing outside the write coordinator assigns a position.
- Golden byte arrays are layout locks: bump the version, never regenerate.
- New on-disk structures get a CRC and a rejection test suite before they get features.
