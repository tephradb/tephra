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

- [x] `writer/tips.rs`: `TagTips` (durable, lossy) with a bounded window map and
      `may_match() -> Verdict{DefinitelyNoMatch, Unknown}`, plus `StagedTips` (batch,
      complete). **Two types, not one**: they disagree on what an absent tag means
      (`Unknown` vs "definitely not staged"), which is load-bearing. Keys are tag strings
      (`Box<str>`), not a hash, for diagnosability (CLAUDE.md 6). Floor is monotonic
      non-decreasing so an event predating the floor never yields a false negative
- [x] Tested the window boundary, the `after < window_floor` fallthrough, the
      `after == tip` with staged-at-`tip + 1` boundary, and floor monotonicity. A
      randomized `verify_tips` property test (600 iters) asserts tips agree with the scan
- [x] ~~Fixed-size hashed array fallback~~ **Not built.** String keys do not collide;
      the collision-safety property is documented as a constraint on any future hashed
      variant rather than tested here (CLAUDE.md 6)
- [x] `writer/condition.rs`: two arms. Staged arm (`StagedTips`) settles intra-batch
      conflicts (conservative, tag-only, `ConflictSite::SameBatch`, advisory/retryable);
      durable arm is tips fast-reject then a scan from `after` (`ConflictSite::Durable`)
- [x] Scan oracle is permanent, not v1 scaffolding: it is the baseline the index is
      differential-tested against, and the `verify_tips` cross-check keeps tips honest
- [x] `writer/batch.rs`: accumulate borrowed record bytes, assign dense positions,
      `commit_ok` (absorb staged tags into the main tips + reply ranges) / `commit_err`
      (reply the one error to every staged request; all-or-nothing)
- [x] `writer/handle.rs`: `WriteHandle`, bounded `SyncSender<Message>`, per-request reply
      oneshot, blocking `append(events, condition) -> Result<PositionRange, AppendError>`
- [x] `writer/coordinator.rs`: thread loop, `recv()` then `try_recv()` drain to a
      record/byte cap, group commit without a timer, one-slot pushback buffer for
      deferred/oversize requests, `after <= tip` invariant asserted per request
- [x] Shutdown: drop-based (channel disconnect) plus an explicit `Message::Shutdown`
      sentinel; `WriteCoordinator::shutdown()` joins and returns the `SegmentSet`
- [x] Backpressure: **block the caller** (bounded `SyncSender`), documented. Tested that
      a tiny queue still serves every write with dense positions
- [x] Concurrency tests: N threads appending, positions dense, unique, no gaps
- [x] Conflict tests: overlapping uniqueness guard, exactly one wins (same-batch and
      durable paths), plus oversize routing/isolation and no-events rejection

**Milestone: after phase 4, this is a correct, durable, DCB-compliant store.** It just
restarts slowly and answers selective queries by scanning. Everything below is
performance, and every bit of it can be differential-tested against this baseline.

---

## Phase 5: Index segments (layer 3)

Split into 5a (in-memory core plus the differential harness) and 5b (on-disk format and
segment-lifecycle wiring). 5a lands the term/posting model and the acceptance test in
isolation, before any serialization rigor or coordinator change.

### Phase 5a: In-memory tail index and differential test

- [x] Term interning, `TermId(u32)` (`TermInterner`) and type interning, `TypeId(u16)`
      (`TypeInterner`, rejecting >`u16::MAX + 1` distinct types per segment with a named
      error at push time). Two concrete interners, not one generic: their overflow
      semantics genuinely differ
- [x] In-memory tail index (`index::TailIndex`): per-tag postings (`TermId` -> ascending
      local positions, dense by construction) plus a dense `u16` type column indexed by
      `position - base_position`. Fed in position order via `push(position, event)` with a
      **real** (not `debug_assert`) feed-order assert, since out-of-order feeding would
      silently produce unsorted postings
- [x] Index-driven evaluator (`index::search`): AND over an item's tag postings, type
      filter via the column, OR/union across items, `after` exclusive. Returns an
      ascending, deduped `impl Iterator<Item = Position>`: the ascending-per-segment
      output is a 5b requirement (cross-segment combine is ordered concatenation of
      disjoint runs, not a k-way merge), distinct from the phase-6 planner streaming
- [x] Differential test (`tests/index.rs`): a random workload appended to a `SegmentSet`
      and fed to a `TailIndex`; for random queries and `after` bounds, `search` returns
      the exact positions the phase-4 scan oracle does. Anchored by a small fixed-dataset
      unit test with hand-derived, spec-sourced answers for the tricky cases (empty types,
      empty tags, empty `Items`, `All`, `after: 0`), so the two sides are pinned to the
      spec, not just to each other
- [x] No coordinator change and no on-disk format in 5a: the index is pure and
      `#[cfg(test)]`-verifiable without touching files. Inline feeding lands in 5b, where
      seal-on-rollover gives the fed data a destination

### Phase 5b: On-disk index segments and wiring

- [x] Index segment format (`index::header`, `index::segment`): 64-byte CRC-locked header
      (`"EVIX"`, golden byte lock + single-bit-flip suite) with base/event-count (min/max
      position) and a second body CRC, `fst` term dictionary, tiered postings region, dense
      type column. Header/body corruption is recoverable (rebuild from log), not fatal
- [x] Tiered postings (`index::postings`): singletons inlined in the FST value, small terms
      as hand-rolled LEB128 varint deltas; dense/Roaring tier reserved but deferred (a
      decode of it is a named hard error, never a silent skip)
- [x] Persist the dense `u16` type column per segment (plus a type dictionary for
      name-to-id), read back through the shared `Arc<[u8]>`. Loaded into memory, not mmap'd
      (SIGBUS on truncation, writer-thread page-fault stalls, cache control); CLAUDE.md 7
      updated
- [x] Inline feeding at the commit seam (`commit_ok`), **before the reply** for
      read-your-writes; seal one index segment per sealed log segment on rollover, publishing
      the in-memory segment before the fsync. `search` made generic over a `SegmentIndex`
      trait so the one evaluator serves the in-memory tail and the on-disk segment
- [x] Recovery (`index::recovery`, `IndexSet::open`): pure `Rebuilder` over `(position,
      event)`; rebuild any missing/corrupt/mismatched `.idx` from a log scan, and always
      rebuild the active tail by scan (covers the durable-but-unindexed tail)
- [x] Extended the differential test across multiple sealed segments (`tests/index.rs` +
      `index::set` tests): small segments force rollovers, then `IndexSet::search_all` equals
      the scan oracle across random queries and `after`; plus delete/corrupt/reopen recovery
      and live feed-and-seal
- [x] Segment pruning by header comparison for `after: p` (`max = base + count - 1 <= after`
      skips a segment). A degraded (unindexable) touched segment errors with its range rather
      than answering short (CLAUDE.md 7)

## Phase 6: Query planner and read paths (layers 4 and 5)

Split into 6a (off-thread read foundation), 6b (append-only active tail plus published
watermark), 6c (cost model, benchmark-first), and 6d (condition fallthrough onto the
index). See the phase-6 plan for the confirmed decisions behind each.

### Phase 6a: Off-thread read foundation

- [x] Reads run on the **caller's own thread** over a shared `Arc<ReadCore>` (published
      sealed `Arc<Segment>`/`Arc<IndexSegment>` plus an atomic watermark), not the writer
      thread. `Message::Search` (the 5b placeholder that rode the writer channel) is dropped;
      `WriteHandle::read` / `ReadHandle` execute on the caller (`src/read`)
- [x] One zero-copy scan shared by the writer and read snapshots: `log::Scan` made generic
      over an owned `SegmentSource` (`&SegmentSet` for the writer, `Arc<Snapshot>` for a
      reader), with an `upto` watermark bound, so there is no second scan implementation
- [x] `read(query, after)` as a streaming lending iterator of events (`Reads`), ascending,
      `after`-exclusive, with `after` restriction and per-segment pruning; cross-segment
      combine is lazy ordered concatenation. `Reads::watermark()` exposes the pinned
      watermark (the phase-7 resume seam)
- [x] Bypass path: `Query::all()` streams a filtered log scan (never the index). The active
      segment's range is answered by a bounded log scan in 6a (the active index stays
      writer-private); an unindexable sealed segment falls back to scanning its own range,
      never a short answer
- [x] Snapshot published before the watermark (writer), watermark loaded before the snapshot
      (reader), so a read snapshot always covers its watermark; read-your-writes holds (the
      writer publishes before replying). Differential test vs the scan oracle across many
      sealed segments, a concurrent-readers consistency test, and a watermark-resume test
      (`tests/read.rs`)

### Phase 6b: Append-only active tail plus published watermark

- [x] Convert `TailIndex` to a shared `ActiveTail`: postings and type column become chunked
      append-only vectors of atomic slots (`AppendColumn`, an append never moves existing
      elements; backbone growth doubles and publishes under a brief `RwLock<Arc>` swap),
      posting slots inline the 1-to-4-tag case and spill when hot, and the tag/type interners
      become concurrent `DashMap`s (reverse maps stay writer-only for the sealer)
- [x] Readers evaluate the active tail lock-free through a watermark-bounded `ActiveView`
      (the tail's `SegmentIndex` impl), replacing 6a's active-range scan on the indexed path;
      the `IndexSet` `!Sync` marker is dropped and `Send + Sync` is asserted. No lock held
      across query evaluation, enforced structurally (the evaluator sees only atomics,
      published `Arc` backbones, and single-shard `DashMap` probes)
- [x] Two orderings kept apart: slot contents `Relaxed` behind a higher release/acquire edge
      (watermark for the type column, per-slot `len` for postings); backbones cloned after the
      watermark load with `AppendColumn::get` bounds-checked, so a reader can never panic or
      read past its pinned prefix. `AppendColumn`/`PostingSlot` unit tests plus a concurrent
      active-index consistency test (`tests/read.rs`); the 6a differential and read-your-writes
      tests stay green with the active range now index-driven

### Phase 6c: Cost model (benchmark-first)

- [x] A read-path benchmark (`benches/read_path.rs`) measuring the index-vs-scan crossover
      *shape* (selectivity, range width, payload size), forcing each arm via `scan_bias` so
      the two curves are measured over one workload; the benchmark is the deliverable, not a
      tuned `K` (cold vs hot is a documented cache caveat, like write_path's fsync caveat)
- [x] Planner cost model (`index::plan`): estimate the result size from exact posting
      lengths (`SegmentIndex::term_len`, exact on the sealed segment, a watermark-bounded
      upper bound on the active tail) vs the post-pruning range width, biased toward scanning
      at the margin, `K` the `ReadConfig::scan_bias` knob, every decision trace-logged (the
      aggregate verdict at `debug`, per-item estimates at `trace`). The decision is
      whole-query for now; the per-item planner is deferred (CLAUDE.md 8), with the per-item
      logs as the data that would justify building it
- [x] Broad-projection bypass: a query whose estimated result is a large fraction of the
      range streams a filtered log scan rather than the index (`Query::all` stays the
      zero-copy unfiltered bypass). Proven answer-invariant across `scan_bias` values, so the
      planner only ever changes the path, never the result

### Phase 6d: Condition fallthrough onto the index

- [x] Wired the `TagTips` `Unknown` fallthrough to an early-terminating index existence check
      (`IndexSet::find_match`, over the active `ActiveTail` plus the sealed segments, probing
      touched segments in ascending order and returning the first match, reusing the one
      spec-pinned `index::search` evaluator) instead of a full scan. Scan fallback on an
      unindexable touched segment (logged at warn), plus a `WriterConfig::condition_force_scan`
      escape hatch. `find_match` differential-tested against the scan oracle (a `set.rs` unit
      test equals the scan's first match for every query/`after`; the `verify_tips` property
      test now cross-checks the `Unknown` index arm too)
- [x] Made `verify_tips` non-fatal: a fast-path/scan disagreement is logged and the writer
      degrades to the authoritative scan answer (panicking only in debug builds), never
      poisoning the writer thread; the pre-existing `DefinitelyNoMatch` `assert!` was folded
      into the same graceful path
- [x] Benchmark (`benches/condition_path.rs`): the `after: 0` uniqueness guard, index vs
      forced-scan, over growing history. Confirms the crossover (index near-flat at a few µs;
      scan linear in history, ~1000x slower at 32k events on tmpfs). The unindexable
      scan-fallback verdict is differential-tested to equal the indexed verdict
      (`coordinator.rs`)

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
