# Site progress

State of the Tephra site build. Updated as pages are drafted and verified. This file survives
context compaction: read it first to resume.

## Decisions

### Server-first (load-bearing)

The standalone TCP server is the common deployment; embedding the engine is the minority case. So
every page a reader follows to run something speaks `tephra-client` against a running server, and
embedding gets its own page (`embedded.mdx`) rather than appearing alongside the server path.

- Landing, getting started, core concepts, and guides use the client's `tephra_client::Event` as
  the default `Event`.
- The engine's packed `tephra::Event` appears only on `embedded.mdx` and `architecture.mdx`, named
  explicitly, because there are two same-named `Event` types and conflating them is a real trap.
- Why: it matches how the store is actually deployed, and it keeps the least ergonomic corner of
  the API (opening a `SegmentSet`) off the first page a reader hits.

### Engine changes made to support the docs

- Added crate-root re-exports so `tephra::{SegmentSet, SegmentConfig}` resolve (they were only at
  `tephra::log::set::`). A store-opening example needing the least ergonomic import path was the
  signal to fix the surface.
- Changed `Tags::new` from `impl Into<SmallVec<[Tag; 4]>>` to `impl IntoIterator<Item = Tag>`, so
  array literals of any length work (`Tags::new([a])`), not only exactly-four-element arrays. Fully
  backward compatible; the whole workspace still builds.

### Benchmark treatment (rerun landed 2026-08-11, all tables now published)

The 256 MiB rerun the earlier hold waited for is done, so every comparative table is now published.
Source: pre-aggregated dashboard JSON at
`pyeventsourcing/event-store-benchmark/web/public/data/` (`2026-08-09T18-43-03.json` = DCB group,
`2026-08-10T04-44-40.json` = Stream/universal group, plus the `tephra-seg-*` / `segsweep-*` sweeps).
Interactive dashboard: https://event-store-benchmark.tqwewe.com. Harness (Ari's fork/branch):
https://github.com/tqwewe/event-store-benchmark/tree/fair-benchmarks-web-ui. Both linked from
`comparison.mdx`.

- **Write comparison published (three-way, conditional).** DCB group, all appends carry a
  one-tag-one-type condition, Tephra + Axon at matched 256 MiB segments, UmaDB page-based. Batch
  sweep at 64 writers: Tephra 21,397 / 533,955 / 984,881 ev/s (batch 1/64/512); UmaDB 6,266 /
  82,264 / 98,614; Axon 9,736 / 22,130 / 2,822 (batch-512 latency-bound). Unbatched scaling to 128
  writers: Tephra 32,738 (p50 3.8 ms), UmaDB 7,776, Axon 10,272.
- **Operations batch table refreshed to 256 MiB** (16 writers): batch 1/64/512 = 6,464 / 192,794 /
  796,724 ev/s, p50 2.4 / 5.2 / 9.7 ms. Peak at 64 writers noted (984,881 at batch 512).
- **Landing contrast refreshed:** 6,464 (batch 1) vs 796,724 (batch 512) at 16 writers, same box.
- **Reads split into warm and cold.** Warm (50k events, in memory): Tephra now leads selective
  reads (single-entity 381k vs UmaDB 240k vs Axon 170k) and ties UmaDB on full scan (1.996M vs
  1.950M, UmaDB tighter tail). This flips the old 16 MiB story where UmaDB won scans.
- **Cold reads are the new honest loss.** 8M events, larger than the 4 GB container memory, so reads
  hit disk. Cold selective read: UmaDB 169,226 vs Tephra 10,894 (256 B) and 173,368 vs 11,955
  (1 KiB), ~15x. This is the random-point-read case the architecture predicted UmaDB wins; now
  measured and stated plainly on `comparison.mdx`.
- **Wider field (Stream group, unconditional writes):** added KurrentDB (= EventStoreDB rebranded),
  Postgres/Marten, EventSourcingDB, Fact. Batched write at 64 writers: KurrentDB 179,329, Marten
  66,586, vs Tephra's conditional 984,881 (Tephra's is the harder path, so the gap understates it).
  EventSourcingDB and Fact could not complete batched/flood writes (near-zero, high errors), so they
  are excluded, matching the session note. Note: the universal-group Tephra write run is byte-
  identical to the DCB one (reused), so Tephra is compared conditional-vs-unconditional, never
  presented as an independent unconditional number.

### Axon reinstated (correction to an earlier wrong call, still holds)

An earlier note said Axon's whole column was tainted; that was wrong (zero operation errors across
runs, the `Unknown Context default` messages clustered in the first ~2 s of boot, trial shutdown
~12 h out). Axon is a full participant in the write and warm-read tables. Edition disclosed on the
site: `axoniq/axonserver:2026.0.5-jdk-21-nonroot`, unregistered twelve-hour trial, no licence,
single-node standalone DCB, not a free or SE edition. Axon writes ran at 256 MiB (matched to
Tephra); Axon reads ran at 16 MiB (a fair 256 MiB read would need reseeding the >4 GB cold corpus
and its cold-read path OOM'd at 256; reads are near segment-insensitive, so 16 MiB can only
understate Axon, never flatter it). Axon is absent from cold reads (OOM).

### The batch-512 latency finding (now sits alongside the published write table)

Kept on `comparison.mdx`, updated to the 256 MiB dataset. Axon's per-append latency climbs with
batch size: p50 39 ms at batch 64 / 16 writers, 183 ms at 64 writers, and at batch 512 it completes
too few appends in 15 s to sample latency (N < 1000, percentiles suppressed), throughput 2,822 ev/s,
below its own batch-64 result. Tephra commits the same batch-512 append in p50 33 ms at 64 writers.
The old 2.42 s / ~101-append figures came from the earlier session and are **not** used: in this
dataset Axon's batch-512 latency is suppressed, so the finding is framed on the suppression plus the
throughput drop. The memory angle stays dropped.

## Follow-up work (survives to the next session)

### Rerun the comparative write benchmarks, then publish

All comparative write-throughput tables (Tephra vs UmaDB and Tephra vs Axon) await this rerun. The
batch-512 latency finding is already published and is independent of it.

1. Rerun the write benchmarks against Tephra at `b5d2d96` (or later) with the current 256 MiB
   default segment size, not the 16 MiB the archived runs used.
2. Add cold reads over a dataset larger than the page cache, so the read numbers characterise disk
   reads and not just the in-memory case the current numbers cover.
3. Then publish the three-way write comparison (Tephra, UmaDB, Axon; batch-512 still out of the
   throughput table) and the cold-read numbers on `comparison.mdx`, replacing the "Write throughput,
   held" section.

## Page tracker

All pages drafted and building. `npm run build` is clean; `cargo test -p tephra-site-examples`
passes (every shown snippet is a compiled, run test).

| Page | Drafted | Verified |
|---|---|---|
| index (landing) | yes | build |
| introduction | yes | build |
| getting-started | yes | build |
| embedded | yes | build |
| core-concepts | yes | build |
| guides/decision-models | yes | build |
| guides/uniqueness-guard | yes | build |
| guides/subscriptions | yes | build |
| guides/conflicts | yes | build |
| operations | yes | build |
| architecture | yes | build |
| comparison | yes | build |
| status | yes | build |

## Flagged for the human

- Repo URLs use `github.com/tqwewe/tephra` (the public repo; `dcbdb` was the old name). External
  link checks pass only once that repo and the benchmark repo are public.
- Every performance figure traces to the benchmark harness output (not committed to this repo) and
  carries hardware, storage, fsync latency, event size, run duration, and versions. No figure is
  from memory.
