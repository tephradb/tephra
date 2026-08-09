# Benchmarks

Write-path benchmarks for the event store, built on
[criterion](https://github.com/bheisler/criterion.rs). They exercise layer 2 (the write
coordinator) end to end through its public API, so the numbers are the store's real append
latency and throughput, not microbenchmarks of internal functions.

Read paths (selective query, projection catch-up) are deliberately not benchmarked yet:
that path is a phase-6 concern (reads still run on the writer thread and return positions
rather than events), so a read benchmark now would measure a placeholder rather than the
intended design.

## Running

```sh
cargo bench --bench write_path                          # everything
cargo bench --bench write_path -- group_commit          # one group (filter by name)
cargo bench --bench write_path -- append_latency/single_event
```

Criterion writes HTML reports and keeps history under `target/criterion/`, so a second run
reports change vs the previous one (regression tracking for free).

## The fsync caveat (read before trusting a number)

The store's core claim is that "the ceiling is fsync, not ordering." A `TempDir` under
`/tmp` is very often a `tmpfs` (RAM) mount where `fsync` is effectively free, so latencies
there look 10x to 100x better than any real disk. That is fine for catching regressions,
but it is **not** a fair comparison against another durable store.

To measure the real durability ceiling, point the harness at the storage device under
test:

```sh
TEPHRA_BENCH_DIR=/mnt/nvme cargo bench --bench write_path
```

## Groups

| Group | What it isolates |
|---|---|
| `append_latency` | One event per `append`, synchronous. The fsync-bound latency floor, reported as appends/sec. |
| `batch_size` | N events in a single atomic `append`. Fsync amortization *within* one request; per-event cost should fall as N grows. |
| `payload_size` | Single-event appends over 64 B to 16 KiB. Separates the fixed per-append cost from payload cost (reported as bytes/sec). |
| `group_commit` | T concurrent writer threads (1 to 16). The headline number: the coordinator coalesces independent appends into one fsync, so aggregate appends/sec should rise with T until the fsync ceiling. |
| `conditional_append` | `unconditional` baseline vs `unique_guard` (uniqueness-guard pattern). The delta is the append-condition cost on its fast-reject path (`TagTips`, no log scan). |

## Comparing against other stores

Keep the workload identical: same event size, same tag shape, same durability setting
(their equivalent of fsync-per-commit must be on), and the same physical device via
`TEPHRA_BENCH_DIR`.

### `compare` bench: tephra vs umadb

`benches/compare.rs` benchmarks tephra against [umadb](https://umadb.io), another
DCB-compliant event store, through umadb's embedded (no gRPC) append API. It is gated
behind the `umadb-compare` feature so the default build never needs umadb:

```sh
cargo bench --features umadb-compare --bench compare
cargo bench --features umadb-compare --bench compare -- append_latency
```

Each group pairs the two engines (`tephra` vs `umadb`) under the same workload as
`write_path`, one durable commit per call. Both are fully durable per append (tephra: one
fsync via the group-commit path; umadb: two fsyncs via its copy-on-write dual-header
B+tree), so it is an apples-to-apples "cost of one durable append" comparison of the two
storage designs.

Two scoping notes:

- **Single-threaded only.** umadb's embedded `append` is single-writer by convention (its
  concurrency is provided by the server writer thread, out of scope here), so there is no
  concurrent group. tephra's concurrency lives in the `group_commit` group of `write_path`.
- **Single-threaded tephra pays a thread hop.** A lone caller still round-trips to tephra's
  writer thread per append; the coalescing win only shows under concurrency. Read the
  `compare` numbers as per-append storage cost, and `write_path`'s `group_commit` for the
  concurrency story.

> umadb is a git dependency pinned to the rev the comparison was run against, and is only
> fetched when the `umadb-compare` feature is enabled, so the default build never needs it.
