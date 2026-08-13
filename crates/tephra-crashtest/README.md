# tephra-crashtest

A crash-consistency and fault-injection test harness for Tephra. It spawns the real
`tephra-server` as a child process on real disk, drives it with concurrent seeded writers through
`tephra-client`, records a durable witness log as ground truth, crashes the server (SIGKILL, a
self-abort at a named crash point, or a simulated power cut), restarts it, and checks a set of
durability invariants after recovery.

See `REPORT.md` for the findings and `FINDINGS.md` for the open bug (F1).

## Building

The server must be built with the gated crash points enabled (off by default, zero cost):

```sh
cargo build --release -p tephra-server --features crash-points
cargo build --release -p tephra-crashtest
```

The harness locates the server via `--server-bin` or `$TEPHRA_SERVER_BIN`, defaulting to
`target/debug/tephra-server`. For the crash-point and fault modes, point it at the
`crash-points`-enabled build.

## Invariants checked after every crash

1. Durability: every acked write is present at its position, byte-identical.
2. Prefix: positions are `1..=max` with no holes.
3. No phantom acks: every recovered event was sent and matches its regenerated content.
4. In-flight are either/or: a sent-not-acked write may be present or absent, never partial.
5. No torn records: recovery truncates to the last complete, CRC-valid record.
6. Position monotonicity across restarts: the next append lands at `recovered_max + 1`.
7. Index and log agree: index/planner query results equal the log-filtered results, and are
   identical after the index is deleted and rebuilt from the log.
8. DCB integrity: a condition conflicting with a recovered event is still rejected after recovery.

Ground truth is a witness log fsynced per record on a directory separate from the data dir, so it
survives a crash of the harness itself.

## Running

Random-timing SIGKILL soak (the default mode):

```sh
./target/release/tephra-crashtest --server-bin target/release/tephra-server-crash \
  --seed 1 --cycles 500 --writers 12 --segment-size 32768 --workload mixed
```

- `--workload` one of `pure`, `conditional`, `subscription`, `mixed`.
- Small `--segment-size` forces frequent rollover and index flushing.
- On any violation the seed and cycle are printed and the data dir + witness are copied to
  `--artifact-dir`.

Targeted crash-point battery (Phase 2), one abort per instrumented site:

```sh
./target/release/tephra-crashtest --phase2 --phase2-cycles 8 \
  --server-bin target/release/tephra-server-crash
```

I/O-fault battery (Phase 3): fsync EIO, ENOSPC on segment extend and index flush, short write:

```sh
./target/release/tephra-crashtest --phase3 --phase3-cycles 10 \
  --server-bin target/release/tephra-server-crash
```

A single named crash point (aborts the server at the site, then recovers and checks):

```sh
./target/release/tephra-crashtest --crash-point "after_fsync_before_ack:abort:5" \
  --cycles 8 --server-bin target/release/tephra-server-crash
```

Crash-point format is `site:action[:skip]`, action one of `abort`, `eio`, `enospc`, `shortwrite`;
`skip` lets earlier hits pass so recovery has real prior state. Sites: `commit_before_fsync`,
`after_fsync_before_ack`, `partial_ack`, `segment_created_before_commit`, `index_after_write`,
`index_after_sync`, `recovery_midway`, `torn_marker`, `commit_fsync`, `segment_extend`,
`index_flush`, `commit_shortwrite`.

Long soak across mixed workloads plus a periodic targeted battery:

```sh
DURATION_SECS=1800 bash crates/tephra-crashtest/scripts/soak.sh
```

## Block-level power loss (root)

`scripts/power_loss_dm_flakey.sh` runs a workload on ext4 over a `dm-flakey` device, then flips the
device to `drop_writes` and unmounts so every not-yet-fsynced write is discarded (a real page-cache
losing power cut), then reopens and checks. Safe: it only touches a loop-backed file; full teardown
on exit.

```sh
sudo SERVER_BIN=$PWD/target/release/tephra-server-crash \
     CRASHTEST_BIN=$PWD/target/release/tephra-crashtest \
     bash crates/tephra-crashtest/scripts/power_loss_dm_flakey.sh
```

Knobs: `SEED`, `WRITERS`, `SEGMENT_SIZE`, `WORKLOAD_MS` (force rollover), `FSTYPE`, `MKFS_OPTS`
(for example `-O ^has_journal` for journal-less ext4).

`scripts/power_loss_dm_log_writes.sh` is the higher-fidelity variant using `dm-log-writes` +
`replay-log` (xfstests); it replays to each flush boundary. It requires the `replay-log` tool.

## Crash points compile out

With the `crash-points` feature off (the default), every `crash_point!` / `crash_io!` macro and the
`torn_marker` block expand to nothing: no branch, no symbol, no runtime cost. Verified against the
default release build; all existing `seglog` and `tephra` tests pass unchanged.
