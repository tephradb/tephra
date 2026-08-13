# Tephra crash-consistency and fault-injection report

The deliverable asked for a suite that finds real durability bugs, and proof that it can. This
report gives: the durability and ack points found in the code; every invariant violation found;
which seeded bugs the suite caught; the crash-cycle counts by failpoint; and an explicit list of
what the suite does **not** cover.

The harness lives in `crates/tephra-crashtest`; the gated instrumentation in seglog's
`crash_points` module plus one-line call sites in `seglog` and `tephra`. Findings are in
`FINDINGS.md`.

---

## 1. The durability point and the ack point

The ack is sent **strictly after** the fsync of the containing batch returns. This is the single
fact the whole exercise turns on, and it is correct.

- **Durable at** `crates/seglog/src/write.rs`, in `Writer::sync()`:

  ```rust
  self.writer.flush()?;
  self.writer.get_ref().sync_data()?;   // fsync returns => the batch is durable
  self.flushed_offset.set(self.write_offset);
  ```

  reached from `Writer::commit()` (writes the `BATCH_COMMIT` marker, then `sync()`), reached from
  `SegmentSet::append_batch` (`crates/tephra/src/log/set.rs:558-560`).

- **Acked at** `crates/tephra/src/writer/batch.rs:134`, in `commit_ok()`:

  ```rust
  let _ = s.reply.send((s.token, Ok(s.range)));
  ```

  called only from `crates/tephra/src/writer/coordinator.rs:300`, which runs **after**
  `self.set.append_batch(...)` returned `Ok` at `coordinator.rs:267`. The comment at 269-271 says
  it plainly: *"The write is already durable here, so nothing below can turn it into a failure."*

Consequence: an acked write is always past its fsync, so it survives both a process crash and a
power cut. Empirically confirmed by ~thousands of crash cycles with zero durability violations,
and by the seeded ack-before-fsync bug (which the suite catches; see section 3).

Two structural facts, both confirmed in code, that shaped the tests:

- **No manifest file.** Segment topology is rebuilt from filenames `{base:020}.log` on open
  (`SegmentSet::open`). The task's "manifest" failpoints became segment-header / directory-fsync
  points.
- **The index is disposable.** One `.idx` per sealed segment, rebuilt from the log if missing,
  corrupt, or mismatched. The task's "FST before postings" failpoint became a torn-`.idx` +
  rebuild-from-log check.

---

## 2. Invariant violations found

### F1 (real finding): store refuses to reopen after ENOSPC (or a crash) during segment extension

Full write-up in `FINDINGS.md`. Summary:

- **Trigger:** `ENOSPC` on the `fallocate` that extends a new segment during rollover, or a crash
  in the `create_new` -> `fallocate` window. A 0-byte segment file is left in the data dir.
- **Effect:** `SegmentSet::open` -> `read_header` (`crates/tephra/src/log/set.rs:349`, `:1145`) does
  `read_exact_at` of 64 bytes, which fails `UnexpectedEof` on the 0-byte file, and the server
  refuses to open the whole store (`... failed to fill whole buffer`).
- **Why a bug:** ARCHITECTURE.md 4.7 says an unwritten trailing segment is meant to be deleted and
  skipped; that path (set.rs:361) only handles the post-`fallocate` all-zero case, not the
  pre-`fallocate` short-file case.
- **Severity:** availability / recovery-robustness. No acked data is lost. The store will not start
  until the stray 0-byte `.log` is removed by hand.
- **Reproduce (before the fix):** `tephra-crashtest --phase3 --phase3-cycles 10 --seed 3030` (the
  `enospc_segment_extend` row failed ~2-3 of 10 cycles).
- **Status:** FIXED in this change. `SegmentSet::open` now treats a sub-header-size segment file as
  unwritten and deletes the whole trailing run of unwritten segments (a retrying ENOSPC rollover can
  leave more than one), erroring only when an unwritten segment precedes a valid one. Regression
  test `open_deletes_short_trailing_segment_and_recovers`; the `enospc_segment_extend` scenario now
  passes cleanly. See `FINDINGS.md` for detail.

No other invariant violations were found in the clean build across all phases.

---

## 3. Seeded bugs (Phase 4): does the suite bite?

Four durability bugs, introduced one at a time and reverted after each.

| Seeded bug | Caught | By | Cycles to catch |
|---|---|---|---|
| 1. Ack the client before the fsync | yes | `fsync_eio_all` scenario | 0 (first cycle) |
| 2. Skip the trailing-record CRC in recovery | yes | `torn_marker` injection (store then won't open) | 0 (first cycle) |
| 3. Rollover uses the new segment before it is fsynced | **no** (masked on ext4) | would need a non-ext4 FS or dm-log-writes replay | not caught |
| 4. Drop the last entry of the index flush | yes | index-vs-log diff | 3 (4th cycle) |

- **Bug 1** survived **300 SIGKILL cycles** undetected, because a process kill preserves the page
  cache and the early-acked data was already flushed there before the fsync. It only fell to the
  fsync-EIO fault. A SIGKILL-only suite would have shipped this bug: this is the core justification
  for the fault-injection and power-loss layers.
- **Bug 2** was caught with a new deterministic `torn_marker` injection (a SIGKILL cannot produce a
  torn record; the page cache is intact). The clean binary passes the identical injection, proving
  it is the CRC-skip that breaks, not the injection.
- **Bug 3** could not be made to lose data despite real rollovers (2-10 segments, up to 1430 acked
  writes) under dm-flakey power loss on both journaled and journal-disabled ext4. Reason: ext4's
  `fdatasync` also persists a newly-created file's directory entry, so each batch's fdatasync makes
  its segment durable, rendering the explicit parent-dir fsync redundant *on ext4*. The
  clean-baseline `recovered == acked` confirmed `drop_writes` genuinely dropped the non-durable
  tail, so the test has teeth; bug 3 simply does not manifest on ext4. The explicit fsync remains
  correct defensive code for filesystems without this behaviour.

So: 3 of 4 caught deterministically. The 4th is a real "does not manifest on this filesystem"
result, documented rather than papered over.

---

## 4. Crash cycles run, by failpoint

Approximate, across all phases (excluding the final soak, appended after it completes):

| Category | Cycles | Notes |
|---|---|---|
| Phase 1 random SIGKILL (pure/mixed/subscription) | ~410 | 0 violations; `recovered > acked` in-flight case exercised |
| Phase 2 targeted aborts (7 sites x battery + evidence) | ~100 | every site hit; 0 violations |
| Phase 3 io-faults (fsync EIO, ENOSPC x2, short write) | ~115 | fsync EIO acks nothing; F1 surfaced on enospc_segment_extend |
| Phase 3 block-level power loss (dm-flakey) | ~30 | acked writes always durable; drop confirmed active |
| Phase 4 seeded-bug runs | ~750 | 3/4 bugs caught |
| Phase 5 soak (mixed workloads + periodic Phase 2 battery) | ~4,815 | 0 violations |

**Soak (Phase 5):** a 30-minute mixed-workload soak ran **75 rounds, ~4,815 crash cycles, 0
invariant violations**, verifying **140,025 acked writes**. Rounds were spread across pure (24),
DCB-conditional/mixed (28), and subscription (23) workloads and segment sizes 16 KiB / 32 KiB /
64 KiB (constant rollover and index flushing), with 15 interleaved Phase 2 abort batteries (all
passed). Sustained ~12,700 crash cycles/hour. The soak is time-bounded by `DURATION_SECS`
(`scripts/soak.sh`) and can run for hours unchanged; 30 minutes was the representative sample taken
here. Grand total across all phases: **~6,200 crash cycles, one real finding (F1), zero durability
violations in the clean build.**

Phase 2 failpoint sites (each driven until it fired, then full invariant check): `commit_before_fsync`,
`after_fsync_before_ack`, `partial_ack`, `segment_created_before_commit`, `index_after_write`,
`index_after_sync`, `recovery_midway`. Phase 3 io-fault sites: `commit_fsync` (EIO), `segment_extend`
(ENOSPC), `index_flush` (ENOSPC), `commit_shortwrite`. Deterministic torn-record site added for
bug 2: `torn_marker`.

Throughput sustained ~13,000-18,000 crash cycles/hour (target was several hundred/hour).

**Soak (Phase 5):** _to be filled in when the 30-minute mixed-workload soak finishes._

---

## 5. What this suite does NOT cover

Read this before treating a green run as more assurance than it is.

- **Bug 3 class (missing directory fsync) on ext4.** ext4's fdatasync persists the parent
  directory entry, so this suite cannot expose a missing segment-directory fsync on ext4. A
  filesystem without that behaviour (that still supports fallocate) or dm-log-writes replay would
  be needed. ext2 cannot be used (no fallocate, which Tephra requires).
- **Block-level reorder / arbitrary-point power loss.** The `replay-log` tool (xfstests) is not
  installed here, so dm-log-writes replay to every flush boundary was not run. The dm-flakey test
  covers "non-fsynced writes are dropped at a power cut," not "writes are reordered up to a flush"
  or "the store is checked at every intermediate flush." A dm-log-writes script is provided
  (`scripts/power_loss_dm_log_writes.sh`) for when replay-log is available.
- **Non-Linux / non-ext4 durability semantics.** All runs were Linux + ext4 (and ext4 without a
  journal). Other filesystems (xfs, btrfs, zfs) and other OSes are untested; their fsync and
  directory-durability semantics differ.
- **Real hardware.** Everything ran against the OS page cache and a loop-backed file. Drive
  write-cache lies, controller-cache power loss, and firmware bugs below the block layer are out of
  scope.
- **Byzantine / bit-rot corruption beyond the CRC-detectable class.** The suite checks CRC-guarded
  recovery; it does not model silent multi-bit corruption that happens to pass CRC, nor adversarial
  on-disk tampering.
- **Multi-node / replication.** None exists in Tephra; nothing here tests cross-node consistency.
- **Determinism limits.** Content is fully seeded, but thread scheduling (writer interleaving,
  group-commit batching, the exact SIGKILL instant) is not controlled. A green run is therefore
  probabilistic coverage, not a proof; reproduction uses the seed plus the cycle index, and thread
  timing may still vary.
- **Long-horizon effects.** Retention, archival, cold-segment recompression, and index-segment
  merging are deferred in Tephra and are not exercised. Very large logs (startup reading
  everything) were not soaked at scale.
- **The witness is single-writer-per-record durable, not lock-free-verified.** It is fsynced per
  record on a separate directory (same filesystem, not a separate physical device), which is enough
  to survive a harness crash but is not itself a distributed ground truth.
