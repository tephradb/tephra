# Crash-test findings

Findings from the crash-consistency and fault-injection suite (`tephra-crashtest`). Per the
suite's charter, findings are reported here; fixes are a separate decision and are **not**
applied by the test suite.

---

## F1: store refuses to reopen after ENOSPC (or a crash) during segment extension

- **Severity:** availability / recovery-robustness. No acked data is lost, but the store
  becomes unopenable until an operator removes a stray file.
- **Status:** FIXED in this change (see "Fix" below). Originally reported unfixed per the "fixes
  are a separate decision" constraint; the fix was then requested.
- **Found by:** Phase 3 io-fault battery, scenario `enospc_segment_extend`.
- **Reproduce:** `tephra-crashtest --phase3 --phase3-cycles 10 --seed 3030`
  (the `enospc_segment_extend` row fails ~2-3 of 10 cycles: only the cycles where a segment
  rollover lands inside the fault window). Also reproducible under a plain `SIGKILL` that lands
  in the `create_new` -> `fallocate` window of a rollover, which is why random timing (Phase 1)
  rarely hits it and targeted injection does.

### What happens

When a segment rollover extends a new segment, `Writer::create`
(`crates/seglog/src/write.rs`) does `OpenOptions::new().create_new(true).open(path)` and then
`fallocate` to the full segment size. If `fallocate` fails with `ENOSPC` (disk full), or the
process is killed after `create_new` but before the header is written and synced, a **0-byte**
(sub-header-size) segment file is left in the data directory.

On the next open, `SegmentSet::open` reads each segment header:

```
crates/tephra/src/log/set.rs:349     let buf = read_header(path)?;
crates/tephra/src/log/set.rs:1145    fn read_header(path: &Path) -> Result<[u8; SEGMENT_HEADER_SIZE], LogError> {
crates/tephra/src/log/set.rs:1148        file.read_exact_at(&mut buf, 0)          // reads 64 bytes
crates/tephra/src/log/set.rs:1149            .map_err(|source| LogError::io(path, source))?;
```

`read_exact_at` of 64 bytes on a 0-byte file returns `UnexpectedEof` ("failed to fill whole
buffer"), which propagates out of `open` and the server refuses to start:

```
ERROR tephra_server: server exited with error
  err=i/o error at ".../00000000000000000158.log": failed to fill whole buffer
```

### Why this is a bug, not intended behavior

ARCHITECTURE.md section 4.7 states the intended handling explicitly:

> "Unwritten is legal only on the last file (crash between create and header write) and that
> file is deleted."

The deletion path exists at `crates/tephra/src/log/set.rs:361-370`, but it only triggers when
`read_header` **succeeds** and `SegmentHeader::from_bytes` returns `HeaderError::Unwritten`
(the all-zero, post-`fallocate` state). A sub-header-size file fails inside `read_header`
first, so the intended "delete the unwritten trailing file and continue" path is never reached.
The implementation handles the post-`fallocate` unwritten case but not the pre-`fallocate` /
short-file case, which is exactly the state ENOSPC-on-extend (and a crash in that window)
leaves behind.

### Fix (applied)

`SegmentSet::open` (`crates/tephra/src/log/set.rs`) now treats a segment file that is shorter than
a full header the same as an all-zero (`Unwritten`) header: a segment whose creation did not
finish and which holds no committed data. `read_header` returns `Option` (`None` for a short
file) instead of erroring, and the open path classifies every segment, then deletes the **trailing
run** of unwritten segments (a retrying rollover under ENOSPC can leave more than one) while still
rejecting an unwritten segment that precedes a valid one (a real gap). Regression test:
`open_deletes_short_trailing_segment_and_recovers` in `set.rs`. Verified: the
`enospc_segment_extend` scenario, which reproduced the bug, now passes.

### Scope of impact

- Acked writes are safe: the failed rollover's batch was never acked, so durability, the prefix
  property, and position monotonicity all still hold on the data that did commit.
- The cost is availability: after a disk-full event at a segment boundary (or an unlucky crash
  during rollover) the store will not start until the stray 0-byte `.log` is removed by hand.

---

## Phase 4: seeded-bug results (proving the suite catches bugs)

Four durability bugs were introduced one at a time (reverted after each) to confirm the suite
goes red. Result: **3 of 4 caught deterministically; the 4th is masked by ext4 and not
observable with the available tooling.**

| Seeded bug | Caught? | By | Cycles to catch |
|---|---|---|---|
| 1. Ack the client before the fsync | yes | `fsync_eio_all` scenario (durability violations; also breaks the acked==0 expectation) | cycle 0 |
| 2. Skip the trailing-record CRC in recovery | yes | `torn_marker` injection: recovery adopts a torn marker, the store then fails to open (`crc32c hash mismatch`). The clean binary passes the identical injection. | cycle 0 |
| 3. Rollover: new segment used before it is fsynced | no (see below) | would need a filesystem without ext4's fsync-persists-parent-dir behaviour, or dm-log-writes replay | not caught |
| 4. Drop the last entry of the index flush | yes | index-vs-log check: a `shard:7` query returned 4 positions via the index vs 23 in the log | cycle 3 |

**Methodology finding (bug 1).** Ack-before-fsync survived 300 SIGKILL cycles undetected: a
process kill preserves the page cache, and the early-acked data was already flushed there before
the fsync, so it is present after restart. The bug only surfaces under a real durability failure
(the fsync-EIO fault) or a genuine power cut. A SIGKILL-only suite would have passed it. This is
the core reason the fault-injection and power-loss layers exist.

**Bug 3 could not be made to lose data.** The injection removes the new segment's `sync_all` and
parent-directory fsync. It was exercised with real rollovers (2 to 10 segments per run, up to
1430 acked writes) under the dm-flakey power-loss test on both journaled ext4 and ext4 with the
journal disabled (`-O ^has_journal`). Every acked write survived every time. The reason is that
ext4's `fdatasync` also persists a newly-created file's directory entry (a known ext4
implementation behaviour, present even without a journal), so each batch's own fdatasync makes
its segment's directory entry durable, rendering the explicit parent-directory fsync redundant on
ext4. The baseline clean run showed `recovered == acked`, confirming `drop_writes` genuinely
dropped the non-durable tail, so the test has teeth; bug 3 simply does not manifest on ext4. The
explicit fsync remains correct defensive code for filesystems that lack this behaviour;
exposing its absence would require such a filesystem (that still supports fallocate, which ext2
does not) or dm-log-writes replay to the precise rollover-before-writeback window (the `replay-log`
tool is not installed here).
