#!/usr/bin/env bash
#
# Block-level power-loss test for Tephra using dm-flakey over a loop-backed file.
#
# A real power cut loses data that reached the OS page cache but was never fsynced, while data
# that was fsynced (forced to the platter) survives. A SIGKILL cannot show this: the page cache
# lives in the kernel and survives a process death. dm-flakey's `drop_writes` feature makes the
# block device silently discard writes, so we can drop exactly the not-yet-durable data:
#
#   1. Run a Tephra workload on an ext4 filesystem over dm-flakey (pass-through). Tephra fdatasyncs
#      each acked batch, so acked data is forced to the device during the run.
#   2. Flip dm-flakey to `drop_writes` and unmount: the dirty page-cache flush at unmount is
#      discarded, so non-fsynced writes never reach the device. This is the power cut.
#   3. Flip back to pass-through, remount (ext4 journal recovery runs), reopen the store, and run
#      the read-only invariants against the off-device witness.
#
# The test is safe by construction: losing non-acked data is allowed by the invariants, so it
# cannot produce a false failure. But if any ACKED write was not truly on the platter, it is
# absent after the drop and the durability invariant fails loudly.
#
# SAFETY: everything operates on a regular backing file exposed as a loop device. No real disk,
# partition, or host filesystem is touched. On exit (even on error) the mount, dm target, and loop
# device are torn down and the backing files removed. The witness is kept OFF the device.
#
# REQUIREMENTS: root (losetup/dmsetup/mount), the dm-flakey module, and mkfs.ext4. Missing pieces
# cause a clean SKIP (exit 3).
#
# USAGE:
#   sudo SERVER_BIN=$PWD/target/release/tephra-server-crash \
#        CRASHTEST_BIN=$PWD/target/release/tephra-crashtest \
#        crates/tephra-crashtest/scripts/power_loss_dm_flakey.sh
#
# Repeat with different SEEDs (and larger WRITERS) for more coverage.
set -euo pipefail

SERVER_BIN="${SERVER_BIN:-$PWD/target/release/tephra-server-crash}"
CRASHTEST_BIN="${CRASHTEST_BIN:-$PWD/target/release/tephra-crashtest}"
WORKDIR="${WORKDIR:-/tmp/tephra-powerloss}"
DATA_SIZE_MB="${DATA_SIZE_MB:-512}"
SEGMENT_SIZE="${SEGMENT_SIZE:-65536}"
WRITERS="${WRITERS:-12}"
SEED="${SEED:-424242}"
# Filesystem under test and mkfs options. ext4 (default) journals metadata, so a segment's
# fdatasync also persists its directory entry, masking a missing directory fsync. Pass
# MKFS_OPTS="-O ^has_journal" to build ext4 WITHOUT a journal: it still supports fallocate (which
# Tephra requires, and which ext2 lacks) but no longer commits the directory entry atomically, so
# a missing directory fsync is exposed.
FSTYPE="${FSTYPE:-ext4}"
MKFS_OPTS="${MKFS_OPTS:-}"
# Workload duration; large enough to roll over several segments when SEGMENT_SIZE is small.
WORKLOAD_MS="${WORKLOAD_MS:-0}"
DM_NAME="tephra_flakey_$$"

skip() { echo "SKIP: $*" >&2; exit 3; }
[[ "$(id -u)" == "0" ]] || skip "needs root (losetup/dmsetup/mount); re-run under sudo."
modprobe dm-flakey 2>/dev/null || [[ -e /sys/module/dm_flakey ]] || skip "dm-flakey module unavailable."
command -v "mkfs.$FSTYPE" >/dev/null || skip "mkfs.$FSTYPE not found."
[[ -x "$SERVER_BIN" ]] || skip "server binary $SERVER_BIN missing (cargo build --release -p tephra-server --features crash-points)."
[[ -x "$CRASHTEST_BIN" ]] || skip "crashtest binary $CRASHTEST_BIN missing (cargo build --release -p tephra-crashtest)."

DATA_IMG="$WORKDIR/data.img"; MNT="$WORKDIR/mnt"; WITNESS_DIR="$WORKDIR/witness"
DATA_LOOP=""

cleanup() {
  set +e
  mountpoint -q "$MNT" && umount "$MNT"
  dmsetup remove "$DM_NAME" 2>/dev/null
  [[ -n "$DATA_LOOP" ]] && losetup -d "$DATA_LOOP" 2>/dev/null
  rm -rf "$WORKDIR"
}
trap cleanup EXIT

mkdir -p "$WORKDIR" "$MNT" "$WITNESS_DIR"
truncate -s "${DATA_SIZE_MB}M" "$DATA_IMG"
DATA_LOOP="$(losetup -f --show "$DATA_IMG")"
SECTORS="$(blockdev --getsz "$DATA_LOOP")"

# Pass-through: up_interval large, down_interval 0 => always up, normal I/O.
PASSTHRU="0 $SECTORS flakey $DATA_LOOP 0 60 0"
# Always down with the drop_writes feature => writes silently discarded, reads pass.
DROP="0 $SECTORS flakey $DATA_LOOP 0 0 60 1 drop_writes"

dmsetup create "$DM_NAME" --table "$PASSTHRU"
DM_DEV="/dev/mapper/$DM_NAME"
"mkfs.$FSTYPE" -q -F $MKFS_OPTS "$DM_DEV"
mount "$DM_DEV" "$MNT"

echo "==> running Tephra workload on $FSTYPE over the logged device (witness off-device)"
WORKLOAD_ARGS=(--server-bin "$SERVER_BIN" --data-root "$MNT/run" --witness-root "$WITNESS_DIR" \
  --seed "$SEED" --writers "$WRITERS" --segment-size "$SEGMENT_SIZE" --workload-only)
[[ "$WORKLOAD_MS" != "0" ]] && WORKLOAD_ARGS+=(--workload-ms "$WORKLOAD_MS")
ACKED_LINE="$("$CRASHTEST_BIN" "${WORKLOAD_ARGS[@]}")"
echo "    $ACKED_LINE"
# Report how many segments the workload created, so we can see a rollover actually happened.
echo "    segments created: $(ls "$MNT"/run/cycle-000000/data/*.log 2>/dev/null | wc -l)"

echo "==> simulating power cut: dropping all not-yet-durable writes"
dmsetup suspend "$DM_NAME"
dmsetup reload "$DM_NAME" --table "$DROP"
dmsetup resume "$DM_NAME"
# Unmount while writes are dropped: the dirty page-cache flush is discarded, so anything not
# fsynced to the device before this point is now gone.
umount "$MNT"
dmsetup suspend "$DM_NAME"
dmsetup reload "$DM_NAME" --table "$PASSTHRU"
dmsetup resume "$DM_NAME"

echo "==> recovering the filesystem and verifying the store"
# fsck before mount: a journal-less filesystem needs it to become mountable after an unclean cut,
# and the fsck finalises the loss of any orphaned segment whose directory entry was never fsynced
# (the durability effect under test). Harmless on a journaled fs (it just replays and checks).
if command -v e2fsck >/dev/null; then
  e2fsck -fy "$DM_DEV" >/dev/null 2>&1 || true
fi
mount "$DM_DEV" "$MNT"
rc=0
"$CRASHTEST_BIN" --server-bin "$SERVER_BIN" --segment-size "$SEGMENT_SIZE" \
  --verify-dir "$MNT/run/cycle-000000/data" --witness "$WITNESS_DIR/cycle-000000.log" || rc=$?
umount "$MNT"

if [[ $rc -eq 0 ]]; then
  echo "POWER-LOSS TEST PASSED: every acked write survived the cut and the store is consistent."
else
  echo "POWER-LOSS TEST FAILED: see violations above." >&2
fi
exit $rc
