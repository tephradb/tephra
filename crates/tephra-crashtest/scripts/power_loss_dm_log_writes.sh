#!/usr/bin/env bash
#
# Block-level power-loss test for Tephra using dm-log-writes over a loop-backed file.
#
# dm-log-writes records every block write and every flush/FUA to a separate log device while
# passing writes through to the data device. After a workload we replay the log to two points and
# check Tephra's read-only invariants at each: this exercises what a SIGKILL cannot, namely writes
# that reached the page cache but were never fsynced being absent, and reordering up to a flush.
#
#   1. full replay          -> the final on-disk state. Every acked write must be present.
#   2. replay to last flush  -> a power cut at the last sync. Writes after it may be gone, but
#                              every acked write (acked only after its fsync) must still be present.
#
# SAFETY: everything operates on regular backing files exposed as loop devices. No real disk,
# partition, or host filesystem is touched. On exit (even on error) the mount, dm target, and loop
# devices are torn down and the backing files removed. The witness log is kept OFF the device so it
# survives as ground truth.
#
# REQUIREMENTS: root (losetup/dmsetup/mount), the dm-log-writes module, mkfs.ext4, and the
# `replay-log` tool from xfstests (src/log-writes). Missing pieces cause a clean SKIP (exit 3).
#
# USAGE:
#   sudo SERVER_BIN=$PWD/target/release/tephra-server-crash \
#        CRASHTEST_BIN=$PWD/target/release/tephra-crashtest \
#        crates/tephra-crashtest/scripts/power_loss_dm_log_writes.sh
set -euo pipefail

SERVER_BIN="${SERVER_BIN:-$PWD/target/release/tephra-server-crash}"
CRASHTEST_BIN="${CRASHTEST_BIN:-$PWD/target/release/tephra-crashtest}"
WORKDIR="${WORKDIR:-/tmp/tephra-powerloss}"
DATA_SIZE_MB="${DATA_SIZE_MB:-512}"
SEGMENT_SIZE="${SEGMENT_SIZE:-65536}"
DM_NAME="tephra_plog_$$"

skip() { echo "SKIP: $*" >&2; exit 3; }
[[ "$(id -u)" == "0" ]] || skip "needs root (losetup/dmsetup/mount); re-run under sudo."
modprobe dm-log-writes 2>/dev/null || [[ -e /sys/module/dm_log_writes ]] || skip "dm-log-writes module unavailable."
command -v mkfs.ext4 >/dev/null || skip "mkfs.ext4 not found."
REPLAY_LOG="$(command -v replay-log || true)"
[[ -n "$REPLAY_LOG" ]] || skip $'replay-log not found. Build from xfstests:\n  git clone https://git.kernel.org/pub/scm/fs/xfs/xfstests-dev.git\n  make -C xfstests-dev/src/log-writes\n  export PATH="$PWD/xfstests-dev/src/log-writes:$PATH"'
[[ -x "$SERVER_BIN" ]] || skip "server binary $SERVER_BIN missing (cargo build --release -p tephra-server --features crash-points)."
[[ -x "$CRASHTEST_BIN" ]] || skip "crashtest binary $CRASHTEST_BIN missing."

DATA_IMG="$WORKDIR/data.img"; LOG_IMG="$WORKDIR/log.img"; MNT="$WORKDIR/mnt"
WITNESS_DIR="$WORKDIR/witness"      # off the device under test
DATA_LOOP=""; LOG_LOOP=""

cleanup() {
  set +e
  mountpoint -q "$MNT" && umount "$MNT"
  dmsetup remove "$DM_NAME" 2>/dev/null
  [[ -n "$DATA_LOOP" ]] && losetup -d "$DATA_LOOP" 2>/dev/null
  [[ -n "$LOG_LOOP" ]] && losetup -d "$LOG_LOOP" 2>/dev/null
  rm -rf "$WORKDIR"
}
trap cleanup EXIT

mkdir -p "$WORKDIR" "$MNT" "$WITNESS_DIR"
truncate -s "${DATA_SIZE_MB}M" "$DATA_IMG"
truncate -s "${DATA_SIZE_MB}M" "$LOG_IMG"
DATA_LOOP="$(losetup -f --show "$DATA_IMG")"
LOG_LOOP="$(losetup -f --show "$LOG_IMG")"

SECTORS="$(blockdev --getsz "$DATA_LOOP")"
dmsetup create "$DM_NAME" --table "0 $SECTORS log-writes $DATA_LOOP $LOG_LOOP"
DM_DEV="/dev/mapper/$DM_NAME"

mkfs.ext4 -q -F "$DM_DEV"
mount "$DM_DEV" "$MNT"

echo "==> running Tephra workload on the logged device (witness kept off-device)"
# One cycle: writers drive the server, the harness SIGKILLs it. --keep-dirs leaves the store.
"$CRASHTEST_BIN" --server-bin "$SERVER_BIN" \
  --data-root "$MNT/run" --witness-root "$WITNESS_DIR" \
  --artifact-dir "$WORKDIR/artifacts" \
  --seed 424242 --cycles 1 --writers 12 --segment-size "$SEGMENT_SIZE" --keep-dirs || true
sync
umount "$MNT"

DATA_DIR_ON_MNT="run/cycle-000000/data"
WITNESS="$WITNESS_DIR/cycle-000000.log"

verify_point() {
  local label="$1"
  # Mount read-write so ext4 journal recovery and then Tephra recovery both run, as on a real
  # reboot, then check the read-only invariants against the off-device witness.
  mount "$DM_DEV" "$MNT"
  local rc=0
  "$CRASHTEST_BIN" --server-bin "$SERVER_BIN" --segment-size "$SEGMENT_SIZE" \
    --verify-dir "$MNT/$DATA_DIR_ON_MNT" --witness "$WITNESS" || rc=$?
  umount "$MNT"
  if [[ $rc -ne 0 ]]; then
    echo "INVARIANT VIOLATION at replay point: $label" >&2
    return 1
  fi
  echo "ok: $label"
}

fail=0
echo "==> replay to the last flush (power cut at the final sync)"
"$REPLAY_LOG" --log "$LOG_LOOP" --replay "$DATA_LOOP" --end-mark last >/dev/null 2>&1 \
  || "$REPLAY_LOG" --log "$LOG_LOOP" --replay "$DATA_LOOP" >/dev/null 2>&1
verify_point "last-flush" || fail=1

echo "==> full replay (final on-disk state)"
"$REPLAY_LOG" --log "$LOG_LOOP" --replay "$DATA_LOOP" >/dev/null 2>&1 || true
verify_point "full" || fail=1

if [[ $fail -eq 0 ]]; then
  echo "power-loss test PASSED"
else
  echo "power-loss test FOUND VIOLATIONS" >&2
fi
exit $fail
