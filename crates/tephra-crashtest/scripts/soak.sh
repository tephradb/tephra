#!/usr/bin/env bash
#
# Long crash-consistency soak: mixed workloads with random crashes, plus a periodic targeted
# failpoint battery. Runs for DURATION_SECS, then prints a summary. Any invariant violation keeps
# its artifact under ARTIFACT_DIR and is counted.
#
# The main loop is the real soak: random workload (pure / DCB conditions / subscriptions), random
# small segment size (constant rollover and index flushing), random seed, timed SIGKILL, full
# invariant check. Every few rounds it also runs the Phase 2 abort battery. The Phase 3 io-fault
# battery is intentionally left out of the loop because its `enospc_segment_extend` scenario
# reproduces the known F1 finding on purpose, which would otherwise be counted as a failure here.
#
# USAGE: DURATION_SECS=1800 bash crates/tephra-crashtest/scripts/soak.sh
set -uo pipefail

BIN="${SERVER_BIN:-target/release/tephra-server-crash}"
CT="${CRASHTEST_BIN:-target/release/tephra-crashtest}"
ROOT="${DATA_ROOT:-/tmp/tephra-soak}"
ART="${ARTIFACT_DIR:-/tmp/tephra-soak-artifacts}"
DURATION="${DURATION_SECS:-1800}"
CYCLES_PER_ROUND="${CYCLES_PER_ROUND:-60}"

[[ -x "$BIN" ]] || { echo "server binary $BIN missing" >&2; exit 1; }
[[ -x "$CT" ]] || { echo "crashtest binary $CT missing" >&2; exit 1; }
rm -rf "$ROOT"; mkdir -p "$ROOT" "$ART"

workloads=(pure mixed subscription)
segsizes=(16384 32768 65536)
start=$(date +%s)
round=0; total_cycles=0; total_fail=0

fails_of() { grep -oP 'failures=\K[0-9]+' <<<"$1" | tail -1; }
summary_of() { grep -E '^summary:|^phase 2 summary' <<<"$1" | tail -1; }

while (( $(date +%s) - start < DURATION )); do
  round=$((round + 1))
  wl=${workloads[$((RANDOM % ${#workloads[@]}))]}
  seg=${segsizes[$((RANDOM % ${#segsizes[@]}))]}
  seed=$((RANDOM * RANDOM))

  out=$("$CT" --server-bin "$BIN" --data-root "$ROOT/r$round" --artifact-dir "$ART" \
        --seed "$seed" --cycles "$CYCLES_PER_ROUND" --writers 12 --segment-size "$seg" \
        --workload "$wl" 2>&1)
  f=$(fails_of "$out"); f=${f:-0}
  total_cycles=$((total_cycles + CYCLES_PER_ROUND))
  total_fail=$((total_fail + f))
  elapsed=$(( $(date +%s) - start ))
  echo "[${elapsed}s] round $round wl=$wl seg=$seg seed=$seed: $(summary_of "$out")"
  if (( f > 0 )); then
    echo "  !!! $f FAILURE(S) this round (seed $seed, workload $wl) - see $ART"
    grep -E 'FAIL|  - ' <<<"$out" | head -12
  fi
  rm -rf "$ROOT/r$round"

  if (( round % 5 == 0 )); then
    p2=$("$CT" --phase2 --phase2-cycles 3 --seed "$((RANDOM * RANDOM))" \
         --server-bin "$BIN" --data-root "$ROOT/p2" --artifact-dir "$ART" 2>&1)
    pf=$(fails_of "$p2"); pf=${pf:-0}
    total_cycles=$((total_cycles + 21))
    total_fail=$((total_fail + pf))
    echo "  phase2 battery: $(summary_of "$p2")"
    (( pf > 0 )) && grep -E 'FAIL|  - ' <<<"$p2" | head -12
    rm -rf "$ROOT/p2"
  fi
done

echo ""
echo "SOAK DONE: ${round} rounds, ~${total_cycles} crash cycles, total_failures=${total_fail}"
echo "artifacts (if any): $ART"
