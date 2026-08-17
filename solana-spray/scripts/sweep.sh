#!/usr/bin/env bash
# Run one benchmark scenario end to end with a clean slate.
#
# Usage:
#   scripts/sweep.sh <name> [env KEY=VAL ...] -- [spray-bench load args ...]
#
# Example:
#   scripts/sweep.sh lean-1k SPRAY_JOURNAL_MODE=lean -- --rate 1000 --duration-s 60
#
# Every run wipes Restate's data directory and restarts the service, so counters
# and RocksDB state never carry between scenarios.
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

NAME="$1"; shift
RESULTS_DIR="${RESULTS_DIR:-$REPO_ROOT/results}"
mkdir -p "$RESULTS_DIR"

ENV_ARGS=()
while [[ $# -gt 0 && "$1" != "--" ]]; do ENV_ARGS+=("$1"); shift; done
[[ "${1:-}" == "--" ]] && shift
BENCH_ARGS=("$@")

echo "=========================================================="
echo "scenario: $NAME"
echo "env     : ${ENV_ARGS[*]:-<defaults>}"
echo "bench   : ${BENCH_ARGS[*]:-<defaults>}"
echo "=========================================================="

start_restate fresh

stop_service
mkdir -p /var/spray
rm -f /var/spray/events.*.jsonl
# shellcheck disable=SC2086
env "${ENV_ARGS[@]}" setsid nohup "$BIN_SERVICE" > /var/spray/service.log 2>&1 < /dev/null &
wait_for "http://$STATS/health" "spray-service stats"
register_deployment > /dev/null
echo "service: $(c "http://$STATS/config")"

# Record the machine's idle state so the report can say what headroom existed.
LOADAVG_BEFORE=$(cut -d' ' -f1-3 /proc/loadavg)
python3 "$REPO_ROOT/scripts/cpu.py" start "/tmp/cpu-$NAME.json"

"$BIN_BENCH" load \
  --run-id "$NAME" \
  --out "$RESULTS_DIR/$NAME.json" \
  "${BENCH_ARGS[@]}" 2>&1 | tee "$RESULTS_DIR/$NAME.log"

COMPLETED=$(python3 -c "
import json,sys
d=json.load(open('$RESULTS_DIR/$NAME.json'))
print((d.get('final_service_stats') or {}).get('completed', 0))
")
echo
echo "-- cpu attribution --"
python3 "$REPO_ROOT/scripts/cpu.py" stop "/tmp/cpu-$NAME.json" "$COMPLETED" \
  | tee "$RESULTS_DIR/$NAME.cpu.json"

# Restate's on-disk footprint after the run: the answer to "does short
# retention actually keep storage bounded".
echo "-- restate disk --"
du -sh "$RESTATE_DATA" 2>/dev/null || true

echo "loadavg before run: $LOADAVG_BEFORE"
echo "loadavg after  run: $(cut -d' ' -f1-3 /proc/loadavg)"
echo "results: $RESULTS_DIR/$NAME.json"
