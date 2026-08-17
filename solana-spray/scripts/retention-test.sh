#!/usr/bin/env bash
# Prove Restate forgets settled transactions.
#
# The concern this answers: "I don't want Restate holding history forever — five
# minutes after a transaction is done it can be gone." Retention is configured
# per handler by the service at registration time, so the check is whether the
# completed invocations actually disappear from the state machine on schedule,
# and whether the disk footprint follows.
#
# Usage: scripts/retention-test.sh [retention_s] [count]
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

RETENTION="${1:-60}"
COUNT="${2:-4000}"

q() {
  c -X POST "http://$ADMIN/query" -H 'content-type: application/json' \
    -H 'accept: application/json' -d "{\"query\":\"$1\"}"
}
invocations() {
  q "SELECT count(*) as n FROM sys_invocation" \
    | python3 -c 'import json,sys; print(json.load(sys.stdin)["rows"][0]["n"])'
}

echo "=========================================================="
echo "retention test: retention=${RETENTION}s count=$COUNT"
echo "=========================================================="

start_restate fresh
start_service \
  SPRAY_JOURNAL_RETENTION_S="$RETENTION" \
  SPRAY_WORKFLOW_RETENTION_S="$RETENTION" \
  SPRAY_SINK=null
register_deployment > /dev/null
echo "service: $(c "http://$STATS/config")"

BASE_DISK=$(du -sm "$RESTATE_DATA" | cut -f1)
echo "disk before load: ${BASE_DISK}M   invocations: $(invocations)"

"$BIN_BENCH" load --run-id retention --rate 200 --duration-s $((COUNT / 200)) \
  --target workflow --drain-timeout-s 300 --connections 4 > "$RUN_DIR/retention-load.log" 2>&1

echo
echo "load done. completed=$(c "http://$STATS/stats" | python3 -c 'import json,sys; print(json.load(sys.stdin)["completed"])')"
echo "invocations retained right after settling: $(invocations)"
echo "disk: $(du -sm "$RESTATE_DATA" | cut -f1)M"

echo
echo "watching retention expire (${RETENTION}s window, cleanup runs periodically)..."
DEADLINE=$(( $(date +%s) + RETENTION + 240 ))
while [[ $(date +%s) -lt $DEADLINE ]]; do
  n=$(invocations)
  echo "  t=$(date +%T)  invocations=$n  disk=$(du -sm "$RESTATE_DATA" | cut -f1)M"
  if [[ "$n" -eq 0 ]]; then
    echo
    echo "RESULT: all completed invocations dropped out of Restate."
    break
  fi
  sleep 20
done

echo "final invocations: $(invocations)"
echo "final disk: $(du -sm "$RESTATE_DATA" | cut -f1)M (baseline was ${BASE_DISK}M)"
