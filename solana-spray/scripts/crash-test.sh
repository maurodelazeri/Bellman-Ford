#!/usr/bin/env bash
# Kill the service mid-flight and prove every in-flight transaction still
# reaches a terminal state with a gapless event trail.
#
# This is the whole reason for using Restate. The test is: SIGKILL the service
# while thousands of transactions are actively spraying, bring it back, and
# check that no transaction is left with events 1 and 2 but not 3 and 4.
#
# Usage: scripts/crash-test.sh [rate] [duration_s] [kills]
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

RATE="${1:-400}"
DURATION="${2:-30}"
KILLS="${3:-2}"
# Seconds the service stays down. Long enough and transactions expire on chain
# while we are dark, which is the case that must still produce a full trail.
DOWNTIME="${4:-3}"
NAME="crash-r${RATE}-k${KILLS}-d${DOWNTIME}"
RESULTS_DIR="${RESULTS_DIR:-$REPO_ROOT/results}"
mkdir -p "$RESULTS_DIR"

# The file sink is required: the verifier reads what actually got written.
export SPRAY_SINK=file
export SPRAY_SINK_PATH=/var/spray/events
export SPRAY_JOURNAL_MODE="${SPRAY_JOURNAL_MODE:-lean}"

echo "=========================================================="
echo "crash test: rate=$RATE duration=${DURATION}s kills=$KILLS downtime=${DOWNTIME}s mode=$SPRAY_JOURNAL_MODE"
echo "=========================================================="

start_restate fresh
stop_service
mkdir -p /var/spray
rm -f /var/spray/events.*.jsonl
setsid nohup "$BIN_SERVICE" > /var/spray/service.log 2>&1 < /dev/null &
wait_for "http://$STATS/health" "spray-service"
register_deployment > /dev/null
echo "service: $(c "http://$STATS/config")"

# Kill and restart the service partway through, while transactions are in the
# middle of their spray phase.
(
  for i in $(seq "$KILLS"); do
    sleep $(( DURATION / (KILLS + 1) ))
    echo ">>> [kill $i] SIGKILL spray-service (in_flight=$(c "http://$STATS/stats" | python3 -c 'import json,sys; print(json.load(sys.stdin)["in_flight"])' 2>/dev/null || echo '?'))"
    pkill -9 -f "$BIN_SERVICE" || true
    sleep "$DOWNTIME"
    setsid nohup "$BIN_SERVICE" > "/var/spray/service.$i.log" 2>&1 < /dev/null &
    wait_for "http://$STATS/health" "spray-service restart $i" 60
    echo ">>> [kill $i] service back up"
  done
) &
KILLER=$!

"$BIN_BENCH" load \
  --run-id "$NAME" \
  --rate "$RATE" \
  --duration-s "$DURATION" \
  --target workflow \
  --drain-timeout-s 400 \
  --connections 8 \
  --out "$RESULTS_DIR/$NAME.json" 2>&1 | tee "$RESULTS_DIR/$NAME.log" || true

wait "$KILLER" 2>/dev/null || true

echo
echo "=========================================================="
echo "event trail verification"
echo "=========================================================="
# The drain watcher tracks the *live* service's counter, which resets to zero on
# every restart, so it cannot know when the backlog is done. Wait on the trail
# itself instead: settle when the written event count stops moving.
prev=-1
for _ in $(seq 120); do
  cur=$(cat /var/spray/events.*.jsonl 2>/dev/null | wc -l)
  inflight=$(c "http://$STATS/stats" | python3 -c 'import json,sys; print(json.load(sys.stdin)["in_flight"])' 2>/dev/null || echo 0)
  if [[ "$cur" == "$prev" && "$inflight" == "0" ]]; then break; fi
  prev="$cur"
  sleep 5
done
echo "events written: $prev  (in_flight=$inflight)"

"$BIN_BENCH" verify --prefix /var/spray/events | tee "$RESULTS_DIR/$NAME.verify.json"
