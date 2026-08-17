#!/usr/bin/env bash
# Kill the service mid-flight and prove every in-flight transaction still
# reaches a terminal state with a gapless event trail.
#
# This is the whole reason for using Restate. The test is: SIGKILL the service
# while thousands of transactions are actively spraying, bring it back, and
# check that no transaction is left with events 1 and 2 but not 3 and 4.
#
# Usage: scripts/crash-test.sh [rate] [duration_s] [kills] [downtime_s]
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

RATE="${1:-400}"
DURATION="${2:-30}"
KILLS="${3:-2}"
# Seconds the service stays down. Long enough and transactions expire on chain
# while we are dark, which is the case that must still produce a full trail.
DOWNTIME="${4:-3}"
NAME="crash-r${RATE}-k${KILLS}-d${DOWNTIME}"

# The file sink is required: the verifier reads what actually got written.
export SPRAY_SINK=file
export SPRAY_JOURNAL_MODE="${SPRAY_JOURNAL_MODE:-lean}"

echo "=========================================================="
echo "crash test: rate=$RATE duration=${DURATION}s kills=$KILLS downtime=${DOWNTIME}s mode=$SPRAY_JOURNAL_MODE"
echo "=========================================================="

start_restate fresh
rm -f "$EVENTS_PREFIX".*.jsonl
start_service
register_deployment > /dev/null
echo "service: $(c "http://$STATS/config")"

# Kill and restart the service partway through, while transactions are in the
# middle of their spray phase.
(
  for i in $(seq "$KILLS"); do
    sleep $(( DURATION / (KILLS + 1) ))
    echo ">>> [kill $i] SIGKILL spray-service (in_flight=$(c "http://$STATS/stats" | python3 -c 'import json,sys; print(json.load(sys.stdin)["in_flight"])' 2>/dev/null || echo '?'))"
    pkill -9 -f "^$BIN_SERVICE" || true
    sleep "$DOWNTIME"
    start_service > /dev/null
    echo ">>> [kill $i] service back up"
  done
) &
KILLER=$!

"$BIN_BENCH" load \
  --run-id "$NAME" \
  --rate "$RATE" \
  --duration-s "$DURATION" \
  --target workflow \
  # The bench's drain watcher polls the live service's completed counter, which
  # resets to zero on every restart, so it can never reach the expected total in
  # this test. Give it a short leash; the authoritative wait is the Restate-based
  # quiet loop below.
  --drain-timeout-s 45 \
  --connections 8 \
  --out "$RESULTS_DIR/$NAME.json" 2>&1 | tee "$RESULTS_DIR/$NAME.log" || true

wait "$KILLER" 2>/dev/null || true

echo
echo "=========================================================="
echo "event trail verification"
echo "=========================================================="
# The drain watcher tracks the *live* service's counter, which resets to zero on
# every restart, so it cannot know when the backlog is done. Wait on Restate
# instead: it is the authority on whether any invocation is still outstanding.
#
# Waiting on "the event count stopped moving" is not enough. After a kill,
# Restate backs off before re-dispatching, so there are windows of several
# seconds with nothing in flight and nothing being written that are not the end
# of the run. Requiring several consecutive quiet polls *and* zero live
# invocations avoids calling the run finished while Restate is mid-backoff.
live_invocations() {
  c -X POST "http://$ADMIN/query" -H 'content-type: application/json' \
    -H 'accept: application/json' \
    -d '{"query":"SELECT count(*) as n FROM sys_invocation WHERE status != '"'"'completed'"'"'"}' \
    | python3 -c 'import json,sys; print(json.load(sys.stdin)["rows"][0]["n"])' 2>/dev/null || echo -1
}

prev=-1; quiet=0
for _ in $(seq 200); do
  cur=$(cat "$EVENTS_PREFIX".*.jsonl 2>/dev/null | wc -l)
  live=$(live_invocations)
  if [[ "$cur" == "$prev" && "$live" == "0" ]]; then
    quiet=$((quiet + 1))
    [[ "$quiet" -ge 3 ]] && break
  else
    quiet=0
  fi
  prev="$cur"
  sleep 5
done
echo "events written: $prev  (live invocations=$live, quiet polls=$quiet)"

"$BIN_BENCH" verify --prefix "$EVENTS_PREFIX" | tee "$RESULTS_DIR/$NAME.verify.json"
