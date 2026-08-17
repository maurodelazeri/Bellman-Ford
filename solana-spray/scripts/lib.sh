#!/usr/bin/env bash
# Shared helpers for the run scripts.
#
# Everything this project reads or writes lives under the project directory.
# Nothing is installed system-wide and nothing is written to /var or /opt, so a
# fresh clone runs without touching the host, and `rm -rf .run vendor` puts the
# machine back exactly as it was.
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# Downloaded restate-server binaries (see scripts/bootstrap.sh).
VENDOR_DIR="${VENDOR_DIR:-$PROJECT_ROOT/vendor/restate}"
RESTATE_BIN="${RESTATE_BIN:-$VENDOR_DIR/restate-server}"

# All mutable runtime state: Restate's RocksDB, event files, logs, pids.
RUN_DIR="${RUN_DIR:-$PROJECT_ROOT/.run}"
RESTATE_DATA="${RESTATE_DATA:-$RUN_DIR/restate}"
SPRAY_RUN_DIR="${SPRAY_RUN_DIR:-$RUN_DIR/service}"
EVENTS_PREFIX="${EVENTS_PREFIX:-$SPRAY_RUN_DIR/events}"

RESTATE_CONFIG="${RESTATE_CONFIG:-$PROJECT_ROOT/config/restate-bench.toml}"
RESULTS_DIR="${RESULTS_DIR:-$PROJECT_ROOT/results}"

INGRESS="${INGRESS:-127.0.0.1:8080}"
ADMIN="${ADMIN:-127.0.0.1:9070}"
SERVICE_ENDPOINT="${SERVICE_ENDPOINT:-http://127.0.0.1:9080}"
STATS="${STATS:-127.0.0.1:9081}"

BIN_SERVICE="$PROJECT_ROOT/target/release/spray-service"
BIN_BENCH="$PROJECT_ROOT/target/release/spray-bench"

mkdir -p "$RUN_DIR" "$SPRAY_RUN_DIR" "$RESULTS_DIR"

# curl must bypass any ambient proxy: everything here is loopback.
c() { curl -sS --noproxy '*' "$@"; }

require_binaries() {
  if [[ ! -x "$RESTATE_BIN" ]]; then
    echo "restate-server not found at $RESTATE_BIN" >&2
    echo "run: scripts/bootstrap.sh" >&2
    return 1
  fi
  if [[ ! -x "$BIN_SERVICE" || ! -x "$BIN_BENCH" ]]; then
    echo "project binaries missing; run: cargo build --release" >&2
    return 1
  fi
}

wait_for() {
  local url="$1" name="$2" tries="${3:-120}"
  for _ in $(seq "$tries"); do
    if c -o /dev/null -f "$url" 2>/dev/null; then return 0; fi
    sleep 0.5
  done
  echo "timed out waiting for $name at $url" >&2
  return 1
}

start_restate() {
  local fresh="${1:-fresh}"
  require_binaries
  stop_restate || true
  if [[ "$fresh" == "fresh" ]]; then
    rm -rf "$RESTATE_DATA"
  fi
  mkdir -p "$RESTATE_DATA"
  RESTATE_BASE_DIR="$RESTATE_DATA" setsid nohup "$RESTATE_BIN" --no-logo \
    -c "$RESTATE_CONFIG" > "$RUN_DIR/restate.log" 2>&1 < /dev/null &
  wait_for "http://$ADMIN/health" "restate admin"
  wait_for "http://$INGRESS/restate/health" "restate ingress"
  echo "restate up (data=$RESTATE_DATA)"
}

# Match on the project-local binary path so these never kill an unrelated
# process, or — as happens with a bare `pkill -f restate-server` — the very
# shell issuing the command, whose own command line contains the pattern.
stop_restate() {
  pkill -f "^$RESTATE_BIN " 2>/dev/null || true
  sleep 1
}

stop_service() {
  pkill -f "^$BIN_SERVICE" 2>/dev/null || true
  sleep 1
}

# Start spray-service with the project-local defaults, plus any KEY=VAL
# overrides passed as arguments.
start_service() {
  require_binaries
  stop_service
  mkdir -p "$SPRAY_RUN_DIR"
  env SPRAY_SINK_PATH="$EVENTS_PREFIX" "$@" \
    setsid nohup "$BIN_SERVICE" > "$SPRAY_RUN_DIR/service.log" 2>&1 < /dev/null &
  wait_for "http://$STATS/health" "spray-service stats"
  echo "spray-service up"
}

register_deployment() {
  # `force` lets a restarted service with the same URL re-register cleanly.
  c -X POST "http://$ADMIN/deployments" \
    -H 'content-type: application/json' \
    -d "{\"uri\":\"$SERVICE_ENDPOINT\",\"force\":true}" > "$RUN_DIR/register.json"
  if ! grep -q '"id"' "$RUN_DIR/register.json"; then
    echo "registration failed:" >&2; cat "$RUN_DIR/register.json" >&2; return 1
  fi
  echo "registered deployment at $SERVICE_ENDPOINT"
}
