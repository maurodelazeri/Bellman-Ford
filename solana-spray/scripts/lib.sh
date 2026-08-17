#!/usr/bin/env bash
# Shared helpers for the run scripts.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RESTATE_BIN="${RESTATE_BIN:-/opt/restate/bin/restate-server}"
RESTATE_CLI="${RESTATE_CLI:-/opt/restate/bin/restate}"
RESTATE_DATA="${RESTATE_DATA:-/var/restate/bench}"
RESTATE_CONFIG="${RESTATE_CONFIG:-$REPO_ROOT/config/restate-bench.toml}"

INGRESS="${INGRESS:-127.0.0.1:8080}"
ADMIN="${ADMIN:-127.0.0.1:9070}"
SERVICE_ENDPOINT="${SERVICE_ENDPOINT:-http://127.0.0.1:9080}"
STATS="${STATS:-127.0.0.1:9081}"

BIN_SERVICE="$REPO_ROOT/target/release/spray-service"
BIN_BENCH="$REPO_ROOT/target/release/spray-bench"

# curl must bypass any ambient proxy: everything here is loopback.
c() { curl -sS --noproxy '*' "$@"; }

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
  stop_restate || true
  if [[ "$fresh" == "fresh" ]]; then
    rm -rf "$RESTATE_DATA"
  fi
  mkdir -p "$RESTATE_DATA"
  RESTATE_BASE_DIR="$RESTATE_DATA" setsid nohup "$RESTATE_BIN" --no-logo \
    -c "$RESTATE_CONFIG" > "$RESTATE_DATA/server.log" 2>&1 < /dev/null &
  wait_for "http://$ADMIN/health" "restate admin"
  wait_for "http://$INGRESS/restate/health" "restate ingress"
  echo "restate up (data=$RESTATE_DATA)"
}

stop_restate() {
  pkill -f 'restate-server --no-logo' 2>/dev/null || true
  sleep 1
}

start_service() {
  stop_service
  mkdir -p /var/spray
  setsid nohup "$BIN_SERVICE" > /var/spray/service.log 2>&1 < /dev/null &
  echo $! > /var/spray/service.pid
  wait_for "http://$STATS/health" "spray-service stats"
  echo "spray-service up (pid $(cat /var/spray/service.pid))"
}

stop_service() {
  pkill -f "$BIN_SERVICE" 2>/dev/null || true
  sleep 1
}

register_deployment() {
  # `force` lets a restarted service with the same URL re-register cleanly.
  c -X POST "http://$ADMIN/deployments" \
    -H 'content-type: application/json' \
    -d "{\"uri\":\"$SERVICE_ENDPOINT\",\"force\":true}" > /tmp/register.json
  if ! grep -q '"id"' /tmp/register.json; then
    echo "registration failed:" >&2; cat /tmp/register.json >&2; return 1
  fi
  echo "registered: $(head -c 400 /tmp/register.json)"
}
