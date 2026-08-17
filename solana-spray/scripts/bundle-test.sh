#!/usr/bin/env bash
# Functional test of the two bundle policies the user described:
#   sequential  - "run this, and only if it lands, run the next one"
#   all-at-once - "just run these three, whatever happens"
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

SPRAY="{\"leaders_ahead\":2,\"interval_ms\":100,\"max_sends\":4000}"
COMMON="\"region\":\"us\",\"blockhash_age_slots\":5,\"validity_slots\":150,\"commitment\":\"confirmed\",\"spray\":$SPRAY"

member() { # $1 = tx_id, $2 = index
  echo "{\"tx_id\":\"$1\",\"payload\":\"QUJDREVGRw\",$COMMON,\"bundle\":{\"bundle_id\":\"$3\",\"index\":$2}}"
}

# The mock cluster's outcome is a pure function of the signature, so we can pick
# ids whose fate we know and assert on the bundle's branching behaviour.
echo "== probing signatures for known outcomes =="
LANDERS=(); FAILERS=()
for i in $(seq 1 60); do
  id="probe-$i"
  out=$(c -X POST "http://$INGRESS/TxWorkflow/$id/run" -H 'content-type: application/json' \
        -d "{\"tx_id\":\"$id\",\"payload\":\"QQ\",$COMMON}" | python3 -c 'import json,sys; print(json.load(sys.stdin)["outcome"])')
  case "$out" in
    confirmed) [[ ${#LANDERS[@]} -lt 4 ]] && LANDERS+=("$id") ;;
    expired|failed_on_chain) [[ ${#FAILERS[@]} -lt 2 ]] && FAILERS+=("$id") ;;
  esac
  [[ ${#LANDERS[@]} -ge 4 && ${#FAILERS[@]} -ge 2 ]] && break
done
echo "landers: ${LANDERS[*]}"
echo "failers: ${FAILERS[*]}"

echo
echo "== sequential bundle, all members land: expect every member executed =="
B=seq-ok-$RANDOM
BODY="{\"bundle_id\":\"$B\",\"mode\":\"sequential\",\"members\":[
  $(member "${LANDERS[0]}-$B" 0 "$B"),
  $(member "${LANDERS[1]}-$B" 1 "$B"),
  $(member "${LANDERS[2]}-$B" 2 "$B")]}"
c -X POST "http://$INGRESS/BundleService/execute" -H 'content-type: application/json' -d "$BODY" \
  | python3 -m json.tool

echo
echo "== sequential bundle with a failure in the middle: expect the tail skipped =="
B=seq-abort-$RANDOM
BODY="{\"bundle_id\":\"$B\",\"mode\":\"sequential\",\"members\":[
  $(member "${LANDERS[0]}-$B" 0 "$B"),
  $(member "${FAILERS[0]}-$B" 1 "$B"),
  $(member "${LANDERS[1]}-$B" 2 "$B")]}"
c -X POST "http://$INGRESS/BundleService/execute" -H 'content-type: application/json' -d "$BODY" \
  | python3 -m json.tool

echo
echo "== all-at-once bundle: expect immediate dispatch, no waiting =="
B=aao-$RANDOM
BODY="{\"bundle_id\":\"$B\",\"mode\":\"all-at-once\",\"members\":[
  $(member "${LANDERS[0]}-$B" 0 "$B"),
  $(member "${FAILERS[0]}-$B" 1 "$B"),
  $(member "${LANDERS[1]}-$B" 2 "$B")]}"
time c -X POST "http://$INGRESS/BundleService/execute" -H 'content-type: application/json' -d "$BODY" \
  | python3 -m json.tool

echo
echo "== members of the all-at-once bundle, queried individually =="
sleep 3
for m in "${LANDERS[0]}-$B" "${FAILERS[0]}-$B" "${LANDERS[1]}-$B"; do
  echo -n "  $m -> "
  c -X POST "http://$INGRESS/TxWorkflow/$m/status"
  echo
done
