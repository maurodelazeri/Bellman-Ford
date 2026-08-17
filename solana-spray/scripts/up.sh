#!/usr/bin/env bash
# Bring up restate-server + spray-service and register the deployment.
#
# Usage: scripts/up.sh [fresh|keep]
#   fresh (default) wipes Restate's data directory first.
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

start_restate "${1:-fresh}"
start_service
register_deployment

echo
echo "ingress : http://$INGRESS"
echo "admin   : http://$ADMIN"
echo "stats   : http://$STATS/stats"
echo "state   : $RUN_DIR"
c "http://$STATS/config"
echo
