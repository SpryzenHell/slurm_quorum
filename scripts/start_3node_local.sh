#!/usr/bin/env bash
set -euo pipefail
ROOT="${1:-.sqo/cluster}"; rm -rf "$ROOT"; mkdir -p "$ROOT"
export PYTHONPATH="${PYTHONPATH:-}:$(pwd)"
cleanup(){ kill "${P1:-}" "${P2:-}" "${P3:-}" 2>/dev/null || true; }
trap cleanup EXIT INT TERM
SQO_NODE_ID=node-1 python -c 'from sqo_orchestrator.node import NodeRuntime; NodeRuntime("node-1","127.0.0.1:8101",{"node-2":"http://127.0.0.1:8102","node-3":"http://127.0.0.1:8103"},state_dir="'$ROOT'/state",lease_dir="'$ROOT'/leases").serve()' & P1=$!
SQO_NODE_ID=node-2 python -c 'from sqo_orchestrator.node import NodeRuntime; NodeRuntime("node-2","127.0.0.1:8102",{"node-1":"http://127.0.0.1:8101","node-3":"http://127.0.0.1:8103"},state_dir="'$ROOT'/state",lease_dir="'$ROOT'/leases").serve()' & P2=$!
SQO_NODE_ID=node-3 python -c 'from sqo_orchestrator.node import NodeRuntime; NodeRuntime("node-3","127.0.0.1:8103",{"node-1":"http://127.0.0.1:8101","node-2":"http://127.0.0.1:8102"},state_dir="'$ROOT'/state",lease_dir="'$ROOT'/leases").serve()' & P3=$!
wait
