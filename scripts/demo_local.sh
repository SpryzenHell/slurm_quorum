#!/usr/bin/env bash
set -euo pipefail

ROOT="${1:-.sqo/demo}"
rm -rf "$ROOT"
mkdir -p "$ROOT"
export PYTHONPATH="${PYTHONPATH:-}:$(pwd)"

echo '== quorum failover =='
python -m sqo_orchestrator failover --root "$ROOT/failover"

echo '== synthetic 60K throughput =='
python -m sqo_orchestrator load --jobs 60000 --workers 8 --root "$ROOT/load"

echo '== package smoke =='
python -m pytest -q

echo 'demo complete'
