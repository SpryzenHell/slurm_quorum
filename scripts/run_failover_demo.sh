#!/usr/bin/env bash
set -euo pipefail
ROOT="${1:-.sqo/failover}"
rm -rf "$ROOT"; PYTHONPATH="${PYTHONPATH:-}:$(pwd)" python -m sqo_orchestrator failover --root "$ROOT"
