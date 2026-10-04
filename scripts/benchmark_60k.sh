#!/usr/bin/env bash
set -euo pipefail
JOBS="${1:-60000}"; WORKERS="${2:-8}"; ROOT="${3:-.sqo/60k}"
rm -rf "$ROOT"; mkdir -p "$ROOT"
PYTHONPATH="${PYTHONPATH:-}:$(pwd)" python -m sqo_orchestrator load --jobs "$JOBS" --workers "$WORKERS" --root "$ROOT" | tee "$ROOT/metrics.json"
