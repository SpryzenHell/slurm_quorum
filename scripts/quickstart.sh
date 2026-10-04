#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"
rm -rf .sqo/quickstart

PYTHON_BIN="${PYTHON_BIN:-python3}"
if ! command -v "$PYTHON_BIN" >/dev/null 2>&1; then
  echo "python3 is required." >&2
  exit 1
fi

"$PYTHON_BIN" - <<'PY'
import sys
if sys.version_info < (3, 11):
    raise SystemExit("Python 3.11 or newer is required.")
print(f"Using Python {sys.version.split()[0]}")
PY

if [ ! -d ".venv" ]; then
  "$PYTHON_BIN" -m venv .venv
fi

# shellcheck disable=SC1091
source .venv/bin/activate

python -m pip install --upgrade pip
python -m pip install -e "./sqo_orchestrator[test]"

echo
echo "== test suite =="
python -m pytest -q

echo
echo "== quorum failover =="
sqo failover --root .sqo/quickstart/failover

echo
echo "== 1,000-job local load =="
sqo load --jobs 1000 --workers 4 --root .sqo/quickstart/load

echo
echo "== three-node network smoke test =="
bash scripts/check_3node_local.sh .sqo/quickstart/network

echo
echo "Quickstart completed successfully."
echo "Virtual environment: $ROOT_DIR/.venv"
