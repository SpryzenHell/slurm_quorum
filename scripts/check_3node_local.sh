#!/usr/bin/env bash
set -euo pipefail

ROOT="${1:-.sqo/cluster-check}"
rm -rf "$ROOT"
mkdir -p "$ROOT"

export PYTHONPATH="${PYTHONPATH:-}:$(pwd)"
cleanup() {
  kill "${P1:-}" "${P2:-}" "${P3:-}" 2>/dev/null || true
  wait "${P1:-}" "${P2:-}" "${P3:-}" 2>/dev/null || true
}
trap cleanup EXIT INT TERM

SQO_NODE_ID=node-1 python -m sqo_orchestrator serve --root "$ROOT" --bind 127.0.0.1:8111 --peers '{"node-2":"http://127.0.0.1:8112","node-3":"http://127.0.0.1:8113"}' >"$ROOT/node1.log" 2>&1 & P1=$!
SQO_NODE_ID=node-2 python -m sqo_orchestrator serve --root "$ROOT" --bind 127.0.0.1:8112 --peers '{"node-1":"http://127.0.0.1:8111","node-3":"http://127.0.0.1:8113"}' >"$ROOT/node2.log" 2>&1 & P2=$!
SQO_NODE_ID=node-3 python -m sqo_orchestrator serve --root "$ROOT" --bind 127.0.0.1:8113 --peers '{"node-1":"http://127.0.0.1:8111","node-2":"http://127.0.0.1:8112"}' >"$ROOT/node3.log" 2>&1 & P3=$!

python - <<'PY'
import json, time, urllib.request

ports=(8111,8112,8113)
for _ in range(80):
    states=[]
    try:
        for port in ports:
            with urllib.request.urlopen(f"http://127.0.0.1:{port}/health", timeout=.3) as r:
                states.append(json.loads(r.read()))
        leaders=[s for s in states if s["role"]=="leader"]
        if len(leaders)==1:
            print(json.dumps({"cluster":states,"leader":leaders[0]},indent=2,sort_keys=True))
            raise SystemExit(0)
    except Exception:
        pass
    time.sleep(.2)
raise SystemExit("3-node network election did not converge")
PY
