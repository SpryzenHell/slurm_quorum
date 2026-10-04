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

SQO_LEASE_TTL_S=4 SQO_ELECTION_MIN_S=1 SQO_ELECTION_MAX_S=2 SQO_NODE_ID=node-1 python -m sqo_orchestrator serve --root "$ROOT" --bind 127.0.0.1:8111 --peers '{"node-2":"http://127.0.0.1:8112","node-3":"http://127.0.0.1:8113"}' >"$ROOT/node1.log" 2>&1 & P1=$!
SQO_LEASE_TTL_S=4 SQO_ELECTION_MIN_S=1 SQO_ELECTION_MAX_S=2 SQO_NODE_ID=node-2 python -m sqo_orchestrator serve --root "$ROOT" --bind 127.0.0.1:8112 --peers '{"node-1":"http://127.0.0.1:8111","node-3":"http://127.0.0.1:8113"}' >"$ROOT/node2.log" 2>&1 & P2=$!
SQO_LEASE_TTL_S=4 SQO_ELECTION_MIN_S=1 SQO_ELECTION_MAX_S=2 SQO_NODE_ID=node-3 python -m sqo_orchestrator serve --root "$ROOT" --bind 127.0.0.1:8113 --peers '{"node-1":"http://127.0.0.1:8111","node-2":"http://127.0.0.1:8112"}' >"$ROOT/node3.log" 2>&1 & P3=$!

export P1 P2 P3
python - <<'PY'
import json, os, signal, time, urllib.request

ports=(8111,8112,8113)
pids={8111:int(os.environ["P1"]),8112:int(os.environ["P2"]),8113:int(os.environ["P3"])}

def health(port):
    with urllib.request.urlopen(f"http://127.0.0.1:{port}/health", timeout=.3) as r:
        return json.loads(r.read())

leader_port = None
for _ in range(80):
    try:
        states=[health(p) for p in ports]
        leaders=[s for s in states if s["role"]=="leader"]
        if len(leaders)==1:
            leader_id=leaders[0]["node_id"]
            leader_port={"node-1":8111,"node-2":8112,"node-3":8113}[leader_id]
            print(json.dumps({"cluster":states,"leader":leaders[0]},indent=2,sort_keys=True))
            break
    except Exception:
        pass
    time.sleep(.2)
if leader_port is None:
    raise SystemExit("3-node network election did not converge")

os.kill(pids[leader_port], signal.SIGTERM)
remaining=[p for p in ports if p != leader_port]
new_leader=None
for _ in range(90):
    try:
        states=[health(p) for p in remaining]
        leaders=[s for s in states if s["role"]=="leader"]
        if len(leaders)==1:
            new_leader=leaders[0]
            print(json.dumps({"failed_port":leader_port,"new_leader":new_leader},indent=2,sort_keys=True))
            break
    except Exception:
        pass
    time.sleep(.2)
if new_leader is None:
    raise SystemExit("leader failover did not converge")
if new_leader["node_id"] == leader_id:
    raise SystemExit("failed leader reclaimed leadership unexpectedly")
PY
