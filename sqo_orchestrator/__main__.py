import argparse, json, os, shutil
from pathlib import Path

from .core import Orchestrator, ClusterSimulator
from .node import NodeRuntime

p = argparse.ArgumentParser(prog="sqo")
s = p.add_subparsers(dest="cmd", required=True)

l = s.add_parser("load")
l.add_argument("--jobs", type=int, default=60000)
l.add_argument("--workers", type=int, default=8)
l.add_argument("--root", type=Path, default=Path(".sqo/load"))
l.add_argument("--s3-bucket")
l.add_argument("--s3-prefix", default="slurm-quorum")
l.add_argument("--s3-telemetry-prefix")

f = s.add_parser("failover")
f.add_argument("--root", type=Path, default=Path(".sqo/failover"))

sv = s.add_parser("serve")
sv.add_argument("--bind", required=True, help="listen address, e.g. 127.0.0.1:8101")
sv.add_argument("--peers", required=True, help='JSON object mapping peer IDs to HTTP base URLs')
sv.add_argument("--root", type=Path, default=Path(".sqo/network"))
sv.add_argument("--state-dir", type=Path)
sv.add_argument("--lease-dir", type=Path)
sv.add_argument("--s3-bucket")
sv.add_argument("--s3-prefix", default="slurm-quorum")
sv.add_argument("--s3-endpoint-url")
sv.add_argument("--s3-region")

a = p.parse_args()

if a.cmd == "load":
    o = Orchestrator(
        a.root,
        s3_bucket=a.s3_bucket,
        s3_prefix=a.s3_prefix,
        s3_telemetry_prefix=a.s3_telemetry_prefix,
    )
    if not o.acquire_master():
        raise SystemExit("could not acquire local master fencing lease")
    print(json.dumps(o.load(a.jobs, a.workers), indent=2, sort_keys=True))

elif a.cmd == "failover":
    if a.root.exists():
        shutil.rmtree(a.root)
    c = ClusterSimulator(a.root)
    first = c.elect("node-1")
    c.fail(first["winner"])
    second = c.elect("node-2")
    print(json.dumps(
        {"initial": first, "failed": first["winner"], "failover": second,
         "majority": c.majority, "new_leader": second["winner"] if second else None},
        indent=2, sort_keys=True,
    ))

else:
    peers = json.loads(a.peers)
    node_id = os.environ.get("SQO_NODE_ID", a.bind.rsplit(":", 1)[-1])
    root = a.root
    state_dir = a.state_dir or (root / "state")
    lease_dir = a.lease_dir or (root / "leases")
    NodeRuntime(
        node_id=node_id,
        bind=a.bind,
        peers=peers,
        state_dir=state_dir,
        lease_dir=lease_dir,
        lease_ttl=float(os.environ.get("SQO_LEASE_TTL_S", "8")),
        election=(float(os.environ.get("SQO_ELECTION_MIN_S", "2")),
                  float(os.environ.get("SQO_ELECTION_MAX_S", "4"))),
        s3_bucket=a.s3_bucket,
        s3_prefix=a.s3_prefix,
        s3_endpoint_url=a.s3_endpoint_url,
        s3_region=a.s3_region,
    ).serve()
