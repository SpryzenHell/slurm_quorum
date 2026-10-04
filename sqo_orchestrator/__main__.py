from __future__ import annotations

import argparse
import json
import os
import shutil
from pathlib import Path

from .core import ClusterSimulator, Orchestrator
from .node import NodeRuntime


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="sqo",
        description="Slurm-Quorum Orchestrator control-plane tools.",
    )
    sub = parser.add_subparsers(dest="cmd", required=True)

    load = sub.add_parser(
        "load",
        help="run the local SQLite-WAL throughput demonstration",
    )
    load.add_argument("--jobs", type=int, default=60000)
    load.add_argument("--workers", type=int, default=8)
    load.add_argument("--root", type=Path, default=Path(".sqo/load"))
    load.add_argument("--s3-bucket")
    load.add_argument("--s3-prefix", default="slurm-quorum")
    load.add_argument("--s3-telemetry-prefix")
    load.add_argument("--s3-endpoint-url")
    load.add_argument("--s3-region")
    load.add_argument("--s3-force-path-style", action="store_true")

    failover = sub.add_parser(
        "failover",
        help="run the local three-node election/failover demonstration",
    )
    failover.add_argument("--root", type=Path, default=Path(".sqo/failover"))

    serve = sub.add_parser(
        "serve",
        help="start one quorum node",
    )
    serve.add_argument(
        "--bind",
        required=True,
        help="listen address, for example 127.0.0.1:8101",
    )
    serve.add_argument(
        "--peers",
        required=True,
        help='JSON object mapping peer IDs to HTTP base URLs',
    )
    serve.add_argument("--root", type=Path, default=Path(".sqo/network"))
    serve.add_argument("--state-dir", type=Path)
    serve.add_argument("--lease-dir", type=Path)
    serve.add_argument("--s3-bucket")
    serve.add_argument("--s3-prefix", default="slurm-quorum")
    serve.add_argument("--s3-endpoint-url")
    serve.add_argument("--s3-region")
    serve.add_argument("--s3-force-path-style", action="store_true")

    return parser


def run_load(args: argparse.Namespace) -> int:
    orchestrator = Orchestrator(
        args.root,
        s3_bucket=args.s3_bucket,
        s3_prefix=args.s3_prefix,
        s3_telemetry_prefix=args.s3_telemetry_prefix,
        s3_endpoint_url=args.s3_endpoint_url,
        s3_region=args.s3_region,
        s3_force_path_style=args.s3_force_path_style,
    )
    if not orchestrator.acquire_master():
        raise SystemExit("could not acquire local master fencing lease")

    try:
        print(
            json.dumps(
                orchestrator.load(args.jobs, args.workers),
                indent=2,
                sort_keys=True,
            )
        )
    finally:
        orchestrator.release_master()
    return 0


def run_failover(args: argparse.Namespace) -> int:
    if args.root.exists():
        shutil.rmtree(args.root)

    cluster = ClusterSimulator(args.root)
    first = cluster.elect("node-1")
    cluster.fail(first["winner"])
    second = cluster.elect("node-2")

    print(
        json.dumps(
            {
                "initial": first,
                "failed": first["winner"],
                "failover": second,
                "majority": cluster.majority,
                "new_leader": second["winner"] if second else None,
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


def run_server(args: argparse.Namespace) -> int:
    peers = json.loads(args.peers)
    node_id = os.environ.get(
        "SQO_NODE_ID",
        args.bind.rsplit(":", 1)[-1],
    )
    state_dir = args.state_dir or (args.root / "state")
    lease_dir = args.lease_dir or (args.root / "leases")

    NodeRuntime(
        node_id=node_id,
        bind=args.bind,
        peers=peers,
        state_dir=state_dir,
        lease_dir=lease_dir,
        lease_ttl=float(os.environ.get("SQO_LEASE_TTL_S", "8")),
        election=(
            float(os.environ.get("SQO_ELECTION_MIN_S", "2")),
            float(os.environ.get("SQO_ELECTION_MAX_S", "4")),
        ),
        s3_bucket=args.s3_bucket,
        s3_prefix=args.s3_prefix,
        s3_endpoint_url=args.s3_endpoint_url,
        s3_region=args.s3_region,
        s3_force_path_style=args.s3_force_path_style,
    ).serve()
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.cmd == "load":
        return run_load(args)
    if args.cmd == "failover":
        return run_failover(args)
    if args.cmd == "serve":
        return run_server(args)

    parser.error(f"unknown command: {args.cmd}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
