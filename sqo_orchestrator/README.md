# sqo_orchestrator

This directory contains the installable Slurm-Quorum Orchestrator package. The repository root README is the main user guide and covers the supported local, Docker, Redis, Slurm and S3 workflows.

## Install

From the repository root:

    python3 -m venv .venv
    source .venv/bin/activate
    python -m pip install -e "./sqo_orchestrator[test]"

The installation provides the `sqo` command:

    sqo --help

For AWS/S3 support:

    python -m pip install -e "./sqo_orchestrator[aws]"

## Local checks

    python -m pytest -q
    sqo failover --root .sqo/failover
    sqo load --jobs 10000 --workers 4 --root .sqo/load
    bash scripts/check_3node_local.sh .sqo/network

The project also includes `scripts/quickstart.sh`, which creates the virtual environment, installs the package, and runs the basic verification sequence.

## Scope

The quorum runtime implements the leader-election and heartbeat transport used by SQO. It is not a complete replicated Raft log.

The local file lease is intended for single-machine development. Use the S3 lease for a multi-machine deployment.

The Slurm integration is real when the host has `sbatch`, `squeue`, and `sacct`. The Compose lab deliberately uses Slurm dry-run mode so it can be exercised without a Slurm controller.
