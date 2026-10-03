# sqo_orchestrator

Self-contained orchestration layer for the Slurm-Quorum project.

## Local commands

    python -m pytest -q
    python -m sqo_orchestrator failover --root .sqo/failover
    python -m sqo_orchestrator load --jobs 60000 --workers 8 --root .sqo/load

## Networked three-node mode

Start three processes with:

    bash scripts/start_3node_local.sh

Each node exposes:

    GET  /health
    POST /raft/request-vote
    POST /raft/append-entries

Set SQO_S3_BUCKET and install the production extra to use the S3 fencing adapter.

The network demo uses the same majority-election and lease-fencing components as the local tests. It is intentionally a control-plane reference; it does not claim to be a complete Raft replicated state machine or a replacement for Slurm itself.
