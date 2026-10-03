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

## Distributed queue and Slurm agent

Redis is the shared cross-node admission layer. Jobs are stored in a priority sorted set; a Redis Lua script atomically moves a claim into an inflight set with an expiry lease. Expired claims can be returned to ready state by another worker.

The RedisSlurmAgent bridges that shared claim into node-local SQLite WAL state and then into Slurm. SlurmClient submits with sbatch, checks live state with squeue, and checks accounting with sacct after the job leaves the live queue. Terminal scheduler states are translated into SQO success, failure, or retry state.

Useful commands:

    python scripts/enqueue_job.py --redis-url redis://127.0.0.1:6379/0 --queue gpu --gpus 4 --cpus 8 --memory-mb 32768 -- python train.py

    python scripts/run_slurm_agent.py --redis-url redis://127.0.0.1:6379/0 --queue gpu --partition dgx --worker-id sqo-worker-1

For a full local smoke suite:

    bash scripts/demo_local.sh

Production S3 fencing and telemetry are enabled by supplying an S3 bucket and using the AWS extra dependencies. The local file lease remains available for CI and development.
