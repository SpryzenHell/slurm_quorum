# Slurm-Quorum Orchestrator

This is the active orchestration layer for the requested resume project, kept separate from the mechanically transformed vendor trees already present in this fork.

## Implemented

- Three-node majority leader election with persistent term/vote state.
- Master fencing lease with an atomic local implementation and an S3 implementation using conditional object creation and ETag-guarded renewal.
- Node-local SQLite in WAL mode with short IMMEDIATE transactions for job claims.
- Exactly-once claim/completion fencing by job ID + owner + state.
- Worker execution with retry-safe completion.
- Append-only telemetry journal with cursor-based asynchronous replication.
- Slurm sbatch adapter for GPU/CPU/memory/time resource requests.

## Verification

    python -m pytest -q
    bash scripts/run_failover_demo.sh
    bash scripts/benchmark_60k.sh 60000 8

The latest verified local 60K run completed all 60,000 synthetic jobs with 8 workers and replicated 180,000 job events to telemetry. The measured throughput was approximately 9.23K jobs/s on the development environment. This is not a DGX-cluster production number.

## Upstream composition

The supplied project configuration identifies:
- https://github.com/rq/rq — queue/worker semantics.
- https://github.com/willemt/raft — majority-term and leader-election model.
- https://github.com/benbjohnson/litestream — WAL-first asynchronous replication model.

The repository already contains transformed/vendor copies of these sources. The new package is the clean orchestration layer rather than a replacement of those projects.

## Safety boundary

The project does not claim that S3 alone implements consensus. The intended safety boundary is majority term + leader identity + S3 fencing lease + fencing token carried into execution. The embedded consensus code implements the leader-election/heartbeat subset required by the orchestration control plane rather than a full replicated Raft state machine.
