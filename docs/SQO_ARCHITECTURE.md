# SQO Architecture

```mermaid
flowchart LR
  C[Client / Workload Submitter] --> R[Redis Ready Queue]
  R --> W1[Slurm Agent A]
  R --> W2[Slurm Agent B]
  R --> W3[Slurm Agent C]
  W1 --> DB1[Node-local SQLite WAL]
  W2 --> DB2[Node-local SQLite WAL]
  W3 --> DB3[Node-local SQLite WAL]
  W1 --> S[Slurm sbatch / squeue / sacct]
  W2 --> S
  W3 --> S
  DB1 --> T[Telemetry Replicator]
  DB2 --> T
  DB3 --> T
  T --> O[S3 / MinIO telemetry]
  Q1[Quorum Node 1] <--> Q2[Quorum Node 2]
  Q2 <--> Q3[Quorum Node 3]
  Q3 <--> Q1
  Q1 --> L[S3 distributed mutex]
  Q2 --> L
  Q3 --> L
```

## Responsibilities

Redis is the shared admission plane. A Lua claim atomically moves a job from the ready set to an inflight set and creates a server-clock-based lease. Expired claims can be returned to the ready set for takeover.

SQLite WAL is the node-local execution journal. Claim, heartbeat, scheduler ID, terminal state, retry state, and telemetry events are committed locally with short transactions.

The three quorum nodes maintain leader terms and votes. A leader must also hold the S3 fencing lease before it is considered the active master. The fencing token and term provide the control-plane boundary; the implementation is intentionally an election/heartbeat subset rather than a full replicated Raft state machine.

Slurm is the external scheduler. SQO submits deterministic job names/comments, uses squeue for live state, and uses sacct for accounting after jobs leave the live queue.

Telemetry is at-least-once and crash-deduplicable. WAL sequence ranges produce deterministic S3 segment keys, and the local cursor advances only after the sink flush succeeds.