# Resume evidence

The current repository implementation directly demonstrates:

1. 3-node majority leader election and term advancement after leader failure.
2. S3 conditional-create / ETag-renewal fencing logic, with a local atomic-file equivalent for CI.
3. SQLite WAL and short IMMEDIATE transactions for worker claims.
4. Owner/state fencing that prevents a late worker from completing a job already reassigned.
5. Asynchronous telemetry export with a durable sequence cursor.
6. Slurm sbatch adapter with GPU, CPU, memory, and wall-time resource requests.
7. A local 60K synthetic workload test.

Latest local evidence:
- 60,000 jobs submitted.
- 8 worker threads.
- 60,000 successful completions.
- 180,000 job events replicated.
- 9,231 jobs/s measured on the current development environment.

That local benchmark supports “60K workloads demonstrated” but does not establish “60K monthly HPC workloads across DGX clusters.” That stronger statement should be retained only with real cluster records.


## Integration-stage evidence

The repository now also contains a shared Redis admission queue, a Redis lease-reaper, a Redis-to-Slurm agent, deterministic Slurm attempt identity, Slurm state reconciliation through squeue/sacct, retry re-enqueue, a batched S3 telemetry sink, and production deployment templates.

The GitHub Actions workflow has already passed the core nine-step control-plane suite on the branch, including Python tests, compile checks, failover demo, a 10K concurrent workload, and the three-process network election/failover smoke. New Redis/Slurm integration tests are being exercised in the current CI queue.

Current evidence still stops short of a real Slurm/DGX execution record and a live AWS S3 bucket test. Those require an environment with Slurm controller/accounting services and AWS credentials.
