# Deployment

The root repository contains inherited/vendor project files, so install the SQO package from `sqo_orchestrator/` rather than the repository root:

    python -m pip install -e ./sqo_orchestrator

For production AWS/S3 support:

    python -m pip install -e './sqo_orchestrator[aws]'

Run Redis separately and submit work with:

    python scripts/enqueue_job.py --redis-url redis://redis.example:6379/0 --queue gpu --gpus 4 --cpus 8 --memory-mb 32768 -- python train.py

Run a scheduler agent on each worker/control node:

    python scripts/run_slurm_agent.py --redis-url redis://redis.example:6379/0 --queue gpu --partition dgx --worker-id sqo-worker-1

The quorum service is started independently on three nodes with `python -m sqo_orchestrator serve ...`. Use the S3 mode for the shared master fencing lock in a multi-machine deployment.

## AWS permissions

`deploy/aws/sqo-s3-iam-policy.json` limits the runtime identity to lock-object Get/Put/Delete and telemetry-object Put operations. S3 conditional writes use `If-None-Match` for first acquisition and `If-Match` for renewals; S3 conditional deletes use `If-Match` on the lock ETag.

## systemd

Copy `deploy/systemd/sqo-agent.service` to the systemd unit directory and adapt `WorkingDirectory`, Python path, environment file, and cluster-specific partition settings.
