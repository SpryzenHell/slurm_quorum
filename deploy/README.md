# Deployment

The deployment files cover the three environments supported by the project:

1. a single-machine local demonstration;
2. a Docker Compose lab using Redis and MinIO with Slurm dry-run mode; and
3. a multi-machine deployment using shared Redis, three quorum nodes, S3 fencing/telemetry and one or more Slurm agents.

The root README is the main user guide. This file focuses on deployment details.

## Package installation

The SQO package is installed from `sqo_orchestrator/`, not from the repository root:

    python3 -m venv .venv
    source .venv/bin/activate
    python -m pip install -e "./sqo_orchestrator[aws]"

The AWS extra is required when the process creates a boto3 S3 client. It is included in the Docker image as well.

## Docker lab

Start the complete local lab:

    docker compose -f deploy/docker-compose.sqo.yml up -d --build

Or run the smoke script:

    bash scripts/demo_compose.sh

The lab contains one Redis instance, one MinIO object store, three quorum nodes, and one SQO agent. The agent uses Slurm dry-run mode; no Slurm controller is expected in the Docker network.

To stop and remove the lab volumes:

    docker compose -f deploy/docker-compose.sqo.yml down -v --remove-orphans

## Production topology

Use a shared Redis/Valkey service for job admission. Run the quorum service on three separate hosts. Run the SQO agent on every host that should be able to submit work to the Slurm controller.

Each quorum node has:

    python -m sqo_orchestrator serve \
      --bind HOST:PORT \
      --peers '{"node-2":"http://HOST2:PORT","node-3":"http://HOST3:PORT"}' \
      --root /var/lib/slurm-quorum/NODE_ID \
      --s3-bucket YOUR_BUCKET \
      --s3-prefix slurm-quorum \
      --s3-region YOUR_REGION

Set a different `SQO_NODE_ID` for each node. The `--peers` object should list the other two nodes.

Each Slurm agent needs access to the same Redis service and to the Slurm client commands:

    sbatch
    squeue
    sacct
    scancel

Start an agent with:

    python scripts/run_slurm_agent.py \
      --redis-url redis://REDIS_HOST:6379/0 \
      --queue gpu \
      --namespace sqo \
      --worker-id sqo-worker-1 \
      --partition dgx

For a real AWS S3 lease and telemetry configuration, add:

    --s3-bucket YOUR_BUCKET
    --s3-prefix slurm-quorum
    --s3-telemetry-prefix slurm-quorum/telemetry
    --s3-region YOUR_REGION

For an S3-compatible endpoint such as MinIO, also add:

    --s3-endpoint-url http://MINIO_HOST:9000
    --s3-force-path-style

## AWS permissions

Start from `deploy/aws/sqo-s3-iam-policy.json` and replace `YOUR_BUCKET`.

The runtime role needs object read/write/delete access only under the SQO lock prefix and object write access under the telemetry prefix.

The example bucket policy denies unconditional lock writes/deletes so application code cannot silently bypass the fencing conditions.

Review the policy against the account's existing bucket policies before applying it.

## systemd

`deploy/systemd/sqo-agent.service` is a starting point for a persistent Slurm agent. Before enabling it:

1. install the package on the host;
2. copy the unit to the systemd unit directory;
3. create `/etc/slurm-quorum/sqo.env`;
4. set the Redis, queue, worker, partition and S3 values;
5. confirm the service account can write its configured local state directory; and
6. run `systemctl daemon-reload` followed by `systemctl enable --now sqo-agent`.

The unit passes the S3 bucket/prefix/endpoint/region values through to the agent. The example is intended for a real AWS endpoint; add `--s3-force-path-style` to the unit only when the deployment uses an S3-compatible service that requires path-style addressing.

## Configuration examples

    configs/sqo.env.example
    configs/slurm_quorum.toml.example

The TOML file is a reference layout; the current CLI uses command-line arguments rather than a TOML parser.

## Operational checks

Before accepting production work, verify:

    python scripts/check_slurm_cluster.py --partition dgx
    python scripts/enqueue_job.py --redis-url redis://REDIS_HOST:6379/0 --queue gpu --partition dgx -- python -c 'print("SQO_OK")'

Then watch the agent output and confirm the corresponding scheduler ID and terminal state.

The quorum endpoints expose:

    GET /health

Use these endpoints from the cluster network to confirm that exactly one node reports `role=leader` during normal operation.

## Troubleshooting

A missing `sbatch`, `squeue` or `sacct` means the host is not ready for real Slurm execution. Use the dry-run mode or install the Slurm client/controller environment first.

An S3 `AccessDenied` response normally means the runtime role or bucket policy does not cover the configured lock/telemetry prefixes.

A Redis connection error means the agent cannot reach the shared admission service; check the Redis address, ACLs, firewall rules and service health.

Do not use the local file lease for a multi-machine production cluster.
