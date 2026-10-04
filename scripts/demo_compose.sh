#!/usr/bin/env bash
set -euo pipefail

COMPOSE='deploy/docker-compose.sqo.yml'
cleanup(){ docker compose -f "$COMPOSE" down -v --remove-orphans >/dev/null 2>&1 || true; }
trap cleanup EXIT INT TERM

docker compose -f "$COMPOSE" up -d --build

python - <<'PY'
import json, time, urllib.request

for _ in range(120):
    try:
        states=[]
        for port in (8101,8102,8103):
            with urllib.request.urlopen(f'http://127.0.0.1:{port}/health', timeout=.5) as r:
                states.append(json.loads(r.read()))
        leaders=[s for s in states if s.get('role')=='leader']
        if len(leaders)==1:
            print(json.dumps({'cluster':states,'leader':leaders[0]}, indent=2, sort_keys=True))
            break
    except Exception:
        pass
    time.sleep(.5)
else:
    raise SystemExit('quorum did not elect a leader')
PY

docker compose -f "$COMPOSE" exec -T sqo-agent python /app/scripts/check_s3_mutex.py \
  --bucket sqo --prefix slurm-quorum --endpoint-url http://localstack:4566 \
  --region us-east-1 --contenders 3 --force-path-style

docker compose -f "$COMPOSE" exec -T sqo-agent python /app/scripts/enqueue_job.py \
  --redis-url redis://redis:6379/0 --queue gpu --namespace sqo \
  --job-id compose-smoke --gpus 1 --cpus 2 --memory-mb 2048 \
  -- python -c 'print("SQO_COMPOSE_SMOKE_OK")'

for _ in $(seq 1 20); do
  ready="$(docker compose -f "$COMPOSE" exec -T redis redis-cli ZCARD sqo:ready:gpu | tr -d '\r')"
  inflight="$(docker compose -f "$COMPOSE" exec -T redis redis-cli ZCARD sqo:inflight:gpu | tr -d '\r')"
  if [ "$ready" = "0" ] && [ "$inflight" = "0" ]; then
    break
  fi
  sleep 1
done

ready="$(docker compose -f "$COMPOSE" exec -T redis redis-cli ZCARD sqo:ready:gpu | tr -d '\r')"
inflight="$(docker compose -f "$COMPOSE" exec -T redis redis-cli ZCARD sqo:inflight:gpu | tr -d '\r')"
echo "ready=$ready inflight=$inflight"
[ "$ready" = "0" ] && [ "$inflight" = "0" ]

echo '== telemetry objects =='
for _ in $(seq 1 20); do
  if docker compose -f "$COMPOSE" exec -T sqo-agent python -c \
    'import boto3; from botocore.config import Config; c=boto3.client("s3",endpoint_url="http://localstack:4566",region_name="us-east-1",config=Config(s3={"addressing_style":"path"})); r=c.list_objects_v2(Bucket="sqo",Prefix="slurm-quorum/telemetry"); raise SystemExit(0 if r.get("Contents") else 1)'; then
    break
  fi
  sleep 1
done

docker compose -f "$COMPOSE" run --rm sqo-agent python -c \
  'import boto3; from botocore.config import Config; c=boto3.client("s3",endpoint_url="http://localstack:4566",region_name="us-east-1",config=Config(s3={"addressing_style":"path"})); print(c.list_objects_v2(Bucket="sqo",Prefix="slurm-quorum/telemetry").get("Contents", []))'

echo 'compose SQO smoke complete'
