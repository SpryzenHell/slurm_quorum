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

docker compose -f "$COMPOSE" run --rm sqo-agent python /app/scripts/enqueue_job.py \
  --redis-url redis://redis:6379/0 --queue gpu --namespace sqo \
  --job-id compose-smoke --gpus 1 --cpus 2 --memory-mb 2048 \
  -- python -c 'print("SQO_COMPOSE_SMOKE_OK")'

sleep 3

docker compose -f "$COMPOSE" exec -T redis redis-cli ZCARD sqo:ready:gpu
docker compose -f "$COMPOSE" exec -T redis redis-cli ZCARD sqo:inflight:gpu

echo '== telemetry objects =='
for _ in $(seq 1 20); do
  if docker compose -f "$COMPOSE" run --rm --entrypoint /bin/sh minio-init -c \
    'mc alias set local http://minio:9000 sqo-minio sqo-minio-password >/dev/null 2>&1 && mc ls --recursive local/sqo/slurm-quorum/telemetry 2>/dev/null | grep -q .'; then
    break
  fi
  sleep 1
done

docker compose -f "$COMPOSE" run --rm --entrypoint /bin/sh minio-init -c \
  'mc alias set local http://minio:9000 sqo-minio sqo-minio-password >/dev/null 2>&1 && mc ls --recursive local/sqo/slurm-quorum/telemetry'

echo 'compose SQO smoke complete'
