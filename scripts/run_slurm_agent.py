#!/usr/bin/env python3
import argparse
import os
import time
from pathlib import Path

import redis

from sqo_orchestrator.agent import RedisSlurmAgent
from sqo_orchestrator.core import NodeDB, SlurmController
from sqo_orchestrator.redis_queue import RedisJobQueue
from sqo_orchestrator.slurm import SlurmClient

p = argparse.ArgumentParser(description='Run the SQO Redis-to-Slurm dispatch agent.')
p.add_argument('--redis-url', default='redis://127.0.0.1:6379/0')
p.add_argument('--queue', default='gpu')
p.add_argument('--namespace', default='sqo')
p.add_argument('--root', type=Path, default=Path('.sqo/agent'))
p.add_argument('--worker-id', default=os.environ.get('SQO_WORKER_ID', 'worker-1'))
p.add_argument('--partition', default='gpu')
p.add_argument('--interval', type=float, default=2.0)
p.add_argument('--lease-s', type=float, default=30.0)
p.add_argument('--s3-bucket', default=os.environ.get('SQO_S3_BUCKET'))
p.add_argument('--s3-prefix', default=os.environ.get('SQO_S3_PREFIX', 'slurm-quorum'))
p.add_argument('--s3-telemetry-prefix', default=os.environ.get('SQO_S3_TELEMETRY_PREFIX'))
p.add_argument('--once', action='store_true')
args = p.parse_args()

db = NodeDB(args.root / 'state.db', args.worker_id)
queue = RedisJobQueue(redis.from_url(args.redis_url), queue_name=args.queue, namespace=args.namespace)
slurm = SlurmClient(partition=args.partition, poll_s=args.interval, work_dir=args.root / 'slurm')
controller = SlurmController(db, slurm, args.worker_id)
telemetry_sink = None
if args.s3_bucket:
    from sqo_orchestrator.telemetry import S3TelemetrySink
    telemetry_sink = S3TelemetrySink(
        args.s3_bucket,
        args.s3_telemetry_prefix or f'{args.s3_prefix}/telemetry',
    )
agent = RedisSlurmAgent(
    queue, db, controller, lease_s=args.lease_s, telemetry_sink=telemetry_sink,
)

if args.once:
    print(agent.tick())
else:
    while True:
        print(agent.tick())
        time.sleep(args.interval)
