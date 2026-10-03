#!/usr/bin/env python3
import argparse
import json
import uuid

import redis

from sqo_orchestrator.core import JobSpec
from sqo_orchestrator.redis_queue import RedisJobQueue

p = argparse.ArgumentParser(description='Enqueue one SQO job into shared Redis.')
p.add_argument('--redis-url', default='redis://127.0.0.1:6379/0')
p.add_argument('--queue', default='gpu')
p.add_argument('--namespace', default='sqo')
p.add_argument('--job-id', default=None)
p.add_argument('--priority', type=int, default=0)
p.add_argument('--gpus', type=int, default=0)
p.add_argument('--cpus', type=int, default=1)
p.add_argument('--memory-mb', type=int, default=1024)
p.add_argument('--time-limit-s', type=int, default=3600)
p.add_argument('--retries', type=int, default=0)
p.add_argument('command', nargs='+')
args = p.parse_args()

job = JobSpec(
    job_id=args.job_id or uuid.uuid4().hex,
    command=args.command,
    queue=args.queue,
    priority=args.priority,
    gpus=args.gpus,
    cpus=args.cpus,
    memory_mb=args.memory_mb,
    time_limit_s=args.time_limit_s,
    retries=args.retries,
)
queue = RedisJobQueue(
    redis.from_url(args.redis_url),
    queue_name=args.queue,
    namespace=args.namespace,
)
print(json.dumps({'job_id': queue.enqueue(job), 'queue': args.queue}, sort_keys=True))
