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
p.add_argument('--once', action='store_true')
args = p.parse_args()

db = NodeDB(args.root / 'state.db', args.worker_id)
queue = RedisJobQueue(redis.from_url(args.redis_url), queue_name=args.queue, namespace=args.namespace)
slurm = SlurmClient(partition=args.partition, poll_s=args.interval, work_dir=args.root / 'slurm')
controller = SlurmController(db, slurm, args.worker_id)
agent = RedisSlurmAgent(queue, db, controller, lease_s=args.lease_s)

if args.once:
    print({
        'dispatched': agent.dispatch_once(),
        'reconciled': agent.reconcile_once(),
        'requeued_claims': agent.reap_expired_claims(),
    })
else:
    while True:
        agent.dispatch_once()
        agent.reconcile_once()
        agent.reap_expired_claims()
        time.sleep(args.interval)
