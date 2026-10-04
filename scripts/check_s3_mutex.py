#!/usr/bin/env python3
import argparse
import json
import multiprocessing as mp
import os
import uuid

from sqo_orchestrator.core import S3Lease


def contender(args):
    bucket, prefix, endpoint, region, resource, owner, force_path_style = args
    lease = S3Lease(
        bucket, prefix, endpoint_url=endpoint, region_name=region,
        force_path_style=force_path_style,
    )
    record = lease.acquire(resource, owner, int(os.getpid()), 20)
    return {
        'owner': owner,
        'acquired': record is not None,
        'fencing_token': record.fencing_token if record else None,
    }


p = argparse.ArgumentParser(description='Race multiple processes for one real S3 mutex.')
p.add_argument('--bucket', required=True)
p.add_argument('--prefix', default='slurm-quorum')
p.add_argument('--endpoint-url', default=os.environ.get('SQO_S3_ENDPOINT_URL'))
p.add_argument('--region', default=os.environ.get('SQO_S3_REGION', 'us-east-1'))
p.add_argument('--contenders', type=int, default=3)
p.add_argument('--force-path-style', action='store_true')
args = p.parse_args()

resource = 'smoke-' + uuid.uuid4().hex
jobs = [
    (args.bucket, args.prefix, args.endpoint_url, args.region, resource, f'contender-{i}', args.force_path_style)
    for i in range(args.contenders)
]
with mp.Pool(args.contenders) as pool:
    results = pool.map(contender, jobs)

print(json.dumps({'resource': resource, 'results': results}, indent=2, sort_keys=True))
winners = [r for r in results if r['acquired']]
if len(winners) != 1:
    raise SystemExit(f'expected exactly one mutex winner, got {len(winners)}')
