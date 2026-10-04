#!/usr/bin/env python3
import argparse
import json
import shutil
import uuid

from sqo_orchestrator.core import JobSpec
from sqo_orchestrator.slurm import SlurmClient

p = argparse.ArgumentParser(description='Run one end-to-end Slurm SQO smoke job.')
p.add_argument('--partition', default='gpu')
p.add_argument('--timeout', type=float, default=300)
args = p.parse_args()

missing = [name for name in ('sbatch', 'squeue', 'sacct') if shutil.which(name) is None]
if missing:
    raise SystemExit('missing Slurm commands: ' + ', '.join(missing))

job = JobSpec(
    job_id='sqo-smoke-' + uuid.uuid4().hex[:12],
    command=['bash', '-lc', 'echo SQO_SLURM_SMOKE_OK'],
    partition=args.partition,
    cpus=1,
    memory_mb=512,
    time_limit_s=120,
)
client = SlurmClient(partition=args.partition, poll_s=2)
scheduler_id = client.submit(job)
status = client.wait(scheduler_id, timeout_s=args.timeout)
print(json.dumps({
    'job_id': job.job_id,
    'scheduler_id': scheduler_id,
    'state': status.state,
    'exit_code': status.exit_code,
}, indent=2, sort_keys=True))
if status.state != 'COMPLETED' or status.exit_code not in ('0:0', '0:0 '):
    raise SystemExit(1)
