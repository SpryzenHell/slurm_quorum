from __future__ import annotations

import time

from .core import NodeDB, SlurmController


class SlurmAgent:
    """Claims SQO jobs, submits them to Slurm, and reconciles scheduler state."""

    def __init__(self, db: NodeDB, controller: SlurmController, interval_s: float = 2.0):
        self.db = db
        self.controller = controller
        self.interval_s = interval_s

    def dispatch_once(self):
        with self.db.connect() as c:
            job = self.db.claim(self.controller.worker_id, c)
        if job is None:
            return None
        try:
            return self.controller.submit_claimed(job)
        except Exception as exc:
            current = self.db.get_job(job.job_id)
            should_retry = bool(current and int(current["attempts"]) <= job.retries)
            with self.db.connect() as c:
                self.db.fail(
                    job.job_id,
                    self.controller.worker_id,
                    str(exc),
                    requeue=should_retry,
                    c=c,
                )
            raise

    def reconcile_once(self):
        return self.controller.reconcile()

    def run_forever(self, stop=None):
        while stop is None or not stop.is_set():
            self.dispatch_once()
            self.reconcile_once()
            if stop is not None:
                stop.wait(self.interval_s)
            else:
                time.sleep(self.interval_s)



class RedisSlurmAgent:
    """Cross-node dispatcher: Redis claim -> local WAL record -> Slurm -> Redis ack."""

    def __init__(self, queue, db: NodeDB, controller: SlurmController, lease_s: float = 30.0, telemetry_sink=None):
        self.queue = queue
        self.db = db
        self.controller = controller
        self.lease_s = lease_s
        if telemetry_sink is not None:
            from .core import TelemetryReplicator
            self.telemetry = TelemetryReplicator(db, telemetry_sink)
        else:
            self.telemetry = None

    def dispatch_once(self):
        claim = self.queue.claim(self.controller.worker_id, lease_s=self.lease_s)
        if claim is None:
            return None
        job = claim.job
        row = self.db.ensure_running(job, self.controller.worker_id)
        job.metadata["sqo_attempt"] = int(row["attempts"])
        try:
            scheduler_id = self.controller.submit_claimed(job)
            self.queue.ack(job.job_id, self.controller.worker_id)
            return scheduler_id
        except Exception as exc:
            row = self.db.get_job(job.job_id)
            should_retry = bool(row and int(row["attempts"]) <= job.retries)
            with self.db.connect() as c:
                self.db.fail(
                    job.job_id,
                    self.controller.worker_id,
                    str(exc),
                    requeue=should_retry,
                    c=c,
                )
            self.queue.ack(job.job_id, self.controller.worker_id)
            raise

    def reconcile_once(self):
        return self.controller.reconcile()

    def reap_expired_claims(self):
        return self.queue.requeue_expired()


    def tick(self):
        dispatched = self.dispatch_once()
        reconciled = self.reconcile_once()
        requeued = self.reap_expired_claims()
        replicated = self.telemetry.flush() if self.telemetry is not None else 0
        return {
            "dispatched": dispatched,
            "reconciled": reconciled,
            "requeued_claims": requeued,
            "telemetry_events_replicated": replicated,
        }
