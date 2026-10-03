import fakeredis

from sqo_orchestrator.agent import RedisSlurmAgent
from sqo_orchestrator.core import JobSpec, NodeDB, SlurmController
from sqo_orchestrator.redis_queue import RedisJobQueue


class FakeSlurm:
    def __init__(self):
        self.submitted = []

    def submit(self, job):
        self.submitted.append(job.job_id)
        return "9001"

    def status(self, scheduler_id):
        from sqo_orchestrator.slurm import SlurmStatus
        return SlurmStatus(scheduler_id, "COMPLETED", "0:0")


def test_redis_claim_is_recorded_locally_and_acked(tmp_path):
    redis = fakeredis.FakeRedis()
    queue = RedisJobQueue(redis, queue_name="gpu")
    db = NodeDB(tmp_path / "db.sqlite", "node-1")
    slurm = FakeSlurm()
    controller = SlurmController(db, slurm, "worker-1")
    agent = RedisSlurmAgent(queue, db, controller)

    queue.enqueue(JobSpec(job_id="j1", command=["true"], queue="gpu"))
    assert agent.dispatch_once() == "9001"

    row = db.get_job("j1")
    assert row["scheduler_id"] == "9001"
    assert row["state"] == "running"
    assert queue.depth() == 0
    assert queue.inflight() == 0

    result = agent.reconcile_once()
    assert result == [("j1", True, "COMPLETED")]
    assert db.get_job("j1")["state"] == "succeeded"


def test_retryable_slurm_failure_returns_to_redis(tmp_path):
    redis = fakeredis.FakeRedis()
    queue = RedisJobQueue(redis, queue_name="gpu")
    db = NodeDB(tmp_path / "db.sqlite", "node-1")

    class FailingSlurm:
        def submit(self, job):
            return "9002"

        def status(self, scheduler_id):
            from sqo_orchestrator.slurm import SlurmStatus
            return SlurmStatus(scheduler_id, "FAILED", "1:0")

    job = JobSpec(job_id="retry-1", command=["false"], queue="gpu", retries=1)
    queue.enqueue(job)

    slurm = FailingSlurm()
    controller = SlurmController(db, slurm, "worker-1")
    agent = RedisSlurmAgent(queue, db, controller)

    assert agent.dispatch_once() == "9002"
    result = agent.reconcile_once()
    assert result[0][3] is True
    assert queue.depth() == 1
    assert db.get_job("retry-1")["state"] == "retry"


def test_retryable_submit_failure_returns_job_to_redis(tmp_path):
    redis = fakeredis.FakeRedis()
    queue = RedisJobQueue(redis, queue_name="gpu")
    db = NodeDB(tmp_path / "db.sqlite", "node-1")

    class BrokenSlurm:
        def submit(self, job):
            raise RuntimeError("controller unavailable")

    job = JobSpec(job_id="submit-retry", command=["true"], queue="gpu", retries=1)
    queue.enqueue(job)
    controller = SlurmController(db, BrokenSlurm(), "worker-1")
    agent = RedisSlurmAgent(queue, db, controller)

    try:
        agent.dispatch_once()
    except RuntimeError:
        pass
    else:
        raise AssertionError("expected submission failure")

    assert queue.depth() == 1
    assert queue.inflight() == 0
    assert db.get_job("submit-retry")["state"] == "retry"
