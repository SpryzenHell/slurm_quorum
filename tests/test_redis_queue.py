import fakeredis

from sqo_orchestrator.core import JobSpec
from sqo_orchestrator.redis_queue import RedisJobQueue


def test_priority_and_lease_claims():
    client = fakeredis.FakeRedis()
    queue = RedisJobQueue(client, queue_name="gpu")
    low = JobSpec(job_id="low", command=["true"], priority=1, queue="gpu")
    high = JobSpec(job_id="high", command=["true"], priority=10, queue="gpu")
    assert queue.enqueue_many(iter([low, high])) == 2

    claim = queue.claim("worker-a", lease_s=30)
    assert claim is not None
    assert claim.job.job_id == "high"
    assert queue.depth() == 1
    assert queue.inflight() == 1
    assert queue.renew("high", "worker-b") is False
    assert queue.renew("high", "worker-a") is True
    assert queue.ack("high", "worker-b") is False
    assert queue.ack("high", "worker-a") is True


def test_expired_claim_returns_to_ready_queue():
    client = fakeredis.FakeRedis()
    queue = RedisJobQueue(client, queue_name="gpu")
    job = JobSpec(job_id="j1", command=["true"], priority=4, queue="gpu")
    queue.enqueue(job)
    claim = queue.claim("worker-a", lease_s=-1)
    assert claim is not None
    assert queue.depth() == 0
    assert queue.requeue_expired() == ["j1"]
    assert queue.depth() == 1
    second = queue.claim("worker-b", lease_s=30)
    assert second is not None
    assert second.job.job_id == "j1"
