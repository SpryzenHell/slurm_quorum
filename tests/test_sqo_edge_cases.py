import fakeredis
import pytest

from sqo_orchestrator.core import NodeDB
from sqo_orchestrator.redis_queue import RedisJobQueue
from sqo_orchestrator.telemetry import S3TelemetrySink
from sqo_orchestrator.core import TelemetryReplicator, JobSpec


def test_enqueue_many_round_trips_through_expired_claim():
    client = fakeredis.FakeRedis()
    queue = RedisJobQueue(client, queue_name="gpu")
    job = JobSpec(job_id="batch-1", command=["true"], queue="gpu", priority=9)

    assert queue.enqueue_many([job]) == 1
    claim = queue.claim("worker-a", lease_s=-1)
    assert claim is not None
    assert queue.requeue_expired() == ["batch-1"]

    second = queue.claim("worker-b", lease_s=30)
    assert second is not None
    assert second.job.job_id == "batch-1"


class FailOnceS3:
    def __init__(self):
        self.calls = 0
        self.objects = []

    def put_object(self, **kwargs):
        self.calls += 1
        if self.calls == 1:
            raise RuntimeError("simulated S3 outage")
        self.objects.append(kwargs)
        return {"ETag": "ok"}


def test_telemetry_upload_failure_keeps_buffer_for_retry():
    client = FailOnceS3()
    sink = S3TelemetrySink("bucket", prefix="sqo/telemetry", client=client, batch_size=2)

    sink.append({"seq": 1, "kind": "job.started"})
    with pytest.raises(RuntimeError):
        sink.append({"seq": 2, "kind": "job.succeeded"})

    assert len(sink.buffer) == 2
    assert client.objects == []

    result = sink.flush()
    assert result is not None
    assert len(client.objects) == 1


def test_telemetry_replicator_retries_after_sink_failure(tmp_path):
    db = NodeDB(tmp_path / "db.sqlite", "node-1")
    with db.connect() as c:
        c.execute("BEGIN IMMEDIATE")
        db._event(c, "job.test", "j1", {"ok": True})
        c.commit()

    client = FailOnceS3()
    sink = S3TelemetrySink("bucket", client=client, batch_size=1)
    rep = TelemetryReplicator(db, sink)

    with pytest.raises(RuntimeError):
        rep.flush()
    assert rep.cursor == 0
    assert db.get_meta("telemetry.cursor") in (None, "0")

    assert rep.flush() == 1
    assert rep.cursor == 1
