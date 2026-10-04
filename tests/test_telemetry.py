from sqo_orchestrator.telemetry import S3TelemetrySink


class FakeS3:
    def __init__(self):
        self.objects = []

    def put_object(self, **kwargs):
        self.objects.append(kwargs)
        return {"ETag": "ok"}


def test_s3_telemetry_batches_and_flushes():
    client = FakeS3()
    sink = S3TelemetrySink(
        "bucket",
        prefix="sqo/telemetry",
        client=client,
        batch_size=2,
    )
    sink.append({"seq": 1, "kind": "job.started"})
    assert client.objects == []
    sink.append({"seq": 2, "kind": "job.succeeded"})
    assert len(client.objects) == 1
    obj = client.objects[0]
    assert obj["Bucket"] == "bucket"
    assert obj["Key"].startswith("sqo/telemetry/node-unknown/segment-")
    assert obj["Key"].endswith(".jsonl.gz")
    assert obj["ContentType"] == "application/gzip"


def test_s3_telemetry_segments_are_namespaced_by_node():
    first = FakeS3()
    first_sink = S3TelemetrySink(
        "bucket", prefix="sqo/telemetry", client=first, batch_size=1,
    )
    first_sink.append({"seq": 1, "node_id": "node-1", "kind": "job.started"})

    second = FakeS3()
    second_sink = S3TelemetrySink(
        "bucket", prefix="sqo/telemetry", client=second, batch_size=1,
    )
    second_sink.append({"seq": 1, "node_id": "node-2", "kind": "job.started"})

    assert first.objects[0]["Key"] != second.objects[0]["Key"]
    assert "/node-1/" in first.objects[0]["Key"]
    assert "/node-2/" in second.objects[0]["Key"]
