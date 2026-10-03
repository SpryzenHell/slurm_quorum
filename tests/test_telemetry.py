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
    assert obj["Key"].startswith("sqo/telemetry/segment-")
    assert obj["Key"].endswith(".jsonl.gz")
    assert obj["ContentType"] == "application/gzip"
