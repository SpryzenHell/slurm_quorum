from __future__ import annotations

import gzip
import io
import json
import time
from pathlib import Path


class S3TelemetrySink:
    """Batch immutable telemetry segments into S3."""

    def __init__(
        self,
        bucket: str,
        prefix: str = "slurm-quorum/telemetry",
        client=None,
        batch_size: int = 1000,
    ):
        if client is None:
            import boto3
            client = boto3.client("s3")
        self.client = client
        self.bucket = bucket
        self.prefix = prefix.rstrip("/")
        self.batch_size = batch_size
        self.buffer: list[dict] = []

    def append(self, event: dict):
        self.buffer.append(event)
        if len(self.buffer) >= self.batch_size:
            self.flush()

    def flush(self):
        if not self.buffer:
            return None
        payload = b"".join(
            json.dumps(event, sort_keys=True).encode() + b"\n"
            for event in self.buffer
        )
        compressed = gzip.compress(payload, mtime=0)
        stamp = int(time.time() * 1_000_000)
        key = f"{self.prefix}/segment-{stamp}-{len(self.buffer)}.jsonl.gz"
        self.client.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=compressed,
            ContentType="application/gzip",
        )
        count = len(self.buffer)
        self.buffer.clear()
        return key, count


class FileTelemetrySink:
    def __init__(self, root: str | Path):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)

    def append(self, event: dict):
        node = event.get("node_id", "unknown")
        with (self.root / f"events-{node}.jsonl").open("a") as handle:
            handle.write(json.dumps(event, sort_keys=True) + "\n")

    def flush(self):
        return None
