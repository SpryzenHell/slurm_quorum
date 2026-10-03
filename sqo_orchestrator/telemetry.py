from __future__ import annotations

import gzip
import io
import json
import time
import uuid
from pathlib import Path


class S3TelemetrySink:
    """Batch immutable telemetry segments into S3."""

    def __init__(
        self,
        bucket: str,
        prefix: str = "slurm-quorum/telemetry",
        client=None,
        batch_size: int = 1000,
        endpoint_url: str | None = None,
        region_name: str | None = None,
    ):
        if client is None:
            import boto3
            kwargs = {}
            if endpoint_url: kwargs["endpoint_url"] = endpoint_url
            if region_name: kwargs["region_name"] = region_name
            client = boto3.client("s3", **kwargs)
        self.client = client
        self.bucket = bucket
        self.prefix = prefix.rstrip("/")
        self.batch_size = batch_size
        self.buffer: list[dict] = []

    def append(self, event: dict):
        self.buffer.append(event)

    def flush(self):
        if not self.buffer:
            return None
        payload = b"".join(
            json.dumps(event, sort_keys=True).encode() + b"\n"
            for event in self.buffer
        )
        compressed = gzip.compress(payload, mtime=0)
        sequences = [event.get("seq") for event in self.buffer]
        numeric = [int(seq) for seq in sequences if isinstance(seq, int)]
        if numeric and len(numeric) == len(self.buffer):
            key = f"{self.prefix}/segment-{numeric[0]}-{numeric[-1]}-{len(self.buffer)}.jsonl.gz"
        else:
            stamp = int(time.time() * 1_000_000)
            key = f"{self.prefix}/segment-{stamp}-{uuid.uuid4().hex[:12]}-{len(self.buffer)}.jsonl.gz"
        try:
            self.client.put_object(
                Bucket=self.bucket,
                Key=key,
                Body=compressed,
                ContentType="application/gzip",
                IfNoneMatch="*",
            )
        except Exception as exc:
            code = getattr(getattr(exc, "response", {}), "get", lambda *_: None)("Error", {}).get("Code") if hasattr(exc, "response") else None
            if code not in {"PreconditionFailed", "412"}:
                raise
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
