import io
import pytest

boto = pytest.importorskip("botocore.exceptions")
from botocore.exceptions import ClientError

from sqo_orchestrator.core import S3Lease


class FakeBody(io.BytesIO):
    pass


class FakeS3:
    def __init__(self):
        self.obj = None
        self.counter = 0
        self.reads = 0

    def put_object(self, **kwargs):
        if kwargs.get("IfNoneMatch") == "*" and self.obj is not None:
            raise ClientError({"Error": {"Code": "PreconditionFailed"}}, "PutObject")
        if "IfMatch" in kwargs and self.obj is not None and self.obj["etag"] != kwargs["IfMatch"]:
            raise ClientError({"Error": {"Code": "PreconditionFailed"}}, "PutObject")
        self.counter += 1
        self.obj = {"body": kwargs["Body"], "etag": f"etag-{self.counter}"}
        return {"ETag": self.obj["etag"]}

    def get_object(self, **kwargs):
        if self.obj is None:
            raise ClientError({"Error": {"Code": "NoSuchKey"}}, "GetObject")
        self.reads += 1
        current = {"Body": FakeBody(self.obj["body"]), "ETag": self.obj["etag"]}
        if self.reads == 2:
            self.counter += 1
            self.obj = {"body": b'{"owner":"other","term":99,"expires_at":9999999999,"fencing_token":"fresh"}', "etag": f"etag-{self.counter}"}
        return current

    def delete_object(self, **kwargs):
        if self.obj is None:
            return {}
        if kwargs.get("IfMatch") and kwargs["IfMatch"] != self.obj["etag"]:
            raise ClientError({"Error": {"Code": "PreconditionFailed"}}, "DeleteObject")
        self.obj = None
        return {}


def test_s3_lease_conditional_create_and_renew():
    client = FakeS3()
    lease = S3Lease("bucket", client=client)
    first = lease.acquire("cluster", "node-1", 1, 60)
    assert first is not None
    assert lease.acquire("cluster", "node-2", 1, 60) is None
    renewed = lease.renew("cluster", "node-1", 1, 60)
    assert renewed is not None
    assert renewed.fencing_token == first.fencing_token


def test_expired_takeover_is_etag_fenced():
    client = FakeS3()
    lease = S3Lease("bucket", client=client)
    first = lease.acquire("cluster", "node-1", 1, -1)
    assert first is not None

    # The fake S3 changes the object after the contender reads the expired lease
    # but before it tries the conditional delete. A safe implementation must abort.
    assert lease.acquire("cluster", "node-2", 2, 60) is None


def test_release_uses_current_etag():
    client = FakeS3()
    lease = S3Lease("bucket", client=client)
    assert lease.acquire("cluster", "node-1", 1, 60) is not None
    assert lease.release("cluster", "node-1", 1) is True
    assert client.obj is None
