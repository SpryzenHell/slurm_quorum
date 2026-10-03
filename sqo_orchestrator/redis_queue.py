from __future__ import annotations

import json
import time
from dataclasses import dataclass

import redis

from .core import JobSpec


@dataclass(slots=True)
class RedisClaim:
    job: JobSpec
    worker_id: str
    lease_until: float


_CLAIM_LUA = r"""
local item = redis.call('ZPOPMAX', KEYS[1], 1)
if #item == 0 then return nil end
local job_id = item[1]
local expiry = tonumber(ARGV[1]) + tonumber(ARGV[2])
redis.call('HSET', KEYS[2], job_id, ARGV[3])
redis.call('ZADD', KEYS[3], expiry, job_id)
return job_id
"""

_REQUEUE_LUA = r"""
local ids = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, ARGV[2])
for _, job_id in ipairs(ids) do
  local meta = redis.call('HGET', KEYS[3], job_id)
  if meta then
    local score = redis.call('HGET', KEYS[4], job_id)
    redis.call('ZREM', KEYS[1], job_id)
    redis.call('ZREM', KEYS[2], job_id)
    redis.call('HDEL', KEYS[3], job_id)
    redis.call('HDEL', KEYS[4], job_id)
    if score then redis.call('ZADD', KEYS[5], tonumber(score), job_id) end
  end
end
return ids
"""


class RedisJobQueue:
    """Shared admission queue with lease-based claims.

    SQLite remains the per-node execution journal; Redis only coordinates
    cross-node admission and claim ownership. Expired claims are reinserted
    into the ready set, enabling another worker to take over.
    """

    def __init__(self, client: redis.Redis, queue_name: str = "default", namespace: str = "sqo"):
        self.redis = client
        self.queue_name = queue_name
        self.namespace = namespace
        self.ready_key = f"{namespace}:ready:{queue_name}"
        self.inflight_key = f"{namespace}:inflight:{queue_name}"
        self.lease_key = f"{namespace}:leases:{queue_name}"
        self.meta_key = f"{namespace}:meta:{queue_name}"
        self.claim_script = self.redis.register_script(_CLAIM_LUA)
        self.requeue_script = self.redis.register_script(_REQUEUE_LUA)

    @staticmethod
    def _score(job: JobSpec) -> float:
        return float(job.priority) * 1_000_000_000_000.0 - job.submitted_at

    def enqueue(self, job: JobSpec):
        payload = job.to_json()
        pipe = self.redis.pipeline(transaction=True)
        pipe.hset(self.meta_key, job.job_id, json.dumps({
            "payload": payload,
            "queue": job.queue,
        }, sort_keys=True))
        pipe.hset(f"{self.namespace}:scores", job.job_id, self._score(job))
        pipe.hset(f"{self.namespace}:payloads", job.job_id, payload)
        pipe.zadd(self.ready_key, {job.job_id: self._score(job)})
        pipe.execute()
        return job.job_id

    def enqueue_many(self, jobs):
        jobs = list(jobs)
        pipe = self.redis.pipeline(transaction=True)
        for job in jobs:
            payload = job.to_json()
            pipe.hset(
                self.meta_key,
                job.job_id,
                json.dumps({"payload": payload, "queue": job.queue}, sort_keys=True),
            )
            pipe.hset(f"{self.namespace}:scores", job.job_id, self._score(job))
            pipe.hset(f"{self.namespace}:payloads", job.job_id, payload)
            pipe.zadd(self.ready_key, {job.job_id: self._score(job)})
        pipe.execute()
        return len(jobs)

    def claim(self, worker_id: str, lease_s: float = 30.0) -> RedisClaim | None:
        now = time.time()
        job_id = self.claim_script(
            keys=[self.ready_key, self.inflight_key, self.lease_key],
            args=[now, lease_s, worker_id],
        )
        if not job_id:
            return None
        if isinstance(job_id, bytes):
            job_id = job_id.decode()
        payload = self.redis.hget(f"{self.namespace}:payloads", job_id)
        if payload is None:
            self.ack(job_id)
            return None
        if isinstance(payload, bytes):
            payload = payload.decode()
        return RedisClaim(
            job=JobSpec.from_json(payload),
            worker_id=worker_id,
            lease_until=now + lease_s,
        )

    def renew(self, job_id: str, worker_id: str, lease_s: float = 30.0) -> bool:
        current = self.redis.hget(self.inflight_key, job_id)
        if current is None:
            return False
        if isinstance(current, bytes):
            current = current.decode()
        if current != worker_id:
            return False
        self.redis.zadd(self.lease_key, {job_id: time.time() + lease_s})
        return True

    def ack(self, job_id: str, worker_id: str | None = None) -> bool:
        current = self.redis.hget(self.inflight_key, job_id)
        if current is None:
            return False
        if worker_id is not None:
            if isinstance(current, bytes):
                current = current.decode()
            if current != worker_id:
                return False
        pipe = self.redis.pipeline(transaction=True)
        pipe.hdel(self.inflight_key, job_id)
        pipe.zrem(self.lease_key, job_id)
        pipe.execute()
        return True

    def requeue_expired(self, limit: int = 100) -> list[str]:
        ids = self.requeue_script(
            keys=[self.lease_key, self.inflight_key, self.meta_key, f"{self.namespace}:scores", self.ready_key],
            args=[time.time(), limit],
        )
        return [
            item.decode() if isinstance(item, bytes) else item
            for item in (ids or [])
        ]

    def depth(self) -> int:
        return int(self.redis.zcard(self.ready_key))

    def inflight(self) -> int:
        return int(self.redis.zcard(self.lease_key))
