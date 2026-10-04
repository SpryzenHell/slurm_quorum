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
local now = redis.call('TIME')
local now_s = tonumber(now[1]) + (tonumber(now[2]) / 1000000)
local expiry = now_s + tonumber(ARGV[1])
redis.call('HSET', KEYS[2], job_id, ARGV[2])
redis.call('ZADD', KEYS[3], expiry, job_id)
return job_id
"""

_RENEW_LUA = r"""
local owner = redis.call('HGET', KEYS[1], ARGV[1])
if not owner or owner ~= ARGV[2] then
  return 0
end
local now = redis.call('TIME')
local now_s = tonumber(now[1]) + (tonumber(now[2]) / 1000000)
local expiry = now_s + tonumber(ARGV[3])
redis.call('ZADD', KEYS[2], expiry, ARGV[1])
return 1
"""

_ACK_LUA = r"""
local owner = redis.call('HGET', KEYS[1], ARGV[1])
if not owner then
  return 0
end
if ARGV[2] ~= '' and owner ~= ARGV[2] then
  return 0
end
redis.call('HDEL', KEYS[1], ARGV[1])
redis.call('ZREM', KEYS[2], ARGV[1])
return 1
"""

_REQUEUE_LUA = r"""
local now = redis.call('TIME')
local now_s = tonumber(now[1]) + (tonumber(now[2]) / 1000000)
local ids = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', now_s, 'LIMIT', 0, ARGV[1])
for _, job_id in ipairs(ids) do
  local meta = redis.call('HGET', KEYS[3], job_id)
  if meta then
    local score = redis.call('HGET', KEYS[4], job_id)
    redis.call('ZREM', KEYS[1], job_id)
    redis.call('HDEL', KEYS[2], job_id)
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
        self.scores_key = f"{namespace}:scores:{queue_name}"
        self.payloads_key = f"{namespace}:payloads:{queue_name}"
        self.claim_script = self.redis.register_script(_CLAIM_LUA)
        self.renew_script = self.redis.register_script(_RENEW_LUA)
        self.ack_script = self.redis.register_script(_ACK_LUA)
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
        pipe.hset(self.scores_key, job.job_id, self._score(job))
        pipe.hset(self.payloads_key, job.job_id, payload)
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
            pipe.hset(self.scores_key, job.job_id, self._score(job))
            pipe.hset(self.payloads_key, job.job_id, payload)
            pipe.zadd(self.ready_key, {job.job_id: self._score(job)})
        pipe.execute()
        return len(jobs)

    def claim(self, worker_id: str, lease_s: float = 30.0) -> RedisClaim | None:
        job_id = self.claim_script(
            keys=[self.ready_key, self.inflight_key, self.lease_key],
            args=[lease_s, worker_id],
        )
        if not job_id:
            return None
        if isinstance(job_id, bytes):
            job_id = job_id.decode()
        payload = self.redis.hget(self.payloads_key, job_id)
        if payload is None:
            self.ack(job_id)
            return None
        if isinstance(payload, bytes):
            payload = payload.decode()
        return RedisClaim(
            job=JobSpec.from_json(payload),
            worker_id=worker_id,
            lease_until=time.time() + lease_s,
        )

    def renew(self, job_id: str, worker_id: str, lease_s: float = 30.0) -> bool:
        result = self.renew_script(
            keys=[self.inflight_key, self.lease_key],
            args=[job_id, worker_id, lease_s],
        )
        return int(result or 0) == 1

    def ack(self, job_id: str, worker_id: str | None = None) -> bool:
        owner = worker_id or ""
        result = self.ack_script(
            keys=[self.inflight_key, self.lease_key],
            args=[job_id, owner],
        )
        return int(result or 0) == 1

    def requeue_expired(self, limit: int = 100) -> list[str]:
        ids = self.requeue_script(
            keys=[self.lease_key, self.inflight_key, self.meta_key, self.scores_key, self.ready_key],
            args=[limit],
        )
        return [
            item.decode() if isinstance(item, bytes) else item
            for item in (ids or [])
        ]

    def depth(self) -> int:
        return int(self.redis.zcard(self.ready_key))

    def inflight(self) -> int:
        return int(self.redis.zcard(self.lease_key))
