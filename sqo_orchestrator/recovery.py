from __future__ import annotations

import time
from .core import NodeDB, JobState

def heartbeat(db: NodeDB, job_id: str, worker_id: str) -> bool:
    with db.connect() as c:
        cur = c.execute("UPDATE jobs SET heartbeat_at=? WHERE job_id=? AND owner=? AND state=?", (time.time(), job_id, worker_id, JobState.RUNNING))
        return cur.rowcount == 1

def requeue_stale(db: NodeDB, timeout_s: float) -> int:
    cutoff = time.time() - timeout_s
    with db.connect() as c:
        rows = c.execute("SELECT job_id FROM jobs WHERE state=? AND heartbeat_at < ?", (JobState.RUNNING, cutoff)).fetchall()
        if rows:
            c.executemany("UPDATE jobs SET state=?, owner=NULL, heartbeat_at=NULL WHERE job_id=? AND state=?", [(JobState.RETRY, r["job_id"], JobState.RUNNING) for r in rows])
            for r in rows:
                db._event(c, "job.requeued_after_stale_worker", r["job_id"], {"timeout_s": timeout_s})
            c.commit()
    return len(rows)
