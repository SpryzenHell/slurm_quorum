import time
from pathlib import Path
from sqo_orchestrator.core import NodeDB, JobSpec, JobState
from sqo_orchestrator.recovery import heartbeat, requeue_stale

def test_stale_worker_is_requeued(tmp_path: Path):
    db=NodeDB(tmp_path/"db.sqlite","node-1")
    db.submit(JobSpec(job_id="j1",command=["true"]))
    c=db.connect()
    try:
        assert db.claim("worker-a", c)
        c.execute("UPDATE jobs SET heartbeat_at=? WHERE job_id='j1'", (time.time()-60,))
        c.commit()
    finally:
        c.close()
    assert requeue_stale(db, 10)==1
    assert db.counts()[JobState.RETRY]==1

def test_heartbeat_requires_owner(tmp_path: Path):
    db=NodeDB(tmp_path/"db.sqlite","node-1")
    db.submit(JobSpec(job_id="j1",command=["true"]))
    c=db.connect()
    try: db.claim("worker-a", c)
    finally: c.close()
    assert heartbeat(db,"j1","worker-b") is False
    assert heartbeat(db,"j1","worker-a") is True


def test_scheduler_owned_job_is_not_requeued(tmp_path: Path):
    db=NodeDB(tmp_path/"db.sqlite","node-1")
    db.submit(JobSpec(job_id="j1",command=["true"]))
    c=db.connect()
    try:
        db.claim("worker-a", c)
        c.execute("UPDATE jobs SET scheduler_id='42',heartbeat_at=? WHERE job_id='j1'", (time.time()-60,))
        c.commit()
    finally:
        c.close()
    assert requeue_stale(db, 10)==0
    assert db.get_job("j1")["state"]=="running"
