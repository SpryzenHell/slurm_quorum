from sqo_orchestrator.core import JobSpec, NodeDB, SlurmController
from sqo_orchestrator.slurm import SlurmClient, SlurmStatus


class FakeSlurm:
    def __init__(self):
        self.next_id = "42"
        self.states = {"42": SlurmStatus("42", "COMPLETED", "0:0")}

    def submit(self, job):
        return self.next_id

    def status(self, scheduler_id):
        return self.states.get(scheduler_id)


def test_dispatch_attaches_slurm_id(tmp_path):
    db = NodeDB(tmp_path / "db.sqlite", "node-1")
    db.submit(JobSpec(job_id="job-1", command=["python", "train.py"]))
    c = db.connect()
    try:
        job = db.claim("worker-1", c)
    finally:
        c.close()

    controller = SlurmController(db, FakeSlurm(), "worker-1")
    assert controller.submit_claimed(job) == "42"
    row = db.get_job("job-1")
    assert row["scheduler_id"] == "42"
    assert row["state"] == "running"


def test_reconcile_success_and_failure_retry(tmp_path):
    db = NodeDB(tmp_path / "db.sqlite", "node-1")
    success = JobSpec(job_id="job-success", command=["true"])
    retry = JobSpec(job_id="job-retry", command=["false"], retries=1)
    db.submit_many([success, retry])

    c = db.connect()
    try:
        a = db.claim("worker-a", c)
        b = db.claim("worker-b", c)
    finally:
        c.close()

    fake = FakeSlurm()
    fake.states = {
        "42": SlurmStatus("42", "COMPLETED", "0:0"),
        "43": SlurmStatus("43", "FAILED", "1:0"),
    }

    fake_ids = iter(["42", "43"])
    fake.submit = lambda job: next(fake_ids)

    controller = SlurmController(db, fake, "recovery")
    controller.submit_claimed(a)
    controller.submit_claimed(b)
    finished = controller.reconcile()

    assert ("job-success", True, "COMPLETED") in finished
    retry_row = db.get_job("job-retry")
    assert retry_row["state"] == "retry"


def test_slurm_time_is_rounded_up():
    client = SlurmClient(partition="dgx", dry_run=True)
    script = client.script(JobSpec(job_id="j1", command=["echo", "ok"], time_limit_s=61))
    assert "#SBATCH --time=2" in script
    assert "#SBATCH --comment=sqo:j1:attempt:1" in script
