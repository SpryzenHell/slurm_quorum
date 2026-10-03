from __future__ import annotations

import json
import re
import shutil
import subprocess
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path

from .core import JobSpec


class SlurmError(RuntimeError):
    pass


@dataclass(slots=True)
class SlurmStatus:
    scheduler_id: str
    state: str
    exit_code: str | None = None
    raw: str = ""


TERMINAL_SUCCESS = {"COMPLETED"}
TERMINAL_FAILURE = {
    "FAILED", "CANCELLED", "TIMEOUT", "OUT_OF_MEMORY", "NODE_FAIL",
    "BOOT_FAIL", "DEADLINE", "PREEMPTED", "REVOKED",
}
PENDING_OR_RUNNING = {
    "PENDING", "RUNNING", "CONFIGURING", "COMPLETING", "SUSPENDED",
    "REQUEUED", "RESIZING",
}


class SlurmClient:
    """Thin, testable wrapper around sbatch/squeue/sacct.

    The controller intentionally avoids polling sacct in tight loops because
    sacct performs an RPC to slurmdbd. It uses squeue for live jobs and sacct
    only after the scheduler record leaves the live queue.
    """

    def __init__(
        self,
        partition: str = "gpu",
        poll_s: float = 2.0,
        dry_run: bool = False,
        work_dir: str | Path | None = None,
    ):
        self.partition = partition
        self.poll_s = poll_s
        self.dry_run = dry_run
        self.work_dir = Path(work_dir or ".sqo/slurm")
        self.work_dir.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def available() -> bool:
        return shutil.which("sbatch") is not None

    @staticmethod
    def _parse_submit_id(output: str) -> str:
        value = output.strip()
        match = re.search(r"([0-9]+(?:_[0-9]+)?)$", value)
        if not match:
            raise SlurmError(f"unable to parse Slurm job id from: {value!r}")
        return match.group(1)

    def script(self, job: JobSpec) -> str:
        env = "\n".join(
            f"export {key}={json.dumps(value)}" for key, value in sorted(job.env.items())
        )
        command = " ".join(
            subprocess.list2cmdline([str(part)]) for part in job.command
        )
        wall_minutes = max(1, (job.time_limit_s + 59) // 60)
        lines = [
            "#!/usr/bin/env bash",
            "set -euo pipefail",
            f"#SBATCH --job-name=sqo-{job.job_id[:24]}",
            f"#SBATCH --partition={self.partition}",
            f"#SBATCH --cpus-per-task={job.cpus}",
            f"#SBATCH --mem={job.memory_mb}M",
            f"#SBATCH --time={wall_minutes}",
            f"#SBATCH --comment=sqo:{job.job_id}",
            f"#SBATCH --output={self.work_dir / (job.job_id + '.out')}",
            f"#SBATCH --error={self.work_dir / (job.job_id + '.err')}",
        ]
        if job.gpus:
            lines.append(f"#SBATCH --gres=gpu:{job.gpus}")
        if env:
            lines.append(env)
        lines.append(command)
        return "\n".join(line for line in lines if line) + "\n"

    def find_existing(self, job: JobSpec) -> str | None:
        if self.dry_run or not self.available():
            return None
        name = f"sqo-{job.job_id[:24]}"
        try:
            output = subprocess.check_output(
                ["squeue", "-h", "--name", name, "-o", "%i"],
                text=True,
                stderr=subprocess.STDOUT,
            )
        except subprocess.CalledProcessError:
            output = ""
        ids = [line.strip() for line in output.splitlines() if line.strip()]
        if ids:
            return ids[0]
        try:
            output = subprocess.check_output(
                ["sacct", "-X", "-n", "-P", "--name", name,
                 "--starttime", "now-1day",
                 "--format=JobIDRaw,State"],
                text=True,
                stderr=subprocess.STDOUT,
            )
        except subprocess.CalledProcessError:
            return None
        for line in output.splitlines():
            parts = line.strip().split("|", 1)
            if len(parts) == 2 and parts[0].strip():
                return parts[0].strip()
        return None

    def submit(self, job: JobSpec) -> str:
        if self.dry_run:
            return f"DRY-{job.job_id}"
        if not self.available():
            raise SlurmError("sbatch is not installed or not on PATH")
        script_path: Path | None = None
        try:
            existing = self.find_existing(job)
            if existing is not None:
                return existing
            with tempfile.NamedTemporaryFile(
                "w", prefix=f"{job.job_id}-", suffix=".sbatch",
                dir=self.work_dir, delete=False,
            ) as handle:
                handle.write(self.script(job))
                script_path = Path(handle.name)
            try:
                output = subprocess.check_output(
                    ["sbatch", "--parsable", str(script_path)],
                    text=True,
                    stderr=subprocess.STDOUT,
                )
            except subprocess.CalledProcessError as exc:
                raise SlurmError(exc.output.strip() or "sbatch failed") from exc
            return self._parse_submit_id(output)
        finally:
            if script_path is not None:
                script_path.unlink(missing_ok=True)

    def cancel(self, scheduler_id: str):
        if self.dry_run or scheduler_id.startswith("DRY-"):
            return True
        try:
            subprocess.check_output(
                ["scancel", scheduler_id],
                text=True,
                stderr=subprocess.STDOUT,
            )
        except subprocess.CalledProcessError as exc:
            raise SlurmError(
                exc.output.strip() or f"scancel failed for {scheduler_id}"
            ) from exc
        return True

    def live_status(self, scheduler_id: str) -> SlurmStatus | None:
        if self.dry_run or scheduler_id.startswith("DRY-"):
            return None
        try:
            output = subprocess.check_output(
                ["squeue", "--only-job-state", "-h", "-j", scheduler_id, "-o", "%T"],
                text=True,
                stderr=subprocess.STDOUT,
            )
        except subprocess.CalledProcessError:
            return None
        states = [line.strip().split()[0] for line in output.splitlines() if line.strip()]
        if not states:
            return None
        return SlurmStatus(scheduler_id=scheduler_id, state=states[0], raw=output)

    def account_status(self, scheduler_id: str) -> SlurmStatus | None:
        if self.dry_run or scheduler_id.startswith("DRY-"):
            return SlurmStatus(scheduler_id=scheduler_id, state="COMPLETED", exit_code="0:0")
        try:
            output = subprocess.check_output(
                ["sacct", "-X", "-n", "-P", "-j", scheduler_id, "--format=State,ExitCode"],
                text=True,
                stderr=subprocess.STDOUT,
            )
        except subprocess.CalledProcessError as exc:
            raise SlurmError(exc.output.strip() or "sacct failed") from exc
        rows = [line.strip() for line in output.splitlines() if line.strip()]
        if not rows:
            return None
        state, _, exit_code = rows[0].partition("|")
        return SlurmStatus(
            scheduler_id=scheduler_id,
            state=state.split("+", 1)[0],
            exit_code=exit_code or None,
            raw=rows[0],
        )

    def status(self, scheduler_id: str) -> SlurmStatus | None:
        live = self.live_status(scheduler_id)
        if live is not None:
            return live
        return self.account_status(scheduler_id)

    def wait(self, scheduler_id: str, timeout_s: float | None = None) -> SlurmStatus:
        started = time.monotonic()
        while True:
            status = self.status(scheduler_id)
            if status is not None and (
                status.state in TERMINAL_SUCCESS or status.state in TERMINAL_FAILURE
            ):
                return status
            if timeout_s is not None and time.monotonic() - started >= timeout_s:
                raise TimeoutError(f"timed out waiting for Slurm job {scheduler_id}")
            time.sleep(self.poll_s)
