from sqo_orchestrator.core import JobSpec, SlurmBackend

def test_slurm_script_requests_gpu_and_resources():
    job=JobSpec(job_id="j42",command=["python","train.py","--steps","100"],gpus=4,cpus=8,memory_mb=32768,time_limit_s=3661)
    script=SlurmBackend("dgx",dry_run=True).script(job)
    assert "#SBATCH --partition=dgx" in script
    assert "#SBATCH --gres=gpu:4" in script
    assert "#SBATCH --cpus-per-task=8" in script
    assert "#SBATCH --mem=32768M" in script
    assert "#SBATCH --time=61:01" in script


def test_slurm_gpu_type_qos_constraint_and_partition():
    job = JobSpec(
        job_id="j43",
        command=["python", "infer.py"],
        gpus=2,
        gpu_type="h100",
        partition="dgx-h100",
        qos="research",
        constraint="ram256g",
    )
    script = SlurmBackend("gpu", dry_run=True).script(job)
    assert "#SBATCH --partition=dgx-h100" in script
    assert "#SBATCH --gres=gpu:h100:2" in script
    assert "#SBATCH --qos=research" in script
    assert "#SBATCH --constraint=ram256g" in script
