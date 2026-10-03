from sqo_orchestrator.core import JobSpec, SlurmBackend

def test_slurm_script_requests_gpu_and_resources():
    job=JobSpec(job_id="j42",command=["python","train.py","--steps","100"],gpus=4,cpus=8,memory_mb=32768,time_limit_s=3661)
    script=SlurmBackend("dgx",dry_run=True).script(job)
    assert "#SBATCH --partition=dgx" in script
    assert "#SBATCH --gres=gpu:4" in script
    assert "#SBATCH --cpus-per-task=8" in script
    assert "#SBATCH --mem=32768M" in script
    assert "#SBATCH --time=61:01" in script
