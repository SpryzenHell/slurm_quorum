from sqo_orchestrator.core import JobSpec
from sqo_orchestrator.slurm import SlurmClient, SlurmError


def test_parse_parsable_id_with_cluster_suffix():
    assert SlurmClient._parse_submit_id('12345') == '12345'
    assert SlurmClient._parse_submit_id('12345;dgx-federation') == '12345'
    assert SlurmClient._parse_submit_id('12345_7;cluster-a') == '12345_7'


def test_parse_parsable_id_rejects_unexpected_output():
    try:
        SlurmClient._parse_submit_id('Submitted batch job 12345')
    except SlurmError:
        pass
    else:
        raise AssertionError('expected SlurmError')


def test_squeue_lookup_uses_full_comment(monkeypatch):
    client = SlurmClient(partition='gpu', dry_run=False)
    job = JobSpec(job_id='abc123', command=['true'])
    monkeypatch.setattr(SlurmClient, 'available', staticmethod(lambda: True))

    calls = []
    def fake_check_output(args, **kwargs):
        calls.append(args)
        if args[0] == 'squeue':
            return '98765    sqo:abc123:attempt:1\n'
        raise AssertionError(f'unexpected command: {args}')

    monkeypatch.setattr('sqo_orchestrator.slurm.subprocess.check_output', fake_check_output)
    assert client.find_existing(job) == '98765'
    assert '-O' in calls[0]
    assert 'JobID,Comment' in calls[0]
