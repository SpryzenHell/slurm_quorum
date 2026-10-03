from pathlib import Path
from sqo_orchestrator.core import NodeDB, JobSpec, FileLease, ClusterSimulator

def test_wal_and_claim_fencing(tmp_path:Path):
    db=NodeDB(tmp_path/'db.sqlite','n1')
    c=db.connect()
    try:
        assert c.execute('PRAGMA journal_mode').fetchone()[0].lower()=='wal'
    finally:c.close()
    db.submit(JobSpec(job_id='j',command=['true']))
    c1,c2=db.connect(),db.connect()
    try:
        a=db.claim('w1',c1); b=db.claim('w2',c2); assert a and b is None
        assert db.complete('j','w1',{'ok':True},c1); assert not db.complete('j','w2',{'ok':True},c2)
    finally:c1.close();c2.close()

def test_quorum_failover(tmp_path:Path):
    c=ClusterSimulator(tmp_path); assert c.elect('node-1')['winner']=='node-1'; c.fail('node-1'); assert c.elect('node-2')['winner']=='node-2'

def test_lease(tmp_path:Path):
    l=FileLease(tmp_path); assert l.acquire('r','a',1,60); assert l.acquire('r','b',1,60) is None; assert l.renew('r','b',1,60) is None; assert l.renew('r','a',1,60); assert l.release('r','a',1)


def test_higher_term_does_not_assign_fake_leader(tmp_path: Path):
    db = NodeDB(tmp_path / "db.sqlite", "n1")
    from sqo_orchestrator.core import ConsensusNode
    node = ConsensusNode("n1", ["n1", "n2", "n3"], db)
    assert node.update_term(7) is True
    assert node.term == 7
    assert node.role == "follower"
    assert node.leader_id is None
