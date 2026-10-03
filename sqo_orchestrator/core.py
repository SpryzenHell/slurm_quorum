from __future__ import annotations

import json, os, random, shutil, sqlite3, subprocess, tempfile, time, uuid
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass, field
from enum import StrEnum
from pathlib import Path
from threading import Event

class JobState(StrEnum):
    QUEUED='queued'; RUNNING='running'; SUCCEEDED='succeeded'; FAILED='failed'; RETRY='retry'

@dataclass(slots=True)
class JobSpec:
    job_id: str = field(default_factory=lambda: uuid.uuid4().hex)
    command: list[str] = field(default_factory=list)
    queue: str = 'default'
    priority: int = 0
    gpus: int = 0
    gpu_type: str | None = None
    cpus: int = 1
    memory_mb: int = 1024
    time_limit_s: int = 3600
    partition: str | None = None
    qos: str | None = None
    constraint: str | None = None
    env: dict[str,str] = field(default_factory=dict)
    retries: int = 0
    metadata: dict = field(default_factory=dict)
    submitted_at: float = field(default_factory=time.time)
    def to_json(self): return json.dumps(asdict(self), sort_keys=True)
    @classmethod
    def from_json(cls, raw): return cls(**json.loads(raw))

SCHEMA='''
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;
PRAGMA busy_timeout=5000;
CREATE TABLE IF NOT EXISTS jobs(
 job_id TEXT PRIMARY KEY,payload TEXT NOT NULL,state TEXT NOT NULL,priority INTEGER NOT NULL,
 attempts INTEGER NOT NULL DEFAULT 0,owner TEXT,queued_at REAL NOT NULL,started_at REAL,
 heartbeat_at REAL,finished_at REAL,result TEXT,error TEXT,
 scheduler_id TEXT,slurm_state TEXT,slurm_exit_code TEXT);
CREATE INDEX IF NOT EXISTS idx_jobs_ready ON jobs(state,priority DESC,queued_at ASC);
CREATE INDEX IF NOT EXISTS idx_jobs_scheduler ON jobs(scheduler_id);
CREATE TABLE IF NOT EXISTS events(
 seq INTEGER PRIMARY KEY AUTOINCREMENT,event_id TEXT UNIQUE NOT NULL,ts REAL NOT NULL,node_id TEXT NOT NULL,
 kind TEXT NOT NULL,job_id TEXT,payload TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY,value TEXT NOT NULL);
'''

class NodeDB:
    def __init__(self,path:Path,node_id:str):
        self.path=Path(path); self.node_id=node_id; self.path.parent.mkdir(parents=True,exist_ok=True); self._init()
    def connect(self):
        c=sqlite3.connect(self.path,timeout=5,isolation_level=None); c.row_factory=sqlite3.Row
        c.execute('PRAGMA busy_timeout=5000'); c.execute('PRAGMA journal_mode=WAL'); c.execute('PRAGMA synchronous=NORMAL'); return c
    def _init(self):
        with self.connect() as c:
            c.executescript(SCHEMA)
            columns = {row[1] for row in c.execute("PRAGMA table_info(jobs)")}
            for name, ddl in (
                ("scheduler_id", "TEXT"),
                ("slurm_state", "TEXT"),
                ("slurm_exit_code", "TEXT"),
            ):
                if name not in columns:
                    c.execute("ALTER TABLE jobs ADD COLUMN " + name + " " + ddl)
    def _event(self,c,kind,job_id,payload):
        eid=uuid.uuid4().hex; c.execute('INSERT INTO events VALUES(NULL,?,?,?,?,?,?)',(eid,time.time(),self.node_id,kind,job_id,json.dumps(payload,sort_keys=True))); return eid
    def submit_many(self,jobs):
        jobs=list(jobs)
        if not jobs:return 0
        with self.connect() as c:
            c.execute('BEGIN IMMEDIATE')
            c.executemany('INSERT INTO jobs(job_id,payload,state,priority,attempts,queued_at) VALUES(?,?,?,?,?,?)',[(j.job_id,j.to_json(),JobState.QUEUED,j.priority,0,j.submitted_at) for j in jobs])
            for j in jobs:self._event(c,'job.queued',j.job_id,{'queue':j.queue,'priority':j.priority})
            c.execute('COMMIT')
        return len(jobs)
    def submit(self,job): self.submit_many([job])
    def claim(self,worker_id,c):
        c.execute('BEGIN IMMEDIATE')
        r=c.execute('SELECT job_id,payload,attempts FROM jobs WHERE state IN (?,?) ORDER BY priority DESC,queued_at ASC LIMIT 1',(JobState.QUEUED,JobState.RETRY)).fetchone()
        if not r:c.execute('ROLLBACK'); return None
        now=time.time(); c.execute('UPDATE jobs SET state=?,owner=?,attempts=attempts+1,started_at=?,heartbeat_at=? WHERE job_id=?',(JobState.RUNNING,worker_id,now,now,r['job_id']))
        job=JobSpec.from_json(r['payload']); self._event(c,'job.started',job.job_id,{'worker':worker_id,'attempt':r['attempts']+1}); c.execute('COMMIT'); return job
    def complete(self,job_id,worker_id,result,c):
        c.execute('BEGIN IMMEDIATE'); cur=c.execute('UPDATE jobs SET state=?,result=?,finished_at=?,heartbeat_at=NULL WHERE job_id=? AND owner=? AND state=?',(JobState.SUCCEEDED,json.dumps(result,sort_keys=True),time.time(),job_id,worker_id,JobState.RUNNING))
        if cur.rowcount:self._event(c,'job.succeeded',job_id,result)
        c.execute('COMMIT'); return cur.rowcount==1
    def fail(self,job_id,worker_id,error,requeue,c):
        st=JobState.RETRY if requeue else JobState.FAILED; c.execute('BEGIN IMMEDIATE'); cur=c.execute('UPDATE jobs SET state=?,error=?,finished_at=?,heartbeat_at=NULL,owner=NULL WHERE job_id=? AND owner=? AND state=?',(st,error[:4000],time.time(),job_id,worker_id,JobState.RUNNING))
        if cur.rowcount:self._event(c,'job.retry' if requeue else 'job.failed',job_id,{'error':error[:4000]})
        c.execute('COMMIT'); return cur.rowcount==1
    def ensure_running(self, job: JobSpec, worker_id: str):
        with self.connect() as c:
            c.execute("BEGIN IMMEDIATE")
            row = c.execute("SELECT * FROM jobs WHERE job_id=?", (job.job_id,)).fetchone()
            now = time.time()
            requested_attempt = max(1, int(job.metadata.get("sqo_attempt", 1)))
            if row is None:
                c.execute(
                    "INSERT INTO jobs(job_id,payload,state,priority,attempts,owner,queued_at,started_at,heartbeat_at) "
                    "VALUES(?,?,?,?,?,?,?,?,?)",
                    (job.job_id, job.to_json(), JobState.RUNNING, job.priority, requested_attempt,
                     worker_id, job.submitted_at, now, now),
                )
                self._event(
                    c, "job.started", job.job_id,
                    {"worker": worker_id, "attempt": requested_attempt, "source": "redis"},
                )
            elif row["state"] == JobState.RUNNING:
                c.execute("UPDATE jobs SET owner=?,heartbeat_at=? WHERE job_id=?", (worker_id, now, job.job_id))
                row = c.execute("SELECT * FROM jobs WHERE job_id=?", (job.job_id,)).fetchone()
            else:
                next_attempt = max(int(row["attempts"]) + 1, requested_attempt)
                c.execute(
                    "UPDATE jobs SET payload=?,state=?,priority=?,attempts=?,owner=?,started_at=?,heartbeat_at=?,finished_at=NULL,error=NULL,scheduler_id=NULL "
                    "WHERE job_id=?",
                    (job.to_json(), JobState.RUNNING, job.priority, next_attempt, worker_id, now, now, job.job_id),
                )
                self._event(
                    c, "job.started", job.job_id,
                    {"worker": worker_id, "attempt": next_attempt, "source": "redis"},
                )
            c.execute("COMMIT")
            return self.get_job(job.job_id)

    def attach_scheduler(self, job_id, scheduler_id):
        with self.connect() as c:
            c.execute("BEGIN IMMEDIATE")
            cur = c.execute(
                "UPDATE jobs SET scheduler_id=? WHERE job_id=? AND state=?",
                (scheduler_id, job_id, JobState.RUNNING),
            )
            if cur.rowcount:
                self._event(c, "slurm.submitted", job_id, {"scheduler_id": scheduler_id})
            c.execute("COMMIT")
            return cur.rowcount == 1

    def set_slurm_status(self, job_id, scheduler_id, state, exit_code=None):
        with self.connect() as c:
            c.execute("BEGIN IMMEDIATE")
            cur = c.execute(
                "UPDATE jobs SET scheduler_id=?,slurm_state=?,slurm_exit_code=? WHERE job_id=?",
                (scheduler_id, state, exit_code, job_id),
            )
            if cur.rowcount:
                self._event(
                    c,
                    "slurm.status",
                    job_id,
                    {"scheduler_id": scheduler_id, "state": state, "exit_code": exit_code},
                )
            c.execute("COMMIT")
            return cur.rowcount == 1

    def slurm_jobs(self):
        with self.connect() as c:
            return c.execute(
                "SELECT job_id,payload,owner,scheduler_id,slurm_state,slurm_exit_code,attempts "
                "FROM jobs WHERE state=? AND scheduler_id IS NOT NULL",
                (JobState.RUNNING,),
            ).fetchall()

    def finalize_slurm(self, job_id, scheduler_id, success, exit_code=None, error=None, requeue=False, slurm_state=None):
        state = JobState.SUCCEEDED if success else (JobState.RETRY if requeue else JobState.FAILED)
        with self.connect() as c:
            c.execute("BEGIN IMMEDIATE")
            cur = c.execute(
                "UPDATE jobs SET state=?,scheduler_id=?,slurm_state=?,slurm_exit_code=?,finished_at=?,owner=NULL,heartbeat_at=NULL,error=? "
                "WHERE job_id=? AND scheduler_id=? AND state=?",
                (
                    state,
                    scheduler_id if success else None,
                    slurm_state or ("COMPLETED" if success else "FAILED"),
                    exit_code,
                    time.time(),
                    error[:4000] if error else None,
                    job_id,
                    scheduler_id,
                    JobState.RUNNING,
                ),
            )
            if cur.rowcount:
                self._event(
                    c,
                    "job.succeeded" if success else ("job.retry" if requeue else "job.failed"),
                    job_id,
                    {"scheduler_id": scheduler_id, "exit_code": exit_code, "error": error},
                )
            c.execute("COMMIT")
            return cur.rowcount == 1

    def get_job(self, job_id):
        with self.connect() as c:
            return c.execute("SELECT * FROM jobs WHERE job_id=?", (job_id,)).fetchone()

    def counts(self):
        with self.connect() as c:return {r['state']:int(r['n']) for r in c.execute('SELECT state,COUNT(*) n FROM jobs GROUP BY state')}
    def pending(self):
        q=self.counts(); return q.get(JobState.QUEUED,0)+q.get(JobState.RETRY,0)+q.get(JobState.RUNNING,0)
    def events_since(self,seq,limit=1000):
        with self.connect() as c:return c.execute('SELECT * FROM events WHERE seq>? ORDER BY seq LIMIT ?',(seq,limit)).fetchall()
    def get_meta(self,k):
        with self.connect() as c:
            r=c.execute('SELECT value FROM meta WHERE key=?',(k,)).fetchone(); return None if r is None else r[0]
    def set_meta(self,k,v):
        with self.connect() as c:c.execute('INSERT INTO meta VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value',(k,v))

@dataclass(slots=True)
class LeaseRecord:
    owner:str; term:int; expires_at:float; fencing_token:str

class FileLease:
    def __init__(self,root): self.root=Path(root); self.root.mkdir(parents=True,exist_ok=True)
    def _p(self,r): return self.root/(r.replace('/','_')+'.json')
    def acquire(self,resource,owner,term,ttl_s):
        p=self._p(resource); rec=LeaseRecord(owner,term,time.time()+ttl_s,uuid.uuid4().hex)
        for _ in range(3):
            try: fd=os.open(p,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
            except FileExistsError:
                try: cur=json.loads(p.read_text())
                except (FileNotFoundError,json.JSONDecodeError): continue
                if cur.get('expires_at',0)>time.time(): return None
                try:p.unlink()
                except FileNotFoundError:pass
                continue
            with os.fdopen(fd,'w') as f:json.dump(asdict(rec),f,sort_keys=True)
            return rec
        return None
    def renew(self,resource,owner,term,ttl_s):
        p=self._p(resource)
        try:cur=json.loads(p.read_text())
        except (FileNotFoundError,json.JSONDecodeError):return None
        if cur.get('owner')!=owner or int(cur.get('term',-1))!=term:return None
        if cur.get('expires_at', 0) <= time.time(): return None
        cur['expires_at']=time.time()+ttl_s
        p.write_text(json.dumps(cur,sort_keys=True))
        return LeaseRecord(**cur)
    def release(self,resource,owner,term):
        p=self._p(resource)
        try:cur=json.loads(p.read_text())
        except (FileNotFoundError,json.JSONDecodeError):return False
        if cur.get('owner')!=owner or int(cur.get('term',-1))!=term:return False
        try:p.unlink(); return True
        except FileNotFoundError:return False

class S3Lease:
    def __init__(self,bucket,prefix='slurm-quorum',client=None,endpoint_url=None,region_name=None,force_path_style=False):
        if client is None:
            import boto3
            kwargs={}
            if endpoint_url: kwargs["endpoint_url"]=endpoint_url
            if region_name: kwargs["region_name"]=region_name
            if force_path_style:
                from botocore.config import Config
                kwargs["config"]=Config(s3={"addressing_style":"path"})
            client=boto3.client('s3',**kwargs)
        self.client=client; self.bucket=bucket; self.prefix=prefix.rstrip('/')
    def _key(self,r):return f'{self.prefix}/locks/{r.replace("/","_")}.json'
    def _read(self,k):
        try:
            o=self.client.get_object(Bucket=self.bucket,Key=k); return json.loads(o['Body'].read()),o.get('ETag','').strip('"')
        except Exception as e:
            code=getattr(getattr(e,'response',{}),'get',lambda *_:None)('Error',{}).get('Code') if hasattr(e,'response') else None
            if code in {'NoSuchKey','404','NotFound'}:return None
            raise
    def acquire(self,resource,owner,term,ttl_s):
        import botocore.exceptions
        k=self._key(resource); rec=LeaseRecord(owner,term,time.time()+ttl_s,uuid.uuid4().hex)
        for _ in range(3):
            try:
                self.client.put_object(
                    Bucket=self.bucket,
                    Key=k,
                    Body=json.dumps(asdict(rec)).encode(),
                    ContentType='application/json',
                    IfNoneMatch='*',
                )
                return rec
            except botocore.exceptions.ClientError as exc:
                code = exc.response.get('Error', {}).get('Code')
                if code not in {'PreconditionFailed', '412', 'Conflict', '409'}:
                    raise
            cur=self._read(k)
            if cur is None:
                continue
            if cur[0].get('expires_at',0) > time.time():
                return None
            _, etag = cur
            try:
                self.client.delete_object(Bucket=self.bucket,Key=k,IfMatch=etag)
            except botocore.exceptions.ClientError:
                return None
        return None
    def renew(self,resource,owner,term,ttl_s):
        import botocore.exceptions
        k=self._key(resource); cur=self._read(k)
        if not cur:return None
        data,etag=cur
        if data.get('owner')!=owner or int(data.get('term',-1))!=term:return None
        if data.get('expires_at', 0) <= time.time(): return None
        data['expires_at']=time.time()+ttl_s
        try:self.client.put_object(Bucket=self.bucket,Key=k,Body=json.dumps(data).encode(),ContentType='application/json',IfMatch=etag)
        except botocore.exceptions.ClientError:return None
        return LeaseRecord(**data)
    def release(self,resource,owner,term):
        import botocore.exceptions
        cur=self._read(self._key(resource))
        if not cur or cur[0].get('owner')!=owner or int(cur[0].get('term',-1))!=term:return False
        _, etag = cur
        try:
            self.client.delete_object(Bucket=self.bucket,Key=self._key(resource),IfMatch=etag)
        except botocore.exceptions.ClientError:
            return False
        return True

class ConsensusNode:
    def __init__(self,node_id,peers,db):
        self.node_id=node_id; self.peers=[p for p in peers if p!=node_id]; self.db=db
        self.term=int(db.get_meta('raft.term') or 0); self.voted_for=db.get_meta('raft.vote') or None; self.role='follower'; self.leader_id=None
    def _persist(self):self.db.set_meta('raft.term',str(self.term)); self.db.set_meta('raft.vote',self.voted_for or '')
    def update_term(self, term):
        if term <= self.term:
            return False
        self.term = term
        self.voted_for = None
        self.role = 'follower'
        self.leader_id = None
        self._persist()
        return True

    def request_vote(self,candidate,term):
        if term<self.term:return False
        if term>self.term:self.term=term; self.voted_for=None; self.role='follower'; self._persist()
        if self.voted_for in (None,'',candidate):self.voted_for=candidate; self._persist(); return True
        return False
    def become_candidate(self):self.term+=1; self.role='candidate'; self.voted_for=self.node_id; self.leader_id=None; self._persist()
    def become_leader(self):self.role='leader'; self.leader_id=self.node_id
    def append_entries(self,term,leader):
        if term<self.term:return False
        self.term=term; self.role='follower'; self.leader_id=leader; self._persist(); return True

class ClusterSimulator:
    def __init__(self,root,nodes=('node-1','node-2','node-3')):
        self.root=Path(root); self.root.mkdir(parents=True,exist_ok=True); self.ids=list(nodes); self.nodes={n:ConsensusNode(n,self.ids,NodeDB(self.root/f'{n}.db',n)) for n in self.ids}; self.dead=set()
    @property
    def majority(self):return len(self.ids)//2+1
    def elect(self,candidate):
        if len([n for n in self.ids if n not in self.dead])<self.majority:return None
        c=self.nodes[candidate]; c.become_candidate(); votes=1
        for n in self.ids:
            if n==candidate or n in self.dead:continue
            if self.nodes[n].request_vote(candidate,c.term):votes+=1
        if votes>=self.majority:
            c.become_leader()
            for n in self.ids:
                if n!=candidate and n not in self.dead:self.nodes[n].append_entries(c.term,candidate)
            return {'winner':candidate,'term':c.term,'votes':votes}
        return None
    def fail(self,node):self.dead.add(node); self.nodes[node].role='dead'

class Telemetry:
    def __init__(self,root):self.root=Path(root);self.root.mkdir(parents=True,exist_ok=True)
    def append(self,e):
        with (self.root/f"events-{e['node_id']}.jsonl").open('a') as f:f.write(json.dumps(e,sort_keys=True)+'\n')
    def count(self):return sum(1 for p in self.root.glob('events-*.jsonl') for _ in p.open())

class TelemetryReplicator:
    def __init__(self,db,sink):self.db=db;self.sink=sink;self.cursor=int(db.get_meta('telemetry.cursor') or 0)
    def flush(self):
        total=0
        while True:
            rows=self.db.events_since(self.cursor)
            if not rows:
                break
            for r in rows:
                self.sink.append({
                    'seq':r['seq'],'event_id':r['event_id'],'ts':r['ts'],
                    'node_id':r['node_id'],'kind':r['kind'],'job_id':r['job_id'],
                    'payload':json.loads(r['payload']),
                })
            flush_sink=getattr(self.sink,'flush',None)
            if flush_sink is not None:
                flush_sink()
            self.cursor=rows[-1]['seq']
            self.db.set_meta('telemetry.cursor',str(self.cursor))
            total+=len(rows)
            if len(rows)<1000:
                break
        return total

class SlurmBackend:
    def __init__(self,partition='gpu',dry_run=False):self.partition=partition;self.dry_run=dry_run
    def script(self,job):
        env='\n'.join(f'export {k}={json.dumps(v)}' for k,v in sorted(job.env.items()))
        cmd=' '.join(subprocess.list2cmdline([x]) for x in job.command)
        partition=job.partition or self.partition
        wall_minutes, wall_seconds = divmod(job.time_limit_s, 60)
        lines=[
            '#!/usr/bin/env bash',
            'set -euo pipefail',
            f'#SBATCH --job-name=sqo-{job.job_id[:12]}',
            f'#SBATCH --partition={partition}',
            f'#SBATCH --cpus-per-task={job.cpus}',
            f'#SBATCH --mem={job.memory_mb}M',
            f'#SBATCH --time={wall_minutes:02d}:{wall_seconds:02d}',
        ]
        if job.gpus:
            gres=f"gpu:{job.gpu_type}:{job.gpus}" if job.gpu_type else f"gpu:{job.gpus}"
            lines.append(f'#SBATCH --gres={gres}')
        if job.qos:
            lines.append(f'#SBATCH --qos={job.qos}')
        if job.constraint:
            lines.append(f'#SBATCH --constraint={job.constraint}')
        if env:
            lines.append(env)
        lines.append(cmd)
        return '\n'.join(x for x in lines if x)+'\n'
    def submit(self,job):
        if self.dry_run or shutil.which('sbatch') is None:return {'job_id':job.job_id,'scheduler_id':f'DRY-{job.job_id}'}
        with tempfile.NamedTemporaryFile('w',suffix='.sh',delete=False) as f:f.write(self.script(job));p=f.name
        try:o=subprocess.check_output(['sbatch',p],text=True)
        finally:os.unlink(p)
        return {'job_id':job.job_id,'scheduler_id':o.strip().split()[-1]}

class SlurmController:
    def __init__(self, db, client, worker_id):
        self.db = db
        self.client = client
        self.worker_id = worker_id

    def submit_claimed(self, job):
        scheduler_id = self.client.submit(job)
        if not self.db.attach_scheduler(job.job_id, scheduler_id):
            cancel = getattr(self.client, "cancel", None)
            if cancel is not None:
                try:
                    cancel(scheduler_id)
                except Exception:
                    pass
            raise RuntimeError(
                f"job {job.job_id} lost ownership before Slurm submission was recorded"
            )
        return scheduler_id

    def reconcile(self):
        from .slurm import TERMINAL_FAILURE, TERMINAL_SUCCESS
        finished = []
        for row in self.db.slurm_jobs():
            status = self.client.status(row["scheduler_id"])
            if status is None:
                continue
            self.db.set_slurm_status(
                row["job_id"], row["scheduler_id"], status.state, status.exit_code
            )
            if status.state in TERMINAL_SUCCESS:
                self.db.finalize_slurm(
                    row["job_id"], row["scheduler_id"], True, status.exit_code,
                    slurm_state=status.state,
                )
                finished.append((row["job_id"], True, status.state))
            elif status.state in TERMINAL_FAILURE:
                job = JobSpec.from_json(row["payload"])
                should_retry = row["attempts"] <= job.retries
                self.db.finalize_slurm(
                    row["job_id"],
                    row["scheduler_id"],
                    False,
                    status.exit_code,
                    error=status.state,
                    requeue=should_retry,
                    slurm_state=status.state,
                )
                finished.append((row["job_id"], False, status.state, should_retry))
        return finished

class Orchestrator:
    def __init__(self,root,node_id='node-1',nodes=None,lease_ttl_s=8,s3_bucket=None,s3_prefix='slurm-quorum',s3_telemetry_prefix=None,s3_endpoint_url=None,s3_region=None,s3_force_path_style=False):
        root=Path(root)
        self.node_id=node_id
        self.nodes=list(nodes or ('node-1','node-2','node-3'))
        self.db=NodeDB(root/f'state/{node_id}.db',node_id)
        self.telemetry=Telemetry(root/'telemetry')
        if s3_bucket:
            from .telemetry import S3TelemetrySink
            sink=S3TelemetrySink(
                s3_bucket,
                s3_telemetry_prefix or f'{s3_prefix}/telemetry',
                endpoint_url=s3_endpoint_url,
                region_name=s3_region,
                force_path_style=s3_force_path_style,
            )
            self.rep=TelemetryReplicator(self.db,sink)
            self.lease=S3Lease(s3_bucket,s3_prefix,endpoint_url=s3_endpoint_url,region_name=s3_region,force_path_style=s3_force_path_style)
        else:
            self.rep=TelemetryReplicator(self.db,self.telemetry)
            self.lease=FileLease(root/'leases')
        self.term=0
        self.lease_ttl=lease_ttl_s
    def acquire_master(self):
        self.term+=1; r=self.lease.acquire('cluster-master',self.node_id,self.term,self.lease_ttl); self.master=bool(r); return self.master
    def renew_master(self):
        if not getattr(self,'master',False):return False
        r=self.lease.renew('cluster-master',self.node_id,self.term,self.lease_ttl); self.master=bool(r); return self.master
    def load(self,jobs,workers=8):
        self.db.submit_many(JobSpec(job_id=f'job-{i:06d}',command=['true'],queue='gpu',priority=i%4,metadata={'index':i}) for i in range(jobs))
        stop=Event()
        def worker(w):
            c=self.db.connect(); n=0
            try:
                while not stop.is_set():
                    job=self.db.claim(f'w{w}',c)
                    if job is None:
                        if self.db.pending()==0:break
                        time.sleep(.001);continue
                    self.db.complete(job.job_id,f'w{w}',{'ok':True},c); n+=1
            finally:c.close()
            return n
        t=time.perf_counter()
        with ThreadPoolExecutor(max_workers=workers) as pool: [f.result() for f in [pool.submit(worker,i) for i in range(workers)]]
        elapsed=time.perf_counter()-t
        replicated=self.rep.flush()
        return {'jobs_submitted':jobs,'workers':workers,'elapsed_s':elapsed,'jobs_per_second':jobs/elapsed,'state_counts':self.db.counts(),'telemetry_events_replicated':replicated,'sqlite_wal':True}
