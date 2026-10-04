from __future__ import annotations

import json, random, threading, time, urllib.error, urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from .core import NodeDB, ConsensusNode, FileLease

class NodeRuntime:
    """Small HTTP transport around ConsensusNode for a real 3-process local/cluster deployment."""
    def __init__(self, node_id, bind, peers, state_dir=".sqo/state", lease_dir=".sqo/leases", lease_ttl=8.0, election=(2.0,4.0), s3_bucket=None, s3_prefix="slurm-quorum", s3_endpoint_url=None, s3_region=None, s3_force_path_style=False):
        host, port = bind.rsplit(":",1)
        self.node_id=node_id; self.host=host; self.port=int(port); self.peers=peers; self.lease_ttl=lease_ttl; self.election=election
        self.db=NodeDB(f"{state_dir}/{node_id}.db",node_id); self.consensus=ConsensusNode(node_id,list(peers),self.db)
        if s3_bucket:
            from .core import S3Lease
            self.lease=S3Lease(
                s3_bucket,
                s3_prefix,
                endpoint_url=s3_endpoint_url,
                region_name=s3_region,
                force_path_style=s3_force_path_style,
            )
        else:
            self.lease=FileLease(lease_dir)
        self.deadline=time.monotonic()+random.uniform(*election); self.stop=threading.Event(); self.server=None

    def post(self,url,path,payload):
        try:
            req=urllib.request.Request(url.rstrip("/") + path,data=json.dumps(payload).encode(),headers={"Content-Type":"application/json"})
            with urllib.request.urlopen(req,timeout=.7) as r:return json.loads(r.read())
        except (urllib.error.URLError,TimeoutError,OSError,json.JSONDecodeError): return None

    def elect(self):
        self.consensus.become_candidate(); term=self.consensus.term; votes=1
        for peer,url in self.peers.items():
            r=self.post(url,"/raft/request-vote",{"candidate_id":self.node_id,"term":term})
            if r and int(r.get("term",0))>term:
                self.consensus.update_term(int(r["term"]))
                return
            if r and r.get("granted"):votes+=1
        if votes >= (len(self.peers)+1)//2+1:
            if self.lease.acquire("cluster-master",self.node_id,term,self.lease_ttl):
                self.consensus.become_leader()
            else:self.consensus.append_entries(term, self.consensus.leader_id or "")
        else:self.consensus.append_entries(term,self.consensus.leader_id or "")
        self.deadline=time.monotonic()+random.uniform(*self.election)

    def tick(self):
        if self.consensus.role=="leader":
            if self.lease.renew("cluster-master",self.node_id,self.consensus.term,self.lease_ttl) is None:
                self.consensus.append_entries(self.consensus.term,"")
                return
            for url in self.peers.values():self.post(url,"/raft/append-entries",{"term":self.consensus.term,"leader_id":self.node_id})
        elif time.monotonic() >= self.deadline:self.elect()

    def serve(self):
        runtime=self
        class H(BaseHTTPRequestHandler):
            def reply(self,code,payload):
                raw=json.dumps(payload).encode(); self.send_response(code); self.send_header("Content-Type","application/json"); self.send_header("Content-Length",str(len(raw))); self.end_headers(); self.wfile.write(raw)
            def do_GET(self):
                if self.path=="/health": self.reply(200,{"node_id":runtime.node_id,"role":runtime.consensus.role,"term":runtime.consensus.term,"leader_id":runtime.consensus.leader_id})
                else:self.reply(404,{"error":"not found"})
            def do_POST(self):
                n=int(self.headers.get("Content-Length","0")); data=json.loads(self.rfile.read(n) or b"{}")
                if self.path=="/raft/request-vote":
                    ok=runtime.consensus.request_vote(data["candidate_id"],int(data["term"])); self.reply(200,{"voter_id":runtime.node_id,"term":runtime.consensus.term,"granted":ok})
                elif self.path=="/raft/append-entries":
                    ok=runtime.consensus.append_entries(int(data["term"]),data["leader_id"])
                    runtime.deadline=time.monotonic()+random.uniform(*runtime.election)
                    self.reply(200,{"follower_id":runtime.node_id,"term":runtime.consensus.term,"accepted":ok})
                else:self.reply(404,{"error":"not found"})
            def log_message(self,*_):pass
        self.server=ThreadingHTTPServer((self.host,self.port),H)
        t=threading.Thread(target=self.loop,daemon=True); t.start(); self.server.serve_forever()
    def loop(self):
        while not self.stop.is_set():
            self.tick(); self.stop.wait(.1)
    def shutdown(self):
        self.stop.set()
        if self.server:self.server.shutdown(); self.server.server_close()
