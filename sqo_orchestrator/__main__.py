import argparse, json
from pathlib import Path
from .core import Orchestrator, ClusterSimulator

p=argparse.ArgumentParser(prog='sqo'); s=p.add_subparsers(dest='cmd',required=True)
l=s.add_parser('load'); l.add_argument('--jobs',type=int,default=60000); l.add_argument('--workers',type=int,default=8); l.add_argument('--root',type=Path,default=Path('.sqo/load'))
f=s.add_parser('failover'); f.add_argument('--root',type=Path,default=Path('.sqo/failover'))
a=p.parse_args()
if a.cmd=='load':
    o=Orchestrator(a.root); print(json.dumps(o.load(a.jobs,a.workers),indent=2,sort_keys=True))
else:
    c=ClusterSimulator(a.root); first=c.elect('node-1'); c.fail(first['winner']); second=c.elect('node-2'); print(json.dumps({'initial':first,'failed':'node-1','failover':second,'majority':c.majority,'new_leader':second['winner'] if second else None},indent=2,sort_keys=True))
