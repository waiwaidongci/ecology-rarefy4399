"""API 层验证：重叠选线经 HTTP 建立研究、复用来源可见、停止单项研究。

运行：python3 -m pytest test-artifacts/gb-0014/tests -q
"""
import os,sys
from datetime import date
import numpy as np
import pytest

BACKEND=os.path.abspath(os.path.join(os.path.dirname(__file__),'..','..','..','backend'))
sys.path.insert(0,BACKEND)
os.environ.setdefault('JWT_SECRET','test-secret')
os.environ.setdefault('DATABASE_URL','sqlite:////tmp/gb0014-api.db')
os.environ.setdefault('ARTIFACT_DIR','/tmp/gb0014-artifacts')
os.environ['SEED_DEMO']='false'

from fastapi.testclient import TestClient
from src.config.database import Base,engine,SessionLocal
import src.models
from src.models import User,Survey,Transect,SpeciesCount
from src.services import computations
from src.utils.security import hash_password,issue_token
from src.main import app

DISPATCHED=[]
def fake_dispatch(ids,attr):
 DISPATCHED.append((attr,list(ids)))
 return []
computations._dispatch=fake_dispatch
from src import worker


@pytest.fixture()
def client():
 Base.metadata.drop_all(engine);Base.metadata.create_all(engine)
 with SessionLocal() as db:
  analyst=User(email='analyst@songji.demo',name='林荞 · 样线研究员',role='analyst',password_hash=hash_password('Songji2026!'))
  viewer=User(email='viewer@songji.demo',name='季苔 · 观察记录员',role='viewer',password_hash=hash_password('Songji2026!'))
  db.add_all([analyst,viewer]);db.commit()
  survey=Survey(name='秋季样线调查',survey_date=date(2026,10,14),filename='sj-transects-2026-3.csv')
  db.add(survey);db.commit()
  rng=np.random.default_rng(149)
  for i in range(4):
   t=Transect(survey_id=survey.id,reference=f'SJ-{i+1:02d}',name='样线',habitat='林缘')
   db.add(t);db.flush()
   counts=rng.multinomial(150,rng.dirichlet(np.ones(24)))
   db.add_all([SpeciesCount(transect_id=t.id,species=f'S{j+1:02d}',count=int(c)) for j,c in enumerate(counts)])
  db.commit()
  tokens={'analyst':issue_token(analyst),'viewer':issue_token(viewer)}
 with TestClient(app) as c:
  c.headers.update({'Authorization':'Bearer '+tokens['analyst']})
  c.viewer_headers={'Authorization':'Bearer '+tokens['viewer']}
  yield c


def queue(client,ids,name):
 return client.post('/runs',json={'name':name,'survey_id':1,'transect_ids':ids,'m':20,'bootstrap':50,'seed':11})


def execute(client,run_id):
 with SessionLocal() as db:
  from src.models import DiversityRun
  while True:
   run=db.get(DiversityRun,run_id)
   for cid in (run.computations or {}).values():
    worker.analyze_computation.run(int(cid))
   outcome=worker.analyze_run.run(run_id)
   if not outcome.get('waiting'):return outcome


def test_overlap_through_api_and_source_visible(client):
 a=queue(client,[1,2,3],'研究A');assert a.status_code==201,a.text
 assert a.json()['reused_transects']==0 and a.json()['scheduled_transects']==3
 execute(client,a.json()['id'])
 b=queue(client,[2,3,4],'研究B');assert b.status_code==201
 assert b.json()['reused_transects']==2 and b.json()['scheduled_transects']==1
 execute(client,b.json()['id'])
 run=client.get(f"/runs/{b.json()['id']}").json()
 assert run['status']=='succeeded'
 prov=run['result']['provenance']
 assert prov['2']['origin_run_id']==a.json()['id']
 source=next(t['source'] for t in run['result']['transects'] if t['id']==2)
 assert source['computation_id']==prov['2']['computation_id'] and source['reused'] is True
 assert source['origin_run_id']==a.json()['id']


def test_stop_endpoint_and_viewer_forbidden(client):
 a=queue(client,[1,2],'研究A');b=queue(client,[2,3],'研究B')
 stopped=client.post(f"/runs/{a.json()['id']}/stop")
 assert stopped.status_code==200 and stopped.json()['status']=='cancelled'
 assert stopped.json()['kept_computations'],stopped.json()
 # B 继续完成，不被 A 的停止影响
 execute(client,b.json()['id'])
 assert client.get(f"/runs/{b.json()['id']}").json()['status']=='succeeded'
 # A 不发布结果
 detail=client.get(f"/runs/{a.json()['id']}").json()
 assert detail['status']=='cancelled' and not detail['result']
 # 观察员不能停止
 forbidden=client.post(f"/runs/{b.json()['id']}/stop",headers=client.viewer_headers)
 assert forbidden.status_code==403


def test_stop_rejects_completed_run(client):
 a=queue(client,[1,2],'研究A');execute(client,a.json()['id'])
 assert client.post(f"/runs/{a.json()['id']}/stop").status_code==409


def test_create_validates_and_reuses_input(client):
 bad=client.post('/runs',json={'name':'坏','survey_id':1,'transect_ids':[1,1],'m':20,'bootstrap':50,'seed':11})
 assert bad.status_code==422
 out=client.post('/runs',json={'name':'坏','survey_id':1,'transect_ids':[1,2],'m':200,'bootstrap':50,'seed':11})
 assert out.status_code==422 and 'm' in out.json()['detail']
