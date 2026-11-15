"""gb-0014 共享计算/停止/接管测试。

运行：python3 -m pytest test-artifacts/gb-0014/tests -q
测试使用 SQLite 与 Celery eager 模式，不依赖 Postgres/Redis/Celery worker。
"""
import os,sys,importlib
from datetime import date,datetime,timedelta,timezone
import numpy as np
import pytest

BACKEND=os.path.abspath(os.path.join(os.path.dirname(__file__),'..','..','..','backend'))
sys.path.insert(0,BACKEND)
os.environ.setdefault('JWT_SECRET','test-secret')
os.environ.setdefault('DATABASE_URL','sqlite:///'+os.path.join('/tmp','gb0014-tests.db'))
os.environ.setdefault('ARTIFACT_DIR','/tmp/gb0014-artifacts')

from src.config.database import Base,engine,SessionLocal
import src.models
from src.models import User,Survey,Transect,SpeciesCount,DiversityRun,TransectComputation
from sqlalchemy import update
from src.services import computations
from src.services import runs as run_service
from src.services.analysis import analyze
from src.services.transects import list_transects
from src.utils.security import hash_password
from src.utils.serialize import row
from src.schemas.run import DiversityInput

_DISPATCH=[]
def fake_dispatch(ids,attr):
 _DISPATCH.append((attr,list(ids)))
 return []
computations._dispatch=fake_dispatch

from src import worker


@pytest.fixture()
def db():
 Base.metadata.drop_all(engine);Base.metadata.create_all(engine)
 with SessionLocal() as session:
  user=User(email='analyst@test',name='测试研究员',role='analyst',password_hash=hash_password('x'))
  session.add(user);session.commit()
  survey=Survey(name='秋季测试调查',survey_date=date(2026,10,14),filename='sj.csv')
  session.add(survey);session.commit()
  rng=np.random.default_rng(149)
  for i in range(6):
   t=Transect(survey_id=survey.id,reference=f'SJ-{i+1:02d}',name=['林缘','溪边','草地'][i%3]+f'样线{i+1:02d}',habitat=['阔叶林缘','溪边灌丛','草地边界'][i%3])
   session.add(t);session.flush()
   counts=np.zeros(24,dtype=int) if i==5 else rng.multinomial(160,rng.dirichlet(np.ones(24)))
   # 让编号更小的样线个体量更小，便于构造共同m边界与重叠选择
   counts=counts[:int(counts.sum())] if False else counts
   session.add_all([SpeciesCount(transect_id=t.id,species=f'S{j+1:02d}',count=int(c)) for j,c in enumerate(counts)])
  session.commit()
  yield session


def data_for(db,survey_id,ids):
 return [t for t in list_transects(db,survey_id) if t['id'] in set(ids)]


def queue_run(db,user,survey_id,ids,m=20,bootstrap=50,seed=11,name='研究'):
 return run_service.create_run(db,DiversityInput(name=name,survey_id=survey_id,transect_ids=ids,m=m,bootstrap=bootstrap,seed=seed),user)


def run_all(db,run_id):
 # 模拟 worker：逐条执行其共享计算，再组装研究
 while True:
  run=db.get(DiversityRun,run_id)
  for cid in (run.computations or {}).values():
   c=db.get(TransectComputation,int(cid))
   if c and c.status in ('queued','failed'):
    worker.analyze_computation.run(int(cid))
  outcome=worker.analyze_run.run(run_id)
  if not outcome.get('waiting'):return outcome


def test_overlap_selection_reuses_and_matches_independent(db):
 user=db.query(User).first();survey=db.query(Survey).first()
 a=queue_run(db,user,survey.id,[1,2,3,4],name='A')
 assert a['scheduled_transects']==4 and a['reused_transects']==0
 run_all(db,a['id'])
 ra=db.get(DiversityRun,a['id']);assert ra.status=='succeeded'
 # 部分重叠：2、3 已成功，只有 5 需要新算
 b=queue_run(db,user,survey.id,[2,3,5],name='B')
 assert b['reused_transects']==2 and b['scheduled_transects']==1
 run_all(db,b['id']);rb=db.get(DiversityRun,b['id']);assert rb.status=='succeeded'
 prov=rb.result['provenance']
 assert prov['2']['origin_run_id']==a['id'] and prov['3']['origin_run_id']==a['id']
 assert prov['5']['origin_run_id']==b['id']
 assert rb.result['transects'][0]['source']['reused'] is True
 # 逐项相同：去掉来源标注后与独立重算完全一致
 independent=analyze(row(survey),data_for(db,survey.id,[2,3,5]),rb.config)
 stripped=[{k:v for k,v in t.items() if k!='source'} for t in rb.result['transects']]
 assert independent['transects']==stripped
 assert independent['fingerprint']==rb.result['fingerprint']
 # 共享表里每个键只有一行
 assert db.query(TransectComputation).filter(TransectComputation.key==prov['2']['key']).count()==1


def test_full_overlap_reuses_without_new_computation(db):
 user=db.query(User).first();survey=db.query(Survey).first()
 a=queue_run(db,user,survey.id,[1,2,3],name='A');run_all(db,a['id'])
 before=db.query(TransectComputation).count()
 b=queue_run(db,user,survey.id,[1,2,3],name='B')
 assert b['reused_transects']==3 and b['scheduled_transects']==0
 assert db.query(TransectComputation).count()==before
 run_all(db,b['id']);rb=db.get(DiversityRun,b['id'])
 assert rb.status=='succeeded'
 assert {p['computation_id'] for p in rb.result['provenance'].values()}=={p['computation_id'] for p in db.get(DiversityRun,a['id']).result['provenance'].values()}


def test_different_params_do_not_share(db):
 user=db.query(User).first();survey=db.query(Survey).first()
 a=queue_run(db,user,survey.id,[1,2],m=20,seed=11,name='A');run_all(db,a['id'])
 b=queue_run(db,user,survey.id,[1,2],m=20,seed=12,name='B')
 assert b['reused_transects']==0 and b['scheduled_transects']==2


def test_concurrent_claim_only_one_executor(db):
 user=db.query(User).first();survey=db.query(Survey).first()
 a=queue_run(db,user,survey.id,[1,2],name='A')
 cid=int(a['computations']['1'])
 # 两个独立会话模拟两个并发任务；条件更新只有一个能拿到租约。
 with SessionLocal() as first,SessionLocal() as second:
  token1=computations.claim(first,cid)
  token2=computations.claim(second,cid)
  assert token1 and token2 is None
  assert first.get(TransectComputation,cid).attempts==1
  assert first.scalar(__import__('sqlalchemy').select(TransectComputation.owner_token))==token1
  # 只有持有者能落结果
  assert computations.finish(second,cid,token2 or 'forged',{'ok':0}) is False
  assert computations.finish(first,cid,token1,{'ok':1}) is True
 assert db.get(TransectComputation,cid).status=='succeeded'
 assert db.get(TransectComputation,cid).result=={'ok':1}


def test_takeover_after_lost_worker_and_late_result_dropped(db):
 user=db.query(User).first();survey=db.query(Survey).first()
 a=queue_run(db,user,survey.id,[1,2],name='A')
 cid=int(a['computations']['1'])
 stale=computations.claim(db,cid,lease_seconds=1)
 row_obj=db.get(TransectComputation,cid);row_obj.lease_until=datetime.now(timezone.utc)-timedelta(seconds=5);db.commit()
 # 失联后：接管扫描把行交还队列并可被新执行者领取
 assert cid in computations.expiring(db)
 worker.reap_expired.run()
 db.expire_all()
 assert db.get(TransectComputation,cid).status=='queued'
 # 接管后由新执行者重算并落库
 assert worker.analyze_computation.run(cid)=={'claimed':True,'succeeded':True}
 db.expire_all();stored=db.get(TransectComputation,cid)
 assert stored.status=='succeeded' and stored.result['n']>0
 computed=stored.result
 # 旧执行者晚到的结果不能覆盖
 assert computations.finish(db,cid,stale,{'ok':'stale'}) is False
 db.expire_all();assert db.get(TransectComputation,cid).result==computed


def test_stale_executor_dropped_while_new_owner_running(db):
 """旧执行者在新执行者仍持租约时落库：凭据不符，结果被静默丢弃。"""
 user=db.query(User).first();survey=db.query(Survey).first()
 a=queue_run(db,user,survey.id,[1],name='A')
 cid=int(a['computations']['1'])
 stale=computations.claim(db,cid,lease_seconds=1)
 row_obj=db.get(TransectComputation,cid);row_obj.lease_until=datetime.now(timezone.utc)-timedelta(seconds=5);db.commit()
 worker.reap_expired.run()
 fresh=computations.claim(db,cid)
 assert fresh and fresh!=stale
 assert computations.finish(db,cid,stale,{'ok':'stale'}) is False
 assert db.get(TransectComputation,cid).status=='running'
 assert computations.finish(db,cid,fresh,{'ok':'fresh'}) is True
 db.expire_all();assert db.get(TransectComputation,cid).result=={'ok':'fresh'}


def test_stop_one_run_does_not_break_shared_dependent(db):
 user=db.query(User).first();survey=db.query(Survey).first()
 a=queue_run(db,user,survey.id,[1,2,3],name='A')
 b=queue_run(db,user,survey.id,[2,3],name='B')
 # 停止 A：2、3 仍被 B 依赖，不能被取消/删除
 result=run_service.stop_run(db,a['id'],user)
 assert result['status']=='cancelled'
 assert set(result['kept_computations'])>=set(int(v) for v in b['computations'].values())
 run_all(db,b['id'])
 rb=db.get(DiversityRun,b['id']);assert rb.status=='succeeded' and rb.result['transects']
 # A 停止后不发布结果，且迟到 worker 不能复活它
 assert db.get(DiversityRun,a['id']).status=='cancelled'
 assert worker.analyze_run.run(a['id'])=={'skipped':'cancelled'}
 assert not db.get(DiversityRun,a['id']).result


def test_worker_assembly_does_not_revive_stopped_run(db):
 """竞态：worker 已读到 running 后研究被停止，组装必须放弃并不发布。"""
 user=db.query(User).first();survey=db.query(Survey).first()
 a=queue_run(db,user,survey.id,[1,2],name='A')
 db.execute(update(DiversityRun).where(DiversityRun.id==a['id']).values(status='running'));db.commit()
 run_service.stop_run(db,a['id'],user)
 assert worker.analyze_run.run(a['id'])=={'skipped':'cancelled'}
 db.expire_all();run=db.get(DiversityRun,a['id'])
 assert run.status=='cancelled' and not run.result


def test_stop_releases_exclusive_unfinished_computation(db):
 user=db.query(User).first();survey=db.query(Survey).first()
 a=queue_run(db,user,survey.id,[1,2],name='A')
 result=run_service.stop_run(db,a['id'],user)
 released=set(result['released_computations'])
 assert released==set(int(v) for v in a['computations'].values())
 for cid in released:assert db.get(TransectComputation,cid).status=='cancelled'
 # 停止后 worker 不能把这些计算重新排上并完成研究
 assert worker.analyze_run.run(a['id'])=={'skipped':'cancelled'}
 assert db.get(DiversityRun,a['id']).status=='cancelled'


def test_shared_then_stopped_during_computation(db):
 """共享期间停止：A、B 共用计算，停止 A 后 B 仍完成；A 独享部分被释放。"""
 user=db.query(User).first();survey=db.query(Survey).first()
 a=queue_run(db,user,survey.id,[1,2,3],name='A')
 b=queue_run(db,user,survey.id,[2,3,4],name='B')
 run_service.stop_run(db,a['id'],user)
 run_all(db,b['id'])
 rb=db.get(DiversityRun,b['id'])
 assert rb.status=='succeeded'
 refs={t['reference'] for t in rb.result['transects'] if t['status']=='eligible'}
 assert refs=={'SJ-02','SJ-03','SJ-04'}
 assert rb.result['provenance']['2']['origin_run_id']==a['id']
 assert db.get(DiversityRun,a['id']).status=='cancelled'
 assert worker.analyze_run.run(a['id'])=={'skipped':'cancelled'}


def test_failed_computation_reported_not_looped(db,monkeypatch):
 user=db.query(User).first();survey=db.query(Survey).first()
 a=queue_run(db,user,survey.id,[1,2],name='坏研究')
 cid=int(a['computations']['1'])
 # 用损坏计数（负数）模拟执行阶段才暴露的坏数据：计算失败，研究报错而不再排队重算。
 from src.services import computations as comp
 original=comp.compute_payload
 monkeypatch.setattr(comp,'compute_payload',lambda *args,**kwargs:(_ for _ in ()).throw(ValueError('计数损坏')))
 worker.analyze_computation.run(cid)
 assert db.get(TransectComputation,cid).status=='failed'
 outcome=worker.analyze_run.run(a['id'])
 assert outcome=={'done':False}
 assert db.get(DiversityRun,a['id']).status=='failed'
 # 再次组装不会把失败的计算重新排入队列
 _DISPATCH.clear();monkeypatch.setattr(comp,'compute_payload',original)
 worker.analyze_run.run(a['id'])
 assert all(not ids for attr,ids in _DISPATCH if attr=='analyze_computation')
