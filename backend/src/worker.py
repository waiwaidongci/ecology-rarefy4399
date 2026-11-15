import json
from pathlib import Path
import numpy as np
from celery import Celery
from sqlalchemy import update
from src.config.database import SessionLocal
from src.config.settings import REDIS_URL,ARTIFACT_DIR
from src.models import DiversityRun,Survey
from src.services.transects import list_transects
from src.services.analysis import build_result,validate_inputs
from src.services.audit import record
from src.services import computations
from src.utils.serialize import row
from src.constants.species import SPECIES
app=Celery('ecology_rarefy',broker=REDIS_URL,backend=REDIS_URL)
app.conf.update(task_track_started=True,worker_prefetch_multiplier=1,task_acks_late=True)
# worker 以 -B 内嵌 beat；不新增服务，每30秒扫描一次失联执行者的租约。
app.conf.beat_schedule={'reap-expired-leases':{'task':'ecology.reap_expired','schedule':30.0}}
ARTIFACT_ROOT=Path(ARTIFACT_DIR)


def _write_artifact(name,text):
 ARTIFACT_ROOT.mkdir(parents=True,exist_ok=True)
 Path(ARTIFACT_ROOT,name).write_text(text)


def _transects(db,survey_id,ids):
 selected=set(ids)
 return [t for t in list_transects(db,survey_id) if t['id'] in selected]


def _transition(db,id,from_states,**values):
 """按原状态条件更新研究；并发停止后 rowcount=0，worker 不会复活已停止的研究。"""
 result=db.execute(update(DiversityRun).where(DiversityRun.id==id,DiversityRun.status.in_(from_states)).values(**values))
 db.commit();return result.rowcount==1


def _runs_using(db,computation_id):
 from sqlalchemy import select
 return [r for r in db.scalars(select(DiversityRun)).all()
   if str(computation_id) in {str(v) for v in (r.computations or {}).values()} and r.status in (computations.QUEUED,computations.RUNNING)]


@app.task(name='ecology.compute_transect',soft_time_limit=150,time_limit=180)
def analyze_computation(id,lease_seconds=computations.LEASE_SECONDS):
 """一条样线的共享计算任务。

 任务可被重复投递、并发领取或失联接管；只有成功 claim 到租约的执行者计算并落库，
 落库时还会再次校验租约凭据，因此被接管后迟到的旧结果不会覆盖新结果。
 早期领取成功、但计算时凭据已被接管的执行者也会在 finish 处丢弃自己的结果。
 """
 with SessionLocal() as db:
  token=computations.claim(db,id,lease_seconds)
  if not token:return {'claimed':False}
  row_obj=db.get(computations.TransectComputation,id)
  try:
   survey=row(db.get(Survey,row_obj.survey_id))
   transect=next(t for t in list_transects(db,row_obj.survey_id) if t['id']==row_obj.transect_id)
   config={'m':int(row_obj.m),'bootstrap':int(row_obj.bootstrap),'seed':int(row_obj.seed),'transect_ids':[row_obj.transect_id]}
   validate_inputs(survey,[transect],config)
   vector=np.asarray([transect['counts'].get(s,0) for s in SPECIES],dtype=np.int64)
   nodes=computations.node_grid(int(vector.sum()),config['m'])
   payload=computations.compute_payload(survey,row_obj.reference,vector,config,nodes)
   if computations.finish(db,id,token,payload):
    record(db,'worker','transect.computation.succeeded',{'computation_id':id,'key':row_obj.key})
    computations.dispatch_runs([r.id for r in _runs_using(db,id)])
    return {'claimed':True,'succeeded':True}
   return {'claimed':True,'superseded':True}
  except Exception as e:
   db.rollback();computations.fail(db,id,token,str(e)[:1000]);record(db,'worker','transect.computation.failed',{'computation_id':id,'error':str(e)[:500]})
   return {'claimed':True,'succeeded':False}


@app.task(name='ecology.reap_expired')
def reap_expired():
 """周期扫描：把租约到期（执行者失联）的 running 行交还队列并重新投递。

 接管只改变领取凭据；旧执行者若稍后完成，其 finish 会因 token 不符被丢弃。
 依赖这些计算的活跃研究也一并重新投递，便于在接管后重新组装结果。
 """
 with SessionLocal() as db:
  ids=computations.expiring(db)
  for id in ids:
   row_obj=db.get(computations.TransectComputation,id)
   if row_obj:row_obj.status=computations.QUEUED;row_obj.owner_token=None;row_obj.lease_until=None
  db.commit()
  run_ids=sorted({r.id for cid in ids for r in _runs_using(db,cid)})
 computations.dispatch_computations(ids)
 computations.dispatch_runs(run_ids)
 return {'reaped':len(ids),'runs':run_ids}


@app.task(name='ecology.analyze',soft_time_limit=180,time_limit=210)
def analyze_run(id):
 """组装研究结果：只使用已成功共享的逐样线结果；停止的研究绝不发布。"""
 with SessionLocal() as db:
  run=db.get(DiversityRun,id)
  if not run:return {'skipped':True}
  if run.status in ('succeeded','failed','cancelled'):return {'skipped':run.status}
  survey=row(db.get(Survey,run.survey_id));data=_transects(db,run.survey_id,run.config['transect_ids'])
  config=dict(run.config)
  try:
   # 先原子地把研究从 queued 推进到 running；已停止的研究在这里就被挡下，不会复活其计算。
   if not _transition(db,id,('queued','running'),status='running',error=None):
    return {'skipped':db.get(DiversityRun,id).status if db.get(DiversityRun,id) else 'missing'}
   # 本任务已确认自己服务活跃研究；仅在停止竞态曾取消其共享行时重新排队。
   plan=computations.plan(db,survey,data,config,retry_cancelled=True)
   if db.get(DiversityRun,id).status=='cancelled':db.rollback();return {'stopped':True}
   entries={tid:c for tid,c in plan['entries'].items() if c is not None}
   pending=[c for c in entries.values() if c.status!=computations.SUCCEEDED]
   if any(c.status==computations.FAILED for c in pending):
    failed=next(c for c in pending if c.status==computations.FAILED)
    message=(failed.error or '共享计算失败')[:1000]
    if _transition(db,id,('queued','running'),status='failed',error=message):
     record(db,'worker','diversity.failed',{'run_id':id,'computation_id':failed.id,'error':message})
    return {'done':False}
   if pending:
    _transition(db,id,('queued','running'),status='running',error=None)
    computations.dispatch_computations(plan['schedule'])
    return {'waiting':len(pending)}
   nodes=computations.node_grid(plan['common_limit'],config['m'])
   records,provenance=computations.assemble(survey,data,config,nodes,plan['entries'],plan['vectors'],run_id=id)
   result=build_result(survey,data,config,plan['common_limit'],len(plan['eligible']),records,provenance)
   if not _transition(db,id,('queued','running'),result=result,status='succeeded',error=None):
    db.rollback();return {'stopped':True}
   _write_artifact(f'diversity-{id}.json',json.dumps(result,ensure_ascii=False,allow_nan=False))
   record(db,'worker','diversity.succeeded',{'run_id':id,'m':result['m'],'eligible':result['eligible'],'empty':result['excluded_empty'],'reused':sum(1 for p in provenance.values() if p.get('reused'))})
   return {'done':True}
  except Exception as e:
   db.rollback();message=str(e)[:1000]
   if _transition(db,id,('queued','running'),status='failed',error=message):
    record(db,'worker','diversity.failed',{'run_id':id,'error':message})
   return {'done':False}
