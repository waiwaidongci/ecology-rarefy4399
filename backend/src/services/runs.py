from sqlalchemy import select
from fastapi import HTTPException
from src.models import DiversityRun,Survey,Transect,TransectComputation
from src.utils.serialize import row
from src.constants.enums import RunStatus
from .audit import record
from . import computations
from .transects import list_transects


def list_runs(db):return [row(r) for r in db.scalars(select(DiversityRun).order_by(DiversityRun.id.desc()))]
def get_run(db,id):
 obj=db.get(DiversityRun,id)
 if not obj:raise HTTPException(404,'研究不存在')
 return obj


def _survey(db,id):
 survey=db.get(Survey,id)
 if not survey:raise HTTPException(404,'调查期不存在')
 return row(survey)


def create_run(db,data,user):
 """登记研究及其逐样线共享计算。

 输入校验与worker共用同一函数；计算当时结算被复用的部分，登记后即返回来源统计，
 便于页面直接显示“本次有多少条样线沿用别的研究的结果”。
 """
 if not db.get(Survey,data.survey_id):raise HTTPException(404,'调查期不存在')
 valid=set(db.scalars(select(Transect.id).where(Transect.survey_id==data.survey_id)))
 if not set(data.transect_ids)<=valid:raise HTTPException(422,'所选样线必须来自同一调查期')
 config=data.model_dump(exclude={'name','survey_id'})
 survey=_survey(db,data.survey_id);selected=set(data.transect_ids)
 transects=[t for t in list_transects(db,data.survey_id) if t['id'] in selected]
 try:plan=computations.plan(db,survey,transects,config,retry_cancelled=True)
 except ValueError as e:raise HTTPException(422,str(e))
 obj=DiversityRun(survey_id=data.survey_id,name=data.name,config=config,creator_id=user.id)
 db.add(obj);db.flush()
 obj.computations={str(tid):int(c.id) for tid,c in plan['entries'].items() if c is not None}
 for c in plan['entries'].values():
  if c is not None and c.origin_run_id is None:c.origin_run_id=obj.id
 db.commit()
 reused=sum(1 for c in plan['entries'].values() if c is not None and c.status==computations.SUCCEEDED and c.id not in plan['schedule'])
 try:
  computations.dispatch_runs([obj.id])
  computations.dispatch_computations(plan['schedule'])
 except Exception:
  obj.status=RunStatus.FAILED;obj.error='队列不可用';db.commit();raise HTTPException(503,obj.error)
 record(db,user.email,'diversity.queued',{'run_id':obj.id,'reused':reused,'scheduled':len(plan['schedule'])})
 return dict(row(obj),reused_transects=reused,scheduled_transects=len(plan['schedule']))


def stop_run(db,id,user):
 """停止一项研究，不影响仍依赖同一批计算的其他研究。

 先以 cancelled 落库，worker 之后对该研究的任何写入都会因状态不符被丢弃。计算被
 其他活跃研究挂接时保持原状；否则标记停止并清空结果，避免停止后的队列继续计算
 无人需要的结果，也保证停止的研究不留下已发布数值。
 """
 obj=db.get(DiversityRun,id)
 if not obj:raise HTTPException(404,'研究不存在')
 if obj.status in (RunStatus.SUCCEEDED,RunStatus.FAILED):raise HTTPException(409,'研究已结束，无法停止')
 if obj.status==RunStatus.CANCELLED:return row(obj)
 obj.status=RunStatus.CANCELLED;obj.result={};obj.error='研究已停止';obj.stopped_at=computations.now();db.commit()
 ids=sorted({int(v) for v in (obj.computations or {}).values()})
 kept=[];released=[]
 others=db.scalars(select(DiversityRun).where(DiversityRun.id!=obj.id,DiversityRun.status.in_((RunStatus.QUEUED,RunStatus.RUNNING,RunStatus.SUCCEEDED)))).all()
 active={str(v) for r in others for v in (r.computations or {}).values()}
 for cid in ids:
  computation=db.get(TransectComputation,cid)
  if computation is None:continue
  if str(cid) in active:kept.append(cid);continue
  computation.status=computations.CANCELLED;computation.result=None;computation.error=None;computation.owner_token=None;computation.lease_until=None
  released.append(cid)
 db.commit()
 record(db,user.email,'diversity.stopped',{'run_id':obj.id,'kept':kept,'released':released})
 return dict(row(obj),kept_computations=kept,released_computations=released)
