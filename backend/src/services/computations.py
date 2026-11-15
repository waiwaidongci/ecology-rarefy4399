from datetime import datetime,timedelta,timezone
from uuid import uuid4
from sqlalchemy import select,update
from sqlalchemy.exc import IntegrityError
from src.models import TransectComputation
from .analysis import validate_inputs,compute_record,empty_record,record_from_shared
from .fingerprint import transect_key,ALGORITHM

LEASE_SECONDS=300
QUEUED='queued'
RUNNING='running'
SUCCEEDED='succeeded'
FAILED='failed'
CANCELLED='cancelled'


def now():return datetime.now(timezone.utc)

def _dispatch(ids,attr):
 ids=[i for i in ids if i]
 if not ids:return []
 from src import worker
 return [getattr(worker,attr).delay(i) for i in ids]

def dispatch_computations(ids):return _dispatch(ids,'analyze_computation')
def dispatch_runs(ids):return _dispatch(ids,'analyze_run')


def node_grid(limit,m):
 from .analysis import curve_grid
 return curve_grid(limit,m)


def plan(db,survey,transects,config,retry_cancelled=False,retry_failed=False):
 """按样线登记共享计算；返回逐样线登记行与共同范围。

 键只含单条样线输入、m/bootstrap/seed 与算法版本，因此不同研究的重叠选线命中
 同一行；成功行直接复用，排队/计算中的行等待执行。被停止的操作留下的 cancelled
 行只有在新研究登记（retry_cancelled）时重新排队；worker 组装研究时不复活它们，
 避免停止后的旧任务继续消耗队列。失败行默认保持失败（研究据此报错），只有显式
  retry_failed 才重新排队，避免同一坏输入反复重算。空样线不建行，直接以空记录单列。
 """
 vectors,eligible,common_limit=validate_inputs(survey,transects,config)
 entries={};schedule=[]
 for t in transects:
  if int(vectors[t['id']].sum())==0:entries[t['id']]=None;continue
  key=transect_key(survey,t,config['m'],config['bootstrap'],config['seed'])
  row=db.scalar(select(TransectComputation).where(TransectComputation.key==key))
  if not row:
   row=TransectComputation(key=key,survey_id=survey['id'],transect_id=t['id'],reference=t['reference'],m=config['m'],bootstrap=config['bootstrap'],seed=config['seed'],algorithm=ALGORITHM,status=QUEUED,attempts=0)
   try:
    with db.begin_nested():
     db.add(row);db.flush()
    schedule.append(row.id)
   except IntegrityError:
    # 并发登记同一键：唯一约束只保留一行，读回胜出者复用。
    row=db.scalar(select(TransectComputation).where(TransectComputation.key==key))
   if row is None:entries[t['id']]=None;continue
  if (retry_cancelled and row.status==CANCELLED) or (retry_failed and row.status==FAILED):
   row.status=QUEUED;row.error=None;row.result=None;row.owner_token=None;row.lease_until=None;row.origin_run_id=None
   schedule.append(row.id)
  entries[t['id']]=row
 db.commit()
 return {'entries':entries,'vectors':vectors,'eligible':eligible,'common_limit':common_limit,'schedule':schedule}


def compute_payload(survey,reference,a,config,nodes):
 """一条样线的共享载荷。

 曲线在样线自身N的等距节点（外加m）上保存，研究网格中未缓存的节点按同一公式
 补算，因而不同共同范围的研究也能复用同一行。
 """
 N=int(a.sum())
 if not N:return None
 rec=compute_record({'id':survey['id'],'reference':reference,'name':'','habitat':''},a,config,nodes,survey,key=str(survey['id'])+'/'+reference)
 for k in ('id','reference','name','habitat','status'):rec.pop(k,None)
 rec['curve']={str(p['m']):p['expected'] for p in rec['curve']}
 rec['curve_nodes']=list(nodes)
 return rec


def assemble(survey,transects,config,nodes,entries,vectors,run_id=None):
 """从共享行组装研究记录，同时给出来源标注。

 未成功的共享行（排队、计算中、失败）不发布数值；空线单列且不标注来源。
 逐样线 source 与 provenance 给出 computation_id、共享键、首次计算的来源研究，
 以及本次研究是否复用了别的研究已经算好的结果。
 """
 def mark(record,item):
  record['source']={'computation_id':item.get('computation_id'),'key':item.get('key'),'origin_run_id':item.get('origin_run_id'),
   'reused':item.get('origin_run_id') is not None and run_id is not None and int(item['origin_run_id'])!=int(run_id)}
  return record
 records=[];provenance={}
 for t in sorted(transects,key=lambda x:x['reference']):
  tid=t['id'];row=entries.get(tid)
  if row is None:
   records.append(empty_record(t));provenance[str(tid)]={'status':'empty','reason':'空样线单列，不参加共同量计算'};continue
  meta={k:t[k] for k in ('id','reference','name','habitat')}
  if row.status!=SUCCEEDED:
   records.append({**meta,'status':row.status,'n':int(vectors[tid].sum()),'observed_species':None,'rarefied':None,'shannon':None,'simpson':None,'curve':[],'intervals':None,'reason':'共享计算尚未完成'});continue
  item={'computation_id':int(row.id),'key':row.key,'origin_run_id':row.origin_run_id,'attempts':int(row.attempts or 0)}
  records.append(mark(record_from_shared(t,vectors[tid],row.result,nodes,survey),item))
  provenance[str(tid)]={'status':'shared',**item}
 return records,provenance


def claim(db,id,lease_seconds=LEASE_SECONDS):
 """原子领取：只有把排队中或租约过期的行改成自己 running 的调用者才是有效执行者。

 同一行被并发领取时条件更新只有一个调用者 rowcount=1，其余得到 None。
 """
 token=uuid4().hex;stamp=now();until=stamp+timedelta(seconds=lease_seconds)
 result=db.execute(update(TransectComputation).where(
   TransectComputation.id==id,
   TransectComputation.status.in_((QUEUED,RUNNING)),
   (TransectComputation.status==QUEUED)|(TransectComputation.lease_until.is_(None))|(TransectComputation.lease_until<stamp)
  ).values(status=RUNNING,owner_token=token,lease_until=until,attempts=TransectComputation.attempts+1,updated_at=stamp))
 db.commit();return token if result.rowcount==1 else None


def finish(db,id,token,payload):
 """落结果时校验执行者凭据；被接管后旧执行者晚到的结果 rowcount=0，静默丢弃。"""
 stamp=now()
 result=db.execute(update(TransectComputation).where(TransectComputation.id==id,TransectComputation.owner_token==token,TransectComputation.status==RUNNING).values(status=SUCCEEDED,result=payload,error=None,owner_token=None,lease_until=None,updated_at=stamp))
 db.commit();return result.rowcount==1


def fail(db,id,token,message):
 stamp=now()
 result=db.execute(update(TransectComputation).where(TransectComputation.id==id,TransectComputation.owner_token==token,TransectComputation.status==RUNNING).values(status=FAILED,error=str(message)[:1000],owner_token=None,lease_until=None,updated_at=stamp))
 db.commit();return result.rowcount==1


def expiring(db):
 stamp=now()
 return [r.id for r in db.scalars(select(TransectComputation).where(TransectComputation.status==RUNNING,TransectComputation.lease_until.is_not(None),TransectComputation.lease_until<stamp))]
