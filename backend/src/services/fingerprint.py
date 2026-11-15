import json,hashlib
from src.constants.species import SPECIES

ALGORITHM='individual-rarefaction-multinomial-v1'

def canonical_transect(t):
 return {'id':t['id'],'survey_id':t['survey_id'],'reference':t['reference'],'name':t['name'],'habitat':t['habitat'],'counts':{s:int(t['counts'].get(s,0)) for s in sorted(SPECIES)}}

def canonical_survey(survey):
 return {k:survey[k] for k in ('id','name','survey_date','filename')}

def _digest(data):
 return hashlib.sha256(json.dumps(data,sort_keys=True,allow_nan=False,separators=(',',':')).encode()).hexdigest()

def transect_key(survey,transect,m,bootstrap,seed):
 """共享计算键：样线输入 + 计算参数 + 算法版本。

 只含单条样线的规范化计数与元数据，同一调查期内不同研究的重叠选线因此得到
 同一键；m/bootstrap/seed 或任一计数变化都会改变键。
 """
 return _digest({'algorithm':ALGORITHM,'survey':canonical_survey(survey),'transect':canonical_transect(transect),'config':{'m':m,'bootstrap':bootstrap,'seed':seed}})

def fingerprint(survey,transects,config):
 canonical=[canonical_transect(t) for t in sorted(transects,key=lambda x:x['reference'])]
 cfg={**config,'transect_ids':sorted(config['transect_ids'])}
 data={'survey':canonical_survey(survey),'transects':canonical,'config':cfg,'algorithm':ALGORITHM}
 return _digest(data)
