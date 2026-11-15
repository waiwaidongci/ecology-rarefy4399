import numpy as np
from src.constants.species import SPECIES
from .rarefaction import valid_counts,expected_species
from .diversity import indices
from .bootstrap import bootstrap_intervals
from .fingerprint import fingerprint


def validate_inputs(survey,transects,config):
 """研究工作台与worker共用的输入校验；返回逐样线计数向量与共同范围。"""
 if not transects:raise ValueError('未选择样线')
 if len({t['id'] for t in transects})!=len(transects) or len({t['reference'] for t in transects})!=len(transects):raise ValueError('样线重复')
 if any(t['survey_id']!=survey['id'] for t in transects):raise ValueError('只能比较同一调查期样线')
 if set(config['transect_ids'])!={t['id'] for t in transects}:raise ValueError('选择样线与配置不一致')
 vectors={}
 for t in transects:
  if not set(t['counts'])<=set(SPECIES):raise ValueError('未知物种')
  vectors[t['id']]=valid_counts([t['counts'].get(s,0) for s in SPECIES])
 eligible=[t for t in transects if vectors[t['id']].sum()>0]
 if not eligible:raise ValueError('所有样线为空，无可比较群落')
 common_limit=min(int(vectors[t['id']].sum()) for t in eligible);m=config['m']
 if not isinstance(m,int) or not 0<=m<=common_limit:raise ValueError(f'共同稀释量m超出资格范围0..{common_limit}')
 return vectors,eligible,common_limit


def curve_grid(limit,m):
 """共同区间0..limit上最多41个等距整数节点，再加入精确m。"""
 return sorted(set(np.linspace(0,limit,41,dtype=int).tolist()+[m]))


def transect_meta(t):
 return {k:t[k] for k in ('id','reference','name','habitat')}


def empty_record(t):
 return {**transect_meta(t),'status':'empty','n':0,'observed_species':0,'rarefied':None,'shannon':None,'simpson':None,'curve':[],'intervals':None,'reason':'空样线单列；不参加共同m比较'}


def compute_record(t,a,config,grid,survey,key=None):
 """独立计算一条样线的完整结果；边界m也完全由同一稀释函数给出。"""
 N=int(a.sum())
 if not N:return empty_record(t)
 result=indices(a)
 result.update(transect_meta(t),status='eligible',n=N,rarefied=expected_species(a,config['m']),
   curve=[{'m':x,'expected':expected_species(a,x)} for x in grid],
   intervals=bootstrap_intervals(a,config['m'],config['bootstrap'],config['seed'],key or str(survey['id'])+'/'+t['reference']),
   rare_species=[{'species':s,'count':int(c)} for s,c in zip(SPECIES,a) if 0<c<=2])
 return result


def record_from_shared(t,a,payload,grid,survey=None):
 """用共享计算的核心结果组装研究内记录；曲线节点未缓存时按同一公式补算。"""
 if payload is None:return empty_record(t)
 N=int(payload['n'])
 if not N:return empty_record(t)
 cached=payload.get('curve') or {}
 curve=[{'m':x,'expected':float(cached[str(x)]) if str(x) in cached else expected_species(a,x)} for x in grid]
 return {**transect_meta(t),'status':'eligible','n':N,'observed_species':payload['observed_species'],
   'rarefied':payload['rarefied'],'shannon':payload['shannon'],'simpson':payload['simpson'],'curve':curve,
   'intervals':payload['intervals'],'rare_species':payload['rare_species']}


def build_result(survey,transects,config,common_limit,eligible_count,records,provenance=None):
 m=config['m']
 return {'fingerprint':fingerprint(survey,transects,config),'m':m,'common_limit':common_limit,'eligible':eligible_count,'excluded_empty':len(transects)-eligible_count,'bootstrap':config['bootstrap'],'seed':config['seed'],'transects':records,'provenance':provenance or {},'definitions':{'rarefaction':'individual-based without replacement E[S_m]=sum(1-C(N-n_i,m)/C(N,m)); same survey, same m','shannon':'-sum(p_i ln p_i), plug-in at original N','simpson':'1-sum(p_i^2), plug-in at original N','intervals':'conditional observed-composition multinomial percentile95%; not full field-sampling uncertainty'},'interpretation':'Compare rarefied richness only at common m; do not rank raw richness or original-N indices as if effort were equal; observational differences do not establish conservation causality.'}


def analyze(survey,transects,config):
 """独立路径：不读共享表，逐样线从原始计数重算。"""
 vectors,eligible,common_limit=validate_inputs(survey,transects,config)
 grid=curve_grid(common_limit,config['m'])
 records=[compute_record(t,vectors[t['id']],config,grid,survey) for t in sorted(transects,key=lambda x:x['reference'])]
 return build_result(survey,transects,config,common_limit,len(eligible),records)
