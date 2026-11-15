from datetime import datetime,timezone
from sqlalchemy.orm import Mapped,mapped_column
from sqlalchemy import String,JSON,DateTime,Integer
from src.config.database import Base


def utcnow():return datetime.now(timezone.utc)


class TransectComputation(Base):
 """一条样线在固定算法版本与计算参数下的共享计算单元。

 key 只由该样线的输入、m/bootstrap/seed 与算法版本决定，因此不同研究的
 重叠选线命中同一行；结果按样线保存，部分重叠的选线集合也能复用。
 """
 __tablename__='transect_computations'
 id:Mapped[int]=mapped_column(primary_key=True)
 key:Mapped[str]=mapped_column(String(64),unique=True,index=True)
 survey_id:Mapped[int]=mapped_column(index=True)
 transect_id:Mapped[int]=mapped_column(index=True)
 reference:Mapped[str]=mapped_column(String(50))
 m:Mapped[int]=mapped_column(Integer)
 bootstrap:Mapped[int]=mapped_column(Integer)
 seed:Mapped[int]=mapped_column(Integer)
 algorithm:Mapped[str]=mapped_column(String(80))
 status:Mapped[str]=mapped_column(String(20),default='queued')
 result:Mapped[dict|None]=mapped_column(JSON,nullable=True)
 error:Mapped[str|None]=mapped_column(String(1000),nullable=True)
 origin_run_id:Mapped[int|None]=mapped_column(Integer,nullable=True)
 owner_token:Mapped[str|None]=mapped_column(String(64),nullable=True)
 lease_until:Mapped[datetime|None]=mapped_column(DateTime(timezone=True),nullable=True)
 attempts:Mapped[int]=mapped_column(Integer,default=0)
 created_at:Mapped[datetime]=mapped_column(DateTime(timezone=True),default=utcnow)
 updated_at:Mapped[datetime]=mapped_column(DateTime(timezone=True),default=utcnow,onupdate=utcnow)
