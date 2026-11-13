import logging
from uuid import uuid4
from fastapi import HTTPException
from sqlalchemy import select,update
from ..models.run import DelayRun
from ..models.study import DelayStudy
from ..models.dataset import MilestoneDataset
from ..models.group import RouteGroup
from ..utils.queue import enqueue
from .paired_milestones import summarize
from .tail_attribution import attribute

log=logging.getLogger(__name__)
MAX_ATTEMPTS=3
ACTIVE=("queued","running")

def create(db,body):
 if not db.get(DelayStudy,body.study_id):raise HTTPException(422,"研究不存在")
 active=db.scalar(select(DelayRun).where(DelayRun.study_id==body.study_id,DelayRun.status.in_(ACTIVE)).order_by(DelayRun.created_at.desc()))
 if active:
  # Repeated submissions reuse the in-flight run instead of duplicating results.
  try: enqueue(active.id)
  except Exception: log.warning("Run %s re-enqueue skipped; startup recovery will pick it up",active.id)
  return active
 row=DelayRun(id=str(uuid4()),study_id=body.study_id,status="queued",result={},error="");db.add(row);db.commit()
 try: enqueue(row.id)
 except Exception:
  row.status="failed";row.error="Redis队列不可用，请稍后重试";db.commit();raise HTTPException(503,row.error)
 return row

def claim(db,run_id):
 # Atomically take ownership: only a queued run can be claimed, exactly once.
 res=db.execute(update(DelayRun).where(DelayRun.id==run_id,DelayRun.status=="queued").values(status="running",attempts=DelayRun.attempts+1,fence=DelayRun.fence+1))
 db.commit()
 if not res.rowcount:return None
 row=db.get(DelayRun,run_id,populate_existing=True)
 return row.fence,row.attempts

def save_result(db,run_id,fence,result):
 # Only the currently valid processor may write; a stale fence cannot overwrite.
 res=db.execute(update(DelayRun).where(DelayRun.id==run_id,DelayRun.fence==fence,DelayRun.status=="running").values(status="completed",result=result,error=""))
 db.commit()
 if not res.rowcount:log.warning("Run %s: stale processor (fence=%s) finished late; result discarded",run_id,fence)
 return bool(res.rowcount)

def save_failure(db,run_id,fence,attempts,error):
 final=attempts>=MAX_ATTEMPTS
 res=db.execute(update(DelayRun).where(DelayRun.id==run_id,DelayRun.fence==fence,DelayRun.status=="running").values(status="failed" if final else "queued",error=error))
 db.commit()
 if not res.rowcount:log.warning("Run %s: stale processor (fence=%s) failure not recorded",run_id,fence);return False
 if final:log.warning("Run %s stopped after %s failed attempts: %s",run_id,attempts,error);return True
 try: enqueue(run_id);log.info("Run %s attempt %s failed; re-queued: %s",run_id,attempts,error)
 except Exception: log.exception("Run %s re-queue failed; left queued for startup recovery",run_id)
 return True

def compute(db,run_id):
 claimed=claim(db,run_id)
 if claimed is None:return
 fence,attempts=claimed
 row=db.get(DelayRun,run_id)
 try:
  study=db.get(DelayStudy,row.study_id);dataset=db.get(MilestoneDataset,study.dataset_id);group=db.get(RouteGroup,study.group_id)
  paired=summarize(dataset.events,group.stations);tail=attribute(paired.pop("matrix"),**study.config)
  result={**paired,"tail":tail,"dataset_name":dataset.name,"group_name":group.name,"valid_count":len(paired["rows"]),"rejected_count":len(paired["rejected"])}
 except Exception as e:
  save_failure(db,run_id,fence,attempts,str(e)[:1000]);return
 save_result(db,run_id,fence,result)

def recover_pending(db):
 # Called once at worker startup: interrupted runs resume with their original id;
 # completed/failed runs are never re-run. Enqueue is idempotent.
 rows=db.scalars(select(DelayRun).where(DelayRun.status.in_(ACTIVE))).all()
 pending=[]
 for row in rows:
  if row.status=="running":
   res=db.execute(update(DelayRun).where(DelayRun.id==row.id,DelayRun.status=="running").values(status="queued"))
   db.commit()
   if res.rowcount:pending.append(row.id)
  else:pending.append(row.id)
 for run_id in pending:
  try: enqueue(run_id)
  except Exception: log.exception("Run %s re-enqueue failed during recovery",run_id)
 return len(pending)
