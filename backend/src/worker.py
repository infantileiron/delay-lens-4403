import time,logging
from pathlib import Path
from .config.database import Session,initialize
from .utils.queue import dequeue
from .services.run_service import compute,recover_pending
heartbeat=Path("/artifacts/worker-heartbeat")
logging.basicConfig(level=logging.INFO,format="%(asctime)s %(levelname)s %(message)s")
log=logging.getLogger("worker")
def beat():
 try: heartbeat.touch()
 except OSError: pass
def recover():
 while True:
  try:
   initialize()
   with Session() as db: count=recover_pending(db)
   if count: log.info("Re-queued %s queued/interrupted run(s) for resume",count)
   return
  except Exception: log.exception("Startup recovery failed; retrying in 2s");time.sleep(2)
def main():
 recover()
 while True:
  beat()
  try:
   run_id=dequeue()
   if run_id is None: time.sleep(2);continue
   with Session() as db: compute(db,run_id)
  except Exception: log.exception("Worker error");time.sleep(2)
if __name__=="__main__": main()
