from pytz import timezone
import time
from pathlib import Path
import logging
from datetime import datetime

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.executors.pool import ThreadPoolExecutor, ProcessPoolExecutor
from apscheduler.triggers.cron import CronTrigger

from helper.date_calculate import now
from utils.shells import run_sh

# ── 1. Log to stdout so nohup >> scheduler.log captures it ──
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
log = logging.getLogger(__name__)

# ── 2. Task wrapper with visible output ──
BASE_DIR = Path(__file__).resolve().parent
SCRIPT    = BASE_DIR / "tasks" / "update_stock_price_5m.py"


def run_task():
    log.info("===> run_task started (%s)", now().isoformat(timespec="seconds"))
    try:
        if not SCRIPT.exists():
            raise FileNotFoundError(f"{SCRIPT} not found")
        run_sh(command=f"python3.10 {SCRIPT}", stream_callback=log.info)
        log.info("run_task finished OK")
    except Exception as e:
        log.exception("run_task failed: %s", e)


if __name__ == "__main__":
    vn_tz = timezone("Asia/Ho_Chi_Minh")

    job_stores = {
        'default': SQLAlchemyJobStore(url='sqlite:///jobs.sqlite')
    }
    executors = {
        'default': ThreadPoolExecutor(4),
        'processpool': ProcessPoolExecutor(2)
    }
    job_defaults = {
        'coalesce': False,
        'max_instances': 4
    }
    scheduler = BackgroundScheduler(jobstores=job_stores, executors=executors, job_defaults=job_defaults, timezone=vn_tz)

    # At 08:45, 08:50, 08:55 on Mon–Fri
    trigger_early = CronTrigger(
        day_of_week="mon-fri",
        hour="8",
        minute="45,50,55",
        timezone=vn_tz,
    )
    # Every 5 minutes between 09:00 and 13:55 on Mon–Fri
    trigger_midday = CronTrigger(
        day_of_week="mon-fri",
        hour="9-13",
        minute="*/5",
        timezone=vn_tz
    )
    # At 14:00, 14:05, …, 14:45 on Mon–Fri
    trigger_late = CronTrigger(
        day_of_week="mon-fri",
        hour="14",
        minute="0-45/5",
        timezone=vn_tz
    )

    log.info("--ADD JOBS----------------------------------------------")
    scheduler.add_job(run_task, trigger_early, id="job_08_early", replace_existing=True)
    scheduler.add_job(run_task, trigger_midday, id="job_09_13_mid", replace_existing=True)
    scheduler.add_job(run_task, trigger_late, id="job_14_late", replace_existing=True)
    scheduler.start()

    log.info("------------------------------------------------")
    for job in scheduler.get_jobs():
        log.info(f"{job.id}: next run at {job.next_run_time}")

    try:
        while True:
            time.sleep(1)
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()
        log.info("Scheduler shut down gracefully")