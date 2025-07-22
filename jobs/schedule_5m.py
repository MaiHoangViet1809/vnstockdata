from __future__ import annotations

import time
from pathlib import Path

from pytz import timezone
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.executors.pool import ThreadPoolExecutor, ProcessPoolExecutor
from apscheduler.triggers.cron import CronTrigger

from helper.date_calculate import now
from utils.shells import run_sh
from utils.logging_setup import configure_logging

# ──────────────────────────────────
# 1. Logging (rotates daily)
# ──────────────────────────────────
BASE_PATH = Path(__file__).resolve().parent.parent       # /apps/vnstockdata
log = configure_logging(BASE_PATH)                       # one TimedRotatingFileHandler
# no logging.basicConfig → avoids accidental stdout duplication

# ──────────────────────────────────
# 2. Task wrapper
# ──────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent               # /apps/vnstockdata/jobs
SCRIPT    = BASE_DIR / "tasks" / "update_stock_price_5m.py"


def run_task(run_dttm: str | None = None) -> None:
    """Execute update_stock_price_5m.py and stream its output into the same log."""
    log.info("===> run_task started (%s)", now().isoformat(timespec="seconds"))
    try:
        if not SCRIPT.is_file():
            raise FileNotFoundError(SCRIPT)

        cmd = f"python3.10 {SCRIPT}" + (f" --run_dttm {run_dttm}" if run_dttm else "")
        log.info("start run %s", cmd)

        # Pipe every stdout line from the child process into the main log
        run_sh(command=cmd, stream_callback=lambda line: log.info(line.rstrip()))
        log.info("run_task finished OK")
    except Exception as exc:
        log.exception("run_task failed: %s", exc)


# ──────────────────────────────────
# 3. APScheduler setup
# ──────────────────────────────────
if __name__ == "__main__":
    vn_tz = timezone("Asia/Ho_Chi_Minh")

    scheduler = BackgroundScheduler(
        jobstores={"default": SQLAlchemyJobStore(url="sqlite:///jobs.sqlite")},
        executors={
            "default": ThreadPoolExecutor(4),
            "processpool": ProcessPoolExecutor(2),
        },
        job_defaults={"coalesce": False, "max_instances": 4},
        timezone=vn_tz,
    )

    trigger_early  = CronTrigger(day_of_week="mon-fri", hour="8",  minute="45,50,55",  timezone=vn_tz)
    trigger_midday = CronTrigger(day_of_week="mon-fri", hour="9-13", minute="*/5",     timezone=vn_tz)
    trigger_late   = CronTrigger(day_of_week="mon-fri", hour="14", minute="0-45/5",    timezone=vn_tz)

    log.info("--ADD JOBS----------------------------------------------")
    scheduler.add_job(run_task, trigger_early,  id="job_08_early",  replace_existing=True)
    scheduler.add_job(run_task, trigger_midday, id="job_09_13_mid", replace_existing=True)
    scheduler.add_job(run_task, trigger_late,   id="job_14_late",   replace_existing=True)
    scheduler.start()

    log.info("------------------------------------------------")
    for job in scheduler.get_jobs():
        log.info("%s: next run at %s", job.id, job.next_run_time)

    try:
        while True:
            time.sleep(1)
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()
        log.info("Scheduler shut down gracefully")
