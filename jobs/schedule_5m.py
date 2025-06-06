from pytz import timezone
import time
from pathlib import Path

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.executors.pool import ThreadPoolExecutor, ProcessPoolExecutor
from apscheduler.triggers.cron import CronTrigger

from utils.shells import run_sh


def run_task():
    try:
        base_dir = Path(__file__).resolve().parent
        script = base_dir / "tasks" / "update_stock_price_5m.py"
        run_sh(command=f"python3.10 {script.as_posix()}")
    except Exception as e:
        print("run_task failed: %s", e)


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

    scheduler.add_job(run_task, trigger_early, id="job_08_early")
    scheduler.add_job(run_task, trigger_midday, id="job_09_13_mid")
    scheduler.add_job(run_task, trigger_late, id="job_14_late")
    scheduler.start()

    try:
        while True:
            time.sleep(1)
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()
        print("Scheduler shut down gracefully")