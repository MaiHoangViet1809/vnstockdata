from datetime import datetime
from pytz import timezone
import time

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.executors.pool import ThreadPoolExecutor, ProcessPoolExecutor
from apscheduler.triggers.cron import CronTrigger

from download_data import DownloadStockFactory
from helper.update_git import GitPusher


src_VN30F = DownloadStockFactory(symbol="VN30F", from_date_yyyymmdd="20250601", to_date_yyyymmdd="20250601", dry_run=True)
src_VN30 = DownloadStockFactory(symbol="VN30", from_date_yyyymmdd="20250601", to_date_yyyymmdd="20250601", dry_run=True)
git_helper = GitPusher(repo_path="..")


def get_data_today():
    git_helper.pull()
    run_dttm = datetime.now().strftime("%Y%m%d")
    src_VN30F.download(from_date_yyyymmdd=run_dttm, to_date_yyyymmdd=run_dttm)
    src_VN30.download(from_date_yyyymmdd=run_dttm, to_date_yyyymmdd=run_dttm)
    git_helper.push()


if __name__ == "__main__":
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
    scheduler = BackgroundScheduler(jobstores=job_stores, executors=executors, job_defaults=job_defaults, timezone=timezone("Asia/Ho_Chi_Minh"))

    # At 08:45, 08:50, 08:55 on Mon–Fri
    trigger_early = CronTrigger(
        day_of_week="mon-fri",
        hour="8",
        minute="45,50,55",
        timezone="Asia/Ho_Chi_Minh"
    )
    # Every 5 minutes between 09:00 and 13:55 on Mon–Fri
    trigger_midday = CronTrigger(
        day_of_week="mon-fri",
        hour="9-13",
        minute="*/5",
        timezone="Asia/Ho_Chi_Minh"
    )
    # At 14:00, 14:05, …, 14:45 on Mon–Fri
    trigger_late = CronTrigger(
        day_of_week="mon-fri",
        hour="14",
        minute="0-45/5",
        timezone="Asia/Ho_Chi_Minh"
    )

    scheduler.add_job(get_data_today, trigger_early, id="job_08_early")
    scheduler.add_job(get_data_today, trigger_midday, id="job_09_13_mid")
    scheduler.add_job(get_data_today, trigger_late, id="job_14_late")
    scheduler.start()

    try:
        while True:
            time.sleep(1)
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()
        print("Scheduler shut down gracefully")