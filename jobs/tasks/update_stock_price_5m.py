from download_data import DownloadStockFactory
from helper.update_git import GitPusher
from utils.timing import timeit_ns
from dotenv import load_dotenv
from helper.date_calculate import now
from os import getenv
from utils.env_info import get_platform
from pathlib import Path
import click

load_dotenv()
PROJECT_HOME = Path(__file__).resolve().parent.parent.as_posix() if get_platform() == "mac" else getenv("PROJECT_HOME")


src_VN30F = DownloadStockFactory(symbol="VN30F", from_date_yyyymmdd="20250601", to_date_yyyymmdd="20250601", dry_run=False)
src_VN30 = DownloadStockFactory(symbol="VN30", from_date_yyyymmdd="20250601", to_date_yyyymmdd="20250601", dry_run=False)
git_helper = GitPusher(repo_path=PROJECT_HOME)


@timeit_ns
def get_data_today(run_dttm: str = None, dry_run = False):
    run_dttm = run_dttm or now().strftime("%Y%m%d")

    if not dry_run:
        git_helper.pull()

    if dry_run:
        src_VN30F.engine.dry_run = True
        src_VN30.engine.dry_run = True

    src_VN30F.download(from_date_yyyymmdd=run_dttm, to_date_yyyymmdd=run_dttm)
    src_VN30.download(from_date_yyyymmdd=run_dttm, to_date_yyyymmdd=run_dttm)

    if not dry_run:
        git_helper.push()


@click.command()
@click.option("--run_dttm", default=None, help="run date (default now)")
def production(run_dttm: str = None):
    get_data_today(run_dttm=run_dttm)


if __name__ == "__main__":
    production()

    # from datetime import datetime
    # from dateutil.relativedelta import relativedelta
    #
    # def now_last_workday(ts: datetime):
    #     while ts.weekday() >= 5 or (ts.weekday() == 0 and ts.hour < 9):  # Sat/Sun roll back
    #         ts -= relativedelta(days=1)
    #     return ts
    #
    # WD = now_last_workday(now())
    # NOW = WD + relativedelta(days=-1 if WD.hour <= 7 else 0) + relativedelta(hours=15 if WD.hour <= 7 else 0)
    # get_data_today(dry_run=True, run_dttm=NOW.strftime("%Y%m%d"))
