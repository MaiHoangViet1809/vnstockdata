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


src_VN30F = DownloadStockFactory(symbol="VN30F", from_date_yyyymmdd="20250601", to_date_yyyymmdd="20250601", dry_run=True)
src_VN30 = DownloadStockFactory(symbol="VN30", from_date_yyyymmdd="20250601", to_date_yyyymmdd="20250601", dry_run=True)
git_helper = GitPusher(repo_path=PROJECT_HOME)


@click.command()
@click.option("--run_dttm", default=None, help="run date (default now)")
@timeit_ns
def get_data_today(run_dttm: str = None):
    git_helper.pull()
    run_dttm = run_dttm or now().strftime("%Y%m%d")
    src_VN30F.download(from_date_yyyymmdd=run_dttm, to_date_yyyymmdd=run_dttm)
    src_VN30.download(from_date_yyyymmdd=run_dttm, to_date_yyyymmdd=run_dttm)
    git_helper.push()


if __name__ == "__main__":
    get_data_today()
