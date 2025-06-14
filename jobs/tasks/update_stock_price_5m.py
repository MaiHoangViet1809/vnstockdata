from datetime import datetime
from download_data import DownloadStockFactory
from helper.update_git import GitPusher
from utils.timing import timeit_ns
from dotenv import load_dotenv
from os import getenv

load_dotenv()
PROJECT_HOME = getenv("PROJECT_HOME")


src_VN30F = DownloadStockFactory(symbol="VN30F", from_date_yyyymmdd="20250601", to_date_yyyymmdd="20250601", dry_run=True)
src_VN30 = DownloadStockFactory(symbol="VN30", from_date_yyyymmdd="20250601", to_date_yyyymmdd="20250601", dry_run=True)
git_helper = GitPusher(repo_path=PROJECT_HOME)


@timeit_ns
def get_data_today():
    git_helper.pull()
    run_dttm = datetime.now().strftime("%Y%m%d")
    src_VN30F.download(from_date_yyyymmdd=run_dttm, to_date_yyyymmdd=run_dttm)
    src_VN30.download(from_date_yyyymmdd=run_dttm, to_date_yyyymmdd=run_dttm)
    git_helper.push()


if __name__ == "__main__":
    get_data_today()