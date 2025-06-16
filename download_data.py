from helper.date_calculate import third_thursday
from time import sleep
from datetime import datetime
from dateutil.relativedelta import relativedelta
from rest_api_interface import save_historical_data
from utils.timing import timeit_ns
from logging import Logger

logger = Logger(name="download_data")


class DownloadStock:
    def __init__(self, symbol: str, from_date_yyyymmdd: str, to_date_yyyymmdd: str, interval: str = relativedelta(days=1), dry_run = False):
        self.symbol = symbol
        self.start_date = datetime.strptime(from_date_yyyymmdd, "%Y%m%d")
        self.end_date = datetime.strptime(to_date_yyyymmdd, "%Y%m%d")
        self.interval = interval
        self.dry_run = dry_run

    def _get_symbol(self, **kwargs):
        return self.symbol

    @timeit_ns # noqa
    def download(self, from_date_yyyymmdd: str = None, to_date_yyyymmdd: str = None):
        if from_date_yyyymmdd:
            self.start_date = datetime.strptime(from_date_yyyymmdd, "%Y%m%d")

        if to_date_yyyymmdd:
            self.end_date = datetime.strptime(to_date_yyyymmdd, "%Y%m%d")

        curr_date = self.start_date
        while curr_date <= self.end_date:
            logger.info("-", curr_date.strftime("%Y-%m"), "-" * 10)
            self.api_call(symbol=self._get_symbol(), curr_date=curr_date, base_path="data")
            curr_date += self.interval

    def api_call(self, symbol: str, curr_date: datetime, **kwargs):
        logger.info("--", curr_date.strftime("%Y-%m-%d"), "-" * 10)
        save_historical_data(
            symbol=symbol,
            dt_from=curr_date,
            dt_to=curr_date + relativedelta(hours=23),
            dry_run=self.dry_run,
            **kwargs
        )
        sleep(0.001)


class DownloadVN30F(DownloadStock):
    def __init__(self, from_date_yyyymmdd: str, to_date_yyyymmdd: str, interval: str = relativedelta(months=1), symbol="VN30F", dry_run = False):
        super().__init__(symbol=symbol,
                         from_date_yyyymmdd=from_date_yyyymmdd,
                         to_date_yyyymmdd=to_date_yyyymmdd,
                         interval=interval,
                         dry_run=dry_run,
                         )

    def _get_symbol(self, **kwargs):
        curr_date = kwargs.get("curr_date")
        name_part = curr_date.strftime("%y%m")
        return f"VN30F{name_part}"

    @timeit_ns # noqa
    def download(self, from_date_yyyymmdd: str = None, to_date_yyyymmdd: str = None):
        if from_date_yyyymmdd:
            self.start_date = datetime.strptime(from_date_yyyymmdd, "%Y%m%d")

        if to_date_yyyymmdd:
            self.end_date = datetime.strptime(to_date_yyyymmdd, "%Y%m%d")

        curr_month = self.start_date
        while curr_month <= self.end_date:
            logger.info("-", curr_month.strftime("%Y-%m"), "-" * 10)

            prev_month = curr_month + relativedelta(months=-1)

            start_date = third_thursday(prev_month.year, prev_month.month) + relativedelta(days=1)
            end_date = third_thursday(curr_month.year, curr_month.month) + relativedelta(days=0)
            curr_date = max(start_date, self.start_date)

            while curr_date <= end_date and curr_date <= self.end_date:
                if curr_date.weekday() < 5:
                    self.api_call(symbol=self._get_symbol(curr_date=curr_month), curr_date=curr_date, base_path="data/VN30F")

                curr_date += relativedelta(days=1)

            curr_month += self.interval


class DownloadStockFactory:
    def __init__(self, symbol: str, dry_run = False, **kwargs):
        match symbol:
            case "VN30F":
                self.engine = DownloadVN30F(**kwargs, dry_run=dry_run)
            case _:
                self.engine = DownloadStock(symbol=symbol, dry_run=dry_run, **kwargs)

    def download(self, **kwargs):
        self.engine.download()


if __name__ == "__main__":
    download_tool = DownloadStockFactory(symbol="VN30F", from_date_yyyymmdd="20250605", to_date_yyyymmdd="20250606", dry_run = True)
    download_tool.download()

