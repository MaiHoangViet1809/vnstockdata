from helper.date_calculate import third_thursday
from time import sleep
from datetime import datetime
from dateutil.relativedelta import relativedelta
from rest_api_interface import save_historical_data
from utils.timing import timeit_ns
import logging

logger = logging.getLogger(__name__)


class DownloadStock:
    def __init__(self, symbol: str, from_date_yyyymmdd: str, to_date_yyyymmdd: str, interval = relativedelta(days=1), dry_run = False):
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
            self.api_call(symbol=self._get_symbol(), curr_date=curr_date, base_path="data")
            curr_date += self.interval

    def api_call(self, symbol: str, curr_date: datetime, **kwargs):
        logger.info("--" + symbol + " " + curr_date.strftime("%Y-%m-%d") + "-" * 10)
        save_historical_data(
            symbol=symbol,
            dt_from=curr_date,
            dt_to=curr_date + relativedelta(hours=23),
            dry_run=self.dry_run,
            **kwargs
        )
        sleep(0.001)


class DownloadVN30F(DownloadStock):
    def __init__(self, from_date_yyyymmdd: str, to_date_yyyymmdd: str, symbol="VN30F", dry_run = False):
        super().__init__(symbol=symbol,
                         from_date_yyyymmdd=from_date_yyyymmdd,
                         to_date_yyyymmdd=to_date_yyyymmdd,
                         dry_run=dry_run,
                         )

    def _get_symbol(self, **kwargs):
        from helper.date_calculate import krx_vn30f_code
        curr_date: datetime = kwargs.get("curr_date")
        return krx_vn30f_code(year=curr_date.year, month=curr_date.month)

    @timeit_ns # noqa
    def download(self, from_date_yyyymmdd: str = None, to_date_yyyymmdd: str = None):
        if from_date_yyyymmdd:
            self.start_date = datetime.strptime(from_date_yyyymmdd, "%Y%m%d")

        if to_date_yyyymmdd:
            self.end_date = datetime.strptime(to_date_yyyymmdd, "%Y%m%d")

        current_month = self.get_current_month(self.start_date)
        date_range_to_run = self.get_date_range_current_month(current_month_yyyymm=current_month)
        for date_to_run in date_range_to_run:
            self.api_call(symbol=self._get_symbol(curr_date=datetime.strptime(current_month, "%Y%m")), curr_date=date_to_run, base_path="data/VN30F")

    @staticmethod
    def datetime_range(start: datetime,
                       end: datetime,
                       ) -> list[datetime]:
        """Daily datetimes between start and end (inclusive), forwards or backwards."""
        days = (end.date() - start.date()).days
        step = 1 if days >= 0 else -1
        return [start + relativedelta(days=step * i) for i in range(abs(days) + 1) if (start + relativedelta(days=step * i)).weekday() < 5]

    @staticmethod
    def get_current_month(run_date: datetime):
        date_end_vn30_contract = third_thursday(run_date.year, run_date.month)
        run_month = (run_date + relativedelta(months=1 if run_date > date_end_vn30_contract else 0)).strftime("%Y%m")
        return run_month

    def get_date_range_current_month(self, current_month_yyyymm: str):
        current_month = datetime.strptime(current_month_yyyymm, "%Y%m")
        prev_month = current_month + relativedelta(months=-1)

        start_date = third_thursday(prev_month.year, prev_month.month) + relativedelta(days=1)
        end_date = min(third_thursday(current_month.year, current_month.month) + relativedelta(days=0), self.end_date)

        return self.datetime_range(start=start_date, end=end_date)


class DownloadStockFactory:
    def __init__(self, symbol: str, dry_run = False, **kwargs):
        match symbol:
            case "VN30F":
                self.engine = DownloadVN30F(**kwargs, dry_run=dry_run)
            case _:
                self.engine = DownloadStock(symbol=symbol, dry_run=dry_run, **kwargs)

    def download(self, **kwargs):
        self.engine.download(**kwargs)


if __name__ == "__main__":
    import logging
    # Create a handler that outputs to the console (stdout)
    console_handler = logging.StreamHandler()

    # Create a formatter
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(formatter)

    # Add the handler to the logger
    logger.addHandler(console_handler)
    download_tool = DownloadStockFactory(symbol="VN30F", from_date_yyyymmdd="20250605", to_date_yyyymmdd="20250606", dry_run = True)
    download_tool.download()

