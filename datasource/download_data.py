from helper.date_calculate import third_thursday
from time import sleep
from datetime import datetime
from dateutil.relativedelta import relativedelta
from datasource.source_data import save_historical_data
from utils.timing import timeit_ns


class DownloadStock:
    def __init__(self, symbol: str, from_date_yyyymmdd: str, to_date_yyyymmdd: str, interval: str = relativedelta(days=1)):
        self.symbol = symbol
        self.start_date = datetime.strptime(from_date_yyyymmdd, "%Y%m%d")
        self.end_date = datetime.strptime(to_date_yyyymmdd, "%Y%m%d")
        self.interval = interval

    def _get_symbol(self, **kwargs):
        return self.symbol

    @timeit_ns # noqa
    def download(self):
        curr_date = self.start_date
        while curr_date <= self.end_date:
            print("-", curr_date.strftime("%Y-%m"), "-" * 10)
            self.api_call(symbol=self._get_symbol(), curr_date=curr_date)
            curr_date += self.interval

    @staticmethod
    def api_call(symbol: str, curr_date: datetime, **kwargs):
        print("--", curr_date.strftime("%Y-%m-%d"), "-" * 10)
        save_historical_data(
            symbol=symbol,
            dt_from=curr_date,
            dt_to=curr_date + relativedelta(hours=23),
            base_path="../data",
            **kwargs
        )
        sleep(0.001)


class DownloadVN30F(DownloadStock):
    def __init__(self, from_date_yyyymmdd: str, to_date_yyyymmdd: str, interval: str = relativedelta(months=1), symbol="VN30F"):
        super().__init__(symbol=symbol,
                         from_date_yyyymmdd=from_date_yyyymmdd,
                         to_date_yyyymmdd=to_date_yyyymmdd,
                         interval=interval)

    def _get_symbol(self, **kwargs):
        curr_date = kwargs.get("curr_date")
        name_part = curr_date.strftime("%y%m")
        return f"VN30F{name_part}"

    @timeit_ns # noqa
    def download(self):
        curr_month = self.start_date
        while curr_month <= self.end_date:
            print("-", curr_month.strftime("%Y-%m"), "-" * 10)

            prev_month = curr_month + relativedelta(months=-1)
            start_date = third_thursday(prev_month.year, prev_month.month) + relativedelta(days=1)
            end_date = third_thursday(curr_month.year, curr_month.month) + relativedelta(days=0)
            curr_date = start_date
            while curr_date <= end_date:
                if curr_date.weekday() < 5:
                    self.api_call(symbol=self._get_symbol(curr_date=curr_date), curr_date=curr_month, base_path="../data/VN30F")

                curr_date += relativedelta(days=1)

            curr_month += self.interval


if __name__ == "__main__":
    download_tool = DownloadStock(symbol="VN30", from_date_yyyymmdd="20240101", to_date_yyyymmdd="20250509")
    download_tool.download()
