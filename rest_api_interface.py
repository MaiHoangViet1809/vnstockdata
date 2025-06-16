from abc import ABC, abstractmethod
from datetime import datetime
import httpx
import pandas as pd
import numpy as np
import logging
import polars as pl

from utils.debug import print_table
from utils.timing import timeit_ns
from utils.shells import run_sh
from helper.agent import get_headers
from helper.date_calculate import now

logger = logging.getLogger(__name__)


class StockMixin:
    @staticmethod
    def request_data(
        url: str,
        payload: dict,
        timeout: int = 300
    ) -> dict:
        """
        POST to `url` with `payload`, return parsed JSON on HTTP 200.
        Raises on other statuses.
        """
        headers = get_headers(data_source="VCI", random_agent=False)
        with httpx.Client(timeout=timeout, follow_redirects=True, headers=headers) as client:
            resp = client.post(url, json=payload, headers=headers)
            resp.raise_for_status()
            return resp.json()

    @staticmethod
    def transform_json(
        raw: dict | list[dict],
        tz: str = "Asia/Ho_Chi_Minh"
    ) -> pd.DataFrame:
        """
        Convert raw dict or list-of-dicts into a single DataFrame,
        converting any time fields (containing 'time' or 't') to the given tz.
        """
        items = raw if isinstance(raw, list) else [raw]
        frames = []
        for obj in items:
            length = len(next(v for v in obj.values() if isinstance(v, list)))
            df = pd.DataFrame({
                k: (np.repeat(v, length) if isinstance(v, str) else v)
                for k, v in obj.items()
            })
            for col in df.columns:
                if "time" in col.lower() or col == "t":
                    df[col] = pd.to_datetime(df[col].astype(int), unit="s", utc=True).dt.tz_convert(tz)
            frames.append(df)
        return pd.concat(frames, ignore_index=True)


class DataFetcher(ABC, StockMixin):
    """
    Template Method:
     1) build_payload
     2) call request_data
     3) call transform_json
     4) post_process hook
    """

    def fetch(self) -> pd.DataFrame | None:
        payload = self.build_payload()
        print(f"Payload: {payload}")
        raw = self.request_data(self.endpoint_url(), payload)
        if raw:
            df = self.transform_json(raw)
            return self.post_process(df)
        return None

    @staticmethod
    def post_process(df: pd.DataFrame) -> pd.DataFrame:
        """Optional hook for final tweaks; default is identity."""
        return df

    @abstractmethod
    def endpoint_url(self) -> str:
        ...

    @abstractmethod
    def build_payload(self) -> dict:
        ...


# Concrete fetchers:

class CandleFetcher(DataFetcher):
    ENDPOINT = "https://trading.vietcap.com.vn/api/chart/OHLCChart/gap"

    def __init__(self, symbol: str,
                 dt_from: datetime, dt_to: datetime):
        self.symbol = symbol
        self.dt_from = dt_from
        self.dt_to = dt_to

    def endpoint_url(self) -> str:
        return self.ENDPOINT

    def build_payload(self) -> dict:
        return {
            "timeFrame": "ONE_MINUTE",
            "symbols": [self.symbol],
            "from": int(self.dt_from.timestamp()),
            "to":   int(self.dt_to.timestamp()),
        }


class MatchingFetcher(DataFetcher):
    ENDPOINT = "https://trading.vietcap.com.vn/api/market-watch/LEData/getAll"

    def __init__(self, symbol: str, limit: int = 10_000, truncTime: int = None):
        self.symbol = symbol
        self.limit = limit
        self.truncTime = truncTime

    def endpoint_url(self) -> str:
        return self.ENDPOINT

    def build_payload(self) -> dict:
        payload = {"symbol": self.symbol, "limit": self.limit}
        if self.truncTime is not None:
            payload["truncTime"] = self.truncTime
        return payload


class StepFetcher(DataFetcher):
    ENDPOINT = "https://trading.vietcap.com.vn/api/market-watch/AccumulatedPriceStepVol/getSymbolData"

    def __init__(self, symbol: str, **kwargs):
        self.symbol = symbol
        self.kwargs = kwargs

    def endpoint_url(self) -> str:
        return self.ENDPOINT

    def build_payload(self) -> dict:
        payload = dict(symbol=self.symbol, **self.kwargs)
        return payload


# High-level service:
class StockService:
    @staticmethod
    def get_candle(
        symbol: str,
        dt_from: datetime, dt_to: datetime
    ) -> pd.DataFrame:
        return CandleFetcher(symbol, dt_from, dt_to).fetch()

    @staticmethod
    def get_matching(
        symbol: str,
        limit: int = 10_000, truncTime: int = None
    ) -> pd.DataFrame:
        return MatchingFetcher(symbol, limit=limit, truncTime=truncTime).fetch()

    @staticmethod
    def get_step(symbol: str, **kwargs) -> pd.DataFrame:
        return StepFetcher(symbol, **kwargs).fetch()

# # list bid/ask
# data = StockMixin.request_data(url="https://trading.vietcap.com.vn/api/price/symbols/getList", payload=dict(symbols=['VN30F2505']))
# print_table(pd.DataFrame(data[0]), 1000)


@timeit_ns
def save_historical_data(symbol: str, base_path: str = "./data", stock_service: StockService = None, dry_run = False, **kwargs):
    if not stock_service:
        stock_service = StockService()

    df_candle = stock_service.get_candle(symbol=symbol, **kwargs)
    if df_candle is not None:
        df_candle["stock_date"] = df_candle["t"].dt.date
        df_candle["snapshot_dttm"] = now()

        list_date = df_candle["stock_date"].unique().tolist()
        for d in list_date:
            logger.info("-" * 20 + "START for " + d + " -" * 20)
            df_out = df_candle[df_candle["stock_date"] == d]

            if df_out.size > 0:
                print_table(df_out, 3, print_callback=logger.info)
                output_path = f"{base_path}/{symbol}"

                if not dry_run:
                    run_sh(f"rm -rf {output_path}/stock_date={d.strftime('%Y-%m-%d')}")
                    df_write = pl.from_pandas(df_out)
                    df_write.write_parquet(file=output_path, partition_by=["stock_date"])
                else:
                    logger.info(f"[DRY RUN] rm -rf {output_path}/stock_date={d.strftime('%Y-%m-%d')}")
                    logger.info(f"[DRU RUN] saving data to {output_path=}")

                logger.info("-" * 20 + "FINISH " + d + " -" * 20)

#
# # Usage:
# if __name__ == "__main__":
#     from dateutil.relativedelta import relativedelta
#     from helper.date_calculate import third_thursday
#     from time import sleep
#
#     start_month = datetime(2025, 6, 1)
#     end_month = datetime(2025, 6, 6, hour=15)
#     curr_month = start_month
#
#     while curr_month <= end_month:
#         name_part = curr_month.strftime("%y%m")
#         symbol = f"VN30F{name_part}"
#
#         print("-", curr_month.strftime("%Y-%m"), "-" * 10)
#         prev_month = curr_month + relativedelta(months=-1)
#
#         start_date = third_thursday(prev_month.year, prev_month.month) + relativedelta(days=1)
#         end_date = third_thursday(curr_month.year, curr_month.month) + relativedelta(days=0)
#         curr_date = start_date
#
#         while curr_date <= end_date:
#             if curr_date.weekday() < 5:
#                 print("--", curr_date.strftime("%Y-%m-%d"), "-" * 10)
#                 save_historical_data(
#                     symbol=symbol,
#                     dt_from=curr_date,
#                     dt_to=curr_date + relativedelta(hours=23),
#                     dry_run=True,
#                 )
#                 sleep(0.001)
#
#             curr_date += relativedelta(days=1)
#
#         # try new month
#         curr_month += relativedelta(months=1)
#
#     # 1739898000 -- 1739898000
#     # 1742922000 -- 1742835600
#     # Payload: {'timeFrame': 'ONE_MINUTE', 'symbols': ['VN30F2505'], 'from': 1739898000, 'to': 1740762000}
#     # Payload: {'timeFrame': 'ONE_MINUTE', 'symbols': ['VN30F2503'], 'from': 1739898000, 'to': 1742922000}

