from datetime import date
import polars as pl
from datasource.base import DataSource
from utils.timing import timeit_ns


class ParquetSource(DataSource):
    def __init__(self, root_path: str = "./data"):
        self.root = root_path

    # noinspection PyArgumentList
    @timeit_ns
    def load(self, symbol: str, trading_date: date) -> pl.DataFrame:
        path = f"{self.root}/{symbol}/**/stock_date={trading_date:%Y-%m-%d}/*.parquet"
        print("loading path:", path)
        df = pl.read_parquet(
            path,
            allow_missing_columns=True,
            hive_partitioning=True,
        )
        return df.with_columns(pl.col("stock_date").cast(pl.Date)).sort("t")
