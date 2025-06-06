from functools import lru_cache
from datetime import date
import polars as pl
from datasource.base import DataSource


class CachedSource(DataSource):
    def __init__(self, wrapped: DataSource):
        self._wrapped = wrapped

    @lru_cache(maxsize=32)
    def load(self, symbol: str, trading_date: date) -> pl.DataFrame:
        return self._wrapped.load(symbol, trading_date)
