from abc import ABC, abstractmethod
from datetime import date
import polars as pl


class DataSource(ABC):
    @abstractmethod
    def load(self, symbol: str, trading_date: date) -> pl.DataFrame:
        """Return a Polars DataFrame for the given symbol on the given date."""
