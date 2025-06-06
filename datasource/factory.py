from datasource.file_source import ParquetSource
from datasource.cache_source import CachedSource
from datasource.base import DataSource


def datasource_builder(
    root_path: str = "../data",
    use_cache: bool = True,
) -> DataSource:
    """Instantiate a DataSource (Parquet + optional caching)."""
    ds: DataSource = ParquetSource(root_path)
    if use_cache:
        ds = CachedSource(ds)
    return ds


if __name__ == "__main__":
    from datetime import date
    import polars as pl

    ds = datasource_builder(use_cache=True)
    df = ds.load(symbol="VN30", trading_date=date(2025, 5, 8))
    with pl.Config(tbl_cols=100, tbl_rows=10):
        print(df.head(10))
