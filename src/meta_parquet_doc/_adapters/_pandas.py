"""Adapter for pandas DataFrames."""

from pathlib import Path
from typing import Any

import pandas as pd


class PandasAdapter:
    """Read/write Parquet using pandas."""

    def get_column_names(self, df: pd.DataFrame) -> list[str]:
        return list(df.columns)

    def write_parquet(self, df: pd.DataFrame, path: Path, **kwargs: Any) -> None:
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(path, index=False, **kwargs)

    def read_parquet(self, path: Path, **kwargs: Any) -> pd.DataFrame:
        return pd.read_parquet(path, **kwargs)
