"""Adapter registry and auto-detection for DataFrame types."""

from typing import Any, Protocol, runtime_checkable
from pathlib import Path


@runtime_checkable
class DataFrameAdapter(Protocol):
    """Protocol that all DataFrame adapters must satisfy."""

    def get_column_names(self, df: Any) -> list[str]: ...
    def write_parquet(self, df: Any, path: Path, **kwargs: Any) -> None: ...
    def read_parquet(self, path: Path, **kwargs: Any) -> Any: ...


def detect_adapter(df: Any) -> DataFrameAdapter:
    """
    Auto-detect which adapter to use based on the runtime type of df.
    Checks pandas, pyarrow, then pyspark (if available).
    """
    import pandas as pd
    import pyarrow as pa

    from meta_parquet_doc._adapters._pandas import PandasAdapter
    from meta_parquet_doc._adapters._pyarrow import PyArrowAdapter
    from meta_parquet_doc.exceptions import UnsupportedDataFrameError

    if isinstance(df, pd.DataFrame):
        return PandasAdapter()
    if isinstance(df, pa.Table):
        return PyArrowAdapter()

    # Try PySpark (optional dependency)
    try:
        from pyspark.sql import DataFrame as SparkDataFrame

        if isinstance(df, SparkDataFrame):
            from meta_parquet_doc._adapters._pyspark import PySparkAdapter
            return PySparkAdapter()
    except ImportError:
        pass

    raise UnsupportedDataFrameError(
        f"Unsupported DataFrame type: {type(df).__name__}. "
        "Supported: pandas.DataFrame, pyarrow.Table, pyspark.sql.DataFrame."
    )


def get_adapter_by_engine(engine: str) -> DataFrameAdapter:
    """Get an adapter instance by engine name."""
    from meta_parquet_doc._adapters._pandas import PandasAdapter
    from meta_parquet_doc._adapters._pyarrow import PyArrowAdapter
    from meta_parquet_doc.exceptions import UnsupportedDataFrameError

    if engine == "pandas":
        return PandasAdapter()
    elif engine == "pyarrow":
        return PyArrowAdapter()
    elif engine == "pyspark":
        try:
            from meta_parquet_doc._adapters._pyspark import PySparkAdapter
            return PySparkAdapter()
        except ImportError as e:
            raise UnsupportedDataFrameError(
                "PySpark is not installed. Install with: pip install meta-parquet-doc[spark]"
            ) from e
    else:
        raise UnsupportedDataFrameError(
            f"Unknown engine: '{engine}'. Supported: 'pandas', 'pyarrow', 'pyspark'."
        )
