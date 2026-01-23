"""Adapter for PySpark DataFrames. Requires optional pyspark dependency."""

from pathlib import Path
from typing import Any


class PySparkAdapter:
    """Read/write Parquet using PySpark."""

    def get_column_names(self, df: Any) -> list[str]:
        return df.columns

    def write_parquet(self, df: Any, path: Path, **kwargs: Any) -> None:
        write_mode = kwargs.pop("spark_mode", "overwrite")
        partition_cols = kwargs.pop("partition_cols", None)
        writer = df.write.mode(write_mode)
        if partition_cols:
            writer = writer.partitionBy(*partition_cols)
        writer.parquet(str(path))

    def read_parquet(self, path: Path, **kwargs: Any) -> Any:
        from pyspark.sql import SparkSession

        spark = kwargs.pop("spark", None) or SparkSession.builder.getOrCreate()
        return spark.read.parquet(str(path))
