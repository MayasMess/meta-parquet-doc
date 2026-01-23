"""Adapter for PyArrow Tables."""

from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq


class PyArrowAdapter:
    """Read/write Parquet using PyArrow. Supports partitioned datasets."""

    def get_column_names(self, table: pa.Table) -> list[str]:
        return table.schema.names

    def write_parquet(self, table: pa.Table, path: Path, **kwargs: Any) -> None:
        partition_cols = kwargs.pop("partition_cols", None)
        if partition_cols:
            # Partitioned dataset: write to a directory
            pq.write_to_dataset(
                table, root_path=str(path), partition_cols=partition_cols, **kwargs
            )
        else:
            # Single file
            Path(path).parent.mkdir(parents=True, exist_ok=True)
            pq.write_table(table, str(path), **kwargs)

    def read_parquet(self, path: Path, **kwargs: Any) -> pa.Table:
        return pq.read_table(str(path), **kwargs)
