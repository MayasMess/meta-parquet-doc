"""
meta-parquet-doc: Governance and documentation for Parquet datasets.

Provides read/write functions that ensure every Parquet dataset is accompanied
by a _metadata.json file documenting columns, ownership, and PII classification.
"""

from meta_parquet_doc._types import (
    ColumnMetadata,
    DatasetMetadata,
    ParquetDocMetadata,
)
from meta_parquet_doc._validation import ValidationResult
from meta_parquet_doc.api import (
    init_metadata,
    read_dataset,
    validate_dataset,
    write_dataset,
)
from meta_parquet_doc.exceptions import (
    MetaParquetDocError,
    MetadataFileNotFoundError,
    MetadataValidationError,
    UnsupportedDataFrameError,
)

__version__ = "0.1.0"
__all__ = [
    "write_dataset",
    "read_dataset",
    "validate_dataset",
    "init_metadata",
    "ColumnMetadata",
    "DatasetMetadata",
    "ParquetDocMetadata",
    "ValidationResult",
    "MetaParquetDocError",
    "MetadataValidationError",
    "MetadataFileNotFoundError",
    "UnsupportedDataFrameError",
    "__version__",
]


def main() -> None:
    """CLI entry point."""
    print(f"meta-parquet-doc v{__version__}")
