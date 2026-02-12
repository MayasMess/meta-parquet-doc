"""Public API: write_dataset, read_dataset, validate_dataset, init_metadata."""

import logging
from pathlib import Path
from typing import Any, Literal

import pyarrow.parquet as pq

from meta_parquet_doc._adapters import detect_adapter, get_adapter_by_engine
from meta_parquet_doc._metadata_io import (
    read_metadata_file,
    resolve_metadata_path,
    write_metadata_file,
)
from meta_parquet_doc._schema import validate_metadata_structure
from meta_parquet_doc._types import (
    ColumnMetadata,
    DatasetMetadata,
    ParquetDocMetadata,
)
from meta_parquet_doc._validation import (
    ValidationResult,
    validate_columns_coverage,
    validate_mandatory_fields,
)
from meta_parquet_doc.exceptions import MetadataValidationError

logger = logging.getLogger(__name__)


def write_dataset(
    df: Any,
    path: str | Path,
    *,
    metadata_path: str | Path = "_metadata.json",
    dataset_metadata: dict[str, Any] | None = None,
    columns_metadata: dict[str, dict[str, Any]] | None = None,
    mode: Literal["warn", "strict"] = "warn",
    **parquet_kwargs: Any,
) -> Path:
    """
    Write a DataFrame to Parquet and create/update the _metadata.json file.

    Parameters
    ----------
    df : pandas.DataFrame | pyarrow.Table | pyspark.sql.DataFrame
        The data to write.
    path : str or Path
        Destination path for the Parquet file or directory.
    metadata_path : str or Path
        Path for the _metadata.json file (relative to data or absolute).
    dataset_metadata : dict, optional
        Keys: description (required), owner, tags.
    columns_metadata : dict, optional
        Mapping of column_name -> {description, nullable, pii, pii_category}.
    mode : "warn" | "strict"
        Validation mode. "strict" raises on issues, "warn" emits warnings.
    **parquet_kwargs
        Extra arguments passed to the Parquet writer (e.g., partition_cols, compression).

    Returns
    -------
    Path
        The resolved path where _metadata.json was written.
    """
    path = Path(path)
    adapter = detect_adapter(df)
    actual_columns = adapter.get_column_names(df)

    # Build or load metadata
    if columns_metadata is not None:
        # Validate the provided metadata
        validate_mandatory_fields(columns_metadata, mode=mode)
        validate_columns_coverage(actual_columns, columns_metadata, mode=mode)
        columns = {
            name: ColumnMetadata.from_dict(col_data)
            for name, col_data in columns_metadata.items()
        }
    else:
        # Try reading existing metadata
        resolved = resolve_metadata_path(path, metadata_path)
        if resolved.exists():
            raw = read_metadata_file(resolved)
            existing = ParquetDocMetadata.from_dict(raw)
            columns = existing.columns
            validate_columns_coverage(
                actual_columns, {k: v.to_dict() for k, v in columns.items()}, mode=mode
            )
        else:
            # No metadata provided and no file exists: create skeleton
            columns = {
                col: ColumnMetadata(description="", nullable=True, pii=False)
                for col in actual_columns
            }
            if mode == "strict":
                raise MetadataValidationError(
                    [f"No columns_metadata provided and no existing _metadata.json at {resolved}."]
                )

    # Build dataset-level metadata
    if dataset_metadata is not None:
        ds_meta = DatasetMetadata.from_dict(dataset_metadata)
    else:
        ds_meta = DatasetMetadata(description="")

    metadata = ParquetDocMetadata(dataset=ds_meta, columns=columns)

    # Write parquet data
    adapter.write_parquet(df, path, **parquet_kwargs)

    # Write metadata file
    resolved = resolve_metadata_path(path, metadata_path)
    write_metadata_file(resolved, metadata)

    return resolved


def read_dataset(
    path: str | Path,
    *,
    metadata_path: str | Path = "_metadata.json",
    mode: Literal["warn", "strict"] = "warn",
    engine: Literal["pandas", "pyarrow", "pyspark"] = "pandas",
    **parquet_kwargs: Any,
) -> tuple[Any, ParquetDocMetadata]:
    """
    Read a Parquet dataset and its accompanying _metadata.json.

    Parameters
    ----------
    path : str or Path
        Path to Parquet file or directory.
    metadata_path : str or Path
        Path to the _metadata.json file.
    mode : "warn" | "strict"
        Validation mode applied after reading.
    engine : "pandas" | "pyarrow" | "pyspark"
        Which adapter to use for reading.
    **parquet_kwargs
        Extra arguments for the reader.

    Returns
    -------
    tuple of (DataFrame, ParquetDocMetadata)
        The loaded data and its parsed metadata.
    """
    path = Path(path)
    resolved = resolve_metadata_path(path, metadata_path)

    # Read parquet data
    adapter = get_adapter_by_engine(engine)
    df = adapter.read_parquet(path, **parquet_kwargs)
    actual_columns = adapter.get_column_names(df)

    if not resolved.exists():
        # Create skeleton metadata from actual columns
        columns = {
            col: ColumnMetadata(description="", nullable=True, pii=False)
            for col in actual_columns
        }
        ds_meta = DatasetMetadata(description="")
        metadata = ParquetDocMetadata(dataset=ds_meta, columns=columns)
        write_metadata_file(resolved, metadata)
        logger.warning(
            "No .meta.json found at %s – created default metadata file.", resolved
        )
        _log_empty_descriptions(metadata)
        return df, metadata

    # Read and parse existing metadata
    raw = read_metadata_file(resolved)
    struct_errors = validate_metadata_structure(raw)
    if struct_errors and mode == "strict":
        raise MetadataValidationError(struct_errors)
    metadata = ParquetDocMetadata.from_dict(raw)

    # Validate columns match
    columns_dict = {k: v.to_dict() for k, v in metadata.columns.items()}
    validate_columns_coverage(actual_columns, columns_dict, mode=mode)
    validate_mandatory_fields(columns_dict, mode=mode)

    _log_empty_descriptions(metadata)
    return df, metadata


def validate_dataset(
    path: str | Path,
    *,
    metadata_path: str | Path = "_metadata.json",
    mode: Literal["warn", "strict"] = "warn",
    engine: Literal["pandas", "pyarrow", "pyspark"] = "pyarrow",
) -> ValidationResult:
    """
    Validate an existing dataset's _metadata.json without loading full data.
    Uses PyArrow schema introspection to get column names cheaply.

    Returns
    -------
    ValidationResult
        Object containing errors, warnings, and is_valid flag.
    """
    path = Path(path)
    resolved = resolve_metadata_path(path, metadata_path)

    # Read metadata
    raw = read_metadata_file(resolved)
    struct_errors = validate_metadata_structure(raw)
    if struct_errors:
        if mode == "strict":
            raise MetadataValidationError(struct_errors)
        return ValidationResult(errors=struct_errors)

    metadata = ParquetDocMetadata.from_dict(raw)

    # Get column names cheaply via PyArrow schema (reads only parquet footer)
    schema = pq.read_schema(str(path))
    actual_columns = schema.names

    columns_dict = {k: v.to_dict() for k, v in metadata.columns.items()}
    result = ValidationResult()

    # Coverage check (don't raise in strict here, accumulate first)
    actual_set = set(actual_columns)
    metadata_set = set(columns_dict.keys())
    for col in sorted(actual_set - metadata_set):
        result.errors.append(
            f"Column '{col}' exists in data but is not documented in _metadata.json."
        )
    for col in sorted(metadata_set - actual_set):
        result.warnings.append(
            f"Column '{col}' documented in _metadata.json but not found in data (stale entry)."
        )

    # Mandatory fields check
    for col_name, col_data in columns_dict.items():
        from meta_parquet_doc._schema import MANDATORY_COLUMN_FIELDS, _FIELD_TYPES

        for field_name in MANDATORY_COLUMN_FIELDS:
            if field_name not in col_data:
                result.errors.append(
                    f"Column '{col_name}' is missing mandatory field '{field_name}'."
                )
            elif field_name in _FIELD_TYPES:
                expected = _FIELD_TYPES[field_name]
                if not isinstance(col_data[field_name], expected):
                    actual = type(col_data[field_name]).__name__
                    result.errors.append(
                        f"Column '{col_name}' field '{field_name}' "
                        f"should be {expected.__name__}, got {actual}."
                    )

    if mode == "strict" and result.errors:
        raise MetadataValidationError(result.errors)

    return result


def init_metadata(
    path: str | Path,
    *,
    metadata_path: str | Path = "_metadata.json",
    dataset_description: str = "",
    owner: str | None = None,
) -> Path:
    """
    Generate a skeleton _metadata.json from an existing Parquet file.
    Pre-fills column names with empty descriptions and default values.

    Returns
    -------
    Path
        Path to the generated _metadata.json.
    """
    path = Path(path)

    # Read schema cheaply via PyArrow
    schema = pq.read_schema(str(path))
    column_names = schema.names

    # Build skeleton metadata
    columns = {
        col: ColumnMetadata(description="", nullable=True, pii=False)
        for col in column_names
    }
    dataset = DatasetMetadata(description=dataset_description, owner=owner)
    metadata = ParquetDocMetadata(dataset=dataset, columns=columns)

    # Write to disk
    resolved = resolve_metadata_path(path, metadata_path)
    write_metadata_file(resolved, metadata)

    return resolved


def _log_empty_descriptions(metadata: ParquetDocMetadata) -> None:
    """Log warnings for empty description fields in dataset and columns."""
    if metadata.dataset.description == "":
        logger.warning("Dataset description is empty.")
    for col_name, col_meta in metadata.columns.items():
        if col_meta.description == "":
            logger.warning("Column '%s' has an empty description.", col_name)
