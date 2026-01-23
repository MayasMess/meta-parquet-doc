"""Column coverage and mandatory field validation."""

import warnings
from dataclasses import dataclass, field
from typing import Any, Literal

from meta_parquet_doc._schema import MANDATORY_COLUMN_FIELDS, _FIELD_TYPES
from meta_parquet_doc.exceptions import MetadataValidationError

ValidationMode = Literal["warn", "strict"]


@dataclass
class ValidationResult:
    """Holds the outcome of metadata validation."""

    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    @property
    def is_valid(self) -> bool:
        return len(self.errors) == 0


def validate_columns_coverage(
    actual_columns: list[str],
    metadata_columns: dict[str, Any],
    mode: ValidationMode = "warn",
) -> ValidationResult:
    """
    Check that every column in the data is documented, and that
    there are no stale entries in metadata for columns not in the data.
    """
    result = ValidationResult()
    actual_set = set(actual_columns)
    metadata_set = set(metadata_columns.keys())

    # Columns in data but not documented
    undocumented = actual_set - metadata_set
    for col in sorted(undocumented):
        result.errors.append(
            f"Column '{col}' exists in data but is not documented in _metadata.json."
        )

    # Stale entries: documented but not in data
    stale = metadata_set - actual_set
    for col in sorted(stale):
        result.warnings.append(
            f"Column '{col}' documented in _metadata.json but not found in data (stale entry)."
        )

    _apply_mode(result, mode)
    return result


def validate_mandatory_fields(
    metadata_columns: dict[str, Any],
    mode: ValidationMode = "warn",
) -> ValidationResult:
    """
    Check that each column entry has all mandatory fields
    (description, nullable, pii) with correct types.
    """
    result = ValidationResult()

    for col_name, col_data in metadata_columns.items():
        if not isinstance(col_data, dict):
            result.errors.append(f"Column '{col_name}' metadata must be a dict.")
            continue
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

    _apply_mode(result, mode)
    return result


def _apply_mode(result: ValidationResult, mode: ValidationMode) -> None:
    """Raise or warn depending on mode."""
    if mode == "strict" and result.errors:
        raise MetadataValidationError(result.errors)
    elif mode == "warn":
        for msg in result.errors:
            warnings.warn(msg, UserWarning, stacklevel=3)
        for msg in result.warnings:
            warnings.warn(msg, UserWarning, stacklevel=3)
