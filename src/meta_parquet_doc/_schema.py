"""Structural validation of raw _metadata.json dictionaries."""

from typing import Any

MANDATORY_COLUMN_FIELDS: tuple[str, ...] = ("description", "nullable", "pii")

# Expected types for each column field
_FIELD_TYPES: dict[str, type] = {
    "description": str,
    "nullable": bool,
    "pii": bool,
    "pii_category": str,
}


def validate_metadata_structure(raw: dict[str, Any]) -> list[str]:
    """
    Validate that a raw JSON dict matches the expected _metadata.json schema.

    Returns a list of error messages. An empty list means the structure is valid.
    """
    errors: list[str] = []

    # Check $schema key
    if "$schema" not in raw:
        errors.append("Missing '$schema' key at root level.")

    # Check dataset section
    if "dataset" not in raw:
        errors.append("Missing 'dataset' section.")
    else:
        dataset = raw["dataset"]
        if not isinstance(dataset, dict):
            errors.append("'dataset' must be an object.")
        else:
            if "description" not in dataset:
                errors.append("'dataset.description' is required.")
            elif not isinstance(dataset["description"], str):
                errors.append("'dataset.description' must be a string.")

    # Check columns section
    if "columns" not in raw:
        errors.append("Missing 'columns' section.")
    else:
        columns = raw["columns"]
        if not isinstance(columns, dict):
            errors.append("'columns' must be an object.")
        else:
            for col_name, col_data in columns.items():
                if not isinstance(col_data, dict):
                    errors.append(f"Column '{col_name}' must be an object.")
                    continue
                # Check mandatory fields
                for field_name in MANDATORY_COLUMN_FIELDS:
                    if field_name not in col_data:
                        errors.append(
                            f"Column '{col_name}' is missing mandatory field '{field_name}'."
                        )
                    elif field_name in _FIELD_TYPES:
                        expected = _FIELD_TYPES[field_name]
                        actual = type(col_data[field_name])
                        if not isinstance(col_data[field_name], expected):
                            errors.append(
                                f"Column '{col_name}' field '{field_name}' "
                                f"should be {expected.__name__}, got {actual.__name__}."
                            )

    return errors
