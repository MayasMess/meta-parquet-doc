"""Tests for _validation.py."""

import warnings

import pytest

from meta_parquet_doc._validation import (
    validate_columns_coverage,
    validate_mandatory_fields,
)
from meta_parquet_doc.exceptions import MetadataValidationError


class TestValidateColumnsCoverage:
    def test_all_columns_documented(self):
        result = validate_columns_coverage(
            ["a", "b", "c"],
            {"a": {}, "b": {}, "c": {}},
            mode="warn",
        )
        assert result.is_valid
        assert not result.errors
        assert not result.warnings

    def test_undocumented_column_warn(self):
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            result = validate_columns_coverage(
                ["a", "b", "c"],
                {"a": {}, "b": {}},
                mode="warn",
            )
        assert not result.is_valid
        assert any("'c'" in e for e in result.errors)
        assert len(w) >= 1

    def test_undocumented_column_strict(self):
        with pytest.raises(MetadataValidationError) as exc_info:
            validate_columns_coverage(
                ["a", "b"],
                {"a": {}},
                mode="strict",
            )
        assert "'b'" in str(exc_info.value)

    def test_stale_column_warning(self):
        with warnings.catch_warnings(record=True):
            warnings.simplefilter("always")
            result = validate_columns_coverage(
                ["a"],
                {"a": {}, "old_col": {}},
                mode="warn",
            )
        assert result.is_valid  # stale entries are warnings, not errors
        assert any("old_col" in w for w in result.warnings)

    def test_stale_column_strict_no_raise(self):
        # Stale columns don't cause strict mode to raise (they're warnings)
        result = validate_columns_coverage(
            ["a"],
            {"a": {}, "stale": {}},
            mode="strict",
        )
        assert result.is_valid


class TestValidateMandatoryFields:
    def test_all_fields_present(self, sample_columns_metadata):
        result = validate_mandatory_fields(sample_columns_metadata, mode="strict")
        assert result.is_valid

    def test_missing_description(self):
        with pytest.raises(MetadataValidationError):
            validate_mandatory_fields(
                {"col": {"nullable": True, "pii": False}},
                mode="strict",
            )

    def test_missing_nullable(self):
        with pytest.raises(MetadataValidationError):
            validate_mandatory_fields(
                {"col": {"description": "X", "pii": False}},
                mode="strict",
            )

    def test_missing_pii(self):
        with pytest.raises(MetadataValidationError):
            validate_mandatory_fields(
                {"col": {"description": "X", "nullable": True}},
                mode="strict",
            )

    def test_wrong_type_description(self):
        with pytest.raises(MetadataValidationError):
            validate_mandatory_fields(
                {"col": {"description": 123, "nullable": True, "pii": False}},
                mode="strict",
            )

    def test_warn_mode_does_not_raise(self):
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            result = validate_mandatory_fields(
                {"col": {"nullable": True}},  # missing description and pii
                mode="warn",
            )
        assert not result.is_valid
        assert len(w) >= 2
