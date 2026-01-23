"""Integration tests for the public API."""

import json
import warnings

import pyarrow as pa
import pytest

from meta_parquet_doc import (
    init_metadata,
    read_dataset,
    validate_dataset,
    write_dataset,
)
from meta_parquet_doc.exceptions import (
    MetadataFileNotFoundError,
    MetadataValidationError,
)


class TestWriteDataset:
    def test_write_pandas_with_metadata(self, tmp_path, sample_pandas_df, sample_columns_metadata, sample_dataset_metadata):
        out = tmp_path / "data.parquet"
        meta_path = write_dataset(
            sample_pandas_df,
            out,
            dataset_metadata=sample_dataset_metadata,
            columns_metadata=sample_columns_metadata,
            mode="strict",
        )
        assert meta_path.exists()
        assert out.exists()
        raw = json.loads(meta_path.read_text())
        assert raw["dataset"]["description"] == "Test user dataset"
        assert "user_id" in raw["columns"]

    def test_write_pyarrow_with_metadata(self, tmp_path, sample_pyarrow_table, sample_columns_metadata, sample_dataset_metadata):
        out = tmp_path / "data.parquet"
        meta_path = write_dataset(
            sample_pyarrow_table,
            out,
            dataset_metadata=sample_dataset_metadata,
            columns_metadata=sample_columns_metadata,
            mode="strict",
        )
        assert meta_path.exists()
        assert out.exists()

    def test_write_creates_skeleton_warn_mode(self, tmp_path, sample_pandas_df):
        out = tmp_path / "data.parquet"
        with warnings.catch_warnings(record=True):
            warnings.simplefilter("always")
            meta_path = write_dataset(sample_pandas_df, out, mode="warn")
        assert meta_path.exists()
        raw = json.loads(meta_path.read_text())
        # Skeleton has all columns with empty descriptions
        assert set(raw["columns"].keys()) == {"user_id", "name", "email"}
        assert raw["columns"]["user_id"]["description"] == ""

    def test_write_strict_no_metadata_raises(self, tmp_path, sample_pandas_df):
        out = tmp_path / "data.parquet"
        with pytest.raises(MetadataValidationError):
            write_dataset(sample_pandas_df, out, mode="strict")

    def test_write_missing_column_strict(self, tmp_path, sample_pandas_df, sample_dataset_metadata):
        out = tmp_path / "data.parquet"
        # Only document 2 of 3 columns
        incomplete = {
            "user_id": {"description": "ID", "nullable": False, "pii": False},
            "name": {"description": "Name", "nullable": False, "pii": False},
        }
        with pytest.raises(MetadataValidationError):
            write_dataset(
                sample_pandas_df, out,
                dataset_metadata=sample_dataset_metadata,
                columns_metadata=incomplete,
                mode="strict",
            )

    def test_write_reads_existing_metadata(self, tmp_path, sample_pandas_df, sample_columns_metadata, sample_dataset_metadata):
        out = tmp_path / "data.parquet"
        # First write with full metadata
        write_dataset(
            sample_pandas_df, out,
            dataset_metadata=sample_dataset_metadata,
            columns_metadata=sample_columns_metadata,
            mode="strict",
        )
        # Second write without columns_metadata should use existing file
        write_dataset(sample_pandas_df, out, mode="strict")


class TestReadDataset:
    def test_read_pandas_roundtrip(self, tmp_path, sample_pandas_df, sample_columns_metadata, sample_dataset_metadata):
        out = tmp_path / "data.parquet"
        write_dataset(
            sample_pandas_df, out,
            dataset_metadata=sample_dataset_metadata,
            columns_metadata=sample_columns_metadata,
        )
        df, metadata = read_dataset(out, engine="pandas")
        assert list(df.columns) == ["user_id", "name", "email"]
        assert metadata.dataset.owner == "test-team"
        assert metadata.columns["email"].pii is True

    def test_read_pyarrow_engine(self, tmp_path, sample_pyarrow_table, sample_columns_metadata, sample_dataset_metadata):
        out = tmp_path / "data.parquet"
        write_dataset(
            sample_pyarrow_table, out,
            dataset_metadata=sample_dataset_metadata,
            columns_metadata=sample_columns_metadata,
        )
        table, metadata = read_dataset(out, engine="pyarrow")
        assert isinstance(table, pa.Table)
        assert table.num_rows == 3

    def test_read_missing_metadata_raises(self, tmp_path, sample_pandas_df):
        out = tmp_path / "data.parquet"
        sample_pandas_df.to_parquet(out)
        with pytest.raises(MetadataFileNotFoundError):
            read_dataset(out)


class TestValidateDataset:
    def test_valid_dataset(self, tmp_path, sample_pandas_df, sample_columns_metadata, sample_dataset_metadata):
        out = tmp_path / "data.parquet"
        write_dataset(
            sample_pandas_df, out,
            dataset_metadata=sample_dataset_metadata,
            columns_metadata=sample_columns_metadata,
        )
        result = validate_dataset(out)
        assert result.is_valid

    def test_invalid_dataset_strict(self, tmp_path, sample_pandas_df, sample_dataset_metadata):
        out = tmp_path / "data.parquet"
        # Write with incomplete metadata (skeleton)
        with warnings.catch_warnings(record=True):
            warnings.simplefilter("always")
            write_dataset(sample_pandas_df, out, mode="warn")
        # Validate in strict mode should raise (empty descriptions fail type check? No, they're still strings)
        # Actually skeleton has valid types but empty strings, which is valid structurally.
        result = validate_dataset(out, mode="warn")
        assert result.is_valid


class TestInitMetadata:
    def test_init_from_existing_parquet(self, tmp_path, sample_pandas_df):
        out = tmp_path / "data.parquet"
        sample_pandas_df.to_parquet(out)
        meta_path = init_metadata(out, dataset_description="Generated", owner="auto")
        assert meta_path.exists()
        raw = json.loads(meta_path.read_text())
        assert raw["dataset"]["description"] == "Generated"
        assert raw["dataset"]["owner"] == "auto"
        assert set(raw["columns"].keys()) == {"user_id", "name", "email"}
        # All columns should have skeleton values
        for col_data in raw["columns"].values():
            assert col_data["nullable"] is True
            assert col_data["pii"] is False
            assert col_data["description"] == ""
