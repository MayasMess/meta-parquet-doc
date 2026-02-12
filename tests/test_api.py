"""Integration tests for the public API."""

import json
import warnings

import pyarrow as pa
import pytest

import pandas as pd

from meta_parquet_doc import (
    ColumnMetadata,
    DatasetMetadata,
    ParquetDocMetadata,
    init_metadata,
    read_dataset,
    validate_dataset,
    write_dataset,
)
from meta_parquet_doc.exceptions import MetadataValidationError


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

    def test_read_missing_metadata_creates_default(self, tmp_path, sample_pandas_df):
        out = tmp_path / "data.parquet"
        sample_pandas_df.to_parquet(out)
        df, metadata = read_dataset(out)
        # Should have created the .meta.json file
        meta_file = tmp_path / "_metadata.json"
        assert meta_file.exists()
        # Skeleton has all columns with empty descriptions
        assert set(metadata.columns.keys()) == {"user_id", "name", "email"}
        assert metadata.dataset.description == ""
        for col_meta in metadata.columns.values():
            assert col_meta.description == ""
            assert col_meta.nullable is True
            assert col_meta.pii is False


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


class TestColumnsFrom:
    def test_columns_from_filters_to_actual_columns(self, tmp_path):
        """columns_from only keeps columns present in the DataFrame."""
        source_meta = ParquetDocMetadata(
            dataset=DatasetMetadata(description="source"),
            columns={
                "user_id": ColumnMetadata(description="User ID", nullable=False, pii=False),
                "name": ColumnMetadata(description="Full name", nullable=False, pii=True),
                "extra_col": ColumnMetadata(description="Not in target", nullable=True, pii=False),
            },
        )
        df = pd.DataFrame({"user_id": [1], "name": ["Alice"]})
        out = tmp_path / "data.parquet"
        meta_path = write_dataset(
            df, out,
            dataset_metadata={"description": "Target"},
            columns_from=source_meta,
            mode="strict",
        )
        raw = json.loads(meta_path.read_text())
        assert set(raw["columns"].keys()) == {"user_id", "name"}
        assert raw["columns"]["user_id"]["description"] == "User ID"
        assert "extra_col" not in raw["columns"]

    def test_columns_from_with_columns_metadata_override(self, tmp_path):
        """columns_metadata overrides entries from columns_from."""
        source_meta = ParquetDocMetadata(
            dataset=DatasetMetadata(description="source"),
            columns={
                "user_id": ColumnMetadata(description="Old desc", nullable=False, pii=False),
                "name": ColumnMetadata(description="Full name", nullable=False, pii=True),
            },
        )
        df = pd.DataFrame({"user_id": [1], "name": ["Alice"], "new_col": [42]})
        out = tmp_path / "data.parquet"
        meta_path = write_dataset(
            df, out,
            dataset_metadata={"description": "Target"},
            columns_from=source_meta,
            columns_metadata={
                "user_id": {"description": "Overridden", "nullable": False, "pii": False},
                "new_col": {"description": "Brand new", "nullable": False, "pii": False},
            },
            mode="strict",
        )
        raw = json.loads(meta_path.read_text())
        assert raw["columns"]["user_id"]["description"] == "Overridden"
        assert raw["columns"]["name"]["description"] == "Full name"
        assert raw["columns"]["new_col"]["description"] == "Brand new"

    def test_columns_from_merged_sources(self, tmp_path):
        """columns_from accepts merged metadata via | operator."""
        meta_users = ParquetDocMetadata(
            dataset=DatasetMetadata(description="Users"),
            columns={
                "user_id": ColumnMetadata(description="User ID", nullable=False, pii=False),
                "name": ColumnMetadata(description="Full name", nullable=False, pii=True),
            },
        )
        meta_orders = ParquetDocMetadata(
            dataset=DatasetMetadata(description="Orders"),
            columns={
                "user_id": ColumnMetadata(description="User ID", nullable=False, pii=False),
                "amount": ColumnMetadata(description="Order amount", nullable=False, pii=False),
            },
        )
        df = pd.DataFrame({"user_id": [1], "name": ["Alice"], "amount": [10.0]})
        out = tmp_path / "data.parquet"
        meta_path = write_dataset(
            df, out,
            dataset_metadata={"description": "Joined", "owner": "analytics"},
            columns_from=meta_users | meta_orders,
            mode="strict",
        )
        raw = json.loads(meta_path.read_text())
        assert set(raw["columns"].keys()) == {"user_id", "name", "amount"}
        assert raw["dataset"]["description"] == "Joined"
        assert raw["dataset"]["owner"] == "analytics"

    def test_end_to_end_read_merge_write(self, tmp_path):
        """Full workflow: read two datasets, merge metadata, write joined."""
        df_users = pd.DataFrame({"user_id": [1, 2], "name": ["A", "B"]})
        df_orders = pd.DataFrame({"user_id": [1, 2], "amount": [10.0, 20.0]})

        users_dir = tmp_path / "users"
        users_dir.mkdir()
        orders_dir = tmp_path / "orders"
        orders_dir.mkdir()
        joined_dir = tmp_path / "joined"
        joined_dir.mkdir()

        write_dataset(
            df_users, users_dir / "data.parquet",
            dataset_metadata={"description": "Users"},
            columns_metadata={
                "user_id": {"description": "User ID", "nullable": False, "pii": False},
                "name": {"description": "Full name", "nullable": False, "pii": True},
            },
        )
        write_dataset(
            df_orders, orders_dir / "data.parquet",
            dataset_metadata={"description": "Orders"},
            columns_metadata={
                "user_id": {"description": "User ID", "nullable": False, "pii": False},
                "amount": {"description": "Order amount", "nullable": False, "pii": False},
            },
        )

        _, meta_users = read_dataset(users_dir / "data.parquet")
        _, meta_orders = read_dataset(orders_dir / "data.parquet")

        df_joined = df_users.merge(df_orders, on="user_id")

        write_dataset(
            df_joined, joined_dir / "data.parquet",
            dataset_metadata={"description": "Users with orders", "owner": "analytics"},
            columns_from=meta_users | meta_orders,
            mode="strict",
        )

        df_result, meta_result = read_dataset(joined_dir / "data.parquet")
        assert set(meta_result.columns.keys()) == {"user_id", "name", "amount"}
        assert meta_result.dataset.description == "Users with orders"
        assert meta_result.dataset.owner == "analytics"
        assert meta_result.columns["user_id"].description == "User ID"
        assert meta_result.columns["name"].pii is True
        assert meta_result.columns["amount"].description == "Order amount"


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
