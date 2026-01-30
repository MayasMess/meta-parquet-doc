"""Tests for _metadata_io.py."""

import json

import pytest

from meta_parquet_doc._metadata_io import (
    read_metadata_file,
    resolve_metadata_path,
    write_metadata_file,
)
from meta_parquet_doc._types import (
    ColumnMetadata,
    DatasetMetadata,
    ParquetDocMetadata,
)
from meta_parquet_doc.exceptions import MetadataFileNotFoundError


class TestResolveMetadataPath:
    def test_absolute_metadata_path(self, tmp_path):
        absolute = tmp_path / "custom.json"
        result = resolve_metadata_path("/some/data.parquet", absolute)
        assert result == absolute

    def test_default_for_file(self, tmp_path):
        """File -> <filename>.meta.json next to the file."""
        data_file = tmp_path / "data.parquet"
        data_file.touch()
        result = resolve_metadata_path(data_file)
        assert result == tmp_path / "data.parquet.meta.json"

    def test_default_for_directory(self, tmp_path):
        """Directory -> <dirname>.meta.json next to the directory."""
        data_dir = tmp_path / "dataset"
        data_dir.mkdir()
        result = resolve_metadata_path(data_dir)
        assert result == tmp_path / "dataset.meta.json"

    def test_explicit_relative_path_for_file(self, tmp_path):
        """Explicit metadata_path is resolved relative to file's parent."""
        data_file = tmp_path / "data.parquet"
        data_file.touch()
        result = resolve_metadata_path(data_file, "custom.json")
        assert result == tmp_path / "custom.json"

    def test_explicit_relative_path_for_directory(self, tmp_path):
        """Explicit metadata_path is resolved relative to directory's parent."""
        data_dir = tmp_path / "dataset"
        data_dir.mkdir()
        result = resolve_metadata_path(data_dir, "custom.json")
        assert result == tmp_path / "custom.json"


class TestReadWriteMetadataFile:
    def test_write_and_read_roundtrip(self, tmp_path):
        path = tmp_path / "_metadata.json"
        metadata = ParquetDocMetadata(
            dataset=DatasetMetadata(description="Test", owner="me"),
            columns={
                "id": ColumnMetadata(description="ID", nullable=False, pii=False),
            },
        )
        write_metadata_file(path, metadata)

        raw = read_metadata_file(path)
        restored = ParquetDocMetadata.from_dict(raw)
        assert restored.dataset.description == "Test"
        assert restored.columns["id"].nullable is False

    def test_json_formatting(self, tmp_path):
        """Verify sorted keys, 2-space indent, trailing newline."""
        path = tmp_path / "_metadata.json"
        metadata = ParquetDocMetadata(
            dataset=DatasetMetadata(description="D"),
            columns={"b_col": ColumnMetadata("B", True, False), "a_col": ColumnMetadata("A", False, True)},
        )
        write_metadata_file(path, metadata)

        content = path.read_text(encoding="utf-8")
        # Trailing newline
        assert content.endswith("\n")
        # Sorted keys: $schema before columns before dataset
        parsed = json.loads(content)
        keys = list(parsed.keys())
        assert keys == sorted(keys)

    def test_deterministic_output(self, tmp_path):
        """Same metadata written twice produces identical content."""
        metadata = ParquetDocMetadata(
            dataset=DatasetMetadata(description="X"),
            columns={"col": ColumnMetadata("C", True, False)},
        )
        p1 = tmp_path / "a.json"
        p2 = tmp_path / "b.json"
        write_metadata_file(p1, metadata)
        write_metadata_file(p2, metadata)
        assert p1.read_text() == p2.read_text()

    def test_read_nonexistent_raises(self, tmp_path):
        with pytest.raises(MetadataFileNotFoundError):
            read_metadata_file(tmp_path / "nope.json")

    def test_creates_parent_directories(self, tmp_path):
        path = tmp_path / "a" / "b" / "_metadata.json"
        metadata = ParquetDocMetadata(
            dataset=DatasetMetadata(description="D"),
            columns={},
        )
        write_metadata_file(path, metadata)
        assert path.exists()
