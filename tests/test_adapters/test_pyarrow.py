"""Tests for the PyArrow adapter."""

import pyarrow as pa

from meta_parquet_doc._adapters._pyarrow import PyArrowAdapter


class TestPyArrowAdapter:
    def setup_method(self):
        self.adapter = PyArrowAdapter()

    def test_get_column_names(self, sample_pyarrow_table):
        assert self.adapter.get_column_names(sample_pyarrow_table) == ["user_id", "name", "email"]

    def test_write_and_read_single_file(self, tmp_path, sample_pyarrow_table):
        path = tmp_path / "test.parquet"
        self.adapter.write_parquet(sample_pyarrow_table, path)
        assert path.exists()

        loaded = self.adapter.read_parquet(path)
        assert isinstance(loaded, pa.Table)
        assert loaded.num_rows == 3

    def test_write_partitioned(self, tmp_path):
        table = pa.table({
            "region": ["us", "eu", "us", "eu"],
            "value": [1, 2, 3, 4],
        })
        path = tmp_path / "partitioned"
        self.adapter.write_parquet(table, path, partition_cols=["region"])
        assert path.is_dir()

        loaded = self.adapter.read_parquet(path)
        assert loaded.num_rows == 4
        assert "region" in loaded.schema.names
