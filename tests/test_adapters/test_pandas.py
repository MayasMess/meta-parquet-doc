"""Tests for the Pandas adapter."""

import pandas as pd

from meta_parquet_doc._adapters._pandas import PandasAdapter


class TestPandasAdapter:
    def setup_method(self):
        self.adapter = PandasAdapter()

    def test_get_column_names(self, sample_pandas_df):
        assert self.adapter.get_column_names(sample_pandas_df) == ["user_id", "name", "email"]

    def test_write_and_read_roundtrip(self, tmp_path, sample_pandas_df):
        path = tmp_path / "test.parquet"
        self.adapter.write_parquet(sample_pandas_df, path)
        assert path.exists()

        loaded = self.adapter.read_parquet(path)
        assert isinstance(loaded, pd.DataFrame)
        assert list(loaded.columns) == ["user_id", "name", "email"]
        assert len(loaded) == 3
