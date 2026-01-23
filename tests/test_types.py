"""Tests for _types.py dataclasses and serialization."""

from meta_parquet_doc._types import (
    ColumnMetadata,
    DatasetMetadata,
    ParquetDocMetadata,
    SCHEMA_VERSION,
)


class TestColumnMetadata:
    def test_to_dict_minimal(self):
        col = ColumnMetadata(description="An ID", nullable=False, pii=False)
        d = col.to_dict()
        assert d == {"description": "An ID", "nullable": False, "pii": False}
        assert "pii_category" not in d

    def test_to_dict_with_pii_category(self):
        col = ColumnMetadata(description="Email", nullable=True, pii=True, pii_category="contact")
        d = col.to_dict()
        assert d["pii_category"] == "contact"

    def test_from_dict_minimal(self):
        col = ColumnMetadata.from_dict({"description": "X", "nullable": True})
        assert col.pii is False
        assert col.pii_category is None

    def test_roundtrip(self):
        original = ColumnMetadata(description="Test", nullable=False, pii=True, pii_category="id")
        restored = ColumnMetadata.from_dict(original.to_dict())
        assert restored == original


class TestDatasetMetadata:
    def test_to_dict_minimal(self):
        ds = DatasetMetadata(description="My dataset")
        d = ds.to_dict()
        assert d == {"description": "My dataset"}

    def test_to_dict_full(self):
        ds = DatasetMetadata(description="D", owner="team", tags=["a", "b"])
        d = ds.to_dict()
        assert d == {"description": "D", "owner": "team", "tags": ["a", "b"]}

    def test_from_dict(self):
        ds = DatasetMetadata.from_dict({"description": "X", "owner": "me", "tags": ["t"]})
        assert ds.owner == "me"
        assert ds.tags == ["t"]

    def test_roundtrip(self):
        original = DatasetMetadata(description="DS", owner="o", tags=["x"])
        restored = DatasetMetadata.from_dict(original.to_dict())
        assert restored == original


class TestParquetDocMetadata:
    def test_to_dict_structure(self):
        meta = ParquetDocMetadata(
            dataset=DatasetMetadata(description="Test"),
            columns={"col_a": ColumnMetadata(description="A", nullable=False, pii=False)},
        )
        d = meta.to_dict()
        assert d["$schema"] == SCHEMA_VERSION
        assert "dataset" in d
        assert "columns" in d
        assert "col_a" in d["columns"]

    def test_roundtrip(self):
        original = ParquetDocMetadata(
            dataset=DatasetMetadata(description="DS", owner="team"),
            columns={
                "id": ColumnMetadata(description="ID", nullable=False, pii=False),
                "email": ColumnMetadata(description="E", nullable=True, pii=True, pii_category="c"),
            },
        )
        restored = ParquetDocMetadata.from_dict(original.to_dict())
        assert restored.dataset == original.dataset
        assert restored.columns == original.columns
