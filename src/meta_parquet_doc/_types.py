"""Dataclass definitions for metadata structures."""

from dataclasses import dataclass, field
from typing import Any


@dataclass
class ColumnMetadata:
    """Documentation for a single column in the dataset."""

    description: str
    nullable: bool
    pii: bool = False
    pii_category: str | None = None

    def to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {
            "description": self.description,
            "nullable": self.nullable,
            "pii": self.pii,
        }
        if self.pii_category is not None:
            result["pii_category"] = self.pii_category
        return result

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ColumnMetadata":
        return cls(
            description=data["description"],
            nullable=data["nullable"],
            pii=data.get("pii", False),
            pii_category=data.get("pii_category"),
        )


@dataclass
class DatasetMetadata:
    """Top-level documentation for the dataset itself."""

    description: str
    owner: str | None = None
    tags: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {"description": self.description}
        if self.owner is not None:
            result["owner"] = self.owner
        if self.tags:
            result["tags"] = self.tags
        return result

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "DatasetMetadata":
        return cls(
            description=data["description"],
            owner=data.get("owner"),
            tags=data.get("tags", []),
        )


SCHEMA_VERSION = "meta-parquet-doc/v1"


@dataclass
class ParquetDocMetadata:
    """Complete metadata document stored as _metadata.json."""

    dataset: DatasetMetadata
    columns: dict[str, ColumnMetadata]
    schema_version: str = SCHEMA_VERSION

    def to_dict(self) -> dict[str, Any]:
        return {
            "$schema": self.schema_version,
            "dataset": self.dataset.to_dict(),
            "columns": {
                name: col.to_dict() for name, col in self.columns.items()
            },
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ParquetDocMetadata":
        return cls(
            schema_version=data.get("$schema", SCHEMA_VERSION),
            dataset=DatasetMetadata.from_dict(data["dataset"]),
            columns={
                name: ColumnMetadata.from_dict(col_data)
                for name, col_data in data.get("columns", {}).items()
            },
        )
