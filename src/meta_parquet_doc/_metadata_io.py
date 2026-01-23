"""Read and write _metadata.json files to disk."""

import json
from pathlib import Path

from meta_parquet_doc._types import ParquetDocMetadata
from meta_parquet_doc.exceptions import MetadataFileNotFoundError


def resolve_metadata_path(
    data_path: str | Path, metadata_path: str | Path = "_metadata.json"
) -> Path:
    """
    Resolve the location of _metadata.json relative to the data path.

    - If metadata_path is absolute, use it directly.
    - If data_path is a directory (partitioned dataset), resolve relative to it.
    - If data_path is a file, resolve relative to its parent directory.
    """
    metadata_path = Path(metadata_path)
    if metadata_path.is_absolute():
        return metadata_path

    data_path = Path(data_path)
    if data_path.is_dir():
        return data_path / metadata_path
    else:
        return data_path.parent / metadata_path


def read_metadata_file(path: Path) -> dict:
    """Read and parse _metadata.json from disk."""
    path = Path(path)
    if not path.exists():
        raise MetadataFileNotFoundError(f"Metadata file not found: {path}")
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def write_metadata_file(path: Path, metadata: ParquetDocMetadata) -> None:
    """
    Write metadata to _metadata.json.

    Uses 2-space indent and sorted keys for human readability
    and stable git diffs. Ends with a trailing newline.
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    data = metadata.to_dict()
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, sort_keys=True, ensure_ascii=False)
        f.write("\n")
