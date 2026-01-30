"""Read and write _metadata.json files to disk."""

import json
from pathlib import Path

from meta_parquet_doc._types import ParquetDocMetadata
from meta_parquet_doc.exceptions import MetadataFileNotFoundError


def resolve_metadata_path(
    data_path: str | Path, metadata_path: str | Path | None = None
) -> Path:
    """
    Resolve the location of the metadata file relative to the data path.

    - If metadata_path is absolute, use it directly.
    - If metadata_path is explicitly provided, resolve relative to data location.
    - Otherwise, auto-generate: <data_path>.meta.json (next to the file or directory).

    Examples:
        data.parquet      -> data.parquet.meta.json
        dataset/          -> dataset.meta.json
        /abs/custom.json  -> /abs/custom.json (used as-is)
    """
    data_path = Path(data_path)

    # If metadata_path is explicitly provided
    if metadata_path is not None:
        metadata_path = Path(metadata_path)
        if metadata_path.is_absolute():
            return metadata_path
        # Resolve relative to parent of data_path
        return data_path.parent / metadata_path

    # Default behavior: <data_path>.meta.json next to the data
    return data_path.parent / (data_path.name + ".meta.json")


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
