"""Exception hierarchy for meta-parquet-doc."""


class MetaParquetDocError(Exception):
    """Base exception for all meta-parquet-doc errors."""


class MetadataValidationError(MetaParquetDocError):
    """Raised when metadata validation fails in strict mode."""

    def __init__(self, errors: list[str]):
        self.errors = errors
        msg = f"Metadata validation failed with {len(errors)} error(s):\n"
        msg += "\n".join(f"  - {e}" for e in errors)
        super().__init__(msg)


class MetadataFileNotFoundError(MetaParquetDocError, FileNotFoundError):
    """Raised when _metadata.json is expected but does not exist."""


class UnsupportedDataFrameError(MetaParquetDocError, TypeError):
    """Raised when a DataFrame type is not recognized by any adapter."""
