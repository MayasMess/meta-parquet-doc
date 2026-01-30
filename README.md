# meta-parquet-doc

Governance and documentation for Parquet datasets via a `.meta.json` file stored alongside the data.

`meta-parquet-doc` enables you to:
- Document each column of a dataset (description, nullable, PII)
- Automatically validate that documentation is complete
- Read and write Parquet datasets with **Pandas**, **PyArrow**, or **PySpark**
- Version documentation in Git (readable, deterministic JSON)

## Getting started: minimal code change

### Drop-in replacement for native Parquet writing

You can start using `meta-parquet-doc` with **minimal code changes** — metadata is optional:

**Pandas** - Before:
```python
df.to_parquet("./data/users.parquet")
```

**Pandas** - After:
```python
from meta_parquet_doc import write_dataset

write_dataset(df, path="./data/users.parquet")
# Works immediately, logs warnings about missing documentation
```

**PyArrow** - Before:
```python
import pyarrow.parquet as pq
pq.write_table(table, "./data/users.parquet")
```

**PyArrow** - After:
```python
from meta_parquet_doc import write_dataset

write_dataset(table, path="./data/users.parquet")
# Auto-detects PyArrow, logs warnings about missing documentation
```

**PySpark** - Before:
```python
df.write.parquet("./data/users.parquet")
```

**PySpark** - After:
```python
from meta_parquet_doc import write_dataset

write_dataset(df, path="./data/users.parquet")
# Auto-detects PySpark, logs warnings about missing documentation
```

### Progressive documentation

Start without metadata, then add it progressively:

```python
from meta_parquet_doc import write_dataset

# Step 1: Just replace the write function (logs warnings)
write_dataset(df, path="./data/users.parquet")

# Step 2: Document some columns
write_dataset(
    df,
    path="./data/users.parquet",
    columns_metadata={
        "user_id": {"description": "ID", "nullable": False, "pii": False},
        # Other columns: warnings logged
    },
)

# Step 3: Full documentation + validation
write_dataset(
    df,
    path="./data/users.parquet",
    dataset_metadata={"description": "Users table.", "owner": "data-team"},
    columns_metadata={
        "user_id": {"description": "Unique ID", "nullable": False, "pii": False},
        "email": {"description": "Email", "nullable": True, "pii": True},
    },
    mode="strict",  # Now enforces complete documentation
)
```

By default (`mode="warn"`), missing metadata logs warnings but doesn't block writes. Use `mode="strict"` to enforce complete documentation.

## Why add metadata?

### Without metadata (native Parquet)

```python
import pyarrow.parquet as pq
import pyarrow as pa

table = pa.table({
    "user_id": [1, 2, 3],
    "email": ["a@x.com", "b@x.com", None],
})

pq.write_table(table, "./data/users.parquet")

# ❌ No column documentation
# ❌ No PII data traceability
# ❌ No completeness validation
# ❌ No governance
```

### With full metadata (meta-parquet-doc)

```python
from meta_parquet_doc import write_dataset
import pandas as pd

df = pd.DataFrame({
    "user_id": [1, 2, 3],
    "email": ["a@x.com", "b@x.com", None],
})

write_dataset(
    df,
    path="./data/users.parquet",
    dataset_metadata={
        "description": "Users table.",
        "owner": "data-team",
    },
    columns_metadata={
        "user_id": {
            "description": "Unique identifier.",
            "nullable": False,
            "pii": False,
        },
        "email": {
            "description": "Email address.",
            "nullable": True,
            "pii": True,
            "pii_category": "contact_info",
        },
    },
    mode="strict",
)

# ✓ Produces users.parquet + users.parquet.meta.json
# ✓ Documentation versioned in Git
# ✓ Automatic validation of mandatory fields
# ✓ PII column identification
# ✓ Data governance built-in
```

## Installation

```bash
pip install meta-parquet-doc
```

With PySpark support:

```bash
pip install meta-parquet-doc[spark]
```

## Quick usage

### Write a documented dataset

```python
import pandas as pd
from meta_parquet_doc import write_dataset

df = pd.DataFrame({
    "user_id": [1, 2, 3],
    "email": ["a@x.com", "b@x.com", None],
})

write_dataset(
    df,
    path="./data/users.parquet",
    dataset_metadata={
        "description": "Users table.",
        "owner": "data-team",
    },
    columns_metadata={
        "user_id": {
            "description": "Unique identifier.",
            "nullable": False,
            "pii": False,
        },
        "email": {
            "description": "Email address.",
            "nullable": True,
            "pii": True,
            "pii_category": "contact_info",
        },
    },
    mode="strict",
)
```

This produces two files:
- `./data/users.parquet` — the data
- `./data/users.parquet.meta.json` — the documentation

### Read a dataset with its metadata

```python
from meta_parquet_doc import read_dataset

df, metadata = read_dataset("./data/users.parquet", engine="pandas")

print(metadata.dataset.description)  # "Users table."
print(metadata.columns["email"].pii)  # True
```

### Validate an existing dataset

```python
from meta_parquet_doc import validate_dataset

result = validate_dataset("./data/users.parquet")
if result.is_valid:
    print("Documentation complete.")
else:
    print(result.errors)
```

### Generate a documentation skeleton

For an existing Parquet dataset without metadata:

```python
from meta_parquet_doc import init_metadata

init_metadata(
    "./data/users.parquet",
    dataset_description="Users table.",
    owner="data-team",
)
```

This creates a pre-filled `users.parquet.meta.json` with column names, which you can then complete.

## With PyArrow (partitioned datasets)

```python
import pyarrow as pa
from meta_parquet_doc import write_dataset, read_dataset

table = pa.table({
    "region": ["eu", "us", "eu"],
    "score": [85.5, 92.0, 78.3],
})

write_dataset(
    table,
    path="./data/scores",
    dataset_metadata={"description": "Scores by region."},
    columns_metadata={
        "region": {"description": "Region.", "nullable": False, "pii": False},
        "score": {"description": "Score.", "nullable": False, "pii": False},
    },
    partition_cols=["region"],  # passed directly to PyArrow
)
# Produces: ./data/scores/ (partitioned) + ./data/scores.meta.json

table, meta = read_dataset("./data/scores", engine="pyarrow")
```

## With PySpark

```python
from pyspark.sql import SparkSession
from meta_parquet_doc import write_dataset, read_dataset

spark = SparkSession.builder.master("local[*]").getOrCreate()
df = spark.createDataFrame([(1, "Alice"), (2, "Bob")], ["id", "name"])

write_dataset(
    df,
    path="./data/people.parquet",
    dataset_metadata={"description": "People."},
    columns_metadata={
        "id": {"description": "ID.", "nullable": False, "pii": False},
        "name": {"description": "Name.", "nullable": False, "pii": True},
    },
)
# Produces: ./data/people.parquet + ./data/people.parquet.meta.json

df_loaded, meta = read_dataset(
    "./data/people.parquet",
    engine="pyspark",
    spark=spark,
)
```

## `.meta.json` file format

```json
{
  "$schema": "meta-parquet-doc/v1",
  "columns": {
    "user_id": {
      "description": "Unique identifier.",
      "nullable": false,
      "pii": false
    },
    "email": {
      "description": "Email address.",
      "nullable": true,
      "pii": true,
      "pii_category": "contact_info"
    }
  },
  "dataset": {
    "description": "Users table.",
    "owner": "data-team",
    "tags": ["users"]
  }
}
```

### Per-column fields

| Field | Required | Type | Description |
|-------|:-----------:|------|-------------|
| `description` | yes | str | Column description |
| `nullable` | yes | bool | Can the column contain null values? |
| `pii` | yes | bool | Personally identifiable information? |
| `pii_category` | no | str | PII category (e.g. `direct_identifier`, `contact_info`) |

### Dataset fields

| Field | Required | Type | Description |
|-------|:-----------:|------|-------------|
| `description` | yes | str | Dataset description |
| `owner` | no | str | Responsible team or person |
| `tags` | no | list[str] | Free-form tags for categorization |

## Validation modes

| Mode | Behavior |
|------|-------------|
| `"warn"` (default) | Logs Python warnings, operation continues |
| `"strict"` | Raises `MetadataValidationError` when a problem is detected |

Validations performed:
- Each dataset column is documented in the `.meta.json` file
- Mandatory fields (`description`, `nullable`, `pii`) are present and of the correct type
- Detection of obsolete entries (documented columns absent from the data)

## Complete API

```python
from meta_parquet_doc import (
    write_dataset,         # Write data + metadata
    read_dataset,          # Read data + metadata
    validate_dataset,      # Validate without loading data
    init_metadata,         # Generate .meta.json skeleton

    # Types
    ColumnMetadata,
    DatasetMetadata,
    ParquetDocMetadata,
    ValidationResult,

    # Exceptions
    MetadataValidationError,
    MetadataFileNotFoundError,
    UnsupportedDataFrameError,
)
```

## Contributing

### Prerequisites

- Python >= 3.14
- [uv](https://docs.astral.sh/uv/) for dependency management

### Setup

```bash
git clone https://github.com/MayasMess/meta-parquet-doc.git
cd meta-parquet-doc
uv sync --extra dev
```

### Run tests

```bash
uv run pytest tests/ -v
```

For a specific test:

```bash
uv run pytest tests/test_api.py::TestWriteDataset::test_write_pandas_with_metadata -v
```

### Linter

```bash
uv run ruff check src/ tests/
uv run ruff check --fix src/ tests/   # auto-fix
```

### Project structure

```
src/meta_parquet_doc/
├── api.py              # Public functions (write/read/validate/init)
├── _types.py           # Metadata dataclasses
├── _validation.py      # Validation logic (coverage + mandatory fields)
├── _metadata_io.py     # JSON file read/write
├── _schema.py          # Structural validation of raw JSON
├── _adapters/          # Engine-specific adapters (Pandas, PyArrow, PySpark)
└── exceptions.py       # Exception hierarchy
tests/                  # pytest tests
examples/               # Usage examples
```

### Conventions

- Code follows [ruff](https://docs.astral.sh/ruff/) rules (max 100 char line length)
- Tests use `pytest` with shared fixtures in `conftest.py`
- `.meta.json` files are always written with sorted keys and 2-space indent for stable Git diffs
- PySpark is an optional dependency: Spark tests are marked with `@pytest.mark.spark`
