# meta-parquet-doc

Gouvernance et documentation de datasets Parquet via un fichier `_metadata.json` stocké à côté des données.

`meta-parquet-doc` permet de :
- Documenter chaque colonne d'un dataset (description, nullable, PII)
- Valider automatiquement que la documentation est complète
- Lire et écrire des datasets Parquet avec **Pandas**, **PyArrow** ou **PySpark**
- Versionner la documentation en Git (JSON lisible, déterministe)

## Installation

```bash
pip install meta-parquet-doc
```

Avec support PySpark :

```bash
pip install meta-parquet-doc[spark]
```

## Utilisation rapide

### Écrire un dataset documenté

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
        "description": "Table des utilisateurs.",
        "owner": "data-team",
    },
    columns_metadata={
        "user_id": {
            "description": "Identifiant unique.",
            "nullable": False,
            "pii": False,
        },
        "email": {
            "description": "Adresse email.",
            "nullable": True,
            "pii": True,
            "pii_category": "contact_info",
        },
    },
    mode="strict",
)
```

Cela produit deux fichiers :
- `./data/users.parquet` — les données
- `./data/_metadata.json` — la documentation

### Lire un dataset avec ses métadonnées

```python
from meta_parquet_doc import read_dataset

df, metadata = read_dataset("./data/users.parquet", engine="pandas")

print(metadata.dataset.description)  # "Table des utilisateurs."
print(metadata.columns["email"].pii)  # True
```

### Valider un dataset existant

```python
from meta_parquet_doc import validate_dataset

result = validate_dataset("./data/users.parquet")
if result.is_valid:
    print("Documentation complète.")
else:
    print(result.errors)
```

### Générer un squelette de documentation

Pour un dataset Parquet existant sans `_metadata.json` :

```python
from meta_parquet_doc import init_metadata

init_metadata(
    "./data/users.parquet",
    dataset_description="Table des utilisateurs.",
    owner="data-team",
)
```

Cela crée un `_metadata.json` pré-rempli avec les noms de colonnes, qu'il suffit ensuite de compléter.

## Avec PyArrow (datasets partitionnés)

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
    dataset_metadata={"description": "Scores par région."},
    columns_metadata={
        "region": {"description": "Région.", "nullable": False, "pii": False},
        "score": {"description": "Score.", "nullable": False, "pii": False},
    },
    partition_cols=["region"],  # passé directement à PyArrow
)

table, meta = read_dataset("./data/scores", engine="pyarrow")
```

## Avec PySpark

```python
from pyspark.sql import SparkSession
from meta_parquet_doc import write_dataset, read_dataset

spark = SparkSession.builder.master("local[*]").getOrCreate()
df = spark.createDataFrame([(1, "Alice"), (2, "Bob")], ["id", "name"])

write_dataset(
    df,
    path="./data/people.parquet",
    dataset_metadata={"description": "Personnes."},
    columns_metadata={
        "id": {"description": "ID.", "nullable": False, "pii": False},
        "name": {"description": "Nom.", "nullable": False, "pii": True},
    },
)

df_loaded, meta = read_dataset(
    "./data/people.parquet",
    engine="pyspark",
    spark=spark,
)
```

## Format du fichier `_metadata.json`

```json
{
  "$schema": "meta-parquet-doc/v1",
  "columns": {
    "user_id": {
      "description": "Identifiant unique.",
      "nullable": false,
      "pii": false
    },
    "email": {
      "description": "Adresse email.",
      "nullable": true,
      "pii": true,
      "pii_category": "contact_info"
    }
  },
  "dataset": {
    "description": "Table des utilisateurs.",
    "owner": "data-team",
    "tags": ["users"]
  }
}
```

### Champs par colonne

| Champ | Obligatoire | Type | Description |
|-------|:-----------:|------|-------------|
| `description` | oui | str | Description de la colonne |
| `nullable` | oui | bool | La colonne peut-elle contenir des null ? |
| `pii` | oui | bool | Donnée personnelle identifiable ? |
| `pii_category` | non | str | Catégorie PII (ex: `direct_identifier`, `contact_info`) |

### Champs du dataset

| Champ | Obligatoire | Type | Description |
|-------|:-----------:|------|-------------|
| `description` | oui | str | Description du dataset |
| `owner` | non | str | Équipe ou personne responsable |
| `tags` | non | list[str] | Tags libres pour catégoriser |

## Modes de validation

| Mode | Comportement |
|------|-------------|
| `"warn"` (défaut) | Affiche des warnings Python, l'opération continue |
| `"strict"` | Lève `MetadataValidationError` dès qu'un problème est détecté |

Les validations effectuées :
- Chaque colonne du dataset est documentée dans `_metadata.json`
- Les champs obligatoires (`description`, `nullable`, `pii`) sont présents et du bon type
- Détection des entrées obsolètes (colonnes documentées mais absentes des données)

## API complète

```python
from meta_parquet_doc import (
    write_dataset,         # Écrire données + métadonnées
    read_dataset,          # Lire données + métadonnées
    validate_dataset,      # Valider sans charger les données
    init_metadata,         # Générer un squelette _metadata.json

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

## Contribuer

### Prérequis

- Python >= 3.14
- [uv](https://docs.astral.sh/uv/) pour la gestion des dépendances

### Setup

```bash
git clone https://github.com/MayasMess/meta-parquet-doc.git
cd meta-parquet-doc
uv sync --extra dev
```

### Lancer les tests

```bash
uv run pytest tests/ -v
```

Pour un test spécifique :

```bash
uv run pytest tests/test_api.py::TestWriteDataset::test_write_pandas_with_metadata -v
```

### Linter

```bash
uv run ruff check src/ tests/
uv run ruff check --fix src/ tests/   # auto-fix
```

### Structure du projet

```
src/meta_parquet_doc/
├── api.py              # Fonctions publiques (write/read/validate/init)
├── _types.py           # Dataclasses de métadonnées
├── _validation.py      # Logique de validation (coverage + champs obligatoires)
├── _metadata_io.py     # Lecture/écriture du fichier JSON
├── _schema.py          # Validation structurelle du JSON brut
├── _adapters/          # Adaptateurs par moteur (Pandas, PyArrow, PySpark)
└── exceptions.py       # Hiérarchie d'exceptions
tests/                  # Tests pytest
examples/               # Exemples d'utilisation
```

### Conventions

- Le code suit les règles [ruff](https://docs.astral.sh/ruff/) (ligne max 100 caractères)
- Les tests utilisent `pytest` avec des fixtures partagées dans `conftest.py`
- Le fichier `_metadata.json` est toujours écrit avec des clés triées et un indent de 2 espaces pour des diffs Git stables
- PySpark est une dépendance optionnelle : les tests Spark sont marqués `@pytest.mark.spark`
