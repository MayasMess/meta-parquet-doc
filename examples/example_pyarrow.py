"""Exemple d'utilisation de meta-parquet-doc avec PyArrow."""

import pyarrow as pa

from meta_parquet_doc import write_dataset, read_dataset, validate_dataset

# Créer une table PyArrow
table = pa.table({
    "user_id": [101, 102, 103, 104],
    "region": ["eu", "us", "eu", "us"],
    "score": [85.5, 92.0, 78.3, 91.1],
})

# Écrire en mode partitionné par région
meta_path = write_dataset(
    table,
    path="./output/scores_partitioned",
    dataset_metadata={
        "description": "Scores utilisateurs partitionnés par région.",
        "owner": "data-platform",
    },
    columns_metadata={
        "user_id": {
            "description": "Identifiant unique de l'utilisateur.",
            "nullable": False,
            "pii": True,
            "pii_category": "direct_identifier",
        },
        "region": {
            "description": "Région géographique (eu/us).",
            "nullable": False,
            "pii": False,
        },
        "score": {
            "description": "Score de performance (0-100).",
            "nullable": False,
            "pii": False,
        },
    },
    mode="strict",
    partition_cols=["region"],  # Argument passé à PyArrow
)
print(f"Métadonnées écrites dans : {meta_path}")

# Valider le dataset sans charger toutes les données
result = validate_dataset("./output/scores_partitioned")
print(f"\nValidation : {'OK' if result.is_valid else 'ERREURS'}")
if result.warnings:
    print(f"Warnings : {result.warnings}")

# Relire avec PyArrow
table_loaded, metadata = read_dataset(
    "./output/scores_partitioned", engine="pyarrow"
)
print(f"\nLignes lues : {table_loaded.num_rows}")
print(f"Schéma : {table_loaded.schema.names}")
