"""Exemple d'utilisation de meta-parquet-doc avec PySpark.

Nécessite l'installation de PySpark :
    pip install meta-parquet-doc[spark]
"""

from pyspark.sql import SparkSession

from meta_parquet_doc import write_dataset, read_dataset

# Initialiser une session Spark
spark = SparkSession.builder \
    .master("local[*]") \
    .appName("meta-parquet-doc-example") \
    .getOrCreate()

# Créer un DataFrame PySpark
data = [
    (1, "Alice", "alice@example.com", 1200.0),
    (2, "Bob", "bob@example.com", 850.0),
    (3, "Charlie", None, 2100.0),
]
df = spark.createDataFrame(data, ["employee_id", "name", "email", "salary"])

# Écrire le dataset avec documentation
meta_path = write_dataset(
    df,
    path="./output/employees.parquet",
    dataset_metadata={
        "description": "Données employés pour le calcul de paie.",
        "owner": "rh-team",
        "tags": ["rh", "confidentiel"],
    },
    columns_metadata={
        "employee_id": {
            "description": "Identifiant unique de l'employé.",
            "nullable": False,
            "pii": False,
        },
        "name": {
            "description": "Nom complet de l'employé.",
            "nullable": False,
            "pii": True,
            "pii_category": "direct_identifier",
        },
        "email": {
            "description": "Email professionnel.",
            "nullable": True,
            "pii": True,
            "pii_category": "contact_info",
        },
        "salary": {
            "description": "Salaire mensuel brut en EUR.",
            "nullable": False,
            "pii": True,
            "pii_category": "financial",
        },
    },
    mode="strict",
)
print(f"Métadonnées écrites dans : {meta_path}")

# Relire avec PySpark
df_loaded, metadata = read_dataset(
    "./output/employees.parquet",
    engine="pyspark",
    spark=spark,  # Passer la session Spark existante
)
print(f"\nDataset : {metadata.dataset.description}")
df_loaded.show()

# Lister les colonnes sensibles
pii_cols = [name for name, col in metadata.columns.items() if col.pii]
print(f"Colonnes PII à protéger : {pii_cols}")

spark.stop()
