"""Exemple d'utilisation de meta-parquet-doc avec Pandas."""

import pandas as pd

from meta_parquet_doc import write_dataset, read_dataset

# Créer un DataFrame de test
df = pd.DataFrame({
    "customer_id": [1, 2, 3],
    "full_name": ["Alice Smith", "Bob Jones", "Charlie Brown"],
    "email": ["alice@example.com", "bob@example.com", None],
    "purchase_amount": [99.99, 149.50, 25.00],
})

# Écrire le dataset avec documentation complète
meta_path = write_dataset(
    df,
    path="./output/customers.parquet",
    dataset_metadata={
        "description": "Historique des achats clients Q1 2025.",
        "owner": "analytics-team",
        "tags": ["finance", "quarterly"],
    },
    columns_metadata={
        "customer_id": {
            "description": "Identifiant unique du client.",
            "nullable": False,
            "pii": False,
        },
        "full_name": {
            "description": "Nom complet du client.",
            "nullable": False,
            "pii": True,
            "pii_category": "direct_identifier",
        },
        "email": {
            "description": "Adresse email du client.",
            "nullable": True,
            "pii": True,
            "pii_category": "contact_info",
        },
        "purchase_amount": {
            "description": "Montant total de l'achat en EUR.",
            "nullable": False,
            "pii": False,
        },
    },
    mode="strict",  # Lève une exception si la documentation est incomplète
)
print(f"Métadonnées écrites dans : {meta_path}")

# Relire le dataset avec ses métadonnées
df_loaded, metadata = read_dataset("./output/customers.parquet", engine="pandas")
print(f"\nDataset : {metadata.dataset.description}")
print(f"Owner : {metadata.dataset.owner}")
print(f"Colonnes documentées : {list(metadata.columns.keys())}")

# Afficher les colonnes PII
pii_cols = [name for name, col in metadata.columns.items() if col.pii]
print(f"Colonnes PII : {pii_cols}")
