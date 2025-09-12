"""
Iceberg data source definitions.

This module defines data sources that connect to our Iceberg tables
in the medallion architecture (Bronze, Silver, Gold layers).
"""

import os
from feast import IcebergSource

# Get configuration from environment variables
ICEBERG_CATALOG = os.getenv("ICEBERG_CATALOG", "bliinkai_catalog")
ICEBERG_WAREHOUSE_PATH = os.getenv("ICEBERG_WAREHOUSE_PATH")
AZURE_STORAGE_ACCOUNT_NAME = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
AZURE_STORAGE_KEY = os.getenv("AZURE_STORAGE_KEY")

# Transaction data source from Silver layer (raw data)
transactions_source = IcebergSource(
    name="transactions_source",
    table=f"{ICEBERG_CATALOG}.default.transactions_silver",
    timestamp_field="transaction_date",
    catalog=ICEBERG_CATALOG,
    warehouse_path=ICEBERG_WAREHOUSE_PATH,
    storage_options={
        "account_name": AZURE_STORAGE_ACCOUNT_NAME,
        "account_key": AZURE_STORAGE_KEY,
    },
)

# Computed feature tables (from Spark)
user_transaction_features_source = IcebergSource(
    name="user_transaction_features_source",
    table=f"{ICEBERG_CATALOG}.default.user_transaction_features",
    timestamp_field="feature_date",
    catalog=ICEBERG_CATALOG,
    warehouse_path=ICEBERG_WAREHOUSE_PATH,
    storage_options={
        "account_name": AZURE_STORAGE_ACCOUNT_NAME,
        "account_key": AZURE_STORAGE_KEY,
    },
)

account_transaction_features_source = IcebergSource(
    name="account_transaction_features_source",
    table=f"{ICEBERG_CATALOG}.default.account_transaction_features",
    timestamp_field="feature_date",
    catalog=ICEBERG_CATALOG,
    warehouse_path=ICEBERG_WAREHOUSE_PATH,
    storage_options={
        "account_name": AZURE_STORAGE_ACCOUNT_NAME,
        "account_key": AZURE_STORAGE_KEY,
    },
)

customer_features_source = IcebergSource(
    name="customer_features_source",
    table=f"{ICEBERG_CATALOG}.default.customer_features",
    timestamp_field="feature_date",
    catalog=ICEBERG_CATALOG,
    warehouse_path=ICEBERG_WAREHOUSE_PATH,
    storage_options={
        "account_name": AZURE_STORAGE_ACCOUNT_NAME,
        "account_key": AZURE_STORAGE_KEY,
    },
)

# Account data source from Silver layer
accounts_source = IcebergSource(
    name="accounts_source",
    table=f"{ICEBERG_CATALOG}.default.accounts_silver",
    timestamp_field="open_date",
    catalog=ICEBERG_CATALOG,
    warehouse_path=ICEBERG_WAREHOUSE_PATH,
    storage_options={
        "account_name": AZURE_STORAGE_ACCOUNT_NAME,
        "account_key": AZURE_STORAGE_KEY,
    },
)

# Entity data source from Silver layer
entities_source = IcebergSource(
    name="entities_source",
    table=f"{ICEBERG_CATALOG}.default.entities_silver",
    timestamp_field="customer_since",
    catalog=ICEBERG_CATALOG,
    warehouse_path=ICEBERG_WAREHOUSE_PATH,
    storage_options={
        "account_name": AZURE_STORAGE_ACCOUNT_NAME,
        "account_key": AZURE_STORAGE_KEY,
    },
)
