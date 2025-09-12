"""
Iceberg data source definitions.

This module defines data sources that connect to our Iceberg tables
in the medallion architecture (Bronze, Silver, Gold layers).
"""

from feast import IcebergSource

# Transaction data source from Silver layer
transactions_source = IcebergSource(
    name="transactions_source",
    table="bliinkai_catalog.default.transactions_silver",
    timestamp_field="transaction_date",
    catalog="bliinkai_catalog",
    warehouse_path="abfss://iceberg-warehouse@blinkaidatalake.dfs.core.windows.net/iceberg/warehouse",
    storage_options={
        "account_name": "blinkaidatalake",
        "account_key": "${AZURE_STORAGE_KEY}",  # Use environment variable
    },
)

# Account data source from Silver layer
accounts_source = IcebergSource(
    name="accounts_source",
    table="bliinkai_catalog.default.accounts_silver",
    timestamp_field="open_date",
    catalog="bliinkai_catalog",
    warehouse_path="abfss://iceberg-warehouse@blinkaidatalake.dfs.core.windows.net/iceberg/warehouse",
    storage_options={
        "account_name": "blinkaidatalake",
        "account_key": "${AZURE_STORAGE_KEY}",  # Use environment variable
    },
)

# Entity data source from Silver layer
entities_source = IcebergSource(
    name="entities_source",
    table="bliinkai_catalog.default.entities_silver",
    timestamp_field="customer_since",
    catalog="bliinkai_catalog",
    warehouse_path="abfss://iceberg-warehouse@blinkaidatalake.dfs.core.windows.net/iceberg/warehouse",
    storage_options={
        "account_name": "blinkaidatalake",
        "account_key": "${AZURE_STORAGE_KEY}",  # Use environment variable
    },
)
