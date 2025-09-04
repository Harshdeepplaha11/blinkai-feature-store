from feast import FileSource
from feast.data_format import ParquetFormat
from feast.infra.offline_stores.contrib.postgres_offline_store.postgres_source import PostgreSQLSource

# PostgreSQL data sources for existing tables
transactions_source = PostgreSQLSource(
    name="transactions_source",
    query="SELECT * FROM transactions",
    timestamp_field="transaction_date",
)

accounts_source = PostgreSQLSource(
    name="accounts_source", 
    query="SELECT * FROM accounts",
    timestamp_field="account_created_date",
)

customers_source = PostgreSQLSource(
    name="customers_source",
    query="SELECT * FROM customers", 
    timestamp_field="created_date",
)

# File-based data sources for batch processing
transactions_file_source = FileSource(
    name="transactions_file_source",
    path="data/transactions.parquet",
    timestamp_field="transaction_date",
    file_format=ParquetFormat(),
)

accounts_file_source = FileSource(
    name="accounts_file_source",
    path="data/accounts.parquet", 
    timestamp_field="account_created_date",
    file_format=ParquetFormat(),
)
