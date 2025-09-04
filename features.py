from datetime import timedelta
from feast import Feature, FeatureView, ValueType
from feast.infra.offline_stores.contrib.postgres_offline_store.postgres import PostgreSQLOfflineStoreConfig
from feast.infra.offline_stores.contrib.postgres_offline_store.postgres_source import PostgreSQLSource

# Import entities from entities.py
from entities import user_entity, account_entity

# Define data sources
user_transactions_source = PostgreSQLSource(
    name="user_transactions_source",
    query="SELECT user_id, account_id, transaction_amount, transaction_date FROM transactions",
    timestamp_field="transaction_date",
)

account_data_source = PostgreSQLSource(
    name="account_data_source",
    query="SELECT account_id, account_balance, account_created_date, risk_score FROM accounts",
    timestamp_field="account_created_date",
)

# Define feature views
user_transaction_features = FeatureView(
    name="user_transaction_features",
    entities=[user_entity],
    ttl=timedelta(days=30),
    source=user_transactions_source,
    tags={"team": "data-engineering", "domain": "payments"},
)

account_features = FeatureView(
    name="account_features",
    entities=[account_entity],
    ttl=timedelta(days=90),
    source=account_data_source,
    tags={"team": "data-engineering", "domain": "accounts"},
)

# User account relationship features
# Note: Multiple entities not supported in Feast 0.52.0
# user_account_features = FeatureView(
#     name="user_account_features",
#     entities=["user_id", "account_id"],
#     ttl=timedelta(days=7),
#     features=[
#         Feature(name="user_account_count", dtype=ValueType.INT64),
#         Feature(name="total_user_balance", dtype=ValueType.FLOAT),
#         Feature(name="avg_user_risk_score", dtype=ValueType.FLOAT),
#     ],
#     source=user_transactions_source,
#     tags={"team": "data-engineering", "domain": "relationships"},
# )
