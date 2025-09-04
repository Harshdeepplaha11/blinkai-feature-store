from datetime import timedelta
from feast import Entity, Feature, FeatureView, ValueType
from feast.data_source import DataSource
from feast.infra.offline_stores.contrib.postgres_offline_store.postgres import PostgreSQLOfflineStoreConfig
from feast.infra.offline_stores.contrib.postgres_offline_store.postgres_source import PostgreSQLSource

# Define entities
user_entity = Entity(
    name="user_id",
    value_type=ValueType.INT64,
    description="User identifier",
)

account_entity = Entity(
    name="account_id", 
    value_type=ValueType.INT64,
    description="Account identifier",
)

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
    entities=["user_id"],
    ttl=timedelta(days=30),
    features=[
        Feature(name="total_transaction_amount", dtype=ValueType.FLOAT),
        Feature(name="transaction_count", dtype=ValueType.INT64),
        Feature(name="avg_transaction_amount", dtype=ValueType.FLOAT),
        Feature(name="max_transaction_amount", dtype=ValueType.FLOAT),
        Feature(name="min_transaction_amount", dtype=ValueType.FLOAT),
    ],
    source=user_transactions_source,
    tags={"team": "data-engineering", "domain": "payments"},
)

account_features = FeatureView(
    name="account_features",
    entities=["account_id"],
    ttl=timedelta(days=90),
    features=[
        Feature(name="account_balance", dtype=ValueType.FLOAT),
        Feature(name="account_age_days", dtype=ValueType.INT64),
        Feature(name="risk_score", dtype=ValueType.FLOAT),
        Feature(name="account_status", dtype=ValueType.STRING),
    ],
    source=account_data_source,
    tags={"team": "data-engineering", "domain": "accounts"},
)

# User account relationship features
user_account_features = FeatureView(
    name="user_account_features",
    entities=["user_id", "account_id"],
    ttl=timedelta(days=7),
    features=[
        Feature(name="user_account_count", dtype=ValueType.INT64),
        Feature(name="total_user_balance", dtype=ValueType.FLOAT),
        Feature(name="avg_user_risk_score", dtype=ValueType.FLOAT),
    ],
    source=user_transactions_source,
    tags={"team": "data-engineering", "domain": "relationships"},
)
