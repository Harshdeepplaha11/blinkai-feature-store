"""
Transaction-related feature definitions.

This module defines features related to transaction behavior and patterns.
"""

from datetime import timedelta
from feast import Feature, FeatureView, ValueType

from ..entities import user_entity, account_entity
from ..data_sources import user_transaction_features_source, account_transaction_features_source

# User-level transaction features (from computed Spark tables)
user_transaction_features = FeatureView(
    name="user_transaction_features",
    entities=[user_entity],
    ttl=timedelta(days=30),
    source=user_transaction_features_source,
    features=[
        # Transaction volume features
        Feature(name="total_transaction_amount", dtype=ValueType.DOUBLE),
        Feature(name="transaction_count", dtype=ValueType.INT64),
        Feature(name="avg_transaction_amount", dtype=ValueType.DOUBLE),
        Feature(name="max_transaction_amount", dtype=ValueType.DOUBLE),
        Feature(name="min_transaction_amount", dtype=ValueType.DOUBLE),
        
        # P2P and Zelle features
        Feature(name="p2p_transaction_count", dtype=ValueType.INT64),
        Feature(name="zelle_transaction_count", dtype=ValueType.INT64),
        Feature(name="p2p_transaction_ratio", dtype=ValueType.DOUBLE),
        
        # Transaction diversity features
        Feature(name="unique_counterparties", dtype=ValueType.INT64),
        Feature(name="unique_countries", dtype=ValueType.INT64),
        Feature(name="unique_channels", dtype=ValueType.INT64),
        
        # Temporal features
        Feature(name="days_since_last_transaction", dtype=ValueType.INT64),
        Feature(name="transaction_frequency_days", dtype=ValueType.DOUBLE),
        
        # Risk indicators
        Feature(name="high_amount_transaction_count", dtype=ValueType.INT64),
        Feature(name="international_transaction_count", dtype=ValueType.INT64),
    ],
    tags={"team": "data-engineering", "domain": "payments", "tier": "production"},
)

# Account-level transaction features (from computed Spark tables)
account_transaction_features = FeatureView(
    name="account_transaction_features", 
    entities=[account_entity],
    ttl=timedelta(days=30),
    source=account_transaction_features_source,
    features=[
        # Account transaction volume
        Feature(name="account_total_transaction_amount", dtype=ValueType.DOUBLE),
        Feature(name="account_transaction_count", dtype=ValueType.INT64),
        Feature(name="account_avg_transaction_amount", dtype=ValueType.DOUBLE),
        
        # Account transaction patterns
        Feature(name="account_p2p_transaction_count", dtype=ValueType.INT64),
        Feature(name="account_zelle_transaction_count", dtype=ValueType.INT64),
        Feature(name="account_debit_transaction_count", dtype=ValueType.INT64),
        Feature(name="account_credit_transaction_count", dtype=ValueType.INT64),
        
        # Account risk indicators
        Feature(name="account_high_amount_transaction_count", dtype=ValueType.INT64),
        Feature(name="account_international_transaction_count", dtype=ValueType.INT64),
    ],
    tags={"team": "data-engineering", "domain": "payments", "tier": "production"},
)
