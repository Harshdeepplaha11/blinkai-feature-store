"""
Account-related feature definitions.

This module defines features related to account characteristics and risk.
"""

from datetime import timedelta
from feast import Feature, FeatureView, ValueType

from ..entities import account_entity
from ..data_sources import accounts_source

# Account balance and financial features
account_balance_features = FeatureView(
    name="account_balance_features",
    entities=[account_entity],
    ttl=timedelta(days=7),  # Balance changes frequently
    source=accounts_source,
    features=[
        # Current account state
        Feature(name="account_balance", dtype=ValueType.DOUBLE),
        Feature(name="account_currency", dtype=ValueType.STRING),
        Feature(name="account_type", dtype=ValueType.STRING),
        Feature(name="account_status", dtype=ValueType.STRING),
        
        # Account characteristics
        Feature(name="is_joint_account", dtype=ValueType.STRING),
        Feature(name="is_zelle_enabled", dtype=ValueType.STRING),
        Feature(name="account_holder_name", dtype=ValueType.STRING),
        Feature(name="branch_id", dtype=ValueType.STRING),
        Feature(name="product_identifier", dtype=ValueType.STRING),
    ],
    tags={"team": "data-engineering", "domain": "accounts", "tier": "production"},
)

# Account risk and age features
account_risk_features = FeatureView(
    name="account_risk_features",
    entities=[account_entity],
    ttl=timedelta(days=90),  # Risk scores change less frequently
    source=accounts_source,
    features=[
        # Risk metrics
        Feature(name="account_risk_score", dtype=ValueType.DOUBLE),
        Feature(name="risk_rating", dtype=ValueType.INT64),
        
        # Account age and lifecycle
        Feature(name="account_age_days", dtype=ValueType.INT64),
        Feature(name="days_since_account_opened", dtype=ValueType.INT64),
        
        # Account relationships
        Feature(name="entity_id", dtype=ValueType.INT64),
        Feature(name="customer_account_id", dtype=ValueType.STRING),
    ],
    tags={"team": "data-engineering", "domain": "accounts", "tier": "production"},
)
