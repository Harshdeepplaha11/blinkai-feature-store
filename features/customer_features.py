"""
Customer feature definitions for BlinkAI Feature Store.

This module defines customer-related features computed from Spark jobs
and stored in Iceberg tables for Feast consumption.
"""

from datetime import timedelta
from feast import Feature, FeatureView, ValueType

from ..entities import customer_entity
from ..data_sources import customer_features_source

# Customer features (from computed Spark tables)
customer_features = FeatureView(
    name="customer_features",
    entities=[customer_entity],
    ttl=timedelta(days=30),
    source=customer_features_source,
    features=[
        # Customer risk features
        Feature(name="customer_risk_score", dtype=ValueType.DOUBLE),
        Feature(name="customer_risk_rating", dtype=ValueType.INT64),
        Feature(name="customer_segment", dtype=ValueType.STRING),
        
        # Customer activity features
        Feature(name="total_accounts", dtype=ValueType.INT64),
        Feature(name="active_accounts", dtype=ValueType.INT64),
        Feature(name="customer_age_days", dtype=ValueType.INT64),
        
        # Customer financial features
        Feature(name="total_balance", dtype=ValueType.DOUBLE),
        Feature(name="avg_account_balance", dtype=ValueType.DOUBLE),
        Feature(name="max_account_balance", dtype=ValueType.DOUBLE),
    ],
    tags={"team": "data-engineering", "domain": "customers", "tier": "production"},
)