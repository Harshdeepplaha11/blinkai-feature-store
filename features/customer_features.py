"""
Customer-related feature definitions.

This module defines features related to customer characteristics and risk.
"""

from datetime import timedelta
from feast import Feature, FeatureView, ValueType

from ..entities import customer_entity
from ..data_sources import entities_source

# Customer risk features
customer_risk_features = FeatureView(
    name="customer_risk_features",
    entities=[customer_entity],
    ttl=timedelta(days=180),  # Risk scores change infrequently
    source=entities_source,
    features=[
        # Risk metrics
        Feature(name="customer_risk_score", dtype=ValueType.INT64),
        Feature(name="customer_risk_rating", dtype=ValueType.INT64),
        
        # Customer characteristics
        Feature(name="entity_type", dtype=ValueType.STRING),
        Feature(name="customer_entity_id", dtype=ValueType.STRING),
    ],
    tags={"team": "data-engineering", "domain": "customers", "tier": "production"},
)

# Customer segment features
customer_segment_features = FeatureView(
    name="customer_segment_features",
    entities=[customer_entity],
    ttl=timedelta(days=90),  # Segments may change over time
    source=entities_source,
    features=[
        # Customer segmentation
        Feature(name="customer_segment", dtype=ValueType.STRING),
        
        # Customer lifecycle
        Feature(name="customer_age_days", dtype=ValueType.INT64),
        Feature(name="days_since_customer_since", dtype=ValueType.INT64),
    ],
    tags={"team": "data-engineering", "domain": "customers", "tier": "production"},
)
