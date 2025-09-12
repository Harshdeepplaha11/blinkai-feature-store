"""
Customer entity definitions.
"""

from feast import Entity, ValueType

# Core customer entity
customer_entity = Entity(
    name="customer_id",
    value_type=ValueType.INT64,
    description="Unique identifier for a customer",
    join_keys=["customer_id"],
)
