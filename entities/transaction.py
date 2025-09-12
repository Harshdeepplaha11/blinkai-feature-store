"""
Transaction entity definitions.
"""

from feast import Entity, ValueType

# Core transaction entity
transaction_entity = Entity(
    name="transaction_id",
    value_type=ValueType.INT64,
    description="Unique identifier for a transaction",
    join_keys=["transaction_id"],
)
