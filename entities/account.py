"""
Account entity definitions.
"""

from feast import Entity, ValueType

# Core account entity
account_entity = Entity(
    name="account_id",
    value_type=ValueType.INT64,
    description="Unique identifier for an account",
    join_keys=["account_id"],
)
