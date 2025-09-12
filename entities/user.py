"""
User entity definitions.
"""

from feast import Entity, ValueType

# Core user entity
user_entity = Entity(
    name="user_id",
    value_type=ValueType.INT64,
    description="Unique identifier for a user",
    join_keys=["user_id"],
)
