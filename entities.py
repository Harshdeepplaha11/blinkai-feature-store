from feast import Entity, ValueType

# Core entities for the payment system
user_entity = Entity(
    name="user_id",
    value_type=ValueType.INT64,
    description="Unique identifier for a user",
    join_keys=["user_id"],
)

account_entity = Entity(
    name="account_id",
    value_type=ValueType.INT64, 
    description="Unique identifier for an account",
    join_keys=["account_id"],
)

customer_entity = Entity(
    name="customer_id",
    value_type=ValueType.INT64,
    description="Unique identifier for a customer",
    join_keys=["customer_id"],
)

transaction_entity = Entity(
    name="transaction_id",
    value_type=ValueType.INT64,
    description="Unique identifier for a transaction",
    join_keys=["transaction_id"],
)

# Composite entities for complex relationships
user_account_entity = Entity(
    name="user_account_id",
    value_type=ValueType.STRING,
    description="Composite key for user-account relationships",
    join_keys=["user_id", "account_id"],
)
