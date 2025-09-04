# BlinkAI Feature Store

This is the feature repository for BlinkAI's machine learning platform, built on Feast.

## Project Structure

```
blinkai-feature-repo/
├── feature_store.yaml    # Feast configuration
├── features.py          # Feature definitions
├── entities.py          # Entity definitions  
├── data_sources.py      # Data source definitions
├── README.md           # This file
└── .gitignore          # Git ignore file
```

## Features

### User Transaction Features
- `total_transaction_amount`: Sum of all transactions for a user
- `transaction_count`: Number of transactions for a user
- `avg_transaction_amount`: Average transaction amount for a user
- `max_transaction_amount`: Maximum transaction amount for a user
- `min_transaction_amount`: Minimum transaction amount for a user

### Account Features
- `account_balance`: Current account balance
- `account_age_days`: Age of the account in days
- `risk_score`: Risk assessment score for the account
- `account_status`: Current status of the account

### User-Account Relationship Features
- `user_account_count`: Number of accounts per user
- `total_user_balance`: Total balance across all user accounts
- `avg_user_risk_score`: Average risk score across user accounts

## Entities

- `user_id`: User identifier
- `account_id`: Account identifier
- `customer_id`: Customer identifier
- `transaction_id`: Transaction identifier
- `user_account_id`: Composite user-account relationship

## Data Sources

- PostgreSQL sources for real-time data
- File sources for batch processing
- Integration with existing BlinkAI data pipeline

## Configuration

The feature store is configured to use:
- **Offline Store**: PostgreSQL (shared with MLflow)
- **Online Store**: Redis
- **Registry**: Azure Blob Storage
- **Provider**: Azure

## Usage

### Apply Features
```bash
feast apply
```

### Materialize Features
```bash
feast materialize
```

### Serve Features
```bash
feast serve
```

### Get Online Features
```python
from feast import FeatureStore

store = FeatureStore(repo_path=".")
features = store.get_online_features(
    features=[
        "user_transaction_features:total_transaction_amount",
        "user_transaction_features:transaction_count",
        "account_features:account_balance",
        "account_features:risk_score"
    ],
    entity_rows=[{"user_id": 123, "account_id": 456}]
).to_dict()
```

## Development

1. Clone this repository
2. Install Feast: `pip install feast[postgres,azure]`
3. Apply features: `feast apply`
4. Test locally: `feast serve`

## Deployment

This feature repository is deployed via the Feast operator in Kubernetes:
- Namespace: `feast`
- FeatureStore CR: `bliink-fs`
- External Access: LoadBalancer service
