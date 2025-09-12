# BlinkAI Feature Store

This is the feature repository for BlinkAI's machine learning platform, built on Feast with a proper enterprise-grade structure.

## 🏗️ Project Structure

```
blinkai-feature-store/
│
├── feature_store.yaml           # Main Feast configuration
├── .feastignore                 # Files to ignore during feast apply
├── requirements.txt             # Python dependencies
├── README.md                    # This documentation
│
├── entities/                    # Entity definitions by domain
│   ├── __init__.py
│   ├── customer.py             # Customer entities
│   ├── account.py              # Account entities
│   ├── transaction.py          # Transaction entities
│   └── user.py                 # User entities
│
├── data_sources/               # Data source configurations
│   ├── __init__.py
│   └── iceberg_sources.py     # Iceberg table definitions
│
├── features/                   # Feature definitions by business domain
│   ├── __init__.py
│   ├── transaction_features.py # Transaction-related features
│   ├── account_features.py     # Account-related features
│   └── customer_features.py    # Customer-related features
│
├── scripts/                    # Utility scripts
│   ├── materialize.py         # Materialization jobs
│   └── validate_features.py   # Feature validation
│
├── transformations/            # On-demand feature transformations
├── validation/                 # Data quality & validation
├── tests/                      # Unit and integration tests
└── notebooks/                  # Jupyter notebooks for exploration
```

## 🎯 Features by Domain

### Transaction Features
- **Volume Features**: `total_transaction_amount`, `transaction_count`, `avg_transaction_amount`
- **P2P Features**: `p2p_transaction_count`, `zelle_transaction_count`, `p2p_transaction_ratio`
- **Diversity Features**: `unique_counterparties`, `unique_countries`, `unique_channels`
- **Temporal Features**: `days_since_last_transaction`, `transaction_frequency_days`
- **Risk Indicators**: `high_amount_transaction_count`, `international_transaction_count`

### Account Features
- **Balance Features**: `account_balance`, `account_currency`, `account_type`, `account_status`
- **Characteristics**: `is_joint_account`, `is_zelle_enabled`, `account_holder_name`
- **Risk Features**: `account_risk_score`, `risk_rating`, `account_age_days`

### Customer Features
- **Risk Features**: `customer_risk_score`, `customer_risk_rating`, `entity_type`
- **Segment Features**: `customer_segment`, `customer_age_days`

## 🏛️ Architecture

### Data Flow
```
Iceberg Tables (Silver Layer) on ADLS2
    ↓
Iceberg Data Sources
    ↓
Feature Views (Grouped Features)
    ↓
Online Store (Redis) ← Real-time serving
Offline Store (Iceberg on ADLS2) ← Training data
```

### Configuration
- **Offline Store**: Iceberg on Azure Data Lake Storage Gen2
- **Online Store**: Redis
- **Registry**: PostgreSQL
- **Provider**: Azure
- **Data Sources**: Iceberg tables (transactions_silver, accounts_silver, entities_silver)
- **Storage**: Configurable (ADLS2 or S3)

## 🚀 Quick Start

### 1. Install Dependencies
```bash
pip install -r requirements.txt
```

### 2. Set Environment Variables
```bash
# For Azure ADLS2
export AZURE_STORAGE_KEY="your_storage_key"

# For AWS S3 (if using)
export AWS_ACCESS_KEY_ID="your_access_key"
export AWS_SECRET_ACCESS_KEY="your_secret_key"
```

### 3. Apply Feature Definitions
```bash
feast apply
```

### 4. Validate Setup
```bash
python scripts/validate_features.py
```

### 5. Materialize Features (Optional)
```bash
python scripts/materialize.py --start-date 2024-01-01 --end-date 2024-01-31
```

## 📊 Usage Examples

### Get Online Features
```python
from feast import FeatureStore

store = FeatureStore(repo_path=".")

# Get features for a specific user
features = store.get_online_features(
    features=[
        "user_transaction_features:total_transaction_amount",
        "user_transaction_features:transaction_count",
        "user_transaction_features:p2p_transaction_count",
        "account_features:account_balance",
        "account_features:account_risk_score"
    ],
    entity_rows=[{"user_id": 12345, "account_id": 67890}]
).to_dict()

print(features)
```

### Get Offline Features for Training
```python
# Get historical features for model training
training_df = store.get_historical_features(
    entity_df=entity_df,  # DataFrame with entity keys and timestamps
    features=[
        "user_transaction_features:total_transaction_amount",
        "user_transaction_features:transaction_count",
        "account_features:account_risk_score",
        "customer_features:customer_risk_score"
    ]
).to_df()
```

## 🔧 Development

### Adding New Features
1. Define the feature in the appropriate domain file (e.g., `features/transaction_features.py`)
2. Update the `__init__.py` file to export the new feature view
3. Run `feast apply` to register the changes
4. Test with `python scripts/validate_features.py`

### Adding New Entities
1. Create entity definition in `entities/` directory
2. Update `entities/__init__.py`
3. Use the entity in your feature views

### Adding New Data Sources
1. Define data source in `data_sources/iceberg_sources.py`
2. Update `data_sources/__init__.py`
3. Use the data source in your feature views

## 🧪 Testing

### Validate Feature Store
```bash
python scripts/validate_features.py
```

### Test Feature Retrieval
```python
# Test online feature retrieval
from feast import FeatureStore
store = FeatureStore(repo_path=".")
features = store.get_online_features(
    features=["user_transaction_features:total_transaction_amount"],
    entity_rows=[{"user_id": 12345}]
)
```

## 📈 Monitoring

### Feature Freshness
Monitor how recent your features are:
```python
store.get_feature_service("user_transaction_features").get_feature_view().ttl
```

### Materialization Status
Check materialization logs and Redis memory usage.

## 🚀 Deployment

This feature repository is deployed via the Feast operator in Kubernetes:
- **Namespace**: `feast`
- **FeatureStore CR**: `bliink-fs`
- **External Access**: LoadBalancer service

## 📚 Best Practices

1. **Feature Naming**: Use consistent naming: `{domain}_{metric}_{aggregation}_{window}`
2. **TTL Management**: Set appropriate TTLs based on feature update frequency
3. **Data Quality**: Implement validation for feature values
4. **Monitoring**: Track feature freshness and serving latency
5. **Version Control**: Use semantic versioning for feature definitions

## 🤝 Contributing

1. Follow the established folder structure
2. Add tests for new features
3. Update documentation
4. Use consistent naming conventions
5. Validate changes with `python scripts/validate_features.py`
