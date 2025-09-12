# WARP.md

This file provides guidance to WARP (warp.dev) when working with code in this repository.

## Common Commands

### Setup / Build
- Install dependencies
  ```bash
  pip install -r requirements.txt
  ```
- Copy environment template (or set variables manually)
  ```bash
  cp env.template .env  # then edit with real values
  ```

### Lint & Static Analysis
- Format code
  ```bash
  black .
  ```
- Lint
  ```bash
  flake8
  ```
- Type-check
  ```bash
  mypy .
  ```

### Tests
- Run full test suite
  ```bash
  pytest
  ```
- Run a single test module / case
  ```bash
  pytest tests/<module>::<TestClass>::<test_function>
  ```

### Feast Operations
- Register (apply) feature definitions
  ```bash
  feast apply
  ```
- Validate local repository configuration
  ```bash
  python scripts/validate_features.py
  ```

### Feature Infrastructure Management

#### Create Feature Tables (Iceberg)
- Create all feature tables with optimized schemas
  ```bash
  python scripts/create_feature_tables.py
  ```

#### Offline Feature Computation (PySpark)
- All features, explicit date range
  ```bash
  python scripts/compute_features.py --start-date 2025-01-01 --end-date 2025-01-31
  ```
- Selected feature types only (transaction example)
  ```bash
  python scripts/compute_features.py --start-date 2025-01-01 --end-date 2025-01-31 --feature-types transaction
  ```
- Standalone Spark computation (with env vars)
  ```bash
  python scripts/compute_features_spark.py
  ```

#### Full Deployment & Testing
- Deploy complete pipeline with Kubernetes jobs
  ```bash
  python scripts/deploy_and_test.py
  ```

## High-Level Architecture

1. **Feast Repository**
   - `feature_store.yaml` configures providers (Azure), registry (PostgreSQL), online store (Redis) and offline store (Iceberg).
   - Entities (`entities/`), data sources (`data_sources/`), and feature definitions (`features/`) are Python modules imported by Feast.

2. **Data Flow**
   Iceberg silver tables → PySpark transformation (`scripts/compute_*`) → Iceberg feature tables → Feast `feast apply` registers FeatureViews →
   • Online store (Redis) for real-time serving
   • Offline store (Iceberg) for training datasets

3. **Computation Layer**
   `scripts/compute_features.py` (wrapper) and `scripts/compute_features_spark.py` (stand-alone) launch Spark jobs.
   - Transformation logic lives in `transformations/` (e.g. `TransactionFeatureComputer`).
   - Environment variables such as `ICEBERG_CATALOG`, `ICEBERG_WAREHOUSE_PATH`, `AZURE_STORAGE_ACCOUNT_NAME/KEY` must be set for Spark & Feast.

4. **Kubernetes Jobs**
   `k8s/compute-features-job.yaml` and `k8s/create-feature-tables-job.yaml` show how feature computation is executed in-cluster.

5. **CI/Automation Hooks**
   No dedicated CI scripts in repo; rely on above commands. Add tests under `tests/` to guard feature logic.

6. **Important Rules & Docs**
   - README.md already captures data flow, environment variables and best practices—consult when extending entities or features.
   - No CLAUDE/Copilot rules detected.

## Quick Reference – Required Env Vars
```
FEAST_REGISTRY_PATH, REDIS_CONNECTION_STRING,
ICEBERG_CATALOG, ICEBERG_CATALOG_TYPE, ICEBERG_WAREHOUSE_PATH,
AZURE_STORAGE_ACCOUNT_NAME, AZURE_STORAGE_KEY
```
Set these (or edit `.env`) before running Feast or Spark jobs.

