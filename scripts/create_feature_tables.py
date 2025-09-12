#!/usr/bin/env python3
"""
Create Iceberg tables for computed features.

This script creates the feature tables that will store computed features
from Spark jobs for consumption by Feast.
"""

import sys
import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, TimestampType, DateType

# Add the parent directory to the path to import modules
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

def create_spark_session():
    """Create Spark session with Iceberg configuration."""
    
    # Get environment variables
    iceberg_catalog = os.getenv("ICEBERG_CATALOG", "bliinkai_catalog")
    iceberg_warehouse_path = os.getenv("ICEBERG_WAREHOUSE_PATH")
    azure_storage_account = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
    azure_storage_key = os.getenv("AZURE_STORAGE_KEY")
    
    if not all([iceberg_warehouse_path, azure_storage_account, azure_storage_key]):
        raise ValueError("Missing required environment variables: ICEBERG_WAREHOUSE_PATH, AZURE_STORAGE_ACCOUNT_NAME, AZURE_STORAGE_KEY")
    
    spark = SparkSession.builder \
        .appName("CreateFeatureTables") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config(f"spark.sql.catalog.{iceberg_catalog}", "org.apache.iceberg.spark.SparkCatalog") \
        .config(f"spark.sql.catalog.{iceberg_catalog}.type", "rest") \
        .config(f"spark.sql.catalog.{iceberg_catalog}.uri", "http://iceberg-rest-simple.iceberg.svc.cluster.local:9001") \
        .config(f"spark.sql.catalog.{iceberg_catalog}.warehouse", iceberg_warehouse_path) \
        .config(f"spark.sql.catalog.{iceberg_catalog}.io-impl", "org.apache.iceberg.azure.adlsv2.ADLSFileIO") \
        .config(f"spark.sql.catalog.{iceberg_catalog}.io-azure-account", azure_storage_account) \
        .config(f"spark.sql.catalog.{iceberg_catalog}.io-azure-container", "iceberg-warehouse") \
        .config("spark.hadoop.fs.azure.account.auth.type.blinkaidatalake.dfs.core.windows.net", "OAuth") \
        .config("spark.hadoop.fs.azure.account.oauth.provider.type.blinkaidatalake.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider") \
        .config("spark.hadoop.fs.azure.account.oauth2.client.id.blinkaidatalake.dfs.core.windows.net", "2e2b3dfa-9e95-49da-a595-5da483dcf6f0") \
        .config("spark.hadoop.fs.azure.account.oauth2.client.endpoint.blinkaidatalake.dfs.core.windows.net", "https://login.microsoftonline.com/f586ff7d-466f-461e-8acc-d7dfbca2c7b1/oauth2/token") \
        .getOrCreate()
    
    return spark

def create_user_transaction_features_table(spark, catalog_name):
    """Create user transaction features table."""
    
    print("🔄 Creating user_transaction_features table...")
    
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {catalog_name}.default.user_transaction_features (
        user_id STRING NOT NULL,
        feature_date DATE NOT NULL,
        
        -- Transaction volume features
        total_transaction_amount DOUBLE,
        transaction_count BIGINT,
        avg_transaction_amount DOUBLE,
        max_transaction_amount DOUBLE,
        min_transaction_amount DOUBLE,
        
        -- P2P and Zelle features
        p2p_transaction_count BIGINT,
        zelle_transaction_count BIGINT,
        p2p_transaction_ratio DOUBLE,
        
        -- Diversity features
        unique_counterparties BIGINT,
        unique_countries BIGINT,
        unique_channels BIGINT,
        
        -- Temporal features
        days_since_last_transaction BIGINT,
        transaction_frequency_days DOUBLE,
        
        -- Risk indicators
        high_amount_transaction_count BIGINT,
        international_transaction_count BIGINT,
        
        -- Metadata
        computed_at TIMESTAMP NOT NULL
    ) USING ICEBERG
    PARTITIONED BY (feature_date)
    TBLPROPERTIES (
        'write.format.default' = 'parquet',
        'write.metadata.delete-after-commit.enabled' = 'true',
        'write.metadata.previous-versions-max' = '10',
        'write.parquet.bloom-filter-enabled.column' = 'user_id',
        'write.parquet.bloom-filter-max-bytes' = '1048576',
        'write.parquet.compression' = 'snappy',
        'write.parquet.row-group-size-bytes' = '134217728'
    )
    """
    
    spark.sql(create_table_sql)
    print("✅ user_transaction_features table created successfully!")

def create_account_transaction_features_table(spark, catalog_name):
    """Create account transaction features table."""
    
    print("🔄 Creating account_transaction_features table...")
    
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {catalog_name}.default.account_transaction_features (
        account_id STRING NOT NULL,
        feature_date DATE NOT NULL,
        
        -- Account transaction volume
        account_total_transaction_amount DOUBLE,
        account_transaction_count BIGINT,
        account_avg_transaction_amount DOUBLE,
        
        -- Account transaction patterns
        account_p2p_transaction_count BIGINT,
        account_zelle_transaction_count BIGINT,
        account_debit_transaction_count BIGINT,
        account_credit_transaction_count BIGINT,
        
        -- Account risk indicators
        account_high_amount_transaction_count BIGINT,
        account_international_transaction_count BIGINT,
        
        -- Metadata
        computed_at TIMESTAMP NOT NULL
    ) USING ICEBERG
    PARTITIONED BY (feature_date)
    TBLPROPERTIES (
        'write.format.default' = 'parquet',
        'write.metadata.delete-after-commit.enabled' = 'true',
        'write.metadata.previous-versions-max' = '10',
        'write.parquet.bloom-filter-enabled.column' = 'account_id',
        'write.parquet.bloom-filter-max-bytes' = '1048576',
        'write.parquet.compression' = 'snappy',
        'write.parquet.row-group-size-bytes' = '134217728'
    )
    """
    
    spark.sql(create_table_sql)
    print("✅ account_transaction_features table created successfully!")

def create_customer_features_table(spark, catalog_name):
    """Create customer features table."""
    
    print("🔄 Creating customer_features table...")
    
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {catalog_name}.default.customer_features (
        customer_id STRING NOT NULL,
        feature_date DATE NOT NULL,
        
        -- Customer risk features
        customer_risk_score DOUBLE,
        customer_risk_rating BIGINT,
        customer_segment STRING,
        
        -- Customer activity features
        total_accounts BIGINT,
        active_accounts BIGINT,
        customer_age_days BIGINT,
        
        -- Customer financial features
        total_balance DOUBLE,
        avg_account_balance DOUBLE,
        max_account_balance DOUBLE,
        
        -- Metadata
        computed_at TIMESTAMP NOT NULL
    ) USING ICEBERG
    PARTITIONED BY (feature_date)
    TBLPROPERTIES (
        'write.format.default' = 'parquet',
        'write.metadata.delete-after-commit.enabled' = 'true',
        'write.metadata.previous-versions-max' = '10',
        'write.parquet.bloom-filter-enabled.column' = 'customer_id',
        'write.parquet.bloom-filter-max-bytes' = '1048576',
        'write.parquet.compression' = 'snappy',
        'write.parquet.row-group-size-bytes' = '134217728'
    )
    """
    
    spark.sql(create_table_sql)
    print("✅ customer_features table created successfully!")

def verify_tables(spark, catalog_name):
    """Verify that all feature tables were created successfully."""
    
    print("\n🔍 Verifying feature tables...")
    
    # List all tables in the catalog
    tables_df = spark.sql(f"SHOW TABLES IN {catalog_name}.default")
    tables = [row.tableName for row in tables_df.collect()]
    
    feature_tables = [
        "user_transaction_features",
        "account_transaction_features", 
        "customer_features"
    ]
    
    print(f"\n📋 Tables in {catalog_name}.default:")
    for table in tables:
        if table in feature_tables:
            print(f"✅ {table}")
        else:
            print(f"   {table}")
    
    # Check if all feature tables exist
    missing_tables = [table for table in feature_tables if table not in tables]
    if missing_tables:
        print(f"\n❌ Missing tables: {missing_tables}")
        return False
    else:
        print(f"\n🎉 All {len(feature_tables)} feature tables created successfully!")
        return True

def main():
    """Main function to create feature tables."""
    
    print("🚀 Creating Iceberg tables for computed features...")
    print("=" * 60)
    
    try:
        # Create Spark session
        spark = create_spark_session()
        catalog_name = os.getenv("ICEBERG_CATALOG", "bliinkai_catalog")
        
        print(f"📊 Using catalog: {catalog_name}")
        print(f"📁 Warehouse path: {os.getenv('ICEBERG_WAREHOUSE_PATH')}")
        
        # Create feature tables
        create_user_transaction_features_table(spark, catalog_name)
        create_account_transaction_features_table(spark, catalog_name)
        create_customer_features_table(spark, catalog_name)
        
        # Verify tables
        success = verify_tables(spark, catalog_name)
        
        if success:
            print("\n🎉 Feature tables creation completed successfully!")
            print("\n📋 Next steps:")
            print("1. Run feature computation jobs to populate the tables")
            print("2. Update Feast data sources to point to these tables")
            print("3. Test feature serving through Feast")
        else:
            print("\n❌ Some tables failed to create. Check the logs above.")
            return 1
            
    except Exception as e:
        print(f"\n❌ Error creating feature tables: {e}")
        return 1
    finally:
        if 'spark' in locals():
            spark.stop()
    
    return 0

if __name__ == "__main__":
    exit(main())



