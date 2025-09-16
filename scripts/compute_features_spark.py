#!/usr/bin/env python3
"""
Spark-based feature computation script for BlinkAI Feature Store.

This script computes features from silver layer tables and writes them
to feature tables for consumption by Feast.
"""

import sys
import os
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, isnan, isnull, count, sum as spark_sum, avg, max as spark_max, 
    min as spark_min, countDistinct, datediff, current_date, lit, 
    date_format, year, month, dayofmonth, current_timestamp
)

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
        .appName("FeatureComputation") \
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

def compute_user_transaction_features(spark, start_date: str, end_date: str, catalog_name: str):
    """Compute user transaction features."""
    
    print(f"🔄 Computing user transaction features from {start_date} to {end_date}")
    
    # Read transactions silver table
    transactions_df = spark.read \
        .format("iceberg") \
        .load(f"{catalog_name}.default.transactions_silver")
    
    # Filter by date range
    transactions_filtered = transactions_df.filter(
        (col("transaction_date") >= start_date) & 
        (col("transaction_date") <= end_date)
    )
    
    # Define high amount threshold
    HIGH_AMOUNT_THRESHOLD = 10000.0
    
    # Compute user-level features
    user_features = transactions_filtered.groupBy("customer_number").agg(
        # Volume features
        spark_sum("amount").alias("total_transaction_amount"),
        count("*").alias("transaction_count"),
        avg("amount").alias("avg_transaction_amount"),
        spark_max("amount").alias("max_transaction_amount"),
        spark_min("amount").alias("min_transaction_amount"),
        
        # P2P and Zelle features
        spark_sum(when(col("is_p2p_transaction") == 1, 1).otherwise(0)).alias("p2p_transaction_count"),
        spark_sum(when(col("is_zelle_transaction") == "Y", 1).otherwise(0)).alias("zelle_transaction_count"),
        
        # Diversity features
        countDistinct("merchant_name").alias("unique_counterparties"),
        countDistinct("location_country").alias("unique_countries"),
        countDistinct("channel").alias("unique_channels"),
        
        # Risk indicators
        spark_sum(when(col("amount") > HIGH_AMOUNT_THRESHOLD, 1).otherwise(0)).alias("high_amount_transaction_count"),
        spark_sum(when(col("location_country") != "US", 1).otherwise(0)).alias("international_transaction_count"),
        
        # Temporal features
        spark_max("transaction_date").alias("last_transaction_date"),
        spark_min("transaction_date").alias("first_transaction_date"),
    )
    
    # Calculate derived features
    user_features = user_features.withColumn(
        "p2p_transaction_ratio",
        when(col("transaction_count") > 0, col("p2p_transaction_count") / col("transaction_count"))
        .otherwise(0.0)
    ).withColumn(
        "days_since_last_transaction",
        datediff(current_date(), col("last_transaction_date"))
    ).withColumn(
        "transaction_frequency_days",
        when(col("transaction_count") > 1, 
             datediff(col("last_transaction_date"), col("first_transaction_date")) / (col("transaction_count") - 1))
        .otherwise(0.0)
    )
    
    # Add metadata columns
    user_features = user_features.withColumn("feature_date", lit(start_date).cast("date")) \
                               .withColumn("computed_at", current_timestamp())
    
    # Rename customer_number to user_id for consistency
    user_features = user_features.withColumnRenamed("customer_number", "user_id")
    
    # Select final columns
    final_features = user_features.select(
        "user_id", "feature_date",
        "total_transaction_amount", "transaction_count", "avg_transaction_amount",
        "max_transaction_amount", "min_transaction_amount",
        "p2p_transaction_count", "zelle_transaction_count", "p2p_transaction_ratio",
        "unique_counterparties", "unique_countries", "unique_channels",
        "days_since_last_transaction", "transaction_frequency_days",
        "high_amount_transaction_count", "international_transaction_count",
        "computed_at"
    )
    
    # Write to feature table
    final_features.write \
        .format("iceberg") \
        .mode("overwrite") \
        .saveAsTable(f"{catalog_name}.default.user_transaction_features")
    
    print("✅ User transaction features computed and written successfully!")

def compute_account_transaction_features(spark, start_date: str, end_date: str, catalog_name: str):
    """Compute account transaction features."""
    
    print(f"🔄 Computing account transaction features from {start_date} to {end_date}")
    
    # Read transactions silver table
    transactions_df = spark.read \
        .format("iceberg") \
        .load(f"{catalog_name}.default.transactions_silver")
    
    # Filter by date range
    transactions_filtered = transactions_df.filter(
        (col("transaction_date") >= start_date) & 
        (col("transaction_date") <= end_date)
    )
    
    # Define high amount threshold
    HIGH_AMOUNT_THRESHOLD = 10000.0
    
    # Compute account-level features
    account_features = transactions_filtered.groupBy("account_id").agg(
        # Account transaction volume
        spark_sum("amount").alias("account_total_transaction_amount"),
        count("*").alias("account_transaction_count"),
        avg("amount").alias("account_avg_transaction_amount"),
        
        # Account transaction patterns
        spark_sum(when(col("is_p2p_transaction") == 1, 1).otherwise(0)).alias("account_p2p_transaction_count"),
        spark_sum(when(col("is_zelle_transaction") == "Y", 1).otherwise(0)).alias("account_zelle_transaction_count"),
        spark_sum(when(col("debit_credit_indicator") == "D", 1).otherwise(0)).alias("account_debit_transaction_count"),
        spark_sum(when(col("debit_credit_indicator") == "C", 1).otherwise(0)).alias("account_credit_transaction_count"),
        
        # Account risk indicators
        spark_sum(when(col("amount") > HIGH_AMOUNT_THRESHOLD, 1).otherwise(0)).alias("account_high_amount_transaction_count"),
        spark_sum(when(col("location_country") != "US", 1).otherwise(0)).alias("account_international_transaction_count"),
    )
    
    # Add metadata columns
    account_features = account_features.withColumn("feature_date", lit(start_date).cast("date")) \
                                     .withColumn("computed_at", current_timestamp())
    
    # Select final columns
    final_features = account_features.select(
        "account_id", "feature_date",
        "account_total_transaction_amount", "account_transaction_count", "account_avg_transaction_amount",
        "account_p2p_transaction_count", "account_zelle_transaction_count",
        "account_debit_transaction_count", "account_credit_transaction_count",
        "account_high_amount_transaction_count", "account_international_transaction_count",
        "computed_at"
    )
    
    # Write to feature table
    final_features.write \
        .format("iceberg") \
        .mode("overwrite") \
        .saveAsTable(f"{catalog_name}.default.account_transaction_features")
    
    print("✅ Account transaction features computed and written successfully!")

def compute_customer_features(spark, start_date: str, end_date: str, catalog_name: str):
    """Compute customer features."""
    
    print(f"🔄 Computing customer features from {start_date} to {end_date}")
    
    # Read customers silver table
    customers_df = spark.read \
        .format("iceberg") \
        .load(f"{catalog_name}.default.customers_silver")
    
    # Read accounts silver table
    accounts_df = spark.read \
        .format("iceberg") \
        .load(f"{catalog_name}.default.accounts_silver")
    
    # Read entities silver table
    entities_df = spark.read \
        .format("iceberg") \
        .load(f"{catalog_name}.default.entities_silver")
    
    # Compute customer features
    customer_features = customers_df.select(
        col("customer_id"),
        col("customer_type"),
        col("customer_status"),
        col("created_date"),
        col("date_of_birth")
    ).withColumn(
        "customer_age_days",
        datediff(current_date(), col("created_date"))
    )
    
    # Add account information
    account_summary = accounts_df.groupBy("customer_id").agg(
        count("*").alias("total_accounts"),
        spark_sum(when(col("account_status") == "ACTIVE", 1).otherwise(0)).alias("active_accounts"),
        spark_sum("balance").alias("total_balance"),
        avg("balance").alias("avg_account_balance"),
        spark_max("balance").alias("max_account_balance")
    )
    
    # Add entity information
    entity_info = entities_df.select(
        col("customer_entity_id").alias("customer_id"),
        col("risk_score").alias("customer_risk_score"),
        col("risk_rating").alias("customer_risk_rating"),
        col("customer_segment")
    )
    
    # Join all customer data
    customer_features = customer_features \
        .join(account_summary, "customer_id", "left") \
        .join(entity_info, "customer_id", "left")
    
    # Add metadata columns
    customer_features = customer_features.withColumn("feature_date", lit(start_date).cast("date")) \
                                       .withColumn("computed_at", current_timestamp())
    
    # Select final columns
    final_features = customer_features.select(
        "customer_id", "feature_date",
        "customer_risk_score", "customer_risk_rating", "customer_segment",
        "total_accounts", "active_accounts", "customer_age_days",
        "total_balance", "avg_account_balance", "max_account_balance",
        "computed_at"
    )
    
    # Write to feature table
    final_features.write \
        .format("iceberg") \
        .mode("overwrite") \
        .saveAsTable(f"{catalog_name}.default.customer_features")
    
    print("✅ Customer features computed and written successfully!")

def main():
    """Main function to compute features."""
    
    print("🚀 Starting feature computation...")
    print("=" * 50)
    
    try:
        # Create Spark session
        spark = create_spark_session()
        catalog_name = os.getenv("ICEBERG_CATALOG", "bliinkai_catalog")
        
        # Get date range from environment or use default
        start_date = os.getenv("START_DATE", "2025-01-01")
        end_date = os.getenv("END_DATE", "2025-01-02")
        
        print(f"📊 Using catalog: {catalog_name}")
        print(f"📅 Date range: {start_date} to {end_date}")
        
        # Compute features
        compute_user_transaction_features(spark, start_date, end_date, catalog_name)
        compute_account_transaction_features(spark, start_date, end_date, catalog_name)
        compute_customer_features(spark, start_date, end_date, catalog_name)
        
        print("\n🎉 Feature computation completed successfully!")
        print("\n📋 Next steps:")
        print("1. Verify features in the feature tables")
        print("2. Update Feast data sources to point to these tables")
        print("3. Test feature serving through Feast")
        
    except Exception as e:
        print(f"\n❌ Error computing features: {e}")
        return 1
    finally:
        if 'spark' in locals():
            spark.stop()
    
    return 0

if __name__ == "__main__":
    exit(main())

