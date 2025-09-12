#!/usr/bin/env python3
"""
Spark-based feature computation script for BlinkAI Feature Store.

This script runs Spark jobs to compute features and write them to Iceberg tables.
"""

import argparse
import sys
import os
from datetime import datetime, timedelta
from pyspark.sql import SparkSession

# Add the parent directory to the path to import modules
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from transformations import TransactionFeatureComputer, AccountFeatureComputer, CustomerFeatureComputer


def compute_all_features(start_date: str, end_date: str, feature_types: list = None):
    """
    Compute all features for the specified date range.
    
    Args:
        start_date: Start date for feature computation (YYYY-MM-DD)
        end_date: End date for feature computation (YYYY-MM-DD)
        feature_types: List of feature types to compute (default: all)
    """
    if feature_types is None:
        feature_types = ["transaction", "account", "customer"]
    
    print(f"🚀 Starting feature computation from {start_date} to {end_date}")
    print(f"📊 Feature types: {feature_types}")
    
    # Spark configuration for feature computation
    spark_config = {
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        "spark.sql.adaptive.skewJoin.enabled": "true",
        "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
        "spark.sql.execution.arrow.pyspark.enabled": "true",
    }
    
    computers = []
    
    try:
        # Initialize feature computers
        if "transaction" in feature_types:
            print("🔄 Computing transaction features...")
            transaction_computer = TransactionFeatureComputer(spark_config)
            computers.append(transaction_computer)
            transaction_computer.compute_features(start_date, end_date)
            print("✅ Transaction features completed!")
        
        if "account" in feature_types:
            print("🔄 Computing account features...")
            account_computer = AccountFeatureComputer(spark_config)
            computers.append(account_computer)
            account_computer.compute_features(start_date, end_date)
            print("✅ Account features completed!")
        
        if "customer" in feature_types:
            print("🔄 Computing customer features...")
            customer_computer = CustomerFeatureComputer(spark_config)
            computers.append(customer_computer)
            customer_computer.compute_features(start_date, end_date)
            print("✅ Customer features completed!")
        
        print("🎉 All feature computation completed successfully!")
        
    except Exception as e:
        print(f"❌ Feature computation failed: {e}")
        raise
    finally:
        # Clean up Spark sessions
        for computer in computers:
            try:
                computer.stop()
            except Exception as e:
                print(f"Warning: Failed to stop Spark session: {e}")


def main():
    """Main function."""
    parser = argparse.ArgumentParser(description="Compute features using Spark")
    parser.add_argument("--start-date", required=True, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", required=True, help="End date (YYYY-MM-DD)")
    parser.add_argument("--feature-types", nargs="+", 
                       choices=["transaction", "account", "customer"],
                       default=["transaction", "account", "customer"],
                       help="Feature types to compute")
    
    args = parser.parse_args()
    
    # Validate dates
    try:
        datetime.strptime(args.start_date, "%Y-%m-%d")
        datetime.strptime(args.end_date, "%Y-%m-%d")
    except ValueError:
        print("❌ Invalid date format. Use YYYY-MM-DD")
        sys.exit(1)
    
    # Validate environment variables
    required_vars = [
        "ICEBERG_CATALOG", "ICEBERG_WAREHOUSE_PATH", 
        "AZURE_STORAGE_ACCOUNT_NAME", "AZURE_STORAGE_KEY"
    ]
    
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    if missing_vars:
        print(f"❌ Missing required environment variables: {missing_vars}")
        sys.exit(1)
    
    # Run feature computation
    compute_all_features(args.start_date, args.end_date, args.feature_types)


if __name__ == "__main__":
    main()
