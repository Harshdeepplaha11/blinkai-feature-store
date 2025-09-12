"""
Spark-based transaction feature computation.

This module computes transaction-related features using Spark
and writes them to Iceberg tables for Feast consumption.
"""

from datetime import datetime, timedelta
from typing import Dict, Any, Optional
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, when, isnan, isnull, count, sum as spark_sum, avg, max as spark_max, 
    min as spark_min, countDistinct, datediff, current_date, lit, 
    date_format, year, month, dayofmonth
)
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, TimestampType, DateType

from .spark_feature_computer import SparkFeatureComputer


class TransactionFeatureComputer(SparkFeatureComputer):
    """
    Computes transaction-related features using Spark.
    """
    
    def __init__(self, spark_config: Optional[Dict[str, Any]] = None):
        super().__init__(spark_config)
        self._setup_feature_tables()
    
    def _setup_feature_tables(self) -> None:
        """Set up the feature tables schema."""
        
        # User transaction features schema
        user_transaction_schema = StructType([
            StructField("user_id", LongType(), False),
            StructField("feature_date", DateType(), False),
            StructField("total_transaction_amount", DoubleType(), True),
            StructField("transaction_count", LongType(), True),
            StructField("avg_transaction_amount", DoubleType(), True),
            StructField("max_transaction_amount", DoubleType(), True),
            StructField("min_transaction_amount", DoubleType(), True),
            StructField("p2p_transaction_count", LongType(), True),
            StructField("zelle_transaction_count", LongType(), True),
            StructField("p2p_transaction_ratio", DoubleType(), True),
            StructField("unique_counterparties", LongType(), True),
            StructField("unique_countries", LongType(), True),
            StructField("unique_channels", LongType(), True),
            StructField("days_since_last_transaction", LongType(), True),
            StructField("transaction_frequency_days", DoubleType(), True),
            StructField("high_amount_transaction_count", LongType(), True),
            StructField("international_transaction_count", LongType(), True),
            StructField("computed_at", TimestampType(), False),
        ])
        
        # Account transaction features schema
        account_transaction_schema = StructType([
            StructField("account_id", LongType(), False),
            StructField("feature_date", DateType(), False),
            StructField("account_total_transaction_amount", DoubleType(), True),
            StructField("account_transaction_count", LongType(), True),
            StructField("account_avg_transaction_amount", DoubleType(), True),
            StructField("account_p2p_transaction_count", LongType(), True),
            StructField("account_zelle_transaction_count", LongType(), True),
            StructField("account_debit_transaction_count", LongType(), True),
            StructField("account_credit_transaction_count", LongType(), True),
            StructField("account_high_amount_transaction_count", LongType(), True),
            StructField("account_international_transaction_count", LongType(), True),
            StructField("computed_at", TimestampType(), False),
        ])
        
        self.create_feature_table_schema("user_transaction_features", user_transaction_schema)
        self.create_feature_table_schema("account_transaction_features", account_transaction_schema)
    
    def compute_features(self, start_date: str, end_date: str) -> None:
        """
        Compute transaction features for the specified date range.
        
        Args:
            start_date: Start date for feature computation (YYYY-MM-DD)
            end_date: End date for feature computation (YYYY-MM-DD)
        """
        print(f"Computing transaction features from {start_date} to {end_date}")
        
        # Read transaction data
        transactions_df = self.read_iceberg_table("transactions_silver")
        
        # Filter by date range
        transactions_filtered = transactions_df.filter(
            (col("transaction_date") >= start_date) & 
            (col("transaction_date") <= end_date)
        )
        
        # Compute user-level features
        self._compute_user_transaction_features(transactions_filtered, start_date, end_date)
        
        # Compute account-level features
        self._compute_account_transaction_features(transactions_filtered, start_date, end_date)
        
        print("Transaction features computation completed!")
    
    def _compute_user_transaction_features(self, transactions_df: DataFrame, start_date: str, end_date: str) -> None:
        """Compute user-level transaction features."""
        
        # Define high amount threshold (e.g., $10,000)
        HIGH_AMOUNT_THRESHOLD = 10000.0
        
        user_features = transactions_df.groupBy("user_id").agg(
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
            countDistinct("counterparty_name").alias("unique_counterparties"),
            countDistinct("transaction_origin_country").alias("unique_countries"),
            countDistinct("channel").alias("unique_channels"),
            
            # Risk indicators
            spark_sum(when(col("amount") > HIGH_AMOUNT_THRESHOLD, 1).otherwise(0)).alias("high_amount_transaction_count"),
            spark_sum(when(col("transaction_origin_country") != "US", 1).otherwise(0)).alias("international_transaction_count"),
            
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
        
        # Write to Iceberg table
        self.write_iceberg_table(final_features, "user_transaction_features", mode="overwrite")
    
    def _compute_account_transaction_features(self, transactions_df: DataFrame, start_date: str, end_date: str) -> None:
        """Compute account-level transaction features."""
        
        # Define high amount threshold
        HIGH_AMOUNT_THRESHOLD = 10000.0
        
        account_features = transactions_df.groupBy("account_id").agg(
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
            spark_sum(when(col("transaction_origin_country") != "US", 1).otherwise(0)).alias("account_international_transaction_count"),
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
        
        # Write to Iceberg table
        self.write_iceberg_table(final_features, "account_transaction_features", mode="overwrite")
