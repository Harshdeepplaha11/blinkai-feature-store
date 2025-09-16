"""
Base Spark feature computer for BlinkAI Feature Store.

This module provides the base class for computing features using Spark
and writing them to Iceberg tables.
"""

import os
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, when, isnan, isnull, count, sum as spark_sum, avg, max as spark_max, min as spark_min
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, TimestampType


class SparkFeatureComputer(ABC):
    """
    Base class for computing features using Spark.
    
    This class provides common functionality for:
    - Spark session management
    - Iceberg table operations
    - Feature computation patterns
    """
    
    def __init__(self, spark_config: Optional[Dict[str, Any]] = None):
        """
        Initialize the Spark feature computer.
        
        Args:
            spark_config: Optional Spark configuration dictionary
        """
        self.spark_config = spark_config or {}
        self.spark = self._create_spark_session()
        
    def _create_spark_session(self) -> SparkSession:
        """
        Create and configure Spark session for Iceberg operations.
        
        Returns:
            Configured SparkSession
        """
        builder = SparkSession.builder \
            .appName("BlinkAI-Feature-Store") \
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
            .config("spark.sql.catalog.bliinkai_catalog", "org.apache.iceberg.spark.SparkCatalog") \
            .config("spark.sql.catalog.bliinkai_catalog.type", "rest") \
            .config("spark.sql.catalog.bliinkai_catalog.uri", os.getenv("ICEBERG_CATALOG_URI", "http://localhost:8181")) \
            .config("spark.sql.catalog.bliinkai_catalog.warehouse", os.getenv("ICEBERG_WAREHOUSE_PATH")) \
            .config("spark.sql.catalog.bliinkai_catalog.io-impl", "org.apache.iceberg.azure.adlsv2.ADLSFileIO") \
            .config("spark.sql.catalog.bliinkai_catalog.adls.account", os.getenv("AZURE_STORAGE_ACCOUNT_NAME")) \
            .config("spark.sql.catalog.bliinkai_catalog.adls.key", os.getenv("AZURE_STORAGE_KEY"))
        
        # Apply additional config
        for key, value in self.spark_config.items():
            builder = builder.config(key, value)
            
        return builder.getOrCreate()
    
    def read_iceberg_table(self, table_name: str) -> DataFrame:
        """
        Read data from an Iceberg table.
        
        Args:
            table_name: Name of the Iceberg table
            
        Returns:
            Spark DataFrame
        """
        return self.spark.read \
            .format("iceberg") \
            .load(f"bliinkai_catalog.default.{table_name}")
    
    def write_iceberg_table(self, df: DataFrame, table_name: str, mode: str = "overwrite") -> None:
        """
        Write DataFrame to an Iceberg table.
        
        Args:
            df: Spark DataFrame to write
            table_name: Name of the target Iceberg table
            mode: Write mode (overwrite, append, etc.)
        """
        df.write \
            .format("iceberg") \
            .mode(mode) \
            .saveAsTable(f"bliinkai_catalog.default.{table_name}")
    
    def create_feature_table_schema(self, table_name: str, schema: StructType) -> None:
        """
        Create an Iceberg table with the specified schema.
        
        Args:
            table_name: Name of the table to create
            schema: Spark DataFrame schema
        """
        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS bliinkai_catalog.default.{table_name} (
                {self._schema_to_sql(schema)}
            ) USING iceberg
            PARTITIONED BY (feature_date)
        """)
    
    def _schema_to_sql(self, schema: StructType) -> str:
        """
        Convert Spark schema to SQL DDL.
        
        Args:
            schema: Spark DataFrame schema
            
        Returns:
            SQL DDL string
        """
        fields = []
        for field in schema.fields:
            sql_type = self._spark_type_to_sql(field.dataType)
            nullable = "NULL" if field.nullable else "NOT NULL"
            fields.append(f"{field.name} {sql_type} {nullable}")
        return ", ".join(fields)
    
    def _spark_type_to_sql(self, data_type) -> str:
        """
        Convert Spark data type to SQL type.
        
        Args:
            data_type: Spark data type
            
        Returns:
            SQL type string
        """
        type_mapping = {
            StringType: "STRING",
            LongType: "BIGINT", 
            DoubleType: "DOUBLE",
            TimestampType: "TIMESTAMP"
        }
        return type_mapping.get(type(data_type), "STRING")
    
    @abstractmethod
    def compute_features(self, start_date: str, end_date: str) -> None:
        """
        Compute features for the specified date range.
        
        Args:
            start_date: Start date for feature computation (YYYY-MM-DD)
            end_date: End date for feature computation (YYYY-MM-DD)
        """
        pass
    
    def stop(self) -> None:
        """Stop the Spark session."""
        if self.spark:
            self.spark.stop()

