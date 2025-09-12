"""
Environment-specific configurations for BlinkAI Feature Store.

This module provides configurations for different environments and storage backends.
"""

import os
from typing import Dict, Any

def get_iceberg_config(environment: str = "azure") -> Dict[str, Any]:
    """
    Get Iceberg configuration for the specified environment.
    
    Args:
        environment: Environment name ("azure" or "aws")
        
    Returns:
        Dictionary with Iceberg configuration
    """
    
    if environment.lower() == "azure":
        return {
            "catalog": "bliinkai_catalog",
            "catalog_type": "rest",
            "warehouse_path": "abfss://iceberg-warehouse@blinkaidatalake.dfs.core.windows.net/iceberg/warehouse",
            "storage_options": {
                "account_name": "blinkaidatalake",
                "account_key": os.getenv("AZURE_STORAGE_KEY"),
            }
        }
    
    elif environment.lower() == "aws":
        return {
            "catalog": "bliinkai_catalog",
            "catalog_type": "glue",  # or "hive"
            "warehouse_path": "s3://your-bucket/iceberg/warehouse",
            "storage_options": {
                "aws_access_key_id": os.getenv("AWS_ACCESS_KEY_ID"),
                "aws_secret_access_key": os.getenv("AWS_SECRET_ACCESS_KEY"),
                "aws_session_token": os.getenv("AWS_SESSION_TOKEN"),
            }
        }
    
    else:
        raise ValueError(f"Unsupported environment: {environment}")

def get_data_source_config(environment: str = "azure") -> Dict[str, str]:
    """
    Get data source table names for the specified environment.
    
    Args:
        environment: Environment name ("azure" or "aws")
        
    Returns:
        Dictionary with table names
    """
    
    if environment.lower() == "azure":
        return {
            "transactions_table": "bliinkai_catalog.default.transactions_silver",
            "accounts_table": "bliinkai_catalog.default.accounts_silver", 
            "entities_table": "bliinkai_catalog.default.entities_silver",
        }
    
    elif environment.lower() == "aws":
        return {
            "transactions_table": "bliinkai_catalog.default.transactions_silver",
            "accounts_table": "bliinkai_catalog.default.accounts_silver",
            "entities_table": "bliinkai_catalog.default.entities_silver",
        }
    
    else:
        raise ValueError(f"Unsupported environment: {environment}")

# Default environment
DEFAULT_ENVIRONMENT = os.getenv("FEAST_ENVIRONMENT", "azure")
