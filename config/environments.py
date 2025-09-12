"""
Environment-specific configurations for BlinkAI Feature Store.

This module provides configurations for different environments and storage backends.
All connection strings and credentials are loaded from environment variables.
"""

import os
from typing import Dict, Any, Optional

def get_feast_config() -> Dict[str, Any]:
    """
    Get complete Feast configuration from environment variables.
    
    Returns:
        Dictionary with Feast configuration
    """
    return {
        "project": os.getenv("FEAST_PROJECT", "bliink"),
        "provider": os.getenv("FEAST_PROVIDER", "azure"),
        "registry": {
            "registry_type": "sql",
            "path": os.getenv("FEAST_REGISTRY_PATH"),
        },
        "online_store": {
            "type": "redis",
            "connection_string": os.getenv("REDIS_CONNECTION_STRING"),
        },
        "offline_store": {
            "type": "feast.infra.offline_stores.contrib.iceberg_offline_store.IcebergOfflineStore",
            "catalog": os.getenv("ICEBERG_CATALOG"),
            "catalog_type": os.getenv("ICEBERG_CATALOG_TYPE", "rest"),
            "warehouse_path": os.getenv("ICEBERG_WAREHOUSE_PATH"),
            "storage_options": {
                "account_name": os.getenv("AZURE_STORAGE_ACCOUNT_NAME"),
                "account_key": os.getenv("AZURE_STORAGE_KEY"),
            }
        }
    }

def get_iceberg_config(environment: str = None) -> Dict[str, Any]:
    """
    Get Iceberg configuration for the specified environment.
    
    Args:
        environment: Environment name ("azure" or "aws"), defaults to FEAST_ENVIRONMENT
        
    Returns:
        Dictionary with Iceberg configuration
    """
    env = environment or os.getenv("FEAST_ENVIRONMENT", "azure")
    
    if env.lower() == "azure":
        return {
            "catalog": os.getenv("ICEBERG_CATALOG", "bliinkai_catalog"),
            "catalog_type": os.getenv("ICEBERG_CATALOG_TYPE", "rest"),
            "warehouse_path": os.getenv("ICEBERG_WAREHOUSE_PATH"),
            "storage_options": {
                "account_name": os.getenv("AZURE_STORAGE_ACCOUNT_NAME"),
                "account_key": os.getenv("AZURE_STORAGE_KEY"),
            }
        }
    
    elif env.lower() == "aws":
        return {
            "catalog": os.getenv("ICEBERG_CATALOG", "bliinkai_catalog"),
            "catalog_type": os.getenv("ICEBERG_CATALOG_TYPE", "glue"),
            "warehouse_path": os.getenv("ICEBERG_WAREHOUSE_PATH"),
            "storage_options": {
                "aws_access_key_id": os.getenv("AWS_ACCESS_KEY_ID"),
                "aws_secret_access_key": os.getenv("AWS_SECRET_ACCESS_KEY"),
                "aws_session_token": os.getenv("AWS_SESSION_TOKEN"),
            }
        }
    
    else:
        raise ValueError(f"Unsupported environment: {env}")

def get_data_source_config(environment: str = None) -> Dict[str, str]:
    """
    Get data source table names for the specified environment.
    
    Args:
        environment: Environment name ("azure" or "aws"), defaults to FEAST_ENVIRONMENT
        
    Returns:
        Dictionary with table names
    """
    env = environment or os.getenv("FEAST_ENVIRONMENT", "azure")
    catalog = os.getenv("ICEBERG_CATALOG", "bliinkai_catalog")
    
    return {
        "transactions_table": f"{catalog}.default.transactions_silver",
        "accounts_table": f"{catalog}.default.accounts_silver", 
        "entities_table": f"{catalog}.default.entities_silver",
    }

def validate_environment_variables() -> bool:
    """
    Validate that all required environment variables are set.
    
    Returns:
        True if all required variables are set, False otherwise
    """
    required_vars = [
        "FEAST_REGISTRY_PATH",
        "REDIS_CONNECTION_STRING", 
        "ICEBERG_CATALOG",
        "ICEBERG_WAREHOUSE_PATH",
        "AZURE_STORAGE_ACCOUNT_NAME",
        "AZURE_STORAGE_KEY",
    ]
    
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    
    if missing_vars:
        print(f"Missing required environment variables: {missing_vars}")
        return False
    
    return True

# Default environment
DEFAULT_ENVIRONMENT = os.getenv("FEAST_ENVIRONMENT", "azure")
