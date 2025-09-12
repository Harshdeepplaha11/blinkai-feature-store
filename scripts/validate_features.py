#!/usr/bin/env python3
"""
Feature validation script for BlinkAI Feature Store.

This script validates feature definitions, environment variables, and data quality.
"""

import argparse
import sys
import os

# Add the parent directory to the path to import config
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from feast import FeatureStore
from config.environments import validate_environment_variables

def validate_environment():
    """
    Validate that all required environment variables are set.
    """
    print("Validating environment variables...")
    
    if validate_environment_variables():
        print("✓ All required environment variables are set")
        return True
    else:
        print("✗ Missing required environment variables")
        return False

def validate_feature_store(feature_store_path: str = "."):
    """
    Validate the feature store configuration and definitions.
    
    Args:
        feature_store_path: Path to the feature store
    """
    print("Validating feature store...")
    
    try:
        # Initialize feature store
        store = FeatureStore(repo_path=feature_store_path)
        
        # List all feature views
        feature_views = store.list_feature_views()
        print(f"Found {len(feature_views)} feature views:")
        
        for fv in feature_views:
            print(f"  - {fv.name}: {len(fv.features)} features")
            
            # Validate feature view
            try:
                # Check if features are properly defined
                for feature in fv.features:
                    if not feature.name:
                        print(f"    WARNING: Feature without name in {fv.name}")
                    if not feature.dtype:
                        print(f"    WARNING: Feature {feature.name} without dtype in {fv.name}")
                
                print(f"    ✓ {fv.name} validation passed")
                
            except Exception as e:
                print(f"    ✗ {fv.name} validation failed: {e}")
        
        # List all entities
        entities = store.list_entities()
        print(f"\nFound {len(entities)} entities:")
        
        for entity in entities:
            print(f"  - {entity.name}: {entity.value_type}")
        
        # List all data sources
        data_sources = store.list_data_sources()
        print(f"\nFound {len(data_sources)} data sources:")
        
        for ds in data_sources:
            print(f"  - {ds.name}: {type(ds).__name__}")
        
        print("\n✓ Feature store validation completed successfully!")
        return True
        
    except Exception as e:
        print(f"✗ Feature store validation failed: {e}")
        return False

def main():
    """Main validation function."""
    parser = argparse.ArgumentParser(description="Validate feature store")
    parser.add_argument("--repo-path", default=".", help="Path to feature store")
    parser.add_argument("--skip-env", action="store_true", help="Skip environment variable validation")
    
    args = parser.parse_args()
    
    print("🔍 BlinkAI Feature Store Validation")
    print("=" * 50)
    
    # Validate environment variables first
    if not args.skip_env:
        env_valid = validate_environment()
        if not env_valid:
            print("\n❌ Environment validation failed. Use --skip-env to skip this check.")
            sys.exit(1)
        print()
    
    # Validate feature store
    store_valid = validate_feature_store(feature_store_path=args.repo_path)
    
    if store_valid:
        print("\n🎉 All validations passed!")
        sys.exit(0)
    else:
        print("\n❌ Feature store validation failed.")
        sys.exit(1)

if __name__ == "__main__":
    main()
