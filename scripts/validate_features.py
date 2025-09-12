#!/usr/bin/env python3
"""
Feature validation script for BlinkAI Feature Store.

This script validates feature definitions and data quality.
"""

import argparse
from feast import FeatureStore

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
        
    except Exception as e:
        print(f"✗ Feature store validation failed: {e}")
        raise

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Validate feature store")
    parser.add_argument("--repo-path", default=".", help="Path to feature store")
    
    args = parser.parse_args()
    
    validate_feature_store(feature_store_path=args.repo_path)
