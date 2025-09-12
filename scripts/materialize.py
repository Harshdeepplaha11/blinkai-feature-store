#!/usr/bin/env python3
"""
Materialization script for BlinkAI Feature Store.

This script handles materializing features to the online store (Redis).
"""

import argparse
from datetime import datetime, timedelta
from feast import FeatureStore

def materialize_features(
    feature_store_path: str = ".",
    start_date: str = None,
    end_date: str = None,
    feature_views: list = None
):
    """
    Materialize features to the online store.
    
    Args:
        feature_store_path: Path to the feature store
        start_date: Start date for materialization (YYYY-MM-DD)
        end_date: End date for materialization (YYYY-MM-DD)
        feature_views: List of feature view names to materialize
    """
    # Initialize feature store
    store = FeatureStore(repo_path=feature_store_path)
    
    # Set default dates if not provided
    if not end_date:
        end_date = datetime.now().strftime("%Y-%m-%d")
    
    if not start_date:
        start_date = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
    
    print(f"Materializing features from {start_date} to {end_date}")
    
    # Get all feature views if none specified
    if not feature_views:
        feature_views = [fv.name for fv in store.list_feature_views()]
    
    print(f"Feature views to materialize: {feature_views}")
    
    # Materialize features
    store.materialize(
        start_date=start_date,
        end_date=end_date,
        feature_views=feature_views
    )
    
    print("Materialization completed successfully!")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Materialize features to online store")
    parser.add_argument("--start-date", help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", help="End date (YYYY-MM-DD)")
    parser.add_argument("--feature-views", nargs="+", help="Feature view names to materialize")
    parser.add_argument("--repo-path", default=".", help="Path to feature store")
    
    args = parser.parse_args()
    
    materialize_features(
        feature_store_path=args.repo_path,
        start_date=args.start_date,
        end_date=args.end_date,
        feature_views=args.feature_views
    )
