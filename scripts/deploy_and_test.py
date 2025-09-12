#!/usr/bin/env python3
"""
Deploy and test the Spark-based feature computation setup.

This script:
1. Creates feature tables
2. Computes features
3. Tests Feast integration
"""

import subprocess
import sys
import time
import os

def run_command(command, description):
    """Run a command and return success status."""
    print(f"\n🔄 {description}")
    print(f"Command: {command}")
    print("-" * 50)
    
    try:
        result = subprocess.run(command, shell=True, check=True, capture_output=True, text=True)
        print("✅ Success!")
        if result.stdout:
            print(f"Output: {result.stdout}")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ Failed!")
        print(f"Error: {e.stderr}")
        return False

def check_prerequisites():
    """Check if required tools are available."""
    print("🔍 Checking prerequisites...")
    
    # Check kubectl
    if not run_command("kubectl version --client", "Checking kubectl"):
        print("❌ kubectl not found. Please install kubectl.")
        return False
    
    # Check if we're connected to the cluster
    if not run_command("kubectl get nodes", "Checking cluster connection"):
        print("❌ Not connected to Kubernetes cluster.")
        return False
    
    print("✅ Prerequisites check passed!")
    return True

def create_feature_tables():
    """Create the feature tables using Spark job."""
    print("\n📊 Creating feature tables...")
    
    # Apply the table creation job
    if not run_command(
        "kubectl apply -f k8s/create-feature-tables-job.yaml",
        "Deploying table creation job"
    ):
        return False
    
    # Wait for job to complete
    print("⏳ Waiting for table creation job to complete...")
    time.sleep(30)
    
    # Check job status
    if not run_command(
        "kubectl get sparkapplication create-feature-tables -n spark-jobs",
        "Checking job status"
    ):
        return False
    
    print("✅ Feature tables creation job deployed!")
    return True

def compute_features():
    """Compute features using Spark job."""
    print("\n🧮 Computing features...")
    
    # Apply the feature computation job
    if not run_command(
        "kubectl apply -f k8s/compute-features-job.yaml",
        "Deploying feature computation job"
    ):
        return False
    
    # Wait for job to complete
    print("⏳ Waiting for feature computation job to complete...")
    time.sleep(60)
    
    # Check job status
    if not run_command(
        "kubectl get sparkapplication compute-features -n spark-jobs",
        "Checking job status"
    ):
        return False
    
    print("✅ Feature computation job deployed!")
    return True

def test_feast_integration():
    """Test Feast integration."""
    print("\n🍽️ Testing Feast integration...")
    
    # Set environment variables
    env_vars = {
        "FEAST_REGISTRY_PATH": "postgresql://feast_fs_user:feast_fs_123@mlflow-postgresql.postgresql.svc.cluster.local:5432/feast_feature_store",
        "REDIS_CONNECTION_STRING": "redis://redis.feast.svc.cluster.local:6379/0",
        "ICEBERG_CATALOG": "bliinkai_catalog",
        "ICEBERG_CATALOG_TYPE": "rest",
        "ICEBERG_WAREHOUSE_PATH": "abfss://iceberg-warehouse@blinkaidatalake.dfs.core.windows.net/iceberg/warehouse",
        "AZURE_STORAGE_ACCOUNT_NAME": "blinkaidatalake",
        "AZURE_STORAGE_KEY": "your-storage-key-here"
    }
    
    # Set environment variables
    for key, value in env_vars.items():
        os.environ[key] = value
    
    # Test feature validation
    if not run_command(
        "python scripts/validate_features.py",
        "Validating Feast features"
    ):
        return False
    
    print("✅ Feast integration test passed!")
    return True

def show_next_steps():
    """Show next steps for the user."""
    print("\n🎉 Deployment completed successfully!")
    print("\n📋 Next Steps:")
    print("1. Check job logs:")
    print("   kubectl logs -n spark-jobs <driver-pod-name>")
    print("\n2. Verify feature tables:")
    print("   kubectl exec -n spark-jobs <driver-pod-name> -- spark-sql --conf spark.sql.catalog.bliinkai_catalog=org.apache.iceberg.spark.SparkCatalog")
    print("\n3. Test Feast feature serving:")
    print("   python scripts/get_features_example.py")
    print("\n4. Monitor jobs:")
    print("   kubectl get sparkapplication -n spark-jobs")
    print("\n5. Check feature tables in Iceberg:")
    print("   kubectl exec -n spark-jobs <driver-pod-name> -- spark-sql -e 'SHOW TABLES IN bliinkai_catalog.default'")

def main():
    """Main deployment function."""
    print("🚀 Deploying Spark-based Feature Computation Setup")
    print("=" * 60)
    
    # Check prerequisites
    if not check_prerequisites():
        print("\n❌ Prerequisites check failed. Please fix the issues above.")
        return 1
    
    # Create feature tables
    if not create_feature_tables():
        print("\n❌ Feature table creation failed.")
        return 1
    
    # Compute features
    if not compute_features():
        print("\n❌ Feature computation failed.")
        return 1
    
    # Test Feast integration
    if not test_feast_integration():
        print("\n❌ Feast integration test failed.")
        return 1
    
    # Show next steps
    show_next_steps()
    
    print("\n🎉 All steps completed successfully!")
    return 0

if __name__ == "__main__":
    exit(main())



