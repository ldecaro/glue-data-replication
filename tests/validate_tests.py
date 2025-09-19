#!/usr/bin/env python3
"""
Simple test validation script to verify the modular structure works.
"""

import sys
import os
import unittest

# Add src directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'src'))

# Mock PySpark and AWS Glue imports
from unittest.mock import MagicMock
mock_modules = [
    'awsglue', 'awsglue.utils', 'awsglue.context', 'awsglue.job',
    'pyspark', 'pyspark.context', 'pyspark.sql', 'pyspark.sql.types',
    'pyspark.sql.functions', 'boto3'
]

for module in mock_modules:
    sys.modules[module] = MagicMock()

def test_imports():
    """Test that all modules can be imported."""
    print("Testing module imports...")
    
    try:
        from glue_job.config import JobConfig, ConnectionConfig, DatabaseEngineManager
        print("✓ Config module imports successful")
    except Exception as e:
        print(f"✗ Config module import failed: {e}")
        return False
    
    try:
        from glue_job.storage import S3BookmarkStorage, JobBookmarkManager
        print("✓ Storage module imports successful")
    except Exception as e:
        print(f"✗ Storage module import failed: {e}")
        return False
    
    try:
        from glue_job.database import JdbcConnectionManager, SchemaCompatibilityValidator
        print("✓ Database module imports successful")
    except Exception as e:
        print(f"✗ Database module import failed: {e}")
        return False
    
    try:
        from glue_job.monitoring import StructuredLogger, CloudWatchMetricsPublisher
        print("✓ Monitoring module imports successful")
    except Exception as e:
        print(f"✗ Monitoring module import failed: {e}")
        return False
    
    try:
        from glue_job.network import NetworkErrorHandler, ConnectionRetryHandler
        print("✓ Network module imports successful")
    except Exception as e:
        print(f"✗ Network module import failed: {e}")
        return False
    
    try:
        from glue_job.utils import S3PathUtilities, EnhancedS3ParallelOperations
        print("✓ Utils module imports successful")
    except Exception as e:
        print(f"✗ Utils module import failed: {e}")
        return False
    
    return True

def test_basic_functionality():
    """Test basic functionality of key classes."""
    print("\nTesting basic functionality...")
    
    try:
        from glue_job.config import ConnectionConfig, JobConfig
        
        # Test ConnectionConfig creation
        config = ConnectionConfig(
            engine_type="postgresql",
            connection_string="jdbc:postgresql://localhost:5432/testdb",
            database="testdb",
            schema="public",
            username="testuser",
            password="testpass",
            jdbc_driver_path="s3://bucket/drivers/postgresql.jar"
        )
        
        assert config.engine_type == "postgresql"
        assert config.database == "testdb"
        assert config.schema == "public"
        print("✓ ConnectionConfig creation works")
        
        # Test JobConfig creation
        job_config = JobConfig(
            job_name="test-job",
            source_connection=config,
            target_connection=config,
            tables=["test_table"]
        )
        
        assert job_config.job_name == "test-job"
        assert job_config.tables == ["test_table"]
        print("✓ JobConfig creation works")
        
    except Exception as e:
        print(f"✗ Basic functionality test failed: {e}")
        return False
    
    return True

def run_sample_tests():
    """Run a few sample unit tests."""
    print("\nRunning sample unit tests...")
    
    try:
        # Import and run a specific test class
        from test_config import TestConnectionConfig
        
        suite = unittest.TestLoader().loadTestsFromTestCase(TestConnectionConfig)
        runner = unittest.TextTestRunner(verbosity=2)
        result = runner.run(suite)
        
        if result.wasSuccessful():
            print("✓ Sample unit tests passed")
            return True
        else:
            print(f"✗ Sample unit tests failed: {len(result.failures)} failures, {len(result.errors)} errors")
            return False
            
    except Exception as e:
        print(f"✗ Sample unit test execution failed: {e}")
        return False

def main():
    """Main validation function."""
    print("="*60)
    print("AWS Glue Data Replication - Test Validation")
    print("Modular Architecture Verification")
    print("="*60)
    
    # Step 1: Test imports
    imports_ok = test_imports()
    
    if not imports_ok:
        print("\n❌ Import tests failed. Cannot proceed.")
        return False
    
    # Step 2: Test basic functionality
    functionality_ok = test_basic_functionality()
    
    if not functionality_ok:
        print("\n❌ Basic functionality tests failed.")
        return False
    
    # Step 3: Run sample unit tests
    unit_tests_ok = run_sample_tests()
    
    if not unit_tests_ok:
        print("\n❌ Unit tests failed.")
        return False
    
    print("\n" + "="*60)
    print("🎉 ALL VALIDATION TESTS PASSED!")
    print("The modular architecture is working correctly.")
    print("Task 11 implementation is successful.")
    print("="*60)
    
    return True

if __name__ == '__main__':
    success = main()
    sys.exit(0 if success else 1)