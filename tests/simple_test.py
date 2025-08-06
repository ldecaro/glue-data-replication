#!/usr/bin/env python3
"""
Simple test to verify the test setup works correctly.
"""

import unittest
import sys
import os
from unittest.mock import Mock, MagicMock

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Mock AWS Glue and PySpark modules for testing
mock_modules = [
    'awsglue', 'awsglue.utils', 'awsglue.context', 'awsglue.job',
    'pyspark', 'pyspark.context', 'pyspark.sql', 'pyspark.sql.types',
    'pyspark.sql.functions', 'boto3'
]

for module in mock_modules:
    sys.modules[module] = MagicMock()

# Test basic imports
try:
    from scripts.glue_data_replication import (
        ConnectionConfig, JobConfig, DatabaseEngineManager
    )
    print("✅ Successfully imported core classes from glue_data_replication.py")
except ImportError as e:
    print(f"❌ Failed to import from glue_data_replication.py: {e}")
    sys.exit(1)

class TestBasicFunctionality(unittest.TestCase):
    """Basic tests to verify setup."""
    
    def test_database_engine_manager(self):
        """Test basic DatabaseEngineManager functionality."""
        engines = DatabaseEngineManager.get_supported_engines()
        self.assertIn('postgresql', engines)
        self.assertIn('oracle', engines)
        self.assertIn('sqlserver', engines)
        self.assertIn('db2', engines)
    
    def test_connection_config_creation(self):
        """Test basic ConnectionConfig creation."""
        config = ConnectionConfig(
            engine_type='postgresql',
            connection_string='jdbc:postgresql://localhost:5432/testdb',
            database='testdb',
            schema='public',
            username='testuser',
            password='testpass',
            jdbc_driver_path='s3://bucket/driver.jar'
        )
        
        self.assertEqual(config.engine_type, 'postgresql')
        self.assertEqual(config.database, 'testdb')

if __name__ == '__main__':
    print("Running simple test to verify setup...")
    unittest.main(verbosity=2)