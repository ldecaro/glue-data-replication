#!/usr/bin/env python3
"""
Unit tests for configuration modules.

This test suite covers:
- JobConfig, NetworkConfig, ConnectionConfig dataclasses
- DatabaseEngineManager and JdbcDriverLoader functionality
- JobConfigurationParser and ConnectionStringBuilder
"""

import unittest
from unittest.mock import Mock, MagicMock, patch
import sys
import os
from dataclasses import dataclass
from typing import Dict, Any

# Add src directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'src'))

# Mock PySpark and AWS Glue imports for testing
mock_modules = [
    'awsglue', 'awsglue.utils', 'awsglue.context', 'awsglue.job',
    'pyspark', 'pyspark.context', 'pyspark.sql', 'pyspark.sql.types',
    'pyspark.sql.functions'
]

for module in mock_modules:
    sys.modules[module] = MagicMock()

# Import the classes to test from new modular structure
from glue_job.config import (
    JobConfig, NetworkConfig, ConnectionConfig,
    DatabaseEngineManager, JdbcDriverLoader,
    JobConfigurationParser, ConnectionStringBuilder
)


class TestConnectionConfig(unittest.TestCase):
    """Test ConnectionConfig dataclass."""
    
    def test_connection_config_creation(self):
        """Test creating a ConnectionConfig instance."""
        config = ConnectionConfig(
            engine_type="postgresql",
            connection_string="jdbc:postgresql://localhost:5432/testdb",
            database="testdb",
            schema="public",
            username="testuser",
            password="testpass",
            jdbc_driver_path="s3://bucket/drivers/postgresql.jar"
        )
        
        self.assertEqual(config.engine_type, "postgresql")
        self.assertEqual(config.database, "testdb")
        self.assertEqual(config.schema, "public")
        self.assertEqual(config.username, "testuser")
        self.assertEqual(config.password, "testpass")


class TestJobConfig(unittest.TestCase):
    """Test JobConfig dataclass."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.source_config = ConnectionConfig(
            engine_type="postgresql",
            connection_string="jdbc:postgresql://source.example.com:5432/sourcedb",
            database="sourcedb",
            schema="public",
            username="sourceuser",
            password="sourcepass",
            jdbc_driver_path="s3://bucket/drivers/postgresql.jar"
        )
        
        self.target_config = ConnectionConfig(
            engine_type="postgresql",
            connection_string="jdbc:postgresql://target.example.com:5432/targetdb",
            database="targetdb",
            schema="public",
            username="targetuser",
            password="targetpass",
            jdbc_driver_path="s3://bucket/drivers/postgresql.jar"
        )
    
    def test_job_config_creation(self):
        """Test creating a JobConfig instance."""
        job_config = JobConfig(
            job_name="test-job",
            source_connection=self.source_config,
            target_connection=self.target_config,
            tables=["test_table"]
        )
        
        self.assertEqual(job_config.job_name, "test-job")
        self.assertEqual(job_config.source_connection, self.source_config)
        self.assertEqual(job_config.target_connection, self.target_config)
        self.assertEqual(job_config.tables, ["test_table"])


class TestDatabaseEngineManager(unittest.TestCase):
    """Test DatabaseEngineManager functionality."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.engine_manager = DatabaseEngineManager()
    
    def test_get_driver_class_postgresql(self):
        """Test getting PostgreSQL driver class."""
        driver_class = self.engine_manager.get_driver_class("postgresql")
        self.assertEqual(driver_class, "org.postgresql.Driver")
    
    def test_get_driver_class_mysql_unsupported(self):
        """Test that MySQL is not supported."""
        with self.assertRaises(ValueError) as context:
            self.engine_manager.get_driver_class("mysql")
        self.assertIn("Unsupported database engine: mysql", str(context.exception))
    
    def test_get_driver_class_sqlserver(self):
        """Test getting SQL Server driver class."""
        driver_class = self.engine_manager.get_driver_class("sqlserver")
        self.assertEqual(driver_class, "com.microsoft.sqlserver.jdbc.SQLServerDriver")
    
    def test_get_driver_class_oracle(self):
        """Test getting Oracle driver class."""
        driver_class = self.engine_manager.get_driver_class("oracle")
        self.assertEqual(driver_class, "oracle.jdbc.OracleDriver")
    
    def test_get_driver_class_unknown(self):
        """Test getting driver class for unknown engine."""
        with self.assertRaises(ValueError):
            self.engine_manager.get_driver_class("unknown_engine")


class TestConnectionStringBuilder(unittest.TestCase):
    """Test ConnectionStringBuilder functionality."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.builder = ConnectionStringBuilder()
    
    def test_build_postgresql_connection_string(self):
        """Test building PostgreSQL connection string."""
        connection_string = self.builder.build_connection_string(
            engine_type="postgresql",
            host="localhost",
            port=5432,
            database="testdb"
        )
        expected = "jdbc:postgresql://localhost:5432/testdb"
        self.assertEqual(connection_string, expected)
    
    def test_build_mysql_connection_string_unsupported(self):
        """Test that MySQL connection string building is not supported."""
        with self.assertRaises(ValueError):
            self.builder.build_connection_string(
                engine_type="mysql",
                host="localhost",
                port=3306,
                database="testdb"
            )


if __name__ == '__main__':
    unittest.main(verbosity=2)