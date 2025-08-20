#!/usr/bin/env python3
"""
Unit tests for database modules.

This test suite covers:
- JdbcConnectionManager and GlueConnectionManager
- SchemaCompatibilityValidator and DataTypeMapper
- FullLoadDataMigrator and IncrementalDataMigrator
- IncrementalColumnDetector
"""

import unittest
from unittest.mock import Mock, MagicMock, patch
import sys
import os

# Add src directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'src'))

# Mock PySpark and AWS Glue imports for testing
mock_modules = [
    'awsglue', 'awsglue.utils', 'awsglue.context', 'awsglue.job',
    'pyspark', 'pyspark.context', 'pyspark.sql', 'pyspark.sql.types',
    'pyspark.sql.functions', 'boto3'
]

for module in mock_modules:
    sys.modules[module] = MagicMock()

# Import the classes to test from new modular structure
from glue_job.database import (
    JdbcConnectionManager, GlueConnectionManager,
    SchemaCompatibilityValidator, DataTypeMapper,
    FullLoadDataMigrator, IncrementalDataMigrator,
    IncrementalColumnDetector
)
from glue_job.config import ConnectionConfig


class TestJdbcConnectionManager(unittest.TestCase):
    """Test JdbcConnectionManager functionality."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.connection_config = ConnectionConfig(
            engine_type="postgresql",
            connection_string="jdbc:postgresql://localhost:5432/testdb",
            database="testdb",
            schema="public",
            username="testuser",
            password="testpass",
            jdbc_driver_path="s3://bucket/drivers/postgresql.jar"
        )
        
        # Mock Spark session
        self.mock_spark = Mock()
        self.connection_manager = JdbcConnectionManager(self.mock_spark)
    
    def test_create_connection_success(self):
        """Test successful JDBC connection creation."""
        # Mock DataFrame
        mock_df = Mock()
        self.mock_spark.read.format.return_value.options.return_value.load.return_value = mock_df
        
        # Test creating connection
        result = self.connection_manager.create_connection(
            self.connection_config, 
            "SELECT * FROM test_table"
        )
        
        self.assertEqual(result, mock_df)
        self.mock_spark.read.format.assert_called_once_with("jdbc")
    
    def test_validate_connection_success(self):
        """Test successful connection validation."""
        # Mock successful connection test
        mock_df = Mock()
        mock_df.count.return_value = 1
        self.mock_spark.read.format.return_value.options.return_value.load.return_value = mock_df
        
        # Test connection validation
        result = self.connection_manager.validate_connection(self.connection_config)
        
        self.assertTrue(result)
    
    def test_validate_connection_failure(self):
        """Test connection validation failure."""
        # Mock connection failure
        self.mock_spark.read.format.return_value.options.return_value.load.side_effect = Exception("Connection failed")
        
        # Test connection validation failure
        result = self.connection_manager.validate_connection(self.connection_config)
        
        self.assertFalse(result)


class TestSchemaCompatibilityValidator(unittest.TestCase):
    """Test SchemaCompatibilityValidator functionality."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.validator = SchemaCompatibilityValidator()
    
    def test_validate_compatible_schemas(self):
        """Test validation of compatible schemas."""
        # Mock source and target schemas
        source_schema = Mock()
        source_schema.fields = [
            Mock(name="id", dataType=Mock(typeName=Mock(return_value="integer"))),
            Mock(name="name", dataType=Mock(typeName=Mock(return_value="string")))
        ]
        
        target_schema = Mock()
        target_schema.fields = [
            Mock(name="id", dataType=Mock(typeName=Mock(return_value="integer"))),
            Mock(name="name", dataType=Mock(typeName=Mock(return_value="string")))
        ]
        
        # Test schema validation
        result = self.validator.validate_schemas(source_schema, target_schema)
        
        self.assertTrue(result)
    
    def test_validate_incompatible_schemas(self):
        """Test validation of incompatible schemas."""
        # Mock incompatible schemas
        source_schema = Mock()
        source_schema.fields = [
            Mock(name="id", dataType=Mock(typeName=Mock(return_value="integer"))),
            Mock(name="name", dataType=Mock(typeName=Mock(return_value="string")))
        ]
        
        target_schema = Mock()
        target_schema.fields = [
            Mock(name="id", dataType=Mock(typeName=Mock(return_value="string"))),  # Different type
            Mock(name="name", dataType=Mock(typeName=Mock(return_value="string")))
        ]
        
        # Test schema validation
        result = self.validator.validate_schemas(source_schema, target_schema)
        
        self.assertFalse(result)


class TestDataTypeMapper(unittest.TestCase):
    """Test DataTypeMapper functionality."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.mapper = DataTypeMapper()
    
    def test_map_postgresql_to_spark_types(self):
        """Test mapping PostgreSQL types to Spark types."""
        # Test integer mapping
        spark_type = self.mapper.map_to_spark_type("postgresql", "integer")
        self.assertEqual(spark_type, "IntegerType")
        
        # Test varchar mapping
        spark_type = self.mapper.map_to_spark_type("postgresql", "varchar")
        self.assertEqual(spark_type, "StringType")
        
        # Test timestamp mapping
        spark_type = self.mapper.map_to_spark_type("postgresql", "timestamp")
        self.assertEqual(spark_type, "TimestampType")
    
    def test_map_mysql_to_spark_types(self):
        """Test mapping MySQL types to Spark types."""
        # Test int mapping
        spark_type = self.mapper.map_to_spark_type("mysql", "int")
        self.assertEqual(spark_type, "IntegerType")
        
        # Test text mapping
        spark_type = self.mapper.map_to_spark_type("mysql", "text")
        self.assertEqual(spark_type, "StringType")


class TestIncrementalColumnDetector(unittest.TestCase):
    """Test IncrementalColumnDetector functionality."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.detector = IncrementalColumnDetector()
    
    def test_detect_timestamp_column(self):
        """Test detecting timestamp-based incremental column."""
        # Mock DataFrame with timestamp column
        mock_df = Mock()
        mock_df.schema.fields = [
            Mock(name="id", dataType=Mock(typeName=Mock(return_value="integer"))),
            Mock(name="updated_at", dataType=Mock(typeName=Mock(return_value="timestamp"))),
            Mock(name="name", dataType=Mock(typeName=Mock(return_value="string")))
        ]
        
        # Test detecting incremental column
        result = self.detector.detect_incremental_column(mock_df)
        
        self.assertEqual(result, "updated_at")
    
    def test_detect_no_incremental_column(self):
        """Test when no suitable incremental column is found."""
        # Mock DataFrame without timestamp columns
        mock_df = Mock()
        mock_df.schema.fields = [
            Mock(name="id", dataType=Mock(typeName=Mock(return_value="integer"))),
            Mock(name="name", dataType=Mock(typeName=Mock(return_value="string")))
        ]
        
        # Test detecting incremental column
        result = self.detector.detect_incremental_column(mock_df)
        
        self.assertIsNone(result)


class TestFullLoadDataMigrator(unittest.TestCase):
    """Test FullLoadDataMigrator functionality."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.migrator = FullLoadDataMigrator()
        
        # Mock target connection config
        self.target_config = ConnectionConfig(
            engine_type="postgresql",
            connection_string="jdbc:postgresql://target.example.com:5432/targetdb",
            database="targetdb",
            schema="public",
            username="targetuser",
            password="targetpass",
            jdbc_driver_path="s3://bucket/drivers/postgresql.jar"
        )
    
    def test_execute_full_load(self):
        """Test executing full load migration."""
        # Mock source DataFrame
        mock_source_df = Mock()
        mock_source_df.count.return_value = 1000
        
        # Mock write operation
        mock_write = Mock()
        mock_source_df.write = mock_write
        mock_write.format.return_value = mock_write
        mock_write.options.return_value = mock_write
        mock_write.mode.return_value = mock_write
        
        # Test full load execution
        result = self.migrator.execute_full_load(
            mock_source_df, 
            self.target_config, 
            "test_table"
        )
        
        # Verify write was called
        mock_write.format.assert_called_once_with("jdbc")
        mock_write.mode.assert_called_once_with("overwrite")


class TestIncrementalDataMigrator(unittest.TestCase):
    """Test IncrementalDataMigrator functionality."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.migrator = IncrementalDataMigrator()
        
        # Mock target connection config
        self.target_config = ConnectionConfig(
            engine_type="postgresql",
            connection_string="jdbc:postgresql://target.example.com:5432/targetdb",
            database="targetdb",
            schema="public",
            username="targetuser",
            password="targetpass",
            jdbc_driver_path="s3://bucket/drivers/postgresql.jar"
        )
    
    def test_execute_incremental_load(self):
        """Test executing incremental load migration."""
        # Mock source DataFrame
        mock_source_df = Mock()
        mock_source_df.count.return_value = 100
        
        # Mock filtered DataFrame
        mock_filtered_df = Mock()
        mock_filtered_df.count.return_value = 50
        mock_source_df.filter.return_value = mock_filtered_df
        
        # Mock write operation
        mock_write = Mock()
        mock_filtered_df.write = mock_write
        mock_write.format.return_value = mock_write
        mock_write.options.return_value = mock_write
        mock_write.mode.return_value = mock_write
        
        # Test incremental load execution
        result = self.migrator.execute_incremental_load(
            mock_source_df,
            self.target_config,
            "test_table",
            "updated_at",
            "2023-01-01 00:00:00"
        )
        
        # Verify filter and write were called
        mock_source_df.filter.assert_called_once()
        mock_write.format.assert_called_once_with("jdbc")
        mock_write.mode.assert_called_once_with("append")


if __name__ == '__main__':
    unittest.main(verbosity=2)