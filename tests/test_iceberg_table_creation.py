#!/usr/bin/env python3
"""
Test for Iceberg table creation functionality (Task 5).

This test verifies the create_table_if_not_exists() method implementation
in IcebergConnectionHandler.
"""

import unittest
from unittest.mock import Mock, MagicMock, patch
import sys
import os

# Add the src directory to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from glue_job.config.iceberg_connection_handler import IcebergConnectionHandler
from glue_job.config.iceberg_models import (
    IcebergConfig, IcebergSchema, IcebergSchemaField,
    IcebergConnectionError, IcebergSchemaCreationError
)


class MockResultSetMetaData:
    """Mock JDBC ResultSetMetaData for testing."""
    
    def __init__(self, columns):
        """Initialize with column definitions.
        
        Args:
            columns: List of tuples (name, type, precision, scale, nullable)
        """
        self.columns = columns
    
    def getColumnCount(self):
        return len(self.columns)
    
    def getColumnName(self, index):
        return self.columns[index - 1][0]  # JDBC is 1-indexed
    
    def getColumnTypeName(self, index):
        return self.columns[index - 1][1]
    
    def getPrecision(self, index):
        return self.columns[index - 1][2] if len(self.columns[index - 1]) > 2 else 0
    
    def getScale(self, index):
        return self.columns[index - 1][3] if len(self.columns[index - 1]) > 3 else 0
    
    def isNullable(self, index):
        return self.columns[index - 1][4] if len(self.columns[index - 1]) > 4 else 1


class TestIcebergTableCreation(unittest.TestCase):
    """Test cases for Iceberg table creation functionality."""
    
    def setUp(self):
        """Set up test fixtures."""
        # Mock Spark and Glue components
        self.mock_spark = Mock()
        self.mock_glue_context = Mock()
        
        # Create handler instance
        self.handler = IcebergConnectionHandler(
            spark_session=self.mock_spark,
            glue_context=self.mock_glue_context
        )
        
        # Mock Glue client
        self.handler.glue_client = Mock()
        
        # Sample configuration
        self.iceberg_config = IcebergConfig(
            database_name="test_db",
            table_name="test_table",
            warehouse_location="s3://test-bucket/warehouse/",
            catalog_id=None,
            format_version="2"
        )
        
        # Sample JDBC metadata
        self.sample_metadata = MockResultSetMetaData([
            ("id", "INTEGER", 10, 0, 0),  # Not nullable
            ("name", "VARCHAR", 255, 0, 1),  # Nullable
            ("created_at", "TIMESTAMP", 0, 0, 1),  # Nullable
            ("amount", "DECIMAL", 10, 2, 1)  # Nullable
        ])
    
    @patch('glue_job.config.iceberg_schema_manager.IcebergSchemaManager')
    def test_create_table_if_not_exists_new_table(self, mock_schema_manager_class):
        """Test creating a new Iceberg table when it doesn't exist."""
        # Setup mocks
        mock_schema_manager = Mock()
        mock_schema_manager_class.return_value = mock_schema_manager
        
        # Mock schema creation
        mock_iceberg_schema = IcebergSchema(
            schema_id=0,
            fields=[
                IcebergSchemaField(id=1, name="id", type="int", required=True),
                IcebergSchemaField(id=2, name="name", type="string", required=False),
                IcebergSchemaField(id=3, name="created_at", type="timestamp", required=False),
                IcebergSchemaField(id=4, name="amount", type="decimal(10,2)", required=False)
            ],
            identifier_field_ids=[1]  # id column as bookmark
        )
        
        mock_schema_manager.create_iceberg_schema_from_jdbc.return_value = mock_iceberg_schema
        mock_schema_manager.validate_iceberg_schema.return_value = {
            'is_valid': True,
            'errors': [],
            'warnings': []
        }
        mock_schema_manager.create_spark_schema_from_iceberg.return_value = Mock()
        
        # Mock table doesn't exist
        self.handler.table_exists = Mock(return_value=False)
        self.handler.configure_iceberg_catalog = Mock()
        self.handler._update_table_identifier_field_ids = Mock()
        
        # Execute test
        result = self.handler.create_table_if_not_exists(
            database="test_db",
            table="test_table",
            source_metadata=self.sample_metadata,
            bookmark_column="id",
            iceberg_config=self.iceberg_config,
            source_engine="postgresql"
        )
        
        # Verify results
        self.assertTrue(result)  # Should return True for new table creation
        
        # Verify method calls
        self.handler.configure_iceberg_catalog.assert_called_once_with(
            self.iceberg_config.warehouse_location,
            self.iceberg_config.catalog_id
        )
        
        mock_schema_manager.create_iceberg_schema_from_jdbc.assert_called_once_with(
            jdbc_metadata=self.sample_metadata,
            bookmark_column="id",
            source_engine="postgresql",
            schema_id=0
        )
        
        mock_schema_manager.validate_iceberg_schema.assert_called_once()
        self.mock_spark.sql.assert_called_once()  # CREATE TABLE SQL executed
        
        # Verify identifier field IDs were updated
        self.handler._update_table_identifier_field_ids.assert_called_once_with(
            database="test_db",
            table="test_table",
            identifier_field_ids=[1],
            catalog_id=self.iceberg_config.catalog_id
        )
    
    def test_create_table_if_not_exists_existing_table(self):
        """Test behavior when table already exists."""
        # Mock table exists
        self.handler.table_exists = Mock(return_value=True)
        self.handler.configure_iceberg_catalog = Mock()
        
        # Execute test
        result = self.handler.create_table_if_not_exists(
            database="test_db",
            table="test_table",
            source_metadata=self.sample_metadata,
            bookmark_column="id",
            iceberg_config=self.iceberg_config,
            source_engine="postgresql"
        )
        
        # Verify results
        self.assertFalse(result)  # Should return False for existing table
        
        # Verify catalog was configured but no table creation occurred
        self.handler.configure_iceberg_catalog.assert_called_once()
        self.mock_spark.sql.assert_not_called()  # No CREATE TABLE SQL
    
    @patch('glue_job.config.iceberg_schema_manager.IcebergSchemaManager')
    def test_create_table_if_not_exists_schema_validation_failure(self, mock_schema_manager_class):
        """Test handling of schema validation failures."""
        # Setup mocks
        mock_schema_manager = Mock()
        mock_schema_manager_class.return_value = mock_schema_manager
        
        # Mock schema creation with validation failure
        mock_iceberg_schema = Mock()
        mock_schema_manager.create_iceberg_schema_from_jdbc.return_value = mock_iceberg_schema
        mock_schema_manager.validate_iceberg_schema.return_value = {
            'is_valid': False,
            'errors': ['Invalid field type', 'Missing required field'],
            'warnings': []
        }
        
        # Mock table doesn't exist
        self.handler.table_exists = Mock(return_value=False)
        self.handler.configure_iceberg_catalog = Mock()
        
        # Execute test and expect exception
        with self.assertRaises(IcebergSchemaCreationError) as context:
            self.handler.create_table_if_not_exists(
                database="test_db",
                table="test_table",
                source_metadata=self.sample_metadata,
                bookmark_column="id",
                iceberg_config=self.iceberg_config,
                source_engine="postgresql"
            )
        
        # Verify error message contains validation errors
        self.assertIn("Generated schema validation failed", str(context.exception))
        self.assertIn("Invalid field type", str(context.exception))
    
    def test_build_create_table_sql(self):
        """Test CREATE TABLE SQL generation."""
        # Create sample schema
        iceberg_schema = IcebergSchema(
            schema_id=0,
            fields=[
                IcebergSchemaField(id=1, name="id", type="int", required=True, doc="Primary key"),
                IcebergSchemaField(id=2, name="name", type="string", required=False, doc="User name"),
                IcebergSchemaField(id=3, name="amount", type="decimal(10,2)", required=False)
            ],
            identifier_field_ids=[1]
        )
        
        # Execute test
        sql = self.handler._build_create_table_sql(
            full_table_name="glue_catalog.test_db.test_table",
            iceberg_schema=iceberg_schema,
            iceberg_config=self.iceberg_config
        )
        
        # Verify SQL structure
        self.assertIn("CREATE TABLE glue_catalog.test_db.test_table", sql)
        self.assertIn("USING iceberg", sql)
        self.assertIn("'format-version'='2'", sql)
        self.assertIn("`id` int COMMENT 'Primary key'", sql)
        self.assertIn("`name` string NULL COMMENT 'User name'", sql)
        self.assertIn("`amount` decimal(10,2) NULL", sql)
        
        # Verify Iceberg properties
        self.assertIn("'write.update.mode'='merge-on-read'", sql)
        self.assertIn("'write.delete.mode'='merge-on-read'", sql)
        self.assertIn("'write.merge.mode'='merge-on-read'", sql)
    
    def test_build_create_table_sql_with_custom_properties(self):
        """Test CREATE TABLE SQL generation with custom table properties."""
        # Create config (IcebergConfig doesn't have table_properties, so we'll test the basic functionality)
        config_with_props = IcebergConfig(
            database_name="test_db",
            table_name="test_table",
            warehouse_location="s3://test-bucket/warehouse/"
        )
        
        # Add table_properties attribute manually for testing
        config_with_props.table_properties = {
            "write.parquet.compression-codec": "snappy",
            "write.target-file-size-bytes": "134217728"
        }
        
        # Create simple schema
        iceberg_schema = IcebergSchema(
            schema_id=0,
            fields=[IcebergSchemaField(id=1, name="id", type="int", required=True)],
            identifier_field_ids=[1]
        )
        
        # Execute test
        sql = self.handler._build_create_table_sql(
            full_table_name="glue_catalog.test_db.test_table",
            iceberg_schema=iceberg_schema,
            iceberg_config=config_with_props
        )
        
        # Verify custom properties are included
        self.assertIn("'write.parquet.compression-codec'='snappy'", sql)
        self.assertIn("'write.target-file-size-bytes'='134217728'", sql)


if __name__ == '__main__':
    unittest.main()