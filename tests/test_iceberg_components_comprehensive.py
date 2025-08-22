#!/usr/bin/env python3
"""
Comprehensive Unit Tests for Iceberg Components (Task 10)

This test suite provides comprehensive unit test coverage for all Iceberg components:
- IcebergConnectionHandler methods
- IcebergSchemaManager data type mapping and schema creation
- Enhanced BookmarkManager with identifier-field-ids functionality
- Iceberg parameter validation logic
- Mock tests for Glue Data Catalog operations

Requirements covered: All requirements - testing coverage
"""

import unittest
from unittest.mock import Mock, MagicMock, patch, call
from datetime import datetime, timezone
import sys
import os
import json
from botocore.exceptions import ClientError

# Add the src directory to the path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

# Import the classes we're testing
from glue_job.config.iceberg_connection_handler import IcebergConnectionHandler
from glue_job.config.iceberg_schema_manager import IcebergSchemaManager
from glue_job.config.iceberg_models import (
    IcebergConfig, IcebergSchema, IcebergSchemaField, IcebergTableMetadata,
    IcebergEngineError, IcebergTableNotFoundError, IcebergCatalogError,
    IcebergConnectionError, IcebergValidationError, IcebergSchemaCreationError,
    map_jdbc_type_to_iceberg, create_iceberg_field
)
from glue_job.config.database_engines import DatabaseEngineManager
from glue_job.config.parsers import JobConfigurationParser
from glue_job.storage.bookmark_manager import JobBookmarkManager, JobBookmarkState


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


class TestIcebergConnectionHandler(unittest.TestCase):
    """Comprehensive tests for IcebergConnectionHandler methods."""
    
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
            catalog_id="123456789012",
            format_version="2"
        )
        
        # Reset mocks for each test
        self.mock_spark.reset_mock()
        self.mock_glue_context.reset_mock()
    
    def test_configure_iceberg_catalog_success(self):
        """Test successful Iceberg catalog configuration."""
        warehouse_location = "s3://test-bucket/warehouse/"
        catalog_id = "123456789012"
        
        # Execute test
        self.handler.configure_iceberg_catalog(warehouse_location, catalog_id)
        
        # Verify Spark configuration calls (only catalog-specific configs, not global Spark configs)
        expected_calls = [
            call("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog"),
            call("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog"),
            call("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO"),
            call("spark.sql.catalog.glue_catalog.warehouse", warehouse_location),
            call("spark.sql.catalog.glue_catalog.glue.id", catalog_id)
        ]
        
        self.mock_spark.conf.set.assert_has_calls(expected_calls, any_order=True)
    
    def test_configure_iceberg_catalog_without_catalog_id(self):
        """Test Iceberg catalog configuration without catalog ID."""
        warehouse_location = "s3://test-bucket/warehouse/"
        
        # Execute test
        self.handler.configure_iceberg_catalog(warehouse_location)
        
        # Verify catalog ID is not set
        catalog_id_calls = [call for call in self.mock_spark.conf.set.call_args_list 
                           if "glue.id" in str(call)]
        self.assertEqual(len(catalog_id_calls), 0)
    
    def test_configure_iceberg_catalog_error_handling(self):
        """Test error handling in catalog configuration."""
        # Mock Spark configuration to raise exception
        self.mock_spark.conf.set.side_effect = Exception("Spark configuration failed")
        
        # Execute test and expect exception
        with self.assertRaises(IcebergConnectionError) as context:
            self.handler.configure_iceberg_catalog("s3://test-bucket/warehouse/")
        
        self.assertIn("Failed to configure Iceberg catalog", str(context.exception))
    
    def test_table_exists_iceberg_table(self):
        """Test table existence check for Iceberg table."""
        # Mock Glue client response for Iceberg table
        self.handler.glue_client.get_table.return_value = {
            'Table': {
                'Parameters': {'table_type': 'ICEBERG'}
            }
        }
        
        # Execute test
        result = self.handler.table_exists("test_db", "test_table", "123456789012")
        
        # Verify result
        self.assertTrue(result)
        self.handler.glue_client.get_table.assert_called_once_with(
            DatabaseName="test_db",
            Name="test_table",
            CatalogId="123456789012"
        )
    
    def test_table_exists_non_iceberg_table(self):
        """Test table existence check for non-Iceberg table."""
        # Mock Glue client response for non-Iceberg table
        self.handler.glue_client.get_table.return_value = {
            'Table': {
                'Parameters': {'table_type': 'EXTERNAL_TABLE'}
            }
        }
        
        # Execute test
        result = self.handler.table_exists("test_db", "test_table")
        
        # Verify result
        self.assertFalse(result)
    
    def test_table_exists_table_not_found(self):
        """Test table existence check when table doesn't exist."""
        # Mock Glue client to raise EntityNotFoundException
        self.handler.glue_client.get_table.side_effect = ClientError(
            error_response={'Error': {'Code': 'EntityNotFoundException'}},
            operation_name='GetTable'
        )
        
        # Execute test
        result = self.handler.table_exists("test_db", "test_table")
        
        # Verify result
        self.assertFalse(result)
    
    def test_table_exists_glue_error(self):
        """Test table existence check with Glue service error."""
        # Mock Glue client to raise service error
        self.handler.glue_client.get_table.side_effect = ClientError(
            error_response={'Error': {'Code': 'ServiceException'}},
            operation_name='GetTable'
        )
        
        # Execute test and expect exception
        with self.assertRaises(IcebergCatalogError):
            self.handler.table_exists("test_db", "test_table")
    
    def test_read_table_success(self):
        """Test successful table reading."""
        # Mock table exists and Glue context
        self.handler.table_exists = Mock(return_value=True)
        mock_dataframe = Mock()
        self.mock_glue_context.create_data_frame_from_catalog.return_value = mock_dataframe
        
        # Execute test
        result = self.handler.read_table("test_db", "test_table", "123456789012")
        
        # Verify result
        self.assertEqual(result, mock_dataframe)
        self.mock_glue_context.create_data_frame_from_catalog.assert_called_once_with(
            database="test_db",
            table_name="test_table",
            additional_options={'catalog_id': '123456789012'}
        )
    
    def test_read_table_not_found(self):
        """Test reading non-existent table."""
        # Mock table doesn't exist
        self.handler.table_exists = Mock(return_value=False)
        
        # Execute test and expect exception
        with self.assertRaises(IcebergTableNotFoundError):
            self.handler.read_table("test_db", "test_table")
    
    def test_write_table_create_mode(self):
        """Test writing table in create mode."""
        # Mock DataFrame and configure catalog
        mock_dataframe = Mock()
        self.handler.configure_iceberg_catalog = Mock()
        
        # Execute test
        self.handler.write_table(
            dataframe=mock_dataframe,
            database="test_db",
            table="test_table",
            mode="create",
            iceberg_config=self.iceberg_config
        )
        
        # Verify catalog configuration
        self.handler.configure_iceberg_catalog.assert_called_once_with(
            self.iceberg_config.warehouse_location,
            self.iceberg_config.catalog_id
        )
        
        # Verify SQL execution - check all calls for CREATE TABLE
        self.mock_spark.sql.assert_called()
        sql_calls = [call[0][0] for call in self.mock_spark.sql.call_args_list]
        create_table_found = any("CREATE TABLE" in sql_call for sql_call in sql_calls)
        self.assertTrue(create_table_found, f"CREATE TABLE not found in SQL calls: {sql_calls}")
        
        # Verify the CREATE TABLE call contains expected elements
        create_sql_call = next(sql_call for sql_call in sql_calls if "CREATE TABLE" in sql_call)
        self.assertIn("USING iceberg", create_sql_call)
    
    def test_write_table_append_mode(self):
        """Test writing table in append mode."""
        # Mock DataFrame and writer
        mock_dataframe = Mock()
        mock_writer = Mock()
        mock_dataframe.writeTo.return_value = mock_writer
        
        # Execute test
        self.handler.write_table(
            dataframe=mock_dataframe,
            database="test_db",
            table="test_table",
            mode="append"
        )
        
        # Verify append operation
        mock_dataframe.writeTo.assert_called_once_with("glue_catalog.test_db.test_table")
        mock_writer.tableProperty.assert_called_once_with("format-version", "2")
        mock_writer.append.assert_called_once()
        
        # Verify temporary view cleanup
        cleanup_calls = [call[0][0] for call in self.mock_spark.sql.call_args_list if "DROP VIEW" in call[0][0]]
        self.assertGreater(len(cleanup_calls), 0, "Expected cleanup SQL calls")
    
    def test_write_table_invalid_mode(self):
        """Test writing table with invalid mode."""
        mock_dataframe = Mock()
        
        # Execute test and expect exception
        with self.assertRaises(IcebergValidationError) as context:
            self.handler.write_table(
                dataframe=mock_dataframe,
                database="test_db",
                table="test_table",
                mode="invalid_mode"
            )
        
        self.assertIn("Invalid write mode", str(context.exception))
    
    def test_write_table_overwrite_mode(self):
        """Test writing table in overwrite mode."""
        # Mock DataFrame
        mock_dataframe = Mock()
        
        # Execute test
        self.handler.write_table(
            dataframe=mock_dataframe,
            database="test_db",
            table="test_table",
            mode="overwrite"
        )
        
        # Verify SQL execution - check for INSERT OVERWRITE
        sql_calls = [call[0][0] for call in self.mock_spark.sql.call_args_list]
        overwrite_found = any("INSERT OVERWRITE" in sql_call for sql_call in sql_calls)
        self.assertTrue(overwrite_found, f"INSERT OVERWRITE not found in SQL calls: {sql_calls}")
    
    def test_get_table_metadata_success(self):
        """Test successful table metadata retrieval."""
        # Mock Glue client response
        self.handler.glue_client.get_table.return_value = {
            'Table': {
                'Name': 'test_table',
                'DatabaseName': 'test_db',
                'Parameters': {
                    'table_type': 'ICEBERG',
                    'identifier-field-ids': '1,2',
                    'format-version': '2'
                },
                'StorageDescriptor': {
                    'Location': 's3://test-bucket/warehouse/test_db/test_table/',
                    'Columns': [
                        {'Name': 'id', 'Type': 'bigint', 'Comment': 'Primary key'},
                        {'Name': 'name', 'Type': 'string', 'Comment': 'Name field'}
                    ]
                },
                'CreateTime': datetime.now(timezone.utc),
                'UpdateTime': datetime.now(timezone.utc)
            }
        }
        
        # Execute test
        result = self.handler.get_table_metadata("test_db", "test_table", "123456789012")
        
        # Verify result
        self.assertIsInstance(result, IcebergTableMetadata)
        self.assertEqual(result.database, "test_db")
        self.assertEqual(result.table, "test_table")
        self.assertEqual(result.identifier_field_ids, [1, 2])
        self.assertIn('fields', result.schema)
    
    def test_get_table_metadata_non_iceberg(self):
        """Test metadata retrieval for non-Iceberg table."""
        # Mock Glue client response for non-Iceberg table
        self.handler.glue_client.get_table.return_value = {
            'Table': {
                'Parameters': {'table_type': 'EXTERNAL_TABLE'}
            }
        }
        
        # Execute test and expect exception
        with self.assertRaises(IcebergValidationError):
            self.handler.get_table_metadata("test_db", "test_table")


class TestIcebergSchemaManager(unittest.TestCase):
    """Comprehensive tests for IcebergSchemaManager data type mapping and schema creation."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.schema_manager = IcebergSchemaManager()
        
        # Sample JDBC metadata
        self.sample_metadata = MockResultSetMetaData([
            ("id", "INTEGER", 10, 0, 0),  # Not nullable
            ("name", "VARCHAR", 255, 0, 1),  # Nullable
            ("created_at", "TIMESTAMP", 0, 0, 1),  # Nullable
            ("amount", "DECIMAL", 10, 2, 1),  # Nullable
            ("is_active", "BOOLEAN", 0, 0, 1)  # Nullable
        ])
    
    def test_create_iceberg_schema_from_jdbc_success(self):
        """Test successful schema creation from JDBC metadata."""
        # Execute test
        result = self.schema_manager.create_iceberg_schema_from_jdbc(
            jdbc_metadata=self.sample_metadata,
            bookmark_column="id",
            source_engine="postgresql",
            schema_id=0
        )
        
        # Verify result
        self.assertIsInstance(result, IcebergSchema)
        self.assertEqual(result.schema_id, 0)
        self.assertEqual(len(result.fields), 5)
        self.assertEqual(result.identifier_field_ids, [1])  # id column
        
        # Verify field mappings
        fields_by_name = {field.name: field for field in result.fields}
        
        self.assertEqual(fields_by_name['id'].type, 'int')
        self.assertTrue(fields_by_name['id'].required)
        
        self.assertEqual(fields_by_name['name'].type, 'string')
        self.assertFalse(fields_by_name['name'].required)
        
        self.assertEqual(fields_by_name['created_at'].type, 'timestamp')
        self.assertEqual(fields_by_name['amount'].type, 'decimal(10,2)')
        self.assertEqual(fields_by_name['is_active'].type, 'boolean')
    
    def test_create_iceberg_schema_from_jdbc_no_bookmark(self):
        """Test schema creation without bookmark column."""
        # Execute test
        result = self.schema_manager.create_iceberg_schema_from_jdbc(
            jdbc_metadata=self.sample_metadata,
            bookmark_column=None,
            source_engine="postgresql"
        )
        
        # Verify no identifier field IDs
        self.assertEqual(result.identifier_field_ids, [])
    
    def test_create_iceberg_schema_from_jdbc_invalid_metadata(self):
        """Test schema creation with invalid metadata."""
        # Execute test with None metadata
        with self.assertRaises(IcebergValidationError):
            self.schema_manager.create_iceberg_schema_from_jdbc(
                jdbc_metadata=None,
                bookmark_column="id"
            )
        
        # Execute test with empty metadata
        empty_metadata = MockResultSetMetaData([])
        with self.assertRaises(IcebergValidationError):
            self.schema_manager.create_iceberg_schema_from_jdbc(
                jdbc_metadata=empty_metadata,
                bookmark_column="id"
            )
    
    def test_map_jdbc_to_iceberg_types_basic_types(self):
        """Test JDBC to Iceberg type mapping for basic types."""
        test_cases = [
            ("VARCHAR", None, None, "string"),
            ("INTEGER", None, None, "int"),
            ("BIGINT", None, None, "long"),
            ("FLOAT", None, None, "float"),
            ("DOUBLE", None, None, "double"),
            ("BOOLEAN", None, None, "boolean"),
            ("DATE", None, None, "date"),
            ("TIMESTAMP", None, None, "timestamp"),
            ("BINARY", None, None, "binary")
        ]
        
        for jdbc_type, precision, scale, expected_iceberg_type in test_cases:
            with self.subTest(jdbc_type=jdbc_type):
                result = self.schema_manager.map_jdbc_to_iceberg_types(
                    jdbc_type, precision, scale
                )
                self.assertEqual(result, expected_iceberg_type)
    
    def test_map_jdbc_to_iceberg_types_decimal_types(self):
        """Test JDBC to Iceberg type mapping for decimal types."""
        test_cases = [
            ("DECIMAL", 10, 2, "decimal(10,2)"),
            ("NUMERIC", 18, 4, "decimal(18,4)"),
            ("NUMBER", 38, 0, "decimal(38,0)")
        ]
        
        for jdbc_type, precision, scale, expected_iceberg_type in test_cases:
            with self.subTest(jdbc_type=jdbc_type, precision=precision, scale=scale):
                result = self.schema_manager.map_jdbc_to_iceberg_types(
                    jdbc_type, precision, scale
                )
                self.assertEqual(result, expected_iceberg_type)
    
    def test_map_jdbc_to_iceberg_types_unsupported_type(self):
        """Test mapping of unsupported JDBC type."""
        with self.assertRaises(IcebergValidationError):
            self.schema_manager.map_jdbc_to_iceberg_types("UNSUPPORTED_TYPE")
    
    def test_map_jdbc_to_iceberg_types_empty_type(self):
        """Test mapping of empty JDBC type."""
        with self.assertRaises(IcebergValidationError):
            self.schema_manager.map_jdbc_to_iceberg_types("")
    
    def test_add_identifier_field_ids_schema_object(self):
        """Test adding identifier field IDs to IcebergSchema object."""
        # Create test schema
        schema = IcebergSchema(
            schema_id=0,
            fields=[
                IcebergSchemaField(id=1, name="id", type="int", required=True),
                IcebergSchemaField(id=2, name="name", type="string", required=False)
            ]
        )
        
        # Execute test
        result = self.schema_manager.add_identifier_field_ids(schema, "id")
        
        # Verify result
        self.assertEqual(result.identifier_field_ids, [1])
    
    def test_add_identifier_field_ids_schema_dict(self):
        """Test adding identifier field IDs to schema dictionary."""
        # Create test schema dictionary
        schema_dict = {
            'fields': [
                {'id': 1, 'name': 'id', 'type': 'int'},
                {'id': 2, 'name': 'name', 'type': 'string'}
            ]
        }
        
        # Execute test
        result = self.schema_manager.add_identifier_field_ids(schema_dict, "id")
        
        # Verify result
        self.assertEqual(result['identifier-field-ids'], [1])
    
    def test_add_identifier_field_ids_column_not_found(self):
        """Test adding identifier field IDs for non-existent column."""
        schema = IcebergSchema(
            schema_id=0,
            fields=[IcebergSchemaField(id=1, name="id", type="int", required=True)]
        )
        
        # Execute test and expect exception
        with self.assertRaises(IcebergValidationError):
            self.schema_manager.add_identifier_field_ids(schema, "nonexistent_column")
    
    def test_validate_iceberg_schema_valid_schema(self):
        """Test validation of valid Iceberg schema."""
        # Create valid schema
        schema = IcebergSchema(
            schema_id=0,
            fields=[
                IcebergSchemaField(id=1, name="id", type="int", required=True),
                IcebergSchemaField(id=2, name="name", type="string", required=False)
            ],
            identifier_field_ids=[1]
        )
        
        # Execute test
        result = self.schema_manager.validate_iceberg_schema(schema, "test_table")
        
        # Verify result
        self.assertTrue(result['is_valid'])
        self.assertEqual(len(result['errors']), 0)
        self.assertEqual(result['field_count'], 2)
        self.assertTrue(result['has_identifier_field_ids'])
    
    def test_validate_iceberg_schema_invalid_schema(self):
        """Test validation of invalid Iceberg schema."""
        # Create invalid schema (duplicate field IDs)
        schema_dict = {
            'fields': [
                {'id': 1, 'name': 'id', 'type': 'int'},
                {'id': 1, 'name': 'name', 'type': 'string'}  # Duplicate ID
            ]
        }
        
        # Execute test
        result = self.schema_manager.validate_iceberg_schema(schema_dict, "test_table")
        
        # Verify result
        self.assertFalse(result['is_valid'])
        self.assertGreater(len(result['errors']), 0)
        self.assertIn("Duplicate field ID", str(result['errors']))
    
    def test_validate_iceberg_schema_missing_fields(self):
        """Test validation of schema with missing fields."""
        # Create schema without fields
        schema_dict = {}
        
        # Execute test
        result = self.schema_manager.validate_iceberg_schema(schema_dict, "test_table")
        
        # Verify result
        self.assertFalse(result['is_valid'])
        self.assertIn("Schema must contain 'fields' list", result['errors'])
    
    def test_get_table_schema_success(self):
        """Test successful table schema retrieval."""
        # Create test metadata
        metadata = IcebergTableMetadata(
            database="test_db",
            table="test_table",
            location="s3://test-bucket/warehouse/",
            schema={
                'fields': [
                    {'name': 'id', 'type': 'bigint', 'comment': 'Primary key'},
                    {'name': 'name', 'type': 'string', 'comment': 'Name field'}
                ]
            },
            identifier_field_ids=[1]
        )
        
        # Execute test
        result = self.schema_manager.get_table_schema(metadata)
        
        # Verify result
        self.assertIsInstance(result, IcebergSchema)
        self.assertEqual(len(result.fields), 2)
        self.assertEqual(result.identifier_field_ids, [1])
    
    def test_get_table_schema_no_schema(self):
        """Test table schema retrieval with no schema."""
        # Create metadata without schema
        metadata = IcebergTableMetadata(
            database="test_db",
            table="test_table",
            location="s3://test-bucket/warehouse/",
            schema=None
        )
        
        # Execute test and expect exception
        with self.assertRaises(IcebergValidationError):
            self.schema_manager.get_table_schema(metadata)


class TestIcebergModels(unittest.TestCase):
    """Tests for Iceberg models and utility functions."""
    
    def test_iceberg_config_validation(self):
        """Test IcebergConfig validation."""
        # Valid configuration
        config = IcebergConfig(
            database_name="test_db",
            table_name="test_table",
            warehouse_location="s3://test-bucket/warehouse/"
        )
        self.assertEqual(config.database_name, "test_db")
        
        # Invalid warehouse location
        with self.assertRaises(ValueError):
            IcebergConfig(
                database_name="test_db",
                table_name="test_table",
                warehouse_location="invalid-location"
            )
        
        # Invalid format version
        with self.assertRaises(ValueError):
            IcebergConfig(
                database_name="test_db",
                table_name="test_table",
                warehouse_location="s3://test-bucket/warehouse/",
                format_version="3"
            )
    
    def test_iceberg_schema_field_to_dict(self):
        """Test IcebergSchemaField to_dict method."""
        field = IcebergSchemaField(
            id=1,
            name="test_field",
            type="string",
            required=True,
            doc="Test field documentation"
        )
        
        result = field.to_dict()
        
        expected = {
            "id": 1,
            "name": "test_field",
            "type": "string",
            "required": True,
            "doc": "Test field documentation"
        }
        
        self.assertEqual(result, expected)
    
    def test_iceberg_schema_to_dict(self):
        """Test IcebergSchema to_dict method."""
        schema = IcebergSchema(
            schema_id=0,
            fields=[
                IcebergSchemaField(id=1, name="id", type="int", required=True),
                IcebergSchemaField(id=2, name="name", type="string", required=False)
            ],
            identifier_field_ids=[1]
        )
        
        result = schema.to_dict()
        
        self.assertEqual(result["schema-id"], 0)
        self.assertEqual(len(result["fields"]), 2)
        self.assertEqual(result["identifier-field-ids"], [1])
    
    def test_iceberg_schema_get_field_by_name(self):
        """Test IcebergSchema get_field_by_name method."""
        schema = IcebergSchema(
            schema_id=0,
            fields=[
                IcebergSchemaField(id=1, name="id", type="int", required=True),
                IcebergSchemaField(id=2, name="name", type="string", required=False)
            ]
        )
        
        # Test existing field
        field = schema.get_field_by_name("id")
        self.assertIsNotNone(field)
        self.assertEqual(field.name, "id")
        
        # Test non-existing field
        field = schema.get_field_by_name("nonexistent")
        self.assertIsNone(field)
    
    def test_map_jdbc_type_to_iceberg_function(self):
        """Test map_jdbc_type_to_iceberg utility function."""
        # Test basic types
        self.assertEqual(map_jdbc_type_to_iceberg("VARCHAR"), "string")
        self.assertEqual(map_jdbc_type_to_iceberg("INTEGER"), "int")
        self.assertEqual(map_jdbc_type_to_iceberg("BIGINT"), "long")
        
        # Test decimal types
        self.assertEqual(map_jdbc_type_to_iceberg("DECIMAL", 10, 2), "decimal(10,2)")
        self.assertEqual(map_jdbc_type_to_iceberg("NUMERIC", 18, 4), "decimal(18,4)")
        
        # Test unsupported type
        with self.assertRaises(IcebergValidationError):
            map_jdbc_type_to_iceberg("UNSUPPORTED_TYPE")
    
    def test_create_iceberg_field_function(self):
        """Test create_iceberg_field utility function."""
        field = create_iceberg_field(
            field_id=1,
            name="test_field",
            jdbc_type="VARCHAR",
            is_nullable=False,
            doc="Test field"
        )
        
        self.assertEqual(field.id, 1)
        self.assertEqual(field.name, "test_field")
        self.assertEqual(field.type, "string")
        self.assertTrue(field.required)  # Not nullable = required
        self.assertEqual(field.doc, "Test field")


class TestDatabaseEngineManager(unittest.TestCase):
    """Tests for Iceberg parameter validation logic in DatabaseEngineManager."""
    
    def test_is_iceberg_engine(self):
        """Test Iceberg engine detection."""
        self.assertTrue(DatabaseEngineManager.is_iceberg_engine("iceberg"))
        self.assertTrue(DatabaseEngineManager.is_iceberg_engine("ICEBERG"))
        self.assertFalse(DatabaseEngineManager.is_iceberg_engine("postgresql"))
        self.assertFalse(DatabaseEngineManager.is_iceberg_engine("oracle"))
    
    def test_get_supported_engines_includes_iceberg(self):
        """Test that supported engines include Iceberg."""
        engines = DatabaseEngineManager.get_supported_engines()
        self.assertIn("iceberg", engines)
    
    def test_requires_jdbc_iceberg_false(self):
        """Test that Iceberg engine doesn't require JDBC."""
        self.assertFalse(DatabaseEngineManager.requires_jdbc("iceberg"))
        self.assertTrue(DatabaseEngineManager.requires_jdbc("postgresql"))
    
    def test_validate_iceberg_config_valid(self):
        """Test validation of valid Iceberg configuration."""
        config = {
            'database_name': 'test_db',
            'table_name': 'test_table',
            'warehouse_location': 's3://test-bucket/warehouse/',
            'catalog_id': '123456789012',
            'format_version': '2'
        }
        
        result = DatabaseEngineManager.validate_iceberg_config(config)
        self.assertTrue(result)
    
    def test_validate_iceberg_config_missing_required(self):
        """Test validation with missing required parameters."""
        config = {
            'database_name': 'test_db',
            # Missing table_name and warehouse_location
        }
        
        result = DatabaseEngineManager.validate_iceberg_config(config)
        self.assertFalse(result)
    
    def test_validate_iceberg_config_invalid_warehouse_location(self):
        """Test validation with invalid warehouse location."""
        config = {
            'database_name': 'test_db',
            'table_name': 'test_table',
            'warehouse_location': 'invalid-location'  # Not S3 URL
        }
        
        result = DatabaseEngineManager.validate_iceberg_config(config)
        self.assertFalse(result)
    
    def test_validate_iceberg_config_invalid_format_version(self):
        """Test validation with invalid format version."""
        config = {
            'database_name': 'test_db',
            'table_name': 'test_table',
            'warehouse_location': 's3://test-bucket/warehouse/',
            'format_version': '3'  # Invalid version
        }
        
        result = DatabaseEngineManager.validate_iceberg_config(config)
        self.assertFalse(result)
    
    def test_validate_iceberg_config_invalid_catalog_id(self):
        """Test validation with invalid catalog ID."""
        config = {
            'database_name': 'test_db',
            'table_name': 'test_table',
            'warehouse_location': 's3://test-bucket/warehouse/',
            'catalog_id': 'invalid-id'  # Not 12-digit AWS account ID
        }
        
        result = DatabaseEngineManager.validate_iceberg_config(config)
        self.assertFalse(result)
    
    def test_validate_connection_string_iceberg_bypass(self):
        """Test that connection string validation is bypassed for Iceberg."""
        # Should return True regardless of connection string for Iceberg
        result = DatabaseEngineManager.validate_connection_string("iceberg", "any_string")
        self.assertTrue(result)
        
        result = DatabaseEngineManager.validate_connection_string("iceberg", "")
        self.assertTrue(result)
    
    def test_get_driver_class_iceberg_error(self):
        """Test that getting driver class for Iceberg raises error."""
        with self.assertRaises(ValueError) as context:
            DatabaseEngineManager.get_driver_class("iceberg")
        
        self.assertIn("Iceberg engine does not use JDBC drivers", str(context.exception))


class TestJobConfigurationParser(unittest.TestCase):
    """Tests for Iceberg parameter validation in JobConfigurationParser."""
    
    def test_validate_iceberg_parameters_source(self):
        """Test validation of source Iceberg parameters."""
        args = {
            'SOURCE_ENGINE_TYPE': 'iceberg',
            'SOURCE_DATABASE': 'test_db',
            'SOURCE_SCHEMA': 'test_table',  # Used as table_name for Iceberg
            'SOURCE_WAREHOUSE_LOCATION': 's3://test-bucket/warehouse/',
            'SOURCE_CATALOG_ID': '123456789012',
            'SOURCE_FORMAT_VERSION': '2'
        }
        
        result = JobConfigurationParser.validate_iceberg_parameters(args, 'SOURCE')
        
        self.assertEqual(result['database_name'], 'test_db')
        self.assertEqual(result['table_name'], 'test_table')
        self.assertEqual(result['warehouse_location'], 's3://test-bucket/warehouse/')
        self.assertEqual(result['catalog_id'], '123456789012')
        self.assertEqual(result['format_version'], '2')
    
    def test_validate_iceberg_parameters_target(self):
        """Test validation of target Iceberg parameters."""
        args = {
            'TARGET_ENGINE_TYPE': 'iceberg',
            'TARGET_DATABASE': 'target_db',
            'TARGET_SCHEMA': 'target_table',
            'TARGET_WAREHOUSE_LOCATION': 's3://target-bucket/warehouse/'
        }
        
        result = JobConfigurationParser.validate_iceberg_parameters(args, 'TARGET')
        
        self.assertEqual(result['database_name'], 'target_db')
        self.assertEqual(result['table_name'], 'target_table')
        self.assertEqual(result['warehouse_location'], 's3://target-bucket/warehouse/')
    
    def test_validate_iceberg_parameters_non_iceberg(self):
        """Test validation for non-Iceberg engine returns empty dict."""
        args = {
            'SOURCE_ENGINE_TYPE': 'postgresql'
        }
        
        result = JobConfigurationParser.validate_iceberg_parameters(args, 'SOURCE')
        self.assertEqual(result, {})
    
    def test_validate_iceberg_parameters_missing_required(self):
        """Test validation with missing required Iceberg parameters."""
        args = {
            'SOURCE_ENGINE_TYPE': 'iceberg',
            'SOURCE_DATABASE': 'test_db'
            # Missing SOURCE_SCHEMA and SOURCE_WAREHOUSE_LOCATION
        }
        
        with self.assertRaises(ValueError) as context:
            JobConfigurationParser.validate_iceberg_parameters(args, 'SOURCE')
        
        self.assertIn("Missing required Iceberg parameter", str(context.exception))
    
    def test_validate_engine_specific_parameters_iceberg(self):
        """Test engine-specific parameter validation for Iceberg."""
        args = {
            'SOURCE_ENGINE_TYPE': 'iceberg',
            'SOURCE_DATABASE': 'test_db',
            'SOURCE_SCHEMA': 'test_table',
            'SOURCE_WAREHOUSE_LOCATION': 's3://test-bucket/warehouse/'
        }
        
        # Should not raise exception
        JobConfigurationParser._validate_engine_specific_parameters(args, 'SOURCE', 'iceberg')
    
    def test_validate_engine_specific_parameters_iceberg_missing(self):
        """Test engine-specific parameter validation for Iceberg with missing params."""
        args = {
            'SOURCE_ENGINE_TYPE': 'iceberg',
            'SOURCE_DATABASE': 'test_db'
            # Missing required parameters
        }
        
        with self.assertRaises(RuntimeError) as context:
            JobConfigurationParser._validate_engine_specific_parameters(args, 'SOURCE', 'iceberg')
        
        self.assertIn("Missing required Iceberg parameters", str(context.exception))


class TestEnhancedBookmarkManager(unittest.TestCase):
    """Tests for enhanced BookmarkManager with identifier-field-ids functionality."""
    
    def setUp(self):
        """Set up test fixtures."""
        # Mock Glue context and job
        self.mock_glue_context = Mock()
        self.mock_job = Mock()
        self.job_name = "test-iceberg-job"
        
        # Create BookmarkManager instance
        self.bookmark_manager = JobBookmarkManager(
            glue_context=self.mock_glue_context,
            job_name=self.job_name,
            job=self.mock_job
        )
        
        # Test data
        self.test_database = "test_database"
        self.test_table = "test_iceberg_table"
        self.test_catalog_id = "123456789012"
    
    @patch('boto3.client')
    def test_extract_identifier_field_ids_success(self, mock_boto_client):
        """Test successful extraction of identifier-field-ids."""
        # Setup mock Glue client
        mock_glue_client = Mock()
        mock_boto_client.return_value = mock_glue_client
        mock_glue_client.get_table.return_value = {
            'Table': {
                'Parameters': {
                    'table_type': 'ICEBERG',
                    'identifier-field-ids': '1,2,3'
                }
            }
        }
        
        # Execute test
        result = self.bookmark_manager.extract_identifier_field_ids(
            database=self.test_database,
            table=self.test_table,
            catalog_id=self.test_catalog_id
        )
        
        # Verify result
        self.assertEqual(result, [1, 2, 3])
    
    @patch('boto3.client')
    def test_extract_identifier_field_ids_invalid_format(self, mock_boto_client):
        """Test extraction with invalid identifier-field-ids format."""
        # Setup mock with invalid format
        mock_glue_client = Mock()
        mock_boto_client.return_value = mock_glue_client
        mock_glue_client.get_table.return_value = {
            'Table': {
                'Parameters': {
                    'table_type': 'ICEBERG',
                    'identifier-field-ids': 'invalid,format,abc'
                }
            }
        }
        
        # Execute test
        result = self.bookmark_manager.extract_identifier_field_ids(
            database=self.test_database,
            table=self.test_table
        )
        
        # Should return None due to parsing error
        self.assertIsNone(result)
    
    @patch('boto3.client')
    def test_extract_identifier_field_ids_non_iceberg(self, mock_boto_client):
        """Test extraction from non-Iceberg table."""
        # Setup mock for non-Iceberg table
        mock_glue_client = Mock()
        mock_boto_client.return_value = mock_glue_client
        mock_glue_client.get_table.return_value = {
            'Table': {
                'Parameters': {
                    'table_type': 'EXTERNAL_TABLE'
                }
            }
        }
        
        # Execute test
        result = self.bookmark_manager.extract_identifier_field_ids(
            database=self.test_database,
            table=self.test_table
        )
        
        # Should return None for non-Iceberg table
        self.assertIsNone(result)
    
    @patch('boto3.client')
    def test_get_iceberg_bookmark_column_success(self, mock_boto_client):
        """Test successful bookmark column extraction."""
        # Setup mock Glue client
        mock_glue_client = Mock()
        mock_boto_client.return_value = mock_glue_client
        mock_glue_client.get_table.return_value = {
            'Table': {
                'Parameters': {
                    'table_type': 'ICEBERG',
                    'identifier-field-ids': '1'
                },
                'StorageDescriptor': {
                    'Columns': [
                        {'Name': 'id', 'Type': 'bigint'},
                        {'Name': 'name', 'Type': 'string'}
                    ]
                }
            }
        }
        
        # Execute test
        result = self.bookmark_manager.get_iceberg_bookmark_column(
            database=self.test_database,
            table=self.test_table
        )
        
        # Verify bookmark column is extracted (first field with ID 1)
        self.assertEqual(result, 'id')
    
    def test_fallback_to_traditional_bookmark_timestamp(self):
        """Test fallback bookmark detection with timestamp column."""
        # Mock DataFrame with timestamp column
        mock_dataframe = Mock()
        mock_schema = Mock()
        
        # Create mock fields
        mock_id_field = Mock()
        mock_id_field.name = 'id'
        mock_id_field.dataType.__str__ = Mock(return_value='LongType()')
        
        mock_timestamp_field = Mock()
        mock_timestamp_field.name = 'updated_at'
        mock_timestamp_field.dataType.__str__ = Mock(return_value='TimestampType()')
        
        mock_schema.fields = [mock_id_field, mock_timestamp_field]
        mock_dataframe.schema = mock_schema
        
        # Execute test
        result = self.bookmark_manager.fallback_to_traditional_bookmark(
            dataframe=mock_dataframe,
            table_name=self.test_table
        )
        
        # Verify timestamp column is detected
        self.assertEqual(result, 'updated_at')
    
    def test_fallback_to_traditional_bookmark_id_only(self):
        """Test fallback bookmark detection with ID column only."""
        # Mock DataFrame with only ID column
        mock_dataframe = Mock()
        mock_schema = Mock()
        
        mock_id_field = Mock()
        mock_id_field.name = 'id'
        mock_id_field.dataType.__str__ = Mock(return_value='LongType()')
        
        mock_name_field = Mock()
        mock_name_field.name = 'name'
        mock_name_field.dataType.__str__ = Mock(return_value='StringType()')
        
        mock_schema.fields = [mock_id_field, mock_name_field]
        mock_dataframe.schema = mock_schema
        
        # Execute test
        result = self.bookmark_manager.fallback_to_traditional_bookmark(
            dataframe=mock_dataframe,
            table_name=self.test_table
        )
        
        # Verify ID column is detected
        self.assertEqual(result, 'id')
    
    def test_fallback_to_traditional_bookmark_no_suitable_column(self):
        """Test fallback when no suitable bookmark column exists."""
        # Mock DataFrame with only string columns
        mock_dataframe = Mock()
        mock_schema = Mock()
        
        mock_name_field = Mock()
        mock_name_field.name = 'name'
        mock_name_field.dataType.__str__ = Mock(return_value='StringType()')
        
        mock_desc_field = Mock()
        mock_desc_field.name = 'description'
        mock_desc_field.dataType.__str__ = Mock(return_value='StringType()')
        
        mock_schema.fields = [mock_name_field, mock_desc_field]
        mock_dataframe.schema = mock_schema
        
        # Execute test
        result = self.bookmark_manager.fallback_to_traditional_bookmark(
            dataframe=mock_dataframe,
            table_name=self.test_table
        )
        
        # Should return None
        self.assertIsNone(result)
    
    @patch('boto3.client')
    def test_initialize_iceberg_bookmark_state_with_identifier_field_ids(self, mock_boto_client):
        """Test Iceberg bookmark state initialization with identifier-field-ids."""
        # Setup mock Glue client
        mock_glue_client = Mock()
        mock_boto_client.return_value = mock_glue_client
        mock_glue_client.get_table.return_value = {
            'Table': {
                'Parameters': {
                    'table_type': 'ICEBERG',
                    'identifier-field-ids': '1'
                },
                'StorageDescriptor': {
                    'Columns': [
                        {'Name': 'id', 'Type': 'bigint'},
                        {'Name': 'name', 'Type': 'string'}
                    ]
                }
            }
        }
        
        # Execute test
        result = self.bookmark_manager.initialize_iceberg_bookmark_state(
            database=self.test_database,
            table=self.test_table,
            incremental_strategy='timestamp',
            catalog_id=self.test_catalog_id
        )
        
        # Verify bookmark state
        self.assertIsInstance(result, JobBookmarkState)
        self.assertEqual(result.table_name, f"{self.test_database}.{self.test_table}")
        self.assertEqual(result.incremental_strategy, 'timestamp')
        self.assertEqual(result.incremental_column, 'id')
        self.assertTrue(result.is_first_run)
    
    @patch('boto3.client')
    def test_is_iceberg_table_detection_by_engine_type(self, mock_boto_client):
        """Test Iceberg table detection by engine type."""
        # Test with engine type hint
        result = self.bookmark_manager._is_iceberg_table(
            table_name=self.test_table,
            engine_type='iceberg'
        )
        self.assertTrue(result)
        
        # Test with non-Iceberg engine type
        result = self.bookmark_manager._is_iceberg_table(
            table_name=self.test_table,
            engine_type='postgresql'
        )
        self.assertFalse(result)
    
    @patch('boto3.client')
    def test_is_iceberg_table_detection_by_catalog_lookup(self, mock_boto_client):
        """Test Iceberg table detection by catalog lookup."""
        # Setup mock Glue client
        mock_glue_client = Mock()
        mock_boto_client.return_value = mock_glue_client
        mock_glue_client.get_table.return_value = {
            'Table': {
                'Parameters': {'table_type': 'ICEBERG'}
            }
        }
        
        # Test with database lookup
        result = self.bookmark_manager._is_iceberg_table(
            table_name=self.test_table,
            database=self.test_database
        )
        self.assertTrue(result)


class TestMockGlueDataCatalogOperations(unittest.TestCase):
    """Mock tests for Glue Data Catalog operations."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.mock_glue_client = Mock()
    
    def test_mock_get_table_success(self):
        """Test mocked get_table operation success."""
        # Setup mock response
        expected_response = {
            'Table': {
                'Name': 'test_table',
                'DatabaseName': 'test_db',
                'Parameters': {
                    'table_type': 'ICEBERG',
                    'identifier-field-ids': '1,2'
                },
                'StorageDescriptor': {
                    'Location': 's3://test-bucket/warehouse/',
                    'Columns': [
                        {'Name': 'id', 'Type': 'bigint'},
                        {'Name': 'name', 'Type': 'string'}
                    ]
                }
            }
        }
        
        self.mock_glue_client.get_table.return_value = expected_response
        
        # Execute mock operation
        result = self.mock_glue_client.get_table(
            DatabaseName='test_db',
            Name='test_table'
        )
        
        # Verify mock behavior
        self.assertEqual(result, expected_response)
        self.mock_glue_client.get_table.assert_called_once_with(
            DatabaseName='test_db',
            Name='test_table'
        )
    
    def test_mock_get_table_not_found(self):
        """Test mocked get_table operation with table not found."""
        # Setup mock to raise EntityNotFoundException
        self.mock_glue_client.get_table.side_effect = ClientError(
            error_response={'Error': {'Code': 'EntityNotFoundException'}},
            operation_name='GetTable'
        )
        
        # Execute and verify exception
        with self.assertRaises(ClientError) as context:
            self.mock_glue_client.get_table(
                DatabaseName='test_db',
                Name='nonexistent_table'
            )
        
        self.assertEqual(
            context.exception.response['Error']['Code'],
            'EntityNotFoundException'
        )
    
    def test_mock_update_table_success(self):
        """Test mocked update_table operation success."""
        # Setup mock response
        self.mock_glue_client.update_table.return_value = {}
        
        table_input = {
            'Name': 'test_table',
            'Parameters': {
                'table_type': 'ICEBERG',
                'identifier-field-ids': '1,2,3'
            }
        }
        
        # Execute mock operation
        self.mock_glue_client.update_table(
            DatabaseName='test_db',
            TableInput=table_input
        )
        
        # Verify mock behavior
        self.mock_glue_client.update_table.assert_called_once_with(
            DatabaseName='test_db',
            TableInput=table_input
        )
    
    def test_mock_catalog_operations_with_catalog_id(self):
        """Test mocked catalog operations with cross-account catalog ID."""
        catalog_id = '123456789012'
        
        # Setup mock responses
        self.mock_glue_client.get_table.return_value = {
            'Table': {'Name': 'test_table'}
        }
        
        # Execute operations with catalog ID
        self.mock_glue_client.get_table(
            DatabaseName='test_db',
            Name='test_table',
            CatalogId=catalog_id
        )
        
        # Verify catalog ID is passed correctly
        self.mock_glue_client.get_table.assert_called_once_with(
            DatabaseName='test_db',
            Name='test_table',
            CatalogId=catalog_id
        )
    
    def test_mock_catalog_error_handling(self):
        """Test mocked catalog error handling scenarios."""
        # Test various AWS service errors
        error_scenarios = [
            ('AccessDeniedException', 'Access denied to catalog'),
            ('InvalidInputException', 'Invalid input parameters'),
            ('InternalServiceException', 'Internal service error'),
            ('ThrottlingException', 'Request throttled')
        ]
        
        for error_code, error_message in error_scenarios:
            with self.subTest(error_code=error_code):
                # Setup mock to raise specific error
                self.mock_glue_client.get_table.side_effect = ClientError(
                    error_response={
                        'Error': {
                            'Code': error_code,
                            'Message': error_message
                        }
                    },
                    operation_name='GetTable'
                )
                
                # Execute and verify exception
                with self.assertRaises(ClientError) as context:
                    self.mock_glue_client.get_table(
                        DatabaseName='test_db',
                        Name='test_table'
                    )
                
                self.assertEqual(
                    context.exception.response['Error']['Code'],
                    error_code
                )


class TestIcebergExceptionHandling(unittest.TestCase):
    """Tests for Iceberg-specific exception handling."""
    
    def test_iceberg_engine_error_base(self):
        """Test base IcebergEngineError exception."""
        error = IcebergEngineError(
            message="Test error",
            error_code="TEST_ERROR",
            context={"key": "value"}
        )
        
        self.assertEqual(str(error), "[TEST_ERROR] Test error")
        self.assertEqual(error.error_code, "TEST_ERROR")
        self.assertEqual(error.context, {"key": "value"})
    
    def test_iceberg_table_not_found_error(self):
        """Test IcebergTableNotFoundError exception."""
        error = IcebergTableNotFoundError(
            database="test_db",
            table="test_table",
            catalog_id="123456789012"
        )
        
        self.assertIn("test_db.test_table", str(error))
        self.assertIn("123456789012", str(error))
        self.assertEqual(error.error_code, "TABLE_NOT_FOUND")
    
    def test_iceberg_schema_creation_error(self):
        """Test IcebergSchemaCreationError exception."""
        source_error = ValueError("Invalid column type")
        error = IcebergSchemaCreationError(
            message="Schema creation failed",
            database="test_db",
            table="test_table",
            source_error=source_error
        )
        
        self.assertIn("test_db.test_table", str(error))
        self.assertIn("Schema creation failed", str(error))
        self.assertIn("Invalid column type", str(error))
        self.assertEqual(error.error_code, "SCHEMA_CREATION_ERROR")
    
    def test_iceberg_catalog_error(self):
        """Test IcebergCatalogError exception."""
        aws_error = ClientError(
            error_response={'Error': {'Code': 'AccessDenied'}},
            operation_name='GetTable'
        )
        
        error = IcebergCatalogError(
            message="Catalog operation failed",
            operation="GetTable",
            catalog_id="123456789012",
            aws_error=aws_error
        )
        
        self.assertIn("GetTable", str(error))
        self.assertIn("123456789012", str(error))
        self.assertEqual(error.error_code, "CATALOG_ERROR")
    
    def test_iceberg_connection_error(self):
        """Test IcebergConnectionError exception."""
        spark_error = Exception("Spark session error")
        error = IcebergConnectionError(
            message="Connection failed",
            warehouse_location="s3://test-bucket/warehouse/",
            spark_error=spark_error
        )
        
        self.assertIn("Connection failed", str(error))
        self.assertIn("s3://test-bucket/warehouse/", str(error))
        self.assertEqual(error.error_code, "CONNECTION_ERROR")
    
    def test_iceberg_validation_error(self):
        """Test IcebergValidationError exception."""
        error = IcebergValidationError(
            message="Validation failed",
            field="warehouse_location",
            value="invalid-location"
        )
        
        self.assertIn("Validation failed", str(error))
        self.assertIn("warehouse_location", str(error))
        self.assertEqual(error.error_code, "VALIDATION_ERROR")


if __name__ == '__main__':
    # Create test suite
    test_suite = unittest.TestSuite()
    
    # Add all test classes
    test_classes = [
        TestIcebergConnectionHandler,
        TestIcebergSchemaManager,
        TestIcebergModels,
        TestDatabaseEngineManager,
        TestJobConfigurationParser,
        TestEnhancedBookmarkManager,
        TestMockGlueDataCatalogOperations,
        TestIcebergExceptionHandling
    ]
    
    for test_class in test_classes:
        tests = unittest.TestLoader().loadTestsFromTestCase(test_class)
        test_suite.addTests(tests)
    
    # Run the tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(test_suite)
    
    # Print summary
    print(f"\n{'='*60}")
    print(f"ICEBERG COMPONENTS UNIT TEST SUMMARY")
    print(f"{'='*60}")
    print(f"Tests run: {result.testsRun}")
    print(f"Failures: {len(result.failures)}")
    print(f"Errors: {len(result.errors)}")
    print(f"Success rate: {((result.testsRun - len(result.failures) - len(result.errors)) / result.testsRun * 100):.1f}%")
    
    if result.failures:
        print(f"\nFAILURES ({len(result.failures)}):")
        for test, traceback in result.failures:
            error_msg = traceback.split('AssertionError: ')[-1].split('\n')[0] if 'AssertionError: ' in traceback else 'Unknown failure'
            print(f"- {test}: {error_msg}")
    
    if result.errors:
        print(f"\nERRORS ({len(result.errors)}):")
        for test, traceback in result.errors:
            error_lines = traceback.split('\n')
            error_msg = error_lines[-2] if error_lines else 'Unknown error'
            print(f"- {test}: {error_msg}")
    
    # Exit with appropriate code
    exit_code = 0 if result.wasSuccessful() else 1
    print(f"\nTest execution completed with exit code: {exit_code}")
    exit(exit_code)