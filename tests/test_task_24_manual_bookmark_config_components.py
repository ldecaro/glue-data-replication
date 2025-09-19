"""
Unit tests for manual bookmark configuration components (Task 24).

This module provides comprehensive unit tests for:
- ManualBookmarkConfig class validation and creation
- Manual configuration parsing with valid and invalid JSON structures
- JDBC metadata querying with mocked database connections
- Data type to strategy mapping for all supported JDBC types

Requirements covered: 8.2, 8.3, 8.6
"""

import pytest
import unittest
import json
from unittest.mock import Mock, MagicMock, patch
from typing import Dict, Any

from src.glue_job.storage.manual_bookmark_config import (
    ManualBookmarkConfig, 
    BookmarkStrategyResolver
)


class TestManualBookmarkConfigValidation(unittest.TestCase):
    """Test cases for ManualBookmarkConfig class validation and creation."""
    
    def test_valid_config_creation_basic(self):
        """Test creating valid ManualBookmarkConfig with basic names."""
        config = ManualBookmarkConfig(
            table_name="customers",
            column_name="updated_at"
        )
        
        self.assertEqual(config.table_name, "customers")
        self.assertEqual(config.column_name, "updated_at")
    
    def test_valid_config_creation_with_underscores(self):
        """Test creating valid ManualBookmarkConfig with underscores."""
        config = ManualBookmarkConfig(
            table_name="customer_orders_history",
            column_name="last_modified_timestamp"
        )
        
        self.assertEqual(config.table_name, "customer_orders_history")
        self.assertEqual(config.column_name, "last_modified_timestamp")
    
    def test_valid_config_creation_with_numbers(self):
        """Test creating valid ManualBookmarkConfig with numbers (not at start)."""
        config = ManualBookmarkConfig(
            table_name="table_v2_backup",
            column_name="col_123_updated"
        )
        
        self.assertEqual(config.table_name, "table_v2_backup")
        self.assertEqual(config.column_name, "col_123_updated")
    
    def test_valid_config_creation_starting_with_underscore(self):
        """Test creating valid ManualBookmarkConfig starting with underscore."""
        config = ManualBookmarkConfig(
            table_name="_internal_table",
            column_name="_system_timestamp"
        )
        
        self.assertEqual(config.table_name, "_internal_table")
        self.assertEqual(config.column_name, "_system_timestamp")
    
    def test_valid_config_creation_mixed_case(self):
        """Test creating valid ManualBookmarkConfig with mixed case."""
        config = ManualBookmarkConfig(
            table_name="CustomerOrders",
            column_name="LastUpdatedAt"
        )
        
        self.assertEqual(config.table_name, "CustomerOrders")
        self.assertEqual(config.column_name, "LastUpdatedAt")
    
    def test_whitespace_trimming_both_fields(self):
        """Test that whitespace is properly trimmed from both fields."""
        config = ManualBookmarkConfig(
            table_name="  customers  ",
            column_name="  updated_at  "
        )
        
        self.assertEqual(config.table_name, "customers")
        self.assertEqual(config.column_name, "updated_at")
    
    def test_whitespace_trimming_tabs_and_newlines(self):
        """Test trimming of tabs and newlines."""
        config = ManualBookmarkConfig(
            table_name="\t\ncustomers\t\n",
            column_name="\t\nupdated_at\t\n"
        )
        
        self.assertEqual(config.table_name, "customers")
        self.assertEqual(config.column_name, "updated_at")
    
    def test_empty_table_name_validation(self):
        """Test validation fails for empty table name."""
        with self.assertRaises(ValueError) as context:
            ManualBookmarkConfig(
                table_name="",
                column_name="updated_at"
            )
        
        self.assertIn("table_name is required", str(context.exception))
    
    def test_empty_column_name_validation(self):
        """Test validation fails for empty column name."""
        with self.assertRaises(ValueError) as context:
            ManualBookmarkConfig(
                table_name="customers",
                column_name=""
            )
        
        self.assertIn("column_name is required", str(context.exception))
    
    def test_none_table_name_validation(self):
        """Test validation fails for None table name."""
        with self.assertRaises(ValueError) as context:
            ManualBookmarkConfig(
                table_name=None,
                column_name="updated_at"
            )
        
        self.assertIn("table_name is required", str(context.exception))
    
    def test_none_column_name_validation(self):
        """Test validation fails for None column name."""
        with self.assertRaises(ValueError) as context:
            ManualBookmarkConfig(
                table_name="customers",
                column_name=None
            )
        
        self.assertIn("column_name is required", str(context.exception))
    
    def test_non_string_table_name_validation(self):
        """Test validation fails for non-string table name."""
        with self.assertRaises(ValueError) as context:
            ManualBookmarkConfig(
                table_name=123,
                column_name="updated_at"
            )
        
        self.assertIn("table_name is required and must be a non-empty string", str(context.exception))
    
    def test_non_string_column_name_validation(self):
        """Test validation fails for non-string column name."""
        with self.assertRaises(ValueError) as context:
            ManualBookmarkConfig(
                table_name="customers",
                column_name=123
            )
        
        self.assertIn("column_name is required and must be a non-empty string", str(context.exception))
    
    def test_whitespace_only_table_name_validation(self):
        """Test validation fails for whitespace-only table name."""
        with self.assertRaises(ValueError) as context:
            ManualBookmarkConfig(
                table_name="   \t\n   ",
                column_name="updated_at"
            )
        
        self.assertIn("table_name cannot be empty or whitespace only", str(context.exception))
    
    def test_whitespace_only_column_name_validation(self):
        """Test validation fails for whitespace-only column name."""
        with self.assertRaises(ValueError) as context:
            ManualBookmarkConfig(
                table_name="customers",
                column_name="   \t\n   "
            )
        
        self.assertIn("column_name cannot be empty or whitespace only", str(context.exception))
    
    def test_invalid_table_name_special_characters(self):
        """Test validation fails for table name with invalid special characters."""
        invalid_names = [
            "table-name", "table.name", "table@name", "table name", 
            "table$name", "table#name", "table%name", "table&name",
            "table*name", "table+name", "table=name", "table[name]",
            "table{name}", "table|name", "table\\name", "table/name",
            "table:name", "table;name", "table\"name", "table'name",
            "table<name>", "table,name", "table?name", "table!name"
        ]
        
        for invalid_name in invalid_names:
            with self.assertRaises(ValueError) as context:
                ManualBookmarkConfig(
                    table_name=invalid_name,
                    column_name="updated_at"
                )
            
            self.assertIn("must contain only alphanumeric characters", str(context.exception))
            self.assertIn("cannot start with a number", str(context.exception))
    
    def test_invalid_column_name_special_characters(self):
        """Test validation fails for column name with invalid special characters."""
        invalid_names = [
            "col-name", "col.name", "col@name", "col name", 
            "col$name", "col#name", "col%name", "col&name"
        ]
        
        for invalid_name in invalid_names:
            with self.assertRaises(ValueError) as context:
                ManualBookmarkConfig(
                    table_name="customers",
                    column_name=invalid_name
                )
            
            self.assertIn("must contain only alphanumeric characters", str(context.exception))
    
    def test_invalid_table_name_starting_with_number(self):
        """Test validation fails for table name starting with number."""
        invalid_names = ["123_customers", "1table", "9_orders", "0test"]
        
        for invalid_name in invalid_names:
            with self.assertRaises(ValueError) as context:
                ManualBookmarkConfig(
                    table_name=invalid_name,
                    column_name="updated_at"
                )
            
            self.assertIn("cannot start with a number", str(context.exception))
    
    def test_invalid_column_name_starting_with_number(self):
        """Test validation fails for column name starting with number."""
        invalid_names = ["123_updated_at", "1column", "9_timestamp", "0id"]
        
        for invalid_name in invalid_names:
            with self.assertRaises(ValueError) as context:
                ManualBookmarkConfig(
                    table_name="customers",
                    column_name=invalid_name
                )
            
            self.assertIn("cannot start with a number", str(context.exception))
    
    def test_from_dict_valid_data(self):
        """Test creating ManualBookmarkConfig from valid dictionary."""
        data = {
            "table_name": "customers",
            "column_name": "updated_at"
        }
        
        config = ManualBookmarkConfig.from_dict(data)
        
        self.assertEqual(config.table_name, "customers")
        self.assertEqual(config.column_name, "updated_at")
    
    def test_from_dict_with_whitespace_trimming(self):
        """Test creating ManualBookmarkConfig from dictionary with whitespace."""
        data = {
            "table_name": "  customers  ",
            "column_name": "  updated_at  "
        }
        
        config = ManualBookmarkConfig.from_dict(data)
        
        self.assertEqual(config.table_name, "customers")
        self.assertEqual(config.column_name, "updated_at")
    
    def test_from_dict_with_type_conversion(self):
        """Test from_dict converts non-string values to strings."""
        data = {
            "table_name": 123,  # Will be converted to string
            "column_name": 456  # Will be converted to string
        }
        
        # This should fail validation after conversion since "123" starts with number
        with self.assertRaises(ValueError):
            ManualBookmarkConfig.from_dict(data)
    
    def test_from_dict_missing_table_name(self):
        """Test from_dict fails when table_name is missing."""
        data = {
            "column_name": "updated_at"
        }
        
        with self.assertRaises(ValueError) as context:
            ManualBookmarkConfig.from_dict(data)
        
        self.assertIn("Missing required field 'table_name'", str(context.exception))
    
    def test_from_dict_missing_column_name(self):
        """Test from_dict fails when column_name is missing."""
        data = {
            "table_name": "customers"
        }
        
        with self.assertRaises(ValueError) as context:
            ManualBookmarkConfig.from_dict(data)
        
        self.assertIn("Missing required field 'column_name'", str(context.exception))
    
    def test_from_dict_none_table_name(self):
        """Test from_dict fails when table_name is None."""
        data = {
            "table_name": None,
            "column_name": "updated_at"
        }
        
        with self.assertRaises(ValueError) as context:
            ManualBookmarkConfig.from_dict(data)
        
        self.assertIn("Missing required field 'table_name'", str(context.exception))
    
    def test_from_dict_none_column_name(self):
        """Test from_dict fails when column_name is None."""
        data = {
            "table_name": "customers",
            "column_name": None
        }
        
        with self.assertRaises(ValueError) as context:
            ManualBookmarkConfig.from_dict(data)
        
        self.assertIn("Missing required field 'column_name'", str(context.exception))
    
    def test_from_dict_invalid_data_type(self):
        """Test from_dict fails when data is not a dictionary."""
        invalid_data_types = [
            ["not", "a", "dictionary"],
            "string_instead_of_dict",
            123,
            None,
            set(["not", "dict"])
        ]
        
        for invalid_data in invalid_data_types:
            with self.assertRaises(TypeError) as context:
                ManualBookmarkConfig.from_dict(invalid_data)
            
            self.assertIn("Configuration data must be a dictionary", str(context.exception))
    
    def test_from_dict_with_extra_fields(self):
        """Test from_dict ignores extra fields in dictionary."""
        data = {
            "table_name": "customers",
            "column_name": "updated_at",
            "extra_field": "ignored",
            "another_field": 123,
            "nested_field": {"key": "value"}
        }
        
        config = ManualBookmarkConfig.from_dict(data)
        
        self.assertEqual(config.table_name, "customers")
        self.assertEqual(config.column_name, "updated_at")
        # Extra fields should be ignored
        self.assertFalse(hasattr(config, "extra_field"))
        self.assertFalse(hasattr(config, "another_field"))
        self.assertFalse(hasattr(config, "nested_field"))
    
    def test_to_dict_conversion(self):
        """Test converting ManualBookmarkConfig to dictionary."""
        config = ManualBookmarkConfig(
            table_name="customers",
            column_name="updated_at"
        )
        
        result = config.to_dict()
        
        expected = {
            "table_name": "customers",
            "column_name": "updated_at"
        }
        
        self.assertEqual(result, expected)
        self.assertIsInstance(result, dict)
    
    def test_to_dict_with_complex_names(self):
        """Test to_dict with complex valid names."""
        config = ManualBookmarkConfig(
            table_name="Customer_Orders_V2",
            column_name="Last_Modified_Timestamp_UTC"
        )
        
        result = config.to_dict()
        
        expected = {
            "table_name": "Customer_Orders_V2",
            "column_name": "Last_Modified_Timestamp_UTC"
        }
        
        self.assertEqual(result, expected)
    
    def test_str_representation(self):
        """Test string representation of ManualBookmarkConfig."""
        config = ManualBookmarkConfig(
            table_name="customers",
            column_name="updated_at"
        )
        
        str_repr = str(config)
        
        self.assertIn("ManualBookmarkConfig", str_repr)
        self.assertIn("customers", str_repr)
        self.assertIn("updated_at", str_repr)
    
    def test_repr_representation(self):
        """Test detailed string representation of ManualBookmarkConfig."""
        config = ManualBookmarkConfig(
            table_name="customers",
            column_name="updated_at"
        )
        
        repr_str = repr(config)
        
        self.assertIn("ManualBookmarkConfig", repr_str)
        self.assertIn("table_name='customers'", repr_str)
        self.assertIn("column_name='updated_at'", repr_str)
    
    def test_equality_comparison(self):
        """Test equality comparison between ManualBookmarkConfig instances."""
        config1 = ManualBookmarkConfig("customers", "updated_at")
        config2 = ManualBookmarkConfig("customers", "updated_at")
        config3 = ManualBookmarkConfig("orders", "updated_at")
        
        self.assertEqual(config1, config2)
        self.assertNotEqual(config1, config3)


class TestManualConfigurationParsing(unittest.TestCase):
    """Test cases for manual configuration parsing with valid and invalid JSON structures."""
    
    def setUp(self):
        """Set up test fixtures."""
        # Import JobBookmarkManager to test the parsing method
        from src.glue_job.storage.bookmark_manager import JobBookmarkManager
        
        # Create a mock JobBookmarkManager instance for testing
        self.mock_glue_context = Mock()
        self.bookmark_manager = JobBookmarkManager(
            glue_context=self.mock_glue_context,
            job_name="test_job"
        )
    
    def test_parse_valid_single_table_config(self):
        """Test parsing valid JSON with single table configuration."""
        config_json = json.dumps({
            "customers": {
                "table_name": "customers",
                "column_name": "updated_at"
            }
        })
        
        # Access the private method for testing
        result = self.bookmark_manager._parse_manual_bookmark_config(config_json)
        
        self.assertEqual(len(result), 1)
        self.assertIn("customers", result)
        self.assertEqual(result["customers"].table_name, "customers")
        self.assertEqual(result["customers"].column_name, "updated_at")
    
    def test_parse_valid_multiple_tables_config(self):
        """Test parsing valid JSON with multiple table configurations."""
        config_json = json.dumps({
            "customers": {
                "table_name": "customers",
                "column_name": "updated_at"
            },
            "orders": {
                "table_name": "orders",
                "column_name": "order_timestamp"
            },
            "products": {
                "table_name": "products",
                "column_name": "product_id"
            }
        })
        
        result = self.bookmark_manager._parse_manual_bookmark_config(config_json)
        
        self.assertEqual(len(result), 3)
        
        # Check customers config
        self.assertIn("customers", result)
        self.assertEqual(result["customers"].table_name, "customers")
        self.assertEqual(result["customers"].column_name, "updated_at")
        
        # Check orders config
        self.assertIn("orders", result)
        self.assertEqual(result["orders"].table_name, "orders")
        self.assertEqual(result["orders"].column_name, "order_timestamp")
        
        # Check products config
        self.assertIn("products", result)
        self.assertEqual(result["products"].table_name, "products")
        self.assertEqual(result["products"].column_name, "product_id")
    
    def test_parse_valid_config_with_whitespace(self):
        """Test parsing valid JSON with whitespace in values."""
        config_json = json.dumps({
            "customers": {
                "table_name": "  customers  ",
                "column_name": "  updated_at  "
            }
        })
        
        result = self.bookmark_manager._parse_manual_bookmark_config(config_json)
        
        self.assertEqual(len(result), 1)
        # Whitespace should be trimmed
        self.assertEqual(result["customers"].table_name, "customers")
        self.assertEqual(result["customers"].column_name, "updated_at")
    
    def test_parse_empty_json_object(self):
        """Test parsing empty JSON object raises ValueError."""
        config_json = json.dumps({})
        
        with self.assertRaises(ValueError) as context:
            self.bookmark_manager._parse_manual_bookmark_config(config_json)
        
        self.assertIn("cannot be an empty object", str(context.exception))
    
    def test_parse_invalid_json_syntax(self):
        """Test parsing invalid JSON syntax."""
        invalid_json_strings = [
            '{"customers": {"table_name": "customers", "column_name": "updated_at"}',  # Missing closing brace
            '{"customers": {"table_name": "customers" "column_name": "updated_at"}}',  # Missing comma
            '{"customers": {"table_name": "customers", "column_name": "updated_at",}}',  # Trailing comma
            '{customers: {"table_name": "customers", "column_name": "updated_at"}}',  # Unquoted key
            '{"customers": {table_name: "customers", "column_name": "updated_at"}}',  # Unquoted nested key
            'not_json_at_all',
            '',
            'null',
            '[]'  # Array instead of object
        ]
        
        for invalid_json in invalid_json_strings:
            with self.assertRaises((json.JSONDecodeError, ValueError, TypeError)) as context:
                self.bookmark_manager._parse_manual_bookmark_config(invalid_json)
    
    def test_parse_json_with_missing_table_name(self):
        """Test parsing JSON with missing table_name field."""
        config_json = json.dumps({
            "customers": {
                "column_name": "updated_at"
                # Missing table_name
            }
        })
        
        with self.assertRaises(ValueError) as context:
            self.bookmark_manager._parse_manual_bookmark_config(config_json)
        
        self.assertIn("Missing required field 'table_name'", str(context.exception))
    
    def test_parse_json_with_missing_column_name(self):
        """Test parsing JSON with missing column_name field."""
        config_json = json.dumps({
            "customers": {
                "table_name": "customers"
                # Missing column_name
            }
        })
        
        with self.assertRaises(ValueError) as context:
            self.bookmark_manager._parse_manual_bookmark_config(config_json)
        
        self.assertIn("Missing required field 'column_name'", str(context.exception))
    
    def test_parse_json_with_null_values(self):
        """Test parsing JSON with null values."""
        config_json = json.dumps({
            "customers": {
                "table_name": None,
                "column_name": "updated_at"
            }
        })
        
        with self.assertRaises(ValueError) as context:
            self.bookmark_manager._parse_manual_bookmark_config(config_json)
        
        self.assertIn("Missing required field 'table_name'", str(context.exception))
    
    def test_parse_json_with_invalid_table_names(self):
        """Test parsing JSON with invalid table names."""
        invalid_configs = [
            {
                "customers": {
                    "table_name": "123_invalid",  # Starts with number
                    "column_name": "updated_at"
                }
            },
            {
                "customers": {
                    "table_name": "table-name",  # Contains hyphen
                    "column_name": "updated_at"
                }
            },
            {
                "customers": {
                    "table_name": "",  # Empty string
                    "column_name": "updated_at"
                }
            }
        ]
        
        for invalid_config in invalid_configs:
            config_json = json.dumps(invalid_config)
            with self.assertRaises(ValueError):
                self.bookmark_manager._parse_manual_bookmark_config(config_json)
    
    def test_parse_json_with_invalid_column_names(self):
        """Test parsing JSON with invalid column names."""
        invalid_configs = [
            {
                "customers": {
                    "table_name": "customers",
                    "column_name": "123_invalid"  # Starts with number
                }
            },
            {
                "customers": {
                    "table_name": "customers",
                    "column_name": "col-name"  # Contains hyphen
                }
            },
            {
                "customers": {
                    "table_name": "customers",
                    "column_name": ""  # Empty string
                }
            }
        ]
        
        for invalid_config in invalid_configs:
            config_json = json.dumps(invalid_config)
            with self.assertRaises(ValueError):
                self.bookmark_manager._parse_manual_bookmark_config(config_json)
    
    def test_parse_json_with_non_object_table_config(self):
        """Test parsing JSON where table config is not an object or string."""
        invalid_configs = [
            {"customers": ["array", "instead"]},
            {"customers": 123},
            {"customers": None},
            {"customers": True},
            {"customers": {"nested": {"object": "invalid"}}}
        ]
        
        for invalid_config in invalid_configs:
            config_json = json.dumps(invalid_config)
            with self.assertRaises(ValueError):
                self.bookmark_manager._parse_manual_bookmark_config(config_json)
    
    def test_parse_json_with_extra_fields(self):
        """Test parsing JSON with extra fields (should be ignored)."""
        config_json = json.dumps({
            "customers": {
                "table_name": "customers",
                "column_name": "updated_at",
                "extra_field": "ignored",
                "another_field": 123
            }
        })
        
        result = self.bookmark_manager._parse_manual_bookmark_config(config_json)
        
        self.assertEqual(len(result), 1)
        self.assertEqual(result["customers"].table_name, "customers")
        self.assertEqual(result["customers"].column_name, "updated_at")
        # Extra fields should not be present
        self.assertFalse(hasattr(result["customers"], "extra_field"))
    
    def test_parse_json_partial_failure_handling(self):
        """Test parsing JSON where some table configs are valid and others invalid."""
        config_json = json.dumps({
            "valid_table": {
                "table_name": "valid_table",
                "column_name": "updated_at"
            },
            "invalid_table": {
                "table_name": "123_invalid",  # Invalid name
                "column_name": "updated_at"
            }
        })
        
        # Should fail completely on first invalid config
        with self.assertRaises(ValueError):
            self.bookmark_manager._parse_manual_bookmark_config(config_json)
    
    def test_parse_large_config_json(self):
        """Test parsing large JSON configuration with many tables."""
        large_config = {}
        for i in range(100):
            table_name = f"table_{i}"
            large_config[table_name] = {
                "table_name": table_name,
                "column_name": f"column_{i}"
            }
        
        config_json = json.dumps(large_config)
        result = self.bookmark_manager._parse_manual_bookmark_config(config_json)
        
        self.assertEqual(len(result), 100)
        for i in range(100):
            table_name = f"table_{i}"
            self.assertIn(table_name, result)
            self.assertEqual(result[table_name].table_name, table_name)
            self.assertEqual(result[table_name].column_name, f"column_{i}")


class TestJDBCMetadataQuerying(unittest.TestCase):
    """Test cases for JDBC metadata querying with mocked database connections."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.resolver = BookmarkStrategyResolver({})
    
    def test_get_column_metadata_success(self):
        """Test successful JDBC metadata retrieval."""
        # Mock JDBC connection and metadata
        mock_connection = Mock()
        mock_metadata = Mock()
        mock_result_set = Mock()
        
        mock_connection.getMetaData.return_value = mock_metadata
        mock_metadata.getColumns.return_value = mock_result_set
        mock_result_set.next.return_value = True
        mock_result_set.getString.side_effect = lambda col: {
            'COLUMN_NAME': 'updated_at',
            'TYPE_NAME': 'TIMESTAMP'
        }[col]
        mock_result_set.getInt.side_effect = lambda col: {
            'DATA_TYPE': 93,
            'COLUMN_SIZE': 23,
            'NULLABLE': 1
        }[col]
        
        result = self.resolver._get_column_metadata(mock_connection, "customers", "updated_at")
        
        self.assertIsNotNone(result)
        self.assertEqual(result['column_name'], 'updated_at')
        self.assertEqual(result['data_type'], 'TIMESTAMP')
        self.assertEqual(result['jdbc_type'], 93)
        self.assertEqual(result['column_size'], 23)
        self.assertEqual(result['nullable'], 1)
        
        # Verify method calls
        mock_connection.getMetaData.assert_called_once()
        mock_metadata.getColumns.assert_called_once_with(None, None, "customers", "updated_at")
    
    def test_get_column_metadata_column_not_found(self):
        """Test JDBC metadata retrieval when column is not found."""
        # Mock JDBC connection and metadata
        mock_connection = Mock()
        mock_metadata = Mock()
        mock_result_set = Mock()
        
        mock_connection.getMetaData.return_value = mock_metadata
        mock_metadata.getColumns.return_value = mock_result_set
        mock_result_set.next.return_value = False  # No results
        
        result = self.resolver._get_column_metadata(mock_connection, "customers", "nonexistent_column")
        
        self.assertIsNone(result)
        
        # Verify method calls
        mock_connection.getMetaData.assert_called_once()
        mock_metadata.getColumns.assert_called_once_with(None, None, "customers", "nonexistent_column")
    
    def test_get_column_metadata_caching_hit(self):
        """Test that JDBC metadata queries are cached and cache hits work."""
        # Mock JDBC connection and metadata
        mock_connection = Mock()
        mock_metadata = Mock()
        mock_result_set = Mock()
        
        mock_connection.getMetaData.return_value = mock_metadata
        mock_metadata.getColumns.return_value = mock_result_set
        mock_result_set.next.return_value = True
        mock_result_set.getString.side_effect = lambda col: {
            'COLUMN_NAME': 'updated_at',
            'TYPE_NAME': 'TIMESTAMP'
        }[col]
        mock_result_set.getInt.side_effect = lambda col: {
            'DATA_TYPE': 93,
            'COLUMN_SIZE': 23,
            'NULLABLE': 1
        }[col]
        
        # First call
        result1 = self.resolver._get_column_metadata(mock_connection, "customers", "updated_at")
        
        # Second call should use cache
        result2 = self.resolver._get_column_metadata(mock_connection, "customers", "updated_at")
        
        # Should only call getMetaData once due to caching
        self.assertEqual(mock_connection.getMetaData.call_count, 1)
        self.assertEqual(result1, result2)
        self.assertIsNotNone(result1)
    
    def test_get_column_metadata_caching_none_result(self):
        """Test that None results are also cached."""
        # Mock JDBC connection and metadata
        mock_connection = Mock()
        mock_metadata = Mock()
        mock_result_set = Mock()
        
        mock_connection.getMetaData.return_value = mock_metadata
        mock_metadata.getColumns.return_value = mock_result_set
        mock_result_set.next.return_value = False  # No results
        
        # First call
        result1 = self.resolver._get_column_metadata(mock_connection, "customers", "nonexistent")
        
        # Second call should use cache
        result2 = self.resolver._get_column_metadata(mock_connection, "customers", "nonexistent")
        
        # Should only call getMetaData once due to caching
        self.assertEqual(mock_connection.getMetaData.call_count, 1)
        self.assertEqual(result1, result2)
        self.assertIsNone(result1)
    
    def test_get_column_metadata_different_tables_no_cache_collision(self):
        """Test that different table/column combinations don't collide in cache."""
        # Mock JDBC connection and metadata
        mock_connection = Mock()
        mock_metadata = Mock()
        mock_result_set = Mock()
        
        mock_connection.getMetaData.return_value = mock_metadata
        mock_metadata.getColumns.return_value = mock_result_set
        
        # Setup different responses for different calls
        call_count = [0]
        def mock_next():
            call_count[0] += 1
            return True
        
        def mock_getString(col):
            if call_count[0] == 1:  # First call
                return {'COLUMN_NAME': 'updated_at', 'TYPE_NAME': 'TIMESTAMP'}[col]
            else:  # Second call
                return {'COLUMN_NAME': 'order_id', 'TYPE_NAME': 'BIGINT'}[col]
        
        def mock_getInt(col):
            if call_count[0] == 1:  # First call
                return {'DATA_TYPE': 93, 'COLUMN_SIZE': 23, 'NULLABLE': 1}[col]
            else:  # Second call
                return {'DATA_TYPE': -5, 'COLUMN_SIZE': 19, 'NULLABLE': 0}[col]
        
        mock_result_set.next.side_effect = mock_next
        mock_result_set.getString.side_effect = mock_getString
        mock_result_set.getInt.side_effect = mock_getInt
        
        # Call for different table/column combinations
        result1 = self.resolver._get_column_metadata(mock_connection, "customers", "updated_at")
        result2 = self.resolver._get_column_metadata(mock_connection, "orders", "order_id")
        
        # Should call getMetaData twice (no cache collision)
        self.assertEqual(mock_connection.getMetaData.call_count, 2)
        
        # Results should be different
        self.assertNotEqual(result1, result2)
        self.assertEqual(result1['data_type'], 'TIMESTAMP')
        self.assertEqual(result2['data_type'], 'BIGINT')
    
    def test_get_column_metadata_connection_exception(self):
        """Test JDBC metadata retrieval when connection fails."""
        # Mock JDBC connection that raises exception
        mock_connection = Mock()
        mock_connection.getMetaData.side_effect = Exception("Database connection error")
        
        result = self.resolver._get_column_metadata(mock_connection, "customers", "updated_at")
        
        self.assertIsNone(result)
        
        # Verify the failure is cached
        cache_key = "customers.updated_at"
        self.assertIn(cache_key, self.resolver.jdbc_metadata_cache)
        self.assertIsNone(self.resolver.jdbc_metadata_cache[cache_key])
    
    def test_get_column_metadata_metadata_exception(self):
        """Test JDBC metadata retrieval when getColumns fails."""
        # Mock JDBC connection and metadata
        mock_connection = Mock()
        mock_metadata = Mock()
        
        mock_connection.getMetaData.return_value = mock_metadata
        mock_metadata.getColumns.side_effect = Exception("Metadata query error")
        
        result = self.resolver._get_column_metadata(mock_connection, "customers", "updated_at")
        
        self.assertIsNone(result)
    
    def test_get_column_metadata_result_set_exception(self):
        """Test JDBC metadata retrieval when result set processing fails."""
        # Mock JDBC connection and metadata
        mock_connection = Mock()
        mock_metadata = Mock()
        mock_result_set = Mock()
        
        mock_connection.getMetaData.return_value = mock_metadata
        mock_metadata.getColumns.return_value = mock_result_set
        mock_result_set.next.return_value = True
        mock_result_set.getString.side_effect = Exception("Result set error")
        
        result = self.resolver._get_column_metadata(mock_connection, "customers", "updated_at")
        
        self.assertIsNone(result)
    
    def test_get_column_metadata_with_various_data_types(self):
        """Test JDBC metadata retrieval with various data types."""
        test_cases = [
            ('TIMESTAMP', 93, 23, 1),
            ('BIGINT', -5, 19, 0),
            ('VARCHAR', 12, 255, 1),
            ('INTEGER', 4, 10, 0),
            ('DECIMAL', 3, 10, 1)
        ]
        
        for data_type, jdbc_type, column_size, nullable in test_cases:
            # Mock JDBC connection and metadata for each test case
            mock_connection = Mock()
            mock_metadata = Mock()
            mock_result_set = Mock()
            
            mock_connection.getMetaData.return_value = mock_metadata
            mock_metadata.getColumns.return_value = mock_result_set
            mock_result_set.next.return_value = True
            mock_result_set.getString.side_effect = lambda col: {
                'COLUMN_NAME': 'test_column',
                'TYPE_NAME': data_type
            }[col]
            mock_result_set.getInt.side_effect = lambda col: {
                'DATA_TYPE': jdbc_type,
                'COLUMN_SIZE': column_size,
                'NULLABLE': nullable
            }[col]
            
            # Create new resolver for each test to avoid cache interference
            resolver = BookmarkStrategyResolver({})
            result = resolver._get_column_metadata(mock_connection, "test_table", "test_column")
            
            self.assertIsNotNone(result)
            self.assertEqual(result['data_type'], data_type)
            self.assertEqual(result['jdbc_type'], jdbc_type)
            self.assertEqual(result['column_size'], column_size)
            self.assertEqual(result['nullable'], nullable)
    
    def test_clear_cache_functionality(self):
        """Test clearing the JDBC metadata cache."""
        # Add some data to cache
        self.resolver.jdbc_metadata_cache["test.column1"] = {"data_type": "VARCHAR"}
        self.resolver.jdbc_metadata_cache["test.column2"] = {"data_type": "INTEGER"}
        
        self.assertEqual(len(self.resolver.jdbc_metadata_cache), 2)
        
        self.resolver.clear_cache()
        
        self.assertEqual(len(self.resolver.jdbc_metadata_cache), 0)
    
    def test_get_cache_stats(self):
        """Test getting cache statistics."""
        # Add some data to cache
        self.resolver.jdbc_metadata_cache["test.column"] = {"data_type": "VARCHAR"}
        
        # Add manual configs
        manual_configs = {
            "table1": ManualBookmarkConfig("table1", "col1"),
            "table2": ManualBookmarkConfig("table2", "col2")
        }
        resolver_with_configs = BookmarkStrategyResolver(manual_configs)
        resolver_with_configs.jdbc_metadata_cache["test.column"] = {"data_type": "VARCHAR"}
        
        stats = resolver_with_configs.get_cache_stats()
        
        self.assertEqual(stats['cached_entries'], 1)
        self.assertEqual(stats['manual_configs'], 2)


class TestDataTypeToStrategyMapping(unittest.TestCase):
    """Test cases for data type to strategy mapping for all supported JDBC types."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.resolver = BookmarkStrategyResolver({})
    
    def test_timestamp_types_mapping(self):
        """Test mapping of all timestamp-related JDBC types to timestamp strategy."""
        # Only test types that are actually in the mapping
        timestamp_types = [
            'TIMESTAMP', 'TIMESTAMP_WITH_TIMEZONE', 'DATE', 'DATETIME', 'TIME'
        ]
        
        for jdbc_type in timestamp_types:
            strategy = self.resolver._map_jdbc_type_to_strategy(jdbc_type)
            self.assertEqual(strategy, 'timestamp', f"Failed for timestamp type: {jdbc_type}")
    
    def test_integer_types_mapping(self):
        """Test mapping of all integer-related JDBC types to primary_key strategy."""
        # Only test types that are actually in the mapping
        integer_types = [
            'INTEGER', 'BIGINT', 'SMALLINT', 'TINYINT', 'SERIAL', 'BIGSERIAL'
        ]
        
        for jdbc_type in integer_types:
            strategy = self.resolver._map_jdbc_type_to_strategy(jdbc_type)
            self.assertEqual(strategy, 'primary_key', f"Failed for integer type: {jdbc_type}")
    
    def test_string_types_mapping(self):
        """Test mapping of all string-related JDBC types to hash strategy."""
        # Only test types that are actually in the mapping
        string_types = [
            'VARCHAR', 'CHAR', 'TEXT', 'CLOB'
        ]
        
        for jdbc_type in string_types:
            strategy = self.resolver._map_jdbc_type_to_strategy(jdbc_type)
            self.assertEqual(strategy, 'hash', f"Failed for string type: {jdbc_type}")
    
    def test_numeric_types_mapping(self):
        """Test mapping of all numeric-related JDBC types to hash strategy."""
        # Only test types that are actually in the mapping
        numeric_types = [
            'DECIMAL', 'NUMERIC', 'FLOAT', 'DOUBLE'
        ]
        
        for jdbc_type in numeric_types:
            strategy = self.resolver._map_jdbc_type_to_strategy(jdbc_type)
            self.assertEqual(strategy, 'hash', f"Failed for numeric type: {jdbc_type}")
    
    def test_boolean_types_mapping(self):
        """Test mapping of boolean-related JDBC types to hash strategy."""
        # Only test types that are actually in the mapping
        boolean_types = ['BOOLEAN']
        
        for jdbc_type in boolean_types:
            strategy = self.resolver._map_jdbc_type_to_strategy(jdbc_type)
            self.assertEqual(strategy, 'hash', f"Failed for boolean type: {jdbc_type}")
    
    def test_binary_types_mapping(self):
        """Test mapping of binary-related JDBC types to hash strategy."""
        # Only test types that are actually in the mapping
        binary_types = [
            'BINARY', 'VARBINARY', 'BLOB'
        ]
        
        for jdbc_type in binary_types:
            strategy = self.resolver._map_jdbc_type_to_strategy(jdbc_type)
            self.assertEqual(strategy, 'hash', f"Failed for binary type: {jdbc_type}")
    
    def test_special_types_mapping(self):
        """Test mapping of special/database-specific JDBC types to hash strategy."""
        # Test unknown types that should fall back to hash strategy
        special_types = [
            'UUID', 'UNIQUEIDENTIFIER', 'GUID', 'XML', 'JSON', 'JSONB'
        ]
        
        for jdbc_type in special_types:
            strategy = self.resolver._map_jdbc_type_to_strategy(jdbc_type)
            self.assertEqual(strategy, 'hash', f"Failed for special type: {jdbc_type}")
    
    def test_case_insensitive_mapping(self):
        """Test that JDBC type mapping is case insensitive."""
        test_cases = [
            ('timestamp', 'timestamp'),
            ('TIMESTAMP', 'timestamp'),
            ('Timestamp', 'timestamp'),
            ('TimEsTaMp', 'timestamp'),
            ('integer', 'primary_key'),
            ('INTEGER', 'primary_key'),
            ('Integer', 'primary_key'),
            ('InTeGeR', 'primary_key'),
            ('varchar', 'hash'),
            ('VARCHAR', 'hash'),
            ('Varchar', 'hash'),
            ('VaRcHaR', 'hash')
        ]
        
        for jdbc_type, expected_strategy in test_cases:
            strategy = self.resolver._map_jdbc_type_to_strategy(jdbc_type)
            self.assertEqual(strategy, expected_strategy, f"Failed for case test: {jdbc_type}")
    
    def test_whitespace_handling_in_mapping(self):
        """Test that whitespace is properly handled in JDBC type mapping."""
        test_cases = [
            ('  TIMESTAMP  ', 'timestamp'),
            ('\tINTEGER\t', 'primary_key'),
            ('\nVARCHAR\n', 'hash'),
            ('  BIGINT  ', 'primary_key'),
            ('\t\nTEXT\t\n', 'hash')
        ]
        
        for jdbc_type, expected_strategy in test_cases:
            strategy = self.resolver._map_jdbc_type_to_strategy(jdbc_type)
            self.assertEqual(strategy, expected_strategy, f"Failed for whitespace test: {jdbc_type}")
    
    def test_unknown_type_defaults_to_hash(self):
        """Test that unknown JDBC types default to hash strategy."""
        unknown_types = [
            'UNKNOWN_TYPE', 'CUSTOM_TYPE', 'WEIRD_TYPE', 'NONEXISTENT',
            'MADE_UP_TYPE', 'RANDOM_STRING', 'TYPE_NOT_IN_MAPPING'
        ]
        
        for jdbc_type in unknown_types:
            strategy = self.resolver._map_jdbc_type_to_strategy(jdbc_type)
            self.assertEqual(strategy, 'hash', f"Failed for unknown type: {jdbc_type}")
    
    def test_partial_match_fallback_timestamp(self):
        """Test partial matching for timestamp-like types."""
        partial_timestamp_types = [
            'CUSTOM_TIMESTAMP', 'TIMESTAMP_CUSTOM', 'MY_DATE_TYPE',
            'SPECIAL_TIME', 'DATETIME_WITH_PRECISION'
        ]
        
        for jdbc_type in partial_timestamp_types:
            strategy = self.resolver._map_jdbc_type_to_strategy(jdbc_type)
            self.assertEqual(strategy, 'timestamp', f"Failed for partial timestamp type: {jdbc_type}")
    
    def test_partial_match_fallback_integer(self):
        """Test partial matching for integer-like types."""
        partial_integer_types = [
            'CUSTOM_INT', 'INT_CUSTOM', 'MY_SERIAL_TYPE',
            'SPECIAL_NUMBER', 'BIGINT_WITH_PRECISION'
        ]
        
        for jdbc_type in partial_integer_types:
            strategy = self.resolver._map_jdbc_type_to_strategy(jdbc_type)
            self.assertEqual(strategy, 'primary_key', f"Failed for partial integer type: {jdbc_type}")
    
    def test_empty_and_none_type_handling(self):
        """Test handling of empty and None JDBC types."""
        edge_cases = ['', '   ', '\t\n', None]
        
        for jdbc_type in edge_cases:
            if jdbc_type is None:
                # None should be handled gracefully
                with self.assertRaises(AttributeError):
                    self.resolver._map_jdbc_type_to_strategy(jdbc_type)
            else:
                strategy = self.resolver._map_jdbc_type_to_strategy(jdbc_type)
                self.assertEqual(strategy, 'hash', f"Failed for edge case: {repr(jdbc_type)}")
    
    def test_database_specific_type_variations(self):
        """Test database-specific type variations are handled correctly."""
        # Test only types that are actually in the mapping or should fall back correctly
        known_types = [
            ('TIMESTAMP', 'timestamp'),
            ('TIMESTAMP_WITH_TIMEZONE', 'timestamp'),
            ('DATE', 'timestamp'),
            ('DATETIME', 'timestamp'),
            ('TIME', 'timestamp'),
            ('INTEGER', 'primary_key'),
            ('BIGINT', 'primary_key'),
            ('SMALLINT', 'primary_key'),
            ('TINYINT', 'primary_key'),
            ('SERIAL', 'primary_key'),
            ('BIGSERIAL', 'primary_key'),
            ('VARCHAR', 'hash'),
            ('CHAR', 'hash'),
            ('TEXT', 'hash'),
            ('CLOB', 'hash'),
            ('DECIMAL', 'hash'),
            ('NUMERIC', 'hash'),
            ('FLOAT', 'hash'),
            ('DOUBLE', 'hash'),
            ('BOOLEAN', 'hash'),
            ('BINARY', 'hash'),
            ('VARBINARY', 'hash'),
            ('BLOB', 'hash')
        ]
        
        for jdbc_type, expected_strategy in known_types:
            strategy = self.resolver._map_jdbc_type_to_strategy(jdbc_type)
            self.assertEqual(strategy, expected_strategy, 
                           f"Failed for database-specific type: {jdbc_type}")
    
    def test_comprehensive_jdbc_type_coverage(self):
        """Test that all types in JDBC_TYPE_TO_STRATEGY mapping work correctly."""
        # Get all types from the actual mapping
        all_mapped_types = self.resolver.JDBC_TYPE_TO_STRATEGY.keys()
        
        for jdbc_type in all_mapped_types:
            expected_strategy = self.resolver.JDBC_TYPE_TO_STRATEGY[jdbc_type]
            actual_strategy = self.resolver._map_jdbc_type_to_strategy(jdbc_type)
            
            self.assertEqual(actual_strategy, expected_strategy,
                           f"Mapping inconsistency for type: {jdbc_type}")
    
    def test_strategy_distribution_correctness(self):
        """Test that the strategy distribution is reasonable across all types."""
        all_types = list(self.resolver.JDBC_TYPE_TO_STRATEGY.keys())
        
        timestamp_count = 0
        primary_key_count = 0
        hash_count = 0
        
        for jdbc_type in all_types:
            strategy = self.resolver.JDBC_TYPE_TO_STRATEGY[jdbc_type]
            if strategy == 'timestamp':
                timestamp_count += 1
            elif strategy == 'primary_key':
                primary_key_count += 1
            elif strategy == 'hash':
                hash_count += 1
        
        # Verify we have reasonable distribution
        self.assertGreater(timestamp_count, 0, "Should have timestamp types")
        self.assertGreater(primary_key_count, 0, "Should have primary key types")
        self.assertGreater(hash_count, 0, "Should have hash types")
        
        # Hash should be the most common (catch-all for many types)
        self.assertGreater(hash_count, timestamp_count, "Hash should be most common")
        self.assertGreater(hash_count, primary_key_count, "Hash should be most common")
        
        # Verify total count
        total_count = timestamp_count + primary_key_count + hash_count
        self.assertEqual(total_count, len(all_types), "All types should be accounted for")


if __name__ == '__main__':
    unittest.main()