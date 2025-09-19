"""
Unit tests for ManualBookmarkConfig dataclass and BookmarkStrategyResolver.

This module tests the manual bookmark configuration functionality including:
- ManualBookmarkConfig validation and creation
- BookmarkStrategyResolver strategy resolution logic
- JDBC data type to strategy mapping
- Error handling for invalid configurations
"""

import pytest
import unittest
from unittest.mock import Mock, MagicMock
from typing import Dict, Any

from src.glue_job.storage.manual_bookmark_config import (
    ManualBookmarkConfig, 
    BookmarkStrategyResolver
)


class TestManualBookmarkConfig(unittest.TestCase):
    """Test cases for ManualBookmarkConfig dataclass."""
    
    def test_valid_config_creation(self):
        """Test creating valid ManualBookmarkConfig instances."""
        # Test basic valid configuration
        config = ManualBookmarkConfig(
            table_name="customers",
            column_name="updated_at"
        )
        
        self.assertEqual(config.table_name, "customers")
        self.assertEqual(config.column_name, "updated_at")
    
    def test_valid_config_with_underscores(self):
        """Test configuration with underscores in names."""
        config = ManualBookmarkConfig(
            table_name="customer_orders",
            column_name="last_modified_date"
        )
        
        self.assertEqual(config.table_name, "customer_orders")
        self.assertEqual(config.column_name, "last_modified_date")
    
    def test_valid_config_with_numbers(self):
        """Test configuration with numbers in names (not at start)."""
        config = ManualBookmarkConfig(
            table_name="table_v2",
            column_name="col_123"
        )
        
        self.assertEqual(config.table_name, "table_v2")
        self.assertEqual(config.column_name, "col_123")
    
    def test_whitespace_trimming(self):
        """Test that whitespace is trimmed from names."""
        config = ManualBookmarkConfig(
            table_name="  customers  ",
            column_name="  updated_at  "
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
    
    def test_whitespace_only_table_name_validation(self):
        """Test validation fails for whitespace-only table name."""
        with self.assertRaises(ValueError) as context:
            ManualBookmarkConfig(
                table_name="   ",
                column_name="updated_at"
            )
        
        self.assertIn("table_name cannot be empty", str(context.exception))
    
    def test_whitespace_only_column_name_validation(self):
        """Test validation fails for whitespace-only column name."""
        with self.assertRaises(ValueError) as context:
            ManualBookmarkConfig(
                table_name="customers",
                column_name="   "
            )
        
        self.assertIn("column_name cannot be empty", str(context.exception))
    
    def test_invalid_table_name_with_special_chars(self):
        """Test validation fails for table name with special characters."""
        invalid_names = ["table-name", "table.name", "table@name", "table name", "table$name"]
        
        for invalid_name in invalid_names:
            with self.assertRaises(ValueError) as context:
                ManualBookmarkConfig(
                    table_name=invalid_name,
                    column_name="updated_at"
                )
            
            self.assertIn("must contain only alphanumeric characters", str(context.exception))
    
    def test_invalid_column_name_with_special_chars(self):
        """Test validation fails for column name with special characters."""
        invalid_names = ["col-name", "col.name", "col@name", "col name", "col$name"]
        
        for invalid_name in invalid_names:
            with self.assertRaises(ValueError) as context:
                ManualBookmarkConfig(
                    table_name="customers",
                    column_name=invalid_name
                )
            
            self.assertIn("must contain only alphanumeric characters", str(context.exception))
    
    def test_invalid_table_name_starting_with_number(self):
        """Test validation fails for table name starting with number."""
        with self.assertRaises(ValueError) as context:
            ManualBookmarkConfig(
                table_name="123_customers",
                column_name="updated_at"
            )
        
        self.assertIn("cannot start with a number", str(context.exception))
    
    def test_invalid_column_name_starting_with_number(self):
        """Test validation fails for column name starting with number."""
        with self.assertRaises(ValueError) as context:
            ManualBookmarkConfig(
                table_name="customers",
                column_name="123_updated_at"
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
    
    def test_from_dict_with_whitespace(self):
        """Test creating ManualBookmarkConfig from dictionary with whitespace."""
        data = {
            "table_name": "  customers  ",
            "column_name": "  updated_at  "
        }
        
        config = ManualBookmarkConfig.from_dict(data)
        
        self.assertEqual(config.table_name, "customers")
        self.assertEqual(config.column_name, "updated_at")
    
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
        invalid_data = ["not", "a", "dictionary"]
        
        with self.assertRaises(TypeError) as context:
            ManualBookmarkConfig.from_dict(invalid_data)
        
        self.assertIn("Configuration data must be a dictionary", str(context.exception))
    
    def test_from_dict_with_extra_fields(self):
        """Test from_dict ignores extra fields in dictionary."""
        data = {
            "table_name": "customers",
            "column_name": "updated_at",
            "extra_field": "ignored"
        }
        
        config = ManualBookmarkConfig.from_dict(data)
        
        self.assertEqual(config.table_name, "customers")
        self.assertEqual(config.column_name, "updated_at")
        # Extra field should be ignored
        self.assertFalse(hasattr(config, "extra_field"))
    
    def test_to_dict(self):
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


class TestBookmarkStrategyResolver(unittest.TestCase):
    """Test cases for BookmarkStrategyResolver class."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.manual_configs = {
            "customers": ManualBookmarkConfig("customers", "updated_at"),
            "orders": ManualBookmarkConfig("orders", "order_id")
        }
        self.resolver = BookmarkStrategyResolver(self.manual_configs)
    
    def test_initialization_with_configs(self):
        """Test BookmarkStrategyResolver initialization with manual configs."""
        self.assertEqual(len(self.resolver.manual_configs), 2)
        self.assertIn("customers", self.resolver.manual_configs)
        self.assertIn("orders", self.resolver.manual_configs)
        self.assertEqual(len(self.resolver.jdbc_metadata_cache), 0)
    
    def test_initialization_empty_configs(self):
        """Test BookmarkStrategyResolver initialization with empty configs."""
        resolver = BookmarkStrategyResolver({})
        
        self.assertEqual(len(resolver.manual_configs), 0)
        self.assertEqual(len(resolver.jdbc_metadata_cache), 0)
    
    def test_initialization_none_configs(self):
        """Test BookmarkStrategyResolver initialization with None configs."""
        resolver = BookmarkStrategyResolver(None)
        
        self.assertEqual(len(resolver.manual_configs), 0)
        self.assertEqual(len(resolver.jdbc_metadata_cache), 0)
    
    def test_jdbc_type_to_strategy_mapping_timestamp_types(self):
        """Test JDBC type to strategy mapping for timestamp types."""
        timestamp_types = [
            'TIMESTAMP', 'TIMESTAMP_WITH_TIMEZONE', 'DATE', 'DATETIME', 'TIME'
        ]
        
        for jdbc_type in timestamp_types:
            strategy = self.resolver._map_jdbc_type_to_strategy(jdbc_type)
            self.assertEqual(strategy, 'timestamp', f"Failed for type: {jdbc_type}")
    
    def test_jdbc_type_to_strategy_mapping_integer_types(self):
        """Test JDBC type to strategy mapping for integer types."""
        integer_types = [
            'INTEGER', 'BIGINT', 'SMALLINT', 'TINYINT', 'SERIAL', 'BIGSERIAL'
        ]
        
        for jdbc_type in integer_types:
            strategy = self.resolver._map_jdbc_type_to_strategy(jdbc_type)
            self.assertEqual(strategy, 'primary_key', f"Failed for type: {jdbc_type}")
    
    def test_jdbc_type_to_strategy_mapping_string_types(self):
        """Test JDBC type to strategy mapping for string and other types."""
        string_types = [
            'VARCHAR', 'CHAR', 'TEXT', 'CLOB', 'DECIMAL', 'NUMERIC', 
            'FLOAT', 'DOUBLE', 'BOOLEAN', 'BINARY', 'VARBINARY', 'BLOB'
        ]
        
        for jdbc_type in string_types:
            strategy = self.resolver._map_jdbc_type_to_strategy(jdbc_type)
            self.assertEqual(strategy, 'hash', f"Failed for type: {jdbc_type}")
    
    def test_jdbc_type_to_strategy_mapping_case_insensitive(self):
        """Test JDBC type to strategy mapping is case insensitive."""
        test_cases = [
            ('timestamp', 'timestamp'),
            ('TIMESTAMP', 'timestamp'),
            ('Timestamp', 'timestamp'),
            ('integer', 'primary_key'),
            ('INTEGER', 'primary_key'),
            ('Integer', 'primary_key'),
            ('varchar', 'hash'),
            ('VARCHAR', 'hash'),
            ('Varchar', 'hash')
        ]
        
        for jdbc_type, expected_strategy in test_cases:
            strategy = self.resolver._map_jdbc_type_to_strategy(jdbc_type)
            self.assertEqual(strategy, expected_strategy, f"Failed for type: {jdbc_type}")
    
    def test_jdbc_type_to_strategy_mapping_unknown_type(self):
        """Test JDBC type to strategy mapping for unknown types defaults to hash."""
        unknown_types = ['UNKNOWN_TYPE', 'CUSTOM_TYPE', 'WEIRD_TYPE']
        
        for jdbc_type in unknown_types:
            strategy = self.resolver._map_jdbc_type_to_strategy(jdbc_type)
            self.assertEqual(strategy, 'hash', f"Failed for unknown type: {jdbc_type}")
    
    def test_jdbc_type_to_strategy_mapping_partial_matches(self):
        """Test JDBC type to strategy mapping with partial matches."""
        test_cases = [
            ('TIMESTAMP_WITH_LOCAL_TIMEZONE', 'timestamp'),
            ('BIGINT_UNSIGNED', 'primary_key'),
            ('VARCHAR2', 'hash')
        ]
        
        for jdbc_type, expected_strategy in test_cases:
            strategy = self.resolver._map_jdbc_type_to_strategy(jdbc_type)
            self.assertEqual(strategy, expected_strategy, f"Failed for type: {jdbc_type}")
    
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
    
    def test_get_column_metadata_caching(self):
        """Test that JDBC metadata queries are cached."""
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
    
    def test_get_column_metadata_exception_handling(self):
        """Test JDBC metadata retrieval exception handling."""
        # Mock JDBC connection that raises exception
        mock_connection = Mock()
        mock_connection.getMetaData.side_effect = Exception("Database connection error")
        
        result = self.resolver._get_column_metadata(mock_connection, "customers", "updated_at")
        
        self.assertIsNone(result)
    
    def test_clear_cache(self):
        """Test clearing the JDBC metadata cache."""
        # Add some data to cache
        self.resolver.jdbc_metadata_cache["test.column"] = {"data_type": "VARCHAR"}
        
        self.assertEqual(len(self.resolver.jdbc_metadata_cache), 1)
        
        self.resolver.clear_cache()
        
        self.assertEqual(len(self.resolver.jdbc_metadata_cache), 0)
    
    def test_get_cache_stats(self):
        """Test getting cache statistics."""
        # Add some data to cache
        self.resolver.jdbc_metadata_cache["test.column"] = {"data_type": "VARCHAR"}
        
        stats = self.resolver.get_cache_stats()
        
        self.assertEqual(stats['cached_entries'], 1)
        self.assertEqual(stats['manual_configs'], 2)  # From setUp
    
    def test_get_manual_strategy_success(self):
        """Test successful manual strategy resolution."""
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
        
        result = self.resolver._get_manual_strategy("customers", mock_connection)
        
        self.assertIsNotNone(result)
        strategy, column_name, data_type = result
        self.assertEqual(strategy, 'timestamp')
        self.assertEqual(column_name, 'updated_at')
        self.assertEqual(data_type, 'TIMESTAMP')
    
    def test_get_manual_strategy_table_not_configured(self):
        """Test manual strategy resolution when table is not configured."""
        mock_connection = Mock()
        
        result = self.resolver._get_manual_strategy("unconfigured_table", mock_connection)
        
        self.assertIsNone(result)
    
    def test_get_manual_strategy_column_not_found(self):
        """Test manual strategy resolution when configured column doesn't exist."""
        # Mock JDBC connection and metadata
        mock_connection = Mock()
        mock_metadata = Mock()
        mock_result_set = Mock()
        
        mock_connection.getMetaData.return_value = mock_metadata
        mock_metadata.getColumns.return_value = mock_result_set
        mock_result_set.next.return_value = False  # Column not found
        
        result = self.resolver._get_manual_strategy("customers", mock_connection)
        
        self.assertIsNone(result)
    
    def test_get_manual_strategy_metadata_exception(self):
        """Test manual strategy resolution when metadata query fails."""
        # Mock JDBC connection that raises exception
        mock_connection = Mock()
        mock_connection.getMetaData.side_effect = Exception("Database connection error")
        
        result = self.resolver._get_manual_strategy("customers", mock_connection)
        
        self.assertIsNone(result)
    
    def test_resolve_strategy_with_manual_config(self):
        """Test resolve_strategy using manual configuration."""
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
        
        strategy, column_name, is_manual = self.resolver.resolve_strategy("customers", mock_connection)
        
        self.assertEqual(strategy, 'timestamp')
        self.assertEqual(column_name, 'updated_at')
        self.assertTrue(is_manual)
    
    def test_resolve_strategy_fallback_to_automatic(self):
        """Test resolve_strategy falling back to automatic detection."""
        # Mock JDBC connection and metadata properly
        mock_connection = Mock()
        mock_metadata = Mock()
        mock_result_set = Mock()
        
        mock_connection.getMetaData.return_value = mock_metadata
        mock_metadata.getColumns.return_value = mock_result_set
        mock_metadata.getPrimaryKeys.return_value = mock_result_set
        mock_result_set.next.return_value = False  # No columns found
        
        # Test with table that has no manual configuration
        strategy, column_name, is_manual = self.resolver.resolve_strategy("unconfigured_table", mock_connection)
        
        # Should fall back to automatic detection (placeholder implementation returns 'hash', None)
        self.assertEqual(strategy, 'hash')
        self.assertIsNone(column_name)
        self.assertFalse(is_manual)
    
    def test_resolve_strategy_manual_config_fails_fallback(self):
        """Test resolve_strategy falling back when manual config fails."""
        # Mock JDBC connection that fails for metadata query
        mock_connection = Mock()
        mock_connection.getMetaData.side_effect = Exception("Database connection error")
        
        # Test with table that has manual configuration but metadata query fails
        strategy, column_name, is_manual = self.resolver.resolve_strategy("customers", mock_connection)
        
        # Should fall back to automatic detection
        self.assertEqual(strategy, 'hash')
        self.assertIsNone(column_name)
        self.assertFalse(is_manual)
    
    def test_resolve_strategy_integer_column_manual_config(self):
        """Test resolve_strategy with manual config for integer column."""
        # Mock JDBC connection and metadata for integer column
        mock_connection = Mock()
        mock_metadata = Mock()
        mock_result_set = Mock()
        
        mock_connection.getMetaData.return_value = mock_metadata
        mock_metadata.getColumns.return_value = mock_result_set
        mock_result_set.next.return_value = True
        mock_result_set.getString.side_effect = lambda col: {
            'COLUMN_NAME': 'order_id',
            'TYPE_NAME': 'BIGINT'
        }[col]
        mock_result_set.getInt.side_effect = lambda col: {
            'DATA_TYPE': -5,
            'COLUMN_SIZE': 19,
            'NULLABLE': 0
        }[col]
        
        strategy, column_name, is_manual = self.resolver.resolve_strategy("orders", mock_connection)
        
        self.assertEqual(strategy, 'primary_key')
        self.assertEqual(column_name, 'order_id')
        self.assertTrue(is_manual)
    
    def test_detect_timestamp_columns(self):
        """Test automatic detection of timestamp columns."""
        # Mock JDBC connection and metadata for timestamp columns
        mock_connection = Mock()
        mock_metadata = Mock()
        mock_result_set = Mock()
        
        mock_connection.getMetaData.return_value = mock_metadata
        mock_metadata.getColumns.return_value = mock_result_set
        
        # Mock multiple columns with one timestamp column
        columns_data = [
            ('id', 'INTEGER'),
            ('name', 'VARCHAR'),
            ('updated_at', 'TIMESTAMP'),
            ('created_at', 'TIMESTAMP')
        ]
        
        call_count = [0]
        def mock_next():
            if call_count[0] < len(columns_data):
                call_count[0] += 1
                return True
            return False
        
        def mock_getString(col_name):
            if call_count[0] <= len(columns_data):
                col_data = columns_data[call_count[0] - 1]
                if col_name == 'COLUMN_NAME':
                    return col_data[0]
                elif col_name == 'TYPE_NAME':
                    return col_data[1]
            return None
        
        mock_result_set.next.side_effect = mock_next
        mock_result_set.getString.side_effect = mock_getString
        
        timestamp_columns = self.resolver._detect_timestamp_columns(mock_connection, "test_table")
        
        self.assertIn('updated_at', timestamp_columns)
        self.assertIn('created_at', timestamp_columns)
        self.assertEqual(len(timestamp_columns), 2)
    
    def test_detect_primary_key_columns(self):
        """Test automatic detection of primary key columns."""
        # Mock JDBC connection and metadata for primary key columns
        mock_connection = Mock()
        mock_metadata = Mock()
        mock_pk_result_set = Mock()
        mock_columns_result_set = Mock()
        
        mock_connection.getMetaData.return_value = mock_metadata
        mock_metadata.getPrimaryKeys.return_value = mock_pk_result_set
        mock_metadata.getColumns.return_value = mock_columns_result_set
        
        # Mock primary key detection (no actual PKs found)
        mock_pk_result_set.next.return_value = False
        
        # Mock column detection for integer columns with PK-like names
        columns_data = [
            ('id', 'INTEGER'),
            ('name', 'VARCHAR'),
            ('user_id', 'BIGINT'),
            ('description', 'TEXT')
        ]
        
        call_count = [0]
        def mock_next():
            if call_count[0] < len(columns_data):
                call_count[0] += 1
                return True
            return False
        
        def mock_getString(col_name):
            if call_count[0] <= len(columns_data):
                col_data = columns_data[call_count[0] - 1]
                if col_name == 'COLUMN_NAME':
                    return col_data[0]
                elif col_name == 'TYPE_NAME':
                    return col_data[1]
            return None
        
        mock_columns_result_set.next.side_effect = mock_next
        mock_columns_result_set.getString.side_effect = mock_getString
        
        pk_columns = self.resolver._detect_primary_key_columns(mock_connection, "test_table")
        
        self.assertIn('id', pk_columns)
        self.assertIn('user_id', pk_columns)
        self.assertEqual(len(pk_columns), 2)
    
    def test_select_best_timestamp_column(self):
        """Test selection of best timestamp column from candidates."""
        # Test with preferred pattern
        candidates = ['created_at', 'updated_at', 'timestamp']
        best = self.resolver._select_best_timestamp_column(candidates)
        self.assertEqual(best, 'updated_at')  # Should prefer updated_at
        
        # Test with no preferred pattern
        candidates = ['col1_date', 'col2_time']
        best = self.resolver._select_best_timestamp_column(candidates)
        self.assertEqual(best, 'col1_date')  # Should return first one
        
        # Test with empty list
        candidates = []
        best = self.resolver._select_best_timestamp_column(candidates)
        self.assertIsNone(best)
    
    def test_select_best_primary_key_column(self):
        """Test selection of best primary key column from candidates."""
        # Test with preferred pattern
        candidates = ['user_id', 'id', 'pk']
        best = self.resolver._select_best_primary_key_column(candidates)
        self.assertEqual(best, 'id')  # Should prefer 'id'
        
        # Test with no preferred pattern
        candidates = ['col1_seq', 'col2_num']
        best = self.resolver._select_best_primary_key_column(candidates)
        self.assertEqual(best, 'col1_seq')  # Should return first one
        
        # Test with empty list
        candidates = []
        best = self.resolver._select_best_primary_key_column(candidates)
        self.assertIsNone(best)
    
    def test_get_automatic_strategy_with_timestamp_detection(self):
        """Test automatic strategy detection finding timestamp columns."""
        # Create resolver without manual configs
        resolver = BookmarkStrategyResolver({})
        
        # Mock the timestamp detection to return a column
        resolver._detect_timestamp_columns = Mock(return_value=['updated_at', 'created_at'])
        resolver._select_best_timestamp_column = Mock(return_value='updated_at')
        
        mock_connection = Mock()
        
        strategy, column_name = resolver._get_automatic_strategy("test_table", mock_connection)
        
        self.assertEqual(strategy, 'timestamp')
        self.assertEqual(column_name, 'updated_at')
    
    def test_get_automatic_strategy_with_pk_detection(self):
        """Test automatic strategy detection finding primary key columns."""
        # Create resolver without manual configs
        resolver = BookmarkStrategyResolver({})
        
        # Mock the detection methods
        resolver._detect_timestamp_columns = Mock(return_value=[])  # No timestamp columns
        resolver._detect_primary_key_columns = Mock(return_value=['id', 'user_id'])
        resolver._select_best_primary_key_column = Mock(return_value='id')
        
        mock_connection = Mock()
        
        strategy, column_name = resolver._get_automatic_strategy("test_table", mock_connection)
        
        self.assertEqual(strategy, 'primary_key')
        self.assertEqual(column_name, 'id')
    
    def test_get_automatic_strategy_fallback_to_hash(self):
        """Test automatic strategy detection falling back to hash."""
        # Create resolver without manual configs
        resolver = BookmarkStrategyResolver({})
        
        # Mock the detection methods to return no columns
        resolver._detect_timestamp_columns = Mock(return_value=[])
        resolver._detect_primary_key_columns = Mock(return_value=[])
        
        mock_connection = Mock()
        
        strategy, column_name = resolver._get_automatic_strategy("test_table", mock_connection)
        
        self.assertEqual(strategy, 'hash')
        self.assertIsNone(column_name)


if __name__ == '__main__':
    unittest.main()