"""
Unit tests for JDBC metadata querying functionality in JobBookmarkManager.

This module tests the JDBC metadata querying functionality including:
- _get_column_data_type() method with various scenarios
- _determine_strategy_from_data_type() method with different data types
- Caching mechanism for metadata queries
- Error handling for metadata query failures
"""

import pytest
import unittest
import time
from unittest.mock import Mock, MagicMock, patch
from typing import Dict, Any, Optional

# Mock the dependencies that might not be available during testing
import sys
from unittest.mock import Mock

# Mock all AWS and Spark dependencies
mock_modules = {
    'awsglue': Mock(),
    'awsglue.context': Mock(),
    'awsglue.job': Mock(),
    'awsglue.transforms': Mock(),
    'awsglue.utils': Mock(),
    'pyspark': Mock(),
    'pyspark.sql': Mock(),
    'pyspark.sql.functions': Mock(),
    'pyspark.sql.types': Mock(),
    # 'boto3': Mock(),  # Removed to allow proper exception handling
    'botocore': Mock(),
    'botocore.config': Mock(),
    'botocore.exceptions': Mock(),
}

for module_name, mock_module in mock_modules.items():
    sys.modules[module_name] = mock_module

# Now import the module under test
from src.glue_job.storage.bookmark_manager import JobBookmarkManager


class TestJDBCMetadataQuerying(unittest.TestCase):
    """Test cases for JDBC metadata querying functionality."""
    
    def setUp(self):
        """Set up test fixtures."""
        # Mock Glue context and job
        self.mock_glue_context = Mock()
        self.mock_job = Mock()
        
        # Create JobBookmarkManager instance
        self.bookmark_manager = JobBookmarkManager(
            glue_context=self.mock_glue_context,
            job_name="test_job",
            job=self.mock_job
        )
        
        # Mock structured logger to avoid import issues
        self.bookmark_manager.structured_logger = Mock()
    
    def test_get_column_data_type_success(self):
        """Test successful JDBC metadata query for column data type."""
        # Mock JDBC connection and metadata
        mock_connection = Mock()
        mock_metadata = Mock()
        mock_result_set = Mock()
        
        mock_connection.getMetaData.return_value = mock_metadata
        mock_metadata.getColumns.return_value = mock_result_set
        mock_result_set.next.return_value = True
        mock_result_set.getString.return_value = "TIMESTAMP"
        mock_result_set.getInt.side_effect = lambda col: {
            'DATA_TYPE': 93,
            'COLUMN_SIZE': 23,
            'NULLABLE': 1
        }[col]
        mock_result_set.close = Mock()
        
        result = self.bookmark_manager._get_column_data_type("customers", "updated_at", mock_connection)
        
        self.assertEqual(result, "TIMESTAMP")
        mock_connection.getMetaData.assert_called_once()
        mock_metadata.getColumns.assert_called_once_with(None, None, "customers", "updated_at")
        mock_result_set.next.assert_called_once()
        mock_result_set.close.assert_called_once()
    
    def test_get_column_data_type_column_not_found(self):
        """Test JDBC metadata query when column is not found."""
        # Mock JDBC connection and metadata
        mock_connection = Mock()
        mock_metadata = Mock()
        mock_result_set = Mock()
        
        mock_connection.getMetaData.return_value = mock_metadata
        mock_metadata.getColumns.return_value = mock_result_set
        mock_result_set.next.return_value = False  # Column not found
        mock_result_set.close = Mock()
        
        result = self.bookmark_manager._get_column_data_type("customers", "nonexistent_column", mock_connection)
        
        self.assertIsNone(result)
        mock_connection.getMetaData.assert_called_once()
        mock_metadata.getColumns.assert_called_once_with(None, None, "customers", "nonexistent_column")
        mock_result_set.next.assert_called_once()
        mock_result_set.close.assert_called_once()
    
    def test_get_column_data_type_invalid_parameters(self):
        """Test JDBC metadata query with invalid parameters."""
        # Test with None table name
        result = self.bookmark_manager._get_column_data_type(None, "column", Mock())
        self.assertIsNone(result)
        
        # Test with empty table name
        result = self.bookmark_manager._get_column_data_type("", "column", Mock())
        self.assertIsNone(result)
        
        # Test with None column name
        result = self.bookmark_manager._get_column_data_type("table", None, Mock())
        self.assertIsNone(result)
        
        # Test with empty column name
        result = self.bookmark_manager._get_column_data_type("table", "", Mock())
        self.assertIsNone(result)
        
        # Test with None connection
        result = self.bookmark_manager._get_column_data_type("table", "column", None)
        self.assertIsNone(result)
    
    def test_get_column_data_type_exception_handling(self):
        """Test JDBC metadata query exception handling."""
        # Mock connection that raises exception
        mock_connection = Mock()
        mock_connection.getMetaData.side_effect = Exception("Database connection error")
        
        result = self.bookmark_manager._get_column_data_type("customers", "updated_at", mock_connection)
        
        self.assertIsNone(result)
        self.bookmark_manager.structured_logger.error.assert_called()
    
    def test_get_column_data_type_caching(self):
        """Test that JDBC metadata queries are cached for performance."""
        # Mock JDBC connection and metadata
        mock_connection = Mock()
        mock_metadata = Mock()
        mock_result_set = Mock()
        
        mock_connection.getMetaData.return_value = mock_metadata
        mock_metadata.getColumns.return_value = mock_result_set
        mock_result_set.next.return_value = True
        mock_result_set.getString.return_value = "TIMESTAMP"
        mock_result_set.getInt.side_effect = lambda col: {
            'DATA_TYPE': 93,
            'COLUMN_SIZE': 23,
            'NULLABLE': 1
        }[col]
        mock_result_set.close = Mock()
        
        # First call
        result1 = self.bookmark_manager._get_column_data_type("customers", "updated_at", mock_connection)
        
        # Second call should use cache
        result2 = self.bookmark_manager._get_column_data_type("customers", "updated_at", mock_connection)
        
        # Should only call getMetaData once due to caching
        self.assertEqual(mock_connection.getMetaData.call_count, 1)
        self.assertEqual(result1, result2)
        self.assertEqual(result1, "TIMESTAMP")
        
        # Verify cache was used for second call
        self.bookmark_manager.structured_logger.debug.assert_called()
    
    def test_get_column_data_type_cache_failure(self):
        """Test that failed queries are also cached to avoid repeated failures."""
        # Mock connection that raises exception
        mock_connection = Mock()
        mock_connection.getMetaData.side_effect = Exception("Database connection error")
        
        # First call (should fail and cache the failure)
        result1 = self.bookmark_manager._get_column_data_type("customers", "updated_at", mock_connection)
        
        # Second call should use cached failure
        result2 = self.bookmark_manager._get_column_data_type("customers", "updated_at", mock_connection)
        
        # Should only call getMetaData once due to caching
        self.assertEqual(mock_connection.getMetaData.call_count, 1)
        self.assertIsNone(result1)
        self.assertIsNone(result2)
    
    def test_determine_strategy_from_data_type_timestamp_types(self):
        """Test strategy determination for timestamp data types."""
        timestamp_types = [
            'TIMESTAMP', 'TIMESTAMP_WITH_TIMEZONE', 'TIMESTAMPTZ', 'DATE', 
            'DATETIME', 'TIME', 'DATETIME2', 'SMALLDATETIME'
        ]
        
        for data_type in timestamp_types:
            strategy = self.bookmark_manager._determine_strategy_from_data_type(data_type)
            self.assertEqual(strategy, 'timestamp', f"Failed for type: {data_type}")
    
    def test_determine_strategy_from_data_type_integer_types(self):
        """Test strategy determination for integer data types."""
        integer_types = [
            'INTEGER', 'BIGINT', 'SMALLINT', 'TINYINT', 'INT', 'INT4', 'INT8',
            'SERIAL', 'BIGSERIAL', 'NUMBER'
        ]
        
        for data_type in integer_types:
            strategy = self.bookmark_manager._determine_strategy_from_data_type(data_type)
            if data_type == 'NUMBER':
                # Oracle NUMBER defaults to hash unless we know it's an integer
                self.assertEqual(strategy, 'hash', f"Failed for type: {data_type}")
            else:
                self.assertEqual(strategy, 'primary_key', f"Failed for type: {data_type}")
    
    def test_determine_strategy_from_data_type_string_types(self):
        """Test strategy determination for string and other data types."""
        string_types = [
            'VARCHAR', 'VARCHAR2', 'NVARCHAR', 'NVARCHAR2', 'CHAR', 'NCHAR',
            'TEXT', 'NTEXT', 'CLOB', 'NCLOB', 'DECIMAL', 'NUMERIC', 'FLOAT',
            'REAL', 'DOUBLE', 'DOUBLE_PRECISION', 'BOOLEAN', 'BIT', 'BINARY',
            'VARBINARY', 'BLOB', 'BYTEA', 'RAW', 'LONG_RAW', 'UUID',
            'UNIQUEIDENTIFIER', 'XML', 'JSON', 'JSONB'
        ]
        
        for data_type in string_types:
            strategy = self.bookmark_manager._determine_strategy_from_data_type(data_type)
            self.assertEqual(strategy, 'hash', f"Failed for type: {data_type}")
    
    def test_determine_strategy_from_data_type_case_insensitive(self):
        """Test that strategy determination is case insensitive."""
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
        
        for data_type, expected_strategy in test_cases:
            strategy = self.bookmark_manager._determine_strategy_from_data_type(data_type)
            self.assertEqual(strategy, expected_strategy, f"Failed for type: {data_type}")
    
    def test_determine_strategy_from_data_type_partial_matches(self):
        """Test strategy determination with partial matches for unknown types."""
        test_cases = [
            ('TIMESTAMP_WITH_LOCAL_TIMEZONE', 'timestamp'),
            ('CUSTOM_TIMESTAMP_TYPE', 'timestamp'),
            ('BIGINT_UNSIGNED', 'primary_key'),
            ('CUSTOM_INT_TYPE', 'primary_key'),
            ('VARCHAR_EXTENDED', 'hash'),
            ('UNKNOWN_TYPE', 'hash')
        ]
        
        for data_type, expected_strategy in test_cases:
            strategy = self.bookmark_manager._determine_strategy_from_data_type(data_type)
            self.assertEqual(strategy, expected_strategy, f"Failed for type: {data_type}")
    
    def test_determine_strategy_from_data_type_invalid_input(self):
        """Test strategy determination with invalid input."""
        # Test with None
        strategy = self.bookmark_manager._determine_strategy_from_data_type(None)
        self.assertEqual(strategy, 'hash')
        
        # Test with empty string
        strategy = self.bookmark_manager._determine_strategy_from_data_type("")
        self.assertEqual(strategy, 'hash')
        
        # Test with non-string input
        strategy = self.bookmark_manager._determine_strategy_from_data_type(123)
        self.assertEqual(strategy, 'hash')
        
        # Verify warning was logged
        self.bookmark_manager.structured_logger.warning.assert_called()
    
    def test_determine_strategy_from_data_type_whitespace_handling(self):
        """Test strategy determination handles whitespace correctly."""
        test_cases = [
            ('  TIMESTAMP  ', 'timestamp'),
            ('\tINTEGER\t', 'primary_key'),
            ('\nVARCHAR\n', 'hash')
        ]
        
        for data_type, expected_strategy in test_cases:
            strategy = self.bookmark_manager._determine_strategy_from_data_type(data_type)
            self.assertEqual(strategy, expected_strategy, f"Failed for type: '{data_type}'")
    
    def test_clear_jdbc_metadata_cache(self):
        """Test clearing the JDBC metadata cache."""
        # Add some data to cache
        self.bookmark_manager._jdbc_metadata_cache = {
            "customers.updated_at": "TIMESTAMP",
            "orders.order_id": "INTEGER"
        }
        
        self.assertEqual(len(self.bookmark_manager._jdbc_metadata_cache), 2)
        
        self.bookmark_manager._clear_jdbc_metadata_cache()
        
        self.assertEqual(len(self.bookmark_manager._jdbc_metadata_cache), 0)
        self.bookmark_manager.structured_logger.info.assert_called()
    
    def test_clear_jdbc_metadata_cache_uninitialized(self):
        """Test clearing cache when it's not initialized."""
        # Ensure cache is not initialized
        if hasattr(self.bookmark_manager, '_jdbc_metadata_cache'):
            delattr(self.bookmark_manager, '_jdbc_metadata_cache')
        
        self.bookmark_manager._clear_jdbc_metadata_cache()
        
        # Should log debug message about uninitialized cache
        self.bookmark_manager.structured_logger.debug.assert_called()
    
    def test_get_jdbc_metadata_cache_stats(self):
        """Test getting JDBC metadata cache statistics."""
        # Test with uninitialized cache
        if hasattr(self.bookmark_manager, '_jdbc_metadata_cache'):
            delattr(self.bookmark_manager, '_jdbc_metadata_cache')
        
        stats = self.bookmark_manager._get_jdbc_metadata_cache_stats()
        
        expected_stats = {
            'cached_entries': 0,
            'cache_initialized': False
        }
        self.assertEqual(stats, expected_stats)
        
        # Test with initialized cache containing successful and failed entries
        self.bookmark_manager._jdbc_metadata_cache = {
            "customers.updated_at": "TIMESTAMP",  # Successful
            "orders.order_id": "INTEGER",         # Successful
            "products.missing_col": None          # Failed
        }
        
        stats = self.bookmark_manager._get_jdbc_metadata_cache_stats()
        
        expected_stats = {
            'cached_entries': 3,
            'cache_initialized': True,
            'successful_entries': 2,
            'failed_entries': 1
        }
        self.assertEqual(stats, expected_stats)
        self.bookmark_manager.structured_logger.debug.assert_called()
    
    def test_get_jdbc_metadata_cache_stats_empty_cache(self):
        """Test getting cache statistics with empty initialized cache."""
        self.bookmark_manager._jdbc_metadata_cache = {}
        
        stats = self.bookmark_manager._get_jdbc_metadata_cache_stats()
        
        expected_stats = {
            'cached_entries': 0,
            'cache_initialized': True,
            'successful_entries': 0,
            'failed_entries': 0
        }
        self.assertEqual(stats, expected_stats)
    
    def test_jdbc_metadata_integration(self):
        """Test integration between _get_column_data_type and _determine_strategy_from_data_type."""
        # Mock JDBC connection and metadata
        mock_connection = Mock()
        mock_metadata = Mock()
        mock_result_set = Mock()
        
        mock_connection.getMetaData.return_value = mock_metadata
        mock_metadata.getColumns.return_value = mock_result_set
        mock_result_set.next.return_value = True
        mock_result_set.getString.return_value = "TIMESTAMP"
        mock_result_set.getInt.side_effect = lambda col: {
            'DATA_TYPE': 93,
            'COLUMN_SIZE': 23,
            'NULLABLE': 1
        }[col]
        mock_result_set.close = Mock()
        
        # Get column data type
        data_type = self.bookmark_manager._get_column_data_type("customers", "updated_at", mock_connection)
        
        # Determine strategy from data type
        strategy = self.bookmark_manager._determine_strategy_from_data_type(data_type)
        
        self.assertEqual(data_type, "TIMESTAMP")
        self.assertEqual(strategy, "timestamp")
    
    def test_performance_logging(self):
        """Test that performance metrics are logged for JDBC operations."""
        # Mock JDBC connection and metadata
        mock_connection = Mock()
        mock_metadata = Mock()
        mock_result_set = Mock()
        
        mock_connection.getMetaData.return_value = mock_metadata
        mock_metadata.getColumns.return_value = mock_result_set
        mock_result_set.next.return_value = True
        mock_result_set.getString.return_value = "TIMESTAMP"
        mock_result_set.getInt.side_effect = lambda col: {
            'DATA_TYPE': 93,
            'COLUMN_SIZE': 23,
            'NULLABLE': 1
        }[col]
        mock_result_set.close = Mock()
        
        with patch('time.time', side_effect=[0.0, 0.1]):  # Mock 100ms duration
            result = self.bookmark_manager._get_column_data_type("customers", "updated_at", mock_connection)
        
        self.assertEqual(result, "TIMESTAMP")
        
        # Verify performance logging was called with duration
        info_calls = self.bookmark_manager.structured_logger.info.call_args_list
        self.assertTrue(any('duration_ms' in str(call) for call in info_calls))


if __name__ == '__main__':
    unittest.main()