"""
Unit tests for BookmarkStrategyResolver class (Task 25).

This module provides comprehensive unit tests for:
- Strategy resolution with manual and automatic detection
- Fallback behavior when manual configuration fails
- Caching of JDBC metadata queries for performance
- Error handling for missing columns and invalid configurations

Requirements covered: 8.1, 8.4, 8.7
"""

import pytest
import unittest
import time
from unittest.mock import Mock, MagicMock, patch, call
from typing import Dict, Any, Optional, Tuple

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from glue_job.storage.manual_bookmark_config import (
    ManualBookmarkConfig, 
    BookmarkStrategyResolver
)


class TestBookmarkStrategyResolverInitialization(unittest.TestCase):
    """Test cases for BookmarkStrategyResolver initialization."""
    
    def test_initialization_with_empty_manual_configs(self):
        """Test initializing resolver with empty manual configurations."""
        resolver = BookmarkStrategyResolver({})
        
        self.assertEqual(len(resolver.manual_configs), 0)
        self.assertEqual(len(resolver.jdbc_metadata_cache), 0)
        self.assertIsNotNone(resolver.logger)
        self.assertEqual(resolver.structured_logger, resolver.logger)
    
    def test_initialization_with_manual_configs(self):
        """Test initializing resolver with manual configurations."""
        manual_configs = {
            "customers": ManualBookmarkConfig("customers", "updated_at"),
            "orders": ManualBookmarkConfig("orders", "order_timestamp")
        }
        
        resolver = BookmarkStrategyResolver(manual_configs)
        
        self.assertEqual(len(resolver.manual_configs), 2)
        self.assertIn("customers", resolver.manual_configs)
        self.assertIn("orders", resolver.manual_configs)
        self.assertEqual(resolver.manual_configs["customers"].column_name, "updated_at")
        self.assertEqual(resolver.manual_configs["orders"].column_name, "order_timestamp")
    
    def test_initialization_with_structured_logger(self):
        """Test initializing resolver with structured logger."""
        mock_structured_logger = Mock()
        resolver = BookmarkStrategyResolver({}, mock_structured_logger)
        
        self.assertEqual(resolver.structured_logger, mock_structured_logger)
    
    def test_initialization_with_none_manual_configs(self):
        """Test initializing resolver with None manual configurations."""
        resolver = BookmarkStrategyResolver(None)
        
        self.assertEqual(len(resolver.manual_configs), 0)
        self.assertIsInstance(resolver.manual_configs, dict)


class TestBookmarkStrategyResolution(unittest.TestCase):
    """Test cases for bookmark strategy resolution with manual and automatic detection."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.mock_connection = Mock()
        self.mock_structured_logger = Mock()
        
        # Create manual configurations for testing
        self.manual_configs = {
            "customers": ManualBookmarkConfig("customers", "updated_at"),
            "orders": ManualBookmarkConfig("orders", "order_id"),
            "products": ManualBookmarkConfig("products", "product_name")
        }
        
        self.resolver = BookmarkStrategyResolver(self.manual_configs, self.mock_structured_logger)
    
    def test_resolve_strategy_with_manual_config_timestamp(self):
        """Test strategy resolution using manual configuration for timestamp column."""
        # Mock successful metadata retrieval for timestamp column
        with patch.object(self.resolver, '_get_column_metadata') as mock_get_metadata:
            mock_get_metadata.return_value = {
                'column_name': 'updated_at',
                'data_type': 'TIMESTAMP',
                'jdbc_type': 93,
                'column_size': 23,
                'nullable': 1
            }
            
            strategy, column, is_manual = self.resolver.resolve_strategy("customers", self.mock_connection)
            
            self.assertEqual(strategy, "timestamp")
            self.assertEqual(column, "updated_at")
            self.assertTrue(is_manual)
            
            # Verify metadata query was called
            mock_get_metadata.assert_called_once_with(self.mock_connection, "customers", "updated_at")
    
    def test_resolve_strategy_with_manual_config_integer(self):
        """Test strategy resolution using manual configuration for integer column."""
        # Mock successful metadata retrieval for integer column
        with patch.object(self.resolver, '_get_column_metadata') as mock_get_metadata:
            mock_get_metadata.return_value = {
                'column_name': 'order_id',
                'data_type': 'INTEGER',
                'jdbc_type': 4,
                'column_size': 10,
                'nullable': 0
            }
            
            strategy, column, is_manual = self.resolver.resolve_strategy("orders", self.mock_connection)
            
            self.assertEqual(strategy, "primary_key")
            self.assertEqual(column, "order_id")
            self.assertTrue(is_manual)
    
    def test_resolve_strategy_with_manual_config_string(self):
        """Test strategy resolution using manual configuration for string column."""
        # Mock successful metadata retrieval for string column
        with patch.object(self.resolver, '_get_column_metadata') as mock_get_metadata:
            mock_get_metadata.return_value = {
                'column_name': 'product_name',
                'data_type': 'VARCHAR',
                'jdbc_type': 12,
                'column_size': 255,
                'nullable': 1
            }
            
            strategy, column, is_manual = self.resolver.resolve_strategy("products", self.mock_connection)
            
            self.assertEqual(strategy, "hash")
            self.assertEqual(column, "product_name")
            self.assertTrue(is_manual)
    
    def test_resolve_strategy_without_manual_config(self):
        """Test strategy resolution falling back to automatic detection."""
        # Mock automatic detection
        with patch.object(self.resolver, '_get_automatic_strategy') as mock_auto_strategy:
            mock_auto_strategy.return_value = ("timestamp", "created_at")
            
            strategy, column, is_manual = self.resolver.resolve_strategy("unknown_table", self.mock_connection)
            
            self.assertEqual(strategy, "timestamp")
            self.assertEqual(column, "created_at")
            self.assertFalse(is_manual)
            
            # Verify automatic detection was called
            mock_auto_strategy.assert_called_once_with("unknown_table", self.mock_connection)
    
    def test_resolve_strategy_manual_config_column_not_found(self):
        """Test strategy resolution when manual config column doesn't exist."""
        # Mock metadata retrieval returning None (column not found)
        with patch.object(self.resolver, '_get_column_metadata') as mock_get_metadata, \
             patch.object(self.resolver, '_get_automatic_strategy') as mock_auto_strategy:
            
            mock_get_metadata.return_value = None
            mock_auto_strategy.return_value = ("hash", None)
            
            strategy, column, is_manual = self.resolver.resolve_strategy("customers", self.mock_connection)
            
            self.assertEqual(strategy, "hash")
            self.assertIsNone(column)
            self.assertFalse(is_manual)
            
            # Verify fallback to automatic detection
            mock_auto_strategy.assert_called_once_with("customers", self.mock_connection)
    
    def test_resolve_strategy_manual_config_metadata_error(self):
        """Test strategy resolution when metadata query raises exception."""
        # Mock metadata retrieval raising exception
        with patch.object(self.resolver, '_get_column_metadata') as mock_get_metadata, \
             patch.object(self.resolver, '_get_automatic_strategy') as mock_auto_strategy:
            
            mock_get_metadata.side_effect = Exception("Database connection error")
            mock_auto_strategy.return_value = ("timestamp", "created_at")
            
            strategy, column, is_manual = self.resolver.resolve_strategy("customers", self.mock_connection)
            
            self.assertEqual(strategy, "timestamp")
            self.assertEqual(column, "created_at")
            self.assertFalse(is_manual)
            
            # Verify fallback to automatic detection
            mock_auto_strategy.assert_called_once_with("customers", self.mock_connection)
    
    def test_resolve_strategy_logging_calls(self):
        """Test that appropriate logging methods are called during strategy resolution."""
        # Mock successful manual configuration
        with patch.object(self.resolver, '_get_column_metadata') as mock_get_metadata:
            mock_get_metadata.return_value = {
                'column_name': 'updated_at',
                'data_type': 'TIMESTAMP',
                'jdbc_type': 93,
                'column_size': 23,
                'nullable': 1
            }
            
            self.resolver.resolve_strategy("customers", self.mock_connection)
            
            # Verify structured logging calls
            self.mock_structured_logger.log_bookmark_strategy_resolution_start.assert_called_once_with(
                "customers", True
            )
            self.mock_structured_logger.log_bookmark_strategy_resolution_success.assert_called_once()
    
    def test_resolve_strategy_performance_timing(self):
        """Test that strategy resolution timing is measured correctly."""
        # Mock time.time to control timing
        with patch('time.time') as mock_time, \
             patch.object(self.resolver, '_get_column_metadata') as mock_get_metadata:
            
            # Set up time progression - need more calls for all the time.time() calls in the code
            mock_time.side_effect = [1000.0, 1000.02, 1000.05, 1000.1]  # Multiple time calls
            
            mock_get_metadata.return_value = {
                'column_name': 'updated_at',
                'data_type': 'TIMESTAMP',
                'jdbc_type': 93,
                'column_size': 23,
                'nullable': 1
            }
            
            self.resolver.resolve_strategy("customers", self.mock_connection)
            
            # Verify timing was captured in logging call
            args, kwargs = self.mock_structured_logger.log_bookmark_strategy_resolution_success.call_args
            resolution_duration_ms = args[4]  # 5th argument is duration
            # Use assertAlmostEqual for floating point comparison
            self.assertAlmostEqual(resolution_duration_ms, 100.0, places=1)


class TestManualStrategyResolution(unittest.TestCase):
    """Test cases for manual strategy resolution logic."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.mock_connection = Mock()
        self.mock_structured_logger = Mock()
        
        self.manual_configs = {
            "customers": ManualBookmarkConfig("customers", "updated_at")
        }
        
        self.resolver = BookmarkStrategyResolver(self.manual_configs, self.mock_structured_logger)
    
    def test_get_manual_strategy_success(self):
        """Test successful manual strategy resolution."""
        with patch.object(self.resolver, '_get_column_metadata') as mock_get_metadata:
            mock_get_metadata.return_value = {
                'column_name': 'updated_at',
                'data_type': 'TIMESTAMP',
                'jdbc_type': 93,
                'column_size': 23,
                'nullable': 1
            }
            
            result = self.resolver._get_manual_strategy("customers", self.mock_connection)
            
            self.assertIsNotNone(result)
            strategy, column, data_type = result
            self.assertEqual(strategy, "timestamp")
            self.assertEqual(column, "updated_at")
            self.assertEqual(data_type, "TIMESTAMP")
    
    def test_get_manual_strategy_no_config(self):
        """Test manual strategy resolution when no config exists for table."""
        result = self.resolver._get_manual_strategy("unknown_table", self.mock_connection)
        
        self.assertIsNone(result)
    
    def test_get_manual_strategy_column_not_found(self):
        """Test manual strategy resolution when column is not found."""
        with patch.object(self.resolver, '_get_column_metadata') as mock_get_metadata:
            mock_get_metadata.return_value = None
            
            result = self.resolver._get_manual_strategy("customers", self.mock_connection)
            
            self.assertIsNone(result)
            
            # Verify error logging
            self.mock_structured_logger.log_manual_config_column_not_found.assert_called_once_with(
                "customers", "updated_at"
            )
    
    def test_get_manual_strategy_metadata_exception(self):
        """Test manual strategy resolution when metadata query raises exception."""
        with patch.object(self.resolver, '_get_column_metadata') as mock_get_metadata:
            mock_get_metadata.side_effect = Exception("Connection timeout")
            
            result = self.resolver._get_manual_strategy("customers", self.mock_connection)
            
            self.assertIsNone(result)
            
            # Verify error logging
            self.mock_structured_logger.log_manual_config_validation_failure.assert_called_once()
    
    def test_get_manual_strategy_validation_timing(self):
        """Test that manual strategy validation timing is measured."""
        with patch('time.time') as mock_time, \
             patch.object(self.resolver, '_get_column_metadata') as mock_get_metadata:
            
            # Set up time progression for validation
            mock_time.side_effect = [2000.0, 2000.05]  # 50ms validation duration
            
            mock_get_metadata.return_value = {
                'column_name': 'updated_at',
                'data_type': 'TIMESTAMP',
                'jdbc_type': 93,
                'column_size': 23,
                'nullable': 1
            }
            
            self.resolver._get_manual_strategy("customers", self.mock_connection)
            
            # Verify validation timing was logged
            self.mock_structured_logger.log_manual_config_validation_success.assert_called_once()
            args, kwargs = self.mock_structured_logger.log_manual_config_validation_success.call_args
            validation_duration_ms = args[2]  # 3rd argument is duration
            # Use assertAlmostEqual for floating point comparison
            self.assertAlmostEqual(validation_duration_ms, 50.0, places=1)


class TestAutomaticStrategyDetection(unittest.TestCase):
    """Test cases for automatic strategy detection fallback."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.mock_connection = Mock()
        self.resolver = BookmarkStrategyResolver({})
    
    def test_get_automatic_strategy_timestamp_detection(self):
        """Test automatic detection of timestamp columns."""
        with patch.object(self.resolver, '_detect_timestamp_columns') as mock_detect_ts, \
             patch.object(self.resolver, '_select_best_timestamp_column') as mock_select_ts:
            
            mock_detect_ts.return_value = ['updated_at', 'created_at', 'modified_date']
            mock_select_ts.return_value = 'updated_at'
            
            strategy, column = self.resolver._get_automatic_strategy("customers", self.mock_connection)
            
            self.assertEqual(strategy, "timestamp")
            self.assertEqual(column, "updated_at")
            
            mock_detect_ts.assert_called_once_with(self.mock_connection, "customers")
            mock_select_ts.assert_called_once_with(['updated_at', 'created_at', 'modified_date'])
    
    def test_get_automatic_strategy_primary_key_detection(self):
        """Test automatic detection of primary key columns when no timestamp columns found."""
        with patch.object(self.resolver, '_detect_timestamp_columns') as mock_detect_ts, \
             patch.object(self.resolver, '_detect_primary_key_columns') as mock_detect_pk, \
             patch.object(self.resolver, '_select_best_primary_key_column') as mock_select_pk:
            
            mock_detect_ts.return_value = []  # No timestamp columns
            mock_detect_pk.return_value = ['id', 'customer_id', 'pk']
            mock_select_pk.return_value = 'id'
            
            strategy, column = self.resolver._get_automatic_strategy("customers", self.mock_connection)
            
            self.assertEqual(strategy, "primary_key")
            self.assertEqual(column, "id")
    
    def test_get_automatic_strategy_hash_fallback(self):
        """Test automatic detection falling back to hash strategy."""
        with patch.object(self.resolver, '_detect_timestamp_columns') as mock_detect_ts, \
             patch.object(self.resolver, '_detect_primary_key_columns') as mock_detect_pk:
            
            mock_detect_ts.return_value = []  # No timestamp columns
            mock_detect_pk.return_value = []  # No primary key columns
            
            strategy, column = self.resolver._get_automatic_strategy("customers", self.mock_connection)
            
            self.assertEqual(strategy, "hash")
            self.assertIsNone(column)
    
    def test_get_automatic_strategy_exception_handling(self):
        """Test automatic detection handles exceptions gracefully."""
        with patch.object(self.resolver, '_detect_timestamp_columns') as mock_detect_ts:
            mock_detect_ts.side_effect = Exception("Database error")
            
            strategy, column = self.resolver._get_automatic_strategy("customers", self.mock_connection)
            
            self.assertEqual(strategy, "hash")
            self.assertIsNone(column)


class TestJDBCMetadataCaching(unittest.TestCase):
    """Test cases for JDBC metadata query caching for performance."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.mock_connection = Mock()
        self.mock_structured_logger = Mock()
        self.resolver = BookmarkStrategyResolver({}, self.mock_structured_logger)
    
    def test_metadata_caching_first_query(self):
        """Test that first metadata query is cached."""
        # Mock JDBC metadata response
        mock_metadata = Mock()
        mock_result_set = Mock()
        
        self.mock_connection.getMetaData.return_value = mock_metadata
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
        
        # First query
        result1 = self.resolver._get_column_metadata(self.mock_connection, "customers", "updated_at")
        
        # Verify result and caching
        self.assertIsNotNone(result1)
        self.assertEqual(result1['data_type'], 'TIMESTAMP')
        self.assertIn("customers.updated_at", self.resolver.jdbc_metadata_cache)
        
        # Verify JDBC calls were made
        self.mock_connection.getMetaData.assert_called_once()
        mock_metadata.getColumns.assert_called_once()
    
    def test_metadata_caching_cache_hit(self):
        """Test that subsequent queries use cached results."""
        # Pre-populate cache
        cache_key = "customers.updated_at"
        cached_result = {
            'column_name': 'updated_at',
            'data_type': 'TIMESTAMP',
            'jdbc_type': 93,
            'column_size': 23,
            'nullable': 1
        }
        self.resolver.jdbc_metadata_cache[cache_key] = cached_result
        
        # Query should return cached result without JDBC calls
        result = self.resolver._get_column_metadata(self.mock_connection, "customers", "updated_at")
        
        self.assertEqual(result, cached_result)
        
        # Verify no JDBC calls were made
        self.mock_connection.getMetaData.assert_not_called()
        
        # Verify cache hit logging
        self.mock_structured_logger.log_jdbc_metadata_cache_hit.assert_called_once_with(
            "customers", "updated_at", "TIMESTAMP"
        )
    
    def test_metadata_caching_cache_miss_then_hit(self):
        """Test cache miss followed by cache hit for same query."""
        # Mock JDBC metadata response
        mock_metadata = Mock()
        mock_result_set = Mock()
        
        self.mock_connection.getMetaData.return_value = mock_metadata
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
        
        # First query (cache miss)
        result1 = self.resolver._get_column_metadata(self.mock_connection, "customers", "updated_at")
        
        # Reset mocks to verify second query doesn't make JDBC calls
        self.mock_connection.reset_mock()
        mock_metadata.reset_mock()
        
        # Second query (cache hit)
        result2 = self.resolver._get_column_metadata(self.mock_connection, "customers", "updated_at")
        
        # Results should be identical
        self.assertEqual(result1, result2)
        
        # Second query should not make JDBC calls
        self.mock_connection.getMetaData.assert_not_called()
    
    def test_metadata_caching_different_tables_columns(self):
        """Test that different table/column combinations are cached separately."""
        # Mock JDBC metadata responses for different queries
        mock_metadata = Mock()
        mock_result_set1 = Mock()
        mock_result_set2 = Mock()
        
        self.mock_connection.getMetaData.return_value = mock_metadata
        
        # First query setup
        mock_result_set1.next.return_value = True
        mock_result_set1.getString.side_effect = lambda col: {
            'COLUMN_NAME': 'updated_at',
            'TYPE_NAME': 'TIMESTAMP'
        }[col]
        mock_result_set1.getInt.side_effect = lambda col: {
            'DATA_TYPE': 93,
            'COLUMN_SIZE': 23,
            'NULLABLE': 1
        }[col]
        
        # Second query setup
        mock_result_set2.next.return_value = True
        mock_result_set2.getString.side_effect = lambda col: {
            'COLUMN_NAME': 'order_id',
            'TYPE_NAME': 'INTEGER'
        }[col]
        mock_result_set2.getInt.side_effect = lambda col: {
            'DATA_TYPE': 4,
            'COLUMN_SIZE': 10,
            'NULLABLE': 0
        }[col]
        
        # Configure getColumns to return different result sets
        mock_metadata.getColumns.side_effect = [mock_result_set1, mock_result_set2]
        
        # Make two different queries
        result1 = self.resolver._get_column_metadata(self.mock_connection, "customers", "updated_at")
        result2 = self.resolver._get_column_metadata(self.mock_connection, "orders", "order_id")
        
        # Verify both results are cached separately
        self.assertIn("customers.updated_at", self.resolver.jdbc_metadata_cache)
        self.assertIn("orders.order_id", self.resolver.jdbc_metadata_cache)
        
        self.assertEqual(result1['data_type'], 'TIMESTAMP')
        self.assertEqual(result2['data_type'], 'INTEGER')
        
        # Verify both JDBC calls were made
        self.assertEqual(mock_metadata.getColumns.call_count, 2)
    
    def test_metadata_caching_null_results(self):
        """Test that null results (column not found) are also cached."""
        # Mock JDBC metadata response for column not found
        mock_metadata = Mock()
        mock_result_set = Mock()
        
        self.mock_connection.getMetaData.return_value = mock_metadata
        mock_metadata.getColumns.return_value = mock_result_set
        mock_result_set.next.return_value = False  # Column not found
        
        # First query
        result1 = self.resolver._get_column_metadata(self.mock_connection, "customers", "nonexistent")
        
        # Reset mocks
        self.mock_connection.reset_mock()
        mock_metadata.reset_mock()
        
        # Second query should use cached null result
        result2 = self.resolver._get_column_metadata(self.mock_connection, "customers", "nonexistent")
        
        # Both results should be None
        self.assertIsNone(result1)
        self.assertIsNone(result2)
        
        # Second query should not make JDBC calls
        self.mock_connection.getMetaData.assert_not_called()
        
        # Verify null result is cached
        self.assertIn("customers.nonexistent", self.resolver.jdbc_metadata_cache)
        self.assertIsNone(self.resolver.jdbc_metadata_cache["customers.nonexistent"])
    
    def test_metadata_caching_exception_handling(self):
        """Test that exceptions during metadata queries are cached as failures."""
        # Mock JDBC metadata to raise exception
        self.mock_connection.getMetaData.side_effect = Exception("Connection timeout")
        
        # First query should handle exception
        result1 = self.resolver._get_column_metadata(self.mock_connection, "customers", "updated_at")
        
        # Reset mock
        self.mock_connection.reset_mock()
        self.mock_connection.getMetaData.side_effect = None  # Remove exception
        
        # Second query should use cached failure
        result2 = self.resolver._get_column_metadata(self.mock_connection, "customers", "updated_at")
        
        # Both results should be None
        self.assertIsNone(result1)
        self.assertIsNone(result2)
        
        # Second query should not make JDBC calls
        self.mock_connection.getMetaData.assert_not_called()
        
        # Verify failure is cached
        self.assertIn("customers.updated_at", self.resolver.jdbc_metadata_cache)
        self.assertIsNone(self.resolver.jdbc_metadata_cache["customers.updated_at"])
    
    def test_clear_cache_functionality(self):
        """Test that cache can be cleared."""
        # Populate cache
        self.resolver.jdbc_metadata_cache["customers.updated_at"] = {"data_type": "TIMESTAMP"}
        self.resolver.jdbc_metadata_cache["orders.order_id"] = {"data_type": "INTEGER"}
        
        # Verify cache has entries
        self.assertEqual(len(self.resolver.jdbc_metadata_cache), 2)
        
        # Clear cache
        self.resolver.clear_cache()
        
        # Verify cache is empty
        self.assertEqual(len(self.resolver.jdbc_metadata_cache), 0)
    
    def test_get_cache_stats(self):
        """Test cache statistics reporting."""
        # Populate cache and manual configs
        self.resolver.jdbc_metadata_cache["customers.updated_at"] = {"data_type": "TIMESTAMP"}
        self.resolver.jdbc_metadata_cache["orders.order_id"] = {"data_type": "INTEGER"}
        
        manual_configs = {
            "customers": ManualBookmarkConfig("customers", "updated_at")
        }
        resolver_with_configs = BookmarkStrategyResolver(manual_configs)
        resolver_with_configs.jdbc_metadata_cache = self.resolver.jdbc_metadata_cache
        
        stats = resolver_with_configs.get_cache_stats()
        
        self.assertEqual(stats['cached_entries'], 2)
        self.assertEqual(stats['manual_configs'], 1)


class TestDataTypeMappingLogic(unittest.TestCase):
    """Test cases for JDBC data type to strategy mapping."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.mock_structured_logger = Mock()
        self.resolver = BookmarkStrategyResolver({}, self.mock_structured_logger)
    
    def test_map_jdbc_type_direct_timestamp_mapping(self):
        """Test direct mapping of timestamp types."""
        timestamp_types = [
            'TIMESTAMP', 'TIMESTAMP_WITH_TIMEZONE', 'DATE', 'DATETIME', 'TIME'
        ]
        
        for jdbc_type in timestamp_types:
            strategy = self.resolver._map_jdbc_type_to_strategy(jdbc_type)
            self.assertEqual(strategy, "timestamp", f"Failed for type: {jdbc_type}")
    
    def test_map_jdbc_type_direct_integer_mapping(self):
        """Test direct mapping of integer types."""
        integer_types = [
            'INTEGER', 'BIGINT', 'SMALLINT', 'TINYINT', 'SERIAL', 'BIGSERIAL'
        ]
        
        for jdbc_type in integer_types:
            strategy = self.resolver._map_jdbc_type_to_strategy(jdbc_type)
            self.assertEqual(strategy, "primary_key", f"Failed for type: {jdbc_type}")
    
    def test_map_jdbc_type_direct_string_mapping(self):
        """Test direct mapping of string and other types."""
        string_types = [
            'VARCHAR', 'CHAR', 'TEXT', 'CLOB', 'DECIMAL', 'NUMERIC', 
            'FLOAT', 'DOUBLE', 'BOOLEAN', 'BINARY', 'VARBINARY', 'BLOB'
        ]
        
        for jdbc_type in string_types:
            strategy = self.resolver._map_jdbc_type_to_strategy(jdbc_type)
            self.assertEqual(strategy, "hash", f"Failed for type: {jdbc_type}")
    
    def test_map_jdbc_type_case_insensitive(self):
        """Test that type mapping is case insensitive."""
        test_cases = [
            ('timestamp', 'timestamp'),
            ('TIMESTAMP', 'timestamp'),
            ('Timestamp', 'timestamp'),
            ('integer', 'primary_key'),
            ('INTEGER', 'primary_key'),
            ('Integer', 'primary_key'),
            ('varchar', 'hash'),
            ('VARCHAR', 'hash'),
            ('VarChar', 'hash')
        ]
        
        for input_type, expected_strategy in test_cases:
            strategy = self.resolver._map_jdbc_type_to_strategy(input_type)
            self.assertEqual(strategy, expected_strategy, f"Failed for type: {input_type}")
    
    def test_map_jdbc_type_whitespace_handling(self):
        """Test that type mapping handles whitespace correctly."""
        test_cases = [
            ('  TIMESTAMP  ', 'timestamp'),
            ('\tINTEGER\t', 'primary_key'),
            ('\nVARCHAR\n', 'hash'),
            ('  timestamp  ', 'timestamp')
        ]
        
        for input_type, expected_strategy in test_cases:
            strategy = self.resolver._map_jdbc_type_to_strategy(input_type)
            self.assertEqual(strategy, expected_strategy, f"Failed for type: '{input_type}'")
    
    def test_map_jdbc_type_partial_match_timestamp(self):
        """Test partial matching for timestamp-like types."""
        timestamp_like_types = [
            'TIMESTAMP_WITH_LOCAL_TIMEZONE',
            'DATETIME2',
            'CUSTOM_TIMESTAMP_TYPE',
            'DATE_MODIFIED',
            'TIME_CREATED'
        ]
        
        for jdbc_type in timestamp_like_types:
            strategy = self.resolver._map_jdbc_type_to_strategy(jdbc_type)
            self.assertEqual(strategy, "timestamp", f"Failed for type: {jdbc_type}")
    
    def test_map_jdbc_type_partial_match_integer(self):
        """Test partial matching for integer-like types."""
        integer_like_types = [
            'BIGINT_UNSIGNED',
            'INTEGER_IDENTITY',
            'SERIAL_PRIMARY_KEY',
            'CUSTOM_INT_TYPE',
            'NUMBER_INT'
        ]
        
        for jdbc_type in integer_like_types:
            strategy = self.resolver._map_jdbc_type_to_strategy(jdbc_type)
            self.assertEqual(strategy, "primary_key", f"Failed for type: {jdbc_type}")
    
    def test_map_jdbc_type_unknown_fallback(self):
        """Test fallback to hash strategy for unknown types."""
        unknown_types = [
            'UNKNOWN_TYPE',
            'CUSTOM_GEOMETRY',
            'PROPRIETARY_TYPE',
            'XML_DOCUMENT',
            'JSON_ARRAY'
        ]
        
        for jdbc_type in unknown_types:
            strategy = self.resolver._map_jdbc_type_to_strategy(jdbc_type)
            self.assertEqual(strategy, "hash", f"Failed for type: {jdbc_type}")
    
    def test_map_jdbc_type_logging_direct_mapping(self):
        """Test logging for direct type mapping."""
        self.resolver._map_jdbc_type_to_strategy("TIMESTAMP")
        
        # Verify logging calls
        self.mock_structured_logger.log_data_type_mapping_start.assert_called_once_with(
            "TIMESTAMP", "unknown", "unknown"
        )
        self.mock_structured_logger.log_data_type_mapping_success.assert_called_once_with(
            "TIMESTAMP", "timestamp", "unknown", "unknown", "direct"
        )
    
    def test_map_jdbc_type_logging_partial_match(self):
        """Test logging for partial match mapping."""
        self.resolver._map_jdbc_type_to_strategy("CUSTOM_TIMESTAMP_TYPE")
        
        # Verify logging calls
        self.mock_structured_logger.log_data_type_mapping_success.assert_called_once_with(
            "CUSTOM_TIMESTAMP_TYPE", "timestamp", "unknown", "unknown", "partial_match"
        )
    
    def test_map_jdbc_type_logging_fallback(self):
        """Test logging for fallback mapping."""
        self.resolver._map_jdbc_type_to_strategy("UNKNOWN_TYPE")
        
        # Verify logging calls
        self.mock_structured_logger.log_data_type_mapping_fallback.assert_called_once_with(
            "UNKNOWN_TYPE", "hash", "unknown", "unknown", "unknown_type"
        )


class TestErrorHandlingAndEdgeCases(unittest.TestCase):
    """Test cases for error handling and edge cases."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.mock_connection = Mock()
        self.mock_structured_logger = Mock()
        
        # Create resolver with invalid manual config to test error handling
        self.manual_configs = {
            "customers": ManualBookmarkConfig("customers", "nonexistent_column")
        }
        
        self.resolver = BookmarkStrategyResolver(self.manual_configs, self.mock_structured_logger)
    
    def test_resolve_strategy_with_invalid_manual_config(self):
        """Test strategy resolution with invalid manual configuration."""
        # Mock metadata query to return None (column not found)
        with patch.object(self.resolver, '_get_column_metadata') as mock_get_metadata, \
             patch.object(self.resolver, '_get_automatic_strategy') as mock_auto_strategy:
            
            mock_get_metadata.return_value = None
            mock_auto_strategy.return_value = ("hash", None)
            
            strategy, column, is_manual = self.resolver.resolve_strategy("customers", self.mock_connection)
            
            # Should fall back to automatic detection
            self.assertEqual(strategy, "hash")
            self.assertIsNone(column)
            self.assertFalse(is_manual)
            
            # Verify fallback logging
            self.mock_structured_logger.log_bookmark_strategy_resolution_fallback.assert_called_once_with(
                "customers", "manual", "automatic", "manual_config_validation_failed", "hash", None
            )
    
    def test_resolve_strategy_with_connection_error(self):
        """Test strategy resolution when database connection fails."""
        # Mock connection to raise exception
        self.mock_connection.getMetaData.side_effect = Exception("Connection lost")
        
        with patch.object(self.resolver, '_get_automatic_strategy') as mock_auto_strategy:
            mock_auto_strategy.return_value = ("hash", None)
            
            strategy, column, is_manual = self.resolver.resolve_strategy("customers", self.mock_connection)
            
            # Should fall back to automatic detection
            self.assertEqual(strategy, "hash")
            self.assertIsNone(column)
            self.assertFalse(is_manual)
    
    def test_resolve_strategy_with_empty_table_name(self):
        """Test strategy resolution with empty table name."""
        with patch.object(self.resolver, '_get_automatic_strategy') as mock_auto_strategy:
            mock_auto_strategy.return_value = ("hash", None)
            
            strategy, column, is_manual = self.resolver.resolve_strategy("", self.mock_connection)
            
            # Should use automatic detection for empty table name
            self.assertEqual(strategy, "hash")
            self.assertIsNone(column)
            self.assertFalse(is_manual)
    
    def test_resolve_strategy_with_none_connection(self):
        """Test strategy resolution with None connection."""
        with patch.object(self.resolver, '_get_automatic_strategy') as mock_auto_strategy:
            mock_auto_strategy.return_value = ("hash", None)
            
            strategy, column, is_manual = self.resolver.resolve_strategy("customers", None)
            
            # Should handle None connection gracefully
            self.assertEqual(strategy, "hash")
            self.assertIsNone(column)
            self.assertFalse(is_manual)
    
    def test_metadata_query_with_malformed_response(self):
        """Test metadata query handling malformed JDBC responses."""
        # Mock JDBC metadata with malformed response
        mock_metadata = Mock()
        mock_result_set = Mock()
        
        self.mock_connection.getMetaData.return_value = mock_metadata
        mock_metadata.getColumns.return_value = mock_result_set
        mock_result_set.next.return_value = True
        
        # Mock getString to raise exception
        mock_result_set.getString.side_effect = Exception("Malformed response")
        
        result = self.resolver._get_column_metadata(self.mock_connection, "customers", "updated_at")
        
        # Should handle exception and return None
        self.assertIsNone(result)
        
        # Verify error logging
        self.mock_structured_logger.log_jdbc_metadata_query_failure.assert_called_once()
    
    def test_automatic_detection_column_selection_edge_cases(self):
        """Test automatic detection column selection with edge cases."""
        # Test empty column lists
        self.assertEqual(
            self.resolver._select_best_timestamp_column([]), 
            None
        )
        
        self.assertEqual(
            self.resolver._select_best_primary_key_column([]), 
            None
        )
        
        # Test single column lists
        self.assertEqual(
            self.resolver._select_best_timestamp_column(['single_column']), 
            'single_column'
        )
        
        self.assertEqual(
            self.resolver._select_best_primary_key_column(['single_column']), 
            'single_column'
        )
    
    def test_detect_columns_with_jdbc_exceptions(self):
        """Test column detection methods handle JDBC exceptions."""
        # Mock connection to raise exception in metadata calls
        mock_metadata = Mock()
        self.mock_connection.getMetaData.return_value = mock_metadata
        mock_metadata.getColumns.side_effect = Exception("JDBC error")
        mock_metadata.getPrimaryKeys.side_effect = Exception("JDBC error")
        
        # Should handle exceptions gracefully
        timestamp_columns = self.resolver._detect_timestamp_columns(self.mock_connection, "customers")
        pk_columns = self.resolver._detect_primary_key_columns(self.mock_connection, "customers")
        
        self.assertEqual(timestamp_columns, [])
        self.assertEqual(pk_columns, [])


if __name__ == '__main__':
    unittest.main()