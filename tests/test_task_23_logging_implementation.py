"""
Test implementation for Task 23: Comprehensive logging for manual bookmark configuration.

This test suite verifies that all logging functionality for manual bookmark configuration
is working correctly, including:
- Manual configuration parsing and validation logging
- JDBC metadata query logging  
- Data type resolution logging
- Bookmark strategy resolution logging
- Summary and statistics logging
"""

import unittest
from unittest.mock import Mock, patch, MagicMock
import json
import time
from datetime import datetime, timezone

# Import the classes we're testing
from src.glue_job.storage.bookmark_manager import JobBookmarkManager, JobBookmarkState
from src.glue_job.storage.manual_bookmark_config import ManualBookmarkConfig, BookmarkStrategyResolver
from src.glue_job.monitoring.logging import StructuredLogger


class TestTask23LoggingImplementation(unittest.TestCase):
    """Test comprehensive logging for manual bookmark configuration."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.mock_glue_context = Mock()
        self.job_name = "test-job"
        
        # Create a mock structured logger to capture log calls
        self.mock_structured_logger = Mock(spec=StructuredLogger)
        
        # Sample manual bookmark configuration
        self.valid_config_json = json.dumps({
            "customers": {
                "table_name": "customers",
                "column_name": "updated_at"
            },
            "orders": {
                "table_name": "orders", 
                "column_name": "order_id"
            }
        })
        
        self.invalid_config_json = '{"invalid": "json"'  # Malformed JSON
    
    def test_manual_config_parsing_success_logging(self):
        """Test logging for successful manual configuration parsing."""
        with patch('src.glue_job.monitoring.logging.StructuredLogger') as mock_logger_class:
            mock_logger_class.return_value = self.mock_structured_logger
            
            # Create JobBookmarkManager with valid manual config
            manager = JobBookmarkManager(
                self.mock_glue_context,
                self.job_name,
                manual_bookmark_config=self.valid_config_json
            )
            
            # Verify parsing start was logged
            self.mock_structured_logger.log_manual_config_parsing_start.assert_called_once()
            
            # Verify parsing success was logged
            self.mock_structured_logger.log_manual_config_parsing_success.assert_called_once()
            
            # Check the arguments passed to success logging
            success_call = self.mock_structured_logger.log_manual_config_parsing_success.call_args
            self.assertEqual(success_call[0][0], 2)  # config_count
            self.assertEqual(set(success_call[0][1]), {"customers", "orders"})  # table_names
            self.assertIsInstance(success_call[0][2], float)  # parsing_duration_ms
    
    def test_manual_config_parsing_failure_logging(self):
        """Test logging for failed manual configuration parsing."""
        with patch('src.glue_job.monitoring.logging.StructuredLogger') as mock_logger_class:
            mock_logger_class.return_value = self.mock_structured_logger
            
            # Create JobBookmarkManager with invalid manual config
            manager = JobBookmarkManager(
                self.mock_glue_context,
                self.job_name,
                manual_bookmark_config=self.invalid_config_json
            )
            
            # Verify parsing start was logged
            self.mock_structured_logger.log_manual_config_parsing_start.assert_called_once()
            
            # Verify parsing failure was logged
            self.mock_structured_logger.log_manual_config_parsing_failure.assert_called_once()
            
            # Check that fallback error logging was also called
            self.mock_structured_logger.error.assert_called()
    
    def test_bookmark_strategy_resolution_logging(self):
        """Test logging for bookmark strategy resolution."""
        # Create manual configs
        manual_configs = {
            "customers": ManualBookmarkConfig("customers", "updated_at")
        }
        
        # Create resolver with mock structured logger
        resolver = BookmarkStrategyResolver(manual_configs, self.mock_structured_logger)
        
        # Mock JDBC connection and metadata
        mock_connection = Mock()
        mock_metadata = Mock()
        mock_result_set = Mock()
        
        mock_connection.getMetaData.return_value = mock_metadata
        mock_metadata.getColumns.return_value = mock_result_set
        
        # Mock successful metadata query
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
        
        # Test strategy resolution
        strategy, column_name, is_manual = resolver.resolve_strategy("customers", mock_connection)
        
        # Verify logging calls
        self.mock_structured_logger.log_bookmark_strategy_resolution_start.assert_called_once_with(
            "customers", True
        )
        
        self.mock_structured_logger.log_manual_config_validation_start.assert_called_once_with(
            "customers", "updated_at"
        )
        
        self.mock_structured_logger.log_jdbc_metadata_query_start.assert_called_once_with(
            "customers", "updated_at", "column_metadata"
        )
        
        self.mock_structured_logger.log_jdbc_metadata_query_success.assert_called_once()
        
        self.mock_structured_logger.log_manual_config_validation_success.assert_called_once()
        
        # Data type mapping success should be called (may be called multiple times due to internal calls)
        self.mock_structured_logger.log_data_type_mapping_success.assert_called()
        
        self.mock_structured_logger.log_bookmark_strategy_resolution_success.assert_called_once()
        
        # Verify the resolution results
        self.assertEqual(strategy, "timestamp")
        self.assertEqual(column_name, "updated_at")
        self.assertTrue(is_manual)
    
    def test_jdbc_metadata_query_failure_logging(self):
        """Test logging for failed JDBC metadata queries."""
        manual_configs = {
            "customers": ManualBookmarkConfig("customers", "nonexistent_column")
        }
        
        resolver = BookmarkStrategyResolver(manual_configs, self.mock_structured_logger)
        
        # Mock JDBC connection that returns no results
        mock_connection = Mock()
        mock_metadata = Mock()
        mock_result_set = Mock()
        
        mock_connection.getMetaData.return_value = mock_metadata
        mock_metadata.getColumns.return_value = mock_result_set
        mock_result_set.next.return_value = False  # Column not found
        
        # Test strategy resolution
        strategy, column_name, is_manual = resolver.resolve_strategy("customers", mock_connection)
        
        # Verify failure logging was called
        self.mock_structured_logger.log_jdbc_metadata_query_failure.assert_called_once()
        self.mock_structured_logger.log_manual_config_column_not_found.assert_called_once_with(
            "customers", "nonexistent_column"
        )
        self.mock_structured_logger.log_manual_config_validation_failure.assert_called_once()
        
        # Should fall back to automatic detection
        self.assertFalse(is_manual)
    
    def test_data_type_mapping_logging(self):
        """Test logging for data type to strategy mapping."""
        resolver = BookmarkStrategyResolver({}, self.mock_structured_logger)
        
        # Test direct mapping
        strategy = resolver._map_jdbc_type_to_strategy("TIMESTAMP")
        
        self.mock_structured_logger.log_data_type_mapping_start.assert_called_with(
            "TIMESTAMP", "unknown", "unknown"
        )
        self.mock_structured_logger.log_data_type_mapping_success.assert_called_with(
            "TIMESTAMP", "timestamp", "unknown", "unknown", "direct"
        )
        
        # Reset mock
        self.mock_structured_logger.reset_mock()
        
        # Test fallback mapping for unknown type
        strategy = resolver._map_jdbc_type_to_strategy("UNKNOWN_TYPE")
        
        self.mock_structured_logger.log_data_type_mapping_fallback.assert_called_with(
            "UNKNOWN_TYPE", "hash", "unknown", "unknown", "unknown_type"
        )
    
    def test_bookmark_detection_summary_logging(self):
        """Test logging for bookmark detection summary."""
        with patch('src.glue_job.monitoring.logging.StructuredLogger') as mock_logger_class:
            mock_logger_class.return_value = self.mock_structured_logger
            
            manager = JobBookmarkManager(
                self.mock_glue_context,
                self.job_name,
                manual_bookmark_config=self.valid_config_json
            )
            
            # Simulate some bookmark strategy resolutions
            manager.bookmark_detection_stats = {
                'total_tables': 3,
                'manual_count': 2,
                'automatic_count': 1,
                'failed_count': 0,
                'strategy_distribution': {'timestamp': 2, 'primary_key': 1, 'hash': 0}
            }
            
            # Call summary logging
            manager.log_bookmark_detection_summary()
            
            # Verify summary was logged
            self.mock_structured_logger.log_bookmark_detection_summary.assert_called_once_with(
                3, 2, 1, 0, {'timestamp': 2, 'primary_key': 1, 'hash': 0}
            )
    
    def test_invalid_manual_config_entry_logging(self):
        """Test logging for invalid manual configuration entries."""
        # Config with all invalid entries - this should trigger parsing failure
        invalid_config_all_bad = json.dumps({
            "invalid_table1": {
                "table_name": "",  # Invalid empty table name
                "column_name": "some_column"
            },
            "invalid_table2": {
                "table_name": "table2",
                "column_name": ""  # Invalid empty column name
            }
        })
        
        with patch('src.glue_job.monitoring.logging.StructuredLogger') as mock_logger_class:
            mock_logger_class.return_value = self.mock_structured_logger
            
            # This should not raise an exception but should log the parsing failure and continue
            manager = JobBookmarkManager(
                self.mock_glue_context,
                self.job_name,
                manual_bookmark_config=invalid_config_all_bad
            )
            
            # Verify that invalid config entry was logged (the actual method being called)
            self.mock_structured_logger.log_invalid_manual_config_entry.assert_called()
            
            # Verify manager was created with empty manual configs (fallback behavior)
            self.assertEqual(len(manager.manual_bookmark_configs), 0)
    
    def test_jdbc_metadata_cache_hit_logging(self):
        """Test logging for JDBC metadata cache hits."""
        resolver = BookmarkStrategyResolver({}, self.mock_structured_logger)
        
        # Mock connection
        mock_connection = Mock()
        mock_metadata = Mock()
        mock_result_set = Mock()
        
        mock_connection.getMetaData.return_value = mock_metadata
        mock_metadata.getColumns.return_value = mock_result_set
        mock_result_set.next.return_value = True
        mock_result_set.getString.side_effect = lambda col: {
            'COLUMN_NAME': 'test_column',
            'TYPE_NAME': 'INTEGER'
        }[col]
        mock_result_set.getInt.side_effect = lambda col: {
            'DATA_TYPE': 4,
            'COLUMN_SIZE': 10,
            'NULLABLE': 0
        }[col]
        
        # First call - should query database
        result1 = resolver._get_column_metadata(mock_connection, "test_table", "test_column")
        
        # Second call - should hit cache
        result2 = resolver._get_column_metadata(mock_connection, "test_table", "test_column")
        
        # Verify cache hit was logged on second call
        self.mock_structured_logger.log_jdbc_metadata_cache_hit.assert_called_once_with(
            "test_table", "test_column", "INTEGER"
        )
    
    def test_manual_config_table_override_logging(self):
        """Test logging when manual configuration overrides automatic detection."""
        with patch('src.glue_job.monitoring.logging.StructuredLogger') as mock_logger_class:
            mock_logger_class.return_value = self.mock_structured_logger
            
            manager = JobBookmarkManager(
                self.mock_glue_context,
                self.job_name,
                manual_bookmark_config=self.valid_config_json
            )
            
            # Mock connection for strategy resolution
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
            
            # Mock automatic detection methods for comparison
            with patch.object(manager.bookmark_strategy_resolver, '_get_automatic_strategy') as mock_auto:
                mock_auto.return_value = ('hash', None)  # Different from manual config
                
                # Call strategy resolution
                strategy, column = manager._get_bookmark_strategy_for_table("customers", mock_connection)
                
                # Verify override logging was called
                self.mock_structured_logger.log_manual_config_table_override.assert_called_once()
    
    def test_comprehensive_logging_integration(self):
        """Test comprehensive integration of all logging components."""
        with patch('src.glue_job.monitoring.logging.StructuredLogger') as mock_logger_class:
            mock_logger_class.return_value = self.mock_structured_logger
            
            # Create manager with mixed valid/invalid config to test various logging paths
            mixed_config = json.dumps({
                "valid_table": {
                    "table_name": "valid_table",
                    "column_name": "valid_column"
                }
            })
            
            manager = JobBookmarkManager(
                self.mock_glue_context,
                self.job_name,
                manual_bookmark_config=mixed_config
            )
            
            # Verify that structured logger was properly initialized and passed to resolver
            self.assertIsNotNone(manager.bookmark_strategy_resolver.structured_logger)
            
            # Verify initial parsing logging occurred
            self.mock_structured_logger.log_manual_config_parsing_start.assert_called_once()
            self.mock_structured_logger.log_manual_config_parsing_success.assert_called_once()
            
            # Verify bookmark detection stats were initialized
            self.assertIsInstance(manager.bookmark_detection_stats, dict)
            self.assertEqual(manager.bookmark_detection_stats['total_tables'], 0)
            self.assertEqual(manager.bookmark_detection_stats['manual_count'], 0)
            self.assertEqual(manager.bookmark_detection_stats['automatic_count'], 0)
            
            # Test summary logging method exists and is callable
            self.assertTrue(hasattr(manager, 'log_bookmark_detection_summary'))
            self.assertTrue(callable(manager.log_bookmark_detection_summary))


if __name__ == '__main__':
    unittest.main()