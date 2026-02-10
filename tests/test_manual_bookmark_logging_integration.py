"""
Integration test for Task 23: Comprehensive logging for manual bookmark configuration.

This test verifies that the logging functionality works correctly in a more realistic scenario.
"""

import unittest
from unittest.mock import Mock, patch
import json
import logging

from src.glue_job.storage.bookmark_manager import JobBookmarkManager
from src.glue_job.storage.manual_bookmark_config import BookmarkStrategyResolver, ManualBookmarkConfig
from src.glue_job.monitoring.logging import StructuredLogger


class TestTask23Integration(unittest.TestCase):
    """Integration test for comprehensive manual bookmark configuration logging."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.mock_glue_context = Mock()
        self.job_name = "integration-test-job"
        
        # Valid manual configuration
        self.manual_config_json = json.dumps({
            "customers": {
                "table_name": "customers",
                "column_name": "last_updated"
            },
            "orders": {
                "table_name": "orders",
                "column_name": "order_id"
            }
        })
    
    def test_end_to_end_logging_flow(self):
        """Test the complete logging flow from configuration parsing to strategy resolution."""
        # Create a real structured logger to capture actual log output
        logger = logging.getLogger("test_logger")
        logger.setLevel(logging.DEBUG)
        
        # Create a list to capture log messages
        log_messages = []
        
        class TestHandler(logging.Handler):
            def emit(self, record):
                log_messages.append(self.format(record))
        
        handler = TestHandler()
        logger.addHandler(handler)
        
        # Create structured logger
        structured_logger = StructuredLogger("integration-test", logger)
        
        # Patch the StructuredLogger creation to use our test logger
        with patch('src.glue_job.monitoring.logging.StructuredLogger') as mock_logger_class:
            mock_logger_class.return_value = structured_logger
            
            # Create JobBookmarkManager with manual configuration
            manager = JobBookmarkManager(
                self.mock_glue_context,
                self.job_name,
                manual_bookmark_config=self.manual_config_json
            )
            
            # Verify that manual configurations were parsed
            self.assertEqual(len(manager.manual_bookmark_configs), 2)
            self.assertIn("customers", manager.manual_bookmark_configs)
            self.assertIn("orders", manager.manual_bookmark_configs)
            
            # Verify that BookmarkStrategyResolver was initialized with structured logger
            self.assertIsNotNone(manager.bookmark_strategy_resolver.structured_logger)
            
            # Verify that bookmark detection stats were initialized
            self.assertIsInstance(manager.bookmark_detection_stats, dict)
            
            # Check that some log messages were generated
            self.assertGreater(len(log_messages), 0)
            
            # Look for specific log message patterns
            log_text = " ".join(log_messages)
            self.assertIn("Manual bookmark configuration", log_text)
            
        # Clean up
        logger.removeHandler(handler)
    
    def test_bookmark_strategy_resolver_logging(self):
        """Test that BookmarkStrategyResolver logs correctly during strategy resolution."""
        # Create manual configs
        manual_configs = {
            "test_table": ManualBookmarkConfig("test_table", "test_column")
        }
        
        # Create a test logger
        logger = logging.getLogger("resolver_test")
        logger.setLevel(logging.DEBUG)
        
        log_messages = []
        
        class TestHandler(logging.Handler):
            def emit(self, record):
                log_messages.append(self.format(record))
        
        handler = TestHandler()
        logger.addHandler(handler)
        
        structured_logger = StructuredLogger("resolver-test", logger)
        
        # Create resolver with structured logger
        resolver = BookmarkStrategyResolver(manual_configs, structured_logger)
        
        # Mock JDBC connection
        mock_connection = Mock()
        mock_metadata = Mock()
        mock_result_set = Mock()
        
        mock_connection.getMetaData.return_value = mock_metadata
        mock_metadata.getColumns.return_value = mock_result_set
        mock_result_set.next.return_value = True
        mock_result_set.getString.side_effect = lambda col: {
            'COLUMN_NAME': 'test_column',
            'TYPE_NAME': 'TIMESTAMP'
        }[col]
        mock_result_set.getInt.side_effect = lambda col: {
            'DATA_TYPE': 93,
            'COLUMN_SIZE': 23,
            'NULLABLE': 1
        }[col]
        
        # Test strategy resolution
        strategy, column_name, is_manual = resolver.resolve_strategy("test_table", mock_connection)
        
        # Verify results
        self.assertEqual(strategy, "timestamp")
        self.assertEqual(column_name, "test_column")
        self.assertTrue(is_manual)
        
        # Verify that log messages were generated
        self.assertGreater(len(log_messages), 0)
        
        # Check for specific logging patterns
        log_text = " ".join(log_messages)
        self.assertIn("bookmark", log_text.lower())
        
        # Clean up
        logger.removeHandler(handler)
    
    def test_data_type_mapping_logging(self):
        """Test that data type mapping generates appropriate log messages."""
        logger = logging.getLogger("mapping_test")
        logger.setLevel(logging.DEBUG)
        
        log_messages = []
        
        class TestHandler(logging.Handler):
            def emit(self, record):
                log_messages.append(self.format(record))
        
        handler = TestHandler()
        logger.addHandler(handler)
        
        structured_logger = StructuredLogger("mapping-test", logger)
        
        # Create resolver
        resolver = BookmarkStrategyResolver({}, structured_logger)
        
        # Test various data type mappings
        test_types = [
            ("TIMESTAMP", "timestamp"),
            ("INTEGER", "primary_key"),
            ("VARCHAR", "hash"),
            ("UNKNOWN_TYPE", "hash")  # Should trigger fallback logging
        ]
        
        for jdbc_type, expected_strategy in test_types:
            strategy = resolver._map_jdbc_type_to_strategy(jdbc_type)
            self.assertEqual(strategy, expected_strategy)
        
        # Verify that log messages were generated
        self.assertGreater(len(log_messages), 0)
        
        # Check for data type mapping messages
        log_text = " ".join(log_messages)
        self.assertIn("data type", log_text.lower())
        
        # Clean up
        logger.removeHandler(handler)
    
    def test_bookmark_detection_summary(self):
        """Test that bookmark detection summary logging works correctly."""
        logger = logging.getLogger("summary_test")
        logger.setLevel(logging.INFO)
        
        log_messages = []
        
        class TestHandler(logging.Handler):
            def emit(self, record):
                log_messages.append(self.format(record))
        
        handler = TestHandler()
        logger.addHandler(handler)
        
        structured_logger = StructuredLogger("summary-test", logger)
        
        # Patch the StructuredLogger creation
        with patch('src.glue_job.monitoring.logging.StructuredLogger') as mock_logger_class:
            mock_logger_class.return_value = structured_logger
            
            # Create manager
            manager = JobBookmarkManager(
                self.mock_glue_context,
                self.job_name,
                manual_bookmark_config=self.manual_config_json
            )
            
            # Simulate some bookmark detection statistics
            manager.bookmark_detection_stats = {
                'total_tables': 5,
                'manual_count': 2,
                'automatic_count': 3,
                'failed_count': 0,
                'strategy_distribution': {'timestamp': 3, 'primary_key': 1, 'hash': 1}
            }
            
            # Call summary logging
            manager.log_bookmark_detection_summary()
            
            # Verify that summary log was generated
            self.assertGreater(len(log_messages), 0)
            
            # Check for summary content
            log_text = " ".join(log_messages)
            self.assertIn("summary", log_text.lower())
        
        # Clean up
        logger.removeHandler(handler)


if __name__ == '__main__':
    unittest.main()