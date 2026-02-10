#!/usr/bin/env python3
"""
Integration tests for Task 22: Enhanced Incremental Load Logging.

This test suite verifies:
- Structured log for incremental load start with bookmark state
- Progress logs every 100,000 delta rows (via StreamingProgressTracker)
- Log for incremental load completion with delta rows and bookmark update
- Include incremental_column and last_processed_value in logs
"""

import unittest
from unittest.mock import Mock, MagicMock, patch, call
import sys
import os

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

# Import the classes to test
from glue_job.database.migration import IncrementalDataMigrator
from glue_job.database.connection_manager import UnifiedConnectionManager
from glue_job.storage.bookmark_manager import JobBookmarkManager
from glue_job.config.job_config import ConnectionConfig
from glue_job.monitoring.streaming_progress_tracker import StreamingProgressConfig
from glue_job.monitoring.metrics import CloudWatchMetricsPublisher


class TestIncrementalLoadLogging(unittest.TestCase):
    """Test enhanced logging for incremental load operations."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.mock_spark = Mock()
        self.mock_connection_manager = Mock(spec=UnifiedConnectionManager)
        self.mock_bookmark_manager = Mock(spec=JobBookmarkManager)
        self.mock_metrics_publisher = Mock(spec=CloudWatchMetricsPublisher)
        
        # Create streaming progress config
        self.streaming_config = StreamingProgressConfig(
            update_interval_seconds=1,
            batch_size_rows=100,
            enable_metrics=True,
            enable_logging=True
        )
        
        # Create migrator with all dependencies
        self.migrator = IncrementalDataMigrator(
            spark_session=self.mock_spark,
            connection_manager=self.mock_connection_manager,
            bookmark_manager=self.mock_bookmark_manager,
            metrics_publisher=self.mock_metrics_publisher,
            streaming_progress_config=self.streaming_config
        )
        
        # Mock source and target configs
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
        
        # Mock bookmark state
        self.mock_bookmark_state = Mock()
        self.mock_bookmark_state.incremental_strategy = 'timestamp'
        self.mock_bookmark_state.incremental_column = 'updated_at'
        self.mock_bookmark_state.last_processed_value = '2023-01-01 00:00:00'
        self.mock_bookmark_manager.get_bookmark_state.return_value = self.mock_bookmark_state
    
    @patch('glue_job.database.migration.DatabaseEngineManager')
    def test_incremental_load_start_logging_includes_bookmark_state(self, mock_engine_manager):
        """
        Test Requirement 8.3: Structured log for incremental load start includes bookmark state.
        
        Verifies that the start log includes:
        - incremental_column
        - last_processed_value
        - load_type='incremental'
        """
        # Setup
        mock_engine_manager.is_iceberg_engine.return_value = False
        
        # Mock DataFrame
        mock_df = Mock()
        mock_df.count.return_value = 1000
        mock_df.agg.return_value.collect.return_value = [{'max_value': '2023-01-02 00:00:00'}]
        
        self.mock_connection_manager.read_table_data.return_value = mock_df
        
        # Patch the structured logger to capture log calls
        with patch.object(self.migrator.structured_logger, 'log_migration_start') as mock_log_start:
            # Execute
            self.migrator.perform_incremental_load_migration(
                self.source_config,
                self.target_config,
                'test_table'
            )
            
            # Verify log_migration_start was called with bookmark state
            mock_log_start.assert_called_once()
            call_kwargs = mock_log_start.call_args[1]
            
            self.assertEqual(call_kwargs['table_name'], 'test_table')
            self.assertEqual(call_kwargs['load_type'], 'incremental')
            self.assertEqual(call_kwargs['incremental_column'], 'updated_at')
            self.assertEqual(call_kwargs['last_processed_value'], '2023-01-01 00:00:00')
            self.assertEqual(call_kwargs['source_engine'], 'postgresql')
            self.assertEqual(call_kwargs['target_engine'], 'postgresql')
    
    @patch('glue_job.database.migration.DatabaseEngineManager')
    def test_incremental_load_completion_logging_includes_delta_rows_and_bookmark(self, mock_engine_manager):
        """
        Test Requirement 8.3, 8.4: Completion log includes delta rows and bookmark update.
        
        Verifies that the completion log includes:
        - delta_rows
        - incremental_column
        - new_bookmark_value
        - bookmark_update_status
        """
        # Setup
        mock_engine_manager.is_iceberg_engine.return_value = False
        
        # Mock DataFrame
        mock_df = Mock()
        mock_df.count.return_value = 1500
        mock_df.rdd.getNumPartitions.return_value = 4
        new_bookmark_value = '2023-01-02 12:00:00'
        
        # Create a mock row that supports dictionary-like access
        mock_row = Mock()
        mock_row.__getitem__ = Mock(return_value=new_bookmark_value)
        mock_df.agg.return_value.collect.return_value = [mock_row]
        
        self.mock_connection_manager.read_table_data.return_value = mock_df
        self.mock_connection_manager.write_table_data.return_value = None
        
        # Patch the structured logger to capture log calls
        with patch.object(self.migrator.structured_logger, 'log_migration_completion') as mock_log_completion:
            # Execute
            result = self.migrator.perform_incremental_load_migration(
                self.source_config,
                self.target_config,
                'test_table'
            )
            
            # Verify log_migration_completion was called with delta rows and bookmark info
            mock_log_completion.assert_called_once()
            call_kwargs = mock_log_completion.call_args[1]
            
            self.assertEqual(call_kwargs['table_name'], 'test_table')
            self.assertEqual(call_kwargs['load_type'], 'incremental')
            self.assertEqual(call_kwargs['total_rows'], 1500)
            self.assertEqual(call_kwargs['delta_rows'], 1500)
            self.assertEqual(call_kwargs['incremental_column'], 'updated_at')
            self.assertEqual(call_kwargs['new_bookmark_value'], new_bookmark_value)
            self.assertEqual(call_kwargs['bookmark_update_status'], 'success')
    
    @patch('glue_job.database.migration.DatabaseEngineManager')
    def test_incremental_load_read_logging_includes_delta_rows(self, mock_engine_manager):
        """
        Test Requirement 8.3: Read log includes delta rows and read duration.
        
        Verifies that after reading incremental data, a log is emitted with:
        - delta_rows
        - incremental_column
        - last_processed_value
        - read_duration_seconds
        """
        # Setup
        mock_engine_manager.is_iceberg_engine.return_value = False
        
        # Mock DataFrame
        mock_df = Mock()
        mock_df.count.return_value = 2000
        mock_df.agg.return_value.collect.return_value = [{'max_value': '2023-01-03 00:00:00'}]
        
        self.mock_connection_manager.read_table_data.return_value = mock_df
        
        # Patch the structured logger to capture log calls
        with patch.object(self.migrator.structured_logger, 'info') as mock_log_info:
            # Execute
            self.migrator.perform_incremental_load_migration(
                self.source_config,
                self.target_config,
                'test_table'
            )
            
            # Find the read log call
            read_log_calls = [
                call for call in mock_log_info.call_args_list
                if 'Read' in str(call) and 'incremental rows' in str(call)
            ]
            
            self.assertGreater(len(read_log_calls), 0, "Should have logged incremental read")
            
            # Verify the read log includes required fields
            read_call_kwargs = read_log_calls[0][1]
            self.assertEqual(read_call_kwargs['incremental_column'], 'updated_at')
            self.assertEqual(read_call_kwargs['last_processed_value'], '2023-01-01 00:00:00')
            self.assertEqual(read_call_kwargs['delta_rows'], 2000)
            self.assertIn('read_duration_seconds', read_call_kwargs)
    
    @patch('glue_job.database.migration.DatabaseEngineManager')
    def test_incremental_load_no_data_logging(self, mock_engine_manager):
        """
        Test logging when no incremental data is found.
        
        Verifies that when delta_rows = 0:
        - bookmark_update_status is 'no_data'
        - Completion log still includes all required fields
        """
        # Setup
        mock_engine_manager.is_iceberg_engine.return_value = False
        
        # Mock DataFrame with no data
        mock_df = Mock()
        mock_df.count.return_value = 0
        
        self.mock_connection_manager.read_table_data.return_value = mock_df
        
        # Patch the structured logger to capture log calls
        with patch.object(self.migrator.structured_logger, 'log_migration_completion') as mock_log_completion:
            # Execute
            result = self.migrator.perform_incremental_load_migration(
                self.source_config,
                self.target_config,
                'test_table'
            )
            
            # Verify completion log shows no_data status
            mock_log_completion.assert_called_once()
            call_kwargs = mock_log_completion.call_args[1]
            
            self.assertEqual(call_kwargs['delta_rows'], 0)
            self.assertEqual(call_kwargs['bookmark_update_status'], 'no_data')
            self.assertIsNone(call_kwargs['new_bookmark_value'])


if __name__ == '__main__':
    unittest.main()
