#!/usr/bin/env python3
"""
Integration tests for Task 20: Streaming Progress Integration into IncrementalDataMigrator.

This test suite verifies:
- StreamingProgressTracker initialization in perform_incremental_load_migration
- load_type set to 'incremental' for progress tracker
- Progress updates during incremental write operations
- Progress metrics emission at configured intervals
- Progress tracker completion after incremental write
"""

import unittest
from unittest.mock import Mock, MagicMock, patch, call
import sys
import os
import time

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
from glue_job.monitoring.streaming_progress_tracker import StreamingProgressTracker, StreamingProgressConfig
from glue_job.monitoring.metrics import CloudWatchMetricsPublisher


class TestIncrementalStreamingProgressIntegration(unittest.TestCase):
    """Test streaming progress integration into incremental load operations."""
    
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
        self.mock_bookmark_state.last_processed_value = '2023-01-01'
        self.mock_bookmark_manager.get_bookmark_state.return_value = self.mock_bookmark_state
    
    @patch('glue_job.database.migration.DatabaseEngineManager')
    def test_streaming_tracker_initialized_with_incremental_load_type(self, mock_engine_manager):
        """Test that streaming tracker is initialized with load_type='incremental'."""
        mock_engine_manager.is_iceberg_engine.return_value = False
        
        # Mock DataFrame
        mock_df = Mock()
        mock_df.count.return_value = 500
        mock_df.rdd.getNumPartitions.return_value = 4
        
        # Create a mock row that supports dictionary-like access
        mock_row = Mock()
        mock_row.__getitem__ = Mock(return_value='2023-01-02')
        mock_df.agg.return_value.collect.return_value = [mock_row]
        
        self.mock_connection_manager.read_table_data.return_value = mock_df
        self.mock_connection_manager.write_table_data.return_value = None
        
        # Execute incremental migration
        result = self.migrator.perform_incremental_load_migration(
            self.source_config,
            self.target_config,
            "test_table"
        )
        
        # Verify migration completed
        self.assertEqual(result.status, 'completed')
        self.assertEqual(result.delta_rows, 500)
        
        # Verify metrics were published with load_type='incremental'
        self.mock_metrics_publisher.publish_migration_progress_metrics.assert_called()
        
        # Check that load_type is 'incremental' in all calls
        calls = self.mock_metrics_publisher.publish_migration_progress_metrics.call_args_list
        for call_args in calls:
            kwargs = call_args[1]
            self.assertEqual(kwargs['load_type'], 'incremental')
    
    @patch('glue_job.database.migration.DatabaseEngineManager')
    def test_streaming_tracker_not_initialized_without_metrics_publisher(self, mock_engine_manager):
        """Test that streaming tracker is not initialized when metrics publisher is None."""
        mock_engine_manager.is_iceberg_engine.return_value = False
        
        # Create migrator without metrics publisher
        migrator_no_metrics = IncrementalDataMigrator(
            spark_session=self.mock_spark,
            connection_manager=self.mock_connection_manager,
            bookmark_manager=self.mock_bookmark_manager,
            metrics_publisher=None,
            streaming_progress_config=self.streaming_config
        )
        
        # Mock DataFrame
        mock_df = Mock()
        mock_df.count.return_value = 500
        mock_df.rdd.getNumPartitions.return_value = 4
        
        # Create a mock row that supports dictionary-like access
        mock_row = Mock()
        mock_row.__getitem__ = Mock(return_value='2023-01-02')
        mock_df.agg.return_value.collect.return_value = [mock_row]
        
        self.mock_connection_manager.read_table_data.return_value = mock_df
        self.mock_connection_manager.write_table_data.return_value = None
        
        # Execute incremental migration
        result = migrator_no_metrics.perform_incremental_load_migration(
            self.source_config,
            self.target_config,
            "test_table"
        )
        
        # Verify migration completed without streaming tracker
        self.assertEqual(result.status, 'completed')
        self.assertEqual(result.delta_rows, 500)
    
    @patch('glue_job.database.migration.DatabaseEngineManager')
    def test_streaming_tracker_started_with_delta_rows(self, mock_engine_manager):
        """Test that streaming tracker is started with total rows set to delta rows."""
        mock_engine_manager.is_iceberg_engine.return_value = False
        
        # Mock DataFrame
        mock_df = Mock()
        mock_df.count.return_value = 1000
        mock_df.rdd.getNumPartitions.return_value = 4
        
        # Create a mock row that supports dictionary-like access
        mock_row = Mock()
        mock_row.__getitem__ = Mock(return_value='2023-01-02')
        mock_df.agg.return_value.collect.return_value = [mock_row]
        
        self.mock_connection_manager.read_table_data.return_value = mock_df
        self.mock_connection_manager.write_table_data.return_value = None
        
        # Execute incremental migration
        result = self.migrator.perform_incremental_load_migration(
            self.source_config,
            self.target_config,
            "test_table"
        )
        
        # Verify delta rows
        self.assertEqual(result.delta_rows, 1000)
        self.assertEqual(result.processed_rows, 1000)
        self.assertEqual(result.status, 'completed')
        
        # Verify progress metrics were published with total rows
        calls = self.mock_metrics_publisher.publish_migration_progress_metrics.call_args_list
        self.assertGreater(len(calls), 0)
        
        # Check that at least one call included total_rows matching delta_rows
        for call_args in calls:
            kwargs = call_args[1]
            if 'total_rows' in kwargs:
                self.assertEqual(kwargs['total_rows'], 1000)
                break
    
    @patch('glue_job.database.migration.DatabaseEngineManager')
    def test_streaming_tracker_completed_after_incremental_write(self, mock_engine_manager):
        """Test that streaming tracker is completed after incremental write operation."""
        mock_engine_manager.is_iceberg_engine.return_value = False
        
        # Mock DataFrame
        mock_df = Mock()
        mock_df.count.return_value = 750
        mock_df.rdd.getNumPartitions.return_value = 4
        
        # Create a mock row that supports dictionary-like access
        mock_row = Mock()
        mock_row.__getitem__ = Mock(return_value='2023-01-02')
        mock_df.agg.return_value.collect.return_value = [mock_row]
        
        self.mock_connection_manager.read_table_data.return_value = mock_df
        self.mock_connection_manager.write_table_data.return_value = None
        
        # Execute incremental migration
        result = self.migrator.perform_incremental_load_migration(
            self.source_config,
            self.target_config,
            "test_table"
        )
        
        # Verify migration completed
        self.assertEqual(result.status, 'completed')
        self.assertEqual(result.delta_rows, 750)
        
        # Verify final metrics were published (tracker was completed)
        calls = self.mock_metrics_publisher.publish_migration_progress_metrics.call_args_list
        self.assertGreater(len(calls), 0)
        
        # The last call should be the final update
        final_call = calls[-1]
        final_kwargs = final_call[1]
        self.assertEqual(final_kwargs['table_name'], 'test_table')
        self.assertEqual(final_kwargs['load_type'], 'incremental')
    
    @patch('glue_job.database.migration.DatabaseEngineManager')
    def test_incremental_write_operation_updates_progress(self, mock_engine_manager):
        """Test that incremental write operation triggers progress updates."""
        mock_engine_manager.is_iceberg_engine.return_value = False
        
        # Mock DataFrame
        mock_df = Mock()
        mock_df.count.return_value = 2000
        mock_df.rdd.getNumPartitions.return_value = 4
        
        # Create a mock row that supports dictionary-like access
        mock_row = Mock()
        mock_row.__getitem__ = Mock(return_value='2023-01-02')
        mock_df.agg.return_value.collect.return_value = [mock_row]
        
        self.mock_connection_manager.read_table_data.return_value = mock_df
        self.mock_connection_manager.write_table_data.return_value = None
        
        # Execute incremental migration
        result = self.migrator.perform_incremental_load_migration(
            self.source_config,
            self.target_config,
            "test_table"
        )
        
        # Verify write completed
        self.assertEqual(result.status, 'completed')
        
        # Verify progress was updated during/after write
        self.mock_metrics_publisher.publish_migration_progress_metrics.assert_called()
        
        # Check that rows_processed was reported
        calls = self.mock_metrics_publisher.publish_migration_progress_metrics.call_args_list
        for call_args in calls:
            kwargs = call_args[1]
            self.assertIn('rows_processed', kwargs)
            self.assertGreaterEqual(kwargs['rows_processed'], 0)
    
    def test_no_delta_rows_skips_write_and_tracking(self):
        """Test that when delta_rows is 0, write and tracking are skipped."""
        # Mock DataFrame with no rows
        mock_df = Mock()
        mock_df.count.return_value = 0
        
        self.mock_connection_manager.read_table_data.return_value = mock_df
        
        # Execute incremental migration
        result = self.migrator.perform_incremental_load_migration(
            self.source_config,
            self.target_config,
            "test_table"
        )
        
        # Verify migration completed with no rows
        self.assertEqual(result.status, 'completed')
        self.assertEqual(result.delta_rows, 0)
        self.assertEqual(result.processed_rows, 0)
        
        # Verify write was not called
        self.mock_connection_manager.write_table_data.assert_not_called()
        
        # Verify bookmark was not updated
        self.mock_bookmark_manager.update_bookmark_state.assert_not_called()
    
    @patch('glue_job.database.migration.DatabaseEngineManager')
    def test_progress_tracking_continues_on_tracker_error(self, mock_engine_manager):
        """Test that migration continues even if progress tracking fails."""
        mock_engine_manager.is_iceberg_engine.return_value = False
        
        # Mock DataFrame
        mock_df = Mock()
        mock_df.count.return_value = 500
        mock_df.rdd.getNumPartitions.return_value = 4
        
        # Create a mock row that supports dictionary-like access
        mock_row = Mock()
        mock_row.__getitem__ = Mock(return_value='2023-01-02')
        mock_df.agg.return_value.collect.return_value = [mock_row]
        
        self.mock_connection_manager.read_table_data.return_value = mock_df
        self.mock_connection_manager.write_table_data.return_value = None
        
        # Make metrics publisher raise an error
        self.mock_metrics_publisher.publish_migration_progress_metrics.side_effect = Exception("Metrics error")
        
        # Execute incremental migration - should not fail
        result = self.migrator.perform_incremental_load_migration(
            self.source_config,
            self.target_config,
            "test_table"
        )
        
        # Verify migration still completed successfully
        self.assertEqual(result.status, 'completed')
        self.assertEqual(result.delta_rows, 500)
    
    def test_iceberg_incremental_load_with_streaming_progress(self):
        """Test incremental load with Iceberg engine and streaming progress."""
        # Update configs to use Iceberg
        self.source_config.engine_type = "iceberg"
        self.target_config.engine_type = "iceberg"
        
        # Create a mock row that supports dictionary-like access for max_value
        mock_row = Mock()
        mock_row.__getitem__ = Mock(return_value='2023-01-02')
        
        # Mock the filtered DataFrame (result of filter operation)
        mock_filtered_df = Mock()
        mock_filtered_df.count.return_value = 1500
        mock_filtered_df.rdd.getNumPartitions.return_value = 4  # Add partition mock
        
        # Create a mock for agg that returns the max value
        mock_agg_result = Mock()
        mock_agg_result.collect.return_value = [mock_row]
        mock_filtered_df.agg.return_value = mock_agg_result
        
        # Patch the col function to return a mock that supports comparison operators
        with patch('glue_job.database.migration.col') as mock_col:
            # Make col return a mock that supports > operator
            mock_column = Mock()
            mock_column.__gt__ = Mock(return_value=Mock())  # Return a mock condition
            mock_col.return_value = mock_column
            
            # Mock the full DataFrame - filter should accept any argument and return filtered df
            mock_full_df = Mock()
            mock_full_df.filter = Mock(return_value=mock_filtered_df)
            
            self.mock_connection_manager.read_table.return_value = mock_full_df
            self.mock_connection_manager.write_table.return_value = None
            
            # Execute incremental migration
            result = self.migrator.perform_incremental_load_migration(
                self.source_config,
                self.target_config,
                "test_table"
            )
            
            # Verify migration completed
            self.assertEqual(result.status, 'completed')
            self.assertEqual(result.delta_rows, 1500)
            
            # Verify filter was called (incremental filtering happened)
            mock_full_df.filter.assert_called_once()
            
            # Verify metrics were published with load_type='incremental'
            calls = self.mock_metrics_publisher.publish_migration_progress_metrics.call_args_list
            self.assertGreater(len(calls), 0)
            
            for call_args in calls:
                kwargs = call_args[1]
                self.assertEqual(kwargs['load_type'], 'incremental')


if __name__ == '__main__':
    unittest.main(verbosity=2)
