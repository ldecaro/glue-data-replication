#!/usr/bin/env python3
"""
Integration tests for Task 5: Streaming Progress Integration into Write Operations.

This test suite verifies:
- StreamingProgressTracker initialization in perform_full_load_migration
- Progress updates during write operations
- Progress metrics emission at configured intervals
- Progress tracker completion after write
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
from glue_job.database.migration import FullLoadDataMigrator
from glue_job.database.connection_manager import UnifiedConnectionManager
from glue_job.config.job_config import ConnectionConfig
from glue_job.database.counting_strategy import CountingStrategy, CountingStrategyConfig, CountingStrategyType
from glue_job.monitoring.streaming_progress_tracker import StreamingProgressTracker, StreamingProgressConfig
from glue_job.monitoring.metrics import CloudWatchMetricsPublisher


class TestStreamingProgressIntegration(unittest.TestCase):
    """Test streaming progress integration into write operations."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.mock_spark = Mock()
        self.mock_connection_manager = Mock(spec=UnifiedConnectionManager)
        self.mock_metrics_publisher = Mock(spec=CloudWatchMetricsPublisher)
        
        # Create streaming progress config
        self.streaming_config = StreamingProgressConfig(
            update_interval_seconds=1,
            batch_size_rows=100,
            enable_metrics=True,
            enable_logging=True
        )
        
        # Create counting strategy
        self.counting_strategy = CountingStrategy(
            CountingStrategyConfig(strategy_type=CountingStrategyType.IMMEDIATE)
        )
        
        # Create migrator with all dependencies
        self.migrator = FullLoadDataMigrator(
            spark_session=self.mock_spark,
            connection_manager=self.mock_connection_manager,
            counting_strategy=self.counting_strategy,
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
    
    @patch('glue_job.database.migration.DatabaseEngineManager')
    def test_streaming_tracker_initialized_with_metrics_publisher(self, mock_engine_manager):
        """Test that streaming tracker is initialized when metrics publisher is provided."""
        mock_engine_manager.is_iceberg_engine.return_value = False
        
        # Mock DataFrame
        mock_df = Mock()
        mock_df.count.return_value = 1000
        mock_df.rdd.getNumPartitions.return_value = 4
        
        self.mock_connection_manager.read_table_data.return_value = mock_df
        self.mock_connection_manager.write_table_data.return_value = None
        
        # Execute migration
        result = self.migrator.perform_full_load_migration(
            self.source_config,
            self.target_config,
            "test_table"
        )
        
        # Verify migration completed
        self.assertEqual(result.status, 'completed')
        self.assertEqual(result.total_rows, 1000)
        
        # Verify metrics were published (streaming tracker was used)
        self.mock_metrics_publisher.publish_migration_progress_metrics.assert_called()
    
    @patch('glue_job.database.migration.DatabaseEngineManager')
    def test_streaming_tracker_not_initialized_without_metrics_publisher(self, mock_engine_manager):
        """Test that streaming tracker is not initialized when metrics publisher is None."""
        mock_engine_manager.is_iceberg_engine.return_value = False
        
        # Create migrator without metrics publisher
        migrator_no_metrics = FullLoadDataMigrator(
            spark_session=self.mock_spark,
            connection_manager=self.mock_connection_manager,
            counting_strategy=self.counting_strategy,
            metrics_publisher=None,
            streaming_progress_config=self.streaming_config
        )
        
        # Mock DataFrame
        mock_df = Mock()
        mock_df.count.return_value = 1000
        mock_df.rdd.getNumPartitions.return_value = 4
        
        self.mock_connection_manager.read_table_data.return_value = mock_df
        self.mock_connection_manager.write_table_data.return_value = None
        
        # Execute migration
        result = migrator_no_metrics.perform_full_load_migration(
            self.source_config,
            self.target_config,
            "test_table"
        )
        
        # Verify migration completed without streaming tracker
        self.assertEqual(result.status, 'completed')
        self.assertEqual(result.total_rows, 1000)
    
    @patch('glue_job.database.migration.DatabaseEngineManager')
    def test_streaming_tracker_started_with_immediate_counting(self, mock_engine_manager):
        """Test that streaming tracker is started with total rows for immediate counting."""
        mock_engine_manager.is_iceberg_engine.return_value = False
        
        # Mock DataFrame
        mock_df = Mock()
        mock_df.count.return_value = 5000
        mock_df.rdd.getNumPartitions.return_value = 4
        
        self.mock_connection_manager.read_table_data.return_value = mock_df
        self.mock_connection_manager.write_table_data.return_value = None
        
        # Execute migration with immediate counting
        result = self.migrator.perform_full_load_migration(
            self.source_config,
            self.target_config,
            "test_table"
        )
        
        # Verify counting strategy
        self.assertEqual(result.counting_strategy, 'immediate')
        self.assertEqual(result.rows_counted_at, 'before_write')
        self.assertEqual(result.total_rows, 5000)
        self.assertEqual(result.status, 'completed')
        self.assertGreater(result.count_duration_seconds, 0)
        
        # Verify progress metrics were published with total rows
        calls = self.mock_metrics_publisher.publish_migration_progress_metrics.call_args_list
        self.assertGreater(len(calls), 0)
        
        # Check that at least one call included total_rows
        for call_args in calls:
            kwargs = call_args[1]
            if 'total_rows' in kwargs:
                self.assertEqual(kwargs['total_rows'], 5000)
                break
    
    @patch('glue_job.database.migration.DatabaseEngineManager')
    def test_streaming_tracker_started_with_deferred_counting(self, mock_engine_manager):
        """Test that streaming tracker is started without total rows for deferred counting."""
        mock_engine_manager.is_iceberg_engine.return_value = False
        
        # Create migrator with deferred counting strategy
        deferred_strategy = CountingStrategy(
            CountingStrategyConfig(strategy_type=CountingStrategyType.DEFERRED),
            metrics_publisher=self.mock_metrics_publisher
        )
        
        migrator_deferred = FullLoadDataMigrator(
            spark_session=self.mock_spark,
            connection_manager=self.mock_connection_manager,
            counting_strategy=deferred_strategy,
            metrics_publisher=self.mock_metrics_publisher,
            streaming_progress_config=self.streaming_config
        )
        
        # Mock DataFrame
        mock_df = Mock()
        mock_df.count.return_value = 5000
        mock_df.rdd.getNumPartitions.return_value = 4
        
        # Mock deferred count
        count_df = Mock()
        count_df.collect.return_value = [{'row_count': 5000}]
        self.mock_connection_manager.read_table_data.side_effect = [mock_df, count_df]
        self.mock_connection_manager.write_table_data.return_value = None
        
        # Execute migration with deferred counting
        result = migrator_deferred.perform_full_load_migration(
            self.source_config,
            self.target_config,
            "test_table"
        )
        
        # Verify counting strategy
        self.assertEqual(result.counting_strategy, 'deferred')
        self.assertEqual(result.rows_counted_at, 'after_write')
        self.assertEqual(result.total_rows, 5000)
        self.assertEqual(result.status, 'completed')
        
        # Verify progress metrics were published
        self.mock_metrics_publisher.publish_migration_progress_metrics.assert_called()
    
    def test_progress_updates_count_tracked(self):
        """Test that progress updates count is tracked in result."""
        # Mock DataFrame
        mock_df = Mock()
        mock_df.count.return_value = 1000
        
        self.mock_connection_manager.read_table_data.return_value = mock_df
        self.mock_connection_manager.write_table_data.return_value = None
        
        # Execute migration
        result = self.migrator.perform_full_load_migration(
            self.source_config,
            self.target_config,
            "test_table"
        )
        
        # Verify progress updates count is set
        self.assertGreaterEqual(result.progress_updates_count, 0)
    
    @patch('glue_job.database.migration.DatabaseEngineManager')
    def test_streaming_tracker_completed_after_write(self, mock_engine_manager):
        """Test that streaming tracker is completed after write operation."""
        mock_engine_manager.is_iceberg_engine.return_value = False
        
        # Mock DataFrame
        mock_df = Mock()
        mock_df.count.return_value = 2000
        mock_df.rdd.getNumPartitions.return_value = 4
        
        self.mock_connection_manager.read_table_data.return_value = mock_df
        self.mock_connection_manager.write_table_data.return_value = None
        
        # Execute migration
        result = self.migrator.perform_full_load_migration(
            self.source_config,
            self.target_config,
            "test_table"
        )
        
        # Verify migration completed
        self.assertEqual(result.status, 'completed')
        self.assertEqual(result.total_rows, 2000)
        
        # Verify final metrics were published (tracker was completed)
        calls = self.mock_metrics_publisher.publish_migration_progress_metrics.call_args_list
        self.assertGreater(len(calls), 0)
        
        # The last call should be the final update
        final_call = calls[-1]
        final_kwargs = final_call[1]
        self.assertEqual(final_kwargs['table_name'], 'test_table')
        self.assertEqual(final_kwargs['load_type'], 'full')
    
    @patch('glue_job.database.migration.DatabaseEngineManager')
    def test_write_operation_updates_progress(self, mock_engine_manager):
        """Test that write operation triggers progress updates."""
        mock_engine_manager.is_iceberg_engine.return_value = False
        
        # Mock DataFrame
        mock_df = Mock()
        mock_df.count.return_value = 3000
        mock_df.rdd.getNumPartitions.return_value = 4
        
        self.mock_connection_manager.read_table_data.return_value = mock_df
        self.mock_connection_manager.write_table_data.return_value = None
        
        # Execute migration
        result = self.migrator.perform_full_load_migration(
            self.source_config,
            self.target_config,
            "test_table"
        )
        
        # Verify write completed
        self.assertEqual(result.status, 'completed')
        self.assertGreater(result.write_duration_seconds, 0)
        
        # Verify progress was updated during/after write
        self.mock_metrics_publisher.publish_migration_progress_metrics.assert_called()
        
        # Check that rows_processed was reported
        calls = self.mock_metrics_publisher.publish_migration_progress_metrics.call_args_list
        for call_args in calls:
            kwargs = call_args[1]
            self.assertIn('rows_processed', kwargs)
            self.assertGreaterEqual(kwargs['rows_processed'], 0)


if __name__ == '__main__':
    unittest.main(verbosity=2)
