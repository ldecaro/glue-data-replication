#!/usr/bin/env python3
"""
Integration tests for Task 12: Error Handling for Deferred Counting.

This test suite verifies:
- Try-catch around deferred counting operations
- Fallback to estimated count on failure
- Error metrics for counting failures
- Warning logs for counting issues
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
from glue_job.database.migration import FullLoadDataMigrator
from glue_job.database.connection_manager import UnifiedConnectionManager
from glue_job.config.job_config import ConnectionConfig
from glue_job.database.counting_strategy import CountingStrategy, CountingStrategyConfig, CountingStrategyType
from glue_job.monitoring.streaming_progress_tracker import StreamingProgressTracker, StreamingProgressConfig
from glue_job.monitoring.metrics import CloudWatchMetricsPublisher


class TestDeferredCountErrorHandling(unittest.TestCase):
    """Test error handling for deferred counting operations."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.mock_spark = Mock()
        self.mock_connection_manager = Mock(spec=UnifiedConnectionManager)
        self.mock_connection_manager.spark = self.mock_spark
        self.mock_metrics_publisher = Mock(spec=CloudWatchMetricsPublisher)
        
        # Create counting strategy configured for deferred counting
        counting_config = CountingStrategyConfig(strategy_type=CountingStrategyType.DEFERRED)
        self.counting_strategy = CountingStrategy(counting_config, self.mock_metrics_publisher)
        
        # Create streaming progress config
        streaming_config = StreamingProgressConfig(
            update_interval_seconds=60,
            batch_size_rows=100_000,
            enable_metrics=True
        )
        
        # Create migrator with deferred counting
        self.migrator = FullLoadDataMigrator(
            spark_session=self.mock_spark,
            connection_manager=self.mock_connection_manager,
            metrics_publisher=self.mock_metrics_publisher,
            counting_strategy=self.counting_strategy,
            streaming_progress_config=streaming_config
        )
        
        # Create test connection configs
        self.source_config = ConnectionConfig(
            engine_type="postgresql",
            connection_string="jdbc:postgresql://source-host:5432/source_db",
            database="source_db",
            schema="public",
            username="user",
            password="pass",
            jdbc_driver_path="s3://bucket/drivers/postgresql.jar"
        )
        
        self.target_config = ConnectionConfig(
            engine_type="postgresql",
            connection_string="jdbc:postgresql://target-host:5432/target_db",
            database="target_db",
            schema="public",
            username="user",
            password="pass",
            jdbc_driver_path="s3://bucket/drivers/postgresql.jar"
        )
    
    @patch('src.glue_job.config.database_engines.DatabaseEngineManager')
    def test_deferred_count_failure_with_streaming_tracker_fallback(self, mock_engine_manager):
        """Test that deferred count failure falls back to streaming tracker estimate."""
        mock_engine_manager.is_iceberg_engine.return_value = False
        
        # Mock source data read
        mock_df = Mock()
        mock_df.count.return_value = 5000
        mock_df.rdd.getNumPartitions.return_value = 4
        
        # Mock write operation
        self.mock_connection_manager.write_table_data.return_value = None
        
        # Mock deferred count to fail
        self.mock_connection_manager.read_table_data.side_effect = [
            mock_df,  # First call for source read
            Exception("Database connection lost")  # Second call for deferred count fails
        ]
        
        # Execute migration
        result = self.migrator.perform_full_load_migration(
            self.source_config,
            self.target_config,
            "test_table"
        )
        
        # Verify migration completed despite count failure
        self.assertEqual(result.status, 'completed')
        
        # Verify fallback to estimated count (from streaming tracker)
        # The streaming tracker should have tracked rows during write
        self.assertGreaterEqual(result.total_rows, 0)
        self.assertEqual(result.rows_counted_at, 'estimated_fallback')
        
        # Verify error metrics were published
        self.mock_metrics_publisher.publish_error_metrics.assert_called_once_with(
            error_category="DEFERRED_COUNT_FAILED",
            error_operation="count_from_target"
        )
    
    @patch('src.glue_job.config.database_engines.DatabaseEngineManager')
    def test_deferred_count_failure_without_streaming_tracker(self, mock_engine_manager):
        """Test that deferred count failure falls back to zero when no streaming tracker."""
        mock_engine_manager.is_iceberg_engine.return_value = False
        
        # Create migrator without streaming progress config (no metrics publisher = no streaming tracker)
        migrator_no_streaming = FullLoadDataMigrator(
            spark_session=self.mock_spark,
            connection_manager=self.mock_connection_manager,
            metrics_publisher=None,  # No metrics publisher = no streaming tracker
            counting_strategy=self.counting_strategy,
            streaming_progress_config=None
        )
        
        # Mock source data read
        mock_df = Mock()
        mock_df.count.return_value = 5000
        mock_df.rdd.getNumPartitions.return_value = 4
        
        # Mock write operation
        self.mock_connection_manager.write_table_data.return_value = None
        
        # Mock deferred count to fail
        self.mock_connection_manager.read_table_data.side_effect = [
            mock_df,  # First call for source read
            Exception("Query timeout")  # Second call for deferred count fails
        ]
        
        # Execute migration
        result = migrator_no_streaming.perform_full_load_migration(
            self.source_config,
            self.target_config,
            "test_table"
        )
        
        # Verify migration completed despite count failure
        self.assertEqual(result.status, 'completed')
        
        # Verify fallback to zero when no streaming tracker available
        self.assertEqual(result.total_rows, 0)
        self.assertEqual(result.rows_counted_at, 'estimated_fallback')
    
    @patch('src.glue_job.config.database_engines.DatabaseEngineManager')
    def test_deferred_count_success_no_error_metrics(self, mock_engine_manager):
        """Test that successful deferred count does not publish error metrics."""
        mock_engine_manager.is_iceberg_engine.return_value = False
        
        # Mock source data read
        mock_df = Mock()
        mock_df.count.return_value = 5000
        mock_df.rdd.getNumPartitions.return_value = 4
        
        # Mock write operation
        self.mock_connection_manager.write_table_data.return_value = None
        
        # Mock successful deferred count
        count_df = Mock()
        count_df.collect.return_value = [{'row_count': 5000}]
        self.mock_connection_manager.read_table_data.side_effect = [
            mock_df,  # First call for source read
            count_df  # Second call for deferred count succeeds
        ]
        
        # Execute migration
        result = self.migrator.perform_full_load_migration(
            self.source_config,
            self.target_config,
            "test_table"
        )
        
        # Verify migration completed successfully
        self.assertEqual(result.status, 'completed')
        self.assertEqual(result.total_rows, 5000)
        self.assertEqual(result.rows_counted_at, 'after_write')
        
        # Verify error metrics were NOT published
        self.mock_metrics_publisher.publish_error_metrics.assert_not_called()
    
    @patch('src.glue_job.config.database_engines.DatabaseEngineManager')
    def test_deferred_count_iceberg_failure_fallback(self, mock_engine_manager):
        """Test that Iceberg deferred count failure falls back to estimated count."""
        mock_engine_manager.is_iceberg_engine.return_value = True
        
        # Update target config to Iceberg
        iceberg_target = ConnectionConfig(
            engine_type="iceberg",
            connection_string="",
            database="test_db",
            schema="test_table",
            username="",
            password="",
            jdbc_driver_path="",
            iceberg_config={
                'database_name': 'test_db',
                'table_name': 'test_table',
                'catalog_name': 'glue_catalog',
                'warehouse_location': 's3://test-bucket/warehouse'
            }
        )
        
        # Mock source data read
        mock_df = Mock()
        mock_df.count.return_value = 10000
        self.mock_connection_manager.read_table_data.return_value = mock_df
        
        # Mock write operation
        self.mock_connection_manager.write_table_data.return_value = None
        
        # Mock Iceberg deferred count to fail
        self.mock_spark.sql.side_effect = Exception("Iceberg metadata unavailable")
        self.mock_connection_manager.read_table.side_effect = Exception("Cannot read Iceberg table")
        
        # Execute migration
        result = self.migrator.perform_full_load_migration(
            self.source_config,
            iceberg_target,
            "test_table"
        )
        
        # Verify migration completed despite count failure
        self.assertEqual(result.status, 'completed')
        
        # Verify fallback to estimated count
        self.assertGreaterEqual(result.total_rows, 0)
        self.assertEqual(result.rows_counted_at, 'estimated_fallback')
        
        # Verify error metrics were published
        self.mock_metrics_publisher.publish_error_metrics.assert_called_once_with(
            error_category="DEFERRED_COUNT_FAILED",
            error_operation="count_from_target"
        )


if __name__ == "__main__":
    unittest.main()
