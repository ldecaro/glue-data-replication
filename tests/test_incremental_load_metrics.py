#!/usr/bin/env python3
"""
Integration tests for Task 21: Incremental Load Metrics to CloudWatch.

This test suite verifies:
- delta_rows metric published for incremental loads
- bookmark_update_status metric published
- incremental_column dimension added to metrics
- load_type='incremental' dimension included in all metrics
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
from glue_job.monitoring.metrics import CloudWatchMetricsPublisher
from glue_job.database.migration import IncrementalDataMigrator
from glue_job.database.connection_manager import UnifiedConnectionManager
from glue_job.storage.bookmark_manager import JobBookmarkManager
from glue_job.config.job_config import ConnectionConfig
from glue_job.monitoring.streaming_progress_tracker import StreamingProgressConfig


class TestIncrementalLoadMetrics(unittest.TestCase):
    """Test incremental load metrics publishing to CloudWatch."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.mock_cloudwatch = Mock()
        self.metrics_publisher = CloudWatchMetricsPublisher(
            job_name="test-job",
            namespace="AWS/Glue/DataReplication"
        )
        self.metrics_publisher.cloudwatch = self.mock_cloudwatch
    
    def test_publish_incremental_load_metrics_with_all_dimensions(self):
        """Test that incremental load metrics include all required dimensions."""
        # Publish incremental load metrics
        self.metrics_publisher.publish_incremental_load_metrics(
            table_name="test_table",
            delta_rows=1500,
            incremental_column="updated_at",
            bookmark_update_status="success",
            processing_rate=150.5,
            duration_seconds=10.0
        )
        
        # Flush metrics to CloudWatch
        self.metrics_publisher.flush_metrics()
        
        # Verify CloudWatch was called
        self.mock_cloudwatch.put_metric_data.assert_called()
        
        # Get all metric data
        call_args = self.mock_cloudwatch.put_metric_data.call_args_list
        all_metrics = []
        for call in call_args:
            metrics = call[1]['MetricData']
            all_metrics.extend(metrics)
        
        # Verify delta_rows metric exists
        delta_rows_metrics = [m for m in all_metrics if m['MetricName'] == 'IncrementalDeltaRows']
        self.assertEqual(len(delta_rows_metrics), 1)
        self.assertEqual(delta_rows_metrics[0]['Value'], 1500)
        
        # Verify dimensions include LoadType, TableName, and IncrementalColumn
        dimensions = {d['Name']: d['Value'] for d in delta_rows_metrics[0]['Dimensions']}
        self.assertEqual(dimensions['LoadType'], 'incremental')
        self.assertEqual(dimensions['TableName'], 'test_table')
        self.assertEqual(dimensions['IncrementalColumn'], 'updated_at')
        self.assertEqual(dimensions['JobName'], 'test-job')
    
    def test_publish_bookmark_update_status_success(self):
        """Test that bookmark update status metric is published correctly for success."""
        # Publish incremental load metrics with success status
        self.metrics_publisher.publish_incremental_load_metrics(
            table_name="test_table",
            delta_rows=1000,
            incremental_column="id",
            bookmark_update_status="success",
            processing_rate=100.0,
            duration_seconds=10.0
        )
        
        # Flush metrics
        self.metrics_publisher.flush_metrics()
        
        # Get all metrics
        call_args = self.mock_cloudwatch.put_metric_data.call_args_list
        all_metrics = []
        for call in call_args:
            metrics = call[1]['MetricData']
            all_metrics.extend(metrics)
        
        # Verify bookmark update status metric (numeric)
        status_metrics = [m for m in all_metrics if m['MetricName'] == 'IncrementalBookmarkUpdateStatus']
        self.assertEqual(len(status_metrics), 1)
        self.assertEqual(status_metrics[0]['Value'], 1)  # 1 for success
        
        # Verify success metric
        success_metrics = [m for m in all_metrics if m['MetricName'] == 'IncrementalBookmarkUpdateSuccess']
        self.assertEqual(len(success_metrics), 1)
        self.assertEqual(success_metrics[0]['Value'], 1)
        
        # Verify failure metric is not published
        failure_metrics = [m for m in all_metrics if m['MetricName'] == 'IncrementalBookmarkUpdateFailure']
        self.assertEqual(len(failure_metrics), 0)
    
    def test_publish_bookmark_update_status_failure(self):
        """Test that bookmark update status metric is published correctly for failure."""
        # Publish incremental load metrics with failure status
        self.metrics_publisher.publish_incremental_load_metrics(
            table_name="test_table",
            delta_rows=500,
            incremental_column="timestamp",
            bookmark_update_status="failed",
            processing_rate=50.0,
            duration_seconds=10.0
        )
        
        # Flush metrics
        self.metrics_publisher.flush_metrics()
        
        # Get all metrics
        call_args = self.mock_cloudwatch.put_metric_data.call_args_list
        all_metrics = []
        for call in call_args:
            metrics = call[1]['MetricData']
            all_metrics.extend(metrics)
        
        # Verify bookmark update status metric (numeric)
        status_metrics = [m for m in all_metrics if m['MetricName'] == 'IncrementalBookmarkUpdateStatus']
        self.assertEqual(len(status_metrics), 1)
        self.assertEqual(status_metrics[0]['Value'], 0)  # 0 for failure
        
        # Verify failure metric
        failure_metrics = [m for m in all_metrics if m['MetricName'] == 'IncrementalBookmarkUpdateFailure']
        self.assertEqual(len(failure_metrics), 1)
        self.assertEqual(failure_metrics[0]['Value'], 1)
        
        # Verify success metric is not published
        success_metrics = [m for m in all_metrics if m['MetricName'] == 'IncrementalBookmarkUpdateSuccess']
        self.assertEqual(len(success_metrics), 0)
    
    def test_publish_processing_rate_and_duration(self):
        """Test that processing rate and duration metrics are published."""
        # Publish incremental load metrics
        self.metrics_publisher.publish_incremental_load_metrics(
            table_name="test_table",
            delta_rows=2000,
            incremental_column="created_at",
            bookmark_update_status="success",
            processing_rate=200.5,
            duration_seconds=9.98
        )
        
        # Flush metrics
        self.metrics_publisher.flush_metrics()
        
        # Get all metrics
        call_args = self.mock_cloudwatch.put_metric_data.call_args_list
        all_metrics = []
        for call in call_args:
            metrics = call[1]['MetricData']
            all_metrics.extend(metrics)
        
        # Verify processing rate metric
        rate_metrics = [m for m in all_metrics if m['MetricName'] == 'IncrementalProcessingRate']
        self.assertEqual(len(rate_metrics), 1)
        self.assertEqual(rate_metrics[0]['Value'], 200.5)
        self.assertEqual(rate_metrics[0]['Unit'], 'Count/Second')
        
        # Verify duration metric
        duration_metrics = [m for m in all_metrics if m['MetricName'] == 'IncrementalLoadDuration']
        self.assertEqual(len(duration_metrics), 1)
        self.assertEqual(duration_metrics[0]['Value'], 9.98)
        self.assertEqual(duration_metrics[0]['Unit'], 'Seconds')
    
    def test_all_metrics_include_load_type_dimension(self):
        """Test that all incremental load metrics include load_type='incremental' dimension."""
        # Publish incremental load metrics
        self.metrics_publisher.publish_incremental_load_metrics(
            table_name="test_table",
            delta_rows=1000,
            incremental_column="updated_at",
            bookmark_update_status="success",
            processing_rate=100.0,
            duration_seconds=10.0
        )
        
        # Flush metrics
        self.metrics_publisher.flush_metrics()
        
        # Get all metrics
        call_args = self.mock_cloudwatch.put_metric_data.call_args_list
        all_metrics = []
        for call in call_args:
            metrics = call[1]['MetricData']
            all_metrics.extend(metrics)
        
        # Verify all metrics have LoadType='incremental' dimension
        for metric in all_metrics:
            dimensions = {d['Name']: d['Value'] for d in metric['Dimensions']}
            self.assertEqual(dimensions['LoadType'], 'incremental',
                           f"Metric {metric['MetricName']} missing LoadType='incremental' dimension")


class TestIncrementalMigratorMetricsIntegration(unittest.TestCase):
    """Test incremental migrator integration with metrics publishing."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.mock_spark = Mock()
        self.mock_connection_manager = Mock(spec=UnifiedConnectionManager)
        self.mock_bookmark_manager = Mock(spec=JobBookmarkManager)
        self.mock_metrics_publisher = Mock(spec=CloudWatchMetricsPublisher)
        
        # Create migrator
        self.migrator = IncrementalDataMigrator(
            spark_session=self.mock_spark,
            connection_manager=self.mock_connection_manager,
            bookmark_manager=self.mock_bookmark_manager,
            metrics_publisher=self.mock_metrics_publisher,
            streaming_progress_config=StreamingProgressConfig()
        )
        
        # Mock configs
        self.source_config = ConnectionConfig(
            engine_type="postgresql",
            connection_string="jdbc:postgresql://source:5432/db",
            database="db",
            schema="public",
            username="user",
            password="pass",
            jdbc_driver_path="s3://bucket/driver.jar"
        )
        
        self.target_config = ConnectionConfig(
            engine_type="postgresql",
            connection_string="jdbc:postgresql://target:5432/db",
            database="db",
            schema="public",
            username="user",
            password="pass",
            jdbc_driver_path="s3://bucket/driver.jar"
        )
        
        # Mock bookmark state
        self.mock_bookmark_state = Mock()
        self.mock_bookmark_state.incremental_strategy = 'timestamp'
        self.mock_bookmark_state.incremental_column = 'updated_at'
        self.mock_bookmark_state.last_processed_value = '2023-01-01'
        self.mock_bookmark_manager.get_bookmark_state.return_value = self.mock_bookmark_state
    
    @patch('glue_job.database.migration.DatabaseEngineManager')
    def test_incremental_load_publishes_metrics_on_success(self, mock_engine_manager):
        """Test that incremental load publishes metrics on successful completion."""
        mock_engine_manager.is_iceberg_engine.return_value = False
        
        # Mock DataFrame
        mock_df = Mock()
        mock_df.count.return_value = 1500
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
        self.assertEqual(result.delta_rows, 1500)
        
        # Verify incremental load metrics were published
        self.mock_metrics_publisher.publish_incremental_load_metrics.assert_called_once()
        
        # Verify call arguments
        call_kwargs = self.mock_metrics_publisher.publish_incremental_load_metrics.call_args[1]
        self.assertEqual(call_kwargs['table_name'], 'test_table')
        self.assertEqual(call_kwargs['delta_rows'], 1500)
        self.assertEqual(call_kwargs['incremental_column'], 'updated_at')
        self.assertEqual(call_kwargs['bookmark_update_status'], 'success')
        self.assertGreater(call_kwargs['processing_rate'], 0)
        self.assertGreater(call_kwargs['duration_seconds'], 0)
    
    def test_incremental_load_publishes_metrics_on_failure(self):
        """Test that incremental load publishes metrics even on failure."""
        # Mock DataFrame
        mock_df = Mock()
        mock_df.count.return_value = 500
        mock_df.agg.return_value.collect.return_value = [{'max_value': '2023-01-02'}]
        
        self.mock_connection_manager.read_table_data.return_value = mock_df
        # Make write fail
        self.mock_connection_manager.write_table_data.side_effect = Exception("Write failed")
        
        # Execute incremental migration
        result = self.migrator.perform_incremental_load_migration(
            self.source_config,
            self.target_config,
            "test_table"
        )
        
        # Verify migration failed
        self.assertEqual(result.status, 'failed')
        
        # Verify incremental load metrics were still published with failed status
        self.mock_metrics_publisher.publish_incremental_load_metrics.assert_called_once()
        
        # Verify call arguments
        call_kwargs = self.mock_metrics_publisher.publish_incremental_load_metrics.call_args[1]
        self.assertEqual(call_kwargs['table_name'], 'test_table')
        self.assertEqual(call_kwargs['bookmark_update_status'], 'failed')
    
    @patch('glue_job.database.migration.DatabaseEngineManager')
    def test_incremental_load_continues_on_metrics_failure(self, mock_engine_manager):
        """Test that incremental load continues even if metrics publishing fails."""
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
        
        # Make metrics publishing fail
        self.mock_metrics_publisher.publish_incremental_load_metrics.side_effect = Exception("Metrics error")
        
        # Execute incremental migration - should not fail
        result = self.migrator.perform_incremental_load_migration(
            self.source_config,
            self.target_config,
            "test_table"
        )
        
        # Verify migration still completed successfully
        self.assertEqual(result.status, 'completed')
        self.assertEqual(result.delta_rows, 1000)


if __name__ == '__main__':
    unittest.main(verbosity=2)
