#!/usr/bin/env python3
"""
Unit tests for monitoring modules.

This test suite covers:
- StructuredLogger functionality
- CloudWatchMetricsPublisher and metrics publishing
- ProcessingMetrics and progress tracking classes
"""

import unittest
from unittest.mock import Mock, MagicMock, patch
import sys
import os
from datetime import datetime, timezone

# Add src directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'src'))

# Mock PySpark and AWS Glue imports for testing
mock_modules = [
    'awsglue', 'awsglue.utils', 'awsglue.context', 'awsglue.job',
    'pyspark', 'pyspark.context', 'pyspark.sql', 'pyspark.sql.types',
    'pyspark.sql.functions', 'boto3'
]

for module in mock_modules:
    sys.modules[module] = MagicMock()

# Import the classes to test from new modular structure
from glue_job.monitoring import (
    StructuredLogger, CloudWatchMetricsPublisher, estimate_dataframe_size,
    ProcessingMetrics, FullLoadProgress, IncrementalLoadProgress
)


class TestStructuredLogger(unittest.TestCase):
    """Test StructuredLogger functionality."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.logger = StructuredLogger("test-job")
    
    def test_logger_initialization(self):
        """Test logger initialization."""
        self.assertEqual(self.logger.job_name, "test-job")
        self.assertIsNotNone(self.logger.logger)
    
    @patch('logging.getLogger')
    def test_log_info_message(self, mock_get_logger):
        """Test logging info message."""
        mock_logger_instance = Mock()
        mock_get_logger.return_value = mock_logger_instance
        
        logger = StructuredLogger("test-job")
        logger.info("Test message", {"key": "value"})
        
        mock_logger_instance.info.assert_called_once()
    
    @patch('logging.getLogger')
    def test_log_error_message(self, mock_get_logger):
        """Test logging error message."""
        mock_logger_instance = Mock()
        mock_get_logger.return_value = mock_logger_instance
        
        logger = StructuredLogger("test-job")
        logger.error("Test error", {"error_code": "E001"})
        
        mock_logger_instance.error.assert_called_once()
    
    @patch('logging.getLogger')
    def test_log_warning_message(self, mock_get_logger):
        """Test logging warning message."""
        mock_logger_instance = Mock()
        mock_get_logger.return_value = mock_logger_instance
        
        logger = StructuredLogger("test-job")
        logger.warning("Test warning", {"warning_type": "performance"})
        
        mock_logger_instance.warning.assert_called_once()


class TestCloudWatchMetricsPublisher(unittest.TestCase):
    """Test CloudWatchMetricsPublisher functionality."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.publisher = CloudWatchMetricsPublisher("test-job")
    
    @patch('boto3.client')
    def test_publish_metric_success(self, mock_boto_client):
        """Test successful metric publishing."""
        # Mock CloudWatch client
        mock_cloudwatch = Mock()
        mock_boto_client.return_value = mock_cloudwatch
        
        # Test publishing metric
        self.publisher.publish_metric("TestMetric", 100.0, "Count")
        
        mock_cloudwatch.put_metric_data.assert_called_once()
    
    @patch('boto3.client')
    def test_publish_metric_with_dimensions(self, mock_boto_client):
        """Test publishing metric with dimensions."""
        # Mock CloudWatch client
        mock_cloudwatch = Mock()
        mock_boto_client.return_value = mock_cloudwatch
        
        # Test publishing metric with dimensions
        dimensions = {"TableName": "test_table", "ProcessingMode": "full-load"}
        self.publisher.publish_metric("RecordsProcessed", 1000.0, "Count", dimensions)
        
        mock_cloudwatch.put_metric_data.assert_called_once()
        
        # Verify dimensions were included
        call_args = mock_cloudwatch.put_metric_data.call_args
        metric_data = call_args[1]['MetricData'][0]
        self.assertIn('Dimensions', metric_data)
    
    @patch('boto3.client')
    def test_publish_metric_failure(self, mock_boto_client):
        """Test metric publishing failure handling."""
        # Mock CloudWatch client with failure
        mock_cloudwatch = Mock()
        mock_cloudwatch.put_metric_data.side_effect = Exception("CloudWatch error")
        mock_boto_client.return_value = mock_cloudwatch
        
        # Test publishing metric (should not raise exception)
        try:
            self.publisher.publish_metric("TestMetric", 100.0, "Count")
        except Exception:
            self.fail("publish_metric should handle exceptions gracefully")


class TestProcessingMetrics(unittest.TestCase):
    """Test ProcessingMetrics functionality."""
    
    def test_processing_metrics_creation(self):
        """Test creating ProcessingMetrics instance."""
        start_time = datetime.now(timezone.utc)
        end_time = datetime.now(timezone.utc)
        
        metrics = ProcessingMetrics(
            records_processed=1000,
            records_failed=5,
            processing_time_seconds=120.5,
            start_time=start_time,
            end_time=end_time
        )
        
        self.assertEqual(metrics.records_processed, 1000)
        self.assertEqual(metrics.records_failed, 5)
        self.assertEqual(metrics.processing_time_seconds, 120.5)
        self.assertEqual(metrics.start_time, start_time)
        self.assertEqual(metrics.end_time, end_time)
    
    def test_processing_metrics_success_rate(self):
        """Test calculating success rate."""
        metrics = ProcessingMetrics(
            records_processed=1000,
            records_failed=50,
            processing_time_seconds=120.5,
            start_time=datetime.now(timezone.utc),
            end_time=datetime.now(timezone.utc)
        )
        
        success_rate = metrics.get_success_rate()
        expected_rate = (1000 - 50) / 1000 * 100
        self.assertEqual(success_rate, expected_rate)
    
    def test_processing_metrics_throughput(self):
        """Test calculating throughput."""
        metrics = ProcessingMetrics(
            records_processed=1000,
            records_failed=0,
            processing_time_seconds=100.0,
            start_time=datetime.now(timezone.utc),
            end_time=datetime.now(timezone.utc)
        )
        
        throughput = metrics.get_throughput()
        expected_throughput = 1000 / 100.0
        self.assertEqual(throughput, expected_throughput)


class TestFullLoadProgress(unittest.TestCase):
    """Test FullLoadProgress functionality."""
    
    def test_full_load_progress_creation(self):
        """Test creating FullLoadProgress instance."""
        progress = FullLoadProgress(
            table_name="test_table",
            total_records=10000,
            processed_records=5000
        )
        
        self.assertEqual(progress.table_name, "test_table")
        self.assertEqual(progress.total_records, 10000)
        self.assertEqual(progress.processed_records, 5000)
    
    def test_full_load_progress_percentage(self):
        """Test calculating progress percentage."""
        progress = FullLoadProgress(
            table_name="test_table",
            total_records=10000,
            processed_records=2500
        )
        
        percentage = progress.get_progress_percentage()
        self.assertEqual(percentage, 25.0)
    
    def test_full_load_progress_is_complete(self):
        """Test checking if progress is complete."""
        progress = FullLoadProgress(
            table_name="test_table",
            total_records=10000,
            processed_records=10000
        )
        
        self.assertTrue(progress.is_complete())
        
        progress.processed_records = 9999
        self.assertFalse(progress.is_complete())


class TestIncrementalLoadProgress(unittest.TestCase):
    """Test IncrementalLoadProgress functionality."""
    
    def test_incremental_load_progress_creation(self):
        """Test creating IncrementalLoadProgress instance."""
        progress = IncrementalLoadProgress(
            table_name="test_table",
            last_processed_value="2023-01-01 00:00:00",
            current_batch_size=500
        )
        
        self.assertEqual(progress.table_name, "test_table")
        self.assertEqual(progress.last_processed_value, "2023-01-01 00:00:00")
        self.assertEqual(progress.current_batch_size, 500)
    
    def test_incremental_load_progress_update(self):
        """Test updating incremental load progress."""
        progress = IncrementalLoadProgress(
            table_name="test_table",
            last_processed_value="2023-01-01 00:00:00",
            current_batch_size=500
        )
        
        progress.update_progress("2023-01-02 00:00:00", 750)
        
        self.assertEqual(progress.last_processed_value, "2023-01-02 00:00:00")
        self.assertEqual(progress.current_batch_size, 750)


class TestEstimateDataFrameSize(unittest.TestCase):
    """Test estimate_dataframe_size utility function."""
    
    def test_estimate_dataframe_size(self):
        """Test estimating DataFrame size."""
        # Mock DataFrame
        mock_df = Mock()
        mock_df.count.return_value = 1000
        
        # Mock schema with fields
        mock_field1 = Mock()
        mock_field1.dataType.typeName.return_value = "string"
        mock_field2 = Mock()
        mock_field2.dataType.typeName.return_value = "integer"
        
        mock_df.schema.fields = [mock_field1, mock_field2]
        
        # Test size estimation
        estimated_size = estimate_dataframe_size(mock_df)
        
        # Should return a positive number
        self.assertGreater(estimated_size, 0)
        self.assertIsInstance(estimated_size, (int, float))


if __name__ == '__main__':
    unittest.main(verbosity=2)