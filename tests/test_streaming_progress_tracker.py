#!/usr/bin/env python3
"""
Unit tests for StreamingProgressTracker.

This test suite covers:
- StreamingProgressTracker initialization and configuration
- Progress tracking and update emission
- ETA calculation
- Progress percentage calculation
- Integration with CloudWatch metrics publisher
"""

import unittest
from unittest.mock import Mock, MagicMock, patch
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
from glue_job.monitoring import (
    StreamingProgressTracker, StreamingProgressConfig,
    CloudWatchMetricsPublisher
)


class TestStreamingProgressConfig(unittest.TestCase):
    """Test StreamingProgressConfig functionality."""
    
    def test_config_creation_with_defaults(self):
        """Test creating config with default values."""
        config = StreamingProgressConfig()
        
        self.assertEqual(config.update_interval_seconds, 60)
        self.assertEqual(config.batch_size_rows, 100_000)
        self.assertTrue(config.enable_metrics)
        self.assertTrue(config.enable_logging)
    
    def test_config_creation_with_custom_values(self):
        """Test creating config with custom values."""
        config = StreamingProgressConfig(
            update_interval_seconds=30,
            batch_size_rows=50_000,
            enable_metrics=False,
            enable_logging=False
        )
        
        self.assertEqual(config.update_interval_seconds, 30)
        self.assertEqual(config.batch_size_rows, 50_000)
        self.assertFalse(config.enable_metrics)
        self.assertFalse(config.enable_logging)
    
    def test_config_validation_positive_interval(self):
        """Test config validation for positive update interval."""
        config = StreamingProgressConfig(update_interval_seconds=-1)
        
        with self.assertRaises(ValueError) as context:
            config.validate()
        
        self.assertIn("update_interval_seconds must be positive", str(context.exception))
    
    def test_config_validation_positive_batch_size(self):
        """Test config validation for positive batch size."""
        config = StreamingProgressConfig(batch_size_rows=0)
        
        with self.assertRaises(ValueError) as context:
            config.validate()
        
        self.assertIn("batch_size_rows must be positive", str(context.exception))


class TestStreamingProgressTracker(unittest.TestCase):
    """Test StreamingProgressTracker functionality."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.mock_metrics_publisher = Mock(spec=CloudWatchMetricsPublisher)
        self.config = StreamingProgressConfig(
            update_interval_seconds=1,  # Short interval for testing
            batch_size_rows=100,
            enable_metrics=True,
            enable_logging=True
        )
    
    def test_tracker_initialization(self):
        """Test tracker initialization."""
        tracker = StreamingProgressTracker(
            table_name="test_table",
            load_type="full",
            config=self.config,
            metrics_publisher=self.mock_metrics_publisher
        )
        
        self.assertEqual(tracker.table_name, "test_table")
        self.assertEqual(tracker.load_type, "full")
        self.assertEqual(tracker.rows_processed, 0)
        self.assertFalse(tracker.is_active)
        self.assertIsNone(tracker.total_rows)
    
    def test_start_tracking_with_total_rows(self):
        """Test starting tracking with known total rows."""
        tracker = StreamingProgressTracker(
            table_name="test_table",
            load_type="full",
            config=self.config,
            metrics_publisher=self.mock_metrics_publisher
        )
        
        tracker.start_tracking(total_rows=10000)
        
        self.assertTrue(tracker.is_active)
        self.assertEqual(tracker.total_rows, 10000)
        self.assertEqual(tracker.rows_processed, 0)
        self.assertEqual(tracker.progress_updates_emitted, 0)
    
    def test_start_tracking_without_total_rows(self):
        """Test starting tracking without known total rows."""
        tracker = StreamingProgressTracker(
            table_name="test_table",
            load_type="incremental",
            config=self.config,
            metrics_publisher=self.mock_metrics_publisher
        )
        
        tracker.start_tracking()
        
        self.assertTrue(tracker.is_active)
        self.assertIsNone(tracker.total_rows)
    
    def test_update_progress(self):
        """Test updating progress."""
        tracker = StreamingProgressTracker(
            table_name="test_table",
            load_type="full",
            config=self.config,
            metrics_publisher=self.mock_metrics_publisher
        )
        
        tracker.start_tracking(total_rows=10000)
        tracker.update_progress(5000)
        
        self.assertEqual(tracker.rows_processed, 5000)
    
    def test_complete_tracking(self):
        """Test completing tracking."""
        tracker = StreamingProgressTracker(
            table_name="test_table",
            load_type="full",
            config=self.config,
            metrics_publisher=self.mock_metrics_publisher
        )
        
        tracker.start_tracking(total_rows=10000)
        tracker.update_progress(5000)
        tracker.complete_tracking(final_row_count=10000)
        
        self.assertFalse(tracker.is_active)
        self.assertEqual(tracker.rows_processed, 10000)
    
    def test_progress_percentage_with_total_rows(self):
        """Test calculating progress percentage with known total."""
        tracker = StreamingProgressTracker(
            table_name="test_table",
            load_type="full",
            config=self.config,
            metrics_publisher=self.mock_metrics_publisher
        )
        
        tracker.start_tracking(total_rows=10000)
        tracker.update_progress(2500)
        
        percentage = tracker.get_progress_percentage()
        self.assertEqual(percentage, 25.0)
    
    def test_progress_percentage_without_total_rows(self):
        """Test calculating progress percentage without known total."""
        tracker = StreamingProgressTracker(
            table_name="test_table",
            load_type="incremental",
            config=self.config,
            metrics_publisher=self.mock_metrics_publisher
        )
        
        tracker.start_tracking()
        tracker.update_progress(1000)
        
        percentage = tracker.get_progress_percentage()
        self.assertEqual(percentage, 0.0)
    
    def test_rows_per_second_calculation(self):
        """Test calculating rows per second."""
        tracker = StreamingProgressTracker(
            table_name="test_table",
            load_type="full",
            config=self.config,
            metrics_publisher=self.mock_metrics_publisher
        )
        
        tracker.start_tracking(total_rows=10000)
        
        # Simulate some processing time
        time.sleep(0.1)
        tracker.update_progress(1000)
        
        rows_per_second = tracker.get_rows_per_second()
        self.assertGreater(rows_per_second, 0)
    
    def test_eta_calculation_with_total_rows(self):
        """Test ETA calculation with known total rows."""
        tracker = StreamingProgressTracker(
            table_name="test_table",
            load_type="full",
            config=self.config,
            metrics_publisher=self.mock_metrics_publisher
        )
        
        tracker.start_tracking(total_rows=10000)
        
        # Simulate some processing
        time.sleep(0.1)
        tracker.update_progress(2500)
        
        eta = tracker._calculate_eta()
        self.assertIsNotNone(eta)
        self.assertGreater(eta, 0)
    
    def test_eta_calculation_without_total_rows(self):
        """Test ETA calculation without known total rows."""
        tracker = StreamingProgressTracker(
            table_name="test_table",
            load_type="incremental",
            config=self.config,
            metrics_publisher=self.mock_metrics_publisher
        )
        
        tracker.start_tracking()
        tracker.update_progress(1000)
        
        eta = tracker._calculate_eta()
        self.assertIsNone(eta)
    
    def test_eta_calculation_when_complete(self):
        """Test ETA calculation when processing is complete."""
        tracker = StreamingProgressTracker(
            table_name="test_table",
            load_type="full",
            config=self.config,
            metrics_publisher=self.mock_metrics_publisher
        )
        
        tracker.start_tracking(total_rows=10000)
        tracker.update_progress(10000)
        
        eta = tracker._calculate_eta()
        self.assertEqual(eta, 0.0)
    
    def test_should_emit_update_by_interval(self):
        """Test update emission based on time interval."""
        config = StreamingProgressConfig(
            update_interval_seconds=0.1,  # Very short for testing
            batch_size_rows=100_000
        )
        
        tracker = StreamingProgressTracker(
            table_name="test_table",
            load_type="full",
            config=config,
            metrics_publisher=self.mock_metrics_publisher
        )
        
        tracker.start_tracking(total_rows=10000)
        
        # Should not emit immediately
        self.assertFalse(tracker._should_emit_update())
        
        # Wait for interval to pass
        time.sleep(0.15)
        
        # Should emit now
        self.assertTrue(tracker._should_emit_update())
    
    def test_should_emit_update_by_batch_size(self):
        """Test update emission based on batch size."""
        config = StreamingProgressConfig(
            update_interval_seconds=60,
            batch_size_rows=100
        )
        
        tracker = StreamingProgressTracker(
            table_name="test_table",
            load_type="full",
            config=config,
            metrics_publisher=self.mock_metrics_publisher
        )
        
        tracker.start_tracking(total_rows=10000)
        
        # Process exactly batch_size rows
        tracker.update_progress(100)
        
        # Should emit at batch boundary
        self.assertTrue(tracker._should_emit_update())
    
    def test_metrics_publishing_on_update(self):
        """Test that metrics are published on progress update."""
        tracker = StreamingProgressTracker(
            table_name="test_table",
            load_type="full",
            config=self.config,
            metrics_publisher=self.mock_metrics_publisher
        )
        
        tracker.start_tracking(total_rows=10000)
        
        # Force an update emission
        time.sleep(1.1)
        tracker.update_progress(5000)
        
        # Verify metrics were published
        self.mock_metrics_publisher.publish_migration_progress_metrics.assert_called()
    
    def test_metrics_publishing_on_complete(self):
        """Test that final metrics are published on completion."""
        tracker = StreamingProgressTracker(
            table_name="test_table",
            load_type="full",
            config=self.config,
            metrics_publisher=self.mock_metrics_publisher
        )
        
        tracker.start_tracking(total_rows=10000)
        tracker.complete_tracking(final_row_count=10000)
        
        # Verify final metrics were published
        self.mock_metrics_publisher.publish_migration_progress_metrics.assert_called()
    
    def test_metrics_disabled(self):
        """Test that metrics are not published when disabled."""
        config = StreamingProgressConfig(
            update_interval_seconds=1,
            enable_metrics=False
        )
        
        tracker = StreamingProgressTracker(
            table_name="test_table",
            load_type="full",
            config=config,
            metrics_publisher=self.mock_metrics_publisher
        )
        
        tracker.start_tracking(total_rows=10000)
        time.sleep(1.1)
        tracker.update_progress(5000)
        
        # Verify metrics were NOT published
        self.mock_metrics_publisher.publish_migration_progress_metrics.assert_not_called()
    
    def test_elapsed_time(self):
        """Test elapsed time calculation."""
        tracker = StreamingProgressTracker(
            table_name="test_table",
            load_type="full",
            config=self.config,
            metrics_publisher=self.mock_metrics_publisher
        )
        
        tracker.start_tracking(total_rows=10000)
        time.sleep(0.1)
        
        elapsed = tracker.get_elapsed_time()
        self.assertGreaterEqual(elapsed, 0.1)
    
    def test_load_type_full(self):
        """Test tracker with full load type."""
        tracker = StreamingProgressTracker(
            table_name="test_table",
            load_type="full",
            config=self.config,
            metrics_publisher=self.mock_metrics_publisher
        )
        
        self.assertEqual(tracker.load_type, "full")
    
    def test_load_type_incremental(self):
        """Test tracker with incremental load type."""
        tracker = StreamingProgressTracker(
            table_name="test_table",
            load_type="incremental",
            config=self.config,
            metrics_publisher=self.mock_metrics_publisher
        )
        
        self.assertEqual(tracker.load_type, "incremental")
    
    def test_iceberg_engine_type_detection(self):
        """Test Iceberg engine type detection."""
        tracker = StreamingProgressTracker(
            table_name="test_table",
            load_type="full",
            config=self.config,
            metrics_publisher=self.mock_metrics_publisher,
            engine_type="iceberg"
        )
        
        self.assertEqual(tracker.engine_type, "iceberg")
        self.assertTrue(tracker.is_iceberg)
    
    def test_jdbc_engine_type_detection(self):
        """Test JDBC engine type detection."""
        tracker = StreamingProgressTracker(
            table_name="test_table",
            load_type="full",
            config=self.config,
            metrics_publisher=self.mock_metrics_publisher,
            engine_type="postgresql"
        )
        
        self.assertEqual(tracker.engine_type, "postgresql")
        self.assertFalse(tracker.is_iceberg)
    
    def test_unknown_engine_type_default(self):
        """Test default engine type when not specified."""
        tracker = StreamingProgressTracker(
            table_name="test_table",
            load_type="full",
            config=self.config,
            metrics_publisher=self.mock_metrics_publisher
        )
        
        self.assertEqual(tracker.engine_type, "unknown")
        self.assertFalse(tracker.is_iceberg)
    
    def test_iceberg_progress_tracking_with_metadata_awareness(self):
        """Test progress tracking for Iceberg with metadata awareness."""
        tracker = StreamingProgressTracker(
            table_name="iceberg_table",
            load_type="full",
            config=self.config,
            metrics_publisher=self.mock_metrics_publisher,
            engine_type="iceberg"
        )
        
        tracker.start_tracking(total_rows=1000000)
        
        # Verify Iceberg-specific attributes are set
        self.assertTrue(tracker.is_iceberg)
        self.assertEqual(tracker.engine_type, "iceberg")
        
        # Update progress
        tracker.update_progress(500000)
        self.assertEqual(tracker.rows_processed, 500000)
        
        # Complete tracking
        tracker.complete_tracking(final_row_count=1000000)
        self.assertFalse(tracker.is_active)


if __name__ == '__main__':
    unittest.main(verbosity=2)
