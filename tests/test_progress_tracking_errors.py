#!/usr/bin/env python3
"""
Unit tests for Task 13: Progress Tracking Error Handling.

This test suite covers:
- Error handling in start_tracking
- Error handling in update_progress
- Error handling in complete_tracking
- Error handling in _emit_progress_update
- Graceful degradation when tracking fails
- Error metrics publishing
- Migration continuation despite tracking failures
"""

import unittest
from unittest.mock import Mock, MagicMock, patch
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
from glue_job.monitoring import (
    StreamingProgressTracker, StreamingProgressConfig,
    CloudWatchMetricsPublisher
)


class TestProgressTrackingErrorHandling(unittest.TestCase):
    """Test error handling in StreamingProgressTracker."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.mock_metrics_publisher = Mock(spec=CloudWatchMetricsPublisher)
        self.config = StreamingProgressConfig(
            update_interval_seconds=1,
            batch_size_rows=100,
            enable_metrics=True,
            enable_logging=True
        )
    
    def test_start_tracking_error_disables_tracking(self):
        """Test that error in start_tracking disables tracking."""
        tracker = StreamingProgressTracker(
            table_name="test_table",
            load_type="full",
            config=self.config,
            metrics_publisher=self.mock_metrics_publisher
        )
        
        # Mock structured logger to raise an exception
        with patch.object(tracker.structured_logger, 'info', side_effect=Exception("Logging failed")):
            tracker.start_tracking(total_rows=10000)
        
        # Tracker should be disabled
        self.assertFalse(tracker.is_active)
        
        # Error metrics should be published
        self.mock_metrics_publisher.publish_error_metrics.assert_called_once_with(
            error_category="PROGRESS_TRACKING_START_FAILED",
            error_operation="start_tracking"
        )
    
    def test_start_tracking_error_logs_warning(self):
        """Test that error in start_tracking logs warning."""
        tracker = StreamingProgressTracker(
            table_name="test_table",
            load_type="full",
            config=self.config,
            metrics_publisher=self.mock_metrics_publisher
        )
        
        # Mock structured logger to raise an exception on info, but allow warning
        with patch.object(tracker.structured_logger, 'info', side_effect=Exception("Logging failed")):
            with patch.object(tracker.structured_logger, 'warning') as mock_warning:
                tracker.start_tracking(total_rows=10000)
                
                # Warning should be logged
                mock_warning.assert_called_once()
                call_args = mock_warning.call_args
                self.assertIn("Failed to start progress tracking", call_args[0][0])
    
    def test_update_progress_error_disables_tracking(self):
        """Test that error in update_progress disables tracking."""
        tracker = StreamingProgressTracker(
            table_name="test_table",
            load_type="full",
            config=self.config,
            metrics_publisher=self.mock_metrics_publisher
        )
        
        tracker.start_tracking(total_rows=10000)
        self.assertTrue(tracker.is_active)
        
        # Mock _should_emit_update to raise an exception
        with patch.object(tracker, '_should_emit_update', side_effect=Exception("Update check failed")):
            tracker.update_progress(5000)
        
        # Tracker should be disabled
        self.assertFalse(tracker.is_active)
        
        # Error metrics should be published
        self.mock_metrics_publisher.publish_error_metrics.assert_called_with(
            error_category="PROGRESS_TRACKING_UPDATE_FAILED",
            error_operation="update_progress"
        )
    
    def test_update_progress_when_inactive_does_nothing(self):
        """Test that update_progress does nothing when tracker is inactive."""
        tracker = StreamingProgressTracker(
            table_name="test_table",
            load_type="full",
            config=self.config,
            metrics_publisher=self.mock_metrics_publisher
        )
        
        # Don't start tracking
        self.assertFalse(tracker.is_active)
        
        # Update should do nothing
        tracker.update_progress(5000)
        
        # Rows processed should still be 0
        self.assertEqual(tracker.rows_processed, 0)
    
    def test_complete_tracking_error_marks_inactive(self):
        """Test that error in complete_tracking marks tracker as inactive."""
        tracker = StreamingProgressTracker(
            table_name="test_table",
            load_type="full",
            config=self.config,
            metrics_publisher=self.mock_metrics_publisher
        )
        
        tracker.start_tracking(total_rows=10000)
        self.assertTrue(tracker.is_active)
        
        # Mock _emit_progress_update to raise an exception
        with patch.object(tracker, '_emit_progress_update', side_effect=Exception("Emit failed")):
            tracker.complete_tracking(final_row_count=10000)
        
        # Tracker should be marked inactive
        self.assertFalse(tracker.is_active)
        
        # Error metrics should be published
        self.mock_metrics_publisher.publish_error_metrics.assert_called_with(
            error_category="PROGRESS_TRACKING_COMPLETE_FAILED",
            error_operation="complete_tracking"
        )
    
    def test_complete_tracking_when_inactive_does_nothing(self):
        """Test that complete_tracking does nothing when tracker is inactive."""
        tracker = StreamingProgressTracker(
            table_name="test_table",
            load_type="full",
            config=self.config,
            metrics_publisher=self.mock_metrics_publisher
        )
        
        # Don't start tracking
        self.assertFalse(tracker.is_active)
        
        # Complete should do nothing
        tracker.complete_tracking(final_row_count=10000)
        
        # Rows processed should still be 0
        self.assertEqual(tracker.rows_processed, 0)
    
    def test_emit_progress_update_metrics_error_continues(self):
        """Test that metrics publishing error doesn't stop progress update."""
        tracker = StreamingProgressTracker(
            table_name="test_table",
            load_type="full",
            config=self.config,
            metrics_publisher=self.mock_metrics_publisher
        )
        
        tracker.start_tracking(total_rows=10000)
        
        # Mock metrics publisher to raise an exception
        self.mock_metrics_publisher.publish_migration_progress_metrics.side_effect = Exception("Metrics failed")
        
        # Force an update emission
        tracker.last_update_time = 0  # Force update
        tracker.update_progress(5000)
        
        # Tracker should still be active
        self.assertTrue(tracker.is_active)
        
        # Rows should be updated
        self.assertEqual(tracker.rows_processed, 5000)
    
    def test_emit_progress_update_logging_error_continues(self):
        """Test that logging error doesn't stop progress update."""
        tracker = StreamingProgressTracker(
            table_name="test_table",
            load_type="full",
            config=self.config,
            metrics_publisher=self.mock_metrics_publisher
        )
        
        tracker.start_tracking(total_rows=10000)
        
        # Mock structured logger to raise an exception
        with patch.object(tracker.structured_logger, 'log_migration_progress', side_effect=Exception("Logging failed")):
            # Force an update emission
            tracker.last_update_time = 0  # Force update
            tracker.update_progress(5000)
        
        # Error metrics should be published
        self.mock_metrics_publisher.publish_error_metrics.assert_called_with(
            error_category="PROGRESS_TRACKING_EMIT_FAILED",
            error_operation="emit_progress_update"
        )
    
    def test_metrics_publishing_error_in_error_handler_is_silent(self):
        """Test that error in error metrics publishing is silently ignored."""
        tracker = StreamingProgressTracker(
            table_name="test_table",
            load_type="full",
            config=self.config,
            metrics_publisher=self.mock_metrics_publisher
        )
        
        # Mock both info and publish_error_metrics to raise exceptions
        with patch.object(tracker.structured_logger, 'info', side_effect=Exception("Logging failed")):
            self.mock_metrics_publisher.publish_error_metrics.side_effect = Exception("Metrics failed")
            
            # Should not raise exception
            tracker.start_tracking(total_rows=10000)
        
        # Tracker should be disabled
        self.assertFalse(tracker.is_active)
    
    def test_graceful_degradation_allows_migration_to_continue(self):
        """Test that tracking errors don't prevent migration from continuing."""
        tracker = StreamingProgressTracker(
            table_name="test_table",
            load_type="full",
            config=self.config,
            metrics_publisher=self.mock_metrics_publisher
        )
        
        # Start tracking fails
        with patch.object(tracker.structured_logger, 'info', side_effect=Exception("Start failed")):
            tracker.start_tracking(total_rows=10000)
        
        self.assertFalse(tracker.is_active)
        
        # Subsequent operations should not raise exceptions
        tracker.update_progress(5000)  # Should do nothing
        tracker.complete_tracking(10000)  # Should do nothing
        
        # No exceptions should be raised
    
    def test_error_handling_preserves_row_count_data(self):
        """Test that error handling preserves row count data when possible."""
        tracker = StreamingProgressTracker(
            table_name="test_table",
            load_type="full",
            config=self.config,
            metrics_publisher=self.mock_metrics_publisher
        )
        
        tracker.start_tracking(total_rows=10000)
        tracker.update_progress(5000)
        
        # Mock _emit_progress_update to raise an exception
        with patch.object(tracker, '_emit_progress_update', side_effect=Exception("Emit failed")):
            tracker.complete_tracking(final_row_count=10000)
        
        # Row count should still be updated
        self.assertEqual(tracker.rows_processed, 10000)
    
    def test_multiple_errors_dont_cause_cascading_failures(self):
        """Test that multiple errors don't cause cascading failures."""
        tracker = StreamingProgressTracker(
            table_name="test_table",
            load_type="full",
            config=self.config,
            metrics_publisher=self.mock_metrics_publisher
        )
        
        tracker.start_tracking(total_rows=10000)
        
        # First error in update
        with patch.object(tracker, '_should_emit_update', side_effect=Exception("Update failed")):
            tracker.update_progress(5000)
        
        self.assertFalse(tracker.is_active)
        
        # Second error in complete (should be handled gracefully)
        tracker.complete_tracking(10000)  # Should not raise
        
        # No exceptions should be raised


class TestProgressTrackingErrorMetrics(unittest.TestCase):
    """Test error metrics publishing for progress tracking failures."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.mock_metrics_publisher = Mock(spec=CloudWatchMetricsPublisher)
        self.config = StreamingProgressConfig(
            update_interval_seconds=1,
            batch_size_rows=100,
            enable_metrics=True,
            enable_logging=True
        )
    
    def test_start_tracking_error_publishes_correct_metrics(self):
        """Test that start_tracking error publishes correct error metrics."""
        tracker = StreamingProgressTracker(
            table_name="test_table",
            load_type="full",
            config=self.config,
            metrics_publisher=self.mock_metrics_publisher
        )
        
        with patch.object(tracker.structured_logger, 'info', side_effect=Exception("Start failed")):
            tracker.start_tracking(total_rows=10000)
        
        self.mock_metrics_publisher.publish_error_metrics.assert_called_once_with(
            error_category="PROGRESS_TRACKING_START_FAILED",
            error_operation="start_tracking"
        )
    
    def test_update_progress_error_publishes_correct_metrics(self):
        """Test that update_progress error publishes correct error metrics."""
        tracker = StreamingProgressTracker(
            table_name="test_table",
            load_type="full",
            config=self.config,
            metrics_publisher=self.mock_metrics_publisher
        )
        
        tracker.start_tracking(total_rows=10000)
        
        with patch.object(tracker, '_should_emit_update', side_effect=Exception("Update failed")):
            tracker.update_progress(5000)
        
        self.mock_metrics_publisher.publish_error_metrics.assert_called_with(
            error_category="PROGRESS_TRACKING_UPDATE_FAILED",
            error_operation="update_progress"
        )
    
    def test_complete_tracking_error_publishes_correct_metrics(self):
        """Test that complete_tracking error publishes correct error metrics."""
        tracker = StreamingProgressTracker(
            table_name="test_table",
            load_type="full",
            config=self.config,
            metrics_publisher=self.mock_metrics_publisher
        )
        
        tracker.start_tracking(total_rows=10000)
        
        with patch.object(tracker, '_emit_progress_update', side_effect=Exception("Complete failed")):
            tracker.complete_tracking(final_row_count=10000)
        
        self.mock_metrics_publisher.publish_error_metrics.assert_called_with(
            error_category="PROGRESS_TRACKING_COMPLETE_FAILED",
            error_operation="complete_tracking"
        )
    
    def test_emit_progress_update_error_publishes_correct_metrics(self):
        """Test that _emit_progress_update error publishes correct error metrics."""
        tracker = StreamingProgressTracker(
            table_name="test_table",
            load_type="full",
            config=self.config,
            metrics_publisher=self.mock_metrics_publisher
        )
        
        tracker.start_tracking(total_rows=10000)
        
        with patch.object(tracker.structured_logger, 'log_migration_progress', side_effect=Exception("Emit failed")):
            tracker.last_update_time = 0  # Force update
            tracker.update_progress(5000)
        
        self.mock_metrics_publisher.publish_error_metrics.assert_called_with(
            error_category="PROGRESS_TRACKING_EMIT_FAILED",
            error_operation="emit_progress_update"
        )


if __name__ == '__main__':
    unittest.main(verbosity=2)
