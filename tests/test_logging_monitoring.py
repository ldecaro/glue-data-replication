#!/usr/bin/env python3
"""
Test script to verify logging and monitoring implementation.
This script tests the key components without requiring a full Glue environment.
"""

import sys
import time
import json
import os
from datetime import datetime, timezone
from unittest.mock import Mock, patch



# Mock AWS dependencies for testing
# Note: boto3 is not mocked to allow proper exception handling
sys.modules['awsglue'] = Mock()
sys.modules['awsglue.utils'] = Mock()
sys.modules['awsglue.context'] = Mock()
sys.modules['awsglue.job'] = Mock()
sys.modules['pyspark'] = Mock()
sys.modules['pyspark.context'] = Mock()
sys.modules['pyspark.sql'] = Mock()
sys.modules['pyspark.sql.types'] = Mock()
sys.modules['pyspark.sql.functions'] = Mock()

# Add src directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'src'))

# Import monitoring classes
from glue_job.monitoring.metrics import PerformanceMonitor

# Import our classes from new modular structure
from glue_job.monitoring import (
    ProcessingMetrics, 
    StructuredLogger, 
    CloudWatchMetricsPublisher
)

def test_processing_metrics():
    """Test ProcessingMetrics functionality."""
    print("Testing ProcessingMetrics...")
    
    # Create metrics instance
    metrics = ProcessingMetrics("test_table")
    assert metrics.table_name == "test_table"
    assert metrics.status == "in_progress"
    
    # Test completion
    time.sleep(0.1)  # Small delay to test duration calculation
    metrics.mark_completed(rows_processed=1000, bytes_processed=50000)
    
    assert metrics.status == "completed"
    assert metrics.rows_processed == 1000
    assert metrics.bytes_processed == 50000
    assert metrics.processing_duration_seconds > 0
    assert metrics.get_throughput_rows_per_second() > 0
    assert metrics.get_throughput_mb_per_second() > 0
    
    print("✓ ProcessingMetrics tests passed")

def test_structured_logger():
    """Test StructuredLogger functionality."""
    print("Testing StructuredLogger...")
    
    logger = StructuredLogger("test_job")
    
    # Test context setting
    logger.set_context(table_name="test_table", operation="test")
    
    # Test logging methods (these will output to console)
    logger.info("Test info message", additional_field="value")
    logger.warning("Test warning message")
    logger.error("Test error message", error_code=500)
    
    # Test specialized logging methods
    logger.log_table_processing_start("test_table", "replication")
    
    metrics = ProcessingMetrics("test_table")
    time.sleep(0.1)
    metrics.mark_completed(1000, 50000)
    logger.log_table_processing_complete("test_table", "replication", metrics)
    
    logger.log_job_summary(5, 4, 1, 10000, 120.5)
    
    print("✓ StructuredLogger tests passed")

def test_cloudwatch_metrics_publisher():
    """Test CloudWatchMetricsPublisher functionality."""
    print("Testing CloudWatchMetricsPublisher...")
    
    # Mock CloudWatch client
    with patch('scripts.glue_data_replication.cloudwatch', None):
        publisher = CloudWatchMetricsPublisher("test_job")
        
        # Test metric creation and buffering
        publisher.put_metric("TestMetric", 100, "Count", {"TestDimension": "TestValue"})
        assert len(publisher.metrics_buffer) == 1
        
        # Test job metrics
        publisher.publish_job_start_metrics()
        publisher.publish_job_completion_metrics(True, 120.0, 5, 4, 10000)
        
        # Test table metrics
        metrics = ProcessingMetrics("test_table")
        metrics.mark_completed(1000, 50000)
        publisher.publish_table_metrics("test_table", metrics)
        
        # Test connection metrics
        publisher.publish_connection_metrics("source", "postgresql", True, 2.5)
        
        # Test error metrics
        publisher.publish_error_metrics("connection", "database_test")
        
        print(f"✓ CloudWatchMetricsPublisher tests passed (buffered {len(publisher.metrics_buffer)} metrics)")

def test_performance_monitor():
    """Test PerformanceMonitor functionality."""
    print("Testing PerformanceMonitor...")
    
    monitor = PerformanceMonitor("test_job")
    
    # Test job monitoring
    monitor.start_job_monitoring()
    
    # Test table processing
    table_metrics = monitor.start_table_processing("test_table_1")
    time.sleep(0.1)
    monitor.complete_table_processing("test_table_1", 1000, 50000)
    
    # Test failed table processing
    monitor.start_table_processing("test_table_2")
    monitor.fail_table_processing("test_table_2", "Connection failed")
    
    # Test connection recording
    monitor.record_connection_attempt("source", "postgresql", True, 2.5)
    monitor.record_connection_attempt("target", "oracle", False, 5.0)
    
    # Test error recording
    monitor.record_error("connection", "database_test", "Connection timeout")
    
    # Test summary
    summary = monitor.get_processing_summary()
    assert summary['job_name'] == "test_job"
    assert summary['total_tables'] == 2
    assert summary['successful_tables'] == 1
    assert summary['failed_tables'] == 1
    
    # Test job completion
    monitor.complete_job_monitoring(success=True)
    
    print("✓ PerformanceMonitor tests passed")
    print(f"  - Job summary: {json.dumps(summary, indent=2)}")

def main():
    """Run all tests."""
    print("Starting logging and monitoring tests...\n")
    
    try:
        test_processing_metrics()
        print()
        
        test_structured_logger()
        print()
        
        test_cloudwatch_metrics_publisher()
        print()
        
        test_performance_monitor()
        print()
        
        print("🎉 All tests passed successfully!")
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()