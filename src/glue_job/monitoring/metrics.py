"""
CloudWatch metrics publishing and performance monitoring for AWS Glue Data Replication.

This module provides CloudWatch metrics publishing capabilities, performance monitoring,
and DataFrame size estimation utilities for comprehensive job monitoring.
"""

import json
import boto3
from datetime import datetime, timezone
from typing import Dict, Any, Optional
# Conditional import for PySpark (only available in Glue runtime)
try:
    from pyspark.sql import DataFrame
except ImportError:
    # Mock DataFrame for local development/testing
    class DataFrame:
        pass

from .logging import StructuredLogger
from .progress import ProcessingMetrics


def estimate_dataframe_size(df: DataFrame, sample_size: int = 1000) -> int:
    """Estimate DataFrame size in bytes using sampling."""
    try:
        row_count = df.count()
        if row_count == 0:
            return 0
        
        # Sample rows to estimate average size
        sample_count = min(sample_size, row_count)
        sample_df = df.limit(sample_count)
        sample_rows = sample_df.collect()
        
        if not sample_rows:
            return 0
        
        # Calculate average row size by converting to string representation
        total_sample_size = 0
        for row in sample_rows:
            # Convert row to dictionary and estimate size
            row_dict = row.asDict()
            row_str = json.dumps(row_dict, default=str)
            total_sample_size += len(row_str.encode('utf-8'))
        
        avg_row_size = total_sample_size / len(sample_rows)
        estimated_total_size = int(avg_row_size * row_count)
        
        return estimated_total_size
        
    except Exception:
        # Return 0 if estimation fails
        return 0


class CloudWatchMetricsPublisher:
    """Publishes metrics to CloudWatch for monitoring and alerting."""
    
    def __init__(self, job_name: str, namespace: str = "AWS/Glue/DataReplication"):
        self.job_name = job_name
        self.namespace = namespace
        self.metrics_buffer = []
        self.structured_logger = StructuredLogger(job_name)
        
        # Initialize CloudWatch client
        try:
            self.cloudwatch = boto3.client('cloudwatch')
        except Exception as e:
            self.structured_logger.warning(f"Failed to initialize CloudWatch client: {e}")
            self.cloudwatch = None
    
    def _create_metric_data(self, metric_name: str, value: float, unit: str = 'Count', 
                           dimensions: Dict[str, str] = None) -> Dict:
        """Create CloudWatch metric data structure."""
        base_dimensions = [
            {'Name': 'JobName', 'Value': self.job_name}
        ]
        
        if dimensions:
            for key, val in dimensions.items():
                base_dimensions.append({'Name': key, 'Value': str(val)})
        
        return {
            'MetricName': metric_name,
            'Value': value,
            'Unit': unit,
            'Dimensions': base_dimensions,
            'Timestamp': datetime.now(timezone.utc)
        }
    
    def put_metric(self, metric_name: str, value: float, unit: str = 'Count', 
                  dimensions: Dict[str, str] = None, buffer: bool = True):
        """Put a single metric to CloudWatch."""
        if not self.cloudwatch:
            self.structured_logger.warning("CloudWatch client not available, skipping metric", 
                                         metric_name=metric_name, value=value)
            return
        
        metric_data = self._create_metric_data(metric_name, value, unit, dimensions)
        
        if buffer:
            self.metrics_buffer.append(metric_data)
            self.structured_logger.debug("Buffered metric", metric_name=metric_name, value=value)
        else:
            try:
                self.cloudwatch.put_metric_data(
                    Namespace=self.namespace,
                    MetricData=[metric_data]
                )
                self.structured_logger.debug("Published metric to CloudWatch", 
                                           metric_name=metric_name, value=value)
            except Exception as e:
                self.structured_logger.error("Failed to publish metric to CloudWatch", 
                                           metric_name=metric_name, error=str(e))
    
    def flush_metrics(self):
        """Flush all buffered metrics to CloudWatch."""
        if not self.cloudwatch or not self.metrics_buffer:
            return
        
        # CloudWatch allows max 20 metrics per put_metric_data call
        batch_size = 20
        
        for i in range(0, len(self.metrics_buffer), batch_size):
            batch = self.metrics_buffer[i:i + batch_size]
            
            try:
                self.cloudwatch.put_metric_data(
                    Namespace=self.namespace,
                    MetricData=batch
                )
                self.structured_logger.debug(f"Published {len(batch)} metrics to CloudWatch")
            except Exception as e:
                self.structured_logger.error(f"Failed to publish metrics batch to CloudWatch: {e}")
        
        # Clear buffer after flushing
        self.metrics_buffer.clear()
        self.structured_logger.info(f"Flushed all metrics to CloudWatch")
    
    def publish_job_start_metrics(self):
        """Publish job start metrics."""
        self.put_metric('JobStarted', 1, 'Count')
        self.structured_logger.info("Published job start metrics")
    
    def publish_job_completion_metrics(self, success: bool, duration_seconds: float, 
                                     total_tables: int, successful_tables: int, 
                                     total_rows: int):
        """Publish job completion metrics."""
        # Job completion status
        self.put_metric('JobCompleted', 1 if success else 0, 'Count')
        self.put_metric('JobFailed', 0 if success else 1, 'Count')
        
        # Job duration
        self.put_metric('JobDurationSeconds', duration_seconds, 'Seconds')
        
        # Table processing metrics
        self.put_metric('TotalTables', total_tables, 'Count')
        self.put_metric('SuccessfulTables', successful_tables, 'Count')
        self.put_metric('FailedTables', total_tables - successful_tables, 'Count')
        
        # Success rate
        success_rate = (successful_tables / total_tables * 100) if total_tables > 0 else 0
        self.put_metric('SuccessRate', success_rate, 'Percent')
        
        # Data processing metrics
        self.put_metric('TotalRowsProcessed', total_rows, 'Count')
        
        # Throughput metrics
        if duration_seconds > 0:
            self.put_metric('RowsPerSecond', total_rows / duration_seconds, 'Count/Second')
        
        self.structured_logger.info("Published job completion metrics", 
                                   success=success, duration=duration_seconds, 
                                   total_rows=total_rows)
    
    def publish_table_metrics(self, table_name: str, metrics: ProcessingMetrics):
        """Publish table-specific processing metrics."""
        dimensions = {'TableName': table_name}
        
        # Processing status
        self.put_metric('TableProcessed', 1 if metrics.status == 'completed' else 0, 
                       'Count', dimensions)
        self.put_metric('TableFailed', 1 if metrics.status == 'failed' else 0, 
                       'Count', dimensions)
        
        # Processing duration
        self.put_metric('TableProcessingDurationSeconds', 
                       metrics.processing_duration_seconds, 'Seconds', dimensions)
        
        # Data volume metrics
        if metrics.status == 'completed':
            self.put_metric('TableRowsProcessed', metrics.rows_processed, 'Count', dimensions)
            self.put_metric('TableBytesProcessed', metrics.bytes_processed, 'Bytes', dimensions)
            
            # Throughput metrics
            if metrics.processing_duration_seconds > 0:
                self.put_metric('TableRowsPerSecond', 
                               metrics.get_throughput_rows_per_second(), 
                               'Count/Second', dimensions)
                self.put_metric('TableMBPerSecond', 
                               metrics.get_throughput_mb_per_second(), 
                               'Bytes/Second', dimensions)
        
        self.structured_logger.debug("Published table metrics", table_name=table_name, 
                                   status=metrics.status)
    
    def publish_connection_metrics(self, connection_type: str, engine_type: str, 
                                 success: bool, duration_seconds: float):
        """Publish database connection metrics."""
        dimensions = {
            'ConnectionType': connection_type,  # source or target
            'EngineType': engine_type
        }
        
        self.put_metric('ConnectionAttempt', 1, 'Count', dimensions)
        self.put_metric('ConnectionSuccess', 1 if success else 0, 'Count', dimensions)
        self.put_metric('ConnectionDurationSeconds', duration_seconds, 'Seconds', dimensions)
        
        self.structured_logger.debug("Published connection metrics", 
                                   connection_type=connection_type, 
                                   engine_type=engine_type, success=success)
    
    def publish_error_metrics(self, error_category: str, error_operation: str):
        """Publish error metrics for monitoring and alerting."""
        dimensions = {
            'ErrorCategory': error_category,
            'Operation': error_operation
        }
        
        self.put_metric('ErrorOccurred', 1, 'Count', dimensions)
        
        self.structured_logger.debug("Published error metrics", 
                                   error_category=error_category, 
                                   operation=error_operation)
    
    # S3 Bookmark Operation Metrics (Requirements 6.4, 6.5)
    def publish_s3_bookmark_metrics(self, operation: str, table_name: str, success: bool, 
                                  duration_ms: float, error_category: str = None, 
                                  file_size_bytes: int = None):
        """Publish comprehensive S3 bookmark operation metrics."""
        dimensions = {
            'Operation': operation,  # read, write, delete
            'TableName': table_name
        }
        
        # Success/failure metrics
        if success:
            self.put_metric('BookmarkS3ReadSuccess' if operation == 'read' else 
                          'BookmarkS3WriteSuccess' if operation == 'write' else 
                          'BookmarkS3DeleteSuccess', 1, 'Count', dimensions)
        else:
            failure_dimensions = {**dimensions}
            if error_category:
                failure_dimensions['ErrorCategory'] = error_category
            
            self.put_metric('BookmarkS3ReadFailure' if operation == 'read' else 
                          'BookmarkS3WriteFailure' if operation == 'write' else 
                          'BookmarkS3DeleteFailure', 1, 'Count', dimensions)
        
        # Performance metrics (Requirement 6.3)
        self.put_metric('BookmarkS3OperationLatency', duration_ms, 'Milliseconds', dimensions)
        
        # File size metrics for write operations
        if operation == 'write' and file_size_bytes is not None:
            self.put_metric('BookmarkS3FileSize', file_size_bytes, 'Bytes', dimensions)
        
        self.structured_logger.debug("Published S3 bookmark metrics", 
                                   operation=operation, table_name=table_name, 
                                   success=success, duration_ms=duration_ms)
    
    def publish_s3_bucket_detection_metrics(self, success: bool, detection_duration_ms: float, 
                                          source_bucket: str = None, target_bucket: str = None, 
                                          selected_bucket: str = None):
        """Publish S3 bucket detection metrics."""
        dimensions = {}
        if source_bucket:
            dimensions['SourceBucket'] = source_bucket
        if target_bucket:
            dimensions['TargetBucket'] = target_bucket
        if selected_bucket:
            dimensions['SelectedBucket'] = selected_bucket
        
        # Detection success/failure
        self.put_metric('S3BucketDetectionSuccess', 1 if success else 0, 'Count', dimensions)
        self.put_metric('S3BucketDetectionFailure', 0 if success else 1, 'Count', dimensions)
        
        # Detection performance
        self.put_metric('S3BucketDetectionLatency', detection_duration_ms, 'Milliseconds', dimensions)
        
        self.structured_logger.debug("Published S3 bucket detection metrics", 
                                   success=success, duration_ms=detection_duration_ms)
    
    def publish_s3_bucket_validation_metrics(self, bucket_name: str, success: bool, 
                                           validation_duration_ms: float, error_category: str = None):
        """Publish S3 bucket validation metrics."""
        dimensions = {'BucketName': bucket_name}
        if error_category:
            dimensions['ErrorCategory'] = error_category
        
        # Validation success/failure
        self.put_metric('S3BucketValidationSuccess', 1 if success else 0, 'Count', dimensions)
        self.put_metric('S3BucketValidationFailure', 0 if success else 1, 'Count', dimensions)
        
        # Validation performance
        self.put_metric('S3BucketValidationLatency', validation_duration_ms, 'Milliseconds', dimensions)
        
        self.structured_logger.debug("Published S3 bucket validation metrics", 
                                   bucket_name=bucket_name, success=success, 
                                   duration_ms=validation_duration_ms)
    
    def publish_bookmark_fallback_metrics(self, table_name: str, fallback_reason: str, 
                                        error_category: str):
        """Publish metrics when falling back to in-memory bookmarks."""
        dimensions = {
            'TableName': table_name,
            'FallbackReason': fallback_reason,
            'ErrorCategory': error_category
        }
        
        self.put_metric('BookmarkFallbackToMemory', 1, 'Count', dimensions)
        
        self.structured_logger.debug("Published bookmark fallback metrics", 
                                   table_name=table_name, fallback_reason=fallback_reason)
    
    def publish_bookmark_corruption_metrics(self, table_name: str, corruption_type: str, 
                                          cleanup_success: bool):
        """Publish metrics for bookmark corruption detection and cleanup."""
        dimensions = {
            'TableName': table_name,
            'CorruptionType': corruption_type
        }
        
        # Corruption detection
        self.put_metric('BookmarkCorruptionDetected', 1, 'Count', dimensions)
        
        # Cleanup success/failure
        self.put_metric('BookmarkCorruptionCleanupSuccess', 1 if cleanup_success else 0, 
                       'Count', dimensions)
        self.put_metric('BookmarkCorruptionCleanupFailure', 0 if cleanup_success else 1, 
                       'Count', dimensions)
        
        self.structured_logger.debug("Published bookmark corruption metrics", 
                                   table_name=table_name, corruption_type=corruption_type, 
                                   cleanup_success=cleanup_success)
    
    def publish_migration_progress_metrics(self, table_name: str, load_type: str,
                                          rows_processed: int, total_rows: Optional[int],
                                          rows_per_second: float, progress_percentage: float):
        """
        Publish real-time migration progress metrics.
        
        Args:
            table_name: Name of the table being migrated
            load_type: Type of load operation ('full' or 'incremental')
            rows_processed: Number of rows processed so far
            total_rows: Total rows (if known)
            rows_per_second: Current processing rate
            progress_percentage: Progress percentage (0-100)
        """
        dimensions = {
            'TableName': table_name,
            'LoadType': load_type
        }
        
        # Progress metrics
        self.put_metric('MigrationRowsProcessed', rows_processed, 'Count', dimensions)
        self.put_metric('MigrationRowsPerSecond', rows_per_second, 'Count/Second', dimensions)
        self.put_metric('MigrationProgressPercentage', progress_percentage, 'Percent', dimensions)
        
        if total_rows is not None:
            self.put_metric('MigrationTotalRows', total_rows, 'Count', dimensions)
        
        self.structured_logger.debug("Published migration progress metrics", 
                                   table_name=table_name, load_type=load_type,
                                   rows_processed=rows_processed, 
                                   rows_per_second=round(rows_per_second, 2),
                                   progress_percentage=round(progress_percentage, 2))
    
    def publish_counting_strategy_metrics(self, table_name: str, strategy_type: str,
                                         count_duration_seconds: float, row_count: int):
        """
        Publish metrics about counting strategy execution.
        
        Args:
            table_name: Name of the table
            strategy_type: Type of strategy used (immediate/deferred)
            count_duration_seconds: Time taken to count
            row_count: Number of rows counted
        """
        dimensions = {
            'TableName': table_name,
            'StrategyType': strategy_type
        }
        
        # Strategy execution metrics
        self.put_metric('CountingStrategyUsed', 1, 'Count', dimensions)
        self.put_metric('CountingDurationSeconds', count_duration_seconds, 'Seconds', dimensions)
        self.put_metric('CountingRowCount', row_count, 'Count', dimensions)
        
        self.structured_logger.debug("Published counting strategy metrics", 
                                   table_name=table_name, 
                                   strategy_type=strategy_type,
                                   count_duration_seconds=round(count_duration_seconds, 2),
                                   row_count=row_count)
    
    def publish_migration_phase_metrics(self, table_name: str, phase: str, load_type: str,
                                       duration_seconds: float, rows_count: Optional[int] = None,
                                       migration_status: Optional[str] = None):
        """
        Publish metrics for individual migration phases.
        
        Args:
            table_name: Name of the table
            phase: Phase name (read, write, count)
            load_type: Type of load operation ('full' or 'incremental')
            duration_seconds: Phase duration
            rows_count: Number of rows (if applicable)
            migration_status: Migration status (success, failed, in_progress) for status metrics
        """
        dimensions = {
            'TableName': table_name,
            'Phase': phase,
            'LoadType': load_type
        }
        
        # Phase duration metrics
        if phase == 'read':
            self.put_metric('MigrationReadDuration', duration_seconds, 'Seconds', dimensions)
        elif phase == 'write':
            self.put_metric('MigrationWriteDuration', duration_seconds, 'Seconds', dimensions)
        elif phase == 'count':
            self.put_metric('MigrationCountDuration', duration_seconds, 'Seconds', dimensions)
        
        # Row count for the phase if provided
        if rows_count is not None:
            self.put_metric('MigrationPhaseRowCount', rows_count, 'Count', dimensions)
        
        # Migration status metric per table with load_type dimension
        if migration_status is not None:
            status_dimensions = {
                'TableName': table_name,
                'LoadType': load_type
            }
            
            # Publish status as numeric values for CloudWatch alarms
            status_value = 1 if migration_status == 'success' else 0 if migration_status == 'failed' else 0.5
            self.put_metric('MigrationStatus', status_value, 'None', status_dimensions)
            
            # Also publish individual status metrics for easier filtering
            if migration_status == 'success':
                self.put_metric('MigrationSuccess', 1, 'Count', status_dimensions)
            elif migration_status == 'failed':
                self.put_metric('MigrationFailed', 1, 'Count', status_dimensions)
            elif migration_status == 'in_progress':
                self.put_metric('MigrationInProgress', 1, 'Count', status_dimensions)
        
        self.structured_logger.debug("Published migration phase metrics", 
                                   table_name=table_name, 
                                   phase=phase,
                                   load_type=load_type,
                                   duration_seconds=round(duration_seconds, 2),
                                   rows_count=rows_count,
                                   migration_status=migration_status)
    
    def publish_incremental_load_metrics(self, table_name: str, delta_rows: int,
                                        incremental_column: str, bookmark_update_status: str,
                                        processing_rate: float, duration_seconds: float):
        """
        Publish metrics specific to incremental load operations.
        
        Args:
            table_name: Name of the table
            delta_rows: Number of delta rows processed in incremental load
            incremental_column: Name of the incremental column used for tracking
            bookmark_update_status: Status of bookmark update (success, failed)
            processing_rate: Processing rate in rows per second
            duration_seconds: Total duration of incremental load
        
        Requirements:
            - 8.2: Publish CloudWatch Metrics for incremental load operations
            - 8.4: Publish final metrics including delta rows and bookmark update status
            - 8.5: Include load_type='incremental' dimension in all metrics
        """
        # Base dimensions with load_type and incremental_column
        dimensions = {
            'TableName': table_name,
            'LoadType': 'incremental',
            'IncrementalColumn': incremental_column
        }
        
        # Requirement 8.4: Publish delta_rows metric for incremental loads
        self.put_metric('IncrementalDeltaRows', delta_rows, 'Count', dimensions)
        
        # Requirement 8.4: Publish bookmark_update_status metric
        # Use numeric values for easier alarming: 1 = success, 0 = failed
        bookmark_status_value = 1 if bookmark_update_status == 'success' else 0
        self.put_metric('IncrementalBookmarkUpdateStatus', bookmark_status_value, 'None', dimensions)
        
        # Also publish named status metrics for easier filtering
        if bookmark_update_status == 'success':
            self.put_metric('IncrementalBookmarkUpdateSuccess', 1, 'Count', dimensions)
        else:
            self.put_metric('IncrementalBookmarkUpdateFailure', 1, 'Count', dimensions)
        
        # Requirement 8.2: Publish processing rate for incremental loads
        self.put_metric('IncrementalProcessingRate', processing_rate, 'Count/Second', dimensions)
        
        # Requirement 8.2: Publish duration for incremental loads
        self.put_metric('IncrementalLoadDuration', duration_seconds, 'Seconds', dimensions)
        
        self.structured_logger.debug("Published incremental load metrics", 
                                   table_name=table_name,
                                   delta_rows=delta_rows,
                                   incremental_column=incremental_column,
                                   bookmark_update_status=bookmark_update_status,
                                   processing_rate=round(processing_rate, 2),
                                   duration_seconds=round(duration_seconds, 2))


class PerformanceMonitor:
    """Monitors and tracks performance metrics during job execution."""
    
    def __init__(self, job_name: str):
        self.job_name = job_name
        self.structured_logger = StructuredLogger(job_name)
        self.metrics_publisher = CloudWatchMetricsPublisher(job_name)
        self.job_start_time = datetime.now(timezone.utc)
        self.table_metrics: Dict[str, ProcessingMetrics] = {}
        self.connection_metrics = []
    
    def start_job_monitoring(self):
        """Start job-level monitoring."""
        self.job_start_time = datetime.now(timezone.utc)
        self.metrics_publisher.publish_job_start_metrics()
        self.structured_logger.info("Started job performance monitoring", 
                                  start_time=self.job_start_time.isoformat())
    
    def start_table_processing(self, table_name: str) -> ProcessingMetrics:
        """Start monitoring for a specific table."""
        metrics = ProcessingMetrics(table_name=table_name)
        self.table_metrics[table_name] = metrics
        
        self.structured_logger.log_table_processing_start(table_name, "data_replication")
        return metrics
    
    def complete_table_processing(self, table_name: str, rows_processed: int = 0, 
                                bytes_processed: int = 0):
        """Complete monitoring for a specific table."""
        if table_name not in self.table_metrics:
            self.structured_logger.warning("Table metrics not found", table_name=table_name)
            return
        
        metrics = self.table_metrics[table_name]
        metrics.mark_completed(rows_processed, bytes_processed)
        
        # Log completion
        self.structured_logger.log_table_processing_complete(table_name, "data_replication", metrics)
        
        # Publish metrics
        self.metrics_publisher.publish_table_metrics(table_name, metrics)
    
    def fail_table_processing(self, table_name: str, error_message: str):
        """Mark table processing as failed."""
        if table_name not in self.table_metrics:
            metrics = ProcessingMetrics(table_name=table_name)
            self.table_metrics[table_name] = metrics
        
        metrics = self.table_metrics[table_name]
        metrics.mark_failed(error_message)
        
        # Log failure
        self.structured_logger.log_table_processing_failed(table_name, "data_replication", 
                                                          error_message, metrics)
        
        # Publish metrics
        self.metrics_publisher.publish_table_metrics(table_name, metrics)
    
    def record_connection_attempt(self, connection_type: str, engine_type: str, 
                                success: bool, duration_seconds: float):
        """Record database connection attempt."""
        self.connection_metrics.append({
            'connection_type': connection_type,
            'engine_type': engine_type,
            'success': success,
            'duration_seconds': duration_seconds,
            'timestamp': datetime.now(timezone.utc)
        })
        
        # Publish connection metrics
        self.metrics_publisher.publish_connection_metrics(connection_type, engine_type, 
                                                         success, duration_seconds)
        
        self.structured_logger.info("Recorded connection attempt", 
                                  connection_type=connection_type, 
                                  engine_type=engine_type, 
                                  success=success, 
                                  duration_seconds=round(duration_seconds, 2))
    
    def record_error(self, error_category: str, operation: str, error_message: str):
        """Record error occurrence for monitoring."""
        self.metrics_publisher.publish_error_metrics(error_category, operation)
        
        self.structured_logger.error("Error recorded for monitoring", 
                                   error_category=error_category, 
                                   operation=operation, 
                                   error_message=error_message)
    
    def complete_job_monitoring(self, success: bool = True):
        """Complete job-level monitoring and publish final metrics."""
        job_end_time = datetime.now(timezone.utc)
        job_duration = (job_end_time - self.job_start_time).total_seconds()
        
        # Calculate summary statistics
        total_tables = len(self.table_metrics)
        successful_tables = sum(1 for m in self.table_metrics.values() if m.status == 'completed')
        failed_tables = total_tables - successful_tables
        total_rows = sum(m.rows_processed for m in self.table_metrics.values() if m.status == 'completed')
        
        # Log job summary
        self.structured_logger.log_job_summary(total_tables, successful_tables, failed_tables, 
                                             total_rows, job_duration)
        
        # Publish job completion metrics
        self.metrics_publisher.publish_job_completion_metrics(success, job_duration, 
                                                             total_tables, successful_tables, 
                                                             total_rows)
        
        # Flush all buffered metrics
        self.metrics_publisher.flush_metrics()
        
        self.structured_logger.info("Completed job performance monitoring", 
                                  duration_seconds=round(job_duration, 2), 
                                  success=success)
    
    def get_processing_summary(self) -> Dict[str, Any]:
        """Get summary of processing metrics."""
        total_tables = len(self.table_metrics)
        successful_tables = sum(1 for m in self.table_metrics.values() if m.status == 'completed')
        total_rows = sum(m.rows_processed for m in self.table_metrics.values() if m.status == 'completed')
        total_bytes = sum(m.bytes_processed for m in self.table_metrics.values() if m.status == 'completed')
        
        job_duration = (datetime.now(timezone.utc) - self.job_start_time).total_seconds()
        
        return {
            'job_name': self.job_name,
            'job_duration_seconds': job_duration,
            'total_tables': total_tables,
            'successful_tables': successful_tables,
            'failed_tables': total_tables - successful_tables,
            'success_rate': (successful_tables / total_tables * 100) if total_tables > 0 else 0,
            'total_rows_processed': total_rows,
            'total_bytes_processed': total_bytes,
            'average_throughput_rows_per_sec': total_rows / job_duration if job_duration > 0 else 0,
            'table_metrics': {name: {
                'status': metrics.status,
                'rows_processed': metrics.rows_processed,
                'duration_seconds': metrics.processing_duration_seconds,
                'throughput_rows_per_sec': metrics.get_throughput_rows_per_second()
            } for name, metrics in self.table_metrics.items()}
        }