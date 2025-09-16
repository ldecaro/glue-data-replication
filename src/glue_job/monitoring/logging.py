"""
Structured logging module for AWS Glue Data Replication.

This module provides enhanced logging capabilities with contextual information,
structured data, and specialized logging methods for S3 operations, bookmark
management, and job execution tracking.
"""

import logging
from datetime import datetime, timezone
from typing import Any, Optional


class StructuredLogger:
    """Enhanced logger with structured logging and contextual information."""
    
    def __init__(self, job_name: str, logger_instance: logging.Logger = None):
        self.job_name = job_name
        self.logger = logger_instance or logging.getLogger(__name__)
        self.context = {"job_name": job_name}
    
    def _format_message(self, message: str, **kwargs) -> str:
        """Format message with context and additional fields."""
        context_data = {**self.context, **kwargs}
        if context_data:
            context_str = " | ".join([f"{k}={v}" for k, v in context_data.items()])
            return f"{message} | {context_str}"
        return message
    
    def set_context(self, **kwargs):
        """Set additional context for all subsequent log messages."""
        self.context.update(kwargs)
    
    def clear_context(self, *keys):
        """Clear specific context keys."""
        for key in keys:
            self.context.pop(key, None)
    
    def info(self, message: str, **kwargs):
        """Log info message with context."""
        self.logger.info(self._format_message(message, **kwargs))
    
    def warning(self, message: str, **kwargs):
        """Log warning message with context."""
        self.logger.warning(self._format_message(message, **kwargs))
    
    def error(self, message: str, **kwargs):
        """Log error message with context."""
        self.logger.error(self._format_message(message, **kwargs))
    
    def debug(self, message: str, **kwargs):
        """Log debug message with context."""
        self.logger.debug(self._format_message(message, **kwargs))
    
    def critical(self, message: str, **kwargs):
        """Log critical message with context."""
        self.logger.critical(self._format_message(message, **kwargs))
    
    # S3 Operation Logging Methods (Requirement 6.1)
    def log_s3_operation_start(self, operation: str, table_name: str, s3_key: str, **kwargs):
        """Log start of S3 bookmark operation with structured data."""
        self.info(
            f"Starting S3 {operation} operation",
            operation=operation,
            table_name=table_name,
            s3_key=s3_key,
            timestamp=datetime.now(timezone.utc).isoformat(),
            **kwargs
        )
    
    def log_s3_operation_success(self, operation: str, table_name: str, s3_key: str, 
                               duration_ms: float, **kwargs):
        """Log successful S3 bookmark operation with performance metrics."""
        self.info(
            f"S3 {operation} operation completed successfully",
            operation=operation,
            table_name=table_name,
            s3_key=s3_key,
            duration_ms=round(duration_ms, 2),
            status="success",
            timestamp=datetime.now(timezone.utc).isoformat(),
            **kwargs
        )
    
    def log_s3_operation_failure(self, operation: str, table_name: str, s3_key: str, 
                               duration_ms: float, error: str, error_category: str, **kwargs):
        """Log failed S3 bookmark operation with error details."""
        self.error(
            f"S3 {operation} operation failed",
            operation=operation,
            table_name=table_name,
            s3_key=s3_key,
            duration_ms=round(duration_ms, 2),
            status="failure",
            error=error,
            error_category=error_category,
            timestamp=datetime.now(timezone.utc).isoformat(),
            **kwargs
        )
    
    def log_s3_operation_retry(self, operation: str, table_name: str, s3_key: str, 
                             attempt: int, wait_seconds: float, error_category: str):
        """Log S3 operation retry attempt."""
        self.warning(
            f"Retrying S3 {operation} operation",
            operation=operation,
            table_name=table_name,
            s3_key=s3_key,
            attempt=attempt,
            wait_seconds=wait_seconds,
            error_category=error_category,
            timestamp=datetime.now(timezone.utc).isoformat()
        )
    
    # S3 Bucket Detection and Validation Logging (Requirement 6.2)
    def log_s3_bucket_detection_start(self, source_jdbc_path: str, target_jdbc_path: str):
        """Log start of S3 bucket detection process."""
        self.info(
            "Starting S3 bucket detection from JDBC paths",
            source_jdbc_path=source_jdbc_path,
            target_jdbc_path=target_jdbc_path,
            timestamp=datetime.now(timezone.utc).isoformat()
        )
    
    def log_s3_bucket_detection_success(self, detected_bucket: str, source_bucket: str, 
                                      target_bucket: str, selection_reason: str):
        """Log successful S3 bucket detection."""
        self.info(
            "S3 bucket detection completed successfully",
            detected_bucket=detected_bucket,
            source_bucket=source_bucket,
            target_bucket=target_bucket,
            selection_reason=selection_reason,
            timestamp=datetime.now(timezone.utc).isoformat()
        )
    
    def log_s3_bucket_validation_start(self, bucket_name: str):
        """Log start of S3 bucket validation."""
        self.info(
            "Starting S3 bucket validation",
            bucket_name=bucket_name,
            timestamp=datetime.now(timezone.utc).isoformat()
        )
    
    def log_s3_bucket_validation_success(self, bucket_name: str, validation_duration_ms: float):
        """Log successful S3 bucket validation."""
        self.info(
            "S3 bucket validation completed successfully",
            bucket_name=bucket_name,
            validation_duration_ms=round(validation_duration_ms, 2),
            status="accessible",
            timestamp=datetime.now(timezone.utc).isoformat()
        )
    
    def log_s3_bucket_validation_failure(self, bucket_name: str, validation_duration_ms: float, 
                                       error: str, fallback_action: str):
        """Log failed S3 bucket validation."""
        self.error(
            "S3 bucket validation failed",
            bucket_name=bucket_name,
            validation_duration_ms=round(validation_duration_ms, 2),
            status="inaccessible",
            error=error,
            fallback_action=fallback_action,
            timestamp=datetime.now(timezone.utc).isoformat()
        )
    
    # Bookmark State Logging (Requirement 6.2)
    def log_bookmark_state_loaded(self, table_name: str, last_processed_value: Any, 
                                 last_update_timestamp: str, is_first_run: bool):
        """Log bookmark state loaded from S3."""
        self.info(
            "Bookmark state loaded from S3",
            table_name=table_name,
            last_processed_value=str(last_processed_value) if last_processed_value is not None else None,
            last_update_timestamp=last_update_timestamp,
            is_first_run=is_first_run,
            timestamp=datetime.now(timezone.utc).isoformat()
        )
    
    def log_bookmark_state_saved(self, table_name: str, new_processed_value: Any, 
                               file_size_bytes: int, bookmark_version: str):
        """Log bookmark state saved to S3."""
        self.info(
            "Bookmark state saved to S3",
            table_name=table_name,
            new_processed_value=str(new_processed_value) if new_processed_value is not None else None,
            file_size_bytes=file_size_bytes,
            bookmark_version=bookmark_version,
            timestamp=datetime.now(timezone.utc).isoformat()
        )
    
    # Fallback and Error Recovery Logging
    def log_fallback_to_memory(self, table_name: str, reason: str, error_category: str):
        """Log fallback to in-memory bookmarks."""
        self.warning(
            "Falling back to in-memory bookmarks",
            table_name=table_name,
            reason=reason,
            error_category=error_category,
            fallback_type="in_memory",
            timestamp=datetime.now(timezone.utc).isoformat()
        )
    
    def log_corrupted_bookmark_cleanup(self, table_name: str, s3_key: str, 
                                     cleanup_success: bool, cleanup_error: str = None):
        """Log cleanup of corrupted bookmark files."""
        if cleanup_success:
            self.info(
                "Corrupted bookmark file cleaned up successfully",
                table_name=table_name,
                s3_key=s3_key,
                cleanup_status="success",
                next_action="full_load_will_be_performed",
                timestamp=datetime.now(timezone.utc).isoformat()
            )
        else:
            self.error(
                "Failed to clean up corrupted bookmark file",
                table_name=table_name,
                s3_key=s3_key,
                cleanup_status="failure",
                cleanup_error=cleanup_error,
                next_action="full_load_will_still_be_performed",
                timestamp=datetime.now(timezone.utc).isoformat()
            )
    
    def log_table_processing_start(self, table_name: str, operation: str):
        """Log start of table processing."""
        self.info(
            f"Starting {operation} for table",
            table_name=table_name,
            operation=operation,
            timestamp=datetime.now(timezone.utc).isoformat()
        )
    
    def log_table_processing_complete(self, table_name: str, operation: str, metrics):
        """Log completion of table processing with metrics."""
        self.info(
            f"Completed {operation} for table",
            table_name=table_name,
            operation=operation,
            rows_processed=metrics.rows_processed,
            duration_seconds=round(metrics.processing_duration_seconds, 2),
            throughput_rows_per_sec=round(metrics.get_throughput_rows_per_second(), 2),
            throughput_mb_per_sec=round(metrics.get_throughput_mb_per_second(), 2),
            bytes_processed=metrics.bytes_processed,
            status=metrics.status
        )
    
    def log_table_processing_failed(self, table_name: str, operation: str, error: str, metrics):
        """Log failure of table processing."""
        self.error(
            f"Failed {operation} for table",
            table_name=table_name,
            operation=operation,
            error=error,
            duration_seconds=round(metrics.processing_duration_seconds, 2),
            status=metrics.status
        )
    
    def log_job_summary(self, total_tables: int, successful_tables: int, failed_tables: int, 
                       total_rows: int, total_duration: float):
        """Log job execution summary."""
        self.info(
            "Job execution summary",
            total_tables=total_tables,
            successful_tables=successful_tables,
            failed_tables=failed_tables,
            success_rate=round((successful_tables / total_tables * 100), 2) if total_tables > 0 else 0,
            total_rows_processed=total_rows,
            total_duration_seconds=round(total_duration, 2),
            average_throughput_rows_per_sec=round(total_rows / total_duration, 2) if total_duration > 0 else 0
        )
    
    # Parallel and Batch Operation Logging (for future performance optimizations)
    def log_parallel_s3_operation_start(self, operation: str, table_count: int, **kwargs):
        """Log start of parallel S3 bookmark operations."""
        self.info(
            f"Starting parallel S3 {operation} operations",
            operation=operation,
            table_count=table_count,
            parallel_execution=True,
            timestamp=datetime.now(timezone.utc).isoformat(),
            **kwargs
        )
    
    def log_parallel_s3_operation_complete(self, operation: str, table_count: int, 
                                         successful_count: int, failed_count: int, 
                                         total_duration_ms: float, **kwargs):
        """Log completion of parallel S3 bookmark operations."""
        self.info(
            f"Completed parallel S3 {operation} operations",
            operation=operation,
            table_count=table_count,
            successful_count=successful_count,
            failed_count=failed_count,
            success_rate=round((successful_count / table_count * 100), 2) if table_count > 0 else 0,
            total_duration_ms=round(total_duration_ms, 2),
            average_duration_per_table_ms=round(total_duration_ms / table_count, 2) if table_count > 0 else 0,
            parallel_execution=True,
            timestamp=datetime.now(timezone.utc).isoformat(),
            **kwargs
        )
    
    def log_batch_s3_operation_start(self, operation: str, batch_size: int, total_operations: int, **kwargs):
        """Log start of batch S3 bookmark operations."""
        self.info(
            f"Starting batch S3 {operation} operations",
            operation=operation,
            batch_size=batch_size,
            total_operations=total_operations,
            batch_count=round(total_operations / batch_size) if batch_size > 0 else 0,
            batch_execution=True,
            timestamp=datetime.now(timezone.utc).isoformat(),
            **kwargs
        )
    
    def log_batch_s3_operation_complete(self, operation: str, batch_number: int, 
                                      batch_size: int, successful_count: int, 
                                      failed_count: int, batch_duration_ms: float, **kwargs):
        """Log completion of a single batch S3 operation."""
        self.info(
            f"Completed batch {batch_number} of S3 {operation} operations",
            operation=operation,
            batch_number=batch_number,
            batch_size=batch_size,
            successful_count=successful_count,
            failed_count=failed_count,
            batch_success_rate=round((successful_count / batch_size * 100), 2) if batch_size > 0 else 0,
            batch_duration_ms=round(batch_duration_ms, 2),
            average_operation_duration_ms=round(batch_duration_ms / batch_size, 2) if batch_size > 0 else 0,
            batch_execution=True,
            timestamp=datetime.now(timezone.utc).isoformat(),
            **kwargs
        )
    
    # S3 Performance and Resource Usage Logging
    def log_s3_operation_performance_summary(self, operation: str, total_operations: int, 
                                           total_duration_ms: float, total_bytes: int, 
                                           success_count: int, failure_count: int):
        """Log comprehensive performance summary for S3 operations."""
        self.info(
            f"S3 {operation} performance summary",
            operation=operation,
            total_operations=total_operations,
            success_count=success_count,
            failure_count=failure_count,
            success_rate=round((success_count / total_operations * 100), 2) if total_operations > 0 else 0,
            total_duration_ms=round(total_duration_ms, 2),
            total_duration_seconds=round(total_duration_ms / 1000, 2),
            average_duration_ms=round(total_duration_ms / total_operations, 2) if total_operations > 0 else 0,
            total_bytes=total_bytes,
            total_mb=round(total_bytes / 1024 / 1024, 2),
            throughput_mb_per_sec=round((total_bytes / 1024 / 1024) / (total_duration_ms / 1000), 2) if total_duration_ms > 0 else 0,
            operations_per_sec=round(total_operations / (total_duration_ms / 1000), 2) if total_duration_ms > 0 else 0,
            timestamp=datetime.now(timezone.utc).isoformat()
        )
    
    def log_s3_resource_usage(self, operation: str, table_name: str, file_size_bytes: int, 
                            request_count: int, retry_count: int = 0):
        """Log S3 resource usage for cost and performance monitoring."""
        self.info(
            f"S3 resource usage for {operation}",
            operation=operation,
            table_name=table_name,
            file_size_bytes=file_size_bytes,
            file_size_kb=round(file_size_bytes / 1024, 2),
            file_size_mb=round(file_size_bytes / 1024 / 1024, 4),
            request_count=request_count,
            retry_count=retry_count,
            total_requests=request_count + retry_count,
            storage_class="STANDARD",
            estimated_cost_usd=round((file_size_bytes / 1024 / 1024 / 1024) * 0.023, 6),  # Rough S3 standard storage cost
            timestamp=datetime.now(timezone.utc).isoformat()
        )
    
    def log_s3_operation_trend(self, operation: str, current_duration_ms: float, 
                             historical_average_ms: float, table_count: int, 
                             trend_direction: str = "stable"):
        """Log S3 operation performance trends for capacity planning."""
        performance_change = 0.0
        if historical_average_ms > 0:
            performance_change = ((current_duration_ms - historical_average_ms) / historical_average_ms) * 100
        
        self.info(
            f"S3 {operation} performance trend analysis",
            operation=operation,
            current_duration_ms=round(current_duration_ms, 2),
            historical_average_ms=round(historical_average_ms, 2),
            performance_change_percent=round(performance_change, 2),
            trend_direction=trend_direction,
            table_count=table_count,
            performance_status="improving" if performance_change < -5 else "degrading" if performance_change > 5 else "stable",
            timestamp=datetime.now(timezone.utc).isoformat()
        )
    
    # Manual Bookmark Configuration Logging Methods (Task 23 - Requirements 8.8, 8.6, 8.7)
    def log_manual_config_parsing_start(self, config_json: str):
        """Log start of manual bookmark configuration parsing."""
        self.info(
            "Starting manual bookmark configuration parsing",
            config_length=len(config_json) if config_json else 0,
            has_config=bool(config_json),
            timestamp=datetime.now(timezone.utc).isoformat()
        )
    
    def log_manual_config_parsing_success(self, config_count: int, table_names: list, 
                                        parsing_duration_ms: float):
        """Log successful manual bookmark configuration parsing."""
        self.info(
            "Manual bookmark configuration parsed successfully",
            config_count=config_count,
            table_names=table_names,
            parsing_duration_ms=round(parsing_duration_ms, 2),
            status="success",
            timestamp=datetime.now(timezone.utc).isoformat()
        )
    
    def log_manual_config_parsing_failure(self, error: str, config_json: str, 
                                        parsing_duration_ms: float, fallback_action: str):
        """Log failed manual bookmark configuration parsing."""
        self.error(
            "Manual bookmark configuration parsing failed",
            error=error,
            config_length=len(config_json) if config_json else 0,
            parsing_duration_ms=round(parsing_duration_ms, 2),
            status="failure",
            fallback_action=fallback_action,
            timestamp=datetime.now(timezone.utc).isoformat()
        )
    
    def log_manual_config_validation_start(self, table_name: str, column_name: str):
        """Log start of manual configuration validation for a specific table."""
        self.info(
            "Starting manual configuration validation",
            table_name=table_name,
            column_name=column_name,
            validation_type="manual_config",
            timestamp=datetime.now(timezone.utc).isoformat()
        )
    
    def log_manual_config_validation_success(self, table_name: str, column_name: str, 
                                           validation_duration_ms: float):
        """Log successful manual configuration validation."""
        self.info(
            "Manual configuration validation successful",
            table_name=table_name,
            column_name=column_name,
            validation_duration_ms=round(validation_duration_ms, 2),
            validation_status="valid",
            timestamp=datetime.now(timezone.utc).isoformat()
        )
    
    def log_manual_config_validation_failure(self, table_name: str, column_name: str, 
                                           error: str, validation_duration_ms: float, 
                                           fallback_action: str):
        """Log failed manual configuration validation."""
        self.error(
            "Manual configuration validation failed",
            table_name=table_name,
            column_name=column_name,
            error=error,
            validation_duration_ms=round(validation_duration_ms, 2),
            validation_status="invalid",
            fallback_action=fallback_action,
            timestamp=datetime.now(timezone.utc).isoformat()
        )
    
    def log_bookmark_strategy_resolution_start(self, table_name: str, has_manual_config: bool):
        """Log start of bookmark strategy resolution for a table."""
        self.info(
            "Starting bookmark strategy resolution",
            table_name=table_name,
            has_manual_config=has_manual_config,
            resolution_method="manual" if has_manual_config else "automatic",
            timestamp=datetime.now(timezone.utc).isoformat()
        )
    
    def log_bookmark_strategy_resolution_success(self, table_name: str, strategy: str, 
                                               column_name: Optional[str], is_manual: bool, 
                                               resolution_duration_ms: float, 
                                               data_type: Optional[str] = None):
        """Log successful bookmark strategy resolution."""
        self.info(
            "Bookmark strategy resolved successfully",
            table_name=table_name,
            strategy=strategy,
            column_name=column_name,
            is_manually_configured=is_manual,
            resolution_method="manual" if is_manual else "automatic",
            column_data_type=data_type,
            resolution_duration_ms=round(resolution_duration_ms, 2),
            status="success",
            timestamp=datetime.now(timezone.utc).isoformat()
        )
    
    def log_bookmark_strategy_resolution_fallback(self, table_name: str, original_method: str, 
                                                 fallback_method: str, reason: str, 
                                                 fallback_strategy: str, fallback_column: Optional[str]):
        """Log fallback from manual to automatic bookmark strategy resolution."""
        self.warning(
            "Bookmark strategy resolution fallback",
            table_name=table_name,
            original_method=original_method,
            fallback_method=fallback_method,
            fallback_reason=reason,
            fallback_strategy=fallback_strategy,
            fallback_column=fallback_column,
            timestamp=datetime.now(timezone.utc).isoformat()
        )
    
    def log_jdbc_metadata_query_start(self, table_name: str, column_name: str, query_type: str):
        """Log start of JDBC metadata query."""
        self.info(
            "Starting JDBC metadata query",
            table_name=table_name,
            column_name=column_name,
            query_type=query_type,
            timestamp=datetime.now(timezone.utc).isoformat()
        )
    
    def log_jdbc_metadata_query_success(self, table_name: str, column_name: str, 
                                      data_type: str, jdbc_type: int, 
                                      query_duration_ms: float, cached: bool = False):
        """Log successful JDBC metadata query."""
        self.info(
            "JDBC metadata query completed successfully",
            table_name=table_name,
            column_name=column_name,
            data_type=data_type,
            jdbc_type=jdbc_type,
            query_duration_ms=round(query_duration_ms, 2),
            cached_result=cached,
            status="success",
            timestamp=datetime.now(timezone.utc).isoformat()
        )
    
    def log_jdbc_metadata_query_failure(self, table_name: str, column_name: str, 
                                      error: str, query_duration_ms: float, 
                                      fallback_action: str):
        """Log failed JDBC metadata query."""
        self.error(
            "JDBC metadata query failed",
            table_name=table_name,
            column_name=column_name,
            error=error,
            query_duration_ms=round(query_duration_ms, 2),
            status="failure",
            fallback_action=fallback_action,
            timestamp=datetime.now(timezone.utc).isoformat()
        )
    
    def log_jdbc_metadata_cache_hit(self, table_name: str, column_name: str, data_type: str):
        """Log JDBC metadata cache hit."""
        self.debug(
            "JDBC metadata cache hit",
            table_name=table_name,
            column_name=column_name,
            data_type=data_type,
            cache_hit=True,
            timestamp=datetime.now(timezone.utc).isoformat()
        )
    
    def log_data_type_mapping_start(self, jdbc_data_type: str, table_name: str, column_name: str):
        """Log start of JDBC data type to strategy mapping."""
        self.debug(
            "Starting data type to strategy mapping",
            jdbc_data_type=jdbc_data_type,
            table_name=table_name,
            column_name=column_name,
            timestamp=datetime.now(timezone.utc).isoformat()
        )
    
    def log_data_type_mapping_success(self, jdbc_data_type: str, strategy: str, 
                                    table_name: str, column_name: str, 
                                    mapping_type: str = "direct"):
        """Log successful data type to strategy mapping."""
        self.info(
            "Data type mapped to bookmark strategy",
            jdbc_data_type=jdbc_data_type,
            bookmark_strategy=strategy,
            table_name=table_name,
            column_name=column_name,
            mapping_type=mapping_type,
            timestamp=datetime.now(timezone.utc).isoformat()
        )
    
    def log_data_type_mapping_fallback(self, jdbc_data_type: str, fallback_strategy: str, 
                                     table_name: str, column_name: str, reason: str):
        """Log fallback data type to strategy mapping for unknown types."""
        self.warning(
            "Data type mapping fallback to default strategy",
            jdbc_data_type=jdbc_data_type,
            fallback_strategy=fallback_strategy,
            table_name=table_name,
            column_name=column_name,
            fallback_reason=reason,
            timestamp=datetime.now(timezone.utc).isoformat()
        )
    
    def log_bookmark_detection_summary(self, total_tables: int, manual_count: int, 
                                     automatic_count: int, failed_count: int, 
                                     strategy_distribution: dict):
        """Log summary of bookmark detection across all tables."""
        self.info(
            "Bookmark detection summary",
            total_tables=total_tables,
            manual_configurations=manual_count,
            automatic_detections=automatic_count,
            failed_detections=failed_count,
            manual_percentage=round((manual_count / total_tables * 100), 2) if total_tables > 0 else 0,
            automatic_percentage=round((automatic_count / total_tables * 100), 2) if total_tables > 0 else 0,
            strategy_distribution=strategy_distribution,
            timestamp=datetime.now(timezone.utc).isoformat()
        )
    
    def log_manual_config_table_override(self, table_name: str, manual_column: str, 
                                       auto_column: Optional[str], manual_strategy: str, 
                                       auto_strategy: Optional[str]):
        """Log when manual configuration overrides automatic detection."""
        self.info(
            "Manual configuration overriding automatic detection",
            table_name=table_name,
            manual_column=manual_column,
            automatic_column=auto_column,
            manual_strategy=manual_strategy,
            automatic_strategy=auto_strategy,
            override_reason="manual_configuration_provided",
            timestamp=datetime.now(timezone.utc).isoformat()
        )
    
    def log_invalid_manual_config_entry(self, table_name: str, error: str, 
                                      config_entry: dict, fallback_action: str):
        """Log invalid manual configuration entry with fallback notification."""
        self.error(
            "Invalid manual configuration entry detected",
            table_name=table_name,
            error=error,
            config_entry=str(config_entry),
            fallback_action=fallback_action,
            timestamp=datetime.now(timezone.utc).isoformat()
        )
    
    def log_manual_config_column_not_found(self, table_name: str, column_name: str, 
                                         available_columns: Optional[list] = None):
        """Log when manually configured column is not found in table."""
        self.error(
            "Manually configured column not found in table",
            table_name=table_name,
            configured_column=column_name,
            available_columns=available_columns[:10] if available_columns else None,  # Limit to first 10
            total_available_columns=len(available_columns) if available_columns else 0,
            fallback_action="automatic_detection",
            timestamp=datetime.now(timezone.utc).isoformat()
        )