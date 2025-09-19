"""
S3 bookmark storage module for AWS Glue Data Replication.

This module provides S3-based persistent bookmark storage functionality including:
- S3BookmarkConfig: Configuration for S3 bookmark storage
- S3BookmarkStorage: Handles S3 operations for persistent bookmark storage
"""

import os
import json
import time
import asyncio
from dataclasses import dataclass
from typing import Dict, List, Optional, Any
from datetime import datetime, timezone, timedelta
from urllib.parse import urlparse

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError, NoCredentialsError, BotoCoreError


@dataclass
class S3BookmarkConfig:
    """Configuration for S3 bookmark storage."""
    bucket_name: str
    bookmark_prefix: str
    job_name: str
    retry_attempts: int = 3
    timeout_seconds: int = 30
    
    def __post_init__(self):
        """Validate configuration after initialization."""
        if not self.bucket_name:
            raise ValueError("S3 bucket name cannot be empty")
        if not self.job_name:
            raise ValueError("Job name cannot be empty")
        if self.retry_attempts < 1:
            raise ValueError("Retry attempts must be at least 1")
        if self.timeout_seconds < 1:
            raise ValueError("Timeout seconds must be at least 1")
        
        # Ensure bookmark_prefix ends with '/' for proper S3 key structure
        if self.bookmark_prefix and not self.bookmark_prefix.endswith('/'):
            self.bookmark_prefix += '/'


class S3BookmarkStorage:
    """Handles S3 operations for persistent bookmark storage."""
    
    def __init__(self, config: S3BookmarkConfig):
        self.config = config
        
        # Import dependencies that may not be available during module import
        try:
            from ..monitoring.logging import StructuredLogger
            from ..monitoring.metrics import CloudWatchMetricsPublisher
            self.structured_logger = StructuredLogger(config.job_name)
            self.metrics_publisher = CloudWatchMetricsPublisher(config.job_name)
        except ImportError:
            # Fallback for when monitoring modules are not available
            import logging
            self.structured_logger = logging.getLogger(__name__)
            self.metrics_publisher = None
        
        # Configure S3 client with retry and timeout settings
        boto_config = Config(
            retries={
                'max_attempts': config.retry_attempts,
                'mode': 'adaptive'
            },
            read_timeout=config.timeout_seconds,
            connect_timeout=config.timeout_seconds,
            region_name=os.environ.get('AWS_DEFAULT_REGION', 'us-east-1')
        )
        
        try:
            self.s3_client = boto3.client('s3', config=boto_config)
            if hasattr(self.structured_logger, 'info'):
                self.structured_logger.info("Initialized S3 client for bookmark storage",
                                          bucket=config.bucket_name,
                                          prefix=config.bookmark_prefix)
            else:
                self.structured_logger.info(f"Initialized S3 client for bookmark storage: {config.bucket_name}")
        except Exception as e:
            if hasattr(self.structured_logger, 'error'):
                self.structured_logger.error("Failed to initialize S3 client", error=str(e))
            else:
                self.structured_logger.error(f"Failed to initialize S3 client: {e}")
            raise
    
    def _get_bookmark_s3_key(self, table_name: str) -> str:
        """Generate S3 key for bookmark file."""
        return f"{self.config.bookmark_prefix}{self.config.job_name}/{table_name}.json"
    
    async def _read_bookmark_from_s3(self, table_name: str) -> Optional[Dict[str, Any]]:
        """
        Private method to read bookmark data from S3 with comprehensive error handling.
        
        This method implements the core S3 read functionality with:
        - JSON parsing and validation for bookmark files
        - Error handling for S3 access denied, not found, and timeout scenarios
        - Retry logic with exponential backoff
        - Automatic cleanup of corrupted bookmark files
        
        Args:
            table_name: Name of the table to read bookmark for
            
        Returns:
            Dictionary containing validated bookmark data or None if not found/error
        """
        s3_key = self._get_bookmark_s3_key(table_name)
        start_time = time.time()
        
        # Log operation start
        if hasattr(self.structured_logger, 'log_s3_operation_start'):
            self.structured_logger.log_s3_operation_start("read", table_name, s3_key,
                                                         bucket=self.config.bucket_name)
        
        for attempt in range(1, self.config.retry_attempts + 1):
            try:
                if hasattr(self.structured_logger, 'debug'):
                    self.structured_logger.debug("Attempting to read bookmark from S3",
                                               table_name=table_name,
                                               s3_key=s3_key,
                                               attempt=attempt)
                
                # Use asyncio to run the S3 operation in a thread pool
                loop = asyncio.get_event_loop()
                response = await loop.run_in_executor(
                    None,
                    lambda: self.s3_client.get_object(
                        Bucket=self.config.bucket_name,
                        Key=s3_key
                    )
                )
                
                # Parse JSON content with validation
                content = response['Body'].read().decode('utf-8')
                bookmark_data = json.loads(content)
                
                # Calculate operation duration for performance logging
                duration_ms = (time.time() - start_time) * 1000
                
                # Validate bookmark JSON structure with enhanced corruption detection
                if not self._validate_bookmark_json(bookmark_data, table_name):
                    if hasattr(self.structured_logger, 'error'):
                        self.structured_logger.error("Bookmark validation failed - corrupted data detected",
                                                   table_name=table_name,
                                                   s3_key=s3_key,
                                                   bookmark_keys=list(bookmark_data.keys()) if isinstance(bookmark_data, dict) else "invalid_data",
                                                   cleanup_action="deleting_corrupted_file")
                    
                    # Publish corruption metrics
                    if hasattr(self, 'metrics_publisher') and self.metrics_publisher:
                        self.metrics_publisher.publish_bookmark_corruption_metrics(
                            table_name, "validation_failed", False)
                    
                    # Delete corrupted file and return None to trigger full load fallback
                    try:
                        delete_success = await self.delete_bookmark(table_name)
                        cleanup_success = delete_success
                        
                        # Log cleanup result
                        if hasattr(self.structured_logger, 'log_corrupted_bookmark_cleanup'):
                            self.structured_logger.log_corrupted_bookmark_cleanup(
                                table_name, s3_key, cleanup_success)
                        
                        # Update corruption metrics with cleanup result
                        if hasattr(self, 'metrics_publisher') and self.metrics_publisher:
                            self.metrics_publisher.publish_bookmark_corruption_metrics(
                                table_name, "validation_failed", cleanup_success)
                        
                    except Exception as delete_error:
                        if hasattr(self.structured_logger, 'log_corrupted_bookmark_cleanup'):
                            self.structured_logger.log_corrupted_bookmark_cleanup(
                                table_name, s3_key, False, str(delete_error))
                        
                        if hasattr(self, 'metrics_publisher') and self.metrics_publisher:
                            self.metrics_publisher.publish_bookmark_corruption_metrics(
                                table_name, "validation_failed", False)
                    
                    # Log operation failure and publish metrics
                    if hasattr(self.structured_logger, 'log_s3_operation_failure'):
                        self.structured_logger.log_s3_operation_failure(
                            "read", table_name, s3_key, duration_ms, 
                            "Bookmark validation failed", "corrupted_data")
                    
                    if hasattr(self, 'metrics_publisher') and self.metrics_publisher:
                        self.metrics_publisher.publish_s3_bookmark_metrics(
                            "read", table_name, False, duration_ms, "corrupted_data")
                    
                    return None
                
                # Log successful operation with performance metrics
                file_size = response.get('ContentLength', 0)
                if hasattr(self.structured_logger, 'log_s3_operation_success'):
                    self.structured_logger.log_s3_operation_success(
                        "read", table_name, s3_key, duration_ms,
                        file_size_bytes=file_size,
                        bookmark_version=bookmark_data.get('version', 'unknown'))
                
                # Log bookmark state details
                if hasattr(self.structured_logger, 'log_bookmark_state_loaded'):
                    self.structured_logger.log_bookmark_state_loaded(
                        table_name,
                        bookmark_data.get('last_processed_value'),
                        bookmark_data.get('last_update_timestamp'),
                        bookmark_data.get('is_first_run', True))
                
                # Publish success metrics
                if hasattr(self, 'metrics_publisher') and self.metrics_publisher:
                    self.metrics_publisher.publish_s3_bookmark_metrics(
                        "read", table_name, True, duration_ms, file_size_bytes=file_size)
                
                return bookmark_data
                
            except ClientError as e:
                # Calculate duration for error logging
                duration_ms = (time.time() - start_time) * 1000
                
                # Use centralized error handling
                error_info = self._handle_s3_error("read", table_name, e)
                
                if error_info['error_category'] == 'key_not_found':
                    # File not found - expected for first run, not an error
                    if hasattr(self.structured_logger, 'info'):
                        self.structured_logger.info("S3 bookmark file not found (expected for first run)",
                                                  table_name=table_name, s3_key=s3_key)
                    return None
                elif not error_info['is_recoverable']:
                    # Permanent error - log failure and publish metrics
                    if hasattr(self.structured_logger, 'log_s3_operation_failure'):
                        self.structured_logger.log_s3_operation_failure(
                            "read", table_name, s3_key, duration_ms, 
                            str(e), error_info['error_category'])
                    
                    if hasattr(self, 'metrics_publisher') and self.metrics_publisher:
                        self.metrics_publisher.publish_s3_bookmark_metrics(
                            "read", table_name, False, duration_ms, error_info['error_category'])
                        self.metrics_publisher.publish_bookmark_fallback_metrics(
                            table_name, "permanent_s3_error", error_info['error_category'])
                    
                    return None
                elif error_info['should_retry'] and attempt < self.config.retry_attempts:
                    # Retry with exponential backoff
                    wait_time = 2 ** (attempt - 1)
                    if hasattr(self.structured_logger, 'log_s3_operation_retry'):
                        self.structured_logger.log_s3_operation_retry(
                            "read", table_name, s3_key, attempt, wait_time, error_info['error_category'])
                    await asyncio.sleep(wait_time)
                    continue
                else:
                    # Max retries reached - log final failure
                    if hasattr(self.structured_logger, 'log_s3_operation_failure'):
                        self.structured_logger.log_s3_operation_failure(
                            "read", table_name, s3_key, duration_ms, 
                            f"Max retries reached: {str(e)}", error_info['error_category'])
                    
                    if hasattr(self, 'metrics_publisher') and self.metrics_publisher:
                        self.metrics_publisher.publish_s3_bookmark_metrics(
                            "read", table_name, False, duration_ms, error_info['error_category'])
                        self.metrics_publisher.publish_bookmark_fallback_metrics(
                            table_name, "max_retries_reached", error_info['error_category'])
                    
                    return None
                        
            except json.JSONDecodeError as e:
                # Calculate duration for error logging
                duration_ms = (time.time() - start_time) * 1000
                
                # Use centralized error handling for JSON corruption
                error_info = self._handle_s3_error("read", table_name, e)
                
                if hasattr(self.structured_logger, 'error'):
                    self.structured_logger.error("Corrupted JSON detected in bookmark file, initiating cleanup",
                                               table_name=table_name,
                                               s3_key=s3_key,
                                               error=str(e),
                                               cleanup_action="deleting_corrupted_file")
                
                # Publish corruption detection metrics
                if hasattr(self, 'metrics_publisher') and self.metrics_publisher:
                    self.metrics_publisher.publish_bookmark_corruption_metrics(
                        table_name, "json_decode_error", False)
                
                # Delete corrupted file and return None to trigger full load fallback
                try:
                    delete_success = await self.delete_bookmark(table_name)
                    cleanup_success = delete_success
                    
                    # Log cleanup result
                    if hasattr(self.structured_logger, 'log_corrupted_bookmark_cleanup'):
                        self.structured_logger.log_corrupted_bookmark_cleanup(
                            table_name, s3_key, cleanup_success)
                    
                    # Update corruption metrics with cleanup result
                    if hasattr(self, 'metrics_publisher') and self.metrics_publisher:
                        self.metrics_publisher.publish_bookmark_corruption_metrics(
                            table_name, "json_decode_error", cleanup_success)
                        
                except Exception as delete_error:
                    if hasattr(self.structured_logger, 'log_corrupted_bookmark_cleanup'):
                        self.structured_logger.log_corrupted_bookmark_cleanup(
                            table_name, s3_key, False, str(delete_error))
                    
                    if hasattr(self, 'metrics_publisher') and self.metrics_publisher:
                        self.metrics_publisher.publish_bookmark_corruption_metrics(
                            table_name, "json_decode_error", False)
                
                # Log operation failure and publish metrics
                if hasattr(self.structured_logger, 'log_s3_operation_failure'):
                    self.structured_logger.log_s3_operation_failure(
                        "read", table_name, s3_key, duration_ms, 
                        f"JSON decode error: {str(e)}", "corrupted_data")
                
                if hasattr(self, 'metrics_publisher') and self.metrics_publisher:
                    self.metrics_publisher.publish_s3_bookmark_metrics(
                        "read", table_name, False, duration_ms, "corrupted_data")
                    self.metrics_publisher.publish_bookmark_fallback_metrics(
                        table_name, "corrupted_json", "corrupted_data")
                
                return None
                
            except Exception as e:
                # Calculate duration for error logging
                duration_ms = (time.time() - start_time) * 1000
                
                # Use centralized error handling for unexpected errors
                error_info = self._handle_s3_error("read", table_name, e)
                
                if error_info['should_retry'] and attempt < self.config.retry_attempts:
                    # Retry with exponential backoff
                    wait_time = 2 ** (attempt - 1)
                    if hasattr(self.structured_logger, 'log_s3_operation_retry'):
                        self.structured_logger.log_s3_operation_retry(
                            "read", table_name, s3_key, attempt, wait_time, error_info['error_category'])
                    await asyncio.sleep(wait_time)
                    continue
                else:
                    # Max retries reached or non-retryable error
                    if hasattr(self.structured_logger, 'log_s3_operation_failure'):
                        self.structured_logger.log_s3_operation_failure(
                            "read", table_name, s3_key, duration_ms, 
                            f"Unexpected error: {str(e)}", error_info['error_category'])
                    
                    if hasattr(self, 'metrics_publisher') and self.metrics_publisher:
                        self.metrics_publisher.publish_s3_bookmark_metrics(
                            "read", table_name, False, duration_ms, error_info['error_category'])
                        self.metrics_publisher.publish_bookmark_fallback_metrics(
                            table_name, "unexpected_error", error_info['error_category'])
                    
                    return None
        
        return None
    
    def _validate_bookmark_json(self, bookmark_data: Dict[str, Any], table_name: str) -> bool:
        """
        Validate bookmark JSON structure and handle missing fields with comprehensive checks.
        
        This method performs enhanced validation of bookmark data including:
        - Basic structure validation
        - Required field presence and format validation
        - Data type validation for all fields
        - Logical consistency checks
        - Corruption detection for malformed data
        
        Args:
            bookmark_data: Dictionary containing bookmark data from S3
            table_name: Name of the table (for logging)
            
        Returns:
            True if bookmark data is valid, False otherwise
        """
        try:
            # Basic structure validation
            if not isinstance(bookmark_data, dict):
                if hasattr(self.structured_logger, 'error'):
                    self.structured_logger.error("Bookmark data is not a valid dictionary",
                                               table_name=table_name,
                                               data_type=type(bookmark_data).__name__)
                return False
            
            # Check for empty data
            if not bookmark_data:
                if hasattr(self.structured_logger, 'error'):
                    self.structured_logger.error("Bookmark data is empty",
                                               table_name=table_name)
                return False
            
            # Use the existing validation from JobBookmarkState
            try:
                from .bookmark_manager import JobBookmarkState
                if not JobBookmarkState._validate_s3_data(bookmark_data):
                    if hasattr(self.structured_logger, 'error'):
                        self.structured_logger.error("Bookmark data failed JobBookmarkState validation",
                                                   table_name=table_name,
                                                   bookmark_keys=list(bookmark_data.keys()))
                    return False
            except ImportError:
                # Fallback validation if JobBookmarkState is not available
                required_fields = ['table_name', 'incremental_strategy']
                for field in required_fields:
                    if field not in bookmark_data or bookmark_data[field] is None:
                        if hasattr(self.structured_logger, 'error'):
                            self.structured_logger.error(f"Missing required field in bookmark data: {field}",
                                                       table_name=table_name)
                        return False
            
            # Additional corruption detection checks
            
            # Check for table name consistency
            if bookmark_data.get('table_name') != table_name:
                if hasattr(self.structured_logger, 'error'):
                    self.structured_logger.error("Table name mismatch in bookmark data",
                                               table_name=table_name,
                                               bookmark_table_name=bookmark_data.get('table_name'))
                return False
            
            # Check for reasonable timestamp values (not in the future by more than 1 hour)
            current_time = datetime.now(timezone.utc)
            future_threshold = current_time + timedelta(hours=1)
            
            timestamp_fields = ['last_update_timestamp', 'created_timestamp', 'updated_timestamp']
            for field in timestamp_fields:
                if field in bookmark_data and bookmark_data[field]:
                    try:
                        timestamp_str = bookmark_data[field]
                        if timestamp_str.endswith('Z'):
                            timestamp_str = timestamp_str[:-1] + '+00:00'
                        elif '+' not in timestamp_str and timestamp_str.count(':') == 2:
                            timestamp_str += '+00:00'
                        
                        parsed_timestamp = datetime.fromisoformat(timestamp_str)
                        
                        # Check if timestamp is unreasonably in the future
                        if parsed_timestamp > future_threshold:
                            if hasattr(self.structured_logger, 'error'):
                                self.structured_logger.error(f"Bookmark {field} is unreasonably in the future",
                                                           table_name=table_name,
                                                           field=field,
                                                           timestamp=bookmark_data[field],
                                                           current_time=current_time.isoformat())
                            return False
                        
                        # Check if created_timestamp is after updated_timestamp (logical inconsistency)
                        if field == 'updated_timestamp' and 'created_timestamp' in bookmark_data:
                            created_str = bookmark_data['created_timestamp']
                            if created_str:
                                if created_str.endswith('Z'):
                                    created_str = created_str[:-1] + '+00:00'
                                elif '+' not in created_str and created_str.count(':') == 2:
                                    created_str += '+00:00'
                                
                                created_timestamp = datetime.fromisoformat(created_str)
                                if created_timestamp > parsed_timestamp:
                                    if hasattr(self.structured_logger, 'error'):
                                        self.structured_logger.error("Created timestamp is after updated timestamp",
                                                                   table_name=table_name,
                                                                   created_timestamp=bookmark_data['created_timestamp'],
                                                                   updated_timestamp=bookmark_data[field])
                                    return False
                                    
                    except (ValueError, TypeError) as e:
                        if hasattr(self.structured_logger, 'error'):
                            self.structured_logger.error(f"Invalid timestamp format in {field}",
                                                       table_name=table_name,
                                                       field=field,
                                                       timestamp=bookmark_data[field],
                                                       error=str(e))
                        return False
            
            # Check for valid incremental strategy and column consistency
            strategy = bookmark_data.get('incremental_strategy')
            column = bookmark_data.get('incremental_column')
            
            if strategy in ['timestamp', 'primary_key'] and not column:
                if hasattr(self.structured_logger, 'error'):
                    self.structured_logger.error("Incremental column required for strategy but missing",
                                               table_name=table_name,
                                               strategy=strategy,
                                               column=column)
                return False
            
            # Check for reasonable last_processed_value format
            last_value = bookmark_data.get('last_processed_value')
            if last_value is not None and strategy == 'timestamp':
                # For timestamp strategy, last_processed_value should be a valid timestamp or date
                try:
                    if isinstance(last_value, str) and len(last_value) > 0:
                        # Try to parse as timestamp
                        if 'T' in last_value or ' ' in last_value:
                            # Looks like a timestamp
                            test_timestamp = last_value
                            if test_timestamp.endswith('Z'):
                                test_timestamp = test_timestamp[:-1] + '+00:00'
                            elif '+' not in test_timestamp and test_timestamp.count(':') >= 2:
                                test_timestamp += '+00:00'
                            datetime.fromisoformat(test_timestamp)
                        elif '-' in last_value and len(last_value) >= 8:
                            # Looks like a date
                            datetime.strptime(last_value[:10], '%Y-%m-%d')
                except (ValueError, TypeError) as e:
                    if hasattr(self.structured_logger, 'warning'):
                        self.structured_logger.warning("Last processed value format may be invalid for timestamp strategy",
                                                     table_name=table_name,
                                                     last_processed_value=last_value,
                                                     strategy=strategy,
                                                     error=str(e))
                    # Don't fail validation for this - it might be a different timestamp format
            
            # Check for suspicious data patterns that might indicate corruption
            
            # Check for extremely long string values that might indicate data corruption
            for key, value in bookmark_data.items():
                if isinstance(value, str) and len(value) > 10000:  # 10KB limit for string fields
                    if hasattr(self.structured_logger, 'error'):
                        self.structured_logger.error("Suspiciously long string value detected",
                                                   table_name=table_name,
                                                   field=key,
                                                   value_length=len(value))
                    return False
            
            # Check for null bytes or other control characters that might indicate corruption
            for key, value in bookmark_data.items():
                if isinstance(value, str) and ('\x00' in value or any(ord(c) < 32 and c not in '\t\n\r' for c in value)):
                    if hasattr(self.structured_logger, 'error'):
                        self.structured_logger.error("Control characters detected in bookmark data",
                                                   table_name=table_name,
                                                   field=key)
                    return False
            
            if hasattr(self.structured_logger, 'debug'):
                self.structured_logger.debug("Bookmark data validation passed all checks",
                                           table_name=table_name,
                                           strategy=strategy,
                                           column=column,
                                           is_first_run=bookmark_data.get('is_first_run', True))
            
            return True
            
        except Exception as e:
            if hasattr(self.structured_logger, 'error'):
                self.structured_logger.error("Unexpected error during bookmark validation",
                                           table_name=table_name,
                                           error=str(e),
                                           error_type=type(e).__name__)
            return False
    
    async def read_bookmark(self, table_name: str) -> Optional[Dict[str, Any]]:
        """
        Read bookmark data from S3 with error handling and fallback logic.
        
        This is the public interface that implements fallback logic when S3 read operations fail.
        
        Args:
            table_name: Name of the table to read bookmark for
            
        Returns:
            Dictionary containing bookmark data or None if not found/error
        """
        try:
            # Attempt to read from S3 using the private method
            bookmark_data = await self._read_bookmark_from_s3(table_name)
            
            if bookmark_data is not None:
                # Successfully read and validated bookmark
                return bookmark_data
            else:
                # Bookmark not found or failed to read - this triggers fallback logic
                if hasattr(self.structured_logger, 'info'):
                    self.structured_logger.info("S3 bookmark read returned None, triggering fallback logic",
                                              table_name=table_name)
                return None
                
        except Exception as e:
            # Unexpected error in the read process - implement fallback logic
            if hasattr(self.structured_logger, 'error'):
                self.structured_logger.error("Unexpected error in bookmark read operation, falling back",
                                           table_name=table_name,
                                           error=str(e))
            return None
    
    async def read_bookmarks_parallel(self, table_names: List[str]) -> Dict[str, Optional[Dict[str, Any]]]:
        """
        Read bookmark data for multiple tables in parallel.
        
        This method implements parallel bookmark reading during job initialization
        to optimize S3 operations for jobs processing many tables simultaneously.
        
        Args:
            table_names: List of table names to read bookmarks for
            
        Returns:
            Dictionary mapping table names to bookmark data (or None if not found/error)
        """
        if not table_names:
            return {}
        
        start_time = time.time()
        
        # Log start of parallel operation
        if hasattr(self.structured_logger, 'log_parallel_s3_operation_start'):
            self.structured_logger.log_parallel_s3_operation_start(
                "read", len(table_names), 
                table_names=table_names
            )
        
        try:
            # Create tasks for parallel execution
            tasks = []
            for table_name in table_names:
                task = asyncio.create_task(
                    self.read_bookmark(table_name),
                    name=f"read_bookmark_{table_name}"
                )
                tasks.append((table_name, task))
            
            # Execute all tasks concurrently with timeout
            results = {}
            successful_count = 0
            failed_count = 0
            
            # Wait for all tasks to complete with a reasonable timeout
            timeout_seconds = self.config.timeout_seconds * len(table_names)  # Scale timeout with table count
            
            try:
                # Use asyncio.gather with return_exceptions=True to handle individual failures
                task_results = await asyncio.wait_for(
                    asyncio.gather(*[task for _, task in tasks], return_exceptions=True),
                    timeout=timeout_seconds
                )
                
                # Process results
                for i, (table_name, _) in enumerate(tasks):
                    result = task_results[i]
                    
                    if isinstance(result, Exception):
                        # Task failed with exception
                        if hasattr(self.structured_logger, 'error'):
                            self.structured_logger.error("Parallel bookmark read failed for table",
                                                       table_name=table_name,
                                                       error=str(result),
                                                       error_type=type(result).__name__)
                        results[table_name] = None
                        failed_count += 1
                    else:
                        # Task completed successfully (result may be None for not found)
                        results[table_name] = result
                        successful_count += 1
                        
                        if result is not None:
                            if hasattr(self.structured_logger, 'debug'):
                                self.structured_logger.debug("Parallel bookmark read successful",
                                                           table_name=table_name,
                                                           bookmark_found=True)
                        else:
                            if hasattr(self.structured_logger, 'debug'):
                                self.structured_logger.debug("Parallel bookmark read completed - no bookmark found",
                                                           table_name=table_name,
                                                           bookmark_found=False)
                
            except asyncio.TimeoutError:
                # Handle timeout - cancel remaining tasks and collect partial results
                if hasattr(self.structured_logger, 'warning'):
                    self.structured_logger.warning("Parallel bookmark read timeout, cancelling remaining tasks",
                                                 timeout_seconds=timeout_seconds,
                                                 table_count=len(table_names))
                
                # Cancel all tasks and collect what we can
                for table_name, task in tasks:
                    if not task.done():
                        task.cancel()
                        results[table_name] = None
                        failed_count += 1
                    else:
                        try:
                            result = task.result()
                            results[table_name] = result
                            successful_count += 1
                        except Exception as e:
                            results[table_name] = None
                            failed_count += 1
            
            # Calculate total duration
            total_duration_ms = (time.time() - start_time) * 1000
            
            # Log completion of parallel operation
            if hasattr(self.structured_logger, 'log_parallel_s3_operation_complete'):
                self.structured_logger.log_parallel_s3_operation_complete(
                    "read", len(table_names), successful_count, failed_count, 
                    total_duration_ms, table_names=table_names
                )
            
            # Publish parallel operation metrics
            if hasattr(self, 'metrics_publisher') and self.metrics_publisher:
                self.metrics_publisher.publish_parallel_s3_metrics(
                    "read", len(table_names), successful_count, failed_count, total_duration_ms
                )
            
            return results
            
        except Exception as e:
            # Handle unexpected errors in parallel processing
            total_duration_ms = (time.time() - start_time) * 1000
            
            if hasattr(self.structured_logger, 'error'):
                self.structured_logger.error("Unexpected error in parallel bookmark read operation",
                                           error=str(e),
                                           error_type=type(e).__name__,
                                           table_count=len(table_names),
                                           duration_ms=total_duration_ms)
            
            # Return empty results for all tables to trigger fallback
            return {table_name: None for table_name in table_names}
    
    async def _write_bookmark_to_s3(self, table_name: str, bookmark_data: Dict[str, Any]) -> bool:
        """
        Private method to write bookmark data to S3 with comprehensive error handling.
        
        This method implements the core S3 write functionality with:
        - JSON serialization and S3 upload functionality
        - Error handling for S3 write failures with appropriate logging
        - Retry logic with exponential backoff
        - Non-blocking operation design to avoid blocking job execution
        
        Args:
            table_name: Name of the table to write bookmark for
            bookmark_data: Dictionary containing bookmark data
            
        Returns:
            True if successful, False otherwise
        """
        s3_key = self._get_bookmark_s3_key(table_name)
        start_time = time.time()
        
        try:
            # Serialize bookmark data to JSON with proper formatting
            json_content = json.dumps(bookmark_data, indent=2, default=str)
            content_bytes = json_content.encode('utf-8')
            
        except Exception as e:
            duration_ms = (time.time() - start_time) * 1000
            if hasattr(self.structured_logger, 'log_s3_operation_failure'):
                self.structured_logger.log_s3_operation_failure(
                    "write", table_name, s3_key, duration_ms,
                    f"JSON serialization failed: {str(e)}", "serialization_error")
            
            if hasattr(self, 'metrics_publisher') and self.metrics_publisher:
                self.metrics_publisher.publish_s3_bookmark_metrics(
                    "write", table_name, False, duration_ms, "serialization_error")
            
            return False
        
        # Log operation start
        if hasattr(self.structured_logger, 'log_s3_operation_start'):
            self.structured_logger.log_s3_operation_start("write", table_name, s3_key,
                                                         bucket=self.config.bucket_name,
                                                         content_size_bytes=len(content_bytes))
        
        for attempt in range(1, self.config.retry_attempts + 1):
            try:
                if hasattr(self.structured_logger, 'debug'):
                    self.structured_logger.debug("Attempting to write bookmark to S3",
                                               table_name=table_name,
                                               s3_key=s3_key,
                                               attempt=attempt,
                                               content_size=len(content_bytes))
                
                # Use asyncio to run the S3 operation in a thread pool to avoid blocking
                loop = asyncio.get_event_loop()
                await loop.run_in_executor(
                    None,
                    lambda: self.s3_client.put_object(
                        Bucket=self.config.bucket_name,
                        Key=s3_key,
                        Body=content_bytes,
                        ContentType='application/json',
                        ServerSideEncryption='AES256'
                    )
                )
                
                # Calculate operation duration for performance logging
                duration_ms = (time.time() - start_time) * 1000
                
                # Log successful operation with performance metrics
                if hasattr(self.structured_logger, 'log_s3_operation_success'):
                    self.structured_logger.log_s3_operation_success(
                        "write", table_name, s3_key, duration_ms,
                        file_size_bytes=len(content_bytes),
                        bookmark_version=bookmark_data.get('version', 'unknown'))
                
                # Log bookmark state details
                if hasattr(self.structured_logger, 'log_bookmark_state_saved'):
                    self.structured_logger.log_bookmark_state_saved(
                        table_name,
                        bookmark_data.get('last_processed_value'),
                        len(content_bytes),
                        bookmark_data.get('version', 'unknown'))
                
                # Publish success metrics
                if hasattr(self, 'metrics_publisher') and self.metrics_publisher:
                    self.metrics_publisher.publish_s3_bookmark_metrics(
                        "write", table_name, True, duration_ms, 
                        file_size_bytes=len(content_bytes))
                
                return True
                
            except ClientError as e:
                # Calculate duration for error logging
                duration_ms = (time.time() - start_time) * 1000
                
                # Use centralized error handling
                error_info = self._handle_s3_error("write", table_name, e)
                
                if not error_info['is_recoverable']:
                    # Permanent error - log failure and publish metrics
                    if hasattr(self.structured_logger, 'log_s3_operation_failure'):
                        self.structured_logger.log_s3_operation_failure(
                            "write", table_name, s3_key, duration_ms, 
                            str(e), error_info['error_category'])
                    
                    if hasattr(self, 'metrics_publisher') and self.metrics_publisher:
                        self.metrics_publisher.publish_s3_bookmark_metrics(
                            "write", table_name, False, duration_ms, error_info['error_category'])
                    
                    return False
                elif error_info['should_retry'] and attempt < self.config.retry_attempts:
                    # Retry with exponential backoff
                    wait_time = 2 ** (attempt - 1)
                    if hasattr(self.structured_logger, 'log_s3_operation_retry'):
                        self.structured_logger.log_s3_operation_retry(
                            "write", table_name, s3_key, attempt, wait_time, error_info['error_category'])
                    await asyncio.sleep(wait_time)
                    continue
                else:
                    # Max retries reached - log final failure
                    if hasattr(self.structured_logger, 'log_s3_operation_failure'):
                        self.structured_logger.log_s3_operation_failure(
                            "write", table_name, s3_key, duration_ms, 
                            f"Max retries reached: {str(e)}", error_info['error_category'])
                    
                    if hasattr(self, 'metrics_publisher') and self.metrics_publisher:
                        self.metrics_publisher.publish_s3_bookmark_metrics(
                            "write", table_name, False, duration_ms, error_info['error_category'])
                    
                    return False
                        
            except Exception as e:
                # Calculate duration for error logging
                duration_ms = (time.time() - start_time) * 1000
                
                # Use centralized error handling for unexpected errors
                error_info = self._handle_s3_error("write", table_name, e)
                
                if error_info['should_retry'] and attempt < self.config.retry_attempts:
                    # Retry with exponential backoff
                    wait_time = 2 ** (attempt - 1)
                    if hasattr(self.structured_logger, 'log_s3_operation_retry'):
                        self.structured_logger.log_s3_operation_retry(
                            "write", table_name, s3_key, attempt, wait_time, error_info['error_category'])
                    await asyncio.sleep(wait_time)
                    continue
                else:
                    # Max retries reached or non-retryable error
                    if hasattr(self.structured_logger, 'log_s3_operation_failure'):
                        self.structured_logger.log_s3_operation_failure(
                            "write", table_name, s3_key, duration_ms, 
                            f"Unexpected error: {str(e)}", error_info['error_category'])
                    
                    if hasattr(self, 'metrics_publisher') and self.metrics_publisher:
                        self.metrics_publisher.publish_s3_bookmark_metrics(
                            "write", table_name, False, duration_ms, error_info['error_category'])
                    
                    return False
        
        return False
    
    async def write_bookmark(self, table_name: str, bookmark_data: Dict[str, Any]) -> bool:
        """
        Write bookmark data to S3 with error handling and fallback logic.
        
        This is the public interface that ensures write operations don't block job execution on failure.
        
        Args:
            table_name: Name of the table to write bookmark for
            bookmark_data: Dictionary containing bookmark data
            
        Returns:
            True if successful, False otherwise (but job execution continues regardless)
        """
        try:
            # Attempt to write to S3 using the private method
            success = await self._write_bookmark_to_s3(table_name, bookmark_data)
            
            if success:
                # Successfully wrote bookmark
                return True
            else:
                # Write failed but don't block job execution
                if hasattr(self.structured_logger, 'warning'):
                    self.structured_logger.warning("S3 bookmark write failed, job execution will continue",
                                                  table_name=table_name)
                return False
                
        except Exception as e:
            # Unexpected error in the write process - don't block job execution
            if hasattr(self.structured_logger, 'error'):
                self.structured_logger.error("Unexpected error in bookmark write operation, job execution continues",
                                           table_name=table_name,
                                           error=str(e))
            return False
    
    async def write_bookmarks_batch(self, bookmark_batch: Dict[str, Dict[str, Any]], 
                                  batch_size: int = 10) -> Dict[str, bool]:
        """
        Write bookmark data for multiple tables in batches.
        
        This method implements batch S3 write operations for multiple table bookmarks
        to optimize S3 operations and avoid blocking data operations.
        
        Args:
            bookmark_batch: Dictionary mapping table names to bookmark data
            batch_size: Number of bookmarks to write per batch (default: 10)
            
        Returns:
            Dictionary mapping table names to success status (True/False)
        """
        if not bookmark_batch:
            return {}
        
        start_time = time.time()
        table_names = list(bookmark_batch.keys())
        total_operations = len(table_names)
        
        # Log start of batch operation
        if hasattr(self.structured_logger, 'log_batch_s3_operation_start'):
            self.structured_logger.log_batch_s3_operation_start(
                "write", batch_size, total_operations,
                table_names=table_names
            )
        
        results = {}
        successful_count = 0
        failed_count = 0
        
        try:
            # Process bookmarks in batches to avoid overwhelming S3
            for batch_number, i in enumerate(range(0, total_operations, batch_size), 1):
                batch_start_time = time.time()
                batch_tables = table_names[i:i + batch_size]
                
                if hasattr(self.structured_logger, 'debug'):
                    self.structured_logger.debug("Processing bookmark write batch",
                                               batch_number=batch_number,
                                               batch_size=len(batch_tables),
                                               table_names=batch_tables)
                
                # Create tasks for this batch
                batch_tasks = []
                for table_name in batch_tables:
                    task = asyncio.create_task(
                        self.write_bookmark(table_name, bookmark_batch[table_name]),
                        name=f"write_bookmark_{table_name}"
                    )
                    batch_tasks.append((table_name, task))
                
                # Execute batch concurrently with timeout
                batch_timeout = self.config.timeout_seconds * len(batch_tables)
                batch_successful = 0
                batch_failed = 0
                
                try:
                    # Wait for all tasks in this batch to complete
                    batch_results = await asyncio.wait_for(
                        asyncio.gather(*[task for _, task in batch_tasks], return_exceptions=True),
                        timeout=batch_timeout
                    )
                    
                    # Process batch results
                    for j, (table_name, _) in enumerate(batch_tasks):
                        result = batch_results[j]
                        
                        if isinstance(result, Exception):
                            # Task failed with exception
                            if hasattr(self.structured_logger, 'error'):
                                self.structured_logger.error("Batch bookmark write failed for table",
                                                           table_name=table_name,
                                                           batch_number=batch_number,
                                                           error=str(result),
                                                           error_type=type(result).__name__)
                            results[table_name] = False
                            batch_failed += 1
                            failed_count += 1
                        else:
                            # Task completed - result is boolean success status
                            results[table_name] = result
                            if result:
                                batch_successful += 1
                                successful_count += 1
                            else:
                                batch_failed += 1
                                failed_count += 1
                
                except asyncio.TimeoutError:
                    # Handle batch timeout
                    if hasattr(self.structured_logger, 'warning'):
                        self.structured_logger.warning("Batch bookmark write timeout",
                                                     batch_number=batch_number,
                                                     timeout_seconds=batch_timeout,
                                                     batch_size=len(batch_tables))
                    
                    # Cancel remaining tasks and mark as failed
                    for table_name, task in batch_tasks:
                        if not task.done():
                            task.cancel()
                            results[table_name] = False
                            batch_failed += 1
                            failed_count += 1
                        else:
                            try:
                                result = task.result()
                                results[table_name] = result
                                if result:
                                    batch_successful += 1
                                    successful_count += 1
                                else:
                                    batch_failed += 1
                                    failed_count += 1
                            except Exception:
                                results[table_name] = False
                                batch_failed += 1
                                failed_count += 1
                
                # Calculate batch duration and log completion
                batch_duration_ms = (time.time() - batch_start_time) * 1000
                
                if hasattr(self.structured_logger, 'log_batch_s3_operation_complete'):
                    self.structured_logger.log_batch_s3_operation_complete(
                        "write", batch_number, len(batch_tables), 
                        batch_successful, batch_failed, batch_duration_ms,
                        table_names=batch_tables
                    )
                
                # Add small delay between batches to avoid rate limiting
                if batch_number * batch_size < total_operations:
                    await asyncio.sleep(0.1)  # 100ms delay between batches
            
            # Calculate total duration
            total_duration_ms = (time.time() - start_time) * 1000
            
            # Log completion of all batches
            if hasattr(self.structured_logger, 'info'):
                self.structured_logger.info("Completed all bookmark write batches",
                                          total_operations=total_operations,
                                          successful_count=successful_count,
                                          failed_count=failed_count,
                                          total_duration_ms=total_duration_ms,
                                          batch_size=batch_size,
                                          batch_count=((total_operations - 1) // batch_size) + 1)
            
            # Publish batch operation metrics
            if hasattr(self, 'metrics_publisher') and self.metrics_publisher:
                self.metrics_publisher.publish_batch_s3_metrics(
                    "write", total_operations, successful_count, failed_count, 
                    total_duration_ms, batch_size
                )
            
            return results
            
        except Exception as e:
            # Handle unexpected errors in batch processing
            total_duration_ms = (time.time() - start_time) * 1000
            
            if hasattr(self.structured_logger, 'error'):
                self.structured_logger.error("Unexpected error in batch bookmark write operation",
                                           error=str(e),
                                           error_type=type(e).__name__,
                                           total_operations=total_operations,
                                           duration_ms=total_duration_ms)
            
            # Return failure status for all remaining tables
            for table_name in table_names:
                if table_name not in results:
                    results[table_name] = False
            
            return results
    
    async def delete_bookmark(self, table_name: str) -> bool:
        """
        Delete corrupted bookmark file from S3.
        
        Args:
            table_name: Name of the table to delete bookmark for
            
        Returns:
            True if successful or file doesn't exist, False on error
        """
        s3_key = self._get_bookmark_s3_key(table_name)
        start_time = time.time()
        
        # Log operation start
        if hasattr(self.structured_logger, 'log_s3_operation_start'):
            self.structured_logger.log_s3_operation_start("delete", table_name, s3_key,
                                                         bucket=self.config.bucket_name,
                                                         reason="corrupted_file_cleanup")
        
        try:
            # Use asyncio to run the S3 operation in a thread pool
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(
                None,
                lambda: self.s3_client.delete_object(
                    Bucket=self.config.bucket_name,
                    Key=s3_key
                )
            )
            
            # Calculate operation duration for performance logging
            duration_ms = (time.time() - start_time) * 1000
            
            # Log successful operation with performance metrics
            if hasattr(self.structured_logger, 'log_s3_operation_success'):
                self.structured_logger.log_s3_operation_success(
                    "delete", table_name, s3_key, duration_ms,
                    reason="corrupted_file_cleanup")
            
            # Publish success metrics
            if hasattr(self, 'metrics_publisher') and self.metrics_publisher:
                self.metrics_publisher.publish_s3_bookmark_metrics(
                    "delete", table_name, True, duration_ms)
            
            return True
            
        except ClientError as e:
            # Calculate duration for error logging
            duration_ms = (time.time() - start_time) * 1000
            
            # Use centralized error handling
            error_info = self._handle_s3_error("delete", table_name, e)
            
            if error_info['error_category'] == 'key_not_found':
                # File doesn't exist, consider it successful
                if hasattr(self.structured_logger, 'info'):
                    self.structured_logger.info("Bookmark file already doesn't exist (expected)",
                                              table_name=table_name,
                                              s3_key=s3_key,
                                              duration_ms=duration_ms)
                
                # Still publish success metrics since this is expected
                if hasattr(self, 'metrics_publisher') and self.metrics_publisher:
                    self.metrics_publisher.publish_s3_bookmark_metrics(
                        "delete", table_name, True, duration_ms)
                
                return True
            else:
                # Other errors - log failure and publish metrics
                if hasattr(self.structured_logger, 'log_s3_operation_failure'):
                    self.structured_logger.log_s3_operation_failure(
                        "delete", table_name, s3_key, duration_ms, 
                        str(e), error_info['error_category'])
                
                if hasattr(self, 'metrics_publisher') and self.metrics_publisher:
                    self.metrics_publisher.publish_s3_bookmark_metrics(
                        "delete", table_name, False, duration_ms, error_info['error_category'])
                
                return False
        
        except Exception as e:
            # Calculate duration for error logging
            duration_ms = (time.time() - start_time) * 1000
            
            # Handle unexpected errors
            if hasattr(self.structured_logger, 'log_s3_operation_failure'):
                self.structured_logger.log_s3_operation_failure(
                    "delete", table_name, s3_key, duration_ms, 
                    f"Unexpected error: {str(e)}", "unexpected_error")
            
            if hasattr(self, 'metrics_publisher') and self.metrics_publisher:
                self.metrics_publisher.publish_s3_bookmark_metrics(
                    "delete", table_name, False, duration_ms, "unexpected_error")
            
            return False
    
    def _handle_s3_error(self, operation: str, table_name: str, error: Exception) -> Dict[str, Any]:
        """
        Handle S3 operation errors with appropriate fallback logic.
        
        This method provides centralized error handling for S3 operations and implements
        the fallback logic when S3 read operations fail. It categorizes errors and
        provides appropriate responses for each error type.
        
        Args:
            operation: Type of S3 operation (read, write, delete)
            table_name: Name of the table being processed
            error: Exception that occurred during S3 operation
            
        Returns:
            Dictionary containing error handling results:
            - should_retry: bool - whether the operation should be retried
            - should_fallback: bool - whether to fall back to in-memory bookmarks
            - is_recoverable: bool - whether the error is recoverable
            - error_category: str - category of the error for monitoring
        """
        error_info = {
            'should_retry': False,
            'should_fallback': False,
            'is_recoverable': True,
            'error_category': 'unknown'
        }
        
        if isinstance(error, ClientError):
            error_code = error.response.get('Error', {}).get('Code', 'Unknown')
            
            if error_code == 'AccessDenied':
                # Access denied - permanent error, fall back to in-memory bookmarks
                if hasattr(self.structured_logger, 'error'):
                    self.structured_logger.error(f"S3 {operation} access denied - check IAM permissions",
                                               operation=operation,
                                               table_name=table_name,
                                               error_code=error_code,
                                               bucket=self.config.bucket_name,
                                               fallback_action="switching_to_in_memory_bookmarks")
                error_info.update({
                    'should_retry': False,
                    'should_fallback': True,
                    'is_recoverable': False,
                    'error_category': 'access_denied'
                })
                
            elif error_code == 'NoSuchBucket':
                # Bucket not found - permanent error, fall back to in-memory bookmarks
                if hasattr(self.structured_logger, 'error'):
                    self.structured_logger.error(f"S3 bucket not found for {operation} operation",
                                               operation=operation,
                                               table_name=table_name,
                                               error_code=error_code,
                                               bucket=self.config.bucket_name,
                                               fallback_action="switching_to_in_memory_bookmarks")
                error_info.update({
                    'should_retry': False,
                    'should_fallback': True,
                    'is_recoverable': False,
                    'error_category': 'bucket_not_found'
                })
                
            elif error_code == 'NoSuchKey':
                # Key not found - expected for first run, not an error
                if hasattr(self.structured_logger, 'info'):
                    self.structured_logger.info(f"S3 bookmark file not found for {operation} (expected for first run)",
                                              operation=operation,
                                              table_name=table_name,
                                              error_code=error_code)
                error_info.update({
                    'should_retry': False,
                    'should_fallback': False,
                    'is_recoverable': True,
                    'error_category': 'key_not_found'
                })
                
            elif error_code in ['RequestTimeout', 'ServiceUnavailable', 'SlowDown', 'ThrottlingException']:
                # Temporary errors - retry with backoff
                if hasattr(self.structured_logger, 'warning'):
                    self.structured_logger.warning(f"S3 {operation} timeout/throttling - will retry",
                                                 operation=operation,
                                                 table_name=table_name,
                                                 error_code=error_code,
                                                 retry_recommended=True)
                error_info.update({
                    'should_retry': True,
                    'should_fallback': False,
                    'is_recoverable': True,
                    'error_category': 'timeout_throttling'
                })
                
            elif error_code in ['InternalError', 'ServiceFailure']:
                # AWS service errors - retry with backoff
                if hasattr(self.structured_logger, 'warning'):
                    self.structured_logger.warning(f"S3 {operation} service error - will retry",
                                                 operation=operation,
                                                 table_name=table_name,
                                                 error_code=error_code,
                                                 retry_recommended=True)
                error_info.update({
                    'should_retry': True,
                    'should_fallback': False,
                    'is_recoverable': True,
                    'error_category': 'service_error'
                })
                
            elif error_code == 'InvalidBucketName':
                # Invalid bucket name - permanent error
                if hasattr(self.structured_logger, 'error'):
                    self.structured_logger.error(f"S3 {operation} failed due to invalid bucket name",
                                               operation=operation,
                                               table_name=table_name,
                                               error_code=error_code,
                                               bucket=self.config.bucket_name,
                                               fallback_action="switching_to_in_memory_bookmarks")
                error_info.update({
                    'should_retry': False,
                    'should_fallback': True,
                    'is_recoverable': False,
                    'error_category': 'invalid_bucket'
                })
                
            else:
                # Other client errors - log and potentially retry
                if hasattr(self.structured_logger, 'error'):
                    self.structured_logger.error(f"S3 {operation} failed with client error",
                                               operation=operation,
                                               table_name=table_name,
                                               error_code=error_code,
                                               error_message=str(error),
                                               retry_recommended=True)
                error_info.update({
                    'should_retry': True,
                    'should_fallback': False,
                    'is_recoverable': True,
                    'error_category': 'client_error'
                })
                
        elif isinstance(error, (BotoCoreError, NoCredentialsError)):
            # Boto3/credential errors - usually permanent, fall back
            if hasattr(self.structured_logger, 'error'):
                self.structured_logger.error(f"S3 {operation} failed with boto3/credential error",
                                           operation=operation,
                                           table_name=table_name,
                                           error_type=type(error).__name__,
                                           error_message=str(error),
                                           fallback_action="switching_to_in_memory_bookmarks")
            error_info.update({
                'should_retry': False,
                'should_fallback': True,
                'is_recoverable': False,
                'error_category': 'credential_error'
            })
            
        elif isinstance(error, json.JSONDecodeError):
            # JSON parsing error - corrupted data, delete and fall back
            if hasattr(self.structured_logger, 'error'):
                self.structured_logger.error(f"S3 {operation} failed due to corrupted JSON data",
                                           operation=operation,
                                           table_name=table_name,
                                           error_type=type(error).__name__,
                                           error_message=str(error),
                                           fallback_action="deleting_corrupted_file_and_performing_full_load")
            error_info.update({
                'should_retry': False,
                'should_fallback': True,
                'is_recoverable': True,
                'error_category': 'corrupted_data'
            })
            
        elif isinstance(error, (ConnectionError, TimeoutError)):
            # Network errors - retry with backoff
            if hasattr(self.structured_logger, 'warning'):
                self.structured_logger.warning(f"S3 {operation} failed due to network error - will retry",
                                             operation=operation,
                                             table_name=table_name,
                                             error_type=type(error).__name__,
                                             error_message=str(error),
                                             retry_recommended=True)
            error_info.update({
                'should_retry': True,
                'should_fallback': False,
                'is_recoverable': True,
                'error_category': 'network_error'
            })
            
        else:
            # Handle other types of errors (unexpected errors)
            if hasattr(self.structured_logger, 'error'):
                self.structured_logger.error(f"S3 {operation} failed with unexpected error",
                                           operation=operation,
                                           table_name=table_name,
                                           error_type=type(error).__name__,
                                           error_message=str(error),
                                           retry_recommended=True)
            error_info.update({
                'should_retry': True,
                'should_fallback': False,
                'is_recoverable': True,
                'error_category': 'unexpected_error'
            })
        
        # Publish CloudWatch metric for error tracking
        self._publish_error_metric(operation, error_info['error_category'], table_name)
        
        return error_info
    
    def _publish_error_metric(self, operation: str, error_category: str, table_name: str) -> None:
        """
        Publish CloudWatch metrics for S3 error tracking.
        
        Args:
            operation: Type of S3 operation (read, write, delete)
            error_category: Category of the error
            table_name: Name of the table being processed
        """
        try:
            # Try to import cloudwatch client
            try:
                import boto3
                cloudwatch = boto3.client('cloudwatch')
            except Exception:
                cloudwatch = None
            
            if cloudwatch:
                metric_name = f"BookmarkS3{operation.capitalize()}Error"
                cloudwatch.put_metric_data(
                    Namespace='GlueDataReplication/Bookmarks/Errors',
                    MetricData=[
                        {
                            'MetricName': metric_name,
                            'Dimensions': [
                                {
                                    'Name': 'JobName',
                                    'Value': self.config.job_name
                                },
                                {
                                    'Name': 'TableName',
                                    'Value': table_name
                                },
                                {
                                    'Name': 'ErrorCategory',
                                    'Value': error_category
                                }
                            ],
                            'Value': 1.0,
                            'Unit': 'Count',
                            'Timestamp': datetime.now(timezone.utc)
                        }
                    ]
                )
                if hasattr(self.structured_logger, 'debug'):
                    self.structured_logger.debug("Published S3 error metric",
                                               metric_name=metric_name,
                                               error_category=error_category,
                                               table_name=table_name)
        except Exception as e:
            # Don't fail the job for CloudWatch metric failures
            if hasattr(self.structured_logger, 'warning'):
                self.structured_logger.warning("Failed to publish S3 error metric",
                                             operation=operation,
                                             error_category=error_category,
                                             table_name=table_name,
                                             error=str(e))
    
    def list_bookmarks(self) -> List[str]:
        """
        List all bookmark files for the current job.
        
        Returns:
            List of table names that have bookmark files
        """
        try:
            prefix = f"{self.config.bookmark_prefix}{self.config.job_name}/"
            
            response = self.s3_client.list_objects_v2(
                Bucket=self.config.bucket_name,
                Prefix=prefix
            )
            
            table_names = []
            for obj in response.get('Contents', []):
                key = obj['Key']
                # Extract table name from key (remove prefix and .json extension)
                if key.endswith('.json'):
                    table_name = key[len(prefix):-5]  # Remove prefix and .json
                    table_names.append(table_name)
            
            if hasattr(self.structured_logger, 'info'):
                self.structured_logger.info("Listed bookmark files",
                                          count=len(table_names),
                                          tables=table_names)
            
            return table_names
            
        except Exception as e:
            self._handle_s3_error("list", "all_tables", e)
            return []

