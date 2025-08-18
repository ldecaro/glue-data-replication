#!/usr/bin/env python3
"""
AWS Glue Data Replication PySpark Job

This script implements a data replication system that supports full-load and incremental
data migration across multiple database types using AWS Glue job bookmarks.

Supported database engines: Oracle, SQL Server, PostgreSQL, DB2
"""

import sys
import os
import json
import logging
import time
import threading
import queue
import asyncio
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError, NoCredentialsError, BotoCoreError
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any
from urllib.parse import urlparse
from datetime import datetime, timezone, timedelta

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType, LongType, DateType
from pyspark.sql.functions import col, max as spark_max, min as spark_min, hash, concat_ws, lit

# Configure structured logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - [%(funcName)s:%(lineno)d] - %(message)s'
)
logger = logging.getLogger(__name__)

# Initialize CloudWatch client for metrics
try:
    cloudwatch = boto3.client('cloudwatch')
except Exception as e:
    logger.warning(f"Failed to initialize CloudWatch client: {e}")
    cloudwatch = None


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
        self.structured_logger = StructuredLogger(config.job_name)
        self.metrics_publisher = CloudWatchMetricsPublisher(config.job_name)
        
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
            self.structured_logger.info("Initialized S3 client for bookmark storage",
                                      bucket=config.bucket_name,
                                      prefix=config.bookmark_prefix)
        except Exception as e:
            self.structured_logger.error("Failed to initialize S3 client", error=str(e))
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
        
        # Log operation start (Requirement 6.1)
        self.structured_logger.log_s3_operation_start("read", table_name, s3_key,
                                                     bucket=self.config.bucket_name)
        
        for attempt in range(1, self.config.retry_attempts + 1):
            try:
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
                    self.structured_logger.error("Bookmark validation failed - corrupted data detected",
                                               table_name=table_name,
                                               s3_key=s3_key,
                                               bookmark_keys=list(bookmark_data.keys()) if isinstance(bookmark_data, dict) else "invalid_data",
                                               cleanup_action="deleting_corrupted_file")
                    
                    # Publish corruption metrics
                    if hasattr(self, 'metrics_publisher'):
                        self.metrics_publisher.publish_bookmark_corruption_metrics(
                            table_name, "validation_failed", False)
                    
                    # Delete corrupted file and return None to trigger full load fallback
                    try:
                        delete_success = await self.delete_bookmark(table_name)
                        cleanup_success = delete_success
                        
                        # Log cleanup result
                        self.structured_logger.log_corrupted_bookmark_cleanup(
                            table_name, s3_key, cleanup_success)
                        
                        # Update corruption metrics with cleanup result
                        if hasattr(self, 'metrics_publisher'):
                            self.metrics_publisher.publish_bookmark_corruption_metrics(
                                table_name, "validation_failed", cleanup_success)
                        
                    except Exception as delete_error:
                        self.structured_logger.log_corrupted_bookmark_cleanup(
                            table_name, s3_key, False, str(delete_error))
                        
                        if hasattr(self, 'metrics_publisher'):
                            self.metrics_publisher.publish_bookmark_corruption_metrics(
                                table_name, "validation_failed", False)
                    
                    # Log operation failure and publish metrics
                    self.structured_logger.log_s3_operation_failure(
                        "read", table_name, s3_key, duration_ms, 
                        "Bookmark validation failed", "corrupted_data")
                    
                    if hasattr(self, 'metrics_publisher'):
                        self.metrics_publisher.publish_s3_bookmark_metrics(
                            "read", table_name, False, duration_ms, "corrupted_data")
                    
                    return None
                
                # Log successful operation with performance metrics (Requirements 6.1, 6.3)
                file_size = response.get('ContentLength', 0)
                self.structured_logger.log_s3_operation_success(
                    "read", table_name, s3_key, duration_ms,
                    file_size_bytes=file_size,
                    bookmark_version=bookmark_data.get('version', 'unknown'))
                
                # Log bookmark state details (Requirement 6.2)
                self.structured_logger.log_bookmark_state_loaded(
                    table_name,
                    bookmark_data.get('last_processed_value'),
                    bookmark_data.get('last_update_timestamp'),
                    bookmark_data.get('is_first_run', True))
                
                # Publish success metrics (Requirements 6.4, 6.5)
                if hasattr(self, 'metrics_publisher'):
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
                    self.structured_logger.info("S3 bookmark file not found (expected for first run)",
                                              table_name=table_name, s3_key=s3_key)
                    return None
                elif not error_info['is_recoverable']:
                    # Permanent error - log failure and publish metrics
                    self.structured_logger.log_s3_operation_failure(
                        "read", table_name, s3_key, duration_ms, 
                        str(e), error_info['error_category'])
                    
                    if hasattr(self, 'metrics_publisher'):
                        self.metrics_publisher.publish_s3_bookmark_metrics(
                            "read", table_name, False, duration_ms, error_info['error_category'])
                        self.metrics_publisher.publish_bookmark_fallback_metrics(
                            table_name, "permanent_s3_error", error_info['error_category'])
                    
                    return None
                elif error_info['should_retry'] and attempt < self.config.retry_attempts:
                    # Retry with exponential backoff
                    wait_time = 2 ** (attempt - 1)
                    self.structured_logger.log_s3_operation_retry(
                        "read", table_name, s3_key, attempt, wait_time, error_info['error_category'])
                    await asyncio.sleep(wait_time)
                    continue
                else:
                    # Max retries reached - log final failure
                    self.structured_logger.log_s3_operation_failure(
                        "read", table_name, s3_key, duration_ms, 
                        f"Max retries reached: {str(e)}", error_info['error_category'])
                    
                    if hasattr(self, 'metrics_publisher'):
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
                
                self.structured_logger.error("Corrupted JSON detected in bookmark file, initiating cleanup",
                                           table_name=table_name,
                                           s3_key=s3_key,
                                           error=str(e),
                                           cleanup_action="deleting_corrupted_file")
                
                # Publish corruption detection metrics
                if hasattr(self, 'metrics_publisher'):
                    self.metrics_publisher.publish_bookmark_corruption_metrics(
                        table_name, "json_decode_error", False)
                
                # Delete corrupted file and return None to trigger full load fallback
                try:
                    delete_success = await self.delete_bookmark(table_name)
                    cleanup_success = delete_success
                    
                    # Log cleanup result
                    self.structured_logger.log_corrupted_bookmark_cleanup(
                        table_name, s3_key, cleanup_success)
                    
                    # Update corruption metrics with cleanup result
                    if hasattr(self, 'metrics_publisher'):
                        self.metrics_publisher.publish_bookmark_corruption_metrics(
                            table_name, "json_decode_error", cleanup_success)
                        
                except Exception as delete_error:
                    self.structured_logger.log_corrupted_bookmark_cleanup(
                        table_name, s3_key, False, str(delete_error))
                    
                    if hasattr(self, 'metrics_publisher'):
                        self.metrics_publisher.publish_bookmark_corruption_metrics(
                            table_name, "json_decode_error", False)
                
                # Log operation failure and publish metrics
                self.structured_logger.log_s3_operation_failure(
                    "read", table_name, s3_key, duration_ms, 
                    f"JSON decode error: {str(e)}", "corrupted_data")
                
                if hasattr(self, 'metrics_publisher'):
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
                    self.structured_logger.log_s3_operation_retry(
                        "read", table_name, s3_key, attempt, wait_time, error_info['error_category'])
                    await asyncio.sleep(wait_time)
                    continue
                else:
                    # Max retries reached or non-retryable error
                    self.structured_logger.log_s3_operation_failure(
                        "read", table_name, s3_key, duration_ms, 
                        f"Unexpected error: {str(e)}", error_info['error_category'])
                    
                    if hasattr(self, 'metrics_publisher'):
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
                self.structured_logger.error("Bookmark data is not a valid dictionary",
                                           table_name=table_name,
                                           data_type=type(bookmark_data).__name__)
                return False
            
            # Check for empty data
            if not bookmark_data:
                self.structured_logger.error("Bookmark data is empty",
                                           table_name=table_name)
                return False
            
            # Use the existing validation from JobBookmarkState
            if not JobBookmarkState._validate_s3_data(bookmark_data):
                self.structured_logger.error("Bookmark data failed JobBookmarkState validation",
                                           table_name=table_name,
                                           bookmark_keys=list(bookmark_data.keys()))
                return False
            
            # Additional corruption detection checks
            
            # Check for table name consistency
            if bookmark_data.get('table_name') != table_name:
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
                                    self.structured_logger.error("Created timestamp is after updated timestamp",
                                                               table_name=table_name,
                                                               created_timestamp=bookmark_data['created_timestamp'],
                                                               updated_timestamp=bookmark_data[field])
                                    return False
                                    
                    except (ValueError, TypeError) as e:
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
                    self.structured_logger.error("Suspiciously long string value detected",
                                               table_name=table_name,
                                               field=key,
                                               value_length=len(value))
                    return False
            
            # Check for null bytes or other control characters that might indicate corruption
            for key, value in bookmark_data.items():
                if isinstance(value, str) and ('\x00' in value or any(ord(c) < 32 and c not in '\t\n\r' for c in value)):
                    self.structured_logger.error("Control characters detected in bookmark data",
                                               table_name=table_name,
                                               field=key)
                    return False
            
            self.structured_logger.debug("Bookmark data validation passed all checks",
                                       table_name=table_name,
                                       strategy=strategy,
                                       column=column,
                                       is_first_run=bookmark_data.get('is_first_run', True))
            
            return True
            
        except Exception as e:
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
                self.structured_logger.info("S3 bookmark read returned None, triggering fallback logic",
                                          table_name=table_name)
                return None
                
        except Exception as e:
            # Unexpected error in the read process - implement fallback logic
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
        
        # Log start of parallel operation (Requirement 7.1)
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
                            self.structured_logger.debug("Parallel bookmark read successful",
                                                       table_name=table_name,
                                                       bookmark_found=True)
                        else:
                            self.structured_logger.debug("Parallel bookmark read completed - no bookmark found",
                                                       table_name=table_name,
                                                       bookmark_found=False)
                
            except asyncio.TimeoutError:
                # Handle timeout - cancel remaining tasks and collect partial results
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
            
            # Log completion of parallel operation (Requirement 7.1)
            self.structured_logger.log_parallel_s3_operation_complete(
                "read", len(table_names), successful_count, failed_count, 
                total_duration_ms, table_names=table_names
            )
            
            # Publish parallel operation metrics
            if hasattr(self, 'metrics_publisher'):
                self.metrics_publisher.publish_parallel_s3_metrics(
                    "read", len(table_names), successful_count, failed_count, total_duration_ms
                )
            
            return results
            
        except Exception as e:
            # Handle unexpected errors in parallel processing
            total_duration_ms = (time.time() - start_time) * 1000
            
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
            self.structured_logger.log_s3_operation_failure(
                "write", table_name, s3_key, duration_ms,
                f"JSON serialization failed: {str(e)}", "serialization_error")
            
            if hasattr(self, 'metrics_publisher'):
                self.metrics_publisher.publish_s3_bookmark_metrics(
                    "write", table_name, False, duration_ms, "serialization_error")
            
            return False
        
        # Log operation start (Requirement 6.1)
        self.structured_logger.log_s3_operation_start("write", table_name, s3_key,
                                                     bucket=self.config.bucket_name,
                                                     content_size_bytes=len(content_bytes))
        
        for attempt in range(1, self.config.retry_attempts + 1):
            try:
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
                
                # Log successful operation with performance metrics (Requirements 6.1, 6.3)
                self.structured_logger.log_s3_operation_success(
                    "write", table_name, s3_key, duration_ms,
                    file_size_bytes=len(content_bytes),
                    bookmark_version=bookmark_data.get('version', 'unknown'))
                
                # Log bookmark state details (Requirement 6.2)
                self.structured_logger.log_bookmark_state_saved(
                    table_name,
                    bookmark_data.get('last_processed_value'),
                    len(content_bytes),
                    bookmark_data.get('version', 'unknown'))
                
                # Publish success metrics (Requirements 6.4, 6.5)
                if hasattr(self, 'metrics_publisher'):
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
                    self.structured_logger.log_s3_operation_failure(
                        "write", table_name, s3_key, duration_ms, 
                        str(e), error_info['error_category'])
                    
                    if hasattr(self, 'metrics_publisher'):
                        self.metrics_publisher.publish_s3_bookmark_metrics(
                            "write", table_name, False, duration_ms, error_info['error_category'])
                    
                    return False
                elif error_info['should_retry'] and attempt < self.config.retry_attempts:
                    # Retry with exponential backoff
                    wait_time = 2 ** (attempt - 1)
                    self.structured_logger.log_s3_operation_retry(
                        "write", table_name, s3_key, attempt, wait_time, error_info['error_category'])
                    await asyncio.sleep(wait_time)
                    continue
                else:
                    # Max retries reached - log final failure
                    self.structured_logger.log_s3_operation_failure(
                        "write", table_name, s3_key, duration_ms, 
                        f"Max retries reached: {str(e)}", error_info['error_category'])
                    
                    if hasattr(self, 'metrics_publisher'):
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
                    self.structured_logger.log_s3_operation_retry(
                        "write", table_name, s3_key, attempt, wait_time, error_info['error_category'])
                    await asyncio.sleep(wait_time)
                    continue
                else:
                    # Max retries reached or non-retryable error
                    self.structured_logger.log_s3_operation_failure(
                        "write", table_name, s3_key, duration_ms, 
                        f"Unexpected error: {str(e)}", error_info['error_category'])
                    
                    if hasattr(self, 'metrics_publisher'):
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
                self.structured_logger.warning("S3 bookmark write failed, job execution will continue",
                                              table_name=table_name)
                return False
                
        except Exception as e:
            # Unexpected error in the write process - don't block job execution
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
        
        # Log start of batch operation (Requirement 7.2)
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
            self.structured_logger.info("Completed all bookmark write batches",
                                      total_operations=total_operations,
                                      successful_count=successful_count,
                                      failed_count=failed_count,
                                      total_duration_ms=total_duration_ms,
                                      batch_size=batch_size,
                                      batch_count=((total_operations - 1) // batch_size) + 1)
            
            # Publish batch operation metrics
            if hasattr(self, 'metrics_publisher'):
                self.metrics_publisher.publish_batch_s3_metrics(
                    "write", total_operations, successful_count, failed_count, 
                    total_duration_ms, batch_size
                )
            
            return results
            
        except Exception as e:
            # Handle unexpected errors in batch processing
            total_duration_ms = (time.time() - start_time) * 1000
            
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
        
        # Log operation start (Requirement 6.1)
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
            
            # Log successful operation with performance metrics (Requirements 6.1, 6.3)
            self.structured_logger.log_s3_operation_success(
                "delete", table_name, s3_key, duration_ms,
                reason="corrupted_file_cleanup")
            
            # Publish success metrics (Requirements 6.4, 6.5)
            if hasattr(self, 'metrics_publisher'):
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
                self.structured_logger.info("Bookmark file already doesn't exist (expected)",
                                          table_name=table_name,
                                          s3_key=s3_key,
                                          duration_ms=duration_ms)
                
                # Still publish success metrics since this is expected
                if hasattr(self, 'metrics_publisher'):
                    self.metrics_publisher.publish_s3_bookmark_metrics(
                        "delete", table_name, True, duration_ms)
                
                return True
            else:
                # Other errors - log failure and publish metrics
                self.structured_logger.log_s3_operation_failure(
                    "delete", table_name, s3_key, duration_ms, 
                    str(e), error_info['error_category'])
                
                if hasattr(self, 'metrics_publisher'):
                    self.metrics_publisher.publish_s3_bookmark_metrics(
                        "delete", table_name, False, duration_ms, error_info['error_category'])
                
                return False
        
        except Exception as e:
            # Calculate duration for error logging
            duration_ms = (time.time() - start_time) * 1000
            
            # Handle unexpected errors
            self.structured_logger.log_s3_operation_failure(
                "delete", table_name, s3_key, duration_ms, 
                f"Unexpected error: {str(e)}", "unexpected_error")
            
            if hasattr(self, 'metrics_publisher'):
                self.metrics_publisher.publish_s3_bookmark_metrics(
                    "delete", table_name, False, duration_ms, "unexpected_error")
            
            return False
                
        except Exception as e:
            # Use centralized error handling for unexpected errors
            error_info = self._handle_s3_error("delete", table_name, e)
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
                self.structured_logger.debug("Published S3 error metric",
                                           metric_name=metric_name,
                                           error_category=error_category,
                                           table_name=table_name)
        except Exception as e:
            # Don't fail the job for CloudWatch metric failures
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
            
            self.structured_logger.info("Listed bookmark files",
                                      count=len(table_names),
                                      tables=table_names)
            
            return table_names
            
        except Exception as e:
            self._handle_s3_error("list", "all_tables", e)
            return []


class S3PathUtilities:
    """Utility class for S3 path operations and bucket detection."""
    
    @staticmethod
    def extract_s3_bucket_name(s3_path: str) -> str:
        """
        Extract S3 bucket name from JDBC driver S3 path.
        
        Args:
            s3_path: S3 path in format s3://bucket-name/path/to/file.jar
            
        Returns:
            Bucket name extracted from the S3 path
            
        Raises:
            ValueError: If the S3 path format is invalid
            
        Examples:
            >>> S3PathUtilities.extract_s3_bucket_name("s3://my-bucket/drivers/oracle.jar")
            'my-bucket'
            >>> S3PathUtilities.extract_s3_bucket_name("s3://glue-assets-123/jdbc/sqlserver.jar")
            'glue-assets-123'
        """
        if not isinstance(s3_path, str):
            raise ValueError(f"S3 path must be a string, got {type(s3_path)}")
        
        if not s3_path.startswith('s3://'):
            raise ValueError(f"Invalid S3 path format: {s3_path}. Must start with 's3://'")
        
        # Remove s3:// prefix and split by /
        path_without_prefix = s3_path[5:]  # Remove 's3://'
        
        if not path_without_prefix:
            raise ValueError(f"Invalid S3 path format: {s3_path}. Missing bucket name")
        
        path_parts = path_without_prefix.split('/')
        
        if len(path_parts) < 1 or not path_parts[0]:
            raise ValueError(f"Invalid S3 path format: {s3_path}. Missing bucket name")
        
        bucket_name = path_parts[0]
        
        # Validate bucket name format (basic validation)
        if not S3PathUtilities._is_valid_bucket_name(bucket_name):
            raise ValueError(f"Invalid S3 bucket name: {bucket_name}")
        
        return bucket_name
    
    @staticmethod
    def _is_valid_bucket_name(bucket_name: str) -> bool:
        """
        Validate S3 bucket name format (basic validation).
        
        Args:
            bucket_name: Bucket name to validate
            
        Returns:
            True if bucket name appears valid, False otherwise
        """
        if not bucket_name:
            return False
        
        # Basic validation - bucket names must be 3-63 characters
        if len(bucket_name) < 3 or len(bucket_name) > 63:
            return False
        
        # Must start and end with alphanumeric character
        if not (bucket_name[0].isalnum() and bucket_name[-1].isalnum()):
            return False
        
        # Can contain lowercase letters, numbers, hyphens, and periods
        import re
        if not re.match(r'^[a-z0-9.-]+$', bucket_name):
            return False
        
        return True
    
    @staticmethod
    def generate_bookmark_s3_key(job_name: str, table_name: str, bookmark_prefix: str = "bookmarks") -> str:
        """
        Generate S3 key for bookmark file using job name and table name.
        
        Args:
            job_name: Name of the Glue job
            table_name: Name of the table
            bookmark_prefix: Prefix for bookmark files (default: "bookmarks")
            
        Returns:
            S3 key in format: bookmarks/{job_name}/{table_name}.json
            
        Raises:
            ValueError: If job_name or table_name is empty
            
        Examples:
            >>> S3PathUtilities.generate_bookmark_s3_key("customer-replication", "customers")
            'bookmarks/customer-replication/customers.json'
            >>> S3PathUtilities.generate_bookmark_s3_key("data-sync", "orders", "job-bookmarks")
            'job-bookmarks/data-sync/orders.json'
        """
        if not job_name or not job_name.strip():
            raise ValueError("Job name cannot be empty")
        
        if not table_name or not table_name.strip():
            raise ValueError("Table name cannot be empty")
        
        # Clean job name and table name (remove invalid characters)
        clean_job_name = S3PathUtilities._sanitize_s3_key_component(job_name.strip())
        clean_table_name = S3PathUtilities._sanitize_s3_key_component(table_name.strip())
        
        # Ensure bookmark_prefix ends with '/' for proper key structure
        if bookmark_prefix and not bookmark_prefix.endswith('/'):
            bookmark_prefix += '/'
        elif not bookmark_prefix:
            bookmark_prefix = "bookmarks/"
        
        return f"{bookmark_prefix}{clean_job_name}/{clean_table_name}.json"
    
    @staticmethod
    def _sanitize_s3_key_component(component: str) -> str:
        """
        Sanitize a component of an S3 key by replacing invalid characters.
        
        Args:
            component: String component to sanitize
            
        Returns:
            Sanitized component safe for use in S3 keys
        """
        import re
        # Replace spaces and special characters with hyphens
        sanitized = re.sub(r'[^a-zA-Z0-9._-]', '-', component)
        # Remove multiple consecutive hyphens
        sanitized = re.sub(r'-+', '-', sanitized)
        # Remove leading/trailing hyphens
        sanitized = sanitized.strip('-')
        return sanitized
    
    @staticmethod
    def validate_s3_path_format(s3_path: str) -> bool:
        """
        Validate S3 path format for JDBC driver paths.
        
        Args:
            s3_path: S3 path to validate
            
        Returns:
            True if path format is valid, False otherwise
            
        Examples:
            >>> S3PathUtilities.validate_s3_path_format("s3://my-bucket/drivers/oracle.jar")
            True
            >>> S3PathUtilities.validate_s3_path_format("invalid-path")
            False
        """
        try:
            # Check basic S3 path format
            if not s3_path.startswith('s3://'):
                return False
            
            # Parse URL to validate structure
            parsed = urlparse(s3_path)
            if not parsed.netloc or not parsed.path:
                return False
            
            # For JDBC drivers, expect .jar extension
            if not parsed.path.lower().endswith('.jar'):
                return False
            
            # Validate bucket name
            bucket_name = parsed.netloc
            if not S3PathUtilities._is_valid_bucket_name(bucket_name):
                return False
            
            return True
            
        except Exception:
            return False
    
    @staticmethod
    def detect_s3_bucket_from_jdbc_paths(source_jdbc_path: str, target_jdbc_path: str, 
                                        structured_logger: 'StructuredLogger' = None) -> str:
        """
        Detect S3 bucket for bookmark storage from JDBC driver paths.
        Prefers source JDBC driver bucket if different buckets are used.
        
        Args:
            source_jdbc_path: S3 path to source JDBC driver
            target_jdbc_path: S3 path to target JDBC driver
            structured_logger: Optional structured logger for enhanced logging
            
        Returns:
            S3 bucket name to use for bookmark storage
            
        Raises:
            ValueError: If both paths are invalid or buckets cannot be extracted
            
        Examples:
            >>> S3PathUtilities.detect_s3_bucket_from_jdbc_paths(
            ...     "s3://my-bucket/drivers/oracle.jar",
            ...     "s3://my-bucket/drivers/postgres.jar"
            ... )
            'my-bucket'
            >>> S3PathUtilities.detect_s3_bucket_from_jdbc_paths(
            ...     "s3://source-bucket/oracle.jar",
            ...     "s3://target-bucket/postgres.jar"
            ... )
            'source-bucket'
        """
        # Log bucket detection start (Requirement 6.2)
        if structured_logger:
            structured_logger.log_s3_bucket_detection_start(source_jdbc_path, target_jdbc_path)
        
        source_bucket = None
        target_bucket = None
        
        # Try to extract source bucket
        try:
            source_bucket = S3PathUtilities.extract_s3_bucket_name(source_jdbc_path)
        except ValueError as e:
            error_msg = f"Failed to extract bucket from source JDBC path '{source_jdbc_path}': {e}"
            if structured_logger:
                structured_logger.warning(error_msg, path_type="source", error=str(e))
            else:
                logger.warning(error_msg)
        
        # Try to extract target bucket
        try:
            target_bucket = S3PathUtilities.extract_s3_bucket_name(target_jdbc_path)
        except ValueError as e:
            error_msg = f"Failed to extract bucket from target JDBC path '{target_jdbc_path}': {e}"
            if structured_logger:
                structured_logger.warning(error_msg, path_type="target", error=str(e))
            else:
                logger.warning(error_msg)
        
        # Determine which bucket to use
        selected_bucket = None
        selection_reason = ""
        
        if source_bucket and target_bucket:
            if source_bucket == target_bucket:
                selected_bucket = source_bucket
                selection_reason = "common_bucket_detected"
            else:
                selected_bucket = source_bucket
                selection_reason = "different_buckets_prefer_source"
        elif source_bucket:
            selected_bucket = source_bucket
            selection_reason = "only_source_bucket_available"
        elif target_bucket:
            selected_bucket = target_bucket
            selection_reason = "only_target_bucket_available"
        else:
            error_msg = (f"Cannot extract valid S3 bucket from JDBC paths. "
                        f"Source: '{source_jdbc_path}', Target: '{target_jdbc_path}'")
            if structured_logger:
                structured_logger.error(error_msg, source_bucket=source_bucket, target_bucket=target_bucket)
            raise ValueError(error_msg)
        
        # Log successful bucket detection (Requirement 6.2)
        if structured_logger:
            structured_logger.log_s3_bucket_detection_success(
                selected_bucket, source_bucket, target_bucket, selection_reason)
        else:
            logger.info(f"Selected S3 bucket for bookmarks: {selected_bucket} (reason: {selection_reason})")
        
        return selected_bucket
    
    @staticmethod
    def validate_s3_bucket_accessibility(bucket_name: str, s3_client=None, 
                                       structured_logger: 'StructuredLogger' = None,
                                       metrics_publisher: 'CloudWatchMetricsPublisher' = None) -> bool:
        """
        Validate that the S3 bucket is accessible with current IAM permissions.
        
        Args:
            bucket_name: Name of the S3 bucket to validate
            s3_client: Optional boto3 S3 client (will create one if not provided)
            structured_logger: Optional structured logger for enhanced logging
            metrics_publisher: Optional metrics publisher for CloudWatch metrics
            
        Returns:
            True if bucket is accessible, False otherwise
        """
        start_time = time.time()
        
        # Log validation start (Requirement 6.2)
        if structured_logger:
            structured_logger.log_s3_bucket_validation_start(bucket_name)
        
        if not s3_client:
            try:
                s3_client = boto3.client('s3')
            except Exception as e:
                duration_ms = (time.time() - start_time) * 1000
                error_msg = f"Failed to create S3 client for bucket validation: {e}"
                
                if structured_logger:
                    structured_logger.log_s3_bucket_validation_failure(
                        bucket_name, duration_ms, error_msg, "s3_client_creation_failed")
                else:
                    logger.error(error_msg)
                
                if metrics_publisher:
                    metrics_publisher.publish_s3_bucket_validation_metrics(
                        bucket_name, False, duration_ms, "s3_client_creation_failed")
                
                return False
        
        try:
            # Try to list objects in the bucket (with limit to minimize cost)
            s3_client.list_objects_v2(Bucket=bucket_name, MaxKeys=1)
            
            # Calculate validation duration for performance logging
            duration_ms = (time.time() - start_time) * 1000
            
            # Log successful validation (Requirements 6.2, 6.3)
            if structured_logger:
                structured_logger.log_s3_bucket_validation_success(bucket_name, duration_ms)
            else:
                logger.debug(f"S3 bucket '{bucket_name}' is accessible")
            
            # Publish success metrics (Requirements 6.4, 6.5)
            if metrics_publisher:
                metrics_publisher.publish_s3_bucket_validation_metrics(
                    bucket_name, True, duration_ms)
            
            return True
            
        except ClientError as e:
            duration_ms = (time.time() - start_time) * 1000
            error_code = e.response.get('Error', {}).get('Code', 'Unknown')
            
            # Determine error category and message
            if error_code == 'NoSuchBucket':
                error_msg = f"S3 bucket '{bucket_name}' does not exist"
                error_category = "bucket_not_found"
            elif error_code == 'AccessDenied':
                error_msg = f"Access denied to S3 bucket '{bucket_name}'. Check IAM permissions"
                error_category = "access_denied"
            else:
                error_msg = f"Failed to access S3 bucket '{bucket_name}': {error_code}"
                error_category = "client_error"
            
            # Log validation failure (Requirements 6.2, 6.3)
            if structured_logger:
                structured_logger.log_s3_bucket_validation_failure(
                    bucket_name, duration_ms, error_msg, "fallback_to_in_memory_bookmarks")
            else:
                logger.error(error_msg)
            
            # Publish failure metrics (Requirements 6.4, 6.5)
            if metrics_publisher:
                metrics_publisher.publish_s3_bucket_validation_metrics(
                    bucket_name, False, duration_ms, error_category)
            
            return False
            
        except Exception as e:
            duration_ms = (time.time() - start_time) * 1000
            error_msg = f"Unexpected error validating S3 bucket '{bucket_name}': {e}"
            
            # Log validation failure (Requirements 6.2, 6.3)
            if structured_logger:
                structured_logger.log_s3_bucket_validation_failure(
                    bucket_name, duration_ms, error_msg, "fallback_to_in_memory_bookmarks")
            else:
                logger.error(error_msg)
            
            # Publish failure metrics (Requirements 6.4, 6.5)
            if metrics_publisher:
                metrics_publisher.publish_s3_bucket_validation_metrics(
                    bucket_name, False, duration_ms, "unexpected_error")
            
            return False
    
    @staticmethod
    def create_bookmark_s3_path(bucket_name: str, job_name: str, table_name: str, 
                               bookmark_prefix: str = "bookmarks") -> str:
        """
        Create complete S3 path for bookmark file.
        
        Args:
            bucket_name: S3 bucket name
            job_name: Name of the Glue job
            table_name: Name of the table
            bookmark_prefix: Prefix for bookmark files (default: "bookmarks")
            
        Returns:
            Complete S3 path for bookmark file
            
        Examples:
            >>> S3PathUtilities.create_bookmark_s3_path("my-bucket", "job1", "customers")
            's3://my-bucket/bookmarks/job1/customers.json'
        """
        s3_key = S3PathUtilities.generate_bookmark_s3_key(job_name, table_name, bookmark_prefix)
        return f"s3://{bucket_name}/{s3_key}"


@dataclass
class ProcessingMetrics:
    """Data class to track processing metrics for a table."""
    table_name: str
    start_time: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    end_time: Optional[datetime] = None
    rows_processed: int = 0
    rows_inserted: int = 0
    rows_updated: int = 0
    rows_failed: int = 0
    bytes_processed: int = 0
    processing_duration_seconds: float = 0.0
    status: str = "in_progress"  # in_progress, completed, failed
    error_message: Optional[str] = None
    
    def mark_completed(self, rows_processed: int = 0, bytes_processed: int = 0):
        """Mark processing as completed and calculate duration."""
        self.end_time = datetime.now(timezone.utc)
        self.processing_duration_seconds = (self.end_time - self.start_time).total_seconds()
        self.rows_processed = rows_processed
        self.bytes_processed = bytes_processed
        self.status = "completed"
    
    def mark_failed(self, error_message: str):
        """Mark processing as failed."""
        self.end_time = datetime.now(timezone.utc)
        self.processing_duration_seconds = (self.end_time - self.start_time).total_seconds()
        self.status = "failed"
        self.error_message = error_message
    
    def get_throughput_rows_per_second(self) -> float:
        """Calculate rows processed per second."""
        if self.processing_duration_seconds > 0:
            return self.rows_processed / self.processing_duration_seconds
        return 0.0
    
    def get_throughput_mb_per_second(self) -> float:
        """Calculate MB processed per second."""
        if self.processing_duration_seconds > 0:
            return (self.bytes_processed / 1024 / 1024) / self.processing_duration_seconds
        return 0.0


class StructuredLogger:
    """Enhanced logger with structured logging and contextual information."""
    
    def __init__(self, job_name: str, logger_instance: logging.Logger = None):
        self.job_name = job_name
        self.logger = logger_instance or logger
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
    
    def log_table_processing_complete(self, table_name: str, operation: str, metrics: ProcessingMetrics):
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
    
    def log_table_processing_failed(self, table_name: str, operation: str, error: str, metrics: ProcessingMetrics):
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


class CloudWatchMetricsPublisher:
    """Publishes metrics to CloudWatch for monitoring and alerting."""
    
    def __init__(self, job_name: str, namespace: str = "AWS/Glue/DataReplication"):
        self.job_name = job_name
        self.namespace = namespace
        self.cloudwatch = cloudwatch
        self.metrics_buffer = []
        self.structured_logger = StructuredLogger(job_name)
    
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
    
    # Parallel and Batch Operation Metrics (for future performance optimizations)
    def publish_parallel_s3_bookmark_metrics(self, operation: str, table_count: int, 
                                            successful_count: int, failed_count: int, 
                                            total_duration_ms: float):
        """Publish metrics for parallel S3 bookmark operations."""
        dimensions = {'Operation': operation}
        
        # Parallel operation counts
        self.put_metric('ParallelS3BookmarkOperations', table_count, 'Count', dimensions)
        self.put_metric('ParallelS3BookmarkSuccess', successful_count, 'Count', dimensions)
        self.put_metric('ParallelS3BookmarkFailure', failed_count, 'Count', dimensions)
        
        # Parallel operation performance
        self.put_metric('ParallelS3BookmarkTotalLatency', total_duration_ms, 'Milliseconds', dimensions)
        if table_count > 0:
            self.put_metric('ParallelS3BookmarkAverageLatency', 
                           total_duration_ms / table_count, 'Milliseconds', dimensions)
        
        # Success rate
        if table_count > 0:
            success_rate = (successful_count / table_count) * 100
            self.put_metric('ParallelS3BookmarkSuccessRate', success_rate, 'Percent', dimensions)
        
        self.structured_logger.debug("Published parallel S3 bookmark metrics", 
                                   operation=operation, table_count=table_count, 
                                   successful_count=successful_count, failed_count=failed_count)
    
    def publish_batch_s3_bookmark_metrics(self, operation: str, batch_number: int, 
                                        batch_size: int, successful_count: int, 
                                        failed_count: int, batch_duration_ms: float):
        """Publish metrics for batch S3 bookmark operations."""
        dimensions = {
            'Operation': operation,
            'BatchNumber': str(batch_number)
        }
        
        # Batch operation counts
        self.put_metric('BatchS3BookmarkOperations', batch_size, 'Count', dimensions)
        self.put_metric('BatchS3BookmarkSuccess', successful_count, 'Count', dimensions)
        self.put_metric('BatchS3BookmarkFailure', failed_count, 'Count', dimensions)
        
        # Batch operation performance
        self.put_metric('BatchS3BookmarkLatency', batch_duration_ms, 'Milliseconds', dimensions)
        if batch_size > 0:
            self.put_metric('BatchS3BookmarkAverageLatency', 
                           batch_duration_ms / batch_size, 'Milliseconds', dimensions)
        
        # Batch success rate
        if batch_size > 0:
            success_rate = (successful_count / batch_size) * 100
            self.put_metric('BatchS3BookmarkSuccessRate', success_rate, 'Percent', dimensions)
        
        self.structured_logger.debug("Published batch S3 bookmark metrics", 
                                   operation=operation, batch_number=batch_number, 
                                   batch_size=batch_size, successful_count=successful_count)
    
    # S3 Performance and Resource Usage Metrics
    def publish_s3_performance_summary_metrics(self, operation: str, total_operations: int, 
                                             total_duration_ms: float, total_bytes: int, 
                                             success_count: int, failure_count: int):
        """Publish comprehensive performance summary metrics for S3 operations."""
        dimensions = {'Operation': operation}
        
        # Operation counts and success rates
        self.put_metric('S3BookmarkTotalOperations', total_operations, 'Count', dimensions)
        self.put_metric('S3BookmarkTotalSuccess', success_count, 'Count', dimensions)
        self.put_metric('S3BookmarkTotalFailure', failure_count, 'Count', dimensions)
        
        if total_operations > 0:
            success_rate = (success_count / total_operations) * 100
            self.put_metric('S3BookmarkOverallSuccessRate', success_rate, 'Percent', dimensions)
        
        # Performance metrics
        self.put_metric('S3BookmarkTotalLatency', total_duration_ms, 'Milliseconds', dimensions)
        if total_operations > 0:
            self.put_metric('S3BookmarkAverageLatency', 
                           total_duration_ms / total_operations, 'Milliseconds', dimensions)
        
        # Throughput metrics
        if total_duration_ms > 0:
            duration_seconds = total_duration_ms / 1000
            self.put_metric('S3BookmarkThroughputOperationsPerSec', 
                           total_operations / duration_seconds, 'Count/Second', dimensions)
            
            if total_bytes > 0:
                throughput_bytes_per_sec = total_bytes / duration_seconds
                self.put_metric('S3BookmarkThroughputBytesPerSec', 
                               throughput_bytes_per_sec, 'Bytes/Second', dimensions)
        
        # Data volume metrics
        if total_bytes > 0:
            self.put_metric('S3BookmarkTotalBytes', total_bytes, 'Bytes', dimensions)
        
        self.structured_logger.debug("Published S3 performance summary metrics", 
                                   operation=operation, total_operations=total_operations, 
                                   success_count=success_count)
    
    def publish_s3_resource_usage_metrics(self, operation: str, table_name: str, 
                                        file_size_bytes: int, request_count: int, 
                                        retry_count: int = 0):
        """Publish S3 resource usage metrics for cost and performance monitoring."""
        dimensions = {
            'Operation': operation,
            'TableName': table_name
        }
        
        # File size metrics
        self.put_metric('S3BookmarkFileSize', file_size_bytes, 'Bytes', dimensions)
        
        # Request metrics
        self.put_metric('S3BookmarkRequestCount', request_count, 'Count', dimensions)
        self.put_metric('S3BookmarkRetryCount', retry_count, 'Count', dimensions)
        self.put_metric('S3BookmarkTotalRequests', request_count + retry_count, 'Count', dimensions)
        
        # Efficiency metrics
        if request_count > 0:
            retry_rate = (retry_count / request_count) * 100
            self.put_metric('S3BookmarkRetryRate', retry_rate, 'Percent', dimensions)
        
        # Cost estimation metrics (for monitoring purposes)
        storage_cost_gb_month = 0.023  # Rough S3 standard storage cost per GB per month
        estimated_monthly_cost = (file_size_bytes / 1024 / 1024 / 1024) * storage_cost_gb_month
        self.put_metric('S3BookmarkEstimatedMonthlyCost', estimated_monthly_cost, 'None', dimensions)
        
        self.structured_logger.debug("Published S3 resource usage metrics", 
                                   operation=operation, table_name=table_name, 
                                   file_size_bytes=file_size_bytes, request_count=request_count)
    
    def publish_s3_performance_trend_metrics(self, operation: str, current_duration_ms: float, 
                                           historical_average_ms: float, table_count: int):
        """Publish S3 operation performance trend metrics for capacity planning."""
        dimensions = {'Operation': operation}
        
        # Current performance metrics
        self.put_metric('S3BookmarkCurrentLatency', current_duration_ms, 'Milliseconds', dimensions)
        self.put_metric('S3BookmarkHistoricalAverageLatency', historical_average_ms, 'Milliseconds', dimensions)
        
        # Performance change metrics
        if historical_average_ms > 0:
            performance_change = ((current_duration_ms - historical_average_ms) / historical_average_ms) * 100
            self.put_metric('S3BookmarkPerformanceChange', performance_change, 'Percent', dimensions)
            
            # Performance status indicators
            if performance_change < -5:
                self.put_metric('S3BookmarkPerformanceImproving', 1, 'Count', dimensions)
                self.put_metric('S3BookmarkPerformanceDegrading', 0, 'Count', dimensions)
            elif performance_change > 5:
                self.put_metric('S3BookmarkPerformanceImproving', 0, 'Count', dimensions)
                self.put_metric('S3BookmarkPerformanceDegrading', 1, 'Count', dimensions)
            else:
                self.put_metric('S3BookmarkPerformanceImproving', 0, 'Count', dimensions)
                self.put_metric('S3BookmarkPerformanceDegrading', 0, 'Count', dimensions)
        
        # Workload metrics
        self.put_metric('S3BookmarkCurrentTableCount', table_count, 'Count', dimensions)
        
        self.structured_logger.debug("Published S3 performance trend metrics", 
                                   operation=operation, current_duration_ms=current_duration_ms, 
                                   historical_average_ms=historical_average_ms)
    
    def publish_parallel_s3_metrics(self, operation: str, table_count: int, 
                                   successful_count: int, failed_count: int, 
                                   total_duration_ms: float):
        """Publish metrics for parallel S3 bookmark operations."""
        dimensions = {'Operation': operation, 'ExecutionType': 'parallel'}
        
        # Parallel operation counts
        self.put_metric('S3BookmarkParallelOperations', table_count, 'Count', dimensions)
        self.put_metric('S3BookmarkParallelSuccess', successful_count, 'Count', dimensions)
        self.put_metric('S3BookmarkParallelFailure', failed_count, 'Count', dimensions)
        
        # Parallel operation performance
        self.put_metric('S3BookmarkParallelTotalLatency', total_duration_ms, 'Milliseconds', dimensions)
        if table_count > 0:
            self.put_metric('S3BookmarkParallelAverageLatency', 
                           total_duration_ms / table_count, 'Milliseconds', dimensions)
        
        # Success rate
        if table_count > 0:
            success_rate = (successful_count / table_count) * 100
            self.put_metric('S3BookmarkParallelSuccessRate', success_rate, 'Percent', dimensions)
        
        self.structured_logger.debug("Published parallel S3 metrics", 
                                   operation=operation, table_count=table_count, 
                                   successful_count=successful_count, failed_count=failed_count)
    
    def publish_batch_s3_metrics(self, operation: str, total_operations: int, 
                                successful_count: int, failed_count: int, 
                                total_duration_ms: float, batch_size: int):
        """Publish metrics for batch S3 bookmark operations."""
        dimensions = {'Operation': operation, 'ExecutionType': 'batch'}
        
        # Batch operation counts
        self.put_metric('S3BookmarkBatchOperations', total_operations, 'Count', dimensions)
        self.put_metric('S3BookmarkBatchSuccess', successful_count, 'Count', dimensions)
        self.put_metric('S3BookmarkBatchFailure', failed_count, 'Count', dimensions)
        
        # Batch configuration metrics
        self.put_metric('S3BookmarkBatchSize', batch_size, 'Count', dimensions)
        batch_count = ((total_operations - 1) // batch_size) + 1 if total_operations > 0 else 0
        self.put_metric('S3BookmarkBatchCount', batch_count, 'Count', dimensions)
        
        # Batch operation performance
        self.put_metric('S3BookmarkBatchTotalLatency', total_duration_ms, 'Milliseconds', dimensions)
        if total_operations > 0:
            self.put_metric('S3BookmarkBatchAverageLatency', 
                           total_duration_ms / total_operations, 'Milliseconds', dimensions)
        
        # Success rate
        if total_operations > 0:
            success_rate = (successful_count / total_operations) * 100
            self.put_metric('S3BookmarkBatchSuccessRate', success_rate, 'Percent', dimensions)
        
        self.structured_logger.debug("Published batch S3 metrics", 
                                   operation=operation, total_operations=total_operations, 
                                   successful_count=successful_count, failed_count=failed_count,
                                   batch_size=batch_size)


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


@dataclass
class NetworkConfig:
    """Network configuration for cross-VPC database connections."""
    vpc_id: Optional[str] = None
    subnet_ids: Optional[List[str]] = None
    security_group_ids: Optional[List[str]] = None
    glue_connection_name: Optional[str] = None
    create_s3_vpc_endpoint: bool = False
    
    def has_network_config(self) -> bool:
        """Check if network configuration is provided."""
        return bool(self.vpc_id and self.subnet_ids and self.security_group_ids)
    
    def requires_glue_connection(self) -> bool:
        """Check if Glue connection is required for cross-VPC access."""
        return self.has_network_config() and bool(self.glue_connection_name)


@dataclass
class ConnectionConfig:
    """Configuration for database connection."""
    engine_type: str
    connection_string: str
    database: str
    schema: str
    username: str
    password: str
    jdbc_driver_path: str
    network_config: Optional[NetworkConfig] = None
    
    def __post_init__(self):
        """Validate connection configuration after initialization."""
        self.validate()
    
    def validate(self) -> None:
        """Validate connection configuration parameters."""
        if not self.engine_type:
            raise ValueError("Engine type cannot be empty")
        if not self.connection_string:
            raise ValueError("Connection string cannot be empty")
        if not self.database:
            raise ValueError("Database name cannot be empty")
        if not self.schema:
            raise ValueError("Schema name cannot be empty")
        if not self.username:
            raise ValueError("Username cannot be empty")
        if not self.password:
            raise ValueError("Password cannot be empty")
        if not self.jdbc_driver_path:
            raise ValueError("JDBC driver path cannot be empty")
    
    def requires_cross_vpc_connection(self) -> bool:
        """Check if this connection requires cross-VPC connectivity."""
        return self.network_config and self.network_config.requires_glue_connection()
    
    def get_glue_connection_name(self) -> Optional[str]:
        """Get the Glue connection name if configured."""
        return self.network_config.glue_connection_name if self.network_config else None


@dataclass
class JobConfig:
    """Main job configuration containing all parameters."""
    job_name: str
    source_connection: ConnectionConfig
    target_connection: ConnectionConfig
    tables: List[str]
    validate_connections: bool = True
    connection_timeout_seconds: int = 30
    
    def __post_init__(self):
        """Validate job configuration after initialization."""
        self.validate()
    
    def validate(self) -> None:
        """Validate job configuration parameters."""
        if not self.job_name:
            raise ValueError("Job name cannot be empty")
        if not self.tables:
            raise ValueError("Table list cannot be empty")
        
        # Validate connection configurations
        self.source_connection.validate()
        self.target_connection.validate()
    
    def has_cross_vpc_connections(self) -> bool:
        """Check if any connections require cross-VPC connectivity."""
        return (self.source_connection.requires_cross_vpc_connection() or 
                self.target_connection.requires_cross_vpc_connection())
    
    def get_network_summary(self) -> Dict[str, Any]:
        """Get summary of network configuration."""
        return {
            'source_cross_vpc': self.source_connection.requires_cross_vpc_connection(),
            'target_cross_vpc': self.target_connection.requires_cross_vpc_connection(),
            'source_glue_connection': self.source_connection.get_glue_connection_name(),
            'target_glue_connection': self.target_connection.get_glue_connection_name(),
            'validate_connections': self.validate_connections,
            'connection_timeout': self.connection_timeout_seconds
        }


class DatabaseEngineManager:
    """Manages database engine configurations and JDBC driver loading."""
    
    # Database engine configurations
    ENGINE_CONFIGS = {
        'oracle': {
            'driver_class': 'oracle.jdbc.OracleDriver',
            'url_template': 'jdbc:oracle:thin:@{host}:{port}:{database}',
            'default_port': 1521
        },
        'sqlserver': {
            'driver_class': 'com.microsoft.sqlserver.jdbc.SQLServerDriver',
            'url_template': 'jdbc:sqlserver://{host}:{port};databaseName={database}',
            'default_port': 1433
        },
        'postgresql': {
            'driver_class': 'org.postgresql.Driver',
            'url_template': 'jdbc:postgresql://{host}:{port}/{database}',
            'default_port': 5432
        },
        'db2': {
            'driver_class': 'com.ibm.db2.jcc.DB2Driver',
            'url_template': 'jdbc:db2://{host}:{port}/{database}',
            'default_port': 50000
        }
    }
    
    @classmethod
    def get_supported_engines(cls) -> List[str]:
        """Get list of supported database engines."""
        return list(cls.ENGINE_CONFIGS.keys())
    
    @classmethod
    def is_engine_supported(cls, engine_type: str) -> bool:
        """Check if database engine is supported."""
        return engine_type.lower() in cls.ENGINE_CONFIGS
    
    @classmethod
    def get_driver_class(cls, engine_type: str) -> str:
        """Get JDBC driver class for the specified engine."""
        engine_type = engine_type.lower()
        if not cls.is_engine_supported(engine_type):
            raise ValueError(f"Unsupported database engine: {engine_type}")
        return cls.ENGINE_CONFIGS[engine_type]['driver_class']
    
    @classmethod
    def validate_connection_string(cls, engine_type: str, connection_string: str) -> bool:
        """Validate connection string format for the specified engine."""
        engine_type = engine_type.lower()
        if not cls.is_engine_supported(engine_type):
            return False
        
        # Basic validation - check if connection string starts with expected JDBC URL prefix
        expected_prefixes = {
            'oracle': 'jdbc:oracle:thin:@',
            'sqlserver': 'jdbc:sqlserver://',
            'postgresql': 'jdbc:postgresql://',
            'db2': 'jdbc:db2://'
        }
        
        return connection_string.startswith(expected_prefixes[engine_type])


class JdbcDriverLoader:
    """Handles JDBC driver loading and validation."""
    
    def __init__(self, spark_context: SparkContext):
        self.spark_context = spark_context
        self.loaded_drivers = set()
    
    def load_driver(self, engine_type: str, driver_s3_path: str) -> None:
        """Load JDBC driver from S3 path."""
        if not driver_s3_path.startswith('s3://'):
            raise ValueError(f"JDBC driver path must be an S3 URL: {driver_s3_path}")
        
        if driver_s3_path in self.loaded_drivers:
            logger.info(f"JDBC driver already loaded: {driver_s3_path}")
            return
        
        try:
            # Add the JAR file to Spark context
            self.spark_context.addPyFile(driver_s3_path)
            self.loaded_drivers.add(driver_s3_path)
            logger.info(f"Successfully loaded JDBC driver for {engine_type}: {driver_s3_path}")
        except Exception as e:
            raise RuntimeError(f"Failed to load JDBC driver from {driver_s3_path}: {str(e)}")
    
    def validate_driver_path(self, driver_s3_path: str) -> bool:
        """Validate S3 driver path format."""
        if not driver_s3_path.startswith('s3://'):
            return False
        
        # Parse S3 URL to validate format
        try:
            parsed = urlparse(driver_s3_path)
            return bool(parsed.netloc and parsed.path and parsed.path.endswith('.jar'))
        except Exception:
            return False


class JobConfigurationParser:
    """Parses job configuration from CloudFormation parameters."""
    
    # Required CloudFormation parameters
    REQUIRED_PARAMS = [
        'JOB_NAME',
        'SOURCE_ENGINE_TYPE',
        'TARGET_ENGINE_TYPE',
        'SOURCE_DATABASE',
        'TARGET_DATABASE',
        'SOURCE_SCHEMA',
        'TARGET_SCHEMA',
        'TABLE_NAMES',
        'SOURCE_DB_USER',
        'SOURCE_DB_PASSWORD',
        'TARGET_DB_USER',
        'TARGET_DB_PASSWORD',
        'SOURCE_JDBC_DRIVER_S3_PATH',
        'TARGET_JDBC_DRIVER_S3_PATH',
        'SOURCE_CONNECTION_STRING',
        'TARGET_CONNECTION_STRING'
    ]
    
    # Optional network configuration parameters
    OPTIONAL_NETWORK_PARAMS = [
        'SOURCE_VPC_ID',
        'SOURCE_SUBNET_IDS',
        'SOURCE_SECURITY_GROUP_IDS',
        'CREATE_SOURCE_S3_VPC_ENDPOINT',
        'TARGET_VPC_ID',
        'TARGET_SUBNET_IDS',
        'TARGET_SECURITY_GROUP_IDS',
        'CREATE_TARGET_S3_VPC_ENDPOINT',
        'SOURCE_GLUE_CONNECTION_NAME',
        'TARGET_GLUE_CONNECTION_NAME',
        'VALIDATE_CONNECTIONS',
        'CONNECTION_TIMEOUT_SECONDS'
    ]
    
    @classmethod
    def parse_job_arguments(cls) -> Dict[str, str]:
        """Parse job arguments from CloudFormation parameters with robust error handling."""
        try:
            logger.info(f"Starting argument parsing. Command line length: {len(sys.argv)}")
            
            # First, try the standard Glue approach
            all_params = cls.REQUIRED_PARAMS + cls.OPTIONAL_NETWORK_PARAMS
            
            try:
                # Parse all parameters at once with getResolvedOptions
                args = getResolvedOptions(sys.argv, all_params)
                logger.info(f"Successfully parsed all parameters using getResolvedOptions")
                
                # Set defaults for optional parameters that might be empty
                for param in cls.OPTIONAL_NETWORK_PARAMS:
                    if param not in args or args[param] is None:
                        args[param] = ''
                
            except Exception as e:
                logger.warning(f"getResolvedOptions failed: {str(e)}, trying manual parsing")
                
                # Fallback to manual parsing if getResolvedOptions fails
                args = cls._manual_parse_arguments(sys.argv)
            
            # Validate that all required parameters are present
            missing_params = [param for param in cls.REQUIRED_PARAMS if param not in args or not args[param]]
            if missing_params:
                raise RuntimeError(f"Missing required parameters: {missing_params}")
            
            # Set defaults for specific parameters
            args.setdefault('VALIDATE_CONNECTIONS', 'true')
            args.setdefault('CONNECTION_TIMEOUT_SECONDS', '30')
            
            # Log final parsed arguments (excluding sensitive data)
            safe_args = {k: v if 'PASSWORD' not in k else '***' for k, v in args.items()}
            logger.info(f"Successfully parsed {len(args)} arguments")
            logger.debug(f"Parsed arguments: {safe_args}")
            
            return args
            
        except Exception as e:
            logger.error(f"CRITICAL: Failed to parse job arguments: {str(e)}")
            logger.error(f"Command line arguments: {sys.argv}")
            raise RuntimeError(f"Job argument parsing failed: {str(e)}")
    
    @classmethod
    def _manual_parse_arguments(cls, argv: List[str]) -> Dict[str, str]:
        """Manually parse command line arguments as fallback."""
        args = {}
        i = 0
        
        while i < len(argv):
            arg = argv[i]
            
            # Look for our custom parameters (start with --)
            if arg.startswith('--') and len(arg) > 2:
                param_name = arg[2:]  # Remove --
                
                # Check if this is one of our expected parameters
                if param_name in cls.REQUIRED_PARAMS + cls.OPTIONAL_NETWORK_PARAMS:
                    # Get the next argument as the value
                    if i + 1 < len(argv) and not argv[i + 1].startswith('--'):
                        args[param_name] = argv[i + 1]
                        i += 2  # Skip both parameter and value
                    else:
                        # Parameter without value, set as empty string
                        args[param_name] = ''
                        i += 1
                else:
                    # Skip unknown parameters
                    i += 1
            else:
                i += 1
        
        logger.info(f"Manual parsing found {len(args)} parameters")
        return args
    
    @classmethod
    def parse_network_config(cls, args: Dict[str, str], prefix: str) -> Optional[NetworkConfig]:
        """Parse network configuration from CloudFormation parameters.
        
        Args:
            args: Parsed job arguments
            prefix: 'SOURCE' or 'TARGET' to identify which network config to parse
            
        Returns:
            NetworkConfig if network parameters are provided, None otherwise
        """
        vpc_id = args.get(f'{prefix}_VPC_ID', '').strip()
        subnet_ids_str = args.get(f'{prefix}_SUBNET_IDS', '').strip()
        security_group_ids_str = args.get(f'{prefix}_SECURITY_GROUP_IDS', '').strip()
        glue_connection_name = args.get(f'{prefix}_GLUE_CONNECTION_NAME', '').strip()
        create_s3_vpc_endpoint = args.get(f'CREATE_{prefix}_S3_VPC_ENDPOINT', 'NO').upper() == 'YES'
        
        # If no network configuration provided, return None
        if not vpc_id and not subnet_ids_str and not security_group_ids_str and not glue_connection_name:
            return None
        
        # Parse comma-separated lists
        subnet_ids = [s.strip() for s in subnet_ids_str.split(',') if s.strip()] if subnet_ids_str else None
        security_group_ids = [s.strip() for s in security_group_ids_str.split(',') if s.strip()] if security_group_ids_str else None
        
        return NetworkConfig(
            vpc_id=vpc_id if vpc_id else None,
            subnet_ids=subnet_ids,
            security_group_ids=security_group_ids,
            glue_connection_name=glue_connection_name if glue_connection_name else None,
            create_s3_vpc_endpoint=create_s3_vpc_endpoint
        )
    
    @classmethod
    def create_job_config(cls, args: Dict[str, str]) -> JobConfig:
        """Create JobConfig from parsed arguments."""
        try:
            # Parse table names (comma-separated)
            table_names = [table.strip() for table in args['TABLE_NAMES'].split(',') if table.strip()]
            
            # Parse network configurations
            source_network_config = cls.parse_network_config(args, 'SOURCE')
            target_network_config = cls.parse_network_config(args, 'TARGET')
            
            # Create source connection config
            source_connection = ConnectionConfig(
                engine_type=args['SOURCE_ENGINE_TYPE'].lower(),
                connection_string=args['SOURCE_CONNECTION_STRING'],
                database=args['SOURCE_DATABASE'],
                schema=args['SOURCE_SCHEMA'],
                username=args['SOURCE_DB_USER'],
                password=args['SOURCE_DB_PASSWORD'],
                jdbc_driver_path=args['SOURCE_JDBC_DRIVER_S3_PATH'],
                network_config=source_network_config
            )
            
            # Create target connection config
            target_connection = ConnectionConfig(
                engine_type=args['TARGET_ENGINE_TYPE'].lower(),
                connection_string=args['TARGET_CONNECTION_STRING'],
                database=args['TARGET_DATABASE'],
                schema=args['TARGET_SCHEMA'],
                username=args['TARGET_DB_USER'],
                password=args['TARGET_DB_PASSWORD'],
                jdbc_driver_path=args['TARGET_JDBC_DRIVER_S3_PATH'],
                network_config=target_network_config
            )
            
            # Parse connection validation settings
            validate_connections = args.get('VALIDATE_CONNECTIONS', 'true').lower() == 'true'
            connection_timeout_seconds = int(args.get('CONNECTION_TIMEOUT_SECONDS', '30'))
            
            # Create job config
            job_config = JobConfig(
                job_name=args['JOB_NAME'],
                source_connection=source_connection,
                target_connection=target_connection,
                tables=table_names,
                validate_connections=validate_connections,
                connection_timeout_seconds=connection_timeout_seconds
            )
            
            logger.info(f"Created job configuration for: {job_config.job_name}")
            
            # Log network configuration summary
            network_summary = job_config.get_network_summary()
            if job_config.has_cross_vpc_connections():
                logger.info(f"Cross-VPC network configuration detected: {network_summary}")
            else:
                logger.info("Using same-VPC connectivity (no cross-VPC configuration)")
            
            return job_config
            
        except Exception as e:
            logger.error(f"Failed to create job configuration: {str(e)}")
            raise RuntimeError(f"Invalid job configuration: {str(e)}")
    
    @classmethod
    def validate_configuration(cls, job_config: JobConfig) -> None:
        """Validate the complete job configuration."""
        # Validate engine types
        if not DatabaseEngineManager.is_engine_supported(job_config.source_connection.engine_type):
            raise ValueError(f"Unsupported source engine: {job_config.source_connection.engine_type}")
        
        if not DatabaseEngineManager.is_engine_supported(job_config.target_connection.engine_type):
            raise ValueError(f"Unsupported target engine: {job_config.target_connection.engine_type}")
        
        # Validate connection strings
        if not DatabaseEngineManager.validate_connection_string(
            job_config.source_connection.engine_type,
            job_config.source_connection.connection_string
        ):
            raise ValueError("Invalid source connection string format")
        
        if not DatabaseEngineManager.validate_connection_string(
            job_config.target_connection.engine_type,
            job_config.target_connection.connection_string
        ):
            raise ValueError("Invalid target connection string format")
        
        # Validate network configurations
        cls.validate_network_configuration(job_config)
        
        logger.info("Job configuration validation completed successfully")
    
    @classmethod
    def validate_network_configuration(cls, job_config: JobConfig) -> None:
        """Validate network configuration parameters."""
        # Validate source network configuration
        if job_config.source_connection.network_config:
            cls._validate_single_network_config(
                job_config.source_connection.network_config, "source"
            )
        
        # Validate target network configuration
        if job_config.target_connection.network_config:
            cls._validate_single_network_config(
                job_config.target_connection.network_config, "target"
            )
        
        logger.info("Network configuration validation completed")
    
    @classmethod
    def _validate_single_network_config(cls, network_config: NetworkConfig, connection_type: str) -> None:
        """Validate a single network configuration."""
        # If Glue connection name is provided, it should be sufficient
        if network_config.glue_connection_name:
            logger.info(f"Using Glue connection for {connection_type}: {network_config.glue_connection_name}")
            return
        
        # If network details are provided, validate completeness
        if network_config.has_network_config():
            if not network_config.vpc_id:
                raise ValueError(f"VPC ID is required for {connection_type} network configuration")
            if not network_config.subnet_ids:
                raise ValueError(f"Subnet IDs are required for {connection_type} network configuration")
            if not network_config.security_group_ids:
                raise ValueError(f"Security Group IDs are required for {connection_type} network configuration")
            
            logger.info(f"Network configuration validated for {connection_type}: VPC {network_config.vpc_id}")
        
        # Warn if partial network configuration is provided
        if (network_config.vpc_id or network_config.subnet_ids or network_config.security_group_ids) and not network_config.has_network_config():
            logger.warning(f"Partial network configuration detected for {connection_type} - some parameters may be missing")


def initialize_spark_session(job_config: JobConfig) -> tuple[SparkSession, GlueContext, Job]:
    """Initialize Spark session, Glue context, and job."""
    try:
        # Initialize Spark context
        sc = SparkContext()
        
        # Initialize Glue context
        glue_context = GlueContext(sc)
        
        # Get Spark session
        spark = glue_context.spark_session
        
        # Initialize Glue job with proper bookmark configuration
        job = Job(glue_context)
        job.init(job_config.job_name, {
            '--job-bookmark-option': 'job-bookmark-enable',
            '--enable-job-bookmark': 'true'
        })
        
        logger.info(f"Initialized Spark session for job: {job_config.job_name} with job bookmarks enabled")
        return spark, glue_context, job
        
    except Exception as e:
        logger.error(f"Failed to initialize Spark session: {str(e)}")
        raise RuntimeError(f"Spark initialization failed: {str(e)}")





def load_jdbc_drivers(spark_context: SparkContext, job_config: JobConfig) -> None:
    """Load JDBC drivers for source and target databases."""
    driver_loader = JdbcDriverLoader(spark_context)
    
    try:
        # Load source JDBC driver
        driver_loader.load_driver(
            job_config.source_connection.engine_type,
            job_config.source_connection.jdbc_driver_path
        )
        
        # Load target JDBC driver (if different from source)
        if (job_config.target_connection.jdbc_driver_path != 
            job_config.source_connection.jdbc_driver_path):
            driver_loader.load_driver(
                job_config.target_connection.engine_type,
                job_config.target_connection.jdbc_driver_path
            )
        
        logger.info("Successfully loaded all JDBC drivers")
        
    except Exception as e:
        logger.error(f"Failed to load JDBC drivers: {str(e)}")
        raise RuntimeError(f"JDBC driver loading failed: {str(e)}")


# Main function moved to end of file after all class definitions





class ConnectionStringBuilder:
    """Builds JDBC connection strings for different database engines."""
    
    @classmethod
    def build_connection_string(cls, engine_type: str, host: str, port: int, 
                              database: str, **kwargs) -> str:
        """Build JDBC connection string for the specified engine."""
        engine_type = engine_type.lower()
        
        if not DatabaseEngineManager.is_engine_supported(engine_type):
            raise ValueError(f"Unsupported database engine: {engine_type}")
        
        config = DatabaseEngineManager.ENGINE_CONFIGS[engine_type]
        
        # Use default port if not specified
        if port is None or port <= 0:
            port = config['default_port']
        
        # Build base connection string
        connection_string = config['url_template'].format(
            host=host,
            port=port,
            database=database
        )
        
        # Add engine-specific parameters
        if engine_type == 'sqlserver':
            # Add common SQL Server parameters
            params = []
            if kwargs.get('encrypt', True):
                params.append('encrypt=true')
            if kwargs.get('trustServerCertificate', False):
                params.append('trustServerCertificate=true')
            if kwargs.get('loginTimeout'):
                params.append(f"loginTimeout={kwargs['loginTimeout']}")
            
            if params:
                connection_string += ';' + ';'.join(params)
        
        elif engine_type == 'oracle':
            # Add Oracle-specific parameters if needed
            if kwargs.get('connectionTimeout'):
                connection_string += f"?oracle.net.CONNECT_TIMEOUT={kwargs['connectionTimeout']}"
        
        elif engine_type == 'postgresql':
            # Add PostgreSQL-specific parameters
            params = []
            if kwargs.get('ssl', False):
                params.append('ssl=true')
            if kwargs.get('connectTimeout'):
                params.append(f"connectTimeout={kwargs['connectTimeout']}")
            if kwargs.get('socketTimeout'):
                params.append(f"socketTimeout={kwargs['socketTimeout']}")
            
            if params:
                connection_string += '?' + '&'.join(params)
        
        elif engine_type == 'db2':
            # Add DB2-specific parameters
            params = []
            if kwargs.get('loginTimeout'):
                params.append(f"loginTimeout={kwargs['loginTimeout']}")
            if kwargs.get('blockingReadConnectionTimeout'):
                params.append(f"blockingReadConnectionTimeout={kwargs['blockingReadConnectionTimeout']}")
            
            if params:
                connection_string += ':' + ';'.join(params) + ';'
        
        return connection_string
    
    @classmethod
    def parse_connection_string(cls, connection_string: str) -> Dict[str, Any]:
        """Parse connection string to extract components."""
        try:
            # Determine engine type from connection string prefix
            engine_type = None
            for engine, config in DatabaseEngineManager.ENGINE_CONFIGS.items():
                url_prefix = config['url_template'].split('{')[0]
                if connection_string.startswith(url_prefix.replace('{host}', '').replace('{port}', '').replace('{database}', '')):
                    engine_type = engine
                    break
            
            if not engine_type:
                raise ValueError("Unable to determine engine type from connection string")
            
            # Basic parsing - this is a simplified implementation
            # In production, you might want more robust parsing
            parsed = {
                'engine_type': engine_type,
                'connection_string': connection_string
            }
            
            return parsed
            
        except Exception as e:
            raise ValueError(f"Failed to parse connection string: {str(e)}")


class ErrorCategory:
    """Defines error categories for different types of failures."""
    CONNECTION = "connection"
    AUTHENTICATION = "authentication"
    NETWORK = "network"
    DATA_PROCESSING = "data_processing"
    SCHEMA_MISMATCH = "schema_mismatch"
    PERMISSION = "permission"
    RESOURCE = "resource"
    TIMEOUT = "timeout"
    UNKNOWN = "unknown"


class ErrorClassifier:
    """Classifies errors into categories for appropriate handling."""
    
    # Error patterns for classification
    ERROR_PATTERNS = {
        ErrorCategory.CONNECTION: [
            'connection refused', 'connection timed out', 'connection reset',
            'no route to host', 'network unreachable', 'connection failed',
            'could not connect', 'unable to connect', 'connection error'
        ],
        ErrorCategory.AUTHENTICATION: [
            'authentication failed', 'login failed', 'invalid credentials',
            'access denied', 'unauthorized', 'invalid username or password',
            'authentication error', 'login incorrect'
        ],
        ErrorCategory.NETWORK: [
            'network error', 'socket timeout', 'read timeout', 'write timeout',
            'network is unreachable', 'host unreachable', 'dns resolution failed',
            'connection timeout', 'socket error'
        ],
        ErrorCategory.DATA_PROCESSING: [
            'data type mismatch', 'conversion error', 'parsing error',
            'invalid data format', 'constraint violation', 'data truncation',
            'null value', 'duplicate key'
        ],
        ErrorCategory.SCHEMA_MISMATCH: [
            'column not found', 'table not found', 'schema mismatch',
            'invalid column name', 'missing column', 'unknown column',
            'table does not exist', 'column does not exist'
        ],
        ErrorCategory.PERMISSION: [
            'permission denied', 'access forbidden', 'insufficient privileges',
            'not authorized', 'privilege error', 'access violation',
            'security error', 'forbidden'
        ],
        ErrorCategory.RESOURCE: [
            'out of memory', 'disk full', 'resource exhausted',
            'too many connections', 'connection pool exhausted',
            'memory error', 'resource unavailable'
        ],
        ErrorCategory.TIMEOUT: [
            'timeout', 'timed out', 'operation timeout', 'query timeout',
            'connection timeout', 'read timeout', 'write timeout'
        ]
    }
    
    @classmethod
    def classify_error(cls, error: Exception) -> str:
        """Classify error into appropriate category."""
        error_message = str(error).lower()
        
        for category, patterns in cls.ERROR_PATTERNS.items():
            for pattern in patterns:
                if pattern in error_message:
                    return category
        
        return ErrorCategory.UNKNOWN
    
    @classmethod
    def is_retryable_error(cls, error: Exception) -> bool:
        """Determine if error is retryable based on its category."""
        category = cls.classify_error(error)
        
        # Retryable error categories
        retryable_categories = {
            ErrorCategory.CONNECTION,
            ErrorCategory.NETWORK,
            ErrorCategory.TIMEOUT,
            ErrorCategory.RESOURCE
        }
        
        return category in retryable_categories
    
    @classmethod
    def get_recovery_strategy(cls, error: Exception) -> str:
        """Get recommended recovery strategy for error."""
        category = cls.classify_error(error)
        
        strategies = {
            ErrorCategory.CONNECTION: "retry_with_backoff",
            ErrorCategory.AUTHENTICATION: "fail_immediately",
            ErrorCategory.NETWORK: "retry_with_backoff",
            ErrorCategory.DATA_PROCESSING: "log_and_continue",
            ErrorCategory.SCHEMA_MISMATCH: "fail_immediately",
            ErrorCategory.PERMISSION: "fail_immediately",
            ErrorCategory.RESOURCE: "retry_with_longer_delay",
            ErrorCategory.TIMEOUT: "retry_with_backoff",
            ErrorCategory.UNKNOWN: "retry_with_backoff"
        }
        
        return strategies.get(category, "retry_with_backoff")


class NetworkConnectivityError(Exception):
    """Exception raised for network connectivity issues."""
    def __init__(self, message: str, error_type: str = 'unknown', connection_name: str = None):
        super().__init__(message)
        self.error_type = error_type
        self.connection_name = connection_name


class GlueConnectionError(Exception):
    """Exception raised for Glue connection specific issues."""
    def __init__(self, message: str, connection_name: str, error_details: Dict[str, Any] = None):
        super().__init__(message)
        self.connection_name = connection_name
        self.error_details = error_details or {}


class VpcEndpointError(Exception):
    """Exception raised for VPC endpoint connectivity issues."""
    def __init__(self, message: str, vpc_id: str = None, endpoint_type: str = None):
        super().__init__(message)
        self.vpc_id = vpc_id
        self.endpoint_type = endpoint_type


class ENICreationError(Exception):
    """Exception raised for Elastic Network Interface creation failures."""
    def __init__(self, message: str, subnet_id: str = None, error_code: str = None):
        super().__init__(message)
        self.subnet_id = subnet_id
        self.error_code = error_code


class NetworkErrorHandler:
    """Specialized error handler for network-specific issues."""
    
    def __init__(self):
        self.structured_logger = StructuredLogger("NetworkErrorHandler")
        try:
            self.ec2_client = boto3.client('ec2')
            self.glue_client = boto3.client('glue')
        except Exception as e:
            self.structured_logger.warning(f"Failed to initialize AWS clients: {e}")
            self.ec2_client = None
            self.glue_client = None
    
    def diagnose_glue_connection_failure(self, connection_name: str, error: Exception) -> Dict[str, Any]:
        """Diagnose Glue connection failures with detailed diagnostics."""
        diagnostics = {
            'connection_name': connection_name,
            'error_type': type(error).__name__,
            'error_message': str(error),
            'diagnostics': [],
            'recommendations': []
        }
        
        if not self.glue_client:
            diagnostics['diagnostics'].append("Unable to perform diagnostics - Glue client not available")
            return diagnostics
        
        try:
            self.structured_logger.info("Starting Glue connection diagnostics", connection_name=connection_name)
            
            # Check if connection exists
            try:
                response = self.glue_client.get_connection(Name=connection_name)
                connection = response.get('Connection', {})
                diagnostics['connection_exists'] = True
                
                # Analyze connection configuration
                physical_reqs = connection.get('PhysicalConnectionRequirements', {})
                subnet_id = physical_reqs.get('SubnetId')
                security_groups = physical_reqs.get('SecurityGroupIdList', [])
                availability_zone = physical_reqs.get('AvailabilityZone')
                
                diagnostics['subnet_id'] = subnet_id
                diagnostics['security_groups'] = security_groups
                diagnostics['availability_zone'] = availability_zone
                
                # Validate subnet accessibility
                if subnet_id:
                    subnet_diagnostics = self._diagnose_subnet_accessibility(subnet_id)
                    diagnostics['subnet_diagnostics'] = subnet_diagnostics
                    diagnostics['diagnostics'].extend(subnet_diagnostics.get('issues', []))
                
                # Validate security group rules
                if security_groups:
                    sg_diagnostics = self._diagnose_security_group_rules(security_groups)
                    diagnostics['security_group_diagnostics'] = sg_diagnostics
                    diagnostics['diagnostics'].extend(sg_diagnostics.get('issues', []))
                
                # Check for ENI creation issues
                eni_diagnostics = self._diagnose_eni_creation_issues(subnet_id)
                diagnostics['eni_diagnostics'] = eni_diagnostics
                diagnostics['diagnostics'].extend(eni_diagnostics.get('issues', []))
                
            except self.glue_client.exceptions.EntityNotFoundException:
                diagnostics['connection_exists'] = False
                diagnostics['diagnostics'].append(f"Glue connection '{connection_name}' does not exist")
                diagnostics['recommendations'].append(f"Create Glue connection '{connection_name}' with proper VPC configuration")
            
        except Exception as diag_error:
            self.structured_logger.error("Failed to diagnose Glue connection", 
                                       connection_name=connection_name, 
                                       error=str(diag_error))
            diagnostics['diagnostics'].append(f"Diagnostic failure: {str(diag_error)}")
        
        return diagnostics
    
    def _diagnose_subnet_accessibility(self, subnet_id: str) -> Dict[str, Any]:
        """Diagnose subnet accessibility issues."""
        diagnostics = {'subnet_id': subnet_id, 'issues': [], 'recommendations': []}
        
        if not self.ec2_client:
            diagnostics['issues'].append("Unable to diagnose subnet - EC2 client not available")
            return diagnostics
        
        try:
            response = self.ec2_client.describe_subnets(SubnetIds=[subnet_id])
            subnet = response['Subnets'][0]
            
            # Check subnet state
            if subnet['State'] != 'available':
                diagnostics['issues'].append(f"Subnet {subnet_id} is in '{subnet['State']}' state, not 'available'")
                diagnostics['recommendations'].append(f"Ensure subnet {subnet_id} is in 'available' state")
            
            # Check available IP addresses
            available_ips = subnet.get('AvailableIpAddressCount', 0)
            if available_ips < 2:
                diagnostics['issues'].append(f"Subnet {subnet_id} has only {available_ips} available IP addresses")
                diagnostics['recommendations'].append(f"Ensure subnet {subnet_id} has sufficient available IP addresses for ENI creation")
            
            # Check route table for internet/NAT gateway access
            vpc_id = subnet['VpcId']
            route_diagnostics = self._diagnose_route_table_connectivity(subnet_id, vpc_id)
            diagnostics.update(route_diagnostics)
            
        except Exception as e:
            diagnostics['issues'].append(f"Failed to describe subnet {subnet_id}: {str(e)}")
        
        return diagnostics
    
    def _diagnose_route_table_connectivity(self, subnet_id: str, vpc_id: str) -> Dict[str, Any]:
        """Diagnose route table connectivity for subnet."""
        diagnostics = {'route_issues': [], 'route_recommendations': []}
        
        if not self.ec2_client:
            diagnostics['route_issues'].append("Unable to diagnose routes - EC2 client not available")
            return diagnostics
        
        try:
            # Get route tables associated with the subnet
            response = self.ec2_client.describe_route_tables(
                Filters=[
                    {'Name': 'association.subnet-id', 'Values': [subnet_id]}
                ]
            )
            
            route_tables = response.get('RouteTables', [])
            if not route_tables:
                # Check VPC main route table
                response = self.ec2_client.describe_route_tables(
                    Filters=[
                        {'Name': 'vpc-id', 'Values': [vpc_id]},
                        {'Name': 'association.main', 'Values': ['true']}
                    ]
                )
                route_tables = response.get('RouteTables', [])
            
            if route_tables:
                route_table = route_tables[0]
                routes = route_table.get('Routes', [])
                
                # Check for internet gateway or NAT gateway routes
                has_internet_route = any(
                    route.get('GatewayId', '').startswith('igw-') or 
                    route.get('NatGatewayId', '').startswith('nat-')
                    for route in routes
                )
                
                if not has_internet_route:
                    diagnostics['route_issues'].append(
                        f"Subnet {subnet_id} route table lacks internet gateway or NAT gateway route"
                    )
                    diagnostics['route_recommendations'].append(
                        "Add route to internet gateway (for public subnet) or NAT gateway (for private subnet)"
                    )
            
        except Exception as e:
            diagnostics['route_issues'].append(f"Failed to analyze route tables: {str(e)}")
        
        return diagnostics
    
    def _diagnose_security_group_rules(self, security_group_ids: List[str]) -> Dict[str, Any]:
        """Diagnose security group rule issues."""
        diagnostics = {'security_groups': security_group_ids, 'issues': [], 'recommendations': []}
        
        if not self.ec2_client:
            diagnostics['issues'].append("Unable to diagnose security groups - EC2 client not available")
            return diagnostics
        
        try:
            response = self.ec2_client.describe_security_groups(GroupIds=security_group_ids)
            
            for sg in response['SecurityGroups']:
                sg_id = sg['GroupId']
                
                # Check outbound rules for database ports
                outbound_rules = sg.get('IpPermissionsEgress', [])
                has_database_outbound = any(
                    self._rule_allows_database_ports(rule) for rule in outbound_rules
                )
                
                if not has_database_outbound:
                    diagnostics['issues'].append(
                        f"Security group {sg_id} lacks outbound rules for common database ports"
                    )
                    diagnostics['recommendations'].append(
                        f"Add outbound rules to security group {sg_id} for database ports (1433, 1521, 5432, 50000)"
                    )
                
                # Check for overly restrictive rules
                inbound_rules = sg.get('IpPermissions', [])
                if not inbound_rules:
                    diagnostics['issues'].append(
                        f"Security group {sg_id} has no inbound rules - may be too restrictive"
                    )
        
        except Exception as e:
            diagnostics['issues'].append(f"Failed to analyze security groups: {str(e)}")
        
        return diagnostics
    
    def _rule_allows_database_ports(self, rule: Dict[str, Any]) -> bool:
        """Check if security group rule allows common database ports."""
        from_port = rule.get('FromPort')
        to_port = rule.get('ToPort')
        
        if from_port is None or to_port is None:
            return False
        
        # Check for rules that allow all traffic (common in outbound rules)
        if from_port == 0 and to_port == 65535:
            return True
        
        # Check for rules that allow all traffic on all protocols (FromPort = -1)
        if from_port == -1:
            return True
        
        database_ports = [1433, 1521, 5432, 50000]  # SQL Server, Oracle, PostgreSQL, DB2
        
        return any(
            from_port <= port <= to_port for port in database_ports
        )
    
    def _diagnose_eni_creation_issues(self, subnet_id: str) -> Dict[str, Any]:
        """Diagnose ENI creation issues and service limits."""
        diagnostics = {'issues': [], 'recommendations': []}
        
        if not self.ec2_client:
            diagnostics['issues'].append("Unable to diagnose ENI issues - EC2 client not available")
            return diagnostics
        
        try:
            # Check ENI limits
            response = self.ec2_client.describe_account_attributes(
                AttributeNames=['max-elastic-network-interfaces']
            )
            
            max_enis = 0
            for attr in response.get('AccountAttributes', []):
                if attr['AttributeName'] == 'max-elastic-network-interfaces':
                    max_enis = int(attr['AttributeValues'][0]['AttributeValue'])
                    break
            
            # Count current ENIs
            eni_response = self.ec2_client.describe_network_interfaces()
            current_enis = len(eni_response.get('NetworkInterfaces', []))
            
            if current_enis >= max_enis * 0.9:  # 90% threshold
                diagnostics['issues'].append(
                    f"ENI usage is high: {current_enis}/{max_enis} (90%+ threshold reached)"
                )
                diagnostics['recommendations'].append(
                    "Consider requesting ENI limit increase or cleaning up unused ENIs"
                )
            
        except Exception as e:
            diagnostics['issues'].append(f"Failed to check ENI limits: {str(e)}")
        
        return diagnostics
    
    def diagnose_vpc_endpoint_issues(self, vpc_id: str, service_name: str = 's3') -> Dict[str, Any]:
        """Diagnose VPC endpoint connectivity issues."""
        diagnostics = {
            'vpc_id': vpc_id,
            'service_name': service_name,
            'issues': [],
            'recommendations': []
        }
        
        if not self.ec2_client:
            diagnostics['issues'].append("Unable to diagnose VPC endpoints - EC2 client not available")
            return diagnostics
        
        try:
            # Check if VPC endpoint exists
            region = boto3.Session().region_name or 'us-east-1'
            response = self.ec2_client.describe_vpc_endpoints(
                Filters=[
                    {'Name': 'vpc-id', 'Values': [vpc_id]},
                    {'Name': 'service-name', 'Values': [f'com.amazonaws.{region}.{service_name}']}
                ]
            )
            
            endpoints = response.get('VpcEndpoints', [])
            if not endpoints:
                diagnostics['issues'].append(f"No {service_name} VPC endpoint found in VPC {vpc_id}")
                diagnostics['recommendations'].append(f"Create {service_name} VPC endpoint in VPC {vpc_id}")
            else:
                endpoint = endpoints[0]
                if endpoint['State'] != 'Available':
                    diagnostics['issues'].append(
                        f"VPC endpoint {endpoint['VpcEndpointId']} is in '{endpoint['State']}' state"
                    )
                    diagnostics['recommendations'].append(
                        f"Wait for VPC endpoint {endpoint['VpcEndpointId']} to become 'Available'"
                    )
        
        except Exception as e:
            diagnostics['issues'].append(f"Failed to check VPC endpoints: {str(e)}")
        
        return diagnostics


class ConnectionRetryHandler:
    """Enhanced connection retry handler with exponential backoff and error classification."""
    
    def __init__(self, max_retries: int = 3, base_delay: float = 1.0, 
                 max_delay: float = 60.0, backoff_factor: float = 2.0,
                 jitter: bool = True):
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.backoff_factor = backoff_factor
        self.jitter = jitter
        self.error_classifier = ErrorClassifier()
        self.network_error_handler = NetworkErrorHandler()
        self.structured_logger = StructuredLogger("ConnectionRetryHandler")
    
    def execute_with_retry(self, operation, operation_name: str, *args, **kwargs):
        """Execute operation with intelligent retry logic based on error classification."""
        last_exception = None
        retry_count = 0
        
        for attempt in range(self.max_retries + 1):
            try:
                self.structured_logger.info(f"Attempting {operation_name} (attempt {attempt + 1}/{self.max_retries + 1})")
                result = operation(*args, **kwargs)
                
                if attempt > 0:
                    self.structured_logger.info(f"{operation_name} succeeded after {attempt + 1} attempts")
                
                return result
                
            except Exception as e:
                last_exception = e
                error_category = self.error_classifier.classify_error(e)
                recovery_strategy = self.error_classifier.get_recovery_strategy(e)
                
                self.structured_logger.warning(
                    f"{operation_name} failed (attempt {attempt + 1}): {str(e)} "
                    f"[Category: {error_category}, Strategy: {recovery_strategy}]"
                )
                
                # Perform network-specific diagnostics for network errors
                self._perform_network_diagnostics(e, error_category, operation_name, *args, **kwargs)
                
                # Check if error is retryable
                if not self.error_classifier.is_retryable_error(e):
                    self.structured_logger.error(f"{operation_name} failed with non-retryable error: {str(e)}")
                    raise RuntimeError(f"{operation_name} failed: {str(e)} [Non-retryable: {error_category}]")
                
                # Don't retry on last attempt
                if attempt < self.max_retries:
                    delay = self._calculate_delay(attempt, recovery_strategy)
                    self.structured_logger.info(f"Retrying {operation_name} in {delay:.1f} seconds...")
                    time.sleep(delay)
                    retry_count += 1
                else:
                    self.structured_logger.error(f"{operation_name} failed after {self.max_retries + 1} attempts: {str(e)}")
        
        # Create detailed error message with retry information
        error_details = {
            'operation': operation_name,
            'total_attempts': self.max_retries + 1,
            'retry_count': retry_count,
            'last_error': str(last_exception),
            'error_category': self.error_classifier.classify_error(last_exception),
            'recovery_strategy': self.error_classifier.get_recovery_strategy(last_exception)
        }
        
        raise RuntimeError(
            f"{operation_name} failed after {self.max_retries + 1} attempts. "
            f"Error details: {error_details}"
        )
    
    def _calculate_delay(self, attempt: int, recovery_strategy: str) -> float:
        """Calculate delay based on attempt number and recovery strategy."""
        if recovery_strategy == "retry_with_longer_delay":
            # Use longer delays for resource exhaustion
            base_delay = self.base_delay * 3
        else:
            base_delay = self.base_delay
        
        # Calculate exponential backoff
        delay = min(base_delay * (self.backoff_factor ** attempt), self.max_delay)
        
        # Add jitter to prevent thundering herd
        if self.jitter:
            import random
            jitter_factor = random.uniform(0.5, 1.5)
            delay *= jitter_factor
        
        return delay
    
    def retry_with_network_recovery(self, operation, operation_name: str, 
                                  connection_config=None, *args, **kwargs):
        """Execute operation with network-aware retry logic and recovery."""
        last_exception = None
        
        for attempt in range(self.max_retries + 1):
            try:
                self.structured_logger.info(
                    f"Attempting {operation_name} with network recovery (attempt {attempt + 1}/{self.max_retries + 1})"
                )
                
                # Pre-validate network connectivity if connection config is provided
                if connection_config and attempt > 0:  # Skip on first attempt
                    self._validate_network_connectivity_before_retry(connection_config)
                
                result = operation(*args, **kwargs)
                
                if attempt > 0:
                    self.structured_logger.info(f"{operation_name} succeeded after {attempt + 1} attempts with network recovery")
                
                return result
                
            except (NetworkConnectivityError, GlueConnectionError, VpcEndpointError, ENICreationError) as network_error:
                last_exception = network_error
                
                self.structured_logger.error(
                    f"Network-specific error in {operation_name} (attempt {attempt + 1})",
                    error_type=type(network_error).__name__,
                    error_message=str(network_error)
                )
                
                # Perform detailed diagnostics for network errors
                if hasattr(network_error, 'connection_name') and network_error.connection_name:
                    diagnostics = self.network_error_handler.diagnose_glue_connection_failure(
                        network_error.connection_name, network_error
                    )
                    self.structured_logger.error(
                        "Network error diagnostics",
                        connection_name=network_error.connection_name,
                        diagnostics=diagnostics
                    )
                
                # Don't retry network configuration errors
                if isinstance(network_error, (GlueConnectionError, VpcEndpointError)):
                    self.structured_logger.error(f"Non-retryable network configuration error: {str(network_error)}")
                    raise network_error
                
                # Retry ENI and connectivity errors with longer delays
                if attempt < self.max_retries:
                    delay = self._calculate_network_recovery_delay(attempt, type(network_error))
                    self.structured_logger.info(f"Retrying {operation_name} after network error in {delay:.1f} seconds...")
                    time.sleep(delay)
                else:
                    self.structured_logger.error(f"{operation_name} failed after {self.max_retries + 1} attempts with network errors")
                    raise network_error
                    
            except Exception as e:
                # Handle non-network errors with standard retry logic
                return self.execute_with_retry(operation, operation_name, *args, **kwargs)
        
        raise last_exception
    
    def _validate_network_connectivity_before_retry(self, connection_config):
        """Validate network connectivity before retry attempt."""
        try:
            if hasattr(connection_config, 'requires_cross_vpc_connection') and connection_config.requires_cross_vpc_connection():
                connection_name = connection_config.get_glue_connection_name()
                if connection_name:
                    # Quick validation of Glue connection existence
                    diagnostics = self.network_error_handler.diagnose_glue_connection_failure(
                        connection_name, Exception("Pre-retry validation")
                    )
                    
                    if not diagnostics.get('connection_exists', False):
                        raise GlueConnectionError(
                            f"Glue connection '{connection_name}' does not exist",
                            connection_name
                        )
                    
                    self.structured_logger.info(
                        "Network connectivity pre-validation passed",
                        connection_name=connection_name
                    )
        
        except Exception as validation_error:
            self.structured_logger.warning(
                "Network connectivity pre-validation failed",
                error=str(validation_error)
            )
            # Don't fail the retry attempt due to validation issues
    
    def _calculate_network_recovery_delay(self, attempt: int, error_type: type) -> float:
        """Calculate delay for network recovery based on error type."""
        # Use longer delays for network-related errors
        base_multiplier = 1.0
        
        if error_type == ENICreationError:
            # ENI creation failures may need longer recovery time
            base_multiplier = 3.0
        elif error_type == NetworkConnectivityError:
            # Network connectivity issues may resolve quickly
            base_multiplier = 1.5
        elif error_type == VpcEndpointError:
            # VPC endpoint issues typically need longer recovery
            base_multiplier = 2.0
        
        delay = min(
            self.base_delay * base_multiplier * (self.backoff_factor ** attempt), 
            self.max_delay
        )
        
        # Add jitter for network recovery
        if self.jitter:
            import random
            jitter_factor = random.uniform(0.8, 1.2)
            delay *= jitter_factor
        
        return delay
    
    def execute_with_circuit_breaker(self, operation, operation_name: str, 
                                   failure_threshold: int = 5, 
                                   recovery_timeout: float = 300.0, *args, **kwargs):
        """Execute operation with circuit breaker pattern for repeated failures."""
        # Simple circuit breaker implementation
        circuit_key = f"circuit_{operation_name}"
        
        # This would typically be stored in a shared cache/database
        # For simplicity, using class-level storage
        if not hasattr(self, '_circuit_states'):
            self._circuit_states = {}
        
        circuit_state = self._circuit_states.get(circuit_key, {
            'failure_count': 0,
            'last_failure_time': 0,
            'state': 'closed'  # closed, open, half_open
        })
        
        current_time = time.time()
        
        # Check circuit state
        if circuit_state['state'] == 'open':
            if current_time - circuit_state['last_failure_time'] > recovery_timeout:
                circuit_state['state'] = 'half_open'
                logger.info(f"Circuit breaker for {operation_name} moving to half-open state")
            else:
                raise RuntimeError(
                    f"Circuit breaker is open for {operation_name}. "
                    f"Will retry after {recovery_timeout - (current_time - circuit_state['last_failure_time']):.1f} seconds"
                )
        
        try:
            result = self.execute_with_retry(operation, operation_name, *args, **kwargs)
            
            # Success - reset circuit breaker
            if circuit_state['state'] == 'half_open':
                circuit_state['state'] = 'closed'
                circuit_state['failure_count'] = 0
                logger.info(f"Circuit breaker for {operation_name} reset to closed state")
            
            self._circuit_states[circuit_key] = circuit_state
            return result
            
        except Exception as e:
            # Failure - update circuit breaker
            circuit_state['failure_count'] += 1
            circuit_state['last_failure_time'] = current_time
            
            if circuit_state['failure_count'] >= failure_threshold:
                circuit_state['state'] = 'open'
                logger.error(
                    f"Circuit breaker opened for {operation_name} after {failure_threshold} failures. "
                    f"Will remain open for {recovery_timeout} seconds"
                )
            
            self._circuit_states[circuit_key] = circuit_state
            raise
    
    def _perform_network_diagnostics(self, error: Exception, error_category: str, operation_name: str, *args, **kwargs):
        """Perform detailed network diagnostics for network-related errors."""
        try:
            # Check if this is a network-related error that warrants diagnostics
            network_error_categories = {
                'network_connectivity', 'cross_vpc_routing', 'glue_connection', 
                'eni_creation', 'vpc_endpoint'
            }
            
            if error_category not in network_error_categories:
                return
            
            self.structured_logger.info(
                "Performing network diagnostics for error",
                operation_name=operation_name,
                error_category=error_category,
                error_message=str(error)
            )
            
            # Extract connection information from arguments
            connection_name = self._extract_connection_name_from_args(*args, **kwargs)
            vpc_id = self._extract_vpc_id_from_args(*args, **kwargs)
            
            # Perform Glue connection diagnostics if connection name is available
            if connection_name and error_category in ['glue_connection', 'cross_vpc_routing']:
                try:
                    diagnostics = self.network_error_handler.diagnose_glue_connection_failure(
                        connection_name, error
                    )
                    self.structured_logger.error(
                        "Glue connection diagnostics completed",
                        operation_name=operation_name,
                        connection_name=connection_name,
                        diagnostics_summary={
                            'connection_exists': diagnostics.get('connection_exists'),
                            'issues_count': len(diagnostics.get('diagnostics', [])),
                            'recommendations_count': len(diagnostics.get('recommendations', []))
                        }
                    )
                    
                    # Log specific issues and recommendations
                    for issue in diagnostics.get('diagnostics', []):
                        self.structured_logger.warning(f"Network issue detected: {issue}")
                    
                    for recommendation in diagnostics.get('recommendations', []):
                        self.structured_logger.info(f"Network recommendation: {recommendation}")
                        
                except Exception as diag_error:
                    self.structured_logger.warning(
                        "Failed to perform Glue connection diagnostics",
                        connection_name=connection_name,
                        error=str(diag_error)
                    )
            
            # Perform VPC endpoint diagnostics if VPC ID is available
            if vpc_id and error_category == 'vpc_endpoint':
                try:
                    vpc_diagnostics = self.network_error_handler.diagnose_vpc_endpoint_issues(vpc_id)
                    self.structured_logger.error(
                        "VPC endpoint diagnostics completed",
                        operation_name=operation_name,
                        vpc_id=vpc_id,
                        diagnostics_summary={
                            'issues_count': len(vpc_diagnostics.get('issues', [])),
                            'recommendations_count': len(vpc_diagnostics.get('recommendations', []))
                        }
                    )
                    
                    # Log specific issues and recommendations
                    for issue in vpc_diagnostics.get('issues', []):
                        self.structured_logger.warning(f"VPC endpoint issue: {issue}")
                    
                    for recommendation in vpc_diagnostics.get('recommendations', []):
                        self.structured_logger.info(f"VPC endpoint recommendation: {recommendation}")
                        
                except Exception as diag_error:
                    self.structured_logger.warning(
                        "Failed to perform VPC endpoint diagnostics",
                        vpc_id=vpc_id,
                        error=str(diag_error)
                    )
            
        except Exception as diag_error:
            self.structured_logger.warning(
                "Failed to perform network diagnostics",
                operation_name=operation_name,
                error=str(diag_error)
            )
    
    def _extract_connection_name_from_args(self, *args, **kwargs) -> Optional[str]:
        """Extract Glue connection name from operation arguments."""
        try:
            # Check kwargs first
            if 'glue_connection_name' in kwargs:
                return kwargs['glue_connection_name']
            
            if 'connection_name' in kwargs:
                return kwargs['connection_name']
            
            # Check args for connection config objects
            for arg in args:
                if hasattr(arg, 'get_glue_connection_name'):
                    connection_name = arg.get_glue_connection_name()
                    if connection_name:
                        return connection_name
                
                if hasattr(arg, 'network_config') and arg.network_config:
                    if hasattr(arg.network_config, 'glue_connection_name'):
                        return arg.network_config.glue_connection_name
            
            return None
            
        except Exception:
            return None
    
    def _extract_vpc_id_from_args(self, *args, **kwargs) -> Optional[str]:
        """Extract VPC ID from operation arguments."""
        try:
            # Check kwargs first
            if 'vpc_id' in kwargs:
                return kwargs['vpc_id']
            
            # Check args for connection config objects
            for arg in args:
                if hasattr(arg, 'network_config') and arg.network_config:
                    if hasattr(arg.network_config, 'vpc_id'):
                        return arg.network_config.vpc_id
            
            return None
            
        except Exception:
            return None


class GlueConnectionManager:
    """Manages Glue connections for cross-VPC database access."""
    
    def __init__(self, glue_context: GlueContext):
        self.glue_context = glue_context
        # Configure Glue client with aggressive timeout settings
        from botocore.config import Config
        config = Config(
            read_timeout=30,  # Reduced from 60 to 30 seconds
            connect_timeout=10,  # Reduced from 30 to 10 seconds
            retries={'max_attempts': 2}  # Reduced from 3 to 2 attempts
        )
        self.glue_client = boto3.client('glue', config=config)
        self.structured_logger = StructuredLogger("GlueConnectionManager")
    
    def get_glue_connection(self, connection_name: str) -> Optional[Dict[str, Any]]:
        """Retrieve Glue connection details for cross-VPC database access with enhanced error handling.
        
        Args:
            connection_name: Name of the Glue connection
            
        Returns:
            Dictionary containing connection details or None if not found
            
        Raises:
            GlueConnectionError: For Glue connection specific issues
        """
        if not connection_name or connection_name.strip() == '':
            self.structured_logger.debug("No Glue connection name provided")
            return None
        
        try:
            self.structured_logger.info("Retrieving Glue connection", connection_name=connection_name)
            
            # Log diagnostic information first
            try:
                # First, try to list connections to verify access
                self.structured_logger.info("Testing Glue API access by listing connections")
                list_response = self.glue_client.get_connections(MaxResults=1)
                self.structured_logger.info("Glue API access verified - can list connections")
            except Exception as list_error:
                self.structured_logger.error(
                    "Cannot access Glue API - this indicates permission or configuration issues",
                    error=str(list_error)
                )
                raise GlueConnectionError(
                    f"Cannot access Glue API: {str(list_error)}. Please check IAM permissions for glue:GetConnections.",
                    connection_name,
                    {'error_type': 'api_access_error', 'original_error': str(list_error)}
                )
            
            # Try to get the specific connection with better error handling
            self.structured_logger.info("Attempting to retrieve specific Glue connection", connection_name=connection_name)
            
            try:
                response = self.glue_client.get_connection(Name=connection_name)
                connection = response.get('Connection', {})
                self.structured_logger.info("Successfully retrieved Glue connection", connection_name=connection_name)
            except Exception as get_error:
                self.structured_logger.error(
                    "Failed to retrieve specific Glue connection",
                    connection_name=connection_name,
                    error=str(get_error),
                    error_type=type(get_error).__name__
                )
                # Re-raise the original exception to be handled by the outer try-catch
                raise
            
            connection_details = {
                'name': connection.get('Name'),
                'connection_type': connection.get('ConnectionType'),
                'connection_properties': connection.get('ConnectionProperties', {}),
                'physical_connection_requirements': connection.get('PhysicalConnectionRequirements', {})
            }
            
            # Validate connection configuration
            self._validate_glue_connection_config(connection_details, connection_name)
            
            self.structured_logger.info(
                "Successfully retrieved and validated Glue connection",
                connection_name=connection_name,
                connection_type=connection_details['connection_type']
            )
            
            return connection_details
            
        except Exception as e:
            self.structured_logger.error(
                "Failed to retrieve Glue connection",
                connection_name=connection_name,
                error=str(e),
                error_type=type(e).__name__
            )
            raise GlueConnectionError(
                f"Failed to retrieve Glue connection '{connection_name}': {str(e)}",
                connection_name,
                {'error_type': 'connection_retrieval_error', 'original_error': str(e)}
            )
    
    def _validate_glue_connection_config(self, connection_details: Dict[str, Any], connection_name: str):
        """Validate Glue connection configuration."""
        connection_type = connection_details.get('connection_type')
        if connection_type not in ['JDBC', 'NETWORK']:
            raise GlueConnectionError(
                f"Glue connection '{connection_name}' is not a JDBC or NETWORK connection (type: {connection_type})",
                connection_name,
                {'error_type': 'invalid_connection_type', 'connection_type': connection_type}
            )
        
        physical_reqs = connection_details.get('physical_connection_requirements', {})
        if not physical_reqs:
            raise GlueConnectionError(
                f"Glue connection '{connection_name}' lacks physical connection requirements",
                connection_name,
                {'error_type': 'missing_physical_requirements'}
            )
        
        # Check for required network configuration
        subnet_id = physical_reqs.get('SubnetId')
        security_groups = physical_reqs.get('SecurityGroupIdList', [])
        
        if not subnet_id:
            raise GlueConnectionError(
                f"Glue connection '{connection_name}' missing subnet configuration",
                connection_name,
                {'error_type': 'missing_subnet', 'physical_requirements': physical_reqs}
            )
        
        if not security_groups:
            raise GlueConnectionError(
                f"Glue connection '{connection_name}' missing security group configuration",
                connection_name,
                {'error_type': 'missing_security_groups', 'physical_requirements': physical_reqs}
            )
    
    def validate_network_connectivity(self, connection_name: str, 
                                    connection_string: str, 
                                    timeout_seconds: int = 30) -> bool:
        """Test database connectivity before processing with enhanced error handling.
        
        Args:
            connection_name: Name of the Glue connection (empty for same-VPC)
            connection_string: JDBC connection string
            timeout_seconds: Connection timeout in seconds
            
        Returns:
            True if connectivity test passes, False otherwise
            
        Raises:
            NetworkConnectivityError: For network connectivity issues
            GlueConnectionError: For Glue connection specific issues
            VpcEndpointError: For VPC endpoint issues
        """
        try:
            # Check for environment variable to skip Glue connection validation
            import os
            skip_glue_validation = os.environ.get('SKIP_GLUE_CONNECTION_VALIDATION', 'false').lower() == 'true'
            
            if skip_glue_validation:
                self.structured_logger.warning(
                    "Skipping Glue connection validation due to environment variable SKIP_GLUE_CONNECTION_VALIDATION=true",
                    connection_name=connection_name or "same-vpc"
                )
                return True
            
            self.structured_logger.info(
                "Starting network connectivity validation",
                connection_name=connection_name or "same-vpc",
                timeout_seconds=timeout_seconds
            )
            
            start_time = time.time()
            
            # If no Glue connection name, assume same-VPC connectivity
            if not connection_name or connection_name.strip() == '':
                self.structured_logger.info("Using same-VPC connectivity (no Glue connection)")
                # For same-VPC, we'll validate during actual connection attempt
                return True
            
            # Retrieve Glue connection details with error handling and fallback
            try:
                self.structured_logger.info("Attempting to retrieve Glue connection for validation", connection_name=connection_name)
                connection_details = self.get_glue_connection(connection_name)
                if not connection_details:
                    self.structured_logger.warning(
                        "Glue connection validation failed - connection not found, will attempt direct connection",
                        connection_name=connection_name
                    )
                    # Return True to allow the job to continue and try direct connection
                    return True
            except Exception as conn_error:
                self.structured_logger.warning(
                    "Glue connection validation failed - will attempt direct connection instead",
                    connection_name=connection_name,
                    error=str(conn_error),
                    error_type=type(conn_error).__name__
                )
                
                # Log specific error types for debugging
                if "EntityNotFoundException" in str(conn_error):
                    self.structured_logger.error(
                        "Glue connection does not exist - please verify the connection name and ensure it's created",
                        connection_name=connection_name
                    )
                elif "AccessDenied" in str(conn_error) or "permission" in str(conn_error).lower():
                    self.structured_logger.error(
                        "Permission denied accessing Glue connection - please verify IAM permissions",
                        connection_name=connection_name
                    )
                elif "timeout" in str(conn_error).lower():
                    self.structured_logger.error(
                        "Timeout accessing Glue connection - may indicate network or service issues",
                        connection_name=connection_name
                    )
                
                # Instead of failing, return True to allow the job to continue
                # The actual database connection will be tested later
                self.structured_logger.info(
                    "Skipping Glue connection validation - will test actual database connectivity instead",
                    connection_name=connection_name
                )
                return True
            
            # Validate connection properties
            connection_properties = connection_details.get('connection_properties', {})
            physical_requirements = connection_details.get('physical_connection_requirements', {})
            
            # Check if connection has required network configuration
            subnet_id = physical_requirements.get('SubnetId')
            security_groups = physical_requirements.get('SecurityGroupIdList', [])
            
            if not subnet_id:
                raise GlueConnectionError(
                    f"Glue connection '{connection_name}' missing subnet configuration",
                    connection_name,
                    {'error_type': 'missing_subnet', 'physical_requirements': physical_requirements}
                )
            
            if not security_groups:
                raise GlueConnectionError(
                    f"Glue connection '{connection_name}' missing security group configuration",
                    connection_name,
                    {'error_type': 'missing_security_groups', 'physical_requirements': physical_requirements}
                )
            
            # Perform detailed network validation
            self._validate_subnet_accessibility(subnet_id, connection_name)
            self._validate_security_group_rules(security_groups, connection_name)
            
            # Validate that connection URL matches expected format
            stored_url = connection_properties.get('JDBC_CONNECTION_URL', '')
            if stored_url and stored_url != connection_string:
                self.structured_logger.warning(
                    "Connection string mismatch between parameter and Glue connection",
                    connection_name=connection_name,
                    parameter_url=connection_string[:50] + "...",
                    stored_url=stored_url[:50] + "..."
                )
            
            duration = time.time() - start_time
            self.structured_logger.info(
                "Network connectivity validation completed successfully",
                connection_name=connection_name,
                duration_seconds=round(duration, 2),
                subnet_id=subnet_id,
                security_groups_count=len(security_groups)
            )
            
            return True
            
        except (NetworkConnectivityError, GlueConnectionError, VpcEndpointError) as network_error:
            duration = time.time() - start_time
            self.structured_logger.error(
                "Network connectivity validation failed with network error",
                connection_name=connection_name or "same-vpc",
                error_type=type(network_error).__name__,
                error_message=str(network_error),
                duration_seconds=round(duration, 2)
            )
            raise network_error
            
        except Exception as e:
            duration = time.time() - start_time
            self.structured_logger.error(
                "Network connectivity validation failed with unexpected error",
                connection_name=connection_name or "same-vpc",
                error=str(e),
                duration_seconds=round(duration, 2)
            )
            raise NetworkConnectivityError(
                f"Network connectivity validation failed: {str(e)}",
                error_type='validation_error',
                connection_name=connection_name
            )
    
    def _validate_subnet_accessibility(self, subnet_id: str, connection_name: str):
        """Validate subnet accessibility for Glue connection."""
        try:
            if not hasattr(self, 'ec2_client'):
                self.ec2_client = boto3.client('ec2')
            
            response = self.ec2_client.describe_subnets(SubnetIds=[subnet_id])
            subnet = response['Subnets'][0]
            
            # Check subnet state
            if subnet['State'] != 'available':
                raise NetworkConnectivityError(
                    f"Subnet {subnet_id} for Glue connection '{connection_name}' is in '{subnet['State']}' state, not 'available'",
                    error_type='subnet_unavailable',
                    connection_name=connection_name
                )
            
            # Check available IP addresses
            available_ips = subnet.get('AvailableIpAddressCount', 0)
            if available_ips < 2:
                raise ENICreationError(
                    f"Subnet {subnet_id} has insufficient IP addresses ({available_ips} available) for ENI creation",
                    subnet_id=subnet_id,
                    error_code='insufficient_ips'
                )
            
            self.structured_logger.debug(
                "Subnet accessibility validation passed",
                subnet_id=subnet_id,
                subnet_state=subnet['State'],
                available_ips=available_ips
            )
            
        except (NetworkConnectivityError, ENICreationError):
            raise
        except Exception as e:
            raise NetworkConnectivityError(
                f"Failed to validate subnet {subnet_id} accessibility: {str(e)}",
                error_type='subnet_validation_error',
                connection_name=connection_name
            )
    
    def _validate_security_group_rules(self, security_group_ids: List[str], connection_name: str):
        """Validate security group rules for database connectivity."""
        try:
            if not hasattr(self, 'ec2_client'):
                self.ec2_client = boto3.client('ec2')
            
            response = self.ec2_client.describe_security_groups(GroupIds=security_group_ids)
            
            for sg in response['SecurityGroups']:
                sg_id = sg['GroupId']
                
                # Check outbound rules for database ports
                outbound_rules = sg.get('IpPermissionsEgress', [])
                has_database_outbound = any(
                    self._rule_allows_database_ports(rule) for rule in outbound_rules
                )
                
                if not has_database_outbound:
                    self.structured_logger.warning(
                        f"Security group {sg_id} may lack outbound rules for database ports",
                        connection_name=connection_name,
                        security_group_id=sg_id
                    )
                    # Don't fail validation, just warn
            
            self.structured_logger.debug(
                "Security group validation completed",
                connection_name=connection_name,
                security_groups=security_group_ids
            )
            
        except Exception as e:
            self.structured_logger.warning(
                "Failed to validate security group rules",
                connection_name=connection_name,
                security_groups=security_group_ids,
                error=str(e)
            )
            # Don't fail validation for security group rule issues
    
    def _rule_allows_database_ports(self, rule: Dict[str, Any]) -> bool:
        """Check if security group rule allows common database ports."""
        from_port = rule.get('FromPort')
        to_port = rule.get('ToPort')
        
        if from_port is None or to_port is None:
            return False
        
        # Check for rules that allow all traffic (common in outbound rules)
        if from_port == 0 and to_port == 65535:
            return True
        
        # Check for rules that allow all traffic on all protocols (FromPort = -1)
        if from_port == -1:
            return True
        
        database_ports = [1433, 1521, 5432, 50000]  # SQL Server, Oracle, PostgreSQL, DB2
        
        return any(
            from_port <= port <= to_port for port in database_ports
        )
    
    def setup_jdbc_with_connection(self, connection_config: 'ConnectionConfig', 
                                 glue_connection_name: str) -> Dict[str, Any]:
        """Configure JDBC connection properties using Glue connection when specified with enhanced error handling.
        
        Args:
            connection_config: Database connection configuration
            glue_connection_name: Name of Glue connection (empty for same-VPC)
            
        Returns:
            Dictionary with JDBC connection properties
            
        Raises:
            GlueConnectionError: For Glue connection specific issues
            NetworkConnectivityError: For network connectivity issues
        """
        try:
            self.structured_logger.info(
                "Setting up JDBC connection with enhanced error handling",
                engine_type=connection_config.engine_type,
                glue_connection_name=glue_connection_name or "same-vpc"
            )
            
            # Base JDBC properties
            jdbc_properties = {
                'url': connection_config.connection_string,
                'user': connection_config.username,
                'password': connection_config.password,
                'driver': DatabaseEngineManager.get_driver_class(connection_config.engine_type)
            }
            
            # If no Glue connection specified, use direct connection
            if not glue_connection_name or glue_connection_name.strip() == '':
                self.structured_logger.info("Using direct JDBC connection (same-VPC)")
                return jdbc_properties
            
            # Retrieve Glue connection details for cross-VPC access with error handling
            try:
                connection_details = self.get_glue_connection(glue_connection_name)
                if not connection_details:
                    raise GlueConnectionError(
                        f"Glue connection '{glue_connection_name}' not found",
                        glue_connection_name,
                        {'error_type': 'not_found'}
                    )
                
                # Check if this is a NETWORK connection (for VPC access only)
                connection_type = connection_details.get('connection_type', '').upper()
                if connection_type == 'NETWORK':
                    self.structured_logger.info(
                        "Using NETWORK connection for VPC access with direct JDBC properties",
                        connection_name=glue_connection_name
                    )
                    # For NETWORK connections, use direct JDBC properties
                    # The network connection just provides VPC access
                    jdbc_properties['_glue_connection_metadata'] = {
                        'connection_name': glue_connection_name,
                        'connection_type': 'NETWORK',
                        'subnet_id': connection_details.get('physical_connection_requirements', {}).get('SubnetId'),
                        'security_groups': connection_details.get('physical_connection_requirements', {}).get('SecurityGroupIdList', [])
                    }
                    return jdbc_properties
                    
            except GlueConnectionError:
                raise
            except Exception as conn_error:
                raise GlueConnectionError(
                    f"Failed to retrieve Glue connection '{glue_connection_name}': {str(conn_error)}",
                    glue_connection_name,
                    {'error_type': 'retrieval_error', 'original_error': str(conn_error)}
                )
            
            # Use connection properties from Glue connection if available
            glue_properties = connection_details.get('connection_properties', {})
            
            # Override with Glue connection properties if they exist
            if glue_properties.get('JDBC_CONNECTION_URL'):
                jdbc_properties['url'] = glue_properties['JDBC_CONNECTION_URL']
                self.structured_logger.debug(
                    "Using connection URL from Glue connection",
                    glue_connection_name=glue_connection_name
                )
            
            if glue_properties.get('USERNAME'):
                jdbc_properties['user'] = glue_properties['USERNAME']
                self.structured_logger.debug(
                    "Using username from Glue connection",
                    glue_connection_name=glue_connection_name
                )
            
            if glue_properties.get('PASSWORD'):
                jdbc_properties['password'] = glue_properties['PASSWORD']
                self.structured_logger.debug(
                    "Using password from Glue connection",
                    glue_connection_name=glue_connection_name
                )
            
            # Add network configuration metadata
            physical_requirements = connection_details.get('physical_connection_requirements', {})
            jdbc_properties['_glue_connection_metadata'] = {
                'connection_name': glue_connection_name,
                'subnet_id': physical_requirements.get('SubnetId'),
                'security_groups': physical_requirements.get('SecurityGroupIdList', []),
                'availability_zone': physical_requirements.get('AvailabilityZone')
            }
            
            # Validate network configuration before returning
            self._validate_network_configuration_for_jdbc(physical_requirements, glue_connection_name)
            
            self.structured_logger.info(
                "Successfully configured JDBC with Glue connection",
                glue_connection_name=glue_connection_name,
                subnet_id=physical_requirements.get('SubnetId'),
                security_groups_count=len(physical_requirements.get('SecurityGroupIdList', []))
            )
            
            return jdbc_properties
            
        except (GlueConnectionError, NetworkConnectivityError):
            raise
        except Exception as e:
            self.structured_logger.error(
                "Failed to setup JDBC with Glue connection",
                glue_connection_name=glue_connection_name,
                error=str(e)
            )
            raise NetworkConnectivityError(
                f"Failed to setup JDBC with Glue connection '{glue_connection_name}': {str(e)}",
                error_type='jdbc_setup_error',
                connection_name=glue_connection_name
            )
    
    def _validate_network_configuration_for_jdbc(self, physical_requirements: Dict[str, Any], connection_name: str):
        """Validate network configuration for JDBC setup."""
        subnet_id = physical_requirements.get('SubnetId')
        security_groups = physical_requirements.get('SecurityGroupIdList', [])
        
        if not subnet_id:
            raise NetworkConnectivityError(
                f"Glue connection '{connection_name}' missing subnet configuration for JDBC setup",
                error_type='missing_subnet',
                connection_name=connection_name
            )
        
        if not security_groups:
            raise NetworkConnectivityError(
                f"Glue connection '{connection_name}' missing security group configuration for JDBC setup",
                error_type='missing_security_groups',
                connection_name=connection_name
            )
        
        self.structured_logger.debug(
            "Network configuration validation passed for JDBC setup",
            connection_name=connection_name,
            subnet_id=subnet_id,
            security_groups_count=len(security_groups)
        )


class JdbcConnectionManager:
    """Manages JDBC database connections with validation and error handling."""
    
    def __init__(self, spark_session: SparkSession, glue_context: GlueContext, 
                 retry_handler: Optional[ConnectionRetryHandler] = None):
        self.spark = spark_session
        self.glue_context = glue_context
        self.retry_handler = retry_handler or ConnectionRetryHandler()
        self.glue_connection_manager = GlueConnectionManager(glue_context)
        self._connection_cache = {}
        self.structured_logger = StructuredLogger("JdbcConnectionManager")
    
    def create_connection_properties(self, connection_config: ConnectionConfig) -> Dict[str, str]:
        """Create JDBC connection properties from connection configuration."""
        properties = {
            'user': str(connection_config.username),
            'password': str(connection_config.password),
            'driver': str(DatabaseEngineManager.get_driver_class(connection_config.engine_type))
        }
        
        # Add engine-specific connection properties
        engine_type = connection_config.engine_type.lower()
        
        if engine_type == 'oracle':
            properties.update({
                'oracle.jdbc.timezoneAsRegion': 'false',
                'oracle.net.CONNECT_TIMEOUT': '30000',
                'oracle.jdbc.ReadTimeout': '60000'
            })
        
        elif engine_type == 'sqlserver':
            properties.update({
                'loginTimeout': '30',
                'socketTimeout': '60000',
                'selectMethod': 'cursor'
            })
        
        elif engine_type == 'postgresql':
            properties.update({
                'connectTimeout': '30',
                'socketTimeout': '60',
                'tcpKeepAlive': 'true'
            })
        
        elif engine_type == 'db2':
            properties.update({
                'loginTimeout': '30',
                'blockingReadConnectionTimeout': '60000',
                'resultSetHoldability': '1'
            })
        
        return properties
    
    def create_connection_with_glue_support(self, connection_config: ConnectionConfig, 
                                          glue_connection_name: str = '') -> DataFrame:
        """Create JDBC connection with optional Glue connection support for cross-VPC access.
        
        Args:
            connection_config: Database connection configuration
            glue_connection_name: Name of Glue connection for cross-VPC access (empty for same-VPC)
            
        Returns:
            DataFrame reader configured with appropriate connection properties
        """
        try:
            # Setup JDBC properties using Glue connection if specified
            jdbc_properties = self.glue_connection_manager.setup_jdbc_with_connection(
                connection_config, glue_connection_name
            )
            
            # Create DataFrame reader with JDBC properties
            df_reader = self.spark.read.format('jdbc')
            
            # Configure connection properties
            for key, value in jdbc_properties.items():
                if not key.startswith('_'):  # Skip metadata keys
                    df_reader = df_reader.option(key, value)
            
            self.structured_logger.info(
                "Created JDBC connection with Glue support",
                engine_type=connection_config.engine_type,
                glue_connection_name=glue_connection_name or "same-vpc",
                has_glue_metadata='_glue_connection_metadata' in jdbc_properties
            )
            
            return df_reader
            
        except Exception as e:
            self.structured_logger.error(
                "Failed to create JDBC connection with Glue support",
                engine_type=connection_config.engine_type,
                glue_connection_name=glue_connection_name,
                error=str(e)
            )
            raise RuntimeError(f"Failed to create JDBC connection: {str(e)}")
    
    def validate_connection_with_network_check(self, connection_config: ConnectionConfig, 
                                             glue_connection_name: str = '',
                                             timeout_seconds: int = 30) -> bool:
        """Validate database connection with network connectivity check and enhanced error handling.
        
        Args:
            connection_config: Database connection configuration
            glue_connection_name: Name of Glue connection for cross-VPC access
            timeout_seconds: Connection timeout in seconds
            
        Returns:
            True if connection is valid, False otherwise
            
        Raises:
            NetworkConnectivityError: For network connectivity issues
            GlueConnectionError: For Glue connection specific issues
            ENICreationError: For ENI creation failures
        """
        try:
            # Use network configuration from connection config if not explicitly provided
            if not glue_connection_name and connection_config.requires_cross_vpc_connection():
                glue_connection_name = connection_config.get_glue_connection_name() or ''
            
            self.structured_logger.info(
                "Starting connection validation with network check",
                engine_type=connection_config.engine_type,
                glue_connection_name=glue_connection_name or "same-vpc",
                cross_vpc_required=connection_config.requires_cross_vpc_connection()
            )
            
            start_time = time.time()
            
            # First, validate network connectivity with enhanced error handling
            try:
                network_valid = self.glue_connection_manager.validate_network_connectivity(
                    glue_connection_name, connection_config.connection_string, timeout_seconds
                )
                if not network_valid:
                    raise NetworkConnectivityError(
                        f"Network connectivity validation failed for connection '{glue_connection_name or 'same-vpc'}'",
                        error_type='connectivity_validation_failed',
                        connection_name=glue_connection_name
                    )
            except (NetworkConnectivityError, GlueConnectionError, VpcEndpointError, ENICreationError):
                raise
            except Exception as network_error:
                raise NetworkConnectivityError(
                    f"Network connectivity validation error: {str(network_error)}",
                    error_type='validation_error',
                    connection_name=glue_connection_name
                )
            
            # Then validate actual database connection using Glue connection if specified
            try:
                if glue_connection_name:
                    # For cross-VPC connections, use the Glue connection for validation
                    connection_valid = self._validate_connection_with_glue(connection_config, glue_connection_name)
                else:
                    # For same-VPC connections, use standard validation
                    connection_valid = self.validate_connection(connection_config)
            except Exception as db_error:
                # Classify database connection errors
                if "connection refused" in str(db_error).lower():
                    raise NetworkConnectivityError(
                        f"Database connection refused: {str(db_error)}",
                        error_type='connection_refused',
                        connection_name=glue_connection_name
                    )
                elif "timeout" in str(db_error).lower():
                    raise NetworkConnectivityError(
                        f"Database connection timeout: {str(db_error)}",
                        error_type='connection_timeout',
                        connection_name=glue_connection_name
                    )
                else:
                    raise NetworkConnectivityError(
                        f"Database connection validation failed: {str(db_error)}",
                        error_type='database_connection_error',
                        connection_name=glue_connection_name
                    )
            
            duration = time.time() - start_time
            self.structured_logger.info(
                "Connection validation with network check completed successfully",
                engine_type=connection_config.engine_type,
                glue_connection_name=glue_connection_name or "same-vpc",
                cross_vpc_required=connection_config.requires_cross_vpc_connection(),
                validation_result="passed" if connection_valid else "failed",
                duration_seconds=round(duration, 2)
            )
            
            return connection_valid
            
        except (NetworkConnectivityError, GlueConnectionError, VpcEndpointError, ENICreationError):
            duration = time.time() - start_time
            self.structured_logger.error(
                "Connection validation with network check failed with network error",
                engine_type=connection_config.engine_type,
                glue_connection_name=glue_connection_name or "same-vpc",
                cross_vpc_required=connection_config.requires_cross_vpc_connection(),
                duration_seconds=round(duration, 2)
            )
            raise
        except Exception as e:
            duration = time.time() - start_time
            self.structured_logger.error(
                "Connection validation with network check failed with unexpected error",
                engine_type=connection_config.engine_type,
                glue_connection_name=glue_connection_name or "same-vpc",
                cross_vpc_required=connection_config.requires_cross_vpc_connection(),
                error=str(e),
                duration_seconds=round(duration, 2)
            )
            raise NetworkConnectivityError(
                f"Connection validation failed with unexpected error: {str(e)}",
                error_type='unexpected_error',
                connection_name=glue_connection_name
            )
    
    def _validate_connection_with_glue(self, connection_config: ConnectionConfig, glue_connection_name: str) -> bool:
        """Validate database connection using Glue connection for cross-VPC access with enhanced error handling."""
        try:
            # Setup JDBC properties using Glue connection with error handling
            try:
                jdbc_properties = self.glue_connection_manager.setup_jdbc_with_connection(
                    connection_config, glue_connection_name
                )
            except (GlueConnectionError, NetworkConnectivityError):
                raise
            except Exception as setup_error:
                raise NetworkConnectivityError(
                    f"Failed to setup JDBC properties for Glue connection '{glue_connection_name}': {str(setup_error)}",
                    error_type='jdbc_setup_error',
                    connection_name=glue_connection_name
                )
            
            # Test connection by executing a simple query with timeout and error handling
            try:
                test_df = self.spark.read \
                    .format("jdbc") \
                    .option("url", jdbc_properties['url']) \
                    .option("user", jdbc_properties['user']) \
                    .option("password", jdbc_properties['password']) \
                    .option("driver", jdbc_properties['driver']) \
                    .option("query", "SELECT 1 as test_column") \
                    .option("connectTimeout", "30") \
                    .option("socketTimeout", "60") \
                    .load()
                
                # Execute the query to test connectivity
                test_result = test_df.collect()
                
                if test_result and len(test_result) > 0:
                    self.structured_logger.info(
                        "Database connection validation successful using Glue connection",
                        glue_connection_name=glue_connection_name,
                        test_result_count=len(test_result)
                    )
                    return True
                else:
                    raise NetworkConnectivityError(
                        f"Database connection test returned no results for Glue connection '{glue_connection_name}'",
                        error_type='empty_test_result',
                        connection_name=glue_connection_name
                    )
                    
            except Exception as db_error:
                error_str = str(db_error).lower()
                
                # Classify database connection errors
                if "connection refused" in error_str or "connection reset" in error_str:
                    raise NetworkConnectivityError(
                        f"Database connection refused for Glue connection '{glue_connection_name}': {str(db_error)}",
                        error_type='connection_refused',
                        connection_name=glue_connection_name
                    )
                elif "timeout" in error_str or "timed out" in error_str:
                    raise NetworkConnectivityError(
                        f"Database connection timeout for Glue connection '{glue_connection_name}': {str(db_error)}",
                        error_type='connection_timeout',
                        connection_name=glue_connection_name
                    )
                elif "network" in error_str or "unreachable" in error_str:
                    raise NetworkConnectivityError(
                        f"Network unreachable for Glue connection '{glue_connection_name}': {str(db_error)}",
                        error_type='network_unreachable',
                        connection_name=glue_connection_name
                    )
                elif "eni" in error_str or "elastic network interface" in error_str:
                    raise ENICreationError(
                        f"ENI creation failed for Glue connection '{glue_connection_name}': {str(db_error)}",
                        error_code='eni_creation_failed'
                    )
                else:
                    raise NetworkConnectivityError(
                        f"Database connection validation failed for Glue connection '{glue_connection_name}': {str(db_error)}",
                        error_type='database_connection_error',
                        connection_name=glue_connection_name
                    )
        except (NetworkConnectivityError, GlueConnectionError, ENICreationError):
            raise
        except Exception as e:
            self.structured_logger.error(
                "Database connection validation failed using Glue connection with unexpected error",
                glue_connection_name=glue_connection_name,
                error=str(e)
            )
            raise NetworkConnectivityError(
                f"Unexpected error during database connection validation for Glue connection '{glue_connection_name}': {str(e)}",
                error_type='unexpected_validation_error',
                connection_name=glue_connection_name
            )
    
    def validate_connection(self, connection_config: ConnectionConfig) -> bool:
        """Validate database connection by executing a simple query using Spark DataFrame."""
        def _validate():
            properties = self.create_connection_properties(connection_config)
            
            # Use a simple query appropriate for each database type
            test_queries = {
                'oracle': 'SELECT 1 as test_column FROM DUAL',
                'sqlserver': 'SELECT 1 as test_column',
                'postgresql': 'SELECT 1 as test_column',
                'db2': 'SELECT 1 as test_column FROM SYSIBM.SYSDUMMY1'
            }
            
            test_query = test_queries.get(connection_config.engine_type.lower(), 'SELECT 1 as test_column')
            
            try:
                # Execute test query using Spark DataFrame (not DynamicFrame)
                reader = self.spark.read.format('jdbc')
                reader = reader.option('url', connection_config.connection_string)
                reader = reader.option('query', test_query)
                
                # Add connection properties
                for key, value in properties.items():
                    reader = reader.option(key, str(value))
                
                df = reader.load()
                
                # Trigger execution by collecting one row
                result = df.collect()
                
                if result and len(result) > 0:
                    logger.info(f"Connection validation successful for {connection_config.engine_type}")
                    return True
                else:
                    raise RuntimeError("Test query returned no results")
                    
            except Exception as e:
                logger.error(f"Connection validation failed for {connection_config.engine_type}: {str(e)}")
                raise
        
        try:
            return self.retry_handler.execute_with_retry(
                _validate,
                f"connection validation for {connection_config.engine_type}"
            )
        except Exception as e:
            logger.error(f"Connection validation failed after retries: {str(e)}")
            return False
    
    def get_table_schema(self, connection_config: ConnectionConfig, table_name: str) -> StructType:
        """Get table schema from database using Spark DataFrame."""
        def _get_schema():
            properties = self.create_connection_properties(connection_config)
            
            # Build full table name with schema
            full_table_name = f"{connection_config.schema}.{table_name}"
            
            try:
                # Read table schema by limiting to 0 rows using Spark DataFrame
                reader = self.spark.read.format('jdbc')
                reader = reader.option('url', connection_config.connection_string)
                reader = reader.option('dbtable', full_table_name)
                
                # Add connection properties
                for key, value in properties.items():
                    reader = reader.option(key, str(value))
                
                df = reader.load().limit(0)
                
                schema = df.schema
                logger.info(f"Retrieved schema for table {full_table_name}: {len(schema.fields)} columns")
                return schema
                
            except Exception as e:
                logger.error(f"Failed to get schema for table {full_table_name}: {str(e)}")
                raise
        
        return self.retry_handler.execute_with_retry(
            _get_schema,
            f"schema retrieval for {connection_config.schema}.{table_name}"
        )
    
    def read_table_data(self, connection_config: ConnectionConfig, table_name: str, 
                       query: Optional[str] = None, **options) -> DataFrame:
        """Read data from database table using Spark DataFrame (not DynamicFrame)."""
        def _read_data():
            properties = self.create_connection_properties(connection_config)
            
            # Add any additional options
            properties.update(options)
            
            try:
                # Use Spark DataFrame reader directly (not DynamicFrame)
                reader = self.spark.read.format('jdbc')
                
                # Set connection properties
                reader = reader.option('url', connection_config.connection_string)
                for key, value in properties.items():
                    reader = reader.option(key, str(value))
                
                if query:
                    # Use custom query
                    df = reader.option('query', query).load()
                else:
                    # Use table name
                    full_table_name = f"{connection_config.schema}.{table_name}"
                    df = reader.option('dbtable', full_table_name).load()
                
                logger.info(f"Successfully read data from {connection_config.engine_type} table: {table_name}")
                return df
                
            except Exception as e:
                logger.error(f"Failed to read data from table {table_name}: {str(e)}")
                raise
        
        return self.retry_handler.execute_with_retry(
            _read_data,
            f"data reading from {connection_config.schema}.{table_name}"
        )
    
    def write_table_data(self, df: DataFrame, connection_config: ConnectionConfig, 
                        table_name: str, mode: str = 'append', **options) -> None:
        """Write data to database table using Spark DataFrame."""
        def _write_data():
            properties = self.create_connection_properties(connection_config)
            
            # Add any additional options
            if options:
                for key, value in options.items():
                    properties[key] = str(value)
            
            # Build full table name with schema
            full_table_name = f"{connection_config.schema}.{table_name}"
            
            try:
                # Build writer using Spark DataFrame (not DynamicFrame)
                writer = df.write.format('jdbc')
                writer = writer.option('url', connection_config.connection_string)
                writer = writer.option('dbtable', full_table_name)
                writer = writer.mode(mode)
                
                # Add properties individually
                for key, value in properties.items():
                    writer = writer.option(key, str(value))
                
                writer.save()
                
                logger.info(f"Successfully wrote data to {connection_config.engine_type} table: {table_name}")
                
            except Exception as e:
                logger.error(f"Failed to write data to table {table_name}: {str(e)}")
                raise
        
        self.retry_handler.execute_with_retry(
            _write_data,
            f"data writing to {connection_config.schema}.{table_name}"
        )
    
    def test_connection(self, connection_config: ConnectionConfig) -> Dict[str, Any]:
        """Test database connection and return connection information."""
        connection_info = {
            'engine_type': connection_config.engine_type,
            'database': connection_config.database,
            'schema': connection_config.schema,
            'connection_valid': False,
            'error_message': None,
            'test_timestamp': time.time()
        }
        
        try:
            # Validate connection
            is_valid = self.validate_connection(connection_config)
            connection_info['connection_valid'] = is_valid
            
            if is_valid:
                logger.info(f"Connection test successful for {connection_config.engine_type}")
            else:
                connection_info['error_message'] = "Connection validation failed"
                
        except Exception as e:
            connection_info['connection_valid'] = False
            connection_info['error_message'] = str(e)
            logger.error(f"Connection test failed for {connection_config.engine_type}: {str(e)}")
        
        return connection_info
    
    def get_connection_cache_key(self, connection_config: ConnectionConfig) -> str:
        """Generate cache key for connection configuration."""
        return f"{connection_config.engine_type}_{connection_config.database}_{connection_config.schema}_{connection_config.username}"
    
    def cache_connection_info(self, connection_config: ConnectionConfig, info: Dict[str, Any]) -> None:
        """Cache connection information for reuse."""
        cache_key = self.get_connection_cache_key(connection_config)
        self._connection_cache[cache_key] = info
        logger.debug(f"Cached connection info for {cache_key}")
    
    def get_cached_connection_info(self, connection_config: ConnectionConfig) -> Optional[Dict[str, Any]]:
        """Get cached connection information."""
        cache_key = self.get_connection_cache_key(connection_config)
        return self._connection_cache.get(cache_key)


class ErrorRecoveryManager:
    """Manages error recovery strategies and graceful failure handling."""
    
    def __init__(self, job_name: str, enable_detailed_logging: bool = True):
        self.job_name = job_name
        self.enable_detailed_logging = enable_detailed_logging
        self.error_history = []
        self.recovery_attempts = {}
        self.critical_errors = []
        
    def handle_database_connection_error(self, error: Exception, connection_config: ConnectionConfig, 
                                       operation_context: str) -> Dict[str, Any]:
        """Handle database connection errors with appropriate recovery strategies."""
        error_info = {
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'error_type': type(error).__name__,
            'error_message': str(error),
            'error_category': ErrorClassifier.classify_error(error),
            'operation_context': operation_context,
            'connection_details': {
                'engine_type': connection_config.engine_type,
                'database': connection_config.database,
                'schema': connection_config.schema,
                'connection_string': self._sanitize_connection_string(connection_config.connection_string)
            },
            'recovery_strategy': ErrorClassifier.get_recovery_strategy(error),
            'is_retryable': ErrorClassifier.is_retryable_error(error)
        }
        
        # Log detailed error information
        if self.enable_detailed_logging:
            logger.error(
                f"Database connection error in {operation_context}: {error_info['error_message']} "
                f"[Category: {error_info['error_category']}, Engine: {connection_config.engine_type}]"
            )
            
            # Log connection details (sanitized)
            logger.debug(f"Connection details: {error_info['connection_details']}")
        
        # Store error in history
        self.error_history.append(error_info)
        
        # Determine if this is a critical error that should stop the job
        if self._is_critical_error(error_info):
            self.critical_errors.append(error_info)
            logger.critical(
                f"Critical database error detected: {error_info['error_message']}. "
                f"Job may need to be terminated."
            )
        
        return error_info
    
    def handle_data_processing_error(self, error: Exception, table_name: str, 
                                   operation_type: str, context_data: Dict[str, Any] = None) -> Dict[str, Any]:
        """Handle data processing errors with recovery options."""
        error_info = {
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'error_type': type(error).__name__,
            'error_message': str(error),
            'error_category': ErrorClassifier.classify_error(error),
            'table_name': table_name,
            'operation_type': operation_type,
            'context_data': context_data or {},
            'recovery_strategy': ErrorClassifier.get_recovery_strategy(error),
            'is_retryable': ErrorClassifier.is_retryable_error(error)
        }
        
        # Log error with context
        logger.error(
            f"Data processing error for table {table_name} during {operation_type}: "
            f"{error_info['error_message']} [Category: {error_info['error_category']}]"
        )
        
        if context_data:
            logger.debug(f"Error context: {context_data}")
        
        # Store error in history
        self.error_history.append(error_info)
        
        # Handle specific data processing error types
        if error_info['error_category'] == ErrorCategory.SCHEMA_MISMATCH:
            logger.error(
                f"Schema mismatch detected for table {table_name}. "
                f"Please verify that target schema matches source schema requirements."
            )
        elif error_info['error_category'] == ErrorCategory.DATA_PROCESSING:
            logger.warning(
                f"Data processing issue for table {table_name}. "
                f"Attempting to continue with remaining data."
            )
        
        return error_info
    
    def handle_infrastructure_error(self, error: Exception, component: str, 
                                  operation_context: str) -> Dict[str, Any]:
        """Handle infrastructure-related errors (S3, IAM, Glue, etc.)."""
        error_info = {
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'error_type': type(error).__name__,
            'error_message': str(error),
            'error_category': ErrorClassifier.classify_error(error),
            'component': component,
            'operation_context': operation_context,
            'recovery_strategy': ErrorClassifier.get_recovery_strategy(error),
            'is_retryable': ErrorClassifier.is_retryable_error(error)
        }
        
        # Log infrastructure error
        logger.error(
            f"Infrastructure error in {component} during {operation_context}: "
            f"{error_info['error_message']} [Category: {error_info['error_category']}]"
        )
        
        # Store error in history
        self.error_history.append(error_info)
        
        # Handle specific infrastructure errors
        if 'S3' in component.upper() or 's3://' in str(error).lower():
            logger.error(
                f"S3 access error detected. Please verify:"
                f"\n- S3 bucket permissions and IAM role access"
                f"\n- JDBC driver file paths and availability"
                f"\n- Network connectivity to S3"
            )
        elif 'IAM' in component.upper() or 'permission' in str(error).lower():
            logger.error(
                f"IAM permission error detected. Please verify:"
                f"\n- Glue job execution role permissions"
                f"\n- Database access permissions"
                f"\n- S3 bucket access permissions"
            )
        
        return error_info
    
    def attempt_graceful_recovery(self, error_info: Dict[str, Any], 
                                recovery_context: Dict[str, Any] = None) -> bool:
        """Attempt graceful recovery based on error type and context."""
        recovery_key = f"{error_info['error_category']}_{error_info.get('table_name', 'global')}"
        
        # Track recovery attempts
        if recovery_key not in self.recovery_attempts:
            self.recovery_attempts[recovery_key] = 0
        
        self.recovery_attempts[recovery_key] += 1
        max_recovery_attempts = 3
        
        if self.recovery_attempts[recovery_key] > max_recovery_attempts:
            logger.error(
                f"Maximum recovery attempts ({max_recovery_attempts}) exceeded for {recovery_key}. "
                f"Giving up on recovery."
            )
            return False
        
        logger.info(
            f"Attempting graceful recovery for {recovery_key} "
            f"(attempt {self.recovery_attempts[recovery_key]}/{max_recovery_attempts})"
        )
        
        try:
            # Implement recovery strategies based on error category
            if error_info['error_category'] == ErrorCategory.CONNECTION:
                return self._recover_connection_error(error_info, recovery_context)
            elif error_info['error_category'] == ErrorCategory.DATA_PROCESSING:
                return self._recover_data_processing_error(error_info, recovery_context)
            elif error_info['error_category'] == ErrorCategory.RESOURCE:
                return self._recover_resource_error(error_info, recovery_context)
            elif error_info['error_category'] == ErrorCategory.TIMEOUT:
                return self._recover_timeout_error(error_info, recovery_context)
            else:
                logger.warning(f"No specific recovery strategy for category: {error_info['error_category']}")
                return False
                
        except Exception as recovery_error:
            logger.error(f"Recovery attempt failed: {str(recovery_error)}")
            return False
    
    def _recover_connection_error(self, error_info: Dict[str, Any], 
                                recovery_context: Dict[str, Any] = None) -> bool:
        """Attempt to recover from connection errors."""
        logger.info("Attempting connection error recovery...")
        
        # Wait before retry to allow transient issues to resolve
        time.sleep(5)
        
        # Could implement connection pool reset, alternative connection strings, etc.
        logger.info("Connection error recovery completed")
        return True
    
    def _recover_data_processing_error(self, error_info: Dict[str, Any], 
                                     recovery_context: Dict[str, Any] = None) -> bool:
        """Attempt to recover from data processing errors."""
        logger.info("Attempting data processing error recovery...")
        
        # Could implement data validation, schema refresh, etc.
        logger.info("Data processing error recovery completed")
        return True
    
    def _recover_resource_error(self, error_info: Dict[str, Any], 
                              recovery_context: Dict[str, Any] = None) -> bool:
        """Attempt to recover from resource exhaustion errors."""
        logger.info("Attempting resource error recovery...")
        
        # Wait longer for resource exhaustion
        time.sleep(30)
        
        # Could implement memory cleanup, connection pool management, etc.
        logger.info("Resource error recovery completed")
        return True
    
    def _recover_timeout_error(self, error_info: Dict[str, Any], 
                             recovery_context: Dict[str, Any] = None) -> bool:
        """Attempt to recover from timeout errors."""
        logger.info("Attempting timeout error recovery...")
        
        # Wait before retry
        time.sleep(10)
        
        # Could implement query optimization, batch size reduction, etc.
        logger.info("Timeout error recovery completed")
        return True
    
    def _is_critical_error(self, error_info: Dict[str, Any]) -> bool:
        """Determine if an error is critical and should stop the job."""
        critical_categories = {
            ErrorCategory.AUTHENTICATION,
            ErrorCategory.PERMISSION,
            ErrorCategory.SCHEMA_MISMATCH
        }
        
        return error_info['error_category'] in critical_categories
    
    def _sanitize_connection_string(self, connection_string: str) -> str:
        """Remove sensitive information from connection string for logging."""
        import re
        
        # Remove password from connection string
        sanitized = re.sub(r'password=[^;]+', 'password=***', connection_string, flags=re.IGNORECASE)
        sanitized = re.sub(r'pwd=[^;]+', 'pwd=***', sanitized, flags=re.IGNORECASE)
        
        return sanitized
    
    def get_error_summary(self) -> Dict[str, Any]:
        """Get summary of all errors encountered during job execution."""
        if not self.error_history:
            return {'total_errors': 0, 'error_categories': {}, 'critical_errors': 0}
        
        # Count errors by category
        error_categories = {}
        for error in self.error_history:
            category = error['error_category']
            error_categories[category] = error_categories.get(category, 0) + 1
        
        return {
            'total_errors': len(self.error_history),
            'error_categories': error_categories,
            'critical_errors': len(self.critical_errors),
            'recovery_attempts': dict(self.recovery_attempts),
            'latest_errors': self.error_history[-5:] if len(self.error_history) > 5 else self.error_history
        }
    
    def log_final_error_report(self) -> None:
        """Log final error report at job completion."""
        summary = self.get_error_summary()
        
        if summary['total_errors'] == 0:
            logger.info(f"Job {self.job_name} completed without errors")
            return
        
        logger.info(
            f"Job {self.job_name} error summary: "
            f"{summary['total_errors']} total errors, "
            f"{summary['critical_errors']} critical errors"
        )
        
        # Log error breakdown by category
        for category, count in summary['error_categories'].items():
            logger.info(f"  {category}: {count} errors")
        
        # Log recovery attempts
        if summary['recovery_attempts']:
            logger.info("Recovery attempts:")
            for recovery_key, attempts in summary['recovery_attempts'].items():
                logger.info(f"  {recovery_key}: {attempts} attempts")
        
        # Log critical errors if any
        if summary['critical_errors'] > 0:
            logger.error(f"Critical errors detected ({summary['critical_errors']}):")
            for error in self.critical_errors:
                logger.error(
                    f"  {error['timestamp']}: {error['error_message']} "
                    f"[{error['error_category']}]"
                )


@dataclass
class FullLoadProgress:
    """Tracks progress of full-load operations."""
    table_name: str
    total_rows: int = 0
    processed_rows: int = 0
    start_time: float = 0.0
    end_time: Optional[float] = None
    status: str = 'pending'  # pending, in_progress, completed, failed
    error_message: Optional[str] = None
    
    @property
    def progress_percentage(self) -> float:
        """Calculate progress percentage."""
        if self.total_rows == 0:
            return 0.0
        return (self.processed_rows / self.total_rows) * 100.0
    
    @property
    def duration_seconds(self) -> float:
        """Calculate operation duration in seconds."""
        if self.start_time == 0.0:
            return 0.0
        end_time = self.end_time or time.time()
        return end_time - self.start_time
    
    @property
    def rows_per_second(self) -> float:
        """Calculate processing rate in rows per second."""
        duration = self.duration_seconds
        if duration == 0.0:
            return 0.0
        return self.processed_rows / duration


@dataclass
class IncrementalLoadProgress:
    """Tracks progress of incremental-load operations."""
    table_name: str
    incremental_strategy: str  # timestamp, primary_key, hash
    incremental_column: Optional[str] = None
    last_processed_value: Optional[Any] = None
    current_max_value: Optional[Any] = None
    delta_rows: int = 0
    processed_rows: int = 0
    start_time: float = 0.0
    end_time: Optional[float] = None
    status: str = 'pending'  # pending, in_progress, completed, failed
    error_message: Optional[str] = None
    bookmark_state: Optional[Dict[str, Any]] = None
    
    @property
    def progress_percentage(self) -> float:
        """Calculate progress percentage."""
        if self.delta_rows == 0:
            return 100.0 if self.status == 'completed' else 0.0
        return (self.processed_rows / self.delta_rows) * 100.0
    
    @property
    def duration_seconds(self) -> float:
        """Calculate operation duration in seconds."""
        if self.start_time == 0.0:
            return 0.0
        end_time = self.end_time or time.time()
        return end_time - self.start_time
    
    @property
    def rows_per_second(self) -> float:
        """Calculate processing rate in rows per second."""
        duration = self.duration_seconds
        if duration == 0.0:
            return 0.0
        return self.processed_rows / duration


@dataclass
class JobBookmarkState:
    """Represents job bookmark state for a table with S3 compatibility."""
    table_name: str
    incremental_strategy: str
    incremental_column: Optional[str] = None
    last_processed_value: Optional[Any] = None
    last_update_timestamp: Optional[datetime] = None
    row_hash_checkpoint: Optional[str] = None
    is_first_run: bool = True
    
    # New S3-specific fields
    job_name: str = ""
    created_timestamp: Optional[datetime] = None
    updated_timestamp: Optional[datetime] = None
    version: str = "1.0"
    s3_key: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert bookmark state to dictionary for storage (legacy method)."""
        return {
            'table_name': self.table_name,
            'incremental_strategy': self.incremental_strategy,
            'incremental_column': self.incremental_column,
            'last_processed_value': str(self.last_processed_value) if self.last_processed_value is not None else None,
            'last_update_timestamp': self.last_update_timestamp.isoformat() if self.last_update_timestamp else None,
            'row_hash_checkpoint': self.row_hash_checkpoint,
            'is_first_run': self.is_first_run
        }
    
    def to_s3_dict(self) -> Dict[str, Any]:
        """Convert bookmark state to S3-compatible dictionary with ISO timestamps."""
        # Set updated_timestamp to current time if not set
        current_time = datetime.now(timezone.utc)
        if self.updated_timestamp is None:
            self.updated_timestamp = current_time
        
        # Set created_timestamp if not set (for new bookmarks)
        if self.created_timestamp is None:
            self.created_timestamp = current_time
        
        return {
            'table_name': self.table_name,
            'incremental_strategy': self.incremental_strategy,
            'incremental_column': self.incremental_column,
            'last_processed_value': str(self.last_processed_value) if self.last_processed_value is not None else None,
            'last_update_timestamp': self.last_update_timestamp.isoformat() if self.last_update_timestamp else None,
            'row_hash_checkpoint': self.row_hash_checkpoint,
            'is_first_run': self.is_first_run,
            'job_name': self.job_name,
            'created_timestamp': self.created_timestamp.isoformat() if self.created_timestamp else None,
            'updated_timestamp': self.updated_timestamp.isoformat() if self.updated_timestamp else None,
            'version': self.version,
            's3_key': self.s3_key
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'JobBookmarkState':
        """Create bookmark state from dictionary (legacy method)."""
        last_update_timestamp = None
        if data.get('last_update_timestamp'):
            last_update_timestamp = datetime.fromisoformat(data['last_update_timestamp'])
        
        return cls(
            table_name=data['table_name'],
            incremental_strategy=data['incremental_strategy'],
            incremental_column=data.get('incremental_column'),
            last_processed_value=data.get('last_processed_value'),
            last_update_timestamp=last_update_timestamp,
            row_hash_checkpoint=data.get('row_hash_checkpoint'),
            is_first_run=data.get('is_first_run', True)
        )
    
    @classmethod
    def from_s3_dict(cls, data: Dict[str, Any]) -> 'JobBookmarkState':
        """Create bookmark state from S3 dictionary with validation."""
        # Validate required fields
        if not cls._validate_s3_data(data):
            raise ValueError("Invalid S3 bookmark data structure")
        
        # Parse timestamps with timezone handling
        last_update_timestamp = None
        if data.get('last_update_timestamp'):
            try:
                # Handle both timezone-aware and naive timestamps
                timestamp_str = data['last_update_timestamp']
                if timestamp_str.endswith('Z'):
                    # Replace Z with +00:00 for proper parsing
                    timestamp_str = timestamp_str[:-1] + '+00:00'
                elif '+' not in timestamp_str and timestamp_str.count(':') == 2:
                    # Assume UTC if no timezone info
                    timestamp_str += '+00:00'
                last_update_timestamp = datetime.fromisoformat(timestamp_str)
            except (ValueError, TypeError) as e:
                logger.warning(f"Failed to parse last_update_timestamp '{data.get('last_update_timestamp')}': {e}")
                last_update_timestamp = None
        
        created_timestamp = None
        if data.get('created_timestamp'):
            try:
                timestamp_str = data['created_timestamp']
                if timestamp_str.endswith('Z'):
                    timestamp_str = timestamp_str[:-1] + '+00:00'
                elif '+' not in timestamp_str and timestamp_str.count(':') == 2:
                    timestamp_str += '+00:00'
                created_timestamp = datetime.fromisoformat(timestamp_str)
            except (ValueError, TypeError) as e:
                logger.warning(f"Failed to parse created_timestamp '{data.get('created_timestamp')}': {e}")
                created_timestamp = None
        
        updated_timestamp = None
        if data.get('updated_timestamp'):
            try:
                timestamp_str = data['updated_timestamp']
                if timestamp_str.endswith('Z'):
                    timestamp_str = timestamp_str[:-1] + '+00:00'
                elif '+' not in timestamp_str and timestamp_str.count(':') == 2:
                    timestamp_str += '+00:00'
                updated_timestamp = datetime.fromisoformat(timestamp_str)
            except (ValueError, TypeError) as e:
                logger.warning(f"Failed to parse updated_timestamp '{data.get('updated_timestamp')}': {e}")
                updated_timestamp = None
        
        return cls(
            table_name=data['table_name'],
            incremental_strategy=data['incremental_strategy'],
            incremental_column=data.get('incremental_column'),
            last_processed_value=data.get('last_processed_value'),
            last_update_timestamp=last_update_timestamp,
            row_hash_checkpoint=data.get('row_hash_checkpoint'),
            is_first_run=data.get('is_first_run', True),
            job_name=data.get('job_name', ''),
            created_timestamp=created_timestamp,
            updated_timestamp=updated_timestamp,
            version=data.get('version', '1.0'),
            s3_key=data.get('s3_key')
        )
    
    @staticmethod
    def _validate_s3_data(data: Dict[str, Any]) -> bool:
        """Validate S3 bookmark data structure and handle missing fields."""
        if not isinstance(data, dict):
            logger.error("Bookmark data must be a dictionary")
            return False
        
        # Check required fields
        required_fields = ['table_name', 'incremental_strategy']
        for field in required_fields:
            if field not in data or data[field] is None:
                logger.error(f"Missing required field in bookmark data: {field}")
                return False
            if not isinstance(data[field], str) or not data[field].strip():
                logger.error(f"Invalid value for required field '{field}': must be non-empty string")
                return False
        
        # Validate incremental_strategy values
        valid_strategies = ['timestamp', 'primary_key', 'hash', 'full_load']
        if data['incremental_strategy'] not in valid_strategies:
            logger.error(f"Invalid incremental_strategy '{data['incremental_strategy']}'. Must be one of: {valid_strategies}")
            return False
        
        # Validate optional fields if present
        if 'is_first_run' in data and not isinstance(data['is_first_run'], bool):
            logger.error(f"Invalid is_first_run value: must be boolean, got {type(data['is_first_run'])}")
            return False
        
        if 'version' in data and not isinstance(data['version'], str):
            logger.error(f"Invalid version value: must be string, got {type(data['version'])}")
            return False
        
        # Validate timestamp formats if present
        timestamp_fields = ['last_update_timestamp', 'created_timestamp', 'updated_timestamp']
        for field in timestamp_fields:
            if field in data and data[field] is not None:
                if not isinstance(data[field], str):
                    logger.error(f"Invalid {field} value: must be ISO format string, got {type(data[field])}")
                    return False
                # Basic ISO format validation
                try:
                    timestamp_str = data[field]
                    if timestamp_str.endswith('Z'):
                        timestamp_str = timestamp_str[:-1] + '+00:00'
                    elif '+' not in timestamp_str and timestamp_str.count(':') == 2:
                        timestamp_str += '+00:00'
                    datetime.fromisoformat(timestamp_str)
                except (ValueError, TypeError):
                    logger.error(f"Invalid {field} format: must be valid ISO timestamp")
                    return False
        
        logger.debug("S3 bookmark data validation passed")
        return True
    
    def validate_s3_data(self) -> bool:
        """Validate current bookmark state for S3 compatibility."""
        # Convert to dict and validate
        try:
            s3_dict = self.to_s3_dict()
            return self._validate_s3_data(s3_dict)
        except Exception as e:
            logger.error(f"Failed to validate bookmark state: {e}")
            return False


class DataTypeMapper:
    """Handles data type mapping between different database engines."""
    
    # Comprehensive data type mappings for cross-database compatibility
    TYPE_MAPPINGS = {
        'oracle_to_postgresql': {
            'NUMBER': 'NUMERIC',
            'VARCHAR2': 'VARCHAR',
            'NVARCHAR2': 'VARCHAR',
            'CHAR': 'CHAR',
            'NCHAR': 'CHAR',
            'DATE': 'TIMESTAMP',
            'TIMESTAMP': 'TIMESTAMP',
            'CLOB': 'TEXT',
            'NCLOB': 'TEXT',
            'BLOB': 'BYTEA',
            'RAW': 'BYTEA',
            'LONG': 'TEXT',
            'LONG RAW': 'BYTEA',
            'BINARY_FLOAT': 'REAL',
            'BINARY_DOUBLE': 'DOUBLE PRECISION'
        },
        'oracle_to_sqlserver': {
            'NUMBER': 'NUMERIC',
            'VARCHAR2': 'NVARCHAR',
            'NVARCHAR2': 'NVARCHAR',
            'CHAR': 'CHAR',
            'NCHAR': 'NCHAR',
            'DATE': 'DATETIME2',
            'TIMESTAMP': 'DATETIME2',
            'CLOB': 'NTEXT',
            'NCLOB': 'NTEXT',
            'BLOB': 'VARBINARY',
            'RAW': 'VARBINARY',
            'LONG': 'NTEXT',
            'LONG RAW': 'VARBINARY',
            'BINARY_FLOAT': 'REAL',
            'BINARY_DOUBLE': 'FLOAT'
        },
        'oracle_to_db2': {
            'NUMBER': 'DECIMAL',
            'VARCHAR2': 'VARCHAR',
            'NVARCHAR2': 'VARCHAR',
            'CHAR': 'CHAR',
            'NCHAR': 'CHAR',
            'DATE': 'TIMESTAMP',
            'TIMESTAMP': 'TIMESTAMP',
            'CLOB': 'CLOB',
            'NCLOB': 'CLOB',
            'BLOB': 'BLOB',
            'RAW': 'VARBINARY',
            'LONG': 'CLOB',
            'LONG RAW': 'BLOB',
            'BINARY_FLOAT': 'REAL',
            'BINARY_DOUBLE': 'DOUBLE'
        },
        'sqlserver_to_postgresql': {
            'INT': 'INTEGER',
            'BIGINT': 'BIGINT',
            'SMALLINT': 'SMALLINT',
            'TINYINT': 'SMALLINT',
            'DECIMAL': 'NUMERIC',
            'NUMERIC': 'NUMERIC',
            'FLOAT': 'DOUBLE PRECISION',
            'REAL': 'REAL',
            'VARCHAR': 'VARCHAR',
            'NVARCHAR': 'VARCHAR',
            'CHAR': 'CHAR',
            'NCHAR': 'CHAR',
            'TEXT': 'TEXT',
            'NTEXT': 'TEXT',
            'DATETIME': 'TIMESTAMP',
            'DATETIME2': 'TIMESTAMP',
            'DATE': 'DATE',
            'TIME': 'TIME',
            'BIT': 'BOOLEAN',
            'VARBINARY': 'BYTEA',
            'IMAGE': 'BYTEA',
            'MONEY': 'NUMERIC',
            'SMALLMONEY': 'NUMERIC',
            'UNIQUEIDENTIFIER': 'UUID'
        },
        'sqlserver_to_oracle': {
            'INT': 'NUMBER',
            'BIGINT': 'NUMBER',
            'SMALLINT': 'NUMBER',
            'TINYINT': 'NUMBER',
            'DECIMAL': 'NUMBER',
            'NUMERIC': 'NUMBER',
            'FLOAT': 'BINARY_DOUBLE',
            'REAL': 'BINARY_FLOAT',
            'VARCHAR': 'VARCHAR2',
            'NVARCHAR': 'NVARCHAR2',
            'CHAR': 'CHAR',
            'NCHAR': 'NCHAR',
            'TEXT': 'CLOB',
            'NTEXT': 'NCLOB',
            'DATETIME': 'TIMESTAMP',
            'DATETIME2': 'TIMESTAMP',
            'DATE': 'DATE',
            'TIME': 'TIMESTAMP',
            'BIT': 'NUMBER',
            'VARBINARY': 'RAW',
            'IMAGE': 'BLOB',
            'MONEY': 'NUMBER',
            'SMALLMONEY': 'NUMBER',
            'UNIQUEIDENTIFIER': 'RAW'
        },
        'sqlserver_to_db2': {
            'INT': 'INTEGER',
            'BIGINT': 'BIGINT',
            'SMALLINT': 'SMALLINT',
            'TINYINT': 'SMALLINT',
            'DECIMAL': 'DECIMAL',
            'NUMERIC': 'NUMERIC',
            'FLOAT': 'DOUBLE',
            'REAL': 'REAL',
            'VARCHAR': 'VARCHAR',
            'NVARCHAR': 'VARCHAR',
            'CHAR': 'CHAR',
            'NCHAR': 'CHAR',
            'TEXT': 'CLOB',
            'NTEXT': 'CLOB',
            'DATETIME': 'TIMESTAMP',
            'DATETIME2': 'TIMESTAMP',
            'DATE': 'DATE',
            'TIME': 'TIME',
            'BIT': 'SMALLINT',
            'VARBINARY': 'VARBINARY',
            'IMAGE': 'BLOB',
            'MONEY': 'DECIMAL',
            'SMALLMONEY': 'DECIMAL',
            'UNIQUEIDENTIFIER': 'CHAR'
        },
        'postgresql_to_oracle': {
            'INTEGER': 'NUMBER',
            'BIGINT': 'NUMBER',
            'SMALLINT': 'NUMBER',
            'NUMERIC': 'NUMBER',
            'DECIMAL': 'NUMBER',
            'REAL': 'BINARY_FLOAT',
            'DOUBLE PRECISION': 'BINARY_DOUBLE',
            'VARCHAR': 'VARCHAR2',
            'CHAR': 'CHAR',
            'TEXT': 'CLOB',
            'DATE': 'DATE',
            'TIME': 'TIMESTAMP',
            'TIMESTAMP': 'TIMESTAMP',
            'BOOLEAN': 'NUMBER',
            'BYTEA': 'BLOB',
            'UUID': 'RAW',
            'JSON': 'CLOB',
            'JSONB': 'CLOB'
        },
        'postgresql_to_sqlserver': {
            'INTEGER': 'INT',
            'BIGINT': 'BIGINT',
            'SMALLINT': 'SMALLINT',
            'NUMERIC': 'NUMERIC',
            'DECIMAL': 'DECIMAL',
            'REAL': 'REAL',
            'DOUBLE PRECISION': 'FLOAT',
            'VARCHAR': 'NVARCHAR',
            'CHAR': 'CHAR',
            'TEXT': 'NTEXT',
            'DATE': 'DATE',
            'TIME': 'TIME',
            'TIMESTAMP': 'DATETIME2',
            'BOOLEAN': 'BIT',
            'BYTEA': 'VARBINARY',
            'UUID': 'UNIQUEIDENTIFIER',
            'JSON': 'NTEXT',
            'JSONB': 'NTEXT'
        },
        'postgresql_to_db2': {
            'INTEGER': 'INTEGER',
            'BIGINT': 'BIGINT',
            'SMALLINT': 'SMALLINT',
            'NUMERIC': 'DECIMAL',
            'DECIMAL': 'DECIMAL',
            'REAL': 'REAL',
            'DOUBLE PRECISION': 'DOUBLE',
            'VARCHAR': 'VARCHAR',
            'CHAR': 'CHAR',
            'TEXT': 'CLOB',
            'DATE': 'DATE',
            'TIME': 'TIME',
            'TIMESTAMP': 'TIMESTAMP',
            'BOOLEAN': 'SMALLINT',
            'BYTEA': 'BLOB',
            'UUID': 'CHAR',
            'JSON': 'CLOB',
            'JSONB': 'CLOB'
        },
        'db2_to_postgresql': {
            'INTEGER': 'INTEGER',
            'BIGINT': 'BIGINT',
            'SMALLINT': 'SMALLINT',
            'DECIMAL': 'NUMERIC',
            'NUMERIC': 'NUMERIC',
            'REAL': 'REAL',
            'DOUBLE': 'DOUBLE PRECISION',
            'VARCHAR': 'VARCHAR',
            'CHAR': 'CHAR',
            'CLOB': 'TEXT',
            'DATE': 'DATE',
            'TIME': 'TIME',
            'TIMESTAMP': 'TIMESTAMP',
            'BLOB': 'BYTEA',
            'VARBINARY': 'BYTEA',
            'GRAPHIC': 'VARCHAR',
            'VARGRAPHIC': 'VARCHAR'
        },
        'db2_to_oracle': {
            'INTEGER': 'NUMBER',
            'BIGINT': 'NUMBER',
            'SMALLINT': 'NUMBER',
            'DECIMAL': 'NUMBER',
            'NUMERIC': 'NUMBER',
            'REAL': 'BINARY_FLOAT',
            'DOUBLE': 'BINARY_DOUBLE',
            'VARCHAR': 'VARCHAR2',
            'CHAR': 'CHAR',
            'CLOB': 'CLOB',
            'DATE': 'DATE',
            'TIME': 'TIMESTAMP',
            'TIMESTAMP': 'TIMESTAMP',
            'BLOB': 'BLOB',
            'VARBINARY': 'RAW',
            'GRAPHIC': 'NVARCHAR2',
            'VARGRAPHIC': 'NVARCHAR2'
        },
        'db2_to_sqlserver': {
            'INTEGER': 'INT',
            'BIGINT': 'BIGINT',
            'SMALLINT': 'SMALLINT',
            'DECIMAL': 'DECIMAL',
            'NUMERIC': 'NUMERIC',
            'REAL': 'REAL',
            'DOUBLE': 'FLOAT',
            'VARCHAR': 'NVARCHAR',
            'CHAR': 'CHAR',
            'CLOB': 'NTEXT',
            'DATE': 'DATE',
            'TIME': 'TIME',
            'TIMESTAMP': 'DATETIME2',
            'BLOB': 'VARBINARY',
            'VARBINARY': 'VARBINARY',
            'GRAPHIC': 'NVARCHAR',
            'VARGRAPHIC': 'NVARCHAR'
        }
    }
    
    # Data transformation functions for specific type conversions
    TRANSFORMATION_FUNCTIONS = {
        'oracle_to_postgresql': {
            'DATE': lambda col: col.cast('timestamp'),
            'NUMBER': lambda col: col.cast('decimal(38,10)'),
            'CLOB': lambda col: col.cast('string')
        },
        'sqlserver_to_postgresql': {
            'BIT': lambda col: col.cast('boolean'),
            'DATETIME': lambda col: col.cast('timestamp'),
            'UNIQUEIDENTIFIER': lambda col: col.cast('string')
        },
        'postgresql_to_oracle': {
            'BOOLEAN': lambda col: col.cast('int'),
            'UUID': lambda col: col.cast('string'),
            'JSON': lambda col: col.cast('string'),
            'JSONB': lambda col: col.cast('string')
        },
        'postgresql_to_sqlserver': {
            'UUID': lambda col: col.cast('string'),
            'JSON': lambda col: col.cast('string'),
            'JSONB': lambda col: col.cast('string')
        }
    }
    
    @classmethod
    def get_mapping_key(cls, source_engine: str, target_engine: str) -> str:
        """Generate mapping key for source-target engine combination."""
        return f"{source_engine.lower()}_to_{target_engine.lower()}"
    
    @classmethod
    def map_data_type(cls, source_engine: str, target_engine: str, source_type: str) -> str:
        """Map data type from source engine to target engine."""
        # If same engine, no mapping needed
        if source_engine.lower() == target_engine.lower():
            return source_type
        
        mapping_key = cls.get_mapping_key(source_engine, target_engine)
        type_mapping = cls.TYPE_MAPPINGS.get(mapping_key, {})
        
        # Return mapped type or original if no mapping exists
        mapped_type = type_mapping.get(source_type.upper(), source_type)
        
        if mapped_type == source_type and mapping_key in cls.TYPE_MAPPINGS:
            logger.warning(
                f"No type mapping found for {source_type} in {source_engine} -> {target_engine} conversion. "
                f"Using original type: {source_type}"
            )
        
        return mapped_type
    
    @classmethod
    def is_cross_database_replication(cls, source_engine: str, target_engine: str) -> bool:
        """Check if this is a cross-database replication scenario."""
        return source_engine.lower() != target_engine.lower()
    
    @classmethod
    def get_supported_mappings(cls) -> List[str]:
        """Get list of all supported database engine mappings."""
        return list(cls.TYPE_MAPPINGS.keys())
    
    @classmethod
    def is_mapping_supported(cls, source_engine: str, target_engine: str) -> bool:
        """Check if mapping between source and target engines is supported."""
        mapping_key = cls.get_mapping_key(source_engine, target_engine)
        return mapping_key in cls.TYPE_MAPPINGS
    
    @classmethod
    def transform_dataframe_types(cls, df: DataFrame, source_engine: str, target_engine: str) -> DataFrame:
        """Transform DataFrame column types for cross-database compatibility."""
        if not cls.is_cross_database_replication(source_engine, target_engine):
            return df
        
        mapping_key = cls.get_mapping_key(source_engine, target_engine)
        transformation_funcs = cls.TRANSFORMATION_FUNCTIONS.get(mapping_key, {})
        
        if not transformation_funcs:
            logger.info(f"No specific transformations needed for {mapping_key}")
            return df
        
        transformed_df = df
        
        for field in df.schema.fields:
            field_type = str(field.dataType).upper()
            
            # Check if this field type needs transformation
            for source_type, transform_func in transformation_funcs.items():
                if source_type in field_type:
                    logger.info(f"Transforming column {field.name} from {field_type} using {source_type} transformation")
                    transformed_df = transformed_df.withColumn(field.name, transform_func(col(field.name)))
                    break
        
        return transformed_df


class SchemaCompatibilityValidator:
    """Validates schema compatibility between source and target databases for cross-database replication."""
    
    def __init__(self, data_type_mapper: DataTypeMapper):
        self.data_type_mapper = data_type_mapper
    
    def validate_schema_compatibility(self, source_schema: StructType, target_schema: StructType,
                                    source_engine: str, target_engine: str, table_name: str) -> Dict[str, Any]:
        """Validate that source and target schemas are compatible for replication."""
        validation_result = {
            'is_compatible': True,
            'warnings': [],
            'errors': [],
            'column_mappings': {},
            'missing_columns': [],
            'extra_columns': [],
            'type_mismatches': []
        }
        
        try:
            logger.info(f"Validating schema compatibility for table {table_name}: {source_engine} -> {target_engine}")
            
            # Create column dictionaries for easier comparison
            source_columns = {field.name.lower(): field for field in source_schema.fields}
            target_columns = {field.name.lower(): field for field in target_schema.fields}
            
            # Check for missing columns in target
            for col_name, source_field in source_columns.items():
                if col_name not in target_columns:
                    validation_result['missing_columns'].append(col_name)
                    validation_result['errors'].append(
                        f"Column '{col_name}' exists in source but missing in target schema"
                    )
                    validation_result['is_compatible'] = False
            
            # Check for extra columns in target (warnings only)
            for col_name in target_columns:
                if col_name not in source_columns:
                    validation_result['extra_columns'].append(col_name)
                    validation_result['warnings'].append(
                        f"Column '{col_name}' exists in target but not in source schema"
                    )
            
            # Validate data type compatibility for matching columns
            for col_name, source_field in source_columns.items():
                if col_name in target_columns:
                    target_field = target_columns[col_name]
                    
                    # Get string representations of data types
                    source_type_str = self._get_type_string(source_field.dataType)
                    target_type_str = self._get_type_string(target_field.dataType)
                    
                    # Map source type to expected target type
                    expected_target_type = self.data_type_mapper.map_data_type(
                        source_engine, target_engine, source_type_str
                    )
                    
                    # Check if target type is compatible
                    if not self._are_types_compatible(target_type_str, expected_target_type):
                        mismatch_info = {
                            'column': col_name,
                            'source_type': source_type_str,
                            'target_type': target_type_str,
                            'expected_type': expected_target_type
                        }
                        validation_result['type_mismatches'].append(mismatch_info)
                        validation_result['errors'].append(
                            f"Type mismatch for column '{col_name}': source={source_type_str}, "
                            f"target={target_type_str}, expected={expected_target_type}"
                        )
                        validation_result['is_compatible'] = False
                    
                    # Store column mapping
                    validation_result['column_mappings'][col_name] = {
                        'source_type': source_type_str,
                        'target_type': target_type_str,
                        'mapped_type': expected_target_type
                    }
            
            # Log validation results
            if validation_result['is_compatible']:
                logger.info(f"Schema validation passed for table {table_name}")
                if validation_result['warnings']:
                    logger.warning(f"Schema validation warnings for table {table_name}: {len(validation_result['warnings'])} warnings")
            else:
                logger.error(f"Schema validation failed for table {table_name}: {len(validation_result['errors'])} errors")
            
            return validation_result
            
        except Exception as e:
            logger.error(f"Schema validation failed with exception for table {table_name}: {str(e)}")
            validation_result['is_compatible'] = False
            validation_result['errors'].append(f"Validation exception: {str(e)}")
            return validation_result
    
    def _get_type_string(self, data_type) -> str:
        """Convert Spark DataType to string representation."""
        type_str = str(data_type)
        
        # Normalize common type representations
        type_mappings = {
            'StringType': 'VARCHAR',
            'IntegerType': 'INTEGER',
            'LongType': 'BIGINT',
            'DoubleType': 'DOUBLE',
            'FloatType': 'FLOAT',
            'BooleanType': 'BOOLEAN',
            'TimestampType': 'TIMESTAMP',
            'DateType': 'DATE',
            'BinaryType': 'BINARY',
            'DecimalType': 'DECIMAL'
        }
        
        for spark_type, db_type in type_mappings.items():
            if spark_type in type_str:
                return db_type
        
        return type_str.upper()
    
    def _are_types_compatible(self, actual_type: str, expected_type: str) -> bool:
        """Check if actual and expected types are compatible."""
        # Normalize types for comparison
        actual = actual_type.upper().strip()
        expected = expected_type.upper().strip()
        
        # Exact match
        if actual == expected:
            return True
        
        # Define compatible type groups
        compatible_groups = [
            {'VARCHAR', 'NVARCHAR', 'TEXT', 'STRING', 'CHAR', 'NCHAR'},
            {'INTEGER', 'INT', 'SMALLINT', 'BIGINT', 'NUMBER'},
            {'DECIMAL', 'NUMERIC', 'NUMBER'},
            {'FLOAT', 'REAL', 'DOUBLE', 'DOUBLE PRECISION'},
            {'TIMESTAMP', 'DATETIME', 'DATETIME2'},
            {'BYTEA', 'VARBINARY', 'BINARY', 'BLOB', 'RAW'},
            {'BOOLEAN', 'BIT'},
            {'DATE'},
            {'TIME'},
            {'UUID', 'UNIQUEIDENTIFIER'}
        ]
        
        # Check if both types are in the same compatibility group
        for group in compatible_groups:
            if actual in group and expected in group:
                return True
        
        # Special cases for partial matches
        if 'VARCHAR' in actual and 'VARCHAR' in expected:
            return True
        if 'DECIMAL' in actual and 'DECIMAL' in expected:
            return True
        if 'NUMERIC' in actual and 'NUMERIC' in expected:
            return True
        
        return False
    
    def generate_compatibility_report(self, validation_result: Dict[str, Any], table_name: str) -> str:
        """Generate a human-readable compatibility report."""
        report_lines = [
            f"Schema Compatibility Report for Table: {table_name}",
            "=" * 60,
            f"Overall Status: {'COMPATIBLE' if validation_result['is_compatible'] else 'INCOMPATIBLE'}",
            ""
        ]
        
        if validation_result['errors']:
            report_lines.extend([
                "ERRORS:",
                "-" * 20
            ])
            for error in validation_result['errors']:
                report_lines.append(f"  • {error}")
            report_lines.append("")
        
        if validation_result['warnings']:
            report_lines.extend([
                "WARNINGS:",
                "-" * 20
            ])
            for warning in validation_result['warnings']:
                report_lines.append(f"  • {warning}")
            report_lines.append("")
        
        if validation_result['column_mappings']:
            report_lines.extend([
                "COLUMN MAPPINGS:",
                "-" * 20
            ])
            for col_name, mapping in validation_result['column_mappings'].items():
                report_lines.append(
                    f"  {col_name}: {mapping['source_type']} -> {mapping['target_type']} "
                    f"(mapped: {mapping['mapped_type']})"
                )
            report_lines.append("")
        
        if validation_result['type_mismatches']:
            report_lines.extend([
                "TYPE MISMATCHES:",
                "-" * 20
            ])
            for mismatch in validation_result['type_mismatches']:
                report_lines.append(
                    f"  {mismatch['column']}: Expected {mismatch['expected_type']}, "
                    f"Found {mismatch['target_type']}"
                )
            report_lines.append("")
        
        return "\n".join(report_lines)
    
    def validate_cross_database_assumptions(self, source_config: ConnectionConfig, 
                                          target_config: ConnectionConfig,
                                          table_names: List[str]) -> Dict[str, Any]:
        """Validate assumptions for cross-database replication scenarios."""
        validation_summary = {
            'overall_compatible': True,
            'table_results': {},
            'unsupported_mappings': [],
            'recommendations': []
        }
        
        # Check if the engine mapping is supported
        if not self.data_type_mapper.is_mapping_supported(source_config.engine_type, target_config.engine_type):
            mapping_key = self.data_type_mapper.get_mapping_key(source_config.engine_type, target_config.engine_type)
            validation_summary['unsupported_mappings'].append(mapping_key)
            validation_summary['overall_compatible'] = False
            validation_summary['recommendations'].append(
                f"Mapping {source_config.engine_type} -> {target_config.engine_type} is not fully supported. "
                "Manual schema verification is recommended."
            )
        
        # Add general recommendations for cross-database replication
        validation_summary['recommendations'].extend([
            "Ensure target database schema and tables are created before running replication",
            "Verify that target table indexes and constraints are properly configured",
            "Test with a small subset of data before full migration",
            "Monitor data type conversions for potential data loss"
        ])
        
        if self.data_type_mapper.is_cross_database_replication(source_config.engine_type, target_config.engine_type):
            validation_summary['recommendations'].append(
                "Cross-database replication detected. Schema migration should be completed before data replication."
            )
        
        logger.info(
            f"Cross-database validation summary: {source_config.engine_type} -> {target_config.engine_type}, "
            f"Compatible: {validation_summary['overall_compatible']}"
        )
        
        return validation_summary


class FullLoadDataMigrator:
    """Handles full-load data migration operations."""
    
    def __init__(self, spark_session: SparkSession, connection_manager: JdbcConnectionManager):
        self.spark = spark_session
        self.connection_manager = connection_manager
        self.structured_logger = StructuredLogger("FullLoadDataMigrator")
    
    def perform_full_load_migration(self, source_config: ConnectionConfig, 
                                  target_config: ConnectionConfig, table_name: str) -> FullLoadProgress:
        """Perform full-load migration for a table."""
        progress = FullLoadProgress(table_name=table_name)
        progress.start_time = time.time()
        progress.status = 'in_progress'
        
        try:
            # Read all data from source
            source_df = self.connection_manager.read_table_data(source_config, table_name)
            progress.total_rows = source_df.count()
            
            # Write to target
            self.connection_manager.write_table_data(source_df, target_config, table_name, mode='overwrite')
            
            progress.processed_rows = progress.total_rows
            progress.end_time = time.time()
            progress.status = 'completed'
            
            self.structured_logger.info(f"Full load completed for {table_name}", 
                                      rows_processed=progress.processed_rows,
                                      duration=progress.duration_seconds)
            
        except Exception as e:
            progress.end_time = time.time()
            progress.status = 'failed'
            progress.error_message = str(e)
            self.structured_logger.error(f"Full load failed for {table_name}", error=str(e))
        
        return progress


class JobBookmarkManager:
    """
    Manages job bookmarks for incremental loading with S3 persistent storage.
    
    This enhanced implementation supports both S3-based persistent bookmark storage
    and fallback to in-memory bookmarks when S3 is unavailable. It automatically
    detects the S3 bucket from JDBC driver paths and initializes S3BookmarkStorage
    for persistent state management across job executions.
    """
    
    def __init__(self, glue_context: GlueContext, job_name: str, job: Job = None, 
                 source_jdbc_path: Optional[str] = None, target_jdbc_path: Optional[str] = None):
        """
        Initialize JobBookmarkManager with S3 persistent storage support.
        
        Args:
            glue_context: AWS Glue context
            job_name: Name of the Glue job
            job: Glue job instance (optional)
            source_jdbc_path: S3 path to source JDBC driver (optional)
            target_jdbc_path: S3 path to target JDBC driver (optional)
        """
        self.glue_context = glue_context
        self.job_name = job_name
        self.job = job
        self.bookmark_states = {}  # In-memory bookmark storage (fallback)
        self.structured_logger = StructuredLogger(job_name)
        
        # Initialize S3 bookmark storage if JDBC paths are provided
        self.s3_bookmark_storage = None
        self.s3_enabled = False
        
        if source_jdbc_path or target_jdbc_path:
            try:
                # Implement S3 bucket detection logic using JDBC driver paths
                bucket_name = self._extract_s3_bucket_from_jdbc_paths(source_jdbc_path, target_jdbc_path)
                
                if bucket_name:
                    # Validate S3 bucket accessibility before initializing storage (Requirement 6.2)
                    bucket_accessible = S3PathUtilities.validate_s3_bucket_accessibility(
                        bucket_name, 
                        structured_logger=self.structured_logger,
                        metrics_publisher=getattr(self, 'metrics_publisher', None)
                    )
                    
                    if bucket_accessible:
                        # Initialize S3BookmarkStorage instance with detected bucket configuration
                        s3_config = S3BookmarkConfig(
                            bucket_name=bucket_name,
                            bookmark_prefix="bookmarks/",
                            job_name=job_name,
                            retry_attempts=3,
                            timeout_seconds=30
                        )
                        
                        self.s3_bookmark_storage = S3BookmarkStorage(s3_config)
                        self.s3_enabled = True
                        
                        self.structured_logger.info("S3 bookmark storage initialized successfully",
                                                  bucket=bucket_name,
                                                  source_jdbc_path=source_jdbc_path,
                                                  target_jdbc_path=target_jdbc_path)
                    else:
                        # Bucket not accessible - fall back to in-memory bookmarks
                        self.structured_logger.log_fallback_to_memory(
                            "initialization", "S3 bucket not accessible", "bucket_validation_failed")
                        
                        if hasattr(self, 'metrics_publisher'):
                            self.metrics_publisher.publish_bookmark_fallback_metrics(
                                "initialization", "bucket_validation_failed", "bucket_validation_failed")
                        
                        self.s3_bookmark_storage = None
                        self.s3_enabled = False
                else:
                    self.structured_logger.warning("Could not extract S3 bucket from JDBC paths, using in-memory bookmarks",
                                                 source_jdbc_path=source_jdbc_path,
                                                 target_jdbc_path=target_jdbc_path)
                    
            except Exception as e:
                # Add fallback initialization for in-memory bookmarks when S3 is unavailable
                self.structured_logger.error("Failed to initialize S3 bookmark storage, falling back to in-memory bookmarks",
                                           error=str(e),
                                           source_jdbc_path=source_jdbc_path,
                                           target_jdbc_path=target_jdbc_path)
                self.s3_bookmark_storage = None
                self.s3_enabled = False
        else:
            self.structured_logger.info("No JDBC S3 paths provided, using in-memory bookmark storage")
    
    def _extract_s3_bucket_from_jdbc_paths(self, source_jdbc_path: Optional[str], 
                                         target_jdbc_path: Optional[str]) -> Optional[str]:
        """
        Extract S3 bucket name from JDBC driver paths with enhanced logging and metrics.
        
        Args:
            source_jdbc_path: S3 path to source JDBC driver
            target_jdbc_path: S3 path to target JDBC driver
            
        Returns:
            S3 bucket name or None if extraction fails
        """
        start_time = time.time()
        
        try:
            # Use enhanced S3PathUtilities with structured logging (Requirement 6.2)
            bucket_name = S3PathUtilities.detect_s3_bucket_from_jdbc_paths(
                source_jdbc_path or "", 
                target_jdbc_path or "", 
                self.structured_logger
            )
            
            # Calculate detection duration for performance logging
            duration_ms = (time.time() - start_time) * 1000
            
            # Publish bucket detection success metrics (Requirements 6.4, 6.5)
            if hasattr(self, 'metrics_publisher'):
                source_bucket = None
                target_bucket = None
                
                try:
                    if source_jdbc_path:
                        source_bucket = S3PathUtilities.extract_s3_bucket_name(source_jdbc_path)
                except ValueError:
                    pass
                
                try:
                    if target_jdbc_path:
                        target_bucket = S3PathUtilities.extract_s3_bucket_name(target_jdbc_path)
                except ValueError:
                    pass
                
                self.metrics_publisher.publish_s3_bucket_detection_metrics(
                    True, duration_ms, source_bucket, target_bucket, bucket_name)
            
            return bucket_name
            
        except ValueError as e:
            # Calculate detection duration for error logging
            duration_ms = (time.time() - start_time) * 1000
            
            self.structured_logger.error("S3 bucket detection failed",
                                       error=str(e),
                                       source_path=source_jdbc_path,
                                       target_path=target_jdbc_path,
                                       duration_ms=duration_ms)
            
            # Publish bucket detection failure metrics (Requirements 6.4, 6.5)
            if hasattr(self, 'metrics_publisher'):
                self.metrics_publisher.publish_s3_bucket_detection_metrics(
                    False, duration_ms)
            
            return None
            
        except Exception as e:
            # Calculate detection duration for error logging
            duration_ms = (time.time() - start_time) * 1000
            
            self.structured_logger.error("Unexpected error during S3 bucket detection",
                                       error=str(e),
                                       source_path=source_jdbc_path,
                                       target_path=target_jdbc_path,
                                       duration_ms=duration_ms)
            
            # Publish bucket detection failure metrics (Requirements 6.4, 6.5)
            if hasattr(self, 'metrics_publisher'):
                self.metrics_publisher.publish_s3_bucket_detection_metrics(
                    False, duration_ms)
            
            return None
    
    def initialize_bookmark_state(self, table_name: str, incremental_strategy: str,
                                incremental_column: Optional[str] = None) -> JobBookmarkState:
        """
        Initialize job bookmark state for a table with S3 integration.
        
        This method implements the enhanced bookmark initialization that:
        - Attempts to read existing bookmark state from S3 first
        - Handles first-run detection based on S3 bookmark existence
        - Falls back to in-memory bookmarks when S3 operations fail
        - Ensures backward compatibility with existing job configurations
        
        Args:
            table_name: Name of the table to initialize bookmark for
            incremental_strategy: Strategy for incremental loading (timestamp, primary_key, hash)
            incremental_column: Column to use for incremental loading (optional)
            
        Returns:
            JobBookmarkState instance with initialized state
        """
        try:
            # Check if we have a cached state from previous processing in this job
            if table_name in self.bookmark_states:
                state = self.bookmark_states[table_name]
                self.structured_logger.info("Using cached bookmark state from current job execution",
                                          table_name=table_name,
                                          is_first_run=state.is_first_run,
                                          last_processed_value=state.last_processed_value)
                return state
            
            # Attempt to read existing bookmark state from S3 first (Requirement 1.2)
            if self.s3_enabled and self.s3_bookmark_storage:
                try:
                    self.structured_logger.info("Attempting to read bookmark state from S3",
                                              table_name=table_name,
                                              s3_enabled=True)
                    
                    # Use asyncio to read bookmark from S3
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                    try:
                        s3_bookmark_data = loop.run_until_complete(
                            self.s3_bookmark_storage.read_bookmark(table_name)
                        )
                    finally:
                        loop.close()
                    
                    if s3_bookmark_data:
                        # Successfully read bookmark from S3 - perform incremental loading (Requirement 1.4)
                        state = JobBookmarkState.from_s3_dict(s3_bookmark_data)
                        
                        # Ensure job_name is set correctly for this execution
                        state.job_name = self.job_name
                        
                        self.structured_logger.info("Successfully loaded bookmark state from S3",
                                                  table_name=table_name,
                                                  is_first_run=state.is_first_run,
                                                  last_processed_value=state.last_processed_value,
                                                  last_update_timestamp=state.last_update_timestamp,
                                                  incremental_strategy=state.incremental_strategy)
                        
                        # Cache the state for this job execution
                        self.bookmark_states[table_name] = state
                        return state
                    else:
                        # No bookmark state exists in S3 - first run detection (Requirement 1.3)
                        self.structured_logger.info("No bookmark state found in S3, performing first run",
                                                  table_name=table_name,
                                                  s3_enabled=True)
                        
                except Exception as s3_error:
                    # Handle S3 read failures with centralized error handling and fallback (Requirement 1.3, 4.3)
                    self._handle_s3_error("read", table_name, s3_error)
                    
                    self.structured_logger.warning("S3 bookmark read failed, falling back to in-memory bookmarks",
                                                  table_name=table_name,
                                                  error=str(s3_error),
                                                  error_type=type(s3_error).__name__,
                                                  fallback_action="creating_new_bookmark_state_for_full_load")
                    
                    # Continue with in-memory bookmark creation below
            else:
                # S3 not enabled - use in-memory bookmarks (backward compatibility - Requirement 4.1)
                self.structured_logger.info("S3 bookmark storage not enabled, using in-memory bookmarks",
                                          table_name=table_name,
                                          s3_enabled=False)
            
            # Create new bookmark state for first run or S3 fallback
            # Set created_timestamp for S3 compatibility
            current_time = datetime.now(timezone.utc)
            
            state = JobBookmarkState(
                table_name=table_name,
                incremental_strategy=incremental_strategy,
                incremental_column=incremental_column,
                is_first_run=True,  # First run - perform full load
                job_name=self.job_name,
                created_timestamp=current_time,
                updated_timestamp=current_time,
                version="1.0"
            )
            
            self.structured_logger.info("Created new bookmark state for first run",
                                      table_name=table_name,
                                      incremental_strategy=incremental_strategy,
                                      incremental_column=incremental_column,
                                      is_first_run=True)
            
            # Cache the state for this job execution
            self.bookmark_states[table_name] = state
            return state
            
        except Exception as e:
            # Handle any unexpected errors with fallback to basic bookmark state (Requirement 4.4)
            self.structured_logger.error("Unexpected error during bookmark initialization, creating fallback state",
                                       table_name=table_name,
                                       error=str(e),
                                       error_type=type(e).__name__)
            
            # Create fallback state to ensure job continues
            current_time = datetime.now(timezone.utc)
            state = JobBookmarkState(
                table_name=table_name,
                incremental_strategy=incremental_strategy,
                incremental_column=incremental_column,
                is_first_run=True,
                job_name=self.job_name,
                created_timestamp=current_time,
                updated_timestamp=current_time,
                version="1.0"
            )
            
            # Cache the fallback state
            self.bookmark_states[table_name] = state
            return state
    
    def update_bookmark_state(self, table_name: str, new_max_value: Any,
                            processed_rows: int = 0) -> None:
        """
        Update job bookmark state after successful processing with S3 persistence.
        
        This enhanced method implements:
        - S3 bookmark persistence after processing
        - Asynchronous S3 write operations to avoid blocking job execution
        - Error handling for S3 write failures with appropriate logging
        - In-memory bookmark state as backup when S3 operations fail
        
        Args:
            table_name: Name of the table to update bookmark for
            new_max_value: New maximum value processed
            processed_rows: Number of rows processed (default: 0)
            
        Requirements: 1.1, 5.2, 7.4
        """
        if table_name not in self.bookmark_states:
            raise ValueError(f"No bookmark state found for table {table_name}")
        
        # Update in-memory bookmark state first (maintain as backup)
        state = self.bookmark_states[table_name]
        state.last_processed_value = new_max_value
        state.last_update_timestamp = datetime.now(timezone.utc)
        state.is_first_run = False
        
        # Update S3-specific metadata
        state.updated_timestamp = datetime.now(timezone.utc)
        if not state.created_timestamp:
            state.created_timestamp = state.updated_timestamp
        
        self.structured_logger.info("Updated in-memory bookmark state",
                                  table_name=table_name,
                                  last_value=str(new_max_value),
                                  processed_rows=processed_rows,
                                  is_first_run=False)
        
        # Attempt to write bookmark data to S3 after processing (Requirement 1.1)
        if self.s3_enabled and self.s3_bookmark_storage:
            try:
                # Prepare S3-compatible bookmark data
                s3_bookmark_data = state.to_s3_dict()
                
                # Implement asynchronous S3 write operations to avoid blocking job execution (Requirement 7.4)
                self.structured_logger.debug("Starting asynchronous S3 bookmark write",
                                           table_name=table_name,
                                           s3_enabled=True)
                
                # Use asyncio to write bookmark to S3 asynchronously
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                try:
                    # Execute S3 write operation asynchronously
                    write_success = loop.run_until_complete(
                        self.s3_bookmark_storage.write_bookmark(table_name, s3_bookmark_data)
                    )
                    
                    if write_success:
                        self.structured_logger.info("Successfully wrote bookmark to S3",
                                                   table_name=table_name,
                                                   last_value=str(new_max_value),
                                                   processed_rows=processed_rows,
                                                   s3_key=self.s3_bookmark_storage._get_bookmark_s3_key(table_name))
                        
                        # Publish CloudWatch metric for successful S3 write
                        self._publish_bookmark_metric("BookmarkS3WriteSuccess", table_name)
                        
                    else:
                        # Add error handling for S3 write failures with appropriate logging (Requirement 5.2)
                        self.structured_logger.warning("S3 bookmark write failed, but in-memory state is maintained",
                                                      table_name=table_name,
                                                      fallback_available=True)
                        
                        # Publish CloudWatch metric for failed S3 write
                        self._publish_bookmark_metric("BookmarkS3WriteFailure", table_name)
                        
                finally:
                    loop.close()
                    
            except Exception as e:
                # Handle S3 write failures with centralized error handling (Requirement 5.2)
                self._handle_s3_error("write", table_name, e)
                
                self.structured_logger.error("Unexpected error during S3 bookmark write operation",
                                           table_name=table_name,
                                           error=str(e),
                                           error_type=type(e).__name__,
                                           fallback_available=True,
                                           job_continues=True)
                
                # Publish CloudWatch metric for failed S3 write
                self._publish_bookmark_metric("BookmarkS3WriteFailure", table_name)
                
                # Maintain in-memory bookmark state as backup when S3 operations fail (Requirement 5.2)
                # Don't raise exception for S3 failures - continue processing
                
        else:
            # S3 not enabled, using in-memory bookmarks only
            self.structured_logger.debug("S3 bookmark storage not enabled, using in-memory bookmarks only",
                                       table_name=table_name,
                                       s3_enabled=self.s3_enabled)
        
        # Log final bookmark state update (always successful for in-memory)
        self.structured_logger.info("Bookmark state update completed",
                                  table_name=table_name,
                                  last_value=str(new_max_value),
                                  processed_rows=processed_rows,
                                  s3_enabled=self.s3_enabled,
                                  in_memory_backup=True)
    
    def _publish_bookmark_metric(self, metric_name: str, table_name: str) -> None:
        """
        Publish CloudWatch custom metrics for bookmark operations.
        
        Args:
            metric_name: Name of the CloudWatch metric
            table_name: Name of the table (used as dimension)
        """
        try:
            if cloudwatch:
                cloudwatch.put_metric_data(
                    Namespace='GlueDataReplication/Bookmarks',
                    MetricData=[
                        {
                            'MetricName': metric_name,
                            'Dimensions': [
                                {
                                    'Name': 'JobName',
                                    'Value': self.job_name
                                },
                                {
                                    'Name': 'TableName',
                                    'Value': table_name
                                }
                            ],
                            'Value': 1.0,
                            'Unit': 'Count',
                            'Timestamp': datetime.now(timezone.utc)
                        }
                    ]
                )
                self.structured_logger.debug("Published CloudWatch metric",
                                           metric_name=metric_name,
                                           table_name=table_name)
        except Exception as e:
            # Don't fail the job for CloudWatch metric failures
            self.structured_logger.warning("Failed to publish CloudWatch metric",
                                         metric_name=metric_name,
                                         table_name=table_name,
                                         error=str(e))
    
    def get_bookmark_state(self, table_name: str) -> Optional[JobBookmarkState]:
        """Get current bookmark state for a table."""
        return self.bookmark_states.get(table_name)
    
    def reset_bookmark_state(self, table_name: str) -> None:
        """Reset bookmark state for a table (force full reload)."""
        try:
            # Update local state to force full reload
            if table_name in self.bookmark_states:
                state = self.bookmark_states[table_name]
                state.last_processed_value = None
                state.last_update_timestamp = None
                state.is_first_run = True
            
            logger.info(f"Reset bookmark state for table {table_name}")
        except Exception as e:
            logger.error(f"Failed to reset bookmark state for {table_name}: {str(e)}")
    
    def get_all_bookmark_states(self) -> Dict[str, JobBookmarkState]:
        """Get all bookmark states."""
        return self.bookmark_states.copy()
    
    def _handle_s3_error(self, operation: str, table_name: str, error: Exception) -> None:
        """
        Handle S3 operation errors with graceful degradation to in-memory bookmarks.
        
        This method implements centralized error handling for S3 operations at the
        JobBookmarkManager level and provides graceful degradation when S3 operations fail.
        
        Args:
            operation: Type of S3 operation (read, write, delete)
            table_name: Name of the table being processed
            error: Exception that occurred during S3 operation
        """
        # Log the error with appropriate context
        self.structured_logger.error(f"S3 bookmark {operation} operation failed",
                                   operation=operation,
                                   table_name=table_name,
                                   error_type=type(error).__name__,
                                   error_message=str(error),
                                   s3_enabled=self.s3_enabled)
        
        # Check if this is a permanent S3 failure that requires disabling S3
        if isinstance(error, ClientError):
            error_code = error.response.get('Error', {}).get('Code', 'Unknown')
            
            # Permanent errors that should disable S3 for this job execution
            permanent_errors = ['AccessDenied', 'NoSuchBucket', 'InvalidBucketName']
            
            if error_code in permanent_errors and self.s3_enabled:
                self.structured_logger.warning("Disabling S3 bookmark storage due to permanent error",
                                             error_code=error_code,
                                             table_name=table_name,
                                             fallback_action="switching_to_in_memory_bookmarks_for_remaining_tables")
                
                # Disable S3 for the remainder of this job execution
                self.s3_enabled = False
                self.s3_bookmark_storage = None
                
                # Publish metric for S3 fallback
                self._publish_bookmark_metric("BookmarkFallbackToMemory", table_name)
        
        elif isinstance(error, (BotoCoreError, NoCredentialsError)):
            # Credential/boto errors - disable S3 for this job execution
            if self.s3_enabled:
                self.structured_logger.warning("Disabling S3 bookmark storage due to credential/boto error",
                                             error_type=type(error).__name__,
                                             table_name=table_name,
                                             fallback_action="switching_to_in_memory_bookmarks_for_remaining_tables")
                
                self.s3_enabled = False
                self.s3_bookmark_storage = None
                
                # Publish metric for S3 fallback
                self._publish_bookmark_metric("BookmarkFallbackToMemory", table_name)
        
        # For other errors (network, JSON corruption, etc.), keep S3 enabled but log the issue
        # Individual operations will handle retries and fallbacks appropriately
    
    def _detect_and_handle_corrupted_bookmark(self, table_name: str, bookmark_data: Dict[str, Any]) -> bool:
        """
        Detect and handle corrupted bookmark JSON files.
        
        This method validates bookmark data structure and handles corruption by
        deleting the corrupted file and triggering a full load fallback.
        
        Args:
            table_name: Name of the table being processed
            bookmark_data: Dictionary containing bookmark data from S3
            
        Returns:
            True if bookmark data is valid, False if corrupted (and handled)
        """
        try:
            # Use the existing validation from JobBookmarkState
            if not JobBookmarkState._validate_s3_data(bookmark_data):
                self.structured_logger.error("Bookmark data validation failed - corrupted data detected",
                                           table_name=table_name,
                                           bookmark_keys=list(bookmark_data.keys()) if isinstance(bookmark_data, dict) else "invalid_data",
                                           cleanup_action="deleting_corrupted_file")
                
                # Attempt to delete the corrupted file
                if self.s3_enabled and self.s3_bookmark_storage:
                    try:
                        loop = asyncio.new_event_loop()
                        asyncio.set_event_loop(loop)
                        try:
                            delete_success = loop.run_until_complete(
                                self.s3_bookmark_storage.delete_bookmark(table_name)
                            )
                            
                            if delete_success:
                                self.structured_logger.info("Successfully deleted corrupted bookmark file",
                                                           table_name=table_name,
                                                           fallback_action="full_load_will_be_performed")
                            else:
                                self.structured_logger.warning("Failed to delete corrupted bookmark file",
                                                             table_name=table_name,
                                                             fallback_action="full_load_will_still_be_performed")
                        finally:
                            loop.close()
                            
                    except Exception as delete_error:
                        self.structured_logger.error("Error during corrupted file cleanup",
                                                   table_name=table_name,
                                                   delete_error=str(delete_error),
                                                   fallback_action="full_load_will_still_be_performed")
                
                # Publish metric for corrupted bookmark detection
                self._publish_bookmark_metric("BookmarkCorruptionDetected", table_name)
                
                return False
            
            return True
            
        except Exception as e:
            self.structured_logger.error("Error during bookmark corruption detection",
                                       table_name=table_name,
                                       error=str(e),
                                       fallback_action="treating_as_corrupted_and_performing_full_load")
            
            # Treat validation errors as corruption
            self._publish_bookmark_metric("BookmarkCorruptionDetected", table_name)
            return False
    
    def initialize_bookmark_states_parallel(self, table_configs: List[Dict[str, Any]]) -> Dict[str, JobBookmarkState]:
        """
        Initialize bookmark states for multiple tables in parallel.
        
        This method implements parallel bookmark reading during job initialization
        to optimize S3 operations for jobs processing many tables simultaneously.
        
        Args:
            table_configs: List of dictionaries containing table configuration:
                          [{'name': str, 'strategy': str, 'column': Optional[str]}, ...]
            
        Returns:
            Dictionary mapping table names to initialized JobBookmarkState objects
        """
        if not table_configs:
            return {}
        
        start_time = time.time()
        table_names = [config['name'] for config in table_configs]
        
        self.structured_logger.info("Starting parallel bookmark state initialization",
                                  table_count=len(table_names),
                                  s3_enabled=self.s3_enabled,
                                  table_names=table_names)
        
        # Initialize results dictionary
        bookmark_states = {}
        
        # If S3 is enabled, attempt parallel reading
        if self.s3_enabled and self.s3_bookmark_storage:
            try:
                # Use asyncio to read bookmarks in parallel
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                try:
                    # Read all bookmarks in parallel (Requirement 7.1)
                    s3_results = loop.run_until_complete(
                        self.s3_bookmark_storage.read_bookmarks_parallel(table_names)
                    )
                    
                    # Process results and create bookmark states
                    for config in table_configs:
                        table_name = config['name']
                        strategy = config['strategy']
                        column = config.get('column')
                        
                        s3_data = s3_results.get(table_name)
                        
                        if s3_data:
                            # Successfully read from S3 - create state from S3 data
                            try:
                                state = JobBookmarkState.from_s3_dict(s3_data)
                                state.job_name = self.job_name  # Ensure job name is current
                                bookmark_states[table_name] = state
                                
                                self.structured_logger.debug("Parallel bookmark initialization from S3",
                                                           table_name=table_name,
                                                           is_first_run=state.is_first_run,
                                                           last_processed_value=state.last_processed_value)
                                
                            except Exception as parse_error:
                                # Failed to parse S3 data - create new state
                                self.structured_logger.warning("Failed to parse S3 bookmark data, creating new state",
                                                             table_name=table_name,
                                                             error=str(parse_error))
                                bookmark_states[table_name] = self._create_new_bookmark_state(
                                    table_name, strategy, column)
                        else:
                            # No S3 data found - create new state for first run
                            bookmark_states[table_name] = self._create_new_bookmark_state(
                                table_name, strategy, column)
                            
                            self.structured_logger.debug("Parallel bookmark initialization - new state created",
                                                       table_name=table_name,
                                                       is_first_run=True)
                    
                finally:
                    loop.close()
                    
            except Exception as s3_error:
                # S3 parallel read failed - fall back to individual initialization
                self.structured_logger.warning("Parallel S3 bookmark read failed, falling back to individual initialization",
                                             error=str(s3_error),
                                             error_type=type(s3_error).__name__,
                                             table_count=len(table_names))
                
                # Create new states for all tables
                for config in table_configs:
                    table_name = config['name']
                    strategy = config['strategy']
                    column = config.get('column')
                    bookmark_states[table_name] = self._create_new_bookmark_state(table_name, strategy, column)
        else:
            # S3 not enabled - create new states for all tables
            self.structured_logger.info("S3 not enabled, creating new bookmark states for all tables",
                                      table_count=len(table_names))
            
            for config in table_configs:
                table_name = config['name']
                strategy = config['strategy']
                column = config.get('column')
                bookmark_states[table_name] = self._create_new_bookmark_state(table_name, strategy, column)
        
        # Cache all states in memory
        self.bookmark_states.update(bookmark_states)
        
        # Calculate and log performance metrics
        total_duration_ms = (time.time() - start_time) * 1000
        successful_count = len(bookmark_states)
        
        self.structured_logger.info("Completed parallel bookmark state initialization",
                                  table_count=len(table_names),
                                  successful_count=successful_count,
                                  total_duration_ms=total_duration_ms,
                                  s3_enabled=self.s3_enabled,
                                  average_duration_per_table_ms=total_duration_ms / len(table_names) if table_names else 0)
        
        return bookmark_states
    
    def update_bookmark_states_batch(self, bookmark_updates: Dict[str, Any], 
                                   processed_rows_map: Dict[str, int] = None) -> Dict[str, bool]:
        """
        Update bookmark states for multiple tables in batch.
        
        This method implements batch S3 write operations for multiple table bookmarks
        to optimize S3 operations and avoid blocking data operations.
        
        Args:
            bookmark_updates: Dictionary mapping table names to new max values
            processed_rows_map: Dictionary mapping table names to processed row counts (optional)
            
        Returns:
            Dictionary mapping table names to update success status (True/False)
        """
        if not bookmark_updates:
            return {}
        
        start_time = time.time()
        table_names = list(bookmark_updates.keys())
        
        self.structured_logger.info("Starting batch bookmark state updates",
                                  table_count=len(table_names),
                                  s3_enabled=self.s3_enabled,
                                  table_names=table_names)
        
        # Update in-memory states first
        s3_bookmark_batch = {}
        update_results = {}
        
        for table_name, new_max_value in bookmark_updates.items():
            processed_rows = processed_rows_map.get(table_name, 0) if processed_rows_map else 0
            
            try:
                # Update in-memory state
                if table_name not in self.bookmark_states:
                    self.structured_logger.warning("No bookmark state found for batch update",
                                                 table_name=table_name)
                    update_results[table_name] = False
                    continue
                
                state = self.bookmark_states[table_name]
                state.last_processed_value = new_max_value
                state.last_update_timestamp = datetime.now(timezone.utc)
                state.is_first_run = False
                state.updated_timestamp = datetime.now(timezone.utc)
                
                if not state.created_timestamp:
                    state.created_timestamp = state.updated_timestamp
                
                # Prepare S3 data if S3 is enabled
                if self.s3_enabled and self.s3_bookmark_storage:
                    s3_bookmark_batch[table_name] = state.to_s3_dict()
                
                self.structured_logger.debug("Updated in-memory bookmark state for batch",
                                           table_name=table_name,
                                           last_value=str(new_max_value),
                                           processed_rows=processed_rows)
                
                update_results[table_name] = True
                
            except Exception as e:
                self.structured_logger.error("Failed to update in-memory bookmark state for batch",
                                           table_name=table_name,
                                           error=str(e))
                update_results[table_name] = False
        
        # Perform batch S3 writes if S3 is enabled and we have data to write
        if self.s3_enabled and self.s3_bookmark_storage and s3_bookmark_batch:
            try:
                # Use asyncio for batch S3 operations (Requirement 7.2)
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                try:
                    s3_results = loop.run_until_complete(
                        self.s3_bookmark_storage.write_bookmarks_batch(s3_bookmark_batch)
                    )
                    
                    # Update results based on S3 write success
                    for table_name, s3_success in s3_results.items():
                        if not s3_success and update_results.get(table_name, False):
                            # S3 write failed but in-memory update succeeded
                            self.structured_logger.warning("S3 write failed in batch but in-memory state maintained",
                                                          table_name=table_name)
                            # Keep update_results[table_name] as True since in-memory state is updated
                        
                        # Publish individual metrics
                        metric_name = "BookmarkS3WriteSuccess" if s3_success else "BookmarkS3WriteFailure"
                        self._publish_bookmark_metric(metric_name, table_name)
                    
                finally:
                    loop.close()
                    
            except Exception as s3_error:
                # Batch S3 operation failed - log but don't fail the updates
                self.structured_logger.error("Batch S3 bookmark write operation failed",
                                           error=str(s3_error),
                                           error_type=type(s3_error).__name__,
                                           table_count=len(s3_bookmark_batch),
                                           in_memory_states_maintained=True)
                
                # Publish failure metrics for all tables
                for table_name in s3_bookmark_batch.keys():
                    self._publish_bookmark_metric("BookmarkS3WriteFailure", table_name)
        
        # Calculate and log performance metrics
        total_duration_ms = (time.time() - start_time) * 1000
        successful_count = sum(1 for success in update_results.values() if success)
        failed_count = len(update_results) - successful_count
        
        self.structured_logger.info("Completed batch bookmark state updates",
                                  table_count=len(table_names),
                                  successful_count=successful_count,
                                  failed_count=failed_count,
                                  total_duration_ms=total_duration_ms,
                                  s3_enabled=self.s3_enabled,
                                  average_duration_per_table_ms=total_duration_ms / len(table_names) if table_names else 0)
        
        return update_results
    
    def _create_new_bookmark_state(self, table_name: str, incremental_strategy: str, 
                                 incremental_column: Optional[str] = None) -> JobBookmarkState:
        """
        Create a new bookmark state for first run.
        
        Args:
            table_name: Name of the table
            incremental_strategy: Strategy for incremental loading
            incremental_column: Column to use for incremental loading (optional)
            
        Returns:
            New JobBookmarkState instance
        """
        current_time = datetime.now(timezone.utc)
        
        return JobBookmarkState(
            table_name=table_name,
            incremental_strategy=incremental_strategy,
            incremental_column=incremental_column,
            is_first_run=True,
            job_name=self.job_name,
            created_timestamp=current_time,
            updated_timestamp=current_time,
            version="1.0"
        )


class IncrementalDataMigrator:
    """Handles incremental data migration operations."""
    
    def __init__(self, spark_session: SparkSession, connection_manager: JdbcConnectionManager, 
                 bookmark_manager: JobBookmarkManager):
        self.spark = spark_session
        self.connection_manager = connection_manager
        self.bookmark_manager = bookmark_manager
        self.structured_logger = StructuredLogger("IncrementalDataMigrator")
    
    def perform_incremental_load_migration(self, source_config: ConnectionConfig, 
                                         target_config: ConnectionConfig, table_name: str) -> IncrementalLoadProgress:
        """Perform incremental migration for a table."""
        # Auto-detect incremental strategy
        schema = self.connection_manager.get_table_schema(source_config, table_name)
        strategy_info = IncrementalColumnDetector.detect_incremental_strategy(schema, table_name)
        
        # Initialize bookmark state with detected strategy
        bookmark_state = self.bookmark_manager.initialize_bookmark_state(
            table_name, strategy_info['strategy'], strategy_info['column']
        )
        
        progress = IncrementalLoadProgress(
            table_name=table_name,
            incremental_strategy=bookmark_state.incremental_strategy,
            incremental_column=bookmark_state.incremental_column
        )
        progress.start_time = time.time()
        progress.status = 'in_progress'
        
        try:
            # Build incremental query
            if bookmark_state.last_processed_value:
                query = f"SELECT * FROM {source_config.schema}.{table_name} WHERE {bookmark_state.incremental_column} > '{bookmark_state.last_processed_value}'"
            else:
                query = f"SELECT * FROM {source_config.schema}.{table_name}"
            
            # Read incremental data
            source_df = self.connection_manager.read_table_data(source_config, table_name, query=query)
            progress.delta_rows = source_df.count()
            
            if progress.delta_rows > 0:
                # Write to target
                self.connection_manager.write_table_data(source_df, target_config, table_name, mode='append')
                
                # Update bookmark
                new_max_value = source_df.agg({bookmark_state.incremental_column: "max"}).collect()[0][f"max({bookmark_state.incremental_column})"]
                self.bookmark_manager.update_bookmark_state(table_name, new_max_value, progress.delta_rows)
            
            progress.processed_rows = progress.delta_rows
            progress.end_time = time.time()
            progress.status = 'completed'
            
            self.structured_logger.info(f"Incremental load completed for {table_name}", 
                                      rows_processed=progress.processed_rows,
                                      duration=progress.duration_seconds)
            
        except Exception as e:
            progress.end_time = time.time()
            progress.status = 'failed'
            progress.error_message = str(e)
            self.structured_logger.error(f"Incremental load failed for {table_name}", error=str(e))
        
        return progress





class IncrementalColumnDetector:
    """Detects suitable columns for incremental loading strategies."""
    
    # Common timestamp column names
    TIMESTAMP_COLUMN_NAMES = [
        'updated_at', 'modified_at', 'last_modified', 'last_updated',
        'created_at', 'insert_date', 'update_date', 'modified_date',
        'timestamp', 'last_change', 'change_date', 'mod_time'
    ]
    
    # Common primary key column names
    PRIMARY_KEY_COLUMN_NAMES = [
        'id', 'pk', 'primary_key', 'key', 'seq', 'sequence',
        'row_id', 'record_id', 'unique_id'
    ]
    
    @classmethod
    def detect_incremental_strategy(cls, schema: StructType, table_name: str) -> Dict[str, Any]:
        """Detect the best incremental loading strategy for a table."""
        column_names = [field.name.lower() for field in schema.fields]
        column_types = {field.name.lower(): field.dataType for field in schema.fields}
        
        strategy_info = {
            'strategy': 'hash',  # Default fallback
            'column': None,
            'confidence': 0.0,
            'reason': 'No suitable incremental column found, using hash-based strategy'
        }
        
        # Strategy 1: Look for timestamp columns
        timestamp_candidates = []
        for col_name in column_names:
            col_type = column_types[col_name]
            
            # Check if it's a timestamp/date type
            if isinstance(col_type, (TimestampType, DateType)):
                confidence = 0.5  # Base confidence for timestamp types
                
                # Boost confidence for common timestamp column names
                for pattern in cls.TIMESTAMP_COLUMN_NAMES:
                    if pattern in col_name:
                        confidence += 0.3
                        break
                
                # Prefer 'updated_at' or 'modified_at' patterns
                if any(pattern in col_name for pattern in ['updated', 'modified', 'last_modified']):
                    confidence += 0.2
                
                timestamp_candidates.append((col_name, confidence))
        
        # Select best timestamp candidate
        if timestamp_candidates:
            best_timestamp = max(timestamp_candidates, key=lambda x: x[1])
            if best_timestamp[1] > strategy_info['confidence']:
                strategy_info = {
                    'strategy': 'timestamp',
                    'column': best_timestamp[0],
                    'confidence': best_timestamp[1],
                    'reason': f'Found timestamp column: {best_timestamp[0]}'
                }
        
        # Strategy 2: Look for auto-incrementing primary key columns
        pk_candidates = []
        for col_name in column_names:
            col_type = column_types[col_name]
            
            # Check if it's an integer type (potential auto-increment)
            if isinstance(col_type, (IntegerType, LongType)):
                confidence = 0.3  # Base confidence for integer types
                
                # Boost confidence for common primary key column names
                for pattern in cls.PRIMARY_KEY_COLUMN_NAMES:
                    if pattern == col_name or col_name.endswith('_' + pattern):
                        confidence += 0.4
                        break
                
                # Special boost for 'id' columns
                if col_name == 'id' or col_name.endswith('_id'):
                    confidence += 0.2
                
                pk_candidates.append((col_name, confidence))
        
        # Select best primary key candidate
        if pk_candidates:
            best_pk = max(pk_candidates, key=lambda x: x[1])
            if best_pk[1] > strategy_info['confidence']:
                strategy_info = {
                    'strategy': 'primary_key',
                    'column': best_pk[0],
                    'confidence': best_pk[1],
                    'reason': f'Found auto-increment primary key column: {best_pk[0]}'
                }
        
        logger.info(
            f"Incremental strategy for table {table_name}: {strategy_info['strategy']} "
            f"(column: {strategy_info['column']}, confidence: {strategy_info['confidence']:.2f}) - "
            f"{strategy_info['reason']}"
        )
        
        return strategy_info
    
    @classmethod
    def validate_incremental_column(cls, connection_manager: JdbcConnectionManager,
                                  connection_config: ConnectionConfig, table_name: str,
                                  column_name: str, strategy: str) -> bool:
        """Validate that the incremental column is suitable for the strategy."""
        try:
            # Build validation query based on strategy
            if strategy == 'timestamp':
                # Check if column has reasonable timestamp values
                validation_query = f"""
                SELECT 
                    COUNT(*) as total_rows,
                    COUNT({column_name}) as non_null_rows,
                    MIN({column_name}) as min_value,
                    MAX({column_name}) as max_value
                FROM {connection_config.schema}.{table_name}
                """
            elif strategy == 'primary_key':
                # Check if column has sequential values
                validation_query = f"""
                SELECT 
                    COUNT(*) as total_rows,
                    COUNT(DISTINCT {column_name}) as unique_rows,
                    MIN({column_name}) as min_value,
                    MAX({column_name}) as max_value
                FROM {connection_config.schema}.{table_name}
                """
            else:
                return True  # Hash strategy doesn't need column validation
            
            df = connection_manager.read_table_data(
                connection_config=connection_config,
                table_name=table_name,
                query=validation_query
            )
            
            result = df.collect()[0]
            total_rows = result['total_rows']
            
            if strategy == 'timestamp':
                non_null_rows = result['non_null_rows']
                if non_null_rows < total_rows * 0.95:  # At least 95% non-null
                    logger.warning(
                        f"Timestamp column {column_name} has too many null values: "
                        f"{non_null_rows}/{total_rows} ({non_null_rows/total_rows*100:.1f}%)"
                    )
                    return False
            
            elif strategy == 'primary_key':
                unique_rows = result['unique_rows']
                if unique_rows != total_rows:  # Must be unique
                    logger.warning(
                        f"Primary key column {column_name} is not unique: "
                        f"{unique_rows}/{total_rows} unique values"
                    )
                    return False
            
            logger.info(f"Incremental column {column_name} validation passed for strategy {strategy}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to validate incremental column {column_name}: {str(e)}")
            return False


def main():
    """Main function for data replication using standard DataFrames"""
    try:
        # Parse job arguments
        args = JobConfigurationParser.parse_job_arguments()
        job_config = JobConfigurationParser.create_job_config(args)
        JobConfigurationParser.validate_configuration(job_config)
        
        # Initialize Spark session, Glue context, and job
        spark, glue_context, job = initialize_spark_session(job_config)
        
        # Load JDBC drivers
        load_jdbc_drivers(spark.sparkContext, job_config)
        
        # Initialize managers
        connection_manager = JdbcConnectionManager(spark, glue_context)
        bookmark_manager = JobBookmarkManager(
            glue_context, 
            job_config.job_name, 
            job,
            source_jdbc_path=job_config.source_connection.jdbc_driver_path,
            target_jdbc_path=job_config.target_connection.jdbc_driver_path
        )
        
        # Initialize migrators for both full and incremental loads
        full_migrator = FullLoadDataMigrator(spark, connection_manager)
        incremental_migrator = IncrementalDataMigrator(spark, connection_manager, bookmark_manager)
        
        successful_tables = 0
        failed_tables = 0
        
        # Process each table
        for table_name in job_config.tables:
            try:
                logger.info(f"Processing table: {table_name}")
                
                # Auto-detect incremental strategy for bookmark initialization
                schema = connection_manager.get_table_schema(job_config.source_connection, table_name)
                strategy_info = IncrementalColumnDetector.detect_incremental_strategy(schema, table_name)
                
                # Check if this is first run (determines full vs incremental)
                bookmark_state = bookmark_manager.initialize_bookmark_state(
                    table_name, strategy_info['strategy'], strategy_info['column']
                )
                
                if bookmark_state.is_first_run:
                    # First run: perform full load
                    logger.info(f"First run detected for {table_name} - performing full load")
                    progress = full_migrator.perform_full_load_migration(
                        job_config.source_connection,
                        job_config.target_connection,
                        table_name
                    )
                    # Update bookmark after successful full load
                    if progress.status == 'completed':
                        try:
                            bookmark_manager.update_bookmark_state(table_name, "full_load_completed", progress.processed_rows)
                        except Exception as bookmark_error:
                            logger.warning(f"Failed to update bookmark for {table_name}: {bookmark_error}. Data processing was successful.")
                else:
                    # Subsequent runs: perform incremental load
                    logger.info(f"Incremental load for {table_name}")
                    progress = incremental_migrator.perform_incremental_load_migration(
                        job_config.source_connection,
                        job_config.target_connection,
                        table_name
                    )
                
                if progress.status == 'completed':
                    successful_tables += 1
                    logger.info(f"Successfully processed table {table_name}: {progress.processed_rows} rows")
                else:
                    failed_tables += 1
                    logger.error(f"Failed to process table {table_name}: {progress.error_message}")
                    
            except Exception as e:
                logger.error(f"Failed to process table {table_name}: {e}")
                failed_tables += 1
                # Continue with next table
        
        logger.info(f"Job completed: {successful_tables} successful, {failed_tables} failed")
        
        # Commit job bookmark
        job.commit()
        
    except Exception as e:
        logger.error(f"Job failed: {str(e)}")
        raise


if __name__ == "__main__":
    main()


