"""
S3 utility classes and functions for AWS Glue Data Replication.

This module provides utilities for S3 path operations, bucket detection,
and enhanced parallel operations integration.
"""

import time
import logging
from typing import Optional
from urllib.parse import urlparse

# Import enhanced parallel operations for Task 11
try:
    import sys
    import os
    # Add the project root to the path to import from scripts
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
    if project_root not in sys.path:
        sys.path.insert(0, project_root)
    from scripts.s3_parallel_operations import EnhancedS3ParallelOperations
except ImportError:
    # Fallback if module not available
    EnhancedS3ParallelOperations = None

logger = logging.getLogger(__name__)


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
    def validate_s3_path_format(s3_path: str) -> bool:
        """
        Validate if a string is a valid S3 path format.
        
        Args:
            s3_path: Path to validate
            
        Returns:
            True if valid S3 path format, False otherwise
        """
        try:
            if not isinstance(s3_path, str):
                return False
            
            if not s3_path.startswith('s3://'):
                return False
            
            # Try to extract bucket name - if it succeeds, path is valid
            S3PathUtilities.extract_s3_bucket_name(s3_path)
            return True
        except Exception:
            return False
    
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
    def detect_s3_bucket_from_jdbc_paths(source_jdbc_path: str, target_jdbc_path: str, 
                                        structured_logger: Optional['StructuredLogger'] = None) -> str:
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
        
        # Try to extract source bucket (skip warning for empty paths - common with Iceberg engines)
        try:
            source_bucket = S3PathUtilities.extract_s3_bucket_name(source_jdbc_path)
        except ValueError as e:
            # Only log warning if the path is not empty (avoid noise for Iceberg engines)
            if source_jdbc_path and source_jdbc_path.strip():
                error_msg = f"Failed to extract bucket from source JDBC path '{source_jdbc_path}': {e}"
                if structured_logger:
                    structured_logger.warning(error_msg, path_type="source", error=str(e))
                else:
                    logger.warning(error_msg)
        
        # Try to extract target bucket (skip warning for empty paths - common with Iceberg engines)
        try:
            target_bucket = S3PathUtilities.extract_s3_bucket_name(target_jdbc_path)
        except ValueError as e:
            # Only log warning if the path is not empty (avoid noise for Iceberg engines)
            if target_jdbc_path and target_jdbc_path.strip():
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
                                       structured_logger: Optional['StructuredLogger'] = None,
                                       metrics_publisher: Optional['CloudWatchMetricsPublisher'] = None) -> bool:
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
        import boto3
        from botocore.exceptions import ClientError
        
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