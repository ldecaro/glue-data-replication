"""
Bookmark management module for AWS Glue Data Replication.

This module provides bookmark management functionality including:
- JobBookmarkManager: Manages job bookmarks for incremental loading with S3 persistent storage
- JobBookmarkState: Represents job bookmark state for a table with S3 compatibility
- FullLoadProgress: Tracks progress of full-load operations
- IncrementalLoadProgress: Tracks progress of incremental-load operations
"""

import time
import asyncio
import threading
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any
from datetime import datetime, timezone, timedelta

from botocore.exceptions import ClientError, NoCredentialsError, BotoCoreError

from .s3_bookmark import S3BookmarkConfig, S3BookmarkStorage
from ..utils.s3_utils import S3PathUtilities


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
                import logging
                logger = logging.getLogger(__name__)
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
                import logging
                logger = logging.getLogger(__name__)
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
                import logging
                logger = logging.getLogger(__name__)
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
        import logging
        logger = logging.getLogger(__name__)
        
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
        
        # Validate incremental_column requirement for certain strategies
        if data['incremental_strategy'] in ['timestamp', 'primary_key']:
            if not data.get('incremental_column'):
                logger.error(f"incremental_column is required for strategy '{data['incremental_strategy']}'")
                return False
        
        # Validate boolean fields
        boolean_fields = ['is_first_run']
        for field in boolean_fields:
            if field in data and data[field] is not None:
                if not isinstance(data[field], bool):
                    logger.error(f"Field '{field}' must be a boolean value")
                    return False
        
        # Validate timestamp fields format (basic check)
        timestamp_fields = ['last_update_timestamp', 'created_timestamp', 'updated_timestamp']
        for field in timestamp_fields:
            if field in data and data[field] is not None:
                if not isinstance(data[field], str):
                    logger.error(f"Timestamp field '{field}' must be a string")
                    return False
                # Basic ISO format check
                if len(data[field]) < 10:  # Minimum for YYYY-MM-DD
                    logger.error(f"Timestamp field '{field}' appears to be too short")
                    return False
        
        return True


class JobBookmarkManager:
    """
    Manages job bookmarks for incremental loading with S3 persistent storage.
    
    This enhanced implementation supports both S3-based persistent bookmark storage
    and fallback to in-memory bookmarks when S3 is unavailable. It automatically
    detects the S3 bucket from JDBC driver paths and initializes S3BookmarkStorage
    for persistent state management across job executions.
    """
    
    def __init__(self, glue_context, job_name: str, job=None, 
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
        
        # Import dependencies that may not be available during module import
        try:
            from ..monitoring.logging import StructuredLogger
            self.structured_logger = StructuredLogger(job_name)
        except ImportError:
            # Fallback for when monitoring modules are not available
            import logging
            self.structured_logger = logging.getLogger(__name__)
        
        # Initialize S3 bookmark storage if JDBC paths are provided
        self.s3_bookmark_storage = None
        self.s3_enabled = False
        self.enhanced_parallel_ops = None  # Enhanced parallel operations for Task 11
        
        if source_jdbc_path or target_jdbc_path:
            try:
                # Implement S3 bucket detection logic using JDBC driver paths
                bucket_name = self._extract_s3_bucket_from_jdbc_paths(source_jdbc_path, target_jdbc_path)
                
                if bucket_name:
                    # Validate S3 bucket accessibility before initializing storage
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
                        
                        # Initialize enhanced parallel operations for Task 11
                        try:
                            from scripts.s3_parallel_operations import EnhancedS3ParallelOperations
                            self.enhanced_parallel_ops = EnhancedS3ParallelOperations(self.s3_bookmark_storage)
                            if hasattr(self.structured_logger, 'info'):
                                self.structured_logger.info("Enhanced parallel S3 operations initialized for Task 11")
                        except ImportError:
                            # Enhanced parallel operations not available
                            pass
                        
                        if hasattr(self.structured_logger, 'info'):
                            self.structured_logger.info("S3 bookmark storage initialized successfully",
                                                      bucket=bucket_name,
                                                      source_jdbc_path=source_jdbc_path,
                                                      target_jdbc_path=target_jdbc_path)
                    else:
                        # Bucket not accessible - fall back to in-memory bookmarks
                        if hasattr(self.structured_logger, 'log_fallback_to_memory'):
                            self.structured_logger.log_fallback_to_memory(
                                "initialization", "S3 bucket not accessible", "bucket_validation_failed")
                        
                        if hasattr(self, 'metrics_publisher'):
                            self.metrics_publisher.publish_bookmark_fallback_metrics(
                                "initialization", "bucket_validation_failed", "bucket_validation_failed")
                        
                        self.s3_bookmark_storage = None
                        self.s3_enabled = False
                else:
                    if hasattr(self.structured_logger, 'warning'):
                        self.structured_logger.warning("Could not extract S3 bucket from JDBC paths, using in-memory bookmarks",
                                                     source_jdbc_path=source_jdbc_path,
                                                     target_jdbc_path=target_jdbc_path)
                    
            except Exception as e:
                # Add fallback initialization for in-memory bookmarks when S3 is unavailable
                if hasattr(self.structured_logger, 'error'):
                    self.structured_logger.error("Failed to initialize S3 bookmark storage, falling back to in-memory bookmarks",
                                               error=str(e),
                                               source_jdbc_path=source_jdbc_path,
                                               target_jdbc_path=target_jdbc_path)
                self.s3_bookmark_storage = None
                self.s3_enabled = False
        else:
            if hasattr(self.structured_logger, 'info'):
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
            # Use enhanced S3PathUtilities with structured logging
            bucket_name = S3PathUtilities.detect_s3_bucket_from_jdbc_paths(
                source_jdbc_path or "", 
                target_jdbc_path or "", 
                self.structured_logger
            )
            
            # Calculate detection duration for performance logging
            duration_ms = (time.time() - start_time) * 1000
            
            # Publish bucket detection success metrics
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
            
            if hasattr(self.structured_logger, 'error'):
                self.structured_logger.error("S3 bucket detection failed",
                                           error=str(e),
                                           source_path=source_jdbc_path,
                                           target_path=target_jdbc_path,
                                           duration_ms=duration_ms)
            
            # Publish bucket detection failure metrics
            if hasattr(self, 'metrics_publisher'):
                self.metrics_publisher.publish_s3_bucket_detection_metrics(
                    False, duration_ms)
            
            return None
            
        except Exception as e:
            # Calculate detection duration for error logging
            duration_ms = (time.time() - start_time) * 1000
            
            if hasattr(self.structured_logger, 'error'):
                self.structured_logger.error("Unexpected error during S3 bucket detection",
                                           error=str(e),
                                           source_path=source_jdbc_path,
                                           target_path=target_jdbc_path,
                                           duration_ms=duration_ms)
            
            # Publish bucket detection failure metrics
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
                if hasattr(self.structured_logger, 'info'):
                    self.structured_logger.info("Using cached bookmark state from current job execution",
                                              table_name=table_name,
                                              is_first_run=state.is_first_run,
                                              last_processed_value=state.last_processed_value)
                return state
            
            # Attempt to read existing bookmark state from S3 first
            if self.s3_enabled and self.s3_bookmark_storage:
                try:
                    if hasattr(self.structured_logger, 'info'):
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
                        # Successfully read bookmark from S3 - perform incremental loading
                        state = JobBookmarkState.from_s3_dict(s3_bookmark_data)
                        
                        # Ensure job_name is set correctly for this execution
                        state.job_name = self.job_name
                        
                        if hasattr(self.structured_logger, 'info'):
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
                        # No bookmark state exists in S3 - first run detection
                        if hasattr(self.structured_logger, 'info'):
                            self.structured_logger.info("No bookmark state found in S3, performing first run",
                                                      table_name=table_name,
                                                      s3_enabled=True)
                        
                except Exception as s3_error:
                    # Handle S3 read failures with centralized error handling and fallback
                    self._handle_s3_error("read", table_name, s3_error)
                    
                    if hasattr(self.structured_logger, 'warning'):
                        self.structured_logger.warning("S3 bookmark read failed, falling back to in-memory bookmarks",
                                                      table_name=table_name,
                                                      error=str(s3_error),
                                                      error_type=type(s3_error).__name__,
                                                      fallback_action="creating_new_bookmark_state_for_full_load")
                    
                    # Continue with in-memory bookmark creation below
            else:
                # S3 not enabled - use in-memory bookmarks (backward compatibility)
                if hasattr(self.structured_logger, 'info'):
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
            
            if hasattr(self.structured_logger, 'info'):
                self.structured_logger.info("Created new bookmark state for first run",
                                          table_name=table_name,
                                          incremental_strategy=incremental_strategy,
                                          incremental_column=incremental_column,
                                          is_first_run=True)
            
            # Cache the state for this job execution
            self.bookmark_states[table_name] = state
            return state
            
        except Exception as e:
            # Handle any unexpected errors with fallback to basic bookmark state
            if hasattr(self.structured_logger, 'error'):
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
        
        if hasattr(self.structured_logger, 'info'):
            self.structured_logger.info("Updated in-memory bookmark state",
                                      table_name=table_name,
                                      last_value=str(new_max_value),
                                      processed_rows=processed_rows,
                                      is_first_run=False)
        
        # Attempt to write bookmark data to S3 after processing
        if self.s3_enabled and self.s3_bookmark_storage:
            try:
                # Prepare S3-compatible bookmark data
                s3_bookmark_data = state.to_s3_dict()
                
                # Implement asynchronous S3 write operations to avoid blocking job execution
                if hasattr(self.structured_logger, 'debug'):
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
                        if hasattr(self.structured_logger, 'info'):
                            self.structured_logger.info("Successfully wrote bookmark to S3",
                                                       table_name=table_name,
                                                       last_value=str(new_max_value),
                                                       processed_rows=processed_rows,
                                                       s3_key=self.s3_bookmark_storage._get_bookmark_s3_key(table_name))
                        
                        # Publish CloudWatch metric for successful S3 write
                        self._publish_bookmark_metric("BookmarkS3WriteSuccess", table_name)
                        
                    else:
                        # Add error handling for S3 write failures with appropriate logging
                        if hasattr(self.structured_logger, 'warning'):
                            self.structured_logger.warning("S3 bookmark write failed, but in-memory state is maintained",
                                                          table_name=table_name,
                                                          fallback_available=True)
                        
                        # Publish CloudWatch metric for failed S3 write
                        self._publish_bookmark_metric("BookmarkS3WriteFailure", table_name)
                        
                finally:
                    loop.close()
                    
            except Exception as e:
                # Handle S3 write failures with centralized error handling
                self._handle_s3_error("write", table_name, e)
                
                if hasattr(self.structured_logger, 'error'):
                    self.structured_logger.error("Unexpected error during S3 bookmark write operation",
                                               table_name=table_name,
                                               error=str(e),
                                               error_type=type(e).__name__,
                                               fallback_available=True,
                                               job_continues=True)
                
                # Publish CloudWatch metric for failed S3 write
                self._publish_bookmark_metric("BookmarkS3WriteFailure", table_name)
                
                # Maintain in-memory bookmark state as backup when S3 operations fail
                # Don't raise exception for S3 failures - continue processing
                
        else:
            # S3 not enabled, using in-memory bookmarks only
            if hasattr(self.structured_logger, 'debug'):
                self.structured_logger.debug("S3 bookmark storage not enabled, using in-memory bookmarks only",
                                           table_name=table_name,
                                           s3_enabled=self.s3_enabled)
        
        # Log final bookmark state update (always successful for in-memory)
        if hasattr(self.structured_logger, 'info'):
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
            import boto3
            cloudwatch = boto3.client('cloudwatch')
            
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
                if hasattr(self.structured_logger, 'debug'):
                    self.structured_logger.debug("Published CloudWatch metric",
                                               metric_name=metric_name,
                                               table_name=table_name)
        except Exception as e:
            # Don't fail the job for CloudWatch metric failures
            if hasattr(self.structured_logger, 'warning'):
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
            
            if hasattr(self.structured_logger, 'info'):
                self.structured_logger.info(f"Reset bookmark state for table {table_name}")
        except Exception as e:
            if hasattr(self.structured_logger, 'error'):
                self.structured_logger.error(f"Failed to reset bookmark state for {table_name}: {str(e)}")
    
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
        if hasattr(self.structured_logger, 'error'):
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
                if hasattr(self.structured_logger, 'warning'):
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
                if hasattr(self.structured_logger, 'warning'):
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
                if hasattr(self.structured_logger, 'error'):
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
                                if hasattr(self.structured_logger, 'info'):
                                    self.structured_logger.info("Successfully deleted corrupted bookmark file",
                                                               table_name=table_name,
                                                               fallback_action="full_load_will_be_performed")
                            else:
                                if hasattr(self.structured_logger, 'warning'):
                                    self.structured_logger.warning("Failed to delete corrupted bookmark file",
                                                                 table_name=table_name,
                                                                 fallback_action="full_load_will_still_be_performed")
                        finally:
                            loop.close()
                            
                    except Exception as delete_error:
                        if hasattr(self.structured_logger, 'error'):
                            self.structured_logger.error("Error during corrupted file cleanup",
                                                       table_name=table_name,
                                                       delete_error=str(delete_error),
                                                       fallback_action="full_load_will_still_be_performed")
                
                # Publish metric for corrupted bookmark detection
                self._publish_bookmark_metric("BookmarkCorruptionDetected", table_name)
                
                return False
            
            return True
            
        except Exception as e:
            if hasattr(self.structured_logger, 'error'):
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
        
        if hasattr(self.structured_logger, 'info'):
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
                    # Use enhanced parallel operations if available (Task 11)
                    if self.enhanced_parallel_ops and len(table_names) > 10:
                        # For large table counts, use optimized parallel operations
                        if hasattr(self.structured_logger, 'info'):
                            self.structured_logger.info("Using enhanced parallel operations for large table count",
                                                       table_count=len(table_names))
                        s3_results = loop.run_until_complete(
                            self.enhanced_parallel_ops.read_bookmarks_parallel_optimized(
                                table_names, 
                                max_concurrent=min(20, max(5, len(table_names) // 5)),
                                chunk_size=min(50, max(10, len(table_names) // 4))
                            )
                        )
                    else:
                        # Use standard parallel operations for smaller table counts
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
                                
                                if hasattr(self.structured_logger, 'debug'):
                                    self.structured_logger.debug("Parallel bookmark initialization from S3",
                                                               table_name=table_name,
                                                               is_first_run=state.is_first_run,
                                                               last_processed_value=state.last_processed_value)
                                
                            except Exception as parse_error:
                                # Failed to parse S3 data - create new state
                                if hasattr(self.structured_logger, 'warning'):
                                    self.structured_logger.warning("Failed to parse S3 bookmark data, creating new state",
                                                                 table_name=table_name,
                                                                 error=str(parse_error))
                                bookmark_states[table_name] = self._create_new_bookmark_state(
                                    table_name, strategy, column)
                        else:
                            # No S3 data found - create new state for first run
                            bookmark_states[table_name] = self._create_new_bookmark_state(
                                table_name, strategy, column)
                            
                            if hasattr(self.structured_logger, 'debug'):
                                self.structured_logger.debug("Parallel bookmark initialization - new state created",
                                                           table_name=table_name,
                                                           is_first_run=True)
                    
                finally:
                    loop.close()
                    
            except Exception as s3_error:
                # S3 parallel read failed - fall back to individual initialization
                if hasattr(self.structured_logger, 'warning'):
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
            if hasattr(self.structured_logger, 'info'):
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
        
        if hasattr(self.structured_logger, 'info'):
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
        
        if hasattr(self.structured_logger, 'info'):
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
                    if hasattr(self.structured_logger, 'warning'):
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
                
                if hasattr(self.structured_logger, 'debug'):
                    self.structured_logger.debug("Updated in-memory bookmark state for batch",
                                               table_name=table_name,
                                               last_value=str(new_max_value),
                                               processed_rows=processed_rows)
                
                update_results[table_name] = True
                
            except Exception as e:
                if hasattr(self.structured_logger, 'error'):
                    self.structured_logger.error("Failed to update in-memory bookmark state for batch",
                                               table_name=table_name,
                                               error=str(e))
                update_results[table_name] = False
        
        # Perform batch S3 writes if S3 is enabled and we have data to write
        if self.s3_enabled and self.s3_bookmark_storage and s3_bookmark_batch:
            try:
                # Use asyncio for batch S3 operations
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                try:
                    # Use enhanced parallel operations if available (Task 11)
                    if self.enhanced_parallel_ops and len(s3_bookmark_batch) > 5:
                        # For larger batches, use optimized batch operations
                        if hasattr(self.structured_logger, 'info'):
                            self.structured_logger.info("Using enhanced batch operations for large bookmark batch",
                                                       batch_size=len(s3_bookmark_batch))
                        s3_results = loop.run_until_complete(
                            self.enhanced_parallel_ops.write_bookmarks_batch_optimized(
                                s3_bookmark_batch,
                                batch_size=min(15, max(5, len(s3_bookmark_batch) // 3)),
                                max_concurrent_batches=min(3, max(1, len(s3_bookmark_batch) // 10))
                            )
                        )
                    else:
                        # Use standard batch operations for smaller batches
                        s3_results = loop.run_until_complete(
                            self.s3_bookmark_storage.write_bookmarks_batch(s3_bookmark_batch)
                        )
                    
                    # Update results based on S3 write success
                    for table_name, s3_success in s3_results.items():
                        if not s3_success and update_results.get(table_name, False):
                            # S3 write failed but in-memory update succeeded
                            if hasattr(self.structured_logger, 'warning'):
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
                if hasattr(self.structured_logger, 'error'):
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
        
        if hasattr(self.structured_logger, 'info'):
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
    
    def update_bookmark_states_async(self, bookmark_updates: Dict[str, Any], 
                                   processed_rows_map: Dict[str, int] = None) -> None:
        """
        Asynchronously update bookmark states to avoid blocking data operations.
        
        This method implements Task 11 Requirement 7.5: Add asynchronous processing 
        to avoid blocking data operations. It performs bookmark updates in a separate 
        thread to ensure data processing continues without interruption.
        
        Args:
            bookmark_updates: Dictionary mapping table names to new max values
            processed_rows_map: Dictionary mapping table names to processed row counts (optional)
        """
        if not bookmark_updates:
            return
        
        def async_update_worker():
            """Worker function to perform bookmark updates asynchronously."""
            try:
                start_time = time.time()
                
                if hasattr(self.structured_logger, 'info'):
                    self.structured_logger.info("Starting asynchronous bookmark updates",
                                              table_count=len(bookmark_updates),
                                              s3_enabled=self.s3_enabled,
                                              thread_name=threading.current_thread().name)
                
                # Perform the actual batch update
                results = self.update_bookmark_states_batch(bookmark_updates, processed_rows_map)
                
                # Log completion
                duration_ms = (time.time() - start_time) * 1000
                successful_count = sum(1 for success in results.values() if success)
                
                if hasattr(self.structured_logger, 'info'):
                    self.structured_logger.info("Completed asynchronous bookmark updates",
                                              table_count=len(bookmark_updates),
                                              successful_count=successful_count,
                                              duration_ms=duration_ms,
                                              thread_name=threading.current_thread().name)
                
            except Exception as e:
                if hasattr(self.structured_logger, 'error'):
                    self.structured_logger.error("Error in asynchronous bookmark update worker",
                                               error=str(e),
                                               error_type=type(e).__name__,
                                               table_count=len(bookmark_updates),
                                               thread_name=threading.current_thread().name)
        
        # Start the update in a separate thread to avoid blocking
        update_thread = threading.Thread(
            target=async_update_worker,
            name=f"bookmark_update_{int(time.time())}",
            daemon=True  # Daemon thread won't prevent program exit
        )
        
        update_thread.start()
        
        if hasattr(self.structured_logger, 'debug'):
            self.structured_logger.debug("Started asynchronous bookmark update thread",
                                       table_count=len(bookmark_updates),
                                       thread_name=update_thread.name)
    
    def get_parallel_operation_stats(self) -> Dict[str, Any]:
        """
        Get statistics about parallel S3 operations performance.
        
        This method provides insights into the performance of parallel operations
        implemented in Task 11, helping with monitoring and optimization.
        
        Returns:
            Dictionary containing parallel operation statistics
        """
        stats = {
            "s3_enabled": self.s3_enabled,
            "enhanced_parallel_ops_available": self.enhanced_parallel_ops is not None,
            "total_bookmark_states": len(self.bookmark_states),
            "job_name": self.job_name
        }
        
        if self.enhanced_parallel_ops:
            # Add enhanced operation capabilities
            stats.update({
                "supports_optimized_parallel_read": True,
                "supports_optimized_batch_write": True,
                "recommended_max_concurrent_reads": 20,
                "recommended_batch_size": 15,
                "recommended_chunk_size": 50
            })
        else:
            stats.update({
                "supports_optimized_parallel_read": False,
                "supports_optimized_batch_write": False,
                "fallback_to_standard_operations": True
            })
        
        return stats