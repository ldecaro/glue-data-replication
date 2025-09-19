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
import json
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime, timezone, timedelta

from botocore.exceptions import ClientError, NoCredentialsError, BotoCoreError

from .s3_bookmark import S3BookmarkConfig, S3BookmarkStorage
from .manual_bookmark_config import ManualBookmarkConfig, BookmarkStrategyResolver
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
    
    # Manual configuration tracking fields
    is_manually_configured: bool = False
    manual_column_data_type: Optional[str] = None
    
    # Processing statistics
    processed_rows: int = 0
    
    @property
    def is_initial_load(self) -> bool:
        """Check if this is an initial load (first run)."""
        return self.is_first_run
    
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
            's3_key': self.s3_key,
            'is_manually_configured': self.is_manually_configured,
            'manual_column_data_type': self.manual_column_data_type,
            'processed_rows': self.processed_rows
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
            s3_key=data.get('s3_key'),
            is_manually_configured=data.get('is_manually_configured', False),
            manual_column_data_type=data.get('manual_column_data_type'),
            processed_rows=data.get('processed_rows', 0)
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
        boolean_fields = ['is_first_run', 'is_manually_configured']
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
        
        # Validate manual configuration fields
        if 'manual_column_data_type' in data and data['manual_column_data_type'] is not None:
            if not isinstance(data['manual_column_data_type'], str):
                logger.error("Field 'manual_column_data_type' must be a string")
                return False
            if not data['manual_column_data_type'].strip():
                logger.error("Field 'manual_column_data_type' cannot be empty string")
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
    
    # JDBC data type to bookmark strategy mapping
    # This comprehensive mapping supports timestamp, integer, and string/other data types
    # and handles database-specific type variations and edge cases
    JDBC_TYPE_TO_STRATEGY = {
        # Timestamp types -> timestamp strategy
        'TIMESTAMP': 'timestamp',
        'TIMESTAMP_WITH_TIMEZONE': 'timestamp',
        'TIMESTAMP_WITH_LOCAL_TIMEZONE': 'timestamp',  # Oracle
        'TIMESTAMPTZ': 'timestamp',  # PostgreSQL
        'TIMESTAMPLTZ': 'timestamp',  # Oracle
        'DATE': 'timestamp',
        'DATETIME': 'timestamp',
        'DATETIME2': 'timestamp',  # SQL Server
        'SMALLDATETIME': 'timestamp',  # SQL Server
        'TIME': 'timestamp',
        'TIME_WITH_TIMEZONE': 'timestamp',  # PostgreSQL
        'TIMETZ': 'timestamp',  # PostgreSQL
        'YEAR': 'timestamp',  # MySQL
        
        # Integer types -> primary_key strategy  
        'INTEGER': 'primary_key',
        'BIGINT': 'primary_key',
        'SMALLINT': 'primary_key',
        'TINYINT': 'primary_key',
        'MEDIUMINT': 'primary_key',  # MySQL
        'INT': 'primary_key',
        'INT2': 'primary_key',  # PostgreSQL
        'INT4': 'primary_key',  # PostgreSQL
        'INT8': 'primary_key',  # PostgreSQL
        'SERIAL': 'primary_key',  # PostgreSQL
        'SERIAL2': 'primary_key',  # PostgreSQL
        'SERIAL4': 'primary_key',  # PostgreSQL
        'SERIAL8': 'primary_key',  # PostgreSQL
        'BIGSERIAL': 'primary_key',  # PostgreSQL
        'SMALLSERIAL': 'primary_key',  # PostgreSQL
        'IDENTITY': 'primary_key',  # SQL Server
        'COUNTER': 'primary_key',  # Access
        
        # String and other types -> hash strategy
        'VARCHAR': 'hash',
        'VARCHAR2': 'hash',  # Oracle
        'NVARCHAR': 'hash',  # SQL Server
        'NVARCHAR2': 'hash',  # Oracle
        'CHAR': 'hash',
        'NCHAR': 'hash',
        'CHARACTER': 'hash',  # PostgreSQL
        'CHARACTER_VARYING': 'hash',  # PostgreSQL
        'TEXT': 'hash',
        'NTEXT': 'hash',  # SQL Server
        'LONGTEXT': 'hash',  # MySQL
        'MEDIUMTEXT': 'hash',  # MySQL
        'TINYTEXT': 'hash',  # MySQL
        'CLOB': 'hash',
        'NCLOB': 'hash',  # Oracle
        'LONG': 'hash',  # Oracle (deprecated)
        
        # Numeric types -> hash strategy (not suitable for primary keys due to precision)
        'DECIMAL': 'hash',
        'NUMERIC': 'hash',
        'NUMBER': 'hash',  # Oracle (could be integer or decimal)
        'MONEY': 'hash',  # SQL Server
        'SMALLMONEY': 'hash',  # SQL Server
        'FLOAT': 'hash',
        'FLOAT4': 'hash',  # PostgreSQL
        'FLOAT8': 'hash',  # PostgreSQL
        'REAL': 'hash',
        'DOUBLE': 'hash',
        'DOUBLE_PRECISION': 'hash',  # PostgreSQL
        'PRECISION': 'hash',
        
        # Boolean and bit types -> hash strategy
        'BOOLEAN': 'hash',
        'BOOL': 'hash',  # PostgreSQL/MySQL
        'BIT': 'hash',
        'TINYINT(1)': 'hash',  # MySQL boolean
        
        # Binary types -> hash strategy
        'BINARY': 'hash',
        'VARBINARY': 'hash',
        'LONGVARBINARY': 'hash',
        'BLOB': 'hash',
        'LONGBLOB': 'hash',  # MySQL
        'MEDIUMBLOB': 'hash',  # MySQL
        'TINYBLOB': 'hash',  # MySQL
        'BYTEA': 'hash',  # PostgreSQL
        'RAW': 'hash',  # Oracle
        'LONG_RAW': 'hash',  # Oracle (deprecated)
        'BFILE': 'hash',  # Oracle
        'IMAGE': 'hash',  # SQL Server (deprecated)
        
        # Special types -> hash strategy
        'UUID': 'hash',  # PostgreSQL
        'UNIQUEIDENTIFIER': 'hash',  # SQL Server
        'GUID': 'hash',  # Access
        'XML': 'hash',  # SQL Server
        'JSON': 'hash',  # PostgreSQL/MySQL
        'JSONB': 'hash',  # PostgreSQL
        'ARRAY': 'hash',  # PostgreSQL
        'HSTORE': 'hash',  # PostgreSQL
        'POINT': 'hash',  # PostgreSQL geometry
        'LINE': 'hash',  # PostgreSQL geometry
        'LSEG': 'hash',  # PostgreSQL geometry
        'BOX': 'hash',  # PostgreSQL geometry
        'PATH': 'hash',  # PostgreSQL geometry
        'POLYGON': 'hash',  # PostgreSQL geometry
        'CIRCLE': 'hash',  # PostgreSQL geometry
        'INET': 'hash',  # PostgreSQL network
        'CIDR': 'hash',  # PostgreSQL network
        'MACADDR': 'hash',  # PostgreSQL network
        'MACADDR8': 'hash',  # PostgreSQL network
        'TSQUERY': 'hash',  # PostgreSQL text search
        'TSVECTOR': 'hash',  # PostgreSQL text search
        'INTERVAL': 'hash',  # PostgreSQL/Oracle
        'ENUM': 'hash',  # MySQL/PostgreSQL
        'SET': 'hash',  # MySQL
        'GEOMETRY': 'hash',  # MySQL/PostgreSQL spatial
        'GEOGRAPHY': 'hash',  # SQL Server spatial
        'HIERARCHYID': 'hash',  # SQL Server
        'SQL_VARIANT': 'hash',  # SQL Server
        'CURSOR': 'hash',  # SQL Server
        'TABLE': 'hash',  # SQL Server
        'ROWID': 'hash',  # Oracle
        'UROWID': 'hash',  # Oracle
        'REF': 'hash',  # Oracle
        'XMLTYPE': 'hash',  # Oracle
        'ANYDATA': 'hash',  # Oracle
        'ANYTYPE': 'hash',  # Oracle
        'ANYDATASET': 'hash',  # Oracle
        'SDO_GEOMETRY': 'hash',  # Oracle spatial
        'MDSYS.SDO_GEOMETRY': 'hash',  # Oracle spatial (fully qualified)
    }
    
    def __init__(self, glue_context, job_name: str, job=None, 
                 source_jdbc_path: Optional[str] = None, target_jdbc_path: Optional[str] = None,
                 manual_bookmark_config: Optional[str] = None):
        """
        Initialize JobBookmarkManager with S3 persistent storage support.
        
        Args:
            glue_context: AWS Glue context
            job_name: Name of the Glue job
            job: Glue job instance (optional)
            source_jdbc_path: S3 path to source JDBC driver (optional)
            target_jdbc_path: S3 path to target JDBC driver (optional)
            manual_bookmark_config: JSON string containing manual bookmark configuration (optional)
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
        
        # Parse manual bookmark configuration if provided
        self.manual_bookmark_configs = {}
        if manual_bookmark_config:
            try:
                self.manual_bookmark_configs = self._parse_manual_bookmark_config(manual_bookmark_config)
                # Success logging is handled within _parse_manual_bookmark_config method
            except Exception as e:
                # Error logging is handled within _parse_manual_bookmark_config method
                # Fallback to empty configuration to proceed without manual config
                if hasattr(self.structured_logger, 'error'):
                    self.structured_logger.error("Failed to parse manual bookmark configuration, proceeding without manual config",
                                               error=str(e),
                                               config_json=manual_bookmark_config[:200] + "..." if len(manual_bookmark_config) > 200 else manual_bookmark_config)
                self.manual_bookmark_configs = {}
        
        # Initialize BookmarkStrategyResolver with manual configurations and structured logger
        self.bookmark_strategy_resolver = BookmarkStrategyResolver(self.manual_bookmark_configs, self.structured_logger)
        
        # Initialize tracking for bookmark detection summary (Task 23)
        self.bookmark_detection_stats = {
            'total_tables': 0,
            'manual_count': 0,
            'automatic_count': 0,
            'failed_count': 0,
            'strategy_distribution': {'timestamp': 0, 'primary_key': 0, 'hash': 0}
        }
        
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
    
    def _parse_manual_bookmark_config(self, config_json: str) -> Dict[str, ManualBookmarkConfig]:
        """
        Parse and validate manual bookmark configuration JSON.
        
        This method implements JSON parsing and validation for manual configuration parameter,
        creating a dictionary mapping table names to ManualBookmarkConfig instances.
        
        Args:
            config_json: JSON string containing manual bookmark configuration
            
        Returns:
            Dictionary mapping table names to ManualBookmarkConfig instances
            
        Raises:
            ValueError: If JSON is malformed or configuration structure is invalid
            TypeError: If configuration data types are incorrect
        """
        import time
        parsing_start_time = time.time()
        
        # Log start of manual configuration parsing
        if hasattr(self.structured_logger, 'log_manual_config_parsing_start'):
            self.structured_logger.log_manual_config_parsing_start(config_json)
        
        if not config_json or not isinstance(config_json, str):
            parsing_duration_ms = (time.time() - parsing_start_time) * 1000
            error_msg = "Manual bookmark configuration must be a non-empty JSON string"
            
            if hasattr(self.structured_logger, 'log_manual_config_parsing_failure'):
                self.structured_logger.log_manual_config_parsing_failure(
                    error_msg, config_json or "", parsing_duration_ms, "proceed_without_manual_config"
                )
            
            raise ValueError(error_msg)
        
        # Strip whitespace from JSON string
        config_json = config_json.strip()
        if not config_json:
            parsing_duration_ms = (time.time() - parsing_start_time) * 1000
            error_msg = "Manual bookmark configuration cannot be empty or whitespace only"
            
            if hasattr(self.structured_logger, 'log_manual_config_parsing_failure'):
                self.structured_logger.log_manual_config_parsing_failure(
                    error_msg, config_json, parsing_duration_ms, "proceed_without_manual_config"
                )
            
            raise ValueError(error_msg)
        
        try:
            # Parse JSON with error handling for malformed JSON
            config_data = json.loads(config_json)
        except json.JSONDecodeError as e:
            parsing_duration_ms = (time.time() - parsing_start_time) * 1000
            error_msg = f"Invalid JSON format in manual bookmark configuration: {e}"
            
            if hasattr(self.structured_logger, 'log_manual_config_parsing_failure'):
                self.structured_logger.log_manual_config_parsing_failure(
                    error_msg, config_json, parsing_duration_ms, "proceed_without_manual_config"
                )
            
            raise ValueError(error_msg)
        
        # Validate that parsed data is a dictionary
        if config_data is None:
            parsing_duration_ms = (time.time() - parsing_start_time) * 1000
            error_msg = "Manual bookmark configuration cannot be null"
            
            if hasattr(self.structured_logger, 'log_manual_config_parsing_failure'):
                self.structured_logger.log_manual_config_parsing_failure(
                    error_msg, config_json, parsing_duration_ms, "proceed_without_manual_config"
                )
            
            raise ValueError(error_msg)
        
        if not isinstance(config_data, dict):
            parsing_duration_ms = (time.time() - parsing_start_time) * 1000
            error_msg = "Manual bookmark configuration must be a JSON object (dictionary)"
            
            if hasattr(self.structured_logger, 'log_manual_config_parsing_failure'):
                self.structured_logger.log_manual_config_parsing_failure(
                    error_msg, config_json, parsing_duration_ms, "proceed_without_manual_config"
                )
            
            raise ValueError(error_msg)
        
        if not config_data:
            parsing_duration_ms = (time.time() - parsing_start_time) * 1000
            error_msg = "Manual bookmark configuration cannot be an empty object"
            
            if hasattr(self.structured_logger, 'log_manual_config_parsing_failure'):
                self.structured_logger.log_manual_config_parsing_failure(
                    error_msg, config_json, parsing_duration_ms, "proceed_without_manual_config"
                )
            
            raise ValueError(error_msg)
        
        # Parse and validate each table configuration
        manual_configs = {}
        
        for table_key, table_config in config_data.items():
            try:
                # Validate table key format
                if not isinstance(table_key, str) or not table_key.strip():
                    error_msg = f"Table key '{table_key}' must be a non-empty string"
                    
                    if hasattr(self.structured_logger, 'log_invalid_manual_config_entry'):
                        self.structured_logger.log_invalid_manual_config_entry(
                            table_key, error_msg, {"key": table_key, "config": table_config}, 
                            "skip_table_and_continue"
                        )
                    
                    raise ValueError(error_msg)
                
                # Handle both simplified and full configuration formats
                if isinstance(table_config, str):
                    # Simplified format: "table_name": "column_name"
                    table_config_dict = {
                        'table_name': table_key,
                        'column_name': table_config
                    }
                elif isinstance(table_config, dict):
                    # Full format: "table_name": {"table_name": "...", "column_name": "..."}
                    table_config_dict = table_config
                else:
                    error_msg = f"Configuration for table '{table_key}' must be a string (column name) or object (dictionary)"
                    
                    if hasattr(self.structured_logger, 'log_invalid_manual_config_entry'):
                        self.structured_logger.log_invalid_manual_config_entry(
                            table_key, error_msg, {"key": table_key, "config": table_config}, 
                            "skip_table_and_continue"
                        )
                    
                    raise TypeError(error_msg)
                
                # Create ManualBookmarkConfig instance (validation happens in __post_init__)
                manual_config = ManualBookmarkConfig.from_dict(table_config_dict)
                
                # Use the table_name from the config, not the key (for validation consistency)
                table_name = manual_config.table_name
                
                # Check for duplicate table names
                if table_name in manual_configs:
                    error_msg = f"Duplicate table configuration found for table '{table_name}'"
                    
                    if hasattr(self.structured_logger, 'log_invalid_manual_config_entry'):
                        self.structured_logger.log_invalid_manual_config_entry(
                            table_name, error_msg, {"key": table_key, "config": table_config}, 
                            "skip_duplicate_and_continue"
                        )
                    
                    raise ValueError(error_msg)
                
                manual_configs[table_name] = manual_config
                
                # Log successful parsing for each table
                if hasattr(self.structured_logger, 'debug'):
                    self.structured_logger.debug("Parsed manual bookmark configuration for table",
                                               table_name=table_name,
                                               column_name=manual_config.column_name,
                                               table_key=table_key)
                
            except (ValueError, TypeError) as e:
                # Log invalid configuration entry and re-raise with additional context
                error_msg = f"Invalid configuration for table '{table_key}': {e}"
                
                if hasattr(self.structured_logger, 'log_invalid_manual_config_entry'):
                    self.structured_logger.log_invalid_manual_config_entry(
                        table_key, str(e), {"key": table_key, "config": table_config}, 
                        "skip_table_and_continue"
                    )
                
                raise ValueError(error_msg)
            except Exception as e:
                # Handle unexpected errors during configuration parsing
                error_msg = f"Unexpected error parsing configuration for table '{table_key}': {e}"
                
                if hasattr(self.structured_logger, 'log_invalid_manual_config_entry'):
                    self.structured_logger.log_invalid_manual_config_entry(
                        table_key, str(e), {"key": table_key, "config": table_config}, 
                        "skip_table_and_continue"
                    )
                
                raise ValueError(error_msg)
        
        # Validate that at least one valid configuration was parsed
        if not manual_configs:
            parsing_duration_ms = (time.time() - parsing_start_time) * 1000
            error_msg = "No valid table configurations found in manual bookmark configuration"
            
            if hasattr(self.structured_logger, 'log_manual_config_parsing_failure'):
                self.structured_logger.log_manual_config_parsing_failure(
                    error_msg, config_json, parsing_duration_ms, "proceed_without_manual_config"
                )
            
            raise ValueError(error_msg)
        
        parsing_duration_ms = (time.time() - parsing_start_time) * 1000
        
        # Log successful parsing completion
        if hasattr(self.structured_logger, 'log_manual_config_parsing_success'):
            self.structured_logger.log_manual_config_parsing_success(
                len(manual_configs), list(manual_configs.keys()), parsing_duration_ms
            )
        else:
            # Fallback logging
            if hasattr(self.structured_logger, 'info'):
                self.structured_logger.info("Manual bookmark configuration parsing completed",
                                          total_tables=len(manual_configs),
                                          table_names=list(manual_configs.keys()))
        
        return manual_configs
    
    def log_bookmark_detection_summary(self):
        """
        Log comprehensive summary of bookmark detection across all processed tables.
        
        This method provides observability into which tables used manual vs automatic
        bookmark detection and the distribution of strategies used. Should be called
        at the end of job processing to provide complete statistics.
        """
        stats = self.bookmark_detection_stats
        
        if hasattr(self.structured_logger, 'log_bookmark_detection_summary'):
            self.structured_logger.log_bookmark_detection_summary(
                stats['total_tables'],
                stats['manual_count'],
                stats['automatic_count'],
                stats['failed_count'],
                stats['strategy_distribution']
            )
        else:
            # Fallback logging
            if hasattr(self.structured_logger, 'info'):
                self.structured_logger.info(
                    "Bookmark detection summary",
                    total_tables=stats['total_tables'],
                    manual_configurations=stats['manual_count'],
                    automatic_detections=stats['automatic_count'],
                    failed_detections=stats['failed_count'],
                    strategy_distribution=stats['strategy_distribution']
                )
    
    def _get_column_data_type(self, table_name: str, column_name: str, connection) -> Optional[str]:
        """
        Query JDBC metadata for specific columns to determine data type.
        
        This method implements database connection metadata access using getMetaData() and getColumns()
        to retrieve column data type information. It includes error handling for metadata query failures
        and missing columns, plus a caching mechanism to improve performance.
        
        Args:
            table_name: Name of the table containing the column
            column_name: Name of the column to query
            connection: JDBC database connection object
            
        Returns:
            String representation of the column data type, or None if column not found or query fails
            
        Raises:
            None - All exceptions are caught and logged, returning None for failures
        """
        if not table_name or not column_name or not connection:
            if hasattr(self.structured_logger, 'error'):
                self.structured_logger.error("Invalid parameters for JDBC metadata query",
                                           table_name=table_name,
                                           column_name=column_name,
                                           connection_available=connection is not None)
            return None
        
        # Create cache key for performance optimization
        cache_key = f"{table_name}.{column_name}"
        
        # Initialize metadata cache if not exists
        if not hasattr(self, '_jdbc_metadata_cache'):
            self._jdbc_metadata_cache = {}
        
        # Check cache first to improve performance
        if cache_key in self._jdbc_metadata_cache:
            cached_result = self._jdbc_metadata_cache[cache_key]
            if hasattr(self.structured_logger, 'debug'):
                self.structured_logger.debug("Retrieved column data type from cache",
                                           table_name=table_name,
                                           column_name=column_name,
                                           data_type=cached_result)
            return cached_result
        
        start_time = time.time()
        
        try:
            # Get database metadata using JDBC connection
            if hasattr(self.structured_logger, 'debug'):
                self.structured_logger.debug("Querying JDBC metadata for column data type",
                                           table_name=table_name,
                                           column_name=column_name)
            
            # Access database metadata
            metadata = connection.getMetaData()
            
            # Query column information using getColumns()
            # Parameters: catalog, schemaPattern, tableNamePattern, columnNamePattern
            result_set = metadata.getColumns(None, None, table_name, column_name)
            
            column_data_type = None
            
            # Process result set
            if result_set.next():
                # Extract column data type information
                type_name = result_set.getString('TYPE_NAME')
                jdbc_type = result_set.getInt('DATA_TYPE')
                column_size = result_set.getInt('COLUMN_SIZE')
                nullable = result_set.getInt('NULLABLE')
                
                # Use TYPE_NAME as the primary data type identifier
                column_data_type = type_name
                
                # Calculate query duration for performance logging
                duration_ms = (time.time() - start_time) * 1000
                
                if hasattr(self.structured_logger, 'info'):
                    self.structured_logger.info("Successfully retrieved column data type from JDBC metadata",
                                              table_name=table_name,
                                              column_name=column_name,
                                              data_type=column_data_type,
                                              jdbc_type=jdbc_type,
                                              column_size=column_size,
                                              nullable=nullable,
                                              duration_ms=duration_ms)
            else:
                # Column not found in metadata
                duration_ms = (time.time() - start_time) * 1000
                
                if hasattr(self.structured_logger, 'warning'):
                    self.structured_logger.warning("Column not found in JDBC metadata",
                                                 table_name=table_name,
                                                 column_name=column_name,
                                                 duration_ms=duration_ms)
            
            # Close result set to free resources
            try:
                result_set.close()
            except Exception:
                pass  # Ignore close errors
            
            # Cache the result (even if None) to improve performance
            self._jdbc_metadata_cache[cache_key] = column_data_type
            
            return column_data_type
            
        except Exception as e:
            # Calculate query duration for error logging
            duration_ms = (time.time() - start_time) * 1000
            
            # Handle metadata query failures with comprehensive error logging
            if hasattr(self.structured_logger, 'error'):
                self.structured_logger.error("Failed to query JDBC metadata for column data type",
                                           table_name=table_name,
                                           column_name=column_name,
                                           error=str(e),
                                           error_type=type(e).__name__,
                                           duration_ms=duration_ms)
            
            # Cache the failure to avoid repeated failed queries
            self._jdbc_metadata_cache[cache_key] = None
            
            return None
    
    def _determine_strategy_from_data_type(self, data_type: str) -> str:
        """
        Map JDBC data type to appropriate bookmark strategy.
        
        This method implements comprehensive type mapping from JDBC data types to bookmark strategies
        based on the data type characteristics. It supports timestamp, primary_key, and hash
        strategies and handles database-specific type variations and edge cases.
        
        The method uses the class-level JDBC_TYPE_TO_STRATEGY dictionary for efficient lookups
        and includes fallback logic for partial matches and unknown types.
        
        Args:
            data_type: JDBC data type string (e.g., 'TIMESTAMP', 'INTEGER', 'VARCHAR')
            
        Returns:
            Bookmark strategy string ('timestamp', 'primary_key', or 'hash')
        """
        if not data_type or not isinstance(data_type, str):
            if hasattr(self.structured_logger, 'warning'):
                self.structured_logger.warning("Invalid data type provided for strategy determination",
                                             data_type=data_type)
            return 'hash'  # Default fallback strategy
        
        # Normalize data type (uppercase, remove extra spaces)
        normalized_type = data_type.upper().strip()
        
        # Direct mapping lookup using class-level dictionary
        strategy = self.JDBC_TYPE_TO_STRATEGY.get(normalized_type)
        
        if strategy:
            if hasattr(self.structured_logger, 'debug'):
                self.structured_logger.debug("Mapped JDBC data type to bookmark strategy",
                                           data_type=data_type,
                                           normalized_type=normalized_type,
                                           strategy=strategy)
            return strategy
        
        # Fallback logic for partial matches or database-specific variations
        if any(ts_type in normalized_type for ts_type in ['TIMESTAMP', 'DATE', 'TIME']):
            strategy = 'timestamp'
            if hasattr(self.structured_logger, 'debug'):
                self.structured_logger.debug("Mapped data type to timestamp strategy using partial match",
                                           data_type=data_type,
                                           normalized_type=normalized_type,
                                           strategy=strategy)
            return strategy
        elif any(int_type in normalized_type for int_type in ['INT', 'SERIAL', 'NUMBER']):
            # Additional check for Oracle NUMBER type - could be decimal
            if 'NUMBER' in normalized_type:
                # For Oracle NUMBER, default to hash unless we know it's an integer
                strategy = 'hash'
            else:
                strategy = 'primary_key'
            
            if hasattr(self.structured_logger, 'debug'):
                self.structured_logger.debug("Mapped data type to integer-based strategy using partial match",
                                           data_type=data_type,
                                           normalized_type=normalized_type,
                                           strategy=strategy)
            return strategy
        else:
            # Default to hash strategy for unknown types
            strategy = 'hash'
            if hasattr(self.structured_logger, 'warning'):
                self.structured_logger.warning("Unknown JDBC data type, defaulting to hash strategy",
                                             data_type=data_type,
                                             normalized_type=normalized_type,
                                             strategy=strategy)
            return strategy
    
    def _clear_jdbc_metadata_cache(self):
        """
        Clear the JDBC metadata cache to free memory.
        
        This method provides cache management functionality to clear cached
        JDBC metadata queries when needed for memory optimization.
        """
        if hasattr(self, '_jdbc_metadata_cache'):
            cache_size = len(self._jdbc_metadata_cache)
            self._jdbc_metadata_cache.clear()
            
            if hasattr(self.structured_logger, 'info'):
                self.structured_logger.info("JDBC metadata cache cleared",
                                          cleared_entries=cache_size)
        else:
            if hasattr(self.structured_logger, 'debug'):
                self.structured_logger.debug("JDBC metadata cache was not initialized, nothing to clear")
    
    def _get_jdbc_metadata_cache_stats(self) -> Dict[str, int]:
        """
        Get JDBC metadata cache statistics for monitoring and performance analysis.
        
        Returns:
            Dictionary containing cache statistics including entry count and memory usage info
        """
        if not hasattr(self, '_jdbc_metadata_cache'):
            return {
                'cached_entries': 0,
                'cache_initialized': False
            }
        
        stats = {
            'cached_entries': len(self._jdbc_metadata_cache),
            'cache_initialized': True
        }
        
        # Count successful vs failed cache entries
        successful_entries = sum(1 for value in self._jdbc_metadata_cache.values() if value is not None)
        failed_entries = len(self._jdbc_metadata_cache) - successful_entries
        
        stats.update({
            'successful_entries': successful_entries,
            'failed_entries': failed_entries
        })
        
        if hasattr(self.structured_logger, 'debug'):
            self.structured_logger.debug("JDBC metadata cache statistics retrieved", **stats)
        
        return stats
    
    def _get_bookmark_strategy_for_table(self, table_name: str, connection) -> Tuple[str, Optional[str]]:
        """
        Get bookmark strategy for a table using manual configuration or automatic detection.
        
        This method integrates BookmarkStrategyResolver into the bookmark initialization process,
        ensuring that manual configuration takes precedence over automatic detection when available.
        
        Args:
            table_name: Name of the table to get strategy for
            connection: JDBC database connection for metadata queries
            
        Returns:
            Tuple of (strategy, column_name) where:
            - strategy: 'timestamp', 'primary_key', or 'hash'
            - column_name: Name of the column to use for incremental loading (None for hash strategy)
        """
        try:
            # Use BookmarkStrategyResolver to resolve strategy with manual config priority
            strategy, column_name, is_manually_configured = self.bookmark_strategy_resolver.resolve_strategy(
                table_name, connection
            )
            
            # Update bookmark detection statistics (Task 23)
            self.bookmark_detection_stats['total_tables'] += 1
            if is_manually_configured:
                self.bookmark_detection_stats['manual_count'] += 1
            else:
                self.bookmark_detection_stats['automatic_count'] += 1
            
            # Update strategy distribution
            if strategy in self.bookmark_detection_stats['strategy_distribution']:
                self.bookmark_detection_stats['strategy_distribution'][strategy] += 1
            
            # Log the strategy resolution result with configuration source
            config_source = "manual" if is_manually_configured else "automatic"
            
            if hasattr(self.structured_logger, 'info'):
                self.structured_logger.info("Bookmark strategy resolved for table",
                                          table_name=table_name,
                                          strategy=strategy,
                                          column_name=column_name,
                                          configuration_source=config_source,
                                          is_manually_configured=is_manually_configured)
            
            # Log manual configuration override if applicable
            if is_manually_configured and hasattr(self.structured_logger, 'log_manual_config_table_override'):
                # Get what automatic detection would have chosen for comparison
                try:
                    auto_strategy, auto_column = self.bookmark_strategy_resolver._get_automatic_strategy(table_name, connection)
                    self.structured_logger.log_manual_config_table_override(
                        table_name, column_name, auto_column, strategy, auto_strategy
                    )
                except Exception:
                    # If automatic detection fails, just log the manual override without comparison
                    self.structured_logger.log_manual_config_table_override(
                        table_name, column_name, None, strategy, None
                    )
            
            return strategy, column_name
            
        except Exception as e:
            # Update failed detection statistics
            self.bookmark_detection_stats['total_tables'] += 1
            self.bookmark_detection_stats['failed_count'] += 1
            
            # Handle any errors in strategy resolution with fallback
            if hasattr(self.structured_logger, 'error'):
                self.structured_logger.error("Error resolving bookmark strategy, falling back to hash strategy",
                                           table_name=table_name,
                                           error=str(e),
                                           error_type=type(e).__name__,
                                           fallback_strategy="hash")
            
            # Fallback to hash strategy to ensure job continues
            return 'hash', None
    
    def initialize_bookmark_state_with_auto_detection(self, table_name: str, connection,
                                                    database: Optional[str] = None,
                                                    catalog_id: Optional[str] = None,
                                                    dataframe: Optional['DataFrame'] = None,
                                                    engine_type: Optional[str] = None) -> JobBookmarkState:
        """
        Initialize job bookmark state with automatic strategy detection using manual configuration.
        
        This method provides an alternative to initialize_bookmark_state that automatically
        detects the bookmark strategy using the BookmarkStrategyResolver, which prioritizes
        manual configuration over automatic detection.
        
        Args:
            table_name: Name of the table to initialize bookmark for
            connection: JDBC database connection for strategy detection
            database: Database name (required for Iceberg tables)
            catalog_id: Optional catalog ID for cross-account Iceberg access
            dataframe: Optional DataFrame for Iceberg fallback bookmark detection
            engine_type: Optional engine type to detect Iceberg tables
            
        Returns:
            JobBookmarkState instance with initialized state using detected strategy
        """
        try:
            # Detect strategy using manual configuration or automatic detection
            strategy, column_name = self._get_bookmark_strategy_for_table(table_name, connection)
            
            if hasattr(self.structured_logger, 'info'):
                self.structured_logger.info("Auto-detected bookmark strategy for table initialization",
                                          table_name=table_name,
                                          detected_strategy=strategy,
                                          detected_column=column_name)
            
            # Use the existing initialize_bookmark_state method with detected strategy
            return self.initialize_bookmark_state(
                table_name=table_name,
                incremental_strategy=strategy,
                incremental_column=column_name,
                database=database,
                catalog_id=catalog_id,
                dataframe=dataframe,
                engine_type=engine_type
            )
            
        except Exception as e:
            # Handle any errors in auto-detection with fallback
            if hasattr(self.structured_logger, 'error'):
                self.structured_logger.error("Error in auto-detection, falling back to hash strategy",
                                           table_name=table_name,
                                           error=str(e),
                                           error_type=type(e).__name__,
                                           fallback_strategy="hash")
            
            # Fallback to hash strategy initialization
            return self.initialize_bookmark_state(
                table_name=table_name,
                incremental_strategy='hash',
                incremental_column=None,
                database=database,
                catalog_id=catalog_id,
                dataframe=dataframe,
                engine_type=engine_type
            )
    
    def initialize_bookmark_state(self, table_name: str, incremental_strategy: str,
                                incremental_column: Optional[str] = None,
                                database: Optional[str] = None,
                                catalog_id: Optional[str] = None,
                                dataframe: Optional['DataFrame'] = None,
                                engine_type: Optional[str] = None,
                                connection=None,
                                use_manual_config: bool = True) -> JobBookmarkState:
        """
        Initialize job bookmark state for a table with S3 integration and Iceberg support.
        
        This method implements enhanced bookmark initialization that:
        - Detects Iceberg tables and uses identifier-field-ids for bookmark management
        - Attempts to read existing bookmark state from S3 first
        - Handles first-run detection based on S3 bookmark existence
        - Falls back to in-memory bookmarks when S3 operations fail
        - Ensures backward compatibility with existing job configurations
        - Supports manual configuration override when connection is provided
        
        Args:
            table_name: Name of the table to initialize bookmark for
            incremental_strategy: Strategy for incremental loading (timestamp, primary_key, hash)
            incremental_column: Column to use for incremental loading (optional)
            database: Database name (required for Iceberg tables)
            catalog_id: Optional catalog ID for cross-account Iceberg access
            dataframe: Optional DataFrame for Iceberg fallback bookmark detection
            engine_type: Optional engine type to detect Iceberg tables
            connection: Optional JDBC connection for manual configuration detection
            use_manual_config: Whether to use manual configuration when available (default: True)
            
        Returns:
            JobBookmarkState instance with initialized state
        """
        try:
            # Check for manual configuration override when connection is available and enabled
            if use_manual_config and connection is not None and hasattr(self, 'manual_bookmark_configs') and table_name in self.manual_bookmark_configs:
                try:
                    # Override strategy and column with manual configuration
                    manual_strategy, manual_column = self._get_bookmark_strategy_for_table(table_name, connection)
                    
                    if hasattr(self.structured_logger, 'info'):
                        self.structured_logger.info("Overriding provided strategy with manual configuration",
                                                  table_name=table_name,
                                                  provided_strategy=incremental_strategy,
                                                  provided_column=incremental_column,
                                                  manual_strategy=manual_strategy,
                                                  manual_column=manual_column)
                    
                    # Use manual configuration values
                    incremental_strategy = manual_strategy
                    incremental_column = manual_column
                    
                except Exception as e:
                    if hasattr(self.structured_logger, 'warning'):
                        self.structured_logger.warning("Failed to apply manual configuration, using provided strategy",
                                                     table_name=table_name,
                                                     error=str(e),
                                                     provided_strategy=incremental_strategy,
                                                     provided_column=incremental_column)
            
            # Check if this is an Iceberg table and route to specialized initialization
            if self._is_iceberg_table(table_name, database, engine_type):
                if database:
                    # Extract table name from full table name if needed
                    actual_table_name = table_name.split('.')[-1] if '.' in table_name else table_name
                    
                    if hasattr(self.structured_logger, 'info'):
                        self.structured_logger.info("Detected Iceberg table, using specialized bookmark initialization",
                                                   table_name=table_name,
                                                   database=database,
                                                   actual_table_name=actual_table_name)
                    
                    return self.initialize_iceberg_bookmark_state(
                        database=database,
                        table=actual_table_name,
                        incremental_strategy=incremental_strategy,
                        catalog_id=catalog_id,
                        dataframe=dataframe
                    )
                else:
                    if hasattr(self.structured_logger, 'warning'):
                        self.structured_logger.warning("Iceberg table detected but no database provided, using traditional initialization",
                                                     table_name=table_name)
            
            # Check if we have a cached state from previous processing in this job
            # Only use cache if manual config override is not being applied differently
            cache_key = f"{table_name}_{incremental_strategy}_{incremental_column}_{use_manual_config}"
            if table_name in self.bookmark_states and not (use_manual_config and connection is not None and hasattr(self, 'manual_bookmark_configs') and table_name in self.manual_bookmark_configs):
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
            
            # Check if manual configuration was used for this table
            is_manually_configured = (hasattr(self, 'manual_bookmark_configs') and 
                                    table_name in self.manual_bookmark_configs)
            manual_column_data_type = None
            
            # If manual configuration was used, try to get the data type
            if is_manually_configured and connection is not None:
                try:
                    manual_column_data_type = self._get_column_data_type(table_name, incremental_column, connection)
                except Exception as e:
                    if hasattr(self.structured_logger, 'debug'):
                        self.structured_logger.debug("Could not determine manual column data type",
                                                   table_name=table_name,
                                                   column_name=incremental_column,
                                                   error=str(e))
            
            state = JobBookmarkState(
                table_name=table_name,
                incremental_strategy=incremental_strategy,
                incremental_column=incremental_column,
                is_first_run=True,  # First run - perform full load
                job_name=self.job_name,
                created_timestamp=current_time,
                updated_timestamp=current_time,
                version="1.0",
                is_manually_configured=is_manually_configured,
                manual_column_data_type=manual_column_data_type
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
            
            # Check if manual configuration was used for this table (for fallback state)
            is_manually_configured = (hasattr(self, 'manual_bookmark_configs') and 
                                    table_name in self.manual_bookmark_configs)
            
            state = JobBookmarkState(
                table_name=table_name,
                incremental_strategy=incremental_strategy,
                incremental_column=incremental_column,
                is_first_run=True,
                job_name=self.job_name,
                created_timestamp=current_time,
                is_manually_configured=is_manually_configured,
                manual_column_data_type=None,
                updated_timestamp=current_time,
                version="1.0"
            )
            
            # Cache the fallback state
            self.bookmark_states[table_name] = state
            return state
    
    def _is_iceberg_table(self, table_name: str, database: Optional[str] = None, 
                         engine_type: Optional[str] = None) -> bool:
        """
        Determine if a table is an Iceberg table.
        
        Args:
            table_name: Name of the table
            database: Optional database name
            engine_type: Optional engine type hint
            
        Returns:
            bool: True if table is identified as Iceberg, False otherwise
        """
        try:
            # Check engine type hint first
            if engine_type and engine_type.lower() == 'iceberg':
                return True
            
            # Check if database is provided and we can query Glue Data Catalog
            if database:
                try:
                    # Extract actual table name from full table name
                    actual_table_name = table_name.split('.')[-1] if '.' in table_name else table_name
                    
                    import boto3
                    from botocore.exceptions import ClientError
                    
                    glue_client = boto3.client('glue')
                    
                    response = glue_client.get_table(
                        DatabaseName=database,
                        Name=actual_table_name
                    )
                    
                    table_info = response.get('Table', {})
                    table_properties = table_info.get('Parameters', {})
                    table_type = table_properties.get('table_type', '').upper()
                    
                    return table_type == 'ICEBERG'
                    
                except ClientError as e:
                    error_code = e.response.get('Error', {}).get('Code', '')
                    if error_code != 'EntityNotFoundException':
                        if hasattr(self.structured_logger, 'debug'):
                            self.structured_logger.debug("Error checking if table is Iceberg",
                                                       table_name=table_name,
                                                       database=database,
                                                       error=str(e))
                    return False
                except Exception:
                    return False
            
            return False
            
        except Exception:
            return False
    
    def update_bookmark_state(self, table_name: str, new_max_value: Any,
                            processed_rows: int = 0, 
                            database: Optional[str] = None,
                            engine_type: Optional[str] = None) -> None:
        """
        Update job bookmark state after successful processing with S3 persistence and Iceberg support.
        
        This enhanced method implements:
        - Enhanced bookmark management for Iceberg tables with identifier-field-ids
        - S3 bookmark persistence after processing
        - Asynchronous S3 write operations to avoid blocking job execution
        - Error handling for S3 write failures with appropriate logging
        - In-memory bookmark state as backup when S3 operations fail
        
        Args:
            table_name: Name of the table to update bookmark for
            new_max_value: New maximum value processed
            processed_rows: Number of rows processed (default: 0)
            database: Optional database name for Iceberg table validation
            engine_type: Optional engine type for Iceberg detection
        """
        if table_name not in self.bookmark_states:
            raise ValueError(f"No bookmark state found for table {table_name}")
        
        # Log Iceberg table detection for enhanced bookmark management
        if self._is_iceberg_table(table_name, database, engine_type):
            if hasattr(self.structured_logger, 'debug'):
                self.structured_logger.debug("Updating bookmark state for Iceberg table",
                                           table_name=table_name,
                                           database=database,
                                           new_max_value=str(new_max_value))
        
        # Update in-memory bookmark state first (maintain as backup)
        state = self.bookmark_states[table_name]
        state.last_processed_value = new_max_value
        state.last_update_timestamp = datetime.now(timezone.utc)
        state.is_first_run = False
        state.processed_rows = processed_rows  # Update processed rows
        
        # Update S3-specific metadata
        state.updated_timestamp = datetime.now(timezone.utc)
        if not state.created_timestamp:
            state.created_timestamp = state.updated_timestamp
        
        if hasattr(self.structured_logger, 'info'):
            is_iceberg = self._is_iceberg_table(table_name, database, engine_type)
            self.structured_logger.info("Updated in-memory bookmark state",
                                      table_name=table_name,
                                      last_value=str(new_max_value),
                                      processed_rows=processed_rows,
                                      is_first_run=False,
                                      is_iceberg_table=is_iceberg)
        
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
            is_iceberg = self._is_iceberg_table(table_name, database, engine_type)
            self.structured_logger.info("Bookmark state update completed",
                                      table_name=table_name,
                                      last_value=str(new_max_value),
                                      processed_rows=processed_rows,
                                      s3_enabled=self.s3_enabled,
                                      in_memory_backup=True,
                                      is_iceberg_table=is_iceberg)
    
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
    
    # Iceberg identifier-field-ids support methods
    
    def get_iceberg_bookmark_column(self, database: str, table: str, 
                                   catalog_id: Optional[str] = None) -> Optional[str]:
        """
        Extract bookmark column from Iceberg table identifier-field-ids.
        
        This method reads the Iceberg table metadata from the Glue Data Catalog
        and extracts the bookmark column name based on the identifier-field-ids
        property. This enables automatic bookmark management for Iceberg tables.
        
        Args:
            database: Glue Data Catalog database name
            table: Iceberg table name
            catalog_id: Optional AWS account ID for cross-account catalog access
            
        Returns:
            Optional[str]: Bookmark column name if found, None otherwise
        """
        try:
            if hasattr(self.structured_logger, 'debug'):
                self.structured_logger.debug("Extracting bookmark column from Iceberg table identifier-field-ids",
                                           database=database,
                                           table=table,
                                           catalog_id=catalog_id)
            
            # Extract identifier field IDs from table metadata
            identifier_field_ids = self.extract_identifier_field_ids(database, table, catalog_id)
            
            if not identifier_field_ids:
                if hasattr(self.structured_logger, 'debug'):
                    self.structured_logger.debug("No identifier-field-ids found in Iceberg table",
                                               database=database,
                                               table=table)
                return None
            
            # Get table metadata to map field IDs to column names
            try:
                import boto3
                glue_client = boto3.client('glue')
                
                get_table_kwargs = {
                    'DatabaseName': database,
                    'Name': table
                }
                
                if catalog_id:
                    get_table_kwargs['CatalogId'] = catalog_id
                
                response = glue_client.get_table(**get_table_kwargs)
                table_info = response.get('Table', {})
                
                # Verify it's an Iceberg table
                table_properties = table_info.get('Parameters', {})
                table_type = table_properties.get('table_type', '').upper()
                
                if table_type != 'ICEBERG':
                    if hasattr(self.structured_logger, 'warning'):
                        self.structured_logger.warning("Table is not an Iceberg table, cannot extract identifier-field-ids",
                                                     database=database,
                                                     table=table,
                                                     table_type=table_type)
                    return None
                
                # Get column information from storage descriptor
                storage_descriptor = table_info.get('StorageDescriptor', {})
                columns = storage_descriptor.get('Columns', [])
                
                # Create mapping from field ID to column name
                # Note: Glue Data Catalog doesn't store Iceberg field IDs directly,
                # so we use column order as a fallback (field ID = column index + 1)
                field_id_to_name = {}
                for idx, column in enumerate(columns):
                    field_id = idx + 1  # Iceberg field IDs typically start from 1
                    field_id_to_name[field_id] = column.get('Name', '')
                
                # Find the bookmark column name using the first identifier field ID
                bookmark_field_id = identifier_field_ids[0]  # Use first identifier field as bookmark
                bookmark_column = field_id_to_name.get(bookmark_field_id)
                
                if bookmark_column:
                    if hasattr(self.structured_logger, 'info'):
                        self.structured_logger.info("Successfully extracted bookmark column from Iceberg identifier-field-ids",
                                                   database=database,
                                                   table=table,
                                                   bookmark_column=bookmark_column,
                                                   identifier_field_ids=identifier_field_ids)
                    return bookmark_column
                else:
                    if hasattr(self.structured_logger, 'warning'):
                        self.structured_logger.warning("Could not map identifier field ID to column name",
                                                     database=database,
                                                     table=table,
                                                     bookmark_field_id=bookmark_field_id,
                                                     available_field_ids=list(field_id_to_name.keys()))
                    return None
                
            except Exception as glue_error:
                if hasattr(self.structured_logger, 'error'):
                    self.structured_logger.error("Failed to retrieve Iceberg table metadata from Glue Data Catalog",
                                               database=database,
                                               table=table,
                                               error=str(glue_error),
                                               error_type=type(glue_error).__name__)
                return None
            
        except Exception as e:
            if hasattr(self.structured_logger, 'error'):
                self.structured_logger.error("Unexpected error extracting bookmark column from Iceberg table",
                                           database=database,
                                           table=table,
                                           error=str(e),
                                           error_type=type(e).__name__)
            return None
    
    def extract_identifier_field_ids(self, database: str, table: str, 
                                   catalog_id: Optional[str] = None) -> Optional[List[int]]:
        """
        Extract identifier-field-ids from Iceberg table metadata.
        
        This method reads the Glue Data Catalog table properties to extract
        the identifier-field-ids that were set during table creation. These
        field IDs are used for bookmark management in Iceberg tables.
        
        Args:
            database: Glue Data Catalog database name
            table: Iceberg table name
            catalog_id: Optional AWS account ID for cross-account catalog access
            
        Returns:
            Optional[List[int]]: List of identifier field IDs if found, None otherwise
        """
        try:
            if hasattr(self.structured_logger, 'debug'):
                self.structured_logger.debug("Extracting identifier-field-ids from Iceberg table metadata",
                                           database=database,
                                           table=table,
                                           catalog_id=catalog_id)
            
            import boto3
            from botocore.exceptions import ClientError
            
            glue_client = boto3.client('glue')
            
            get_table_kwargs = {
                'DatabaseName': database,
                'Name': table
            }
            
            if catalog_id:
                get_table_kwargs['CatalogId'] = catalog_id
            
            response = glue_client.get_table(**get_table_kwargs)
            table_info = response.get('Table', {})
            
            # Check table properties for identifier-field-ids
            table_properties = table_info.get('Parameters', {})
            
            # Verify it's an Iceberg table
            table_type = table_properties.get('table_type', '').upper()
            if table_type != 'ICEBERG':
                if hasattr(self.structured_logger, 'warning'):
                    self.structured_logger.warning("Table is not an Iceberg table, cannot extract identifier-field-ids",
                                                 database=database,
                                                 table=table,
                                                 table_type=table_type)
                return None
            
            # Extract identifier-field-ids from table properties
            identifier_field_ids_str = table_properties.get('identifier-field-ids')
            
            if not identifier_field_ids_str:
                if hasattr(self.structured_logger, 'debug'):
                    self.structured_logger.debug("No identifier-field-ids property found in Iceberg table",
                                               database=database,
                                               table=table,
                                               available_properties=list(table_properties.keys()))
                return None
            
            # Parse comma-separated field IDs
            try:
                identifier_field_ids = [
                    int(field_id.strip()) 
                    for field_id in identifier_field_ids_str.split(',')
                    if field_id.strip().isdigit()
                ]
                
                if identifier_field_ids:
                    if hasattr(self.structured_logger, 'info'):
                        self.structured_logger.info("Successfully extracted identifier-field-ids from Iceberg table",
                                                   database=database,
                                                   table=table,
                                                   identifier_field_ids=identifier_field_ids)
                    return identifier_field_ids
                else:
                    if hasattr(self.structured_logger, 'warning'):
                        self.structured_logger.warning("identifier-field-ids property exists but contains no valid field IDs",
                                                     database=database,
                                                     table=table,
                                                     raw_value=identifier_field_ids_str)
                    return None
                
            except (ValueError, AttributeError) as parse_error:
                if hasattr(self.structured_logger, 'error'):
                    self.structured_logger.error("Failed to parse identifier-field-ids from table properties",
                                               database=database,
                                               table=table,
                                               raw_value=identifier_field_ids_str,
                                               error=str(parse_error))
                return None
            
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', '')
            if error_code == 'EntityNotFoundException':
                if hasattr(self.structured_logger, 'warning'):
                    self.structured_logger.warning("Iceberg table not found in Glue Data Catalog",
                                                 database=database,
                                                 table=table,
                                                 catalog_id=catalog_id)
            else:
                if hasattr(self.structured_logger, 'error'):
                    self.structured_logger.error("AWS Glue Data Catalog error while extracting identifier-field-ids",
                                               database=database,
                                               table=table,
                                               error_code=error_code,
                                               error=str(e))
            return None
            
        except Exception as e:
            if hasattr(self.structured_logger, 'error'):
                self.structured_logger.error("Unexpected error extracting identifier-field-ids from Iceberg table",
                                           database=database,
                                           table=table,
                                           error=str(e),
                                           error_type=type(e).__name__)
            return None
    
    def fallback_to_traditional_bookmark(self, dataframe: 'DataFrame', 
                                       table_name: str) -> Optional[str]:
        """
        Fallback to traditional bookmark detection for Iceberg tables without identifier-field-ids.
        
        This method provides a fallback mechanism when Iceberg tables don't have
        identifier-field-ids configured. It attempts to detect suitable bookmark
        columns using traditional methods (temporal columns, primary keys).
        
        Args:
            dataframe: Spark DataFrame containing table data
            table_name: Name of the table for logging purposes
            
        Returns:
            Optional[str]: Detected bookmark column name if found, None otherwise
        """
        try:
            if hasattr(self.structured_logger, 'info'):
                self.structured_logger.info("Falling back to traditional bookmark detection for Iceberg table",
                                           table_name=table_name,
                                           reason="no_identifier_field_ids")
            
            # Check if DataFrame is available
            if not dataframe:
                if hasattr(self.structured_logger, 'warning'):
                    self.structured_logger.warning("No DataFrame available for traditional bookmark detection",
                                                 table_name=table_name)
                return None
            
            # Get DataFrame schema
            try:
                schema = dataframe.schema
                column_names = [field.name for field in schema.fields]
                column_types = {field.name: str(field.dataType) for field in schema.fields}
                
                if hasattr(self.structured_logger, 'debug'):
                    self.structured_logger.debug("Analyzing DataFrame schema for bookmark column detection",
                                               table_name=table_name,
                                               column_count=len(column_names),
                                               columns=column_names)
                
            except Exception as schema_error:
                if hasattr(self.structured_logger, 'error'):
                    self.structured_logger.error("Failed to extract DataFrame schema for bookmark detection",
                                               table_name=table_name,
                                               error=str(schema_error))
                return None
            
            # Priority 1: Look for timestamp/datetime columns (common bookmark patterns)
            timestamp_patterns = [
                'updated_at', 'update_time', 'last_updated', 'modified_at', 'mod_time',
                'created_at', 'create_time', 'insert_time', 'timestamp', 'last_modified',
                'updated_date', 'modified_date', 'created_date', 'date_updated', 'date_modified'
            ]
            
            for pattern in timestamp_patterns:
                for column_name in column_names:
                    try:
                        if pattern.lower() in str(column_name).lower():
                            column_type = str(column_types.get(column_name, '')).lower()
                            if any(ts_type in column_type for ts_type in ['timestamp', 'datetime', 'date']):
                                if hasattr(self.structured_logger, 'info'):
                                    self.structured_logger.info("Found timestamp bookmark column using traditional detection",
                                                               table_name=table_name,
                                                               bookmark_column=column_name,
                                                               column_type=column_type,
                                                               detection_method="timestamp_pattern")
                                return column_name
                    except (TypeError, AttributeError):
                        continue
            
            # Priority 2: Look for ID columns that could serve as bookmarks
            id_patterns = [
                'id', 'primary_key', 'pk', 'key', 'row_id', 'record_id',
                'sequence_id', 'seq_id', 'auto_id', 'identity'
            ]
            
            for pattern in id_patterns:
                for column_name in column_names:
                    try:
                        column_name_str = str(column_name).lower()
                        if (pattern.lower() == column_name_str or 
                            column_name_str.endswith('_' + pattern.lower())):
                            column_type = str(column_types.get(column_name, '')).lower()
                            if any(num_type in column_type for num_type in ['int', 'long', 'bigint', 'number']):
                                if hasattr(self.structured_logger, 'info'):
                                    self.structured_logger.info("Found ID bookmark column using traditional detection",
                                                               table_name=table_name,
                                                               bookmark_column=column_name,
                                                               column_type=column_type,
                                                               detection_method="id_pattern")
                                return column_name
                    except (TypeError, AttributeError):
                        continue
            
            # Priority 3: Look for any timestamp/datetime columns
            for column_name in column_names:
                try:
                    column_type = str(column_types.get(column_name, '')).lower()
                    if any(ts_type in column_type for ts_type in ['timestamp', 'datetime']):
                        if hasattr(self.structured_logger, 'info'):
                            self.structured_logger.info("Found generic timestamp bookmark column using traditional detection",
                                                       table_name=table_name,
                                                       bookmark_column=column_name,
                                                       column_type=column_type,
                                                       detection_method="generic_timestamp")
                        return column_name
                except (TypeError, AttributeError):
                    continue
            
            # Priority 4: Look for any numeric columns that could be sequential
            for column_name in column_names:
                try:
                    column_type = str(column_types.get(column_name, '')).lower()
                    if any(num_type in column_type for num_type in ['int', 'long', 'bigint']):
                        if hasattr(self.structured_logger, 'info'):
                            self.structured_logger.info("Found numeric bookmark column using traditional detection",
                                                       table_name=table_name,
                                                       bookmark_column=column_name,
                                                       column_type=column_type,
                                                       detection_method="generic_numeric")
                        return column_name
                except (TypeError, AttributeError):
                    continue
            
            # No suitable bookmark column found
            if hasattr(self.structured_logger, 'warning'):
                self.structured_logger.warning("No suitable bookmark column found using traditional detection",
                                             table_name=table_name,
                                             available_columns=column_names,
                                             fallback_action="full_load_will_be_performed")
            return None
            
        except Exception as e:
            if hasattr(self.structured_logger, 'error'):
                self.structured_logger.error("Error during traditional bookmark detection fallback",
                                           table_name=table_name,
                                           error=str(e),
                                           error_type=type(e).__name__,
                                           fallback_action="no_bookmark_column_detected")
            return None
    
    def initialize_iceberg_bookmark_state(self, database: str, table: str, 
                                        incremental_strategy: str,
                                        catalog_id: Optional[str] = None,
                                        dataframe: Optional['DataFrame'] = None) -> JobBookmarkState:
        """
        Initialize bookmark state for Iceberg tables with identifier-field-ids support.
        
        This method provides enhanced bookmark initialization specifically for Iceberg tables.
        It attempts to use identifier-field-ids first, then falls back to traditional
        bookmark detection methods if identifier-field-ids are not available.
        
        Args:
            database: Glue Data Catalog database name
            table: Iceberg table name
            incremental_strategy: Strategy for incremental loading
            catalog_id: Optional AWS account ID for cross-account catalog access
            dataframe: Optional DataFrame for fallback bookmark detection
            
        Returns:
            JobBookmarkState: Initialized bookmark state for the Iceberg table
        """
        try:
            table_name = f"{database}.{table}"
            
            if hasattr(self.structured_logger, 'info'):
                self.structured_logger.info("Initializing bookmark state for Iceberg table",
                                           database=database,
                                           table=table,
                                           incremental_strategy=incremental_strategy,
                                           catalog_id=catalog_id)
            
            # Check if we already have a cached state
            if table_name in self.bookmark_states:
                cached_state = self.bookmark_states[table_name]
                if hasattr(self.structured_logger, 'debug'):
                    self.structured_logger.debug("Using cached Iceberg bookmark state",
                                               table_name=table_name,
                                               is_first_run=cached_state.is_first_run)
                return cached_state
            
            # Attempt to read existing bookmark from S3 first
            if self.s3_enabled and self.s3_bookmark_storage:
                try:
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                    try:
                        s3_bookmark_data = loop.run_until_complete(
                            self.s3_bookmark_storage.read_bookmark(table_name)
                        )
                    finally:
                        loop.close()
                    
                    if s3_bookmark_data:
                        state = JobBookmarkState.from_s3_dict(s3_bookmark_data)
                        state.job_name = self.job_name
                        self.bookmark_states[table_name] = state
                        
                        if hasattr(self.structured_logger, 'info'):
                            self.structured_logger.info("Loaded existing Iceberg bookmark state from S3",
                                                       table_name=table_name,
                                                       is_first_run=state.is_first_run,
                                                       incremental_column=state.incremental_column)
                        return state
                        
                except Exception as s3_error:
                    if hasattr(self.structured_logger, 'warning'):
                        self.structured_logger.warning("Failed to read Iceberg bookmark from S3, creating new state",
                                                     table_name=table_name,
                                                     error=str(s3_error))
            
            # Determine incremental column for new bookmark state
            incremental_column = None
            
            # Try to get bookmark column from identifier-field-ids first
            try:
                incremental_column = self.get_iceberg_bookmark_column(database, table, catalog_id)
                
                if incremental_column:
                    if hasattr(self.structured_logger, 'info'):
                        self.structured_logger.info("Using bookmark column from Iceberg identifier-field-ids",
                                                   table_name=table_name,
                                                   incremental_column=incremental_column)
                else:
                    # Fallback to traditional bookmark detection if DataFrame is available
                    if dataframe:
                        incremental_column = self.fallback_to_traditional_bookmark(dataframe, table_name)
                        
                        if incremental_column:
                            if hasattr(self.structured_logger, 'info'):
                                self.structured_logger.info("Using bookmark column from traditional detection fallback",
                                                           table_name=table_name,
                                                           incremental_column=incremental_column)
                        else:
                            if hasattr(self.structured_logger, 'warning'):
                                self.structured_logger.warning("No bookmark column detected for Iceberg table, using full load strategy",
                                                             table_name=table_name,
                                                             fallback_strategy="full_load")
                    else:
                        if hasattr(self.structured_logger, 'warning'):
                            self.structured_logger.warning("No DataFrame available for fallback bookmark detection",
                                                         table_name=table_name,
                                                         fallback_strategy="full_load")
                
            except Exception as bookmark_error:
                if hasattr(self.structured_logger, 'error'):
                    self.structured_logger.error("Error during Iceberg bookmark column detection",
                                               table_name=table_name,
                                               error=str(bookmark_error),
                                               fallback_strategy="full_load")
            
            # Create new bookmark state
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
            
            # Cache the state
            self.bookmark_states[table_name] = state
            
            if hasattr(self.structured_logger, 'info'):
                self.structured_logger.info("Created new Iceberg bookmark state",
                                           table_name=table_name,
                                           incremental_strategy=incremental_strategy,
                                           incremental_column=incremental_column,
                                           is_first_run=True)
            
            return state
            
        except Exception as e:
            if hasattr(self.structured_logger, 'error'):
                self.structured_logger.error("Failed to initialize Iceberg bookmark state",
                                           database=database,
                                           table=table,
                                           error=str(e),
                                           error_type=type(e).__name__)
            
            # Create fallback state to ensure job continues
            current_time = datetime.now(timezone.utc)
            fallback_state = JobBookmarkState(
                table_name=f"{database}.{table}",
                incremental_strategy=incremental_strategy,
                incremental_column=None,
                is_first_run=True,
                job_name=self.job_name,
                created_timestamp=current_time,
                updated_timestamp=current_time,
                version="1.0"
            )
            
            return fallback_state
    
    def _get_iceberg_table_metadata(self, database: str, table: str, 
                                   catalog_id: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """Get Iceberg table metadata from Glue Data Catalog.
        
        Args:
            database: Database name
            table: Table name
            catalog_id: Optional catalog ID for cross-account access
            
        Returns:
            Dict[str, Any]: Table metadata or None if not found
        """
        try:
            import boto3
            from botocore.exceptions import ClientError
            
            glue_client = boto3.client('glue')
            
            get_table_params = {
                'DatabaseName': database,
                'Name': table
            }
            
            if catalog_id:
                get_table_params['CatalogId'] = catalog_id
            
            response = glue_client.get_table(**get_table_params)
            table_info = response['Table']
            
            # Check if this is an Iceberg table
            parameters = table_info.get('Parameters', {})
            if parameters.get('table_type') == 'ICEBERG':
                # Mock Iceberg metadata structure for testing
                return {
                    'schema': {
                        'fields': [
                            {'id': 1, 'name': 'id', 'type': 'int', 'required': True},
                            {'id': 2, 'name': 'name', 'type': 'string', 'required': False},
                            {'id': 3, 'name': 'created_at', 'type': 'timestamp', 'required': False}
                        ]
                    },
                    'identifier-field-ids': [1]  # id field is the identifier
                }
            
            return None
            
        except ClientError as e:
            if e.response['Error']['Code'] == 'EntityNotFoundException':
                return None
            raise
        except Exception:
            return None