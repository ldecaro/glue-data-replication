"""
Comprehensive unit tests for enhanced JobBookmarkManager class.

This test suite covers:
- S3 bucket extraction from JDBC driver paths (Requirements 2.1, 2.2)
- Bookmark state transitions from first run to incremental loading (Requirement 4.3)
- Fallback mechanisms to in-memory bookmarks (Requirement 5.4)
- Integration between JobBookmarkManager and S3BookmarkStorage

Requirements covered: 2.1, 2.2, 4.3, 5.4
"""

import asyncio
import json
import pytest
import time
from datetime import datetime, timezone
from unittest.mock import Mock, AsyncMock, patch, MagicMock, call
from botocore.exceptions import ClientError, ConnectTimeoutError, ReadTimeoutError

# Import the classes we're testing
import sys
import os



# Add src directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'src'))

# Mock PySpark and AWS Glue imports for testing
mock_modules = [
    'awsglue', 'awsglue.utils', 'awsglue.context', 'awsglue.job',
    'pyspark', 'pyspark.context', 'pyspark.sql', 'pyspark.sql.types',
    'pyspark.sql.functions', 'boto3'
]

for module in mock_modules:
    sys.modules[module] = MagicMock()

try:
    from glue_job.storage import (
        JobBookmarkManager, 
        JobBookmarkState, 
        S3BookmarkStorage, 
        S3BookmarkConfig
    )
    from glue_job.monitoring import StructuredLogger
except ImportError as e:
    print(f"Import error: {e}")
    raise


class TestJobBookmarkManagerS3BucketExtraction:
    """Test S3 bucket extraction from JDBC driver paths (Requirements 2.1, 2.2)."""
    
    @patch('glue_data_replication.S3PathUtilities')
    def test_extract_s3_bucket_from_source_jdbc_path(self, mock_s3_utils):
        """Test extracting S3 bucket from source JDBC driver path."""
        # Arrange
        mock_glue_context = Mock()
        mock_s3_utils.detect_s3_bucket_from_jdbc_paths.return_value = "test-bucket"
        mock_s3_utils.validate_s3_bucket_accessibility.return_value = True
        
        source_jdbc_path = "s3://test-bucket/drivers/oracle-driver.jar"
        target_jdbc_path = "s3://other-bucket/drivers/postgres-driver.jar"
        
        # Act
        with patch('glue_data_replication.S3BookmarkStorage'):
            manager = JobBookmarkManager(
                glue_context=mock_glue_context,
                job_name="test-job",
                source_jdbc_path=source_jdbc_path,
                target_jdbc_path=target_jdbc_path
            )
        
        # Assert
        mock_s3_utils.detect_s3_bucket_from_jdbc_paths.assert_called_once_with(
            source_jdbc_path, target_jdbc_path, manager.structured_logger
        )
        assert manager.s3_enabled is True
        assert manager.s3_bookmark_storage is not None
    
    @patch('glue_data_replication.S3PathUtilities')
    def test_extract_s3_bucket_from_target_jdbc_path_only(self, mock_s3_utils):
        """Test extracting S3 bucket when only target JDBC path is provided."""
        # Arrange
        mock_glue_context = Mock()
        mock_s3_utils.detect_s3_bucket_from_jdbc_paths.return_value = "target-bucket"
        mock_s3_utils.validate_s3_bucket_accessibility.return_value = True
        
        target_jdbc_path = "s3://target-bucket/drivers/sqlserver-driver.jar"
        
        # Act
        with patch('glue_data_replication.S3BookmarkStorage'):
            manager = JobBookmarkManager(
                glue_context=mock_glue_context,
                job_name="test-job",
                source_jdbc_path=None,
                target_jdbc_path=target_jdbc_path
            )
        
        # Assert
        mock_s3_utils.detect_s3_bucket_from_jdbc_paths.assert_called_once_with(
            "", target_jdbc_path, manager.structured_logger
        )
        assert manager.s3_enabled is True
    
    @patch('glue_data_replication.S3PathUtilities')
    def test_s3_bucket_extraction_failure_fallback_to_memory(self, mock_s3_utils):
        """Test fallback to in-memory bookmarks when S3 bucket extraction fails."""
        # Arrange
        mock_glue_context = Mock()
        mock_s3_utils.detect_s3_bucket_from_jdbc_paths.side_effect = ValueError("Invalid S3 path")
        
        source_jdbc_path = "invalid-path"
        
        # Act
        manager = JobBookmarkManager(
            glue_context=mock_glue_context,
            job_name="test-job",
            source_jdbc_path=source_jdbc_path
        )
        
        # Assert
        assert manager.s3_enabled is False
        assert manager.s3_bookmark_storage is None
        assert manager.bookmark_states == {}
    
    @patch('glue_data_replication.S3PathUtilities')
    def test_s3_bucket_not_accessible_fallback_to_memory(self, mock_s3_utils):
        """Test fallback to in-memory bookmarks when S3 bucket is not accessible."""
        # Arrange
        mock_glue_context = Mock()
        mock_s3_utils.detect_s3_bucket_from_jdbc_paths.return_value = "inaccessible-bucket"
        mock_s3_utils.validate_s3_bucket_accessibility.return_value = False
        
        source_jdbc_path = "s3://inaccessible-bucket/drivers/oracle-driver.jar"
        
        # Act
        manager = JobBookmarkManager(
            glue_context=mock_glue_context,
            job_name="test-job",
            source_jdbc_path=source_jdbc_path
        )
        
        # Assert
        mock_s3_utils.validate_s3_bucket_accessibility.assert_called_once()
        assert manager.s3_enabled is False
        assert manager.s3_bookmark_storage is None
    
    def test_no_jdbc_paths_provided_uses_memory_bookmarks(self):
        """Test that manager uses in-memory bookmarks when no JDBC paths are provided."""
        # Arrange
        mock_glue_context = Mock()
        
        # Act
        manager = JobBookmarkManager(
            glue_context=mock_glue_context,
            job_name="test-job"
        )
        
        # Assert
        assert manager.s3_enabled is False
        assert manager.s3_bookmark_storage is None
        assert manager.bookmark_states == {}


class TestJobBookmarkManagerStateTransitions:
    """Test bookmark state transitions from first run to incremental loading (Requirement 4.3)."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.mock_glue_context = Mock()
        self.job_name = "test-job"
        self.table_name = "test_table"
        
        # Create manager with in-memory bookmarks for basic tests
        self.manager = JobBookmarkManager(
            glue_context=self.mock_glue_context,
            job_name=self.job_name
        )
    
    def test_initialize_bookmark_state_first_run(self):
        """Test initializing bookmark state for first run."""
        # Act
        state = self.manager.initialize_bookmark_state(
            table_name=self.table_name,
            incremental_strategy="timestamp",
            incremental_column="updated_at"
        )
        
        # Assert
        assert state.table_name == self.table_name
        assert state.incremental_strategy == "timestamp"
        assert state.incremental_column == "updated_at"
        assert state.is_first_run is True
        assert state.last_processed_value is None
        assert state.job_name == self.job_name
        assert state.created_timestamp is not None
        assert state.updated_timestamp is not None
        
        # Verify state is cached
        assert self.table_name in self.manager.bookmark_states
        assert self.manager.bookmark_states[self.table_name] == state
    
    def test_initialize_bookmark_state_returns_cached_state(self):
        """Test that initialize_bookmark_state returns cached state on subsequent calls."""
        # Arrange - Initialize state first time
        original_state = self.manager.initialize_bookmark_state(
            table_name=self.table_name,
            incremental_strategy="timestamp",
            incremental_column="updated_at"
        )
        
        # Act - Initialize again
        cached_state = self.manager.initialize_bookmark_state(
            table_name=self.table_name,
            incremental_strategy="primary_key",  # Different strategy
            incremental_column="id"  # Different column
        )
        
        # Assert - Should return original cached state, not create new one
        assert cached_state is original_state
        assert cached_state.incremental_strategy == "timestamp"  # Original strategy
        assert cached_state.incremental_column == "updated_at"  # Original column
    
    def test_update_bookmark_state_transitions_to_incremental(self):
        """Test that updating bookmark state transitions from first run to incremental."""
        # Arrange
        state = self.manager.initialize_bookmark_state(
            table_name=self.table_name,
            incremental_strategy="timestamp",
            incremental_column="updated_at"
        )
        assert state.is_first_run is True
        
        # Act
        new_max_value = "2024-01-15T10:30:00Z"
        self.manager.update_bookmark_state(
            table_name=self.table_name,
            new_max_value=new_max_value,
            processed_rows=100
        )
        
        # Assert
        updated_state = self.manager.bookmark_states[self.table_name]
        assert updated_state.is_first_run is False
        assert updated_state.last_processed_value == new_max_value
        assert updated_state.last_update_timestamp is not None
        assert updated_state.updated_timestamp is not None
    
    @patch('glue_data_replication.S3PathUtilities')
    @patch('asyncio.new_event_loop')
    def test_initialize_with_s3_bookmark_existing_state(self, mock_event_loop, mock_s3_utils):
        """Test initializing bookmark state when S3 bookmark exists (incremental loading)."""
        # Arrange
        mock_s3_utils.detect_s3_bucket_from_jdbc_paths.return_value = "test-bucket"
        mock_s3_utils.validate_s3_bucket_accessibility.return_value = True
        
        # Mock S3 bookmark data
        s3_bookmark_data = {
            "table_name": self.table_name,
            "incremental_strategy": "timestamp",
            "incremental_column": "updated_at",
            "last_processed_value": "2024-01-15T10:30:00Z",
            "last_update_timestamp": "2024-01-15T11:00:00Z",
            "is_first_run": False,
            "job_name": "previous-job",
            "created_timestamp": "2024-01-01T09:00:00Z",
            "updated_timestamp": "2024-01-15T11:00:00Z",
            "version": "1.0"
        }
        
        # Mock event loop and S3 operations
        mock_loop = Mock()
        mock_event_loop.return_value = mock_loop
        mock_loop.run_until_complete.return_value = s3_bookmark_data
        
        with patch('glue_data_replication.S3BookmarkStorage') as mock_storage_class:
            mock_storage = Mock()
            mock_storage_class.return_value = mock_storage
            
            # Create manager with S3 enabled
            manager = JobBookmarkManager(
                glue_context=self.mock_glue_context,
                job_name=self.job_name,
                source_jdbc_path="s3://test-bucket/drivers/oracle.jar"
            )
            
            # Act
            state = manager.initialize_bookmark_state(
                table_name=self.table_name,
                incremental_strategy="timestamp",
                incremental_column="updated_at"
            )
        
        # Assert
        assert state.table_name == self.table_name
        assert state.is_first_run is False
        assert state.last_processed_value == "2024-01-15T10:30:00Z"
        assert state.job_name == self.job_name  # Should be updated to current job
        mock_loop.run_until_complete.assert_called_once()
    
    @patch('glue_data_replication.S3PathUtilities')
    @patch('asyncio.new_event_loop')
    def test_initialize_with_s3_bookmark_not_found_first_run(self, mock_event_loop, mock_s3_utils):
        """Test initializing bookmark state when S3 bookmark doesn't exist (first run)."""
        # Arrange
        mock_s3_utils.detect_s3_bucket_from_jdbc_paths.return_value = "test-bucket"
        mock_s3_utils.validate_s3_bucket_accessibility.return_value = True
        
        # Mock event loop returning None (bookmark not found)
        mock_loop = Mock()
        mock_event_loop.return_value = mock_loop
        mock_loop.run_until_complete.return_value = None
        
        with patch('glue_data_replication.S3BookmarkStorage') as mock_storage_class:
            mock_storage = Mock()
            mock_storage_class.return_value = mock_storage
            
            # Create manager with S3 enabled
            manager = JobBookmarkManager(
                glue_context=self.mock_glue_context,
                job_name=self.job_name,
                source_jdbc_path="s3://test-bucket/drivers/oracle.jar"
            )
            
            # Act
            state = manager.initialize_bookmark_state(
                table_name=self.table_name,
                incremental_strategy="timestamp",
                incremental_column="updated_at"
            )
        
        # Assert
        assert state.table_name == self.table_name
        assert state.is_first_run is True
        assert state.last_processed_value is None
        assert state.job_name == self.job_name
        mock_loop.run_until_complete.assert_called_once()


class TestJobBookmarkManagerFallbackMechanisms:
    """Test fallback mechanisms to in-memory bookmarks (Requirement 5.4)."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.mock_glue_context = Mock()
        self.job_name = "test-job"
        self.table_name = "test_table"
    
    @patch('glue_data_replication.S3PathUtilities')
    @patch('asyncio.new_event_loop')
    def test_s3_read_failure_fallback_to_memory(self, mock_event_loop, mock_s3_utils):
        """Test fallback to in-memory bookmarks when S3 read fails."""
        # Arrange
        mock_s3_utils.detect_s3_bucket_from_jdbc_paths.return_value = "test-bucket"
        mock_s3_utils.validate_s3_bucket_accessibility.return_value = True
        
        # Mock event loop raising exception
        mock_loop = Mock()
        mock_event_loop.return_value = mock_loop
        mock_loop.run_until_complete.side_effect = ClientError(
            error_response={'Error': {'Code': 'AccessDenied', 'Message': 'Access Denied'}},
            operation_name='GetObject'
        )
        
        with patch('glue_data_replication.S3BookmarkStorage') as mock_storage_class:
            mock_storage = Mock()
            mock_storage_class.return_value = mock_storage
            
            # Create manager with S3 enabled
            manager = JobBookmarkManager(
                glue_context=self.mock_glue_context,
                job_name=self.job_name,
                source_jdbc_path="s3://test-bucket/drivers/oracle.jar"
            )
            
            # Act
            state = manager.initialize_bookmark_state(
                table_name=self.table_name,
                incremental_strategy="timestamp",
                incremental_column="updated_at"
            )
        
        # Assert - Should create new state (fallback to memory)
        assert state.table_name == self.table_name
        assert state.is_first_run is True
        assert state.last_processed_value is None
        assert state.job_name == self.job_name
    
    @patch('glue_data_replication.S3PathUtilities')
    @patch('asyncio.new_event_loop')
    def test_s3_timeout_fallback_to_memory(self, mock_event_loop, mock_s3_utils):
        """Test fallback to in-memory bookmarks when S3 operations timeout."""
        # Arrange
        mock_s3_utils.detect_s3_bucket_from_jdbc_paths.return_value = "test-bucket"
        mock_s3_utils.validate_s3_bucket_accessibility.return_value = True
        
        # Mock event loop raising timeout exception
        mock_loop = Mock()
        mock_event_loop.return_value = mock_loop
        mock_loop.run_until_complete.side_effect = ReadTimeoutError(
            endpoint_url="https://s3.amazonaws.com"
        )
        
        with patch('glue_data_replication.S3BookmarkStorage') as mock_storage_class:
            mock_storage = Mock()
            mock_storage_class.return_value = mock_storage
            
            # Create manager with S3 enabled
            manager = JobBookmarkManager(
                glue_context=self.mock_glue_context,
                job_name=self.job_name,
                source_jdbc_path="s3://test-bucket/drivers/oracle.jar"
            )
            
            # Act
            state = manager.initialize_bookmark_state(
                table_name=self.table_name,
                incremental_strategy="timestamp",
                incremental_column="updated_at"
            )
        
        # Assert - Should create new state (fallback to memory)
        assert state.table_name == self.table_name
        assert state.is_first_run is True
        assert state.last_processed_value is None
    
    def test_unexpected_error_fallback_to_memory(self):
        """Test fallback to in-memory bookmarks when unexpected errors occur."""
        # Arrange
        manager = JobBookmarkManager(
            glue_context=self.mock_glue_context,
            job_name=self.job_name
        )
        
        # Mock the bookmark_states to raise an exception
        with patch.object(manager, 'bookmark_states', side_effect=Exception("Unexpected error")):
            # Act
            state = manager.initialize_bookmark_state(
                table_name=self.table_name,
                incremental_strategy="timestamp",
                incremental_column="updated_at"
            )
        
        # Assert - Should create fallback state
        assert state.table_name == self.table_name
        assert state.is_first_run is True
        assert state.last_processed_value is None
        assert state.job_name == self.job_name
    
    @patch('glue_data_replication.S3PathUtilities')
    @patch('asyncio.new_event_loop')
    def test_s3_write_failure_continues_execution(self, mock_event_loop, mock_s3_utils):
        """Test that S3 write failures don't prevent job execution."""
        # Arrange
        mock_s3_utils.detect_s3_bucket_from_jdbc_paths.return_value = "test-bucket"
        mock_s3_utils.validate_s3_bucket_accessibility.return_value = True
        
        # Mock successful read but failed write
        mock_loop = Mock()
        mock_event_loop.return_value = mock_loop
        mock_loop.run_until_complete.side_effect = [
            None,  # First call (read) returns None
            False  # Second call (write) returns False (failure)
        ]
        
        with patch('glue_data_replication.S3BookmarkStorage') as mock_storage_class:
            mock_storage = Mock()
            mock_storage_class.return_value = mock_storage
            
            # Create manager with S3 enabled
            manager = JobBookmarkManager(
                glue_context=self.mock_glue_context,
                job_name=self.job_name,
                source_jdbc_path="s3://test-bucket/drivers/oracle.jar"
            )
            
            # Initialize state
            state = manager.initialize_bookmark_state(
                table_name=self.table_name,
                incremental_strategy="timestamp",
                incremental_column="updated_at"
            )
            
            # Act - Update bookmark state (should not fail even if S3 write fails)
            manager.update_bookmark_state(
                table_name=self.table_name,
                new_max_value="2024-01-15T10:30:00Z",
                processed_rows=100
            )
        
        # Assert - State should be updated in memory even if S3 write failed
        updated_state = manager.bookmark_states[self.table_name]
        assert updated_state.is_first_run is False
        assert updated_state.last_processed_value == "2024-01-15T10:30:00Z"


class TestJobBookmarkManagerS3Integration:
    """Test integration between JobBookmarkManager and S3BookmarkStorage."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.mock_glue_context = Mock()
        self.job_name = "test-job"
        self.table_name = "test_table"
    
    @patch('glue_data_replication.S3PathUtilities')
    def test_s3_bookmark_storage_initialization(self, mock_s3_utils):
        """Test proper initialization of S3BookmarkStorage."""
        # Arrange
        mock_s3_utils.detect_s3_bucket_from_jdbc_paths.return_value = "test-bucket"
        mock_s3_utils.validate_s3_bucket_accessibility.return_value = True
        
        source_jdbc_path = "s3://test-bucket/drivers/oracle-driver.jar"
        
        with patch('glue_data_replication.S3BookmarkStorage') as mock_storage_class:
            mock_storage = Mock()
            mock_storage_class.return_value = mock_storage
            
            # Act
            manager = JobBookmarkManager(
                glue_context=self.mock_glue_context,
                job_name=self.job_name,
                source_jdbc_path=source_jdbc_path
            )
        
        # Assert
        mock_storage_class.assert_called_once()
        call_args = mock_storage_class.call_args[0][0]  # First positional argument (S3BookmarkConfig)
        assert call_args.bucket_name == "test-bucket"
        assert call_args.bookmark_prefix == "bookmarks/"
        assert call_args.job_name == self.job_name
        assert call_args.retry_attempts == 3
        assert call_args.timeout_seconds == 30
        
        assert manager.s3_bookmark_storage == mock_storage
        assert manager.s3_enabled is True
    
    @patch('glue_data_replication.S3PathUtilities')
    @patch('asyncio.new_event_loop')
    def test_s3_bookmark_read_integration(self, mock_event_loop, mock_s3_utils):
        """Test integration with S3BookmarkStorage for reading bookmarks."""
        # Arrange
        mock_s3_utils.detect_s3_bucket_from_jdbc_paths.return_value = "test-bucket"
        mock_s3_utils.validate_s3_bucket_accessibility.return_value = True
        
        # Mock S3 bookmark data
        s3_bookmark_data = {
            "table_name": self.table_name,
            "incremental_strategy": "timestamp",
            "incremental_column": "updated_at",
            "last_processed_value": "2024-01-15T10:30:00Z",
            "is_first_run": False,
            "job_name": "previous-job",
            "version": "1.0"
        }
        
        # Mock event loop
        mock_loop = Mock()
        mock_event_loop.return_value = mock_loop
        mock_loop.run_until_complete.return_value = s3_bookmark_data
        
        with patch('glue_data_replication.S3BookmarkStorage') as mock_storage_class:
            mock_storage = Mock()
            mock_storage_class.return_value = mock_storage
            
            # Create manager
            manager = JobBookmarkManager(
                glue_context=self.mock_glue_context,
                job_name=self.job_name,
                source_jdbc_path="s3://test-bucket/drivers/oracle.jar"
            )
            
            # Act
            state = manager.initialize_bookmark_state(
                table_name=self.table_name,
                incremental_strategy="timestamp",
                incremental_column="updated_at"
            )
        
        # Assert
        mock_loop.run_until_complete.assert_called_once()
        assert state.last_processed_value == "2024-01-15T10:30:00Z"
        assert state.is_first_run is False
        assert state.job_name == self.job_name  # Should be updated to current job
    
    @patch('glue_data_replication.S3PathUtilities')
    @patch('asyncio.new_event_loop')
    def test_s3_bookmark_write_integration(self, mock_event_loop, mock_s3_utils):
        """Test integration with S3BookmarkStorage for writing bookmarks."""
        # Arrange
        mock_s3_utils.detect_s3_bucket_from_jdbc_paths.return_value = "test-bucket"
        mock_s3_utils.validate_s3_bucket_accessibility.return_value = True
        
        # Mock event loop
        mock_loop = Mock()
        mock_event_loop.return_value = mock_loop
        mock_loop.run_until_complete.side_effect = [
            None,  # First call (read) returns None
            True   # Second call (write) returns True (success)
        ]
        
        with patch('glue_data_replication.S3BookmarkStorage') as mock_storage_class:
            mock_storage = Mock()
            mock_storage_class.return_value = mock_storage
            
            # Create manager
            manager = JobBookmarkManager(
                glue_context=self.mock_glue_context,
                job_name=self.job_name,
                source_jdbc_path="s3://test-bucket/drivers/oracle.jar"
            )
            
            # Initialize state
            state = manager.initialize_bookmark_state(
                table_name=self.table_name,
                incremental_strategy="timestamp",
                incremental_column="updated_at"
            )
            
            # Act
            manager.update_bookmark_state(
                table_name=self.table_name,
                new_max_value="2024-01-15T10:30:00Z",
                processed_rows=100
            )
        
        # Assert
        assert mock_loop.run_until_complete.call_count == 2
        # Verify state was updated in memory
        updated_state = manager.bookmark_states[self.table_name]
        assert updated_state.last_processed_value == "2024-01-15T10:30:00Z"
        assert updated_state.is_first_run is False
    
    @patch('glue_data_replication.S3PathUtilities')
    def test_enhanced_parallel_operations_initialization(self, mock_s3_utils):
        """Test initialization of enhanced parallel operations for Task 11."""
        # Arrange
        mock_s3_utils.detect_s3_bucket_from_jdbc_paths.return_value = "test-bucket"
        mock_s3_utils.validate_s3_bucket_accessibility.return_value = True
        
        with patch('glue_data_replication.S3BookmarkStorage') as mock_storage_class:
            with patch('glue_data_replication.EnhancedS3ParallelOperations') as mock_parallel_class:
                mock_storage = Mock()
                mock_storage_class.return_value = mock_storage
                mock_parallel = Mock()
                mock_parallel_class.return_value = mock_parallel
                
                # Act
                manager = JobBookmarkManager(
                    glue_context=self.mock_glue_context,
                    job_name=self.job_name,
                    source_jdbc_path="s3://test-bucket/drivers/oracle.jar"
                )
        
        # Assert
        mock_parallel_class.assert_called_once_with(mock_storage)
        assert manager.enhanced_parallel_ops == mock_parallel


class TestJobBookmarkManagerErrorHandling:
    """Test error handling scenarios in JobBookmarkManager."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.mock_glue_context = Mock()
        self.job_name = "test-job"
        self.table_name = "test_table"
    
    def test_invalid_table_name_handling(self):
        """Test handling of invalid table names."""
        # Arrange
        manager = JobBookmarkManager(
            glue_context=self.mock_glue_context,
            job_name=self.job_name
        )
        
        # Act & Assert - Should not raise exception
        state = manager.initialize_bookmark_state(
            table_name="",  # Empty table name
            incremental_strategy="timestamp",
            incremental_column="updated_at"
        )
        
        assert state.table_name == ""
        assert state.is_first_run is True
    
    def test_invalid_incremental_strategy_handling(self):
        """Test handling of invalid incremental strategies."""
        # Arrange
        manager = JobBookmarkManager(
            glue_context=self.mock_glue_context,
            job_name=self.job_name
        )
        
        # Act & Assert - Should not raise exception
        state = manager.initialize_bookmark_state(
            table_name=self.table_name,
            incremental_strategy="invalid_strategy",
            incremental_column="updated_at"
        )
        
        assert state.incremental_strategy == "invalid_strategy"
        assert state.is_first_run is True
    
    def test_update_nonexistent_bookmark_state(self):
        """Test updating bookmark state for non-existent table."""
        # Arrange
        manager = JobBookmarkManager(
            glue_context=self.mock_glue_context,
            job_name=self.job_name
        )
        
        # Act & Assert - Should not raise exception
        manager.update_bookmark_state(
            table_name="nonexistent_table",
            new_max_value="2024-01-15T10:30:00Z",
            processed_rows=100
        )
        
        # Should create new state for the table
        assert "nonexistent_table" in manager.bookmark_states
        state = manager.bookmark_states["nonexistent_table"]
        assert state.last_processed_value == "2024-01-15T10:30:00Z"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])