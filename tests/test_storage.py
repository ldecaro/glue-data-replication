#!/usr/bin/env python3
"""
Unit tests for storage modules.

This test suite covers:
- S3BookmarkConfig and S3BookmarkStorage functionality
- JobBookmarkManager and JobBookmarkState management
- Progress tracking classes
"""

import unittest
from unittest.mock import Mock, MagicMock, patch, call
import sys
import os
import json
from datetime import datetime, timezone
from botocore.exceptions import ClientError

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

# Import the classes to test from new modular structure
from glue_job.storage import (
    S3BookmarkConfig, S3BookmarkStorage,
    JobBookmarkManager, JobBookmarkState,
    FullLoadProgress, IncrementalLoadProgress
)


class TestS3BookmarkConfig(unittest.TestCase):
    """Test S3BookmarkConfig functionality."""
    
    def test_s3_bookmark_config_creation(self):
        """Test creating S3BookmarkConfig instance."""
        config = S3BookmarkConfig(
            bucket_name="test-bucket",
            bookmark_prefix="bookmarks/",
            job_name="test-job"
        )
        
        self.assertEqual(config.bucket_name, "test-bucket")
        self.assertEqual(config.bookmark_prefix, "bookmarks/")
        self.assertEqual(config.job_name, "test-job")


class TestJobBookmarkState(unittest.TestCase):
    """Test JobBookmarkState functionality."""
    
    def test_job_bookmark_state_creation(self):
        """Test creating JobBookmarkState instance."""
        state = JobBookmarkState(
            table_name="test_table",
            incremental_strategy="timestamp",
            last_processed_value="2023-01-01 00:00:00",
            last_update_timestamp=datetime.now(timezone.utc)
        )
        
        self.assertEqual(state.table_name, "test_table")
        self.assertEqual(state.last_processed_value, "2023-01-01 00:00:00")
        self.assertEqual(state.incremental_strategy, "timestamp")
        self.assertIsInstance(state.last_update_timestamp, datetime)
    
    def test_job_bookmark_state_to_dict(self):
        """Test converting JobBookmarkState to dictionary."""
        timestamp = datetime.now(timezone.utc)
        state = JobBookmarkState(
            table_name="test_table",
            incremental_strategy="timestamp",
            last_processed_value="2023-01-01 00:00:00",
            last_update_timestamp=timestamp
        )
        
        state_dict = state.to_dict()
        
        self.assertEqual(state_dict["table_name"], "test_table")
        self.assertEqual(state_dict["last_processed_value"], "2023-01-01 00:00:00")
        self.assertEqual(state_dict["incremental_strategy"], "timestamp")
        self.assertEqual(state_dict["last_update_timestamp"], timestamp.isoformat())


class TestS3BookmarkStorage(unittest.TestCase):
    """Test S3BookmarkStorage functionality."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.config = S3BookmarkConfig(
            bucket_name="test-bucket",
            bookmark_prefix="bookmarks/",
            job_name="test-job"
        )
        # Don't create storage here - create it in each test with proper mocking
    
    @patch('boto3.client')
    def test_read_bookmark_success(self, mock_boto_client):
        """Test successful bookmark reading from S3."""
        # Mock S3 client
        mock_s3 = Mock()
        mock_boto_client.return_value = mock_s3
        
        # Create storage with mocked boto3 client
        storage = S3BookmarkStorage(self.config)
        
        # Mock S3 response
        bookmark_data = {
            "table_name": "test_table",
            "incremental_strategy": "timestamp",
            "incremental_column": "updated_at",
            "last_processed_value": "2023-01-01 00:00:00",
            "last_update_timestamp": "2023-01-01T00:00:00+00:00"
        }
        
        mock_s3.get_object.return_value = {
            'Body': Mock(read=Mock(return_value=json.dumps(bookmark_data).encode()))
        }
        
        # Test reading bookmark - this is an async method, so we need to handle it properly
        import asyncio
        result = asyncio.run(storage.read_bookmark("test_table"))
        
        self.assertIsInstance(result, dict)
        self.assertEqual(result["table_name"], "test_table")
        self.assertEqual(result["last_processed_value"], "2023-01-01 00:00:00")
        self.assertEqual(result["incremental_strategy"], "timestamp")
    
    @patch('boto3.client')
    def test_read_bookmark_not_found(self, mock_boto_client):
        """Test reading bookmark when file doesn't exist."""
        # Mock S3 client
        mock_s3 = Mock()
        mock_boto_client.return_value = mock_s3
        
        # Create storage with mocked boto3 client
        storage = S3BookmarkStorage(self.config)
        
        # Mock S3 NoSuchKey error
        mock_s3.get_object.side_effect = ClientError(
            {'Error': {'Code': 'NoSuchKey'}}, 'GetObject'
        )
        
        # Test reading non-existent bookmark
        import asyncio
        result = asyncio.run(storage.read_bookmark("nonexistent_table"))
        
        self.assertIsNone(result)
    
    @patch('boto3.client')
    def test_write_bookmark_success(self, mock_boto_client):
        """Test successful bookmark writing to S3."""
        # Mock S3 client
        mock_s3 = Mock()
        mock_boto_client.return_value = mock_s3
        
        # Create storage with mocked boto3 client
        storage = S3BookmarkStorage(self.config)
        
        # Mock successful put_object response
        mock_s3.put_object.return_value = {'ETag': '"test-etag"'}
        
        # Create bookmark data
        bookmark_data = {
            "table_name": "test_table",
            "incremental_strategy": "timestamp",
            "last_processed_value": "2023-01-01 00:00:00",
            "last_update_timestamp": datetime.now(timezone.utc).isoformat(),
            "is_first_run": False,
            "job_name": "test-job",
            "version": "1.0"
        }
        
        # Test writing bookmark
        import asyncio
        result = asyncio.run(storage.write_bookmark("test_table", bookmark_data))
        
        self.assertTrue(result)
        mock_s3.put_object.assert_called_once()


class TestJobBookmarkManager(unittest.TestCase):
    """Test JobBookmarkManager functionality."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.mock_glue_context = Mock()
        self.manager = JobBookmarkManager(self.mock_glue_context, "test-job")
    
    def test_get_bookmark_state_existing(self):
        """Test getting existing bookmark state."""
        # Mock existing bookmark
        existing_state = JobBookmarkState(
            table_name="test_table",
            incremental_strategy="timestamp",
            last_processed_value="2023-01-01 00:00:00",
            last_update_timestamp=datetime.now(timezone.utc)
        )
        
        # Mock the bookmark storage
        self.manager.bookmark_states["test_table"] = existing_state
        
        # Test getting bookmark state
        result = self.manager.get_bookmark_state("test_table")
        
        self.assertEqual(result, existing_state)
    
    def test_get_bookmark_state_new(self):
        """Test getting bookmark state for new table."""
        # Test getting bookmark state for new table - should return None if not exists
        result = self.manager.get_bookmark_state("new_table")
        
        # Production code returns None for non-existent tables
        self.assertIsNone(result)
    
    def test_update_bookmark_state(self):
        """Test updating bookmark state."""
        # First initialize a bookmark state for the table
        self.manager.initialize_bookmark_state(
            table_name="test_table",
            incremental_strategy="timestamp",
            incremental_column="updated_at"
        )
        
        # Test updating bookmark state with correct parameters
        self.manager.update_bookmark_state(
            table_name="test_table",
            new_max_value="2023-01-01 00:00:00",
            processed_rows=100
        )
        
        # Verify state was stored
        stored_state = self.manager.get_bookmark_state("test_table")
        self.assertIsNotNone(stored_state)
        self.assertEqual(stored_state.last_processed_value, "2023-01-01 00:00:00")
        self.assertEqual(stored_state.processed_rows, 100)


if __name__ == '__main__':
    unittest.main(verbosity=2)