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
    'pyspark.sql.functions', 'boto3'
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
            key_prefix="bookmarks/",
            enable_encryption=True
        )
        
        self.assertEqual(config.bucket_name, "test-bucket")
        self.assertEqual(config.key_prefix, "bookmarks/")
        self.assertTrue(config.enable_encryption)


class TestJobBookmarkState(unittest.TestCase):
    """Test JobBookmarkState functionality."""
    
    def test_job_bookmark_state_creation(self):
        """Test creating JobBookmarkState instance."""
        state = JobBookmarkState(
            table_name="test_table",
            last_processed_value="2023-01-01 00:00:00",
            processing_mode="incremental",
            last_update_time=datetime.now(timezone.utc)
        )
        
        self.assertEqual(state.table_name, "test_table")
        self.assertEqual(state.last_processed_value, "2023-01-01 00:00:00")
        self.assertEqual(state.processing_mode, "incremental")
        self.assertIsInstance(state.last_update_time, datetime)
    
    def test_job_bookmark_state_to_dict(self):
        """Test converting JobBookmarkState to dictionary."""
        timestamp = datetime.now(timezone.utc)
        state = JobBookmarkState(
            table_name="test_table",
            last_processed_value="2023-01-01 00:00:00",
            processing_mode="incremental",
            last_update_time=timestamp
        )
        
        state_dict = state.to_dict()
        
        self.assertEqual(state_dict["table_name"], "test_table")
        self.assertEqual(state_dict["last_processed_value"], "2023-01-01 00:00:00")
        self.assertEqual(state_dict["processing_mode"], "incremental")
        self.assertEqual(state_dict["last_update_time"], timestamp.isoformat())


class TestS3BookmarkStorage(unittest.TestCase):
    """Test S3BookmarkStorage functionality."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.config = S3BookmarkConfig(
            bucket_name="test-bucket",
            key_prefix="bookmarks/"
        )
        self.storage = S3BookmarkStorage(self.config)
    
    @patch('boto3.client')
    def test_read_bookmark_success(self, mock_boto_client):
        """Test successful bookmark reading from S3."""
        # Mock S3 client
        mock_s3 = Mock()
        mock_boto_client.return_value = mock_s3
        
        # Mock S3 response
        bookmark_data = {
            "table_name": "test_table",
            "last_processed_value": "2023-01-01 00:00:00",
            "processing_mode": "incremental",
            "last_update_time": "2023-01-01T00:00:00+00:00"
        }
        
        mock_s3.get_object.return_value = {
            'Body': Mock(read=Mock(return_value=json.dumps(bookmark_data).encode()))
        }
        
        # Test reading bookmark
        result = self.storage.read_bookmark("test_table")
        
        self.assertIsInstance(result, JobBookmarkState)
        self.assertEqual(result.table_name, "test_table")
        self.assertEqual(result.last_processed_value, "2023-01-01 00:00:00")
        self.assertEqual(result.processing_mode, "incremental")
    
    @patch('boto3.client')
    def test_read_bookmark_not_found(self, mock_boto_client):
        """Test reading bookmark when file doesn't exist."""
        # Mock S3 client
        mock_s3 = Mock()
        mock_boto_client.return_value = mock_s3
        
        # Mock S3 NoSuchKey error
        mock_s3.get_object.side_effect = ClientError(
            {'Error': {'Code': 'NoSuchKey'}}, 'GetObject'
        )
        
        # Test reading non-existent bookmark
        result = self.storage.read_bookmark("nonexistent_table")
        
        self.assertIsNone(result)
    
    @patch('boto3.client')
    def test_write_bookmark_success(self, mock_boto_client):
        """Test successful bookmark writing to S3."""
        # Mock S3 client
        mock_s3 = Mock()
        mock_boto_client.return_value = mock_s3
        
        # Create bookmark state
        state = JobBookmarkState(
            table_name="test_table",
            last_processed_value="2023-01-01 00:00:00",
            processing_mode="incremental",
            last_update_time=datetime.now(timezone.utc)
        )
        
        # Test writing bookmark
        result = self.storage.write_bookmark(state)
        
        self.assertTrue(result)
        mock_s3.put_object.assert_called_once()


class TestJobBookmarkManager(unittest.TestCase):
    """Test JobBookmarkManager functionality."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.mock_storage = Mock(spec=S3BookmarkStorage)
        self.manager = JobBookmarkManager(self.mock_storage)
    
    def test_get_bookmark_state_existing(self):
        """Test getting existing bookmark state."""
        # Mock existing bookmark
        existing_state = JobBookmarkState(
            table_name="test_table",
            last_processed_value="2023-01-01 00:00:00",
            processing_mode="incremental",
            last_update_time=datetime.now(timezone.utc)
        )
        
        self.mock_storage.read_bookmark.return_value = existing_state
        
        # Test getting bookmark state
        result = self.manager.get_bookmark_state("test_table")
        
        self.assertEqual(result, existing_state)
        self.mock_storage.read_bookmark.assert_called_once_with("test_table")
    
    def test_get_bookmark_state_new(self):
        """Test getting bookmark state for new table."""
        # Mock no existing bookmark
        self.mock_storage.read_bookmark.return_value = None
        
        # Test getting bookmark state for new table
        result = self.manager.get_bookmark_state("new_table")
        
        self.assertIsInstance(result, JobBookmarkState)
        self.assertEqual(result.table_name, "new_table")
        self.assertEqual(result.processing_mode, "full-load")
        self.assertIsNone(result.last_processed_value)
    
    def test_update_bookmark_state(self):
        """Test updating bookmark state."""
        # Create bookmark state
        state = JobBookmarkState(
            table_name="test_table",
            last_processed_value="2023-01-01 00:00:00",
            processing_mode="incremental",
            last_update_time=datetime.now(timezone.utc)
        )
        
        self.mock_storage.write_bookmark.return_value = True
        
        # Test updating bookmark state
        self.manager.update_bookmark_state("test_table", state)
        
        self.mock_storage.write_bookmark.assert_called_once_with(state)


if __name__ == '__main__':
    unittest.main(verbosity=2)