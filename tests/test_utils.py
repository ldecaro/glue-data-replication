#!/usr/bin/env python3
"""
Unit tests for utility modules.

This test suite covers:
- S3PathUtilities functionality
- EnhancedS3ParallelOperations
"""

import unittest
from unittest.mock import Mock, MagicMock, patch
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

# Import the classes to test from new modular structure
from glue_job.utils import S3PathUtilities, EnhancedS3ParallelOperations


class TestS3PathUtilities(unittest.TestCase):
    """Test S3PathUtilities functionality."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.s3_utils = S3PathUtilities()
    
    def test_parse_s3_path_valid(self):
        """Test parsing valid S3 path."""
        s3_path = "s3://my-bucket/path/to/file.txt"
        
        bucket, key = self.s3_utils.parse_s3_path(s3_path)
        
        self.assertEqual(bucket, "my-bucket")
        self.assertEqual(key, "path/to/file.txt")
    
    def test_parse_s3_path_with_prefix(self):
        """Test parsing S3 path with prefix."""
        s3_path = "s3://my-bucket/prefix/subfolder/"
        
        bucket, key = self.s3_utils.parse_s3_path(s3_path)
        
        self.assertEqual(bucket, "my-bucket")
        self.assertEqual(key, "prefix/subfolder/")
    
    def test_parse_s3_path_root(self):
        """Test parsing S3 path at bucket root."""
        s3_path = "s3://my-bucket/"
        
        bucket, key = self.s3_utils.parse_s3_path(s3_path)
        
        self.assertEqual(bucket, "my-bucket")
        self.assertEqual(key, "")
    
    def test_parse_s3_path_invalid(self):
        """Test parsing invalid S3 path."""
        invalid_path = "not-an-s3-path"
        
        with self.assertRaises(ValueError):
            self.s3_utils.parse_s3_path(invalid_path)
    
    def test_build_s3_path(self):
        """Test building S3 path from bucket and key."""
        bucket = "my-bucket"
        key = "path/to/file.txt"
        
        s3_path = self.s3_utils.build_s3_path(bucket, key)
        
        self.assertEqual(s3_path, "s3://my-bucket/path/to/file.txt")
    
    def test_build_s3_path_empty_key(self):
        """Test building S3 path with empty key."""
        bucket = "my-bucket"
        key = ""
        
        s3_path = self.s3_utils.build_s3_path(bucket, key)
        
        self.assertEqual(s3_path, "s3://my-bucket/")
    
    def test_extract_bucket_from_jdbc_path(self):
        """Test extracting S3 bucket from JDBC driver path."""
        jdbc_path = "s3://my-bucket/drivers/postgresql-42.3.1.jar"
        
        bucket = self.s3_utils.extract_bucket_from_jdbc_path(jdbc_path)
        
        self.assertEqual(bucket, "my-bucket")
    
    def test_extract_bucket_from_jdbc_path_invalid(self):
        """Test extracting bucket from invalid JDBC path."""
        invalid_path = "/local/path/to/driver.jar"
        
        bucket = self.s3_utils.extract_bucket_from_jdbc_path(invalid_path)
        
        self.assertIsNone(bucket)
    
    def test_is_valid_s3_path(self):
        """Test validating S3 paths."""
        valid_path = "s3://my-bucket/path/to/file.txt"
        invalid_path = "not-an-s3-path"
        
        self.assertTrue(self.s3_utils.is_valid_s3_path(valid_path))
        self.assertFalse(self.s3_utils.is_valid_s3_path(invalid_path))
    
    def test_normalize_s3_path(self):
        """Test normalizing S3 paths."""
        path_with_double_slashes = "s3://my-bucket//path//to//file.txt"
        
        normalized = self.s3_utils.normalize_s3_path(path_with_double_slashes)
        
        self.assertEqual(normalized, "s3://my-bucket/path/to/file.txt")
    
    def test_get_parent_path(self):
        """Test getting parent path."""
        s3_path = "s3://my-bucket/path/to/file.txt"
        
        parent = self.s3_utils.get_parent_path(s3_path)
        
        self.assertEqual(parent, "s3://my-bucket/path/to/")
    
    def test_get_parent_path_root(self):
        """Test getting parent path for root level."""
        s3_path = "s3://my-bucket/file.txt"
        
        parent = self.s3_utils.get_parent_path(s3_path)
        
        self.assertEqual(parent, "s3://my-bucket/")


class TestEnhancedS3ParallelOperations(unittest.TestCase):
    """Test EnhancedS3ParallelOperations functionality."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.s3_ops = EnhancedS3ParallelOperations()
    
    @patch('boto3.client')
    def test_list_objects_single_page(self, mock_boto_client):
        """Test listing objects with single page response."""
        # Mock S3 client
        mock_s3 = Mock()
        mock_boto_client.return_value = mock_s3
        
        # Mock S3 response
        mock_s3.list_objects_v2.return_value = {
            'Contents': [
                {'Key': 'file1.txt', 'Size': 100},
                {'Key': 'file2.txt', 'Size': 200}
            ],
            'IsTruncated': False
        }
        
        # Test listing objects
        objects = self.s3_ops.list_objects("my-bucket", "prefix/")
        
        self.assertEqual(len(objects), 2)
        self.assertEqual(objects[0]['Key'], 'file1.txt')
        self.assertEqual(objects[1]['Key'], 'file2.txt')
    
    @patch('boto3.client')
    def test_list_objects_multiple_pages(self, mock_boto_client):
        """Test listing objects with pagination."""
        # Mock S3 client
        mock_s3 = Mock()
        mock_boto_client.return_value = mock_s3
        
        # Mock paginated S3 responses
        mock_s3.list_objects_v2.side_effect = [
            {
                'Contents': [{'Key': 'file1.txt', 'Size': 100}],
                'IsTruncated': True,
                'NextContinuationToken': 'token1'
            },
            {
                'Contents': [{'Key': 'file2.txt', 'Size': 200}],
                'IsTruncated': False
            }
        ]
        
        # Test listing objects with pagination
        objects = self.s3_ops.list_objects("my-bucket", "prefix/")
        
        self.assertEqual(len(objects), 2)
        self.assertEqual(mock_s3.list_objects_v2.call_count, 2)
    
    @patch('boto3.client')
    def test_object_exists_true(self, mock_boto_client):
        """Test checking if object exists (true case)."""
        # Mock S3 client
        mock_s3 = Mock()
        mock_boto_client.return_value = mock_s3
        
        # Mock successful head_object response
        mock_s3.head_object.return_value = {'ContentLength': 100}
        
        # Test object existence check
        exists = self.s3_ops.object_exists("my-bucket", "path/to/file.txt")
        
        self.assertTrue(exists)
        mock_s3.head_object.assert_called_once_with(Bucket="my-bucket", Key="path/to/file.txt")
    
    @patch('boto3.client')
    def test_object_exists_false(self, mock_boto_client):
        """Test checking if object exists (false case)."""
        # Mock S3 client
        mock_s3 = Mock()
        mock_boto_client.return_value = mock_s3
        
        # Mock 404 error for non-existent object
        from botocore.exceptions import ClientError
        mock_s3.head_object.side_effect = ClientError(
            {'Error': {'Code': '404'}}, 'HeadObject'
        )
        
        # Test object existence check
        exists = self.s3_ops.object_exists("my-bucket", "nonexistent/file.txt")
        
        self.assertFalse(exists)
    
    @patch('boto3.client')
    def test_copy_object_success(self, mock_boto_client):
        """Test successful object copying."""
        # Mock S3 client
        mock_s3 = Mock()
        mock_boto_client.return_value = mock_s3
        
        # Test copying object
        result = self.s3_ops.copy_object(
            "source-bucket", "source/key.txt",
            "dest-bucket", "dest/key.txt"
        )
        
        self.assertTrue(result)
        mock_s3.copy_object.assert_called_once()
    
    @patch('boto3.client')
    def test_copy_object_failure(self, mock_boto_client):
        """Test object copying failure."""
        # Mock S3 client
        mock_s3 = Mock()
        mock_boto_client.return_value = mock_s3
        
        # Mock copy failure
        from botocore.exceptions import ClientError
        mock_s3.copy_object.side_effect = ClientError(
            {'Error': {'Code': 'NoSuchBucket'}}, 'CopyObject'
        )
        
        # Test copying object failure
        result = self.s3_ops.copy_object(
            "nonexistent-bucket", "source/key.txt",
            "dest-bucket", "dest/key.txt"
        )
        
        self.assertFalse(result)
    
    @patch('boto3.client')
    def test_delete_object_success(self, mock_boto_client):
        """Test successful object deletion."""
        # Mock S3 client
        mock_s3 = Mock()
        mock_boto_client.return_value = mock_s3
        
        # Test deleting object
        result = self.s3_ops.delete_object("my-bucket", "path/to/file.txt")
        
        self.assertTrue(result)
        mock_s3.delete_object.assert_called_once_with(
            Bucket="my-bucket", 
            Key="path/to/file.txt"
        )
    
    @patch('boto3.client')
    def test_get_object_size(self, mock_boto_client):
        """Test getting object size."""
        # Mock S3 client
        mock_s3 = Mock()
        mock_boto_client.return_value = mock_s3
        
        # Mock head_object response
        mock_s3.head_object.return_value = {'ContentLength': 1024}
        
        # Test getting object size
        size = self.s3_ops.get_object_size("my-bucket", "path/to/file.txt")
        
        self.assertEqual(size, 1024)
        mock_s3.head_object.assert_called_once_with(
            Bucket="my-bucket", 
            Key="path/to/file.txt"
        )


if __name__ == '__main__':
    unittest.main(verbosity=2)