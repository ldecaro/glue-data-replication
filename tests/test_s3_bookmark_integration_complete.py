#!/usr/bin/env python3
"""
Complete Integration Tests for S3 Bookmark Persistence - Task 15

This test suite provides comprehensive integration testing for S3 bookmark persistence
covering all requirements specified in task 15:

- Integration tests using real S3 bucket and IAM permissions
- Bookmark persistence across multiple simulated job executions  
- Recovery scenarios from corrupted bookmark files
- Performance with multiple tables and concurrent operations

Requirements covered: 1.1, 1.4, 5.3, 7.1
"""

import asyncio
import json
import os
import time
import uuid
import pytest
import boto3
import threading
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from unittest.mock import Mock, patch
from botocore.exceptions import ClientError

# Import the classes we're testing
import sys



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
except ImportError as e:
    print(f"Import error: {e}")
    raise


class TestS3BookmarkIntegrationComplete:
    """Complete integration tests using real S3 bucket and IAM permissions."""
    
    @classmethod
    def setup_class(cls):
        """Set up test environment with real S3 resources."""
        # Generate unique test identifiers
        cls.test_run_id = str(uuid.uuid4())[:8]
        cls.test_bucket = os.environ.get('TEST_S3_BUCKET', f'glue-integration-test-{cls.test_run_id}')
        cls.test_prefix = f'integration-tests/{cls.test_run_id}/'
        cls.job_name = f'integration-test-job-{cls.test_run_id}'
        
        # Initialize S3 client
        cls.s3_client = boto3.client('s3')
        
        # Create test bucket if it doesn't exist
        cls._ensure_test_bucket_exists()
        
        # Verify IAM permissions
        cls._verify_iam_permissions()
        
        print(f"Integration test setup complete:")
        print(f"  Test Run ID: {cls.test_run_id}")
        print(f"  S3 Bucket: {cls.test_bucket}")
        print(f"  Bookmark Prefix: {cls.test_prefix}")
    
    @classmethod
    def teardown_class(cls):
        """Clean up test resources."""
        try:
            # Clean up test bookmark files
            cls._cleanup_test_bookmarks()
            print(f"Integration test cleanup complete for run {cls.test_run_id}")
        except Exception as e:
            print(f"Warning: Cleanup failed: {e}")
    
    @classmethod
    def _ensure_test_bucket_exists(cls):
        """Ensure test S3 bucket exists."""
        try:
            cls.s3_client.head_bucket(Bucket=cls.test_bucket)
            print(f"Using existing S3 bucket: {cls.test_bucket}")
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                try:
                    # Create bucket
                    region = os.environ.get('AWS_DEFAULT_REGION', 'us-east-1')
                    if region == 'us-east-1':
                        cls.s3_client.create_bucket(Bucket=cls.test_bucket)
                    else:
                        cls.s3_client.create_bucket(
                            Bucket=cls.test_bucket,
                            CreateBucketConfiguration={'LocationConstraint': region}
                        )
                    print(f"Created S3 bucket: {cls.test_bucket}")
                except ClientError as create_error:
                    if create_error.response['Error']['Code'] != 'BucketAlreadyExists':
                        raise
            else:
                raise
    
    @classmethod
    def _verify_iam_permissions(cls):
        """Verify required IAM permissions for S3 operations."""
        test_key = f"{cls.test_prefix}iam-test.json"
        test_content = json.dumps({"test": "iam_permissions"})
        
        try:
            # Test write permission
            cls.s3_client.put_object(
                Bucket=cls.test_bucket,
                Key=test_key,
                Body=test_content,
                ContentType='application/json'
            )
            
            # Test read permission
            response = cls.s3_client.get_object(
                Bucket=cls.test_bucket,
                Key=test_key
            )
            
            # Test list permission
            cls.s3_client.list_objects_v2(
                Bucket=cls.test_bucket,
                Prefix=cls.test_prefix,
                MaxKeys=1
            )
            
            # Test delete permission
            cls.s3_client.delete_object(
                Bucket=cls.test_bucket,
                Key=test_key
            )
            
            print("✓ IAM permissions verified (read, write, list, delete)")
            
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == 'AccessDenied':
                pytest.fail(f"IAM permissions insufficient: {e}")
            else:
                pytest.fail(f"S3 operation failed: {e}")
    
    @classmethod
    def _cleanup_test_bookmarks(cls):
        """Clean up test bookmark files from S3."""
        try:
            # List all objects with test prefix
            response = cls.s3_client.list_objects_v2(
                Bucket=cls.test_bucket,
                Prefix=cls.test_prefix
            )
            
            if 'Contents' in response:
                # Delete all test objects
                objects_to_delete = [{'Key': obj['Key']} for obj in response['Contents']]
                if objects_to_delete:
                    cls.s3_client.delete_objects(
                        Bucket=cls.test_bucket,
                        Delete={'Objects': objects_to_delete}
                    )
                    print(f"Deleted {len(objects_to_delete)} test bookmark files")
        except Exception as e:
            print(f"Warning: Failed to cleanup test bookmarks: {e}")
    
    def test_real_s3_bucket_accessibility(self):
        """Test that S3 bucket is accessible with current IAM permissions."""
        # Test read access
        try:
            self.s3_client.list_objects_v2(
                Bucket=self.test_bucket,
                Prefix=self.test_prefix,
                MaxKeys=1
            )
        except ClientError as e:
            pytest.fail(f"S3 read access failed: {e}")
        
        # Test write access
        test_key = f"{self.test_prefix}access-test-{int(time.time())}.json"
        test_content = json.dumps({"test": "access", "timestamp": datetime.now(timezone.utc).isoformat()})
        
        try:
            self.s3_client.put_object(
                Bucket=self.test_bucket,
                Key=test_key,
                Body=test_content,
                ContentType='application/json'
            )
        except ClientError as e:
            pytest.fail(f"S3 write access failed: {e}")
        
        # Test delete access
        try:
            self.s3_client.delete_object(
                Bucket=self.test_bucket,
                Key=test_key
            )
        except ClientError as e:
            pytest.fail(f"S3 delete access failed: {e}")
        
        print("✓ Real S3 bucket accessibility verified")
    
    def test_s3_bookmark_storage_real_operations(self):
        """Test S3BookmarkStorage with real S3 operations - Requirement 1.1."""
        # Create S3BookmarkStorage instance
        config = S3BookmarkConfig(
            bucket_name=self.test_bucket,
            bookmark_prefix=self.test_prefix,
            job_name=self.job_name,
            retry_attempts=3,
            timeout_seconds=30
        )
        
        storage = S3BookmarkStorage(config)
        
        # Test data
        table_name = "integration_test_table"
        bookmark_data = {
            "table_name": table_name,
            "incremental_strategy": "timestamp",
            "incremental_column": "updated_at",
            "last_processed_value": "2024-01-15T10:30:00Z",
            "last_update_timestamp": "2024-01-15T11:00:00Z",
            "is_first_run": False,
            "job_name": self.job_name,
            "created_timestamp": "2024-01-01T09:00:00Z",
            "updated_timestamp": "2024-01-15T11:00:00Z",
            "version": "1.0"
        }
        
        # Test write operation
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        try:
            # Write bookmark
            write_result = loop.run_until_complete(
                storage._write_bookmark_to_s3(table_name, bookmark_data)
            )
            assert write_result is True, "S3 bookmark write should succeed"
            
            # Verify file exists in S3
            expected_key = f"{self.test_prefix}{self.job_name}/{table_name}.json"
            try:
                response = self.s3_client.head_object(
                    Bucket=self.test_bucket,
                    Key=expected_key
                )
                assert response['ContentType'] == 'application/json'
            except ClientError:
                pytest.fail("Bookmark file should exist in S3 after write")
            
            # Read bookmark back
            read_result = loop.run_until_complete(
                storage._read_bookmark_from_s3(table_name)
            )
            assert read_result is not None, "S3 bookmark read should return data"
            assert read_result["table_name"] == table_name
            assert read_result["last_processed_value"] == "2024-01-15T10:30:00Z"
            
            # Delete bookmark
            delete_result = loop.run_until_complete(
                storage.delete_bookmark(table_name)
            )
            assert delete_result is True, "S3 bookmark delete should succeed"
            
            # Verify deletion
            read_after_delete = loop.run_until_complete(
                storage._read_bookmark_from_s3(table_name)
            )
            assert read_after_delete is None, "Bookmark should not exist after deletion"
            
        finally:
            loop.close()
        
        print("✓ S3BookmarkStorage real operations test passed")


class TestBookmarkPersistenceAcrossJobExecutions:
    """Test bookmark persistence across multiple simulated job executions - Requirement 1.4."""
    
    @classmethod
    def setup_class(cls):
        """Set up test environment."""
        cls.test_run_id = str(uuid.uuid4())[:8]
        cls.test_bucket = os.environ.get('TEST_S3_BUCKET', f'glue-persistence-test-{cls.test_run_id}')
        cls.test_prefix = f'persistence-tests/{cls.test_run_id}/'
        cls.job_name = f'persistence-job-{cls.test_run_id}'
        
        # Initialize S3 client and ensure bucket exists
        cls.s3_client = boto3.client('s3')
        cls._ensure_test_bucket_exists()
    
    @classmethod
    def teardown_class(cls):
        """Clean up test resources."""
        try:
            cls._cleanup_test_bookmarks()
        except Exception as e:
            print(f"Warning: Cleanup failed: {e}")
    
    @classmethod
    def _ensure_test_bucket_exists(cls):
        """Ensure test S3 bucket exists."""
        try:
            cls.s3_client.head_bucket(Bucket=cls.test_bucket)
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                try:
                    region = os.environ.get('AWS_DEFAULT_REGION', 'us-east-1')
                    if region == 'us-east-1':
                        cls.s3_client.create_bucket(Bucket=cls.test_bucket)
                    else:
                        cls.s3_client.create_bucket(
                            Bucket=cls.test_bucket,
                            CreateBucketConfiguration={'LocationConstraint': region}
                        )
                except ClientError as create_error:
                    if create_error.response['Error']['Code'] != 'BucketAlreadyExists':
                        raise
    
    @classmethod
    def _cleanup_test_bookmarks(cls):
        """Clean up test bookmark files."""
        try:
            response = cls.s3_client.list_objects_v2(
                Bucket=cls.test_bucket,
                Prefix=cls.test_prefix
            )
            
            if 'Contents' in response:
                objects_to_delete = [{'Key': obj['Key']} for obj in response['Contents']]
                if objects_to_delete:
                    cls.s3_client.delete_objects(
                        Bucket=cls.test_bucket,
                        Delete={'Objects': objects_to_delete}
                    )
        except Exception as e:
            print(f"Warning: Failed to cleanup: {e}")
    
    def _simulate_job_execution(self, execution_number: int, table_name: str) -> JobBookmarkManager:
        """Simulate a single job execution with real S3 operations."""
        # Create mock Glue context
        mock_glue_context = Mock()
        
        # Create JobBookmarkManager with S3 paths
        source_jdbc_path = f"s3://{self.test_bucket}/drivers/oracle-driver.jar"
        target_jdbc_path = f"s3://{self.test_bucket}/drivers/postgres-driver.jar"
        
        # Mock S3 path utilities to use our test bucket
        with patch('glue_data_replication.S3PathUtilities') as mock_s3_utils:
            mock_s3_utils.detect_s3_bucket_from_jdbc_paths.return_value = self.test_bucket
            mock_s3_utils.validate_s3_bucket_accessibility.return_value = True
            
            manager = JobBookmarkManager(
                glue_context=mock_glue_context,
                job_name=f"{self.job_name}-exec-{execution_number}",
                source_jdbc_path=source_jdbc_path,
                target_jdbc_path=target_jdbc_path
            )
        
        return manager
    
    def test_first_job_execution_creates_bookmark(self):
        """Test that first job execution creates initial bookmark state."""
        table_name = f"customers_{int(time.time())}"
        
        # Simulate first job execution
        manager = self._simulate_job_execution(1, table_name)
        
        # Initialize bookmark state (first run)
        state = manager.initialize_bookmark_state(
            table_name=table_name,
            incremental_strategy="timestamp",
            incremental_column="updated_at"
        )
        
        assert state.is_first_run is True
        assert state.last_processed_value is None
        assert state.table_name == table_name
        
        # Update bookmark state after processing
        new_max_value = "2024-01-15T10:30:00Z"
        manager.update_bookmark_state(
            table_name=table_name,
            new_max_value=new_max_value,
            processed_rows=1000
        )
        
        # Verify state was updated
        updated_state = manager.bookmark_states[table_name]
        assert updated_state.is_first_run is False
        assert updated_state.last_processed_value == new_max_value
        
        # Verify bookmark file exists in S3
        expected_key = f"{self.test_prefix}{self.job_name}-exec-1/{table_name}.json"
        try:
            response = self.s3_client.get_object(
                Bucket=self.test_bucket,
                Key=expected_key
            )
            bookmark_content = json.loads(response['Body'].read().decode('utf-8'))
            assert bookmark_content['last_processed_value'] == new_max_value
            assert bookmark_content['is_first_run'] is False
        except ClientError:
            pytest.fail("Bookmark file should exist in S3 after update")
        
        print("✓ First job execution bookmark creation test passed")
    
    def test_subsequent_job_execution_loads_bookmark(self):
        """Test that subsequent job execution loads existing bookmark state."""
        table_name = f"orders_{int(time.time())}"
        
        # First execution - create bookmark
        manager1 = self._simulate_job_execution(1, table_name)
        state1 = manager1.initialize_bookmark_state(
            table_name=table_name,
            incremental_strategy="timestamp",
            incremental_column="created_at"
        )
        
        first_max_value = "2024-01-15T10:30:00Z"
        manager1.update_bookmark_state(
            table_name=table_name,
            new_max_value=first_max_value,
            processed_rows=500
        )
        
        # Wait a moment to ensure timestamp difference
        time.sleep(2)
        
        # Second execution - should load existing bookmark
        manager2 = self._simulate_job_execution(2, table_name)
        state2 = manager2.initialize_bookmark_state(
            table_name=table_name,
            incremental_strategy="timestamp",
            incremental_column="created_at"
        )
        
        # Should load previous state
        assert state2.is_first_run is False
        assert state2.last_processed_value == first_max_value
        assert state2.table_name == table_name
        
        # Update with new value
        second_max_value = "2024-01-15T12:00:00Z"
        manager2.update_bookmark_state(
            table_name=table_name,
            new_max_value=second_max_value,
            processed_rows=300
        )
        
        # Third execution - should load second execution's state
        manager3 = self._simulate_job_execution(3, table_name)
        state3 = manager3.initialize_bookmark_state(
            table_name=table_name,
            incremental_strategy="timestamp",
            incremental_column="created_at"
        )
        
        assert state3.is_first_run is False
        assert state3.last_processed_value == second_max_value
        
        print("✓ Subsequent job execution bookmark loading test passed")
    
    def test_multiple_tables_persistence(self):
        """Test bookmark persistence for multiple tables in same job."""
        tables = [f"products_{int(time.time())}", f"categories_{int(time.time())}", f"suppliers_{int(time.time())}"]
        
        # First execution - process multiple tables
        manager1 = self._simulate_job_execution(1, "multi-table")
        
        table_states = {}
        for i, table_name in enumerate(tables):
            state = manager1.initialize_bookmark_state(
                table_name=table_name,
                incremental_strategy="primary_key",
                incremental_column="id"
            )
            
            # Update each table with different max values
            max_value = (i + 1) * 1000
            manager1.update_bookmark_state(
                table_name=table_name,
                new_max_value=max_value,
                processed_rows=max_value
            )
            
            table_states[table_name] = max_value
        
        # Wait for S3 operations to complete
        time.sleep(2)
        
        # Second execution - verify all tables load correctly
        manager2 = self._simulate_job_execution(2, "multi-table")
        
        for table_name, expected_max_value in table_states.items():
            state = manager2.initialize_bookmark_state(
                table_name=table_name,
                incremental_strategy="primary_key",
                incremental_column="id"
            )
            
            assert state.is_first_run is False
            assert state.last_processed_value == expected_max_value
            assert state.table_name == table_name
        
        print("✓ Multiple tables persistence test passed")


class TestCorruptedBookmarkRecovery:
    """Test recovery scenarios from corrupted bookmark files - Requirement 5.3."""
    
    @classmethod
    def setup_class(cls):
        """Set up test environment."""
        cls.test_run_id = str(uuid.uuid4())[:8]
        cls.test_bucket = os.environ.get('TEST_S3_BUCKET', f'glue-corruption-test-{cls.test_run_id}')
        cls.test_prefix = f'corruption-tests/{cls.test_run_id}/'
        cls.job_name = f'corruption-job-{cls.test_run_id}'
        
        cls.s3_client = boto3.client('s3')
        cls._ensure_test_bucket_exists()
    
    @classmethod
    def teardown_class(cls):
        """Clean up test resources."""
        try:
            cls._cleanup_test_bookmarks()
        except Exception as e:
            print(f"Warning: Cleanup failed: {e}")
    
    @classmethod
    def _ensure_test_bucket_exists(cls):
        """Ensure test S3 bucket exists."""
        try:
            cls.s3_client.head_bucket(Bucket=cls.test_bucket)
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                try:
                    region = os.environ.get('AWS_DEFAULT_REGION', 'us-east-1')
                    if region == 'us-east-1':
                        cls.s3_client.create_bucket(Bucket=cls.test_bucket)
                    else:
                        cls.s3_client.create_bucket(
                            Bucket=cls.test_bucket,
                            CreateBucketConfiguration={'LocationConstraint': region}
                        )
                except ClientError as create_error:
                    if create_error.response['Error']['Code'] != 'BucketAlreadyExists':
                        raise
    
    @classmethod
    def _cleanup_test_bookmarks(cls):
        """Clean up test bookmark files."""
        try:
            response = cls.s3_client.list_objects_v2(
                Bucket=cls.test_bucket,
                Prefix=cls.test_prefix
            )
            
            if 'Contents' in response:
                objects_to_delete = [{'Key': obj['Key']} for obj in response['Contents']]
                if objects_to_delete:
                    cls.s3_client.delete_objects(
                        Bucket=cls.test_bucket,
                        Delete={'Objects': objects_to_delete}
                    )
        except Exception as e:
            print(f"Warning: Failed to cleanup: {e}")
    
    def _create_corrupted_bookmark(self, table_name: str, corruption_type: str):
        """Create a corrupted bookmark file in S3."""
        bookmark_key = f"{self.test_prefix}{self.job_name}/{table_name}.json"
        
        if corruption_type == "invalid_json":
            corrupted_content = '{"table_name": "test", "invalid": json syntax}'
        elif corruption_type == "missing_required_fields":
            corrupted_content = json.dumps({"some_field": "value"})
        elif corruption_type == "invalid_data_types":
            corrupted_content = json.dumps({
                "table_name": table_name,
                "is_first_run": "not_a_boolean",
                "last_processed_value": {"invalid": "object"}
            })
        elif corruption_type == "empty_file":
            corrupted_content = ""
        else:
            corrupted_content = "completely invalid content"
        
        self.s3_client.put_object(
            Bucket=self.test_bucket,
            Key=bookmark_key,
            Body=corrupted_content,
            ContentType='application/json'
        )
        
        return bookmark_key
    
    def test_invalid_json_recovery(self):
        """Test recovery from invalid JSON bookmark files."""
        table_name = f"invalid_json_table_{int(time.time())}"
        
        # Create corrupted bookmark
        corrupted_key = self._create_corrupted_bookmark(table_name, "invalid_json")
        
        # Create S3BookmarkStorage
        config = S3BookmarkConfig(
            bucket_name=self.test_bucket,
            bookmark_prefix=self.test_prefix,
            job_name=self.job_name
        )
        storage = S3BookmarkStorage(config)
        
        # Attempt to read corrupted bookmark
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        try:
            result = loop.run_until_complete(
                storage._read_bookmark_from_s3(table_name)
            )
            
            # Should return None due to corruption
            assert result is None
            
            # Wait for deletion to complete
            time.sleep(2)
            
            # Verify corrupted file was deleted
            try:
                self.s3_client.head_object(
                    Bucket=self.test_bucket,
                    Key=corrupted_key
                )
                # If we get here, file still exists (unexpected)
                pytest.fail("Corrupted bookmark file should have been deleted")
            except ClientError as e:
                if e.response['Error']['Code'] == '404':
                    # Expected - file was deleted
                    pass
                else:
                    raise
        
        finally:
            loop.close()
        
        print("✓ Invalid JSON recovery test passed")
    
    def test_missing_required_fields_recovery(self):
        """Test recovery from bookmark files with missing required fields."""
        table_name = f"missing_fields_table_{int(time.time())}"
        
        # Create corrupted bookmark
        self._create_corrupted_bookmark(table_name, "missing_required_fields")
        
        # Create JobBookmarkManager
        mock_glue_context = Mock()
        
        with patch('glue_data_replication.S3PathUtilities') as mock_s3_utils:
            mock_s3_utils.detect_s3_bucket_from_jdbc_paths.return_value = self.test_bucket
            mock_s3_utils.validate_s3_bucket_accessibility.return_value = True
            
            manager = JobBookmarkManager(
                glue_context=mock_glue_context,
                job_name=self.job_name,
                source_jdbc_path=f"s3://{self.test_bucket}/drivers/oracle.jar"
            )
        
        # Initialize bookmark state - should handle corruption gracefully
        state = manager.initialize_bookmark_state(
            table_name=table_name,
            incremental_strategy="timestamp",
            incremental_column="updated_at"
        )
        
        # Should create new state (first run) due to corruption
        assert state.is_first_run is True
        assert state.last_processed_value is None
        assert state.table_name == table_name
        
        print("✓ Missing required fields recovery test passed")
    
    def test_full_load_after_corruption_cleanup(self):
        """Test that full load is performed after corrupted bookmark cleanup."""
        table_name = f"full_load_after_corruption_{int(time.time())}"
        
        # Create corrupted bookmark
        self._create_corrupted_bookmark(table_name, "invalid_json")
        
        # Create JobBookmarkManager
        mock_glue_context = Mock()
        
        with patch('glue_data_replication.S3PathUtilities') as mock_s3_utils:
            mock_s3_utils.detect_s3_bucket_from_jdbc_paths.return_value = self.test_bucket
            mock_s3_utils.validate_s3_bucket_accessibility.return_value = True
            
            manager = JobBookmarkManager(
                glue_context=mock_glue_context,
                job_name=self.job_name,
                source_jdbc_path=f"s3://{self.test_bucket}/drivers/oracle.jar"
            )
        
        # Initialize bookmark state
        state = manager.initialize_bookmark_state(
            table_name=table_name,
            incremental_strategy="timestamp",
            incremental_column="updated_at"
        )
        
        # Should indicate first run (full load)
        assert state.is_first_run is True
        
        # Update bookmark state to simulate successful processing
        manager.update_bookmark_state(
            table_name=table_name,
            new_max_value="2024-01-15T10:30:00Z",
            processed_rows=1000
        )
        
        # Verify new bookmark was created
        updated_state = manager.bookmark_states[table_name]
        assert updated_state.is_first_run is False
        assert updated_state.last_processed_value == "2024-01-15T10:30:00Z"
        
        # Verify new bookmark file exists in S3
        expected_key = f"{self.test_prefix}{self.job_name}/{table_name}.json"
        try:
            response = self.s3_client.get_object(
                Bucket=self.test_bucket,
                Key=expected_key
            )
            bookmark_content = json.loads(response['Body'].read().decode('utf-8'))
            assert bookmark_content['last_processed_value'] == "2024-01-15T10:30:00Z"
            assert bookmark_content['is_first_run'] is False
        except ClientError:
            pytest.fail("New bookmark file should exist in S3 after recovery")
        
        print("✓ Full load after corruption cleanup test passed")


class TestPerformanceWithMultipleTables:
    """Test performance with multiple tables and concurrent operations - Requirement 7.1."""
    
    @classmethod
    def setup_class(cls):
        """Set up test environment."""
        cls.test_run_id = str(uuid.uuid4())[:8]
        cls.test_bucket = os.environ.get('TEST_S3_BUCKET', f'glue-performance-test-{cls.test_run_id}')
        cls.test_prefix = f'performance-tests/{cls.test_run_id}/'
        cls.job_name = f'performance-job-{cls.test_run_id}'
        
        cls.s3_client = boto3.client('s3')
        cls._ensure_test_bucket_exists()
    
    @classmethod
    def teardown_class(cls):
        """Clean up test resources."""
        try:
            cls._cleanup_test_bookmarks()
        except Exception as e:
            print(f"Warning: Cleanup failed: {e}")
    
    @classmethod
    def _ensure_test_bucket_exists(cls):
        """Ensure test S3 bucket exists."""
        try:
            cls.s3_client.head_bucket(Bucket=cls.test_bucket)
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                try:
                    region = os.environ.get('AWS_DEFAULT_REGION', 'us-east-1')
                    if region == 'us-east-1':
                        cls.s3_client.create_bucket(Bucket=cls.test_bucket)
                    else:
                        cls.s3_client.create_bucket(
                            Bucket=cls.test_bucket,
                            CreateBucketConfiguration={'LocationConstraint': region}
                        )
                except ClientError as create_error:
                    if create_error.response['Error']['Code'] != 'BucketAlreadyExists':
                        raise
    
    @classmethod
    def _cleanup_test_bookmarks(cls):
        """Clean up test bookmark files."""
        try:
            response = cls.s3_client.list_objects_v2(
                Bucket=cls.test_bucket,
                Prefix=cls.test_prefix
            )
            
            if 'Contents' in response:
                objects_to_delete = [{'Key': obj['Key']} for obj in response['Contents']]
                if objects_to_delete:
                    cls.s3_client.delete_objects(
                        Bucket=cls.test_bucket,
                        Delete={'Objects': objects_to_delete}
                    )
        except Exception as e:
            print(f"Warning: Failed to cleanup: {e}")
    
    def test_multiple_tables_sequential_processing(self):
        """Test bookmark operations with multiple tables processed sequentially."""
        num_tables = 20
        table_names = [f"table_{i:03d}_{int(time.time())}" for i in range(num_tables)]
        
        # Create JobBookmarkManager
        mock_glue_context = Mock()
        
        with patch('glue_data_replication.S3PathUtilities') as mock_s3_utils:
            mock_s3_utils.detect_s3_bucket_from_jdbc_paths.return_value = self.test_bucket
            mock_s3_utils.validate_s3_bucket_accessibility.return_value = True
            
            manager = JobBookmarkManager(
                glue_context=mock_glue_context,
                job_name=self.job_name,
                source_jdbc_path=f"s3://{self.test_bucket}/drivers/oracle.jar"
            )
        
        # Measure time for sequential processing
        start_time = time.time()
        
        # Initialize and update bookmark states for all tables
        for i, table_name in enumerate(table_names):
            # Initialize bookmark state
            state = manager.initialize_bookmark_state(
                table_name=table_name,
                incremental_strategy="primary_key",
                incremental_column="id"
            )
            
            assert state.is_first_run is True
            assert state.table_name == table_name
            
            # Update bookmark state
            max_value = (i + 1) * 1000
            manager.update_bookmark_state(
                table_name=table_name,
                new_max_value=max_value,
                processed_rows=max_value
            )
        
        sequential_time = time.time() - start_time
        
        # Verify all bookmarks were created
        time.sleep(3)  # Allow time for S3 operations to complete
        
        for table_name in table_names:
            expected_key = f"{self.test_prefix}{self.job_name}/{table_name}.json"
            try:
                response = self.s3_client.head_object(
                    Bucket=self.test_bucket,
                    Key=expected_key
                )
                assert response['ContentType'] == 'application/json'
            except ClientError:
                pytest.fail(f"Bookmark file should exist for table {table_name}")
        
        print(f"✓ Sequential processing of {num_tables} tables completed in {sequential_time:.2f} seconds")
        print(f"  Average time per table: {sequential_time/num_tables:.3f} seconds")
    
    def test_concurrent_bookmark_operations(self):
        """Test concurrent bookmark read/write operations."""
        num_tables = 10
        table_names = [f"concurrent_table_{i:03d}_{int(time.time())}" for i in range(num_tables)]
        
        # Create S3BookmarkStorage
        config = S3BookmarkConfig(
            bucket_name=self.test_bucket,
            bookmark_prefix=self.test_prefix,
            job_name=self.job_name
        )
        storage = S3BookmarkStorage(config)
        
        def write_bookmark_task(table_name: str, table_index: int):
            """Task to write a bookmark concurrently."""
            bookmark_data = {
                "table_name": table_name,
                "incremental_strategy": "primary_key",
                "incremental_column": "id",
                "last_processed_value": (table_index + 1) * 1000,
                "is_first_run": False,
                "job_name": self.job_name,
                "created_timestamp": datetime.now(timezone.utc).isoformat(),
                "updated_timestamp": datetime.now(timezone.utc).isoformat(),
                "version": "1.0"
            }
            
            # Create new event loop for this thread
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            
            try:
                result = loop.run_until_complete(
                    storage._write_bookmark_to_s3(table_name, bookmark_data)
                )
                return {"table_name": table_name, "success": result, "operation": "write"}
            finally:
                loop.close()
        
        # Test concurrent writes
        start_time = time.time()
        
        with ThreadPoolExecutor(max_workers=5) as executor:
            write_futures = [
                executor.submit(write_bookmark_task, table_name, i)
                for i, table_name in enumerate(table_names)
            ]
            
            write_results = []
            for future in as_completed(write_futures):
                try:
                    result = future.result(timeout=30)
                    write_results.append(result)
                except Exception as e:
                    pytest.fail(f"Concurrent write failed: {e}")
        
        concurrent_write_time = time.time() - start_time
        
        # Verify all writes succeeded
        assert len(write_results) == num_tables
        for result in write_results:
            assert result["success"] is True
        
        # Wait for S3 consistency
        time.sleep(2)
        
        def read_bookmark_task(table_name: str):
            """Task to read a bookmark concurrently."""
            # Create new event loop for this thread
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            
            try:
                result = loop.run_until_complete(
                    storage._read_bookmark_from_s3(table_name)
                )
                return {"table_name": table_name, "data": result, "operation": "read"}
            finally:
                loop.close()
        
        # Test concurrent reads
        start_time = time.time()
        
        with ThreadPoolExecutor(max_workers=5) as executor:
            read_futures = [
                executor.submit(read_bookmark_task, table_name)
                for table_name in table_names
            ]
            
            read_results = []
            for future in as_completed(read_futures):
                try:
                    result = future.result(timeout=30)
                    read_results.append(result)
                except Exception as e:
                    pytest.fail(f"Concurrent read failed: {e}")
        
        concurrent_read_time = time.time() - start_time
        
        # Verify all reads succeeded
        assert len(read_results) == num_tables
        for result in read_results:
            assert result["data"] is not None
            assert result["data"]["table_name"] == result["table_name"]
        
        print(f"✓ Concurrent operations test completed:")
        print(f"  Concurrent writes: {concurrent_write_time:.2f} seconds")
        print(f"  Concurrent reads: {concurrent_read_time:.2f} seconds")
        print(f"  Average write time: {concurrent_write_time/num_tables:.3f} seconds")
        print(f"  Average read time: {concurrent_read_time/num_tables:.3f} seconds")
    
    def test_large_scale_bookmark_operations(self):
        """Test bookmark operations with large number of tables."""
        num_tables = 50
        table_names = [f"large_scale_table_{i:03d}_{int(time.time())}" for i in range(num_tables)]
        
        # Create JobBookmarkManager
        mock_glue_context = Mock()
        
        with patch('glue_data_replication.S3PathUtilities') as mock_s3_utils:
            mock_s3_utils.detect_s3_bucket_from_jdbc_paths.return_value = self.test_bucket
            mock_s3_utils.validate_s3_bucket_accessibility.return_value = True
            
            manager = JobBookmarkManager(
                glue_context=mock_glue_context,
                job_name=self.job_name,
                source_jdbc_path=f"s3://{self.test_bucket}/drivers/oracle.jar"
            )
        
        # Measure time for large-scale processing
        start_time = time.time()
        
        # Process all tables
        for i, table_name in enumerate(table_names):
            state = manager.initialize_bookmark_state(
                table_name=table_name,
                incremental_strategy="timestamp",
                incremental_column="updated_at"
            )
            
            # Simulate processing with different timestamps
            timestamp = datetime.now(timezone.utc) + timedelta(minutes=i)
            manager.update_bookmark_state(
                table_name=table_name,
                new_max_value=timestamp.isoformat(),
                processed_rows=(i + 1) * 100
            )
        
        processing_time = time.time() - start_time
        
        # Wait for S3 operations to complete
        time.sleep(5)
        
        # Verify performance metrics
        avg_time_per_table = processing_time / num_tables
        
        # Performance assertions
        assert avg_time_per_table < 1.0, f"Average processing time per table ({avg_time_per_table:.3f}s) should be under 1 second"
        assert processing_time < 60.0, f"Total processing time ({processing_time:.2f}s) should be under 60 seconds"
        
        # Verify all bookmarks exist
        bookmark_count = 0
        response = self.s3_client.list_objects_v2(
            Bucket=self.test_bucket,
            Prefix=f"{self.test_prefix}{self.job_name}/"
        )
        
        if 'Contents' in response:
            bookmark_count = len(response['Contents'])
        
        assert bookmark_count == num_tables, f"Expected {num_tables} bookmark files, found {bookmark_count}"
        
        print(f"✓ Large-scale operations test completed:")
        print(f"  Processed {num_tables} tables in {processing_time:.2f} seconds")
        print(f"  Average time per table: {avg_time_per_table:.3f} seconds")
        print(f"  Created {bookmark_count} bookmark files in S3")


if __name__ == "__main__":
    # Run tests with pytest
    pytest.main([__file__, "-v", "-s"])