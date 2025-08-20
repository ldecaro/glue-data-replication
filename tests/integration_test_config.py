#!/usr/bin/env python3
"""
Configuration for S3 Bookmark Integration Tests - Task 15

This module provides configuration settings and utilities for running
comprehensive integration tests for S3 bookmark persistence.

Requirements covered: 1.1, 1.4, 5.3, 7.1
"""

import os
import uuid
from dataclasses import dataclass
from typing import Optional, Dict, Any


@dataclass
class IntegrationTestConfig:
    """Configuration for integration tests."""
    
    # S3 Configuration
    test_bucket: str
    test_prefix: str
    aws_region: Optional[str] = None
    
    # Test Execution Configuration
    test_run_id: str = None
    job_name_prefix: str = "integration-test"
    
    # Performance Test Configuration
    small_table_count: int = 5
    medium_table_count: int = 20
    large_table_count: int = 50
    concurrent_workers: int = 5
    
    # Timeout Configuration
    s3_operation_timeout: int = 30
    test_execution_timeout: int = 1800  # 30 minutes
    
    # Retry Configuration
    s3_retry_attempts: int = 3
    test_retry_attempts: int = 2
    
    def __post_init__(self):
        """Initialize derived configuration values."""
        if not self.test_run_id:
            self.test_run_id = str(uuid.uuid4())[:8]
        
        if not self.test_prefix.endswith('/'):
            self.test_prefix += '/'
        
        # Set AWS region from environment if not specified
        if not self.aws_region:
            self.aws_region = os.environ.get('AWS_DEFAULT_REGION', 'us-east-1')
    
    @classmethod
    def from_environment(cls) -> 'IntegrationTestConfig':
        """Create configuration from environment variables."""
        test_run_id = str(uuid.uuid4())[:8]
        
        return cls(
            test_bucket=os.environ.get('TEST_S3_BUCKET', f'glue-integration-test-{test_run_id}'),
            test_prefix=os.environ.get('TEST_S3_PREFIX', f'integration-tests/{test_run_id}/'),
            aws_region=os.environ.get('AWS_DEFAULT_REGION', 'us-east-1'),
            test_run_id=test_run_id,
            job_name_prefix=os.environ.get('TEST_JOB_PREFIX', 'integration-test'),
            small_table_count=int(os.environ.get('TEST_SMALL_TABLE_COUNT', '5')),
            medium_table_count=int(os.environ.get('TEST_MEDIUM_TABLE_COUNT', '20')),
            large_table_count=int(os.environ.get('TEST_LARGE_TABLE_COUNT', '50')),
            concurrent_workers=int(os.environ.get('TEST_CONCURRENT_WORKERS', '5')),
            s3_operation_timeout=int(os.environ.get('TEST_S3_TIMEOUT', '30')),
            test_execution_timeout=int(os.environ.get('TEST_EXECUTION_TIMEOUT', '1800')),
            s3_retry_attempts=int(os.environ.get('TEST_S3_RETRIES', '3')),
            test_retry_attempts=int(os.environ.get('TEST_RETRIES', '2'))
        )
    
    def get_job_name(self, test_name: str, execution_number: int = 1) -> str:
        """Generate a unique job name for testing."""
        return f"{self.job_name_prefix}-{test_name}-{self.test_run_id}-exec-{execution_number}"
    
    def get_table_name(self, base_name: str, unique_suffix: bool = True) -> str:
        """Generate a unique table name for testing."""
        if unique_suffix:
            import time
            return f"{base_name}_{int(time.time())}_{self.test_run_id[:4]}"
        else:
            return f"{base_name}_{self.test_run_id[:4]}"
    
    def get_s3_key(self, job_name: str, table_name: str) -> str:
        """Generate S3 key for bookmark file."""
        return f"{self.test_prefix}{job_name}/{table_name}.json"


class TestDataGenerator:
    """Generates test data for integration tests."""
    
    @staticmethod
    def generate_bookmark_data(table_name: str, job_name: str, 
                             incremental_strategy: str = "timestamp",
                             last_processed_value: Any = None,
                             is_first_run: bool = False) -> Dict[str, Any]:
        """Generate valid bookmark data for testing."""
        from datetime import datetime, timezone
        
        now = datetime.now(timezone.utc)
        
        return {
            "table_name": table_name,
            "incremental_strategy": incremental_strategy,
            "incremental_column": "updated_at" if incremental_strategy == "timestamp" else "id",
            "last_processed_value": last_processed_value,
            "last_update_timestamp": now.isoformat(),
            "is_first_run": is_first_run,
            "job_name": job_name,
            "created_timestamp": now.isoformat(),
            "updated_timestamp": now.isoformat(),
            "version": "1.0"
        }
    
    @staticmethod
    def generate_corrupted_bookmark_data(corruption_type: str, table_name: str = "test_table") -> str:
        """Generate corrupted bookmark data for testing recovery scenarios."""
        import json
        
        if corruption_type == "invalid_json":
            return '{"table_name": "' + table_name + '", "invalid": json syntax}'
        elif corruption_type == "missing_required_fields":
            return json.dumps({"some_field": "value", "other_field": 123})
        elif corruption_type == "invalid_data_types":
            return json.dumps({
                "table_name": table_name,
                "is_first_run": "not_a_boolean",
                "last_processed_value": {"invalid": "object"},
                "incremental_strategy": 123
            })
        elif corruption_type == "empty_file":
            return ""
        elif corruption_type == "malformed_structure":
            return json.dumps([{"wrong": "structure"}])
        else:
            return "completely invalid content that is not JSON"
    
    @staticmethod
    def generate_table_list(count: int, prefix: str = "table") -> list:
        """Generate a list of unique table names."""
        import time
        timestamp = int(time.time())
        return [f"{prefix}_{i:03d}_{timestamp}" for i in range(count)]


class TestAssertions:
    """Custom assertions for integration tests."""
    
    @staticmethod
    def assert_bookmark_file_exists(s3_client, bucket: str, key: str):
        """Assert that a bookmark file exists in S3."""
        from botocore.exceptions import ClientError
        
        try:
            response = s3_client.head_object(Bucket=bucket, Key=key)
            assert response['ContentType'] == 'application/json'
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                raise AssertionError(f"Bookmark file does not exist: s3://{bucket}/{key}")
            else:
                raise AssertionError(f"Error checking bookmark file: {e}")
    
    @staticmethod
    def assert_bookmark_file_not_exists(s3_client, bucket: str, key: str):
        """Assert that a bookmark file does not exist in S3."""
        from botocore.exceptions import ClientError
        
        try:
            s3_client.head_object(Bucket=bucket, Key=key)
            raise AssertionError(f"Bookmark file should not exist: s3://{bucket}/{key}")
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                # Expected - file does not exist
                pass
            else:
                raise AssertionError(f"Error checking bookmark file: {e}")
    
    @staticmethod
    def assert_bookmark_content_valid(s3_client, bucket: str, key: str, expected_table_name: str):
        """Assert that bookmark file content is valid."""
        import json
        from botocore.exceptions import ClientError
        
        try:
            response = s3_client.get_object(Bucket=bucket, Key=key)
            content = json.loads(response['Body'].read().decode('utf-8'))
            
            # Validate required fields
            required_fields = ['table_name', 'incremental_strategy', 'is_first_run']
            for field in required_fields:
                assert field in content, f"Missing required field: {field}"
            
            # Validate table name matches
            assert content['table_name'] == expected_table_name, \
                f"Table name mismatch: expected {expected_table_name}, got {content['table_name']}"
            
            # Validate data types
            assert isinstance(content['is_first_run'], bool), "is_first_run must be boolean"
            assert isinstance(content['table_name'], str), "table_name must be string"
            assert isinstance(content['incremental_strategy'], str), "incremental_strategy must be string"
            
        except ClientError as e:
            raise AssertionError(f"Error reading bookmark file: {e}")
        except json.JSONDecodeError as e:
            raise AssertionError(f"Invalid JSON in bookmark file: {e}")
    
    @staticmethod
    def assert_performance_within_limits(operation_time: float, max_time: float, operation_name: str):
        """Assert that operation performance is within acceptable limits."""
        assert operation_time <= max_time, \
            f"{operation_name} took {operation_time:.2f}s, which exceeds limit of {max_time}s"
    
    @staticmethod
    def assert_s3_operation_count(s3_client, bucket: str, prefix: str, expected_count: int):
        """Assert that the expected number of S3 objects exist with given prefix."""
        try:
            response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
            actual_count = len(response.get('Contents', []))
            
            assert actual_count == expected_count, \
                f"Expected {expected_count} objects with prefix '{prefix}', found {actual_count}"
                
        except Exception as e:
            raise AssertionError(f"Error counting S3 objects: {e}")


class TestMetrics:
    """Collects and reports test metrics."""
    
    def __init__(self):
        self.metrics = {}
    
    def record_operation_time(self, operation: str, duration: float):
        """Record the duration of an operation."""
        if operation not in self.metrics:
            self.metrics[operation] = []
        self.metrics[operation].append(duration)
    
    def get_average_time(self, operation: str) -> float:
        """Get average time for an operation."""
        if operation not in self.metrics or not self.metrics[operation]:
            return 0.0
        return sum(self.metrics[operation]) / len(self.metrics[operation])
    
    def get_total_time(self, operation: str) -> float:
        """Get total time for an operation."""
        if operation not in self.metrics:
            return 0.0
        return sum(self.metrics[operation])
    
    def get_operation_count(self, operation: str) -> int:
        """Get count of operations."""
        if operation not in self.metrics:
            return 0
        return len(self.metrics[operation])
    
    def print_summary(self):
        """Print a summary of collected metrics."""
        print("\nTest Performance Metrics:")
        print("=" * 40)
        
        for operation, times in self.metrics.items():
            if times:
                avg_time = sum(times) / len(times)
                total_time = sum(times)
                count = len(times)
                
                print(f"{operation}:")
                print(f"  Count: {count}")
                print(f"  Total Time: {total_time:.2f}s")
                print(f"  Average Time: {avg_time:.3f}s")
                print(f"  Min Time: {min(times):.3f}s")
                print(f"  Max Time: {max(times):.3f}s")
                print()


# Global test configuration instance
test_config = IntegrationTestConfig.from_environment()

# Global test metrics collector
test_metrics = TestMetrics()

# Test data generator instance
data_generator = TestDataGenerator()

# Test assertions helper
assertions = TestAssertions()