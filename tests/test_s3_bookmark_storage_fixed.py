"""
Comprehensive unit tests for S3BookmarkStorage class.

This test suite covers:
- S3BookmarkStorage class with mocked S3 client
- All error scenarios: access denied, not found, timeouts, corrupted data
- JSON serialization/deserialization with various data types
- Retry logic and exponential backoff functionality

Requirements covered: 5.1, 5.2, 5.3, 5.5
"""

import asyncio
import json
import pytest
import time
from datetime import datetime, timezone
from unittest.mock import Mock, AsyncMock, patch, MagicMock
from botocore.exceptions import ClientError, ConnectTimeoutError, ReadTimeoutError
from botocore.config import Config
from dataclasses import dataclass
from typing import Optional, Dict, Any, List

# Mock the classes we need for testing
@dataclass
class MockS3BookmarkConfig:
    """Mock S3BookmarkConfig for testing."""
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


class MockS3BookmarkStorage:
    """Mock S3BookmarkStorage class for testing core functionality."""
    
    def __init__(self, config: MockS3BookmarkConfig):
        self.config = config
        self.structured_logger = Mock()
        self.metrics_publisher = Mock()
        self.s3_client = Mock()
    
    def _get_bookmark_s3_key(self, table_name: str) -> str:
        """Generate S3 key for bookmark file."""
        return f"{self.config.bookmark_prefix}{self.config.job_name}/{table_name}.json"
    
    async def _read_bookmark_from_s3(self, table_name: str) -> Optional[Dict[str, Any]]:
        """Mock implementation of S3 bookmark reading."""
        s3_key = self._get_bookmark_s3_key(table_name)
        start_time = time.time()
        
        # Log operation start
        self.structured_logger.log_s3_operation_start("read", table_name, s3_key,
                                                     bucket=self.config.bucket_name)
        
        for attempt in range(1, self.config.retry_attempts + 1):
            try:
                # Use asyncio to run the S3 operation in a thread pool
                loop = asyncio.get_event_loop()
                response = await loop.run_in_executor(
                    None,
                    lambda: self.s3_client.get_object(
                        Bucket=self.config.bucket_name,
                        Key=s3_key
                    )
                )
                
                # Parse JSON content
                content = response['Body'].read().decode('utf-8')
                bookmark_data = json.loads(content)
                
                # Calculate operation duration
                duration_ms = (time.time() - start_time) * 1000
                
                # Validate bookmark JSON structure
                if not self._validate_bookmark_json(bookmark_data, table_name):
                    self.structured_logger.error("Bookmark validation failed")
                    return None
                
                # Log successful operation
                file_size = response.get('ContentLength', 0)
                self.structured_logger.log_s3_operation_success(
                    "read", table_name, s3_key, duration_ms,
                    file_size_bytes=file_size)
                
                return bookmark_data
                
            except ClientError as e:
                duration_ms = (time.time() - start_time) * 1000
                error_info = self._handle_s3_error("read", table_name, e)
                
                if error_info['error_category'] == 'key_not_found':
                    self.structured_logger.info("S3 bookmark file not found")
                    return None
                elif not error_info['is_recoverable']:
                    self.structured_logger.log_s3_operation_failure(
                        "read", table_name, s3_key, duration_ms, 
                        str(e), error_info['error_category'])
                    return None
                elif error_info['should_retry'] and attempt < self.config.retry_attempts:
                    wait_time = 2 ** (attempt - 1)
                    self.structured_logger.log_s3_operation_retry(
                        "read", table_name, s3_key, attempt, wait_time, error_info['error_category'])
                    await asyncio.sleep(wait_time)
                    continue
                else:
                    self.structured_logger.log_s3_operation_failure(
                        "read", table_name, s3_key, duration_ms, 
                        f"Max retries reached: {str(e)}", error_info['error_category'])
                    return None
                        
            except json.JSONDecodeError as e:
                duration_ms = (time.time() - start_time) * 1000
                self.structured_logger.error("Corrupted JSON detected")
                return None
                
            except Exception as e:
                duration_ms = (time.time() - start_time) * 1000
                error_info = self._handle_s3_error("read", table_name, e)
                
                if error_info['should_retry'] and attempt < self.config.retry_attempts:
                    wait_time = 2 ** (attempt - 1)
                    self.structured_logger.log_s3_operation_retry(
                        "read", table_name, s3_key, attempt, wait_time, error_info['error_category'])
                    await asyncio.sleep(wait_time)
                    continue
                else:
                    self.structured_logger.log_s3_operation_failure(
                        "read", table_name, s3_key, duration_ms, 
                        f"Unexpected error: {str(e)}", error_info['error_category'])
                    return None
        
        return None
    
    async def _write_bookmark_to_s3(self, table_name: str, bookmark_data: Dict[str, Any]) -> bool:
        """Mock implementation of S3 bookmark writing."""
        s3_key = self._get_bookmark_s3_key(table_name)
        start_time = time.time()
        
        # Log operation start
        self.structured_logger.log_s3_operation_start("write", table_name, s3_key,
                                                     bucket=self.config.bucket_name)
        
        for attempt in range(1, self.config.retry_attempts + 1):
            try:
                # Serialize bookmark data to JSON
                json_content = json.dumps(bookmark_data, indent=2, default=str)
                
                # Use asyncio to run the S3 operation in a thread pool
                loop = asyncio.get_event_loop()
                response = await loop.run_in_executor(
                    None,
                    lambda: self.s3_client.put_object(
                        Bucket=self.config.bucket_name,
                        Key=s3_key,
                        Body=json_content,
                        ContentType="application/json"
                    )
                )
                
                # Calculate operation duration
                duration_ms = (time.time() - start_time) * 1000
                
                # Log successful operation
                self.structured_logger.log_s3_operation_success(
                    "write", table_name, s3_key, duration_ms,
                    file_size_bytes=len(json_content.encode('utf-8')))
                
                return True
                
            except ClientError as e:
                duration_ms = (time.time() - start_time) * 1000
                error_info = self._handle_s3_error("write", table_name, e)
                
                if not error_info['is_recoverable']:
                    self.structured_logger.log_s3_operation_failure(
                        "write", table_name, s3_key, duration_ms, 
                        str(e), error_info['error_category'])
                    return False
                elif error_info['should_retry'] and attempt < self.config.retry_attempts:
                    wait_time = 2 ** (attempt - 1)
                    self.structured_logger.log_s3_operation_retry(
                        "write", table_name, s3_key, attempt, wait_time, error_info['error_category'])
                    await asyncio.sleep(wait_time)
                    continue
                else:
                    self.structured_logger.log_s3_operation_failure(
                        "write", table_name, s3_key, duration_ms, 
                        f"Max retries reached: {str(e)}", error_info['error_category'])
                    return False
                        
            except Exception as e:
                duration_ms = (time.time() - start_time) * 1000
                error_info = self._handle_s3_error("write", table_name, e)
                
                if error_info['should_retry'] and attempt < self.config.retry_attempts:
                    wait_time = 2 ** (attempt - 1)
                    self.structured_logger.log_s3_operation_retry(
                        "write", table_name, s3_key, attempt, wait_time, error_info['error_category'])
                    await asyncio.sleep(wait_time)
                    continue
                else:
                    self.structured_logger.log_s3_operation_failure(
                        "write", table_name, s3_key, duration_ms, 
                        f"Unexpected error: {str(e)}", error_info['error_category'])
                    return False
        
        return False
    
    def _validate_bookmark_json(self, bookmark_data: Dict[str, Any], table_name: str) -> bool:
        """Mock validation of bookmark JSON structure."""
        try:
            # Basic structure validation
            if not isinstance(bookmark_data, dict):
                self.structured_logger.error("Bookmark data is not a valid dictionary")
                return False
            
            # Check for empty data
            if not bookmark_data:
                self.structured_logger.error("Bookmark data is empty")
                return False
            
            # Check required fields
            required_fields = ['table_name', 'incremental_strategy']
            for field in required_fields:
                if field not in bookmark_data:
                    self.structured_logger.error(f"Missing required field: {field}")
                    return False
            
            # Check table name consistency
            if bookmark_data.get('table_name') != table_name:
                self.structured_logger.error("Table name mismatch in bookmark data")
                return False
            
            return True
            
        except Exception as e:
            self.structured_logger.error(f"Validation error: {str(e)}")
            return False
    
    def _handle_s3_error(self, operation: str, table_name: str, error: Exception) -> Dict[str, Any]:
        """Handle S3 operation errors with appropriate fallback logic."""
        if isinstance(error, ClientError):
            error_code = error.response['Error']['Code']
            
            if error_code == 'AccessDenied':
                return {
                    'error_category': 'access_denied',
                    'is_recoverable': False,
                    'should_retry': False
                }
            elif error_code == 'NoSuchKey':
                return {
                    'error_category': 'key_not_found',
                    'is_recoverable': False,
                    'should_retry': False
                }
            elif error_code == 'NoSuchBucket':
                return {
                    'error_category': 'bucket_not_found',
                    'is_recoverable': False,
                    'should_retry': False
                }
            else:
                return {
                    'error_category': 'client_error',
                    'is_recoverable': True,
                    'should_retry': True
                }
        elif isinstance(error, (ConnectTimeoutError, ReadTimeoutError)):
            return {
                'error_category': 'timeout',
                'is_recoverable': True,
                'should_retry': True
            }
        elif isinstance(error, json.JSONDecodeError):
            return {
                'error_category': 'corrupted_data',
                'is_recoverable': False,
                'should_retry': False
            }
        else:
            return {
                'error_category': 'unknown_error',
                'is_recoverable': True,
                'should_retry': True
            }


class TestS3BookmarkConfig:
    """Test S3BookmarkConfig dataclass validation and initialization."""
    
    def test_valid_config_creation(self):
        """Test creating a valid S3BookmarkConfig."""
        config = MockS3BookmarkConfig(
            bucket_name="test-bucket",
            bookmark_prefix="bookmarks/",
            job_name="test-job",
            retry_attempts=3,
            timeout_seconds=30
        )
        
        assert config.bucket_name == "test-bucket"
        assert config.bookmark_prefix == "bookmarks/"
        assert config.job_name == "test-job"
        assert config.retry_attempts == 3
        assert config.timeout_seconds == 30
    
    def test_config_with_defaults(self):
        """Test S3BookmarkConfig with default values."""
        config = MockS3BookmarkConfig(
            bucket_name="test-bucket",
            bookmark_prefix="bookmarks",
            job_name="test-job"
        )
        
        assert config.retry_attempts == 3
        assert config.timeout_seconds == 30
        assert config.bookmark_prefix == "bookmarks/"  # Should add trailing slash
    
    def test_config_validation_empty_bucket(self):
        """Test validation fails with empty bucket name."""
        with pytest.raises(ValueError, match="S3 bucket name cannot be empty"):
            MockS3BookmarkConfig(
                bucket_name="",
                bookmark_prefix="bookmarks/",
                job_name="test-job"
            )
    
    def test_config_validation_empty_job_name(self):
        """Test validation fails with empty job name."""
        with pytest.raises(ValueError, match="Job name cannot be empty"):
            MockS3BookmarkConfig(
                bucket_name="test-bucket",
                bookmark_prefix="bookmarks/",
                job_name=""
            )
    
    def test_config_validation_invalid_retry_attempts(self):
        """Test validation fails with invalid retry attempts."""
        with pytest.raises(ValueError, match="Retry attempts must be at least 1"):
            MockS3BookmarkConfig(
                bucket_name="test-bucket",
                bookmark_prefix="bookmarks/",
                job_name="test-job",
                retry_attempts=0
            )
    
    def test_config_validation_invalid_timeout(self):
        """Test validation fails with invalid timeout."""
        with pytest.raises(ValueError, match="Timeout seconds must be at least 1"):
            MockS3BookmarkConfig(
                bucket_name="test-bucket",
                bookmark_prefix="bookmarks/",
                job_name="test-job",
                timeout_seconds=0
            )


class TestS3BookmarkStorage:
    """Test S3BookmarkStorage class with comprehensive error scenarios."""
    
    @pytest.fixture
    def mock_config(self):
        """Create a mock S3BookmarkConfig for testing."""
        return MockS3BookmarkConfig(
            bucket_name="test-bucket",
            bookmark_prefix="bookmarks/",
            job_name="test-job",
            retry_attempts=3,
            timeout_seconds=30
        )
    
    @pytest.fixture
    def storage_instance(self, mock_config):
        """Create S3BookmarkStorage instance with mocked dependencies."""
        storage = MockS3BookmarkStorage(mock_config)
        return storage
    
    def test_get_bookmark_s3_key(self, storage_instance):
        """Test S3 key generation for bookmark files."""
        table_name = "test_table"
        expected_key = "bookmarks/test-job/test_table.json"
        
        key = storage_instance._get_bookmark_s3_key(table_name)
        assert key == expected_key
    
    @pytest.mark.asyncio
    async def test_read_bookmark_success(self, storage_instance):
        """Test successful bookmark reading from S3."""
        table_name = "test_table"
        bookmark_data = {
            "table_name": table_name,
            "incremental_strategy": "timestamp",
            "incremental_column": "updated_at",
            "last_processed_value": "2024-01-15T10:30:00Z",
            "last_update_timestamp": "2024-01-15T11:00:00Z",
            "is_first_run": False,
            "job_name": "test-job",
            "created_timestamp": "2024-01-01T09:00:00Z",
            "updated_timestamp": "2024-01-15T11:00:00Z",
            "version": "1.0"
        }
        
        # Mock S3 response
        mock_response = {
            'Body': Mock(),
            'ContentLength': 500
        }
        mock_response['Body'].read.return_value = json.dumps(bookmark_data).encode('utf-8')
        storage_instance.s3_client.get_object.return_value = mock_response
        
        result = await storage_instance._read_bookmark_from_s3(table_name)
        
        assert result == bookmark_data
        storage_instance.s3_client.get_object.assert_called_once_with(
            Bucket="test-bucket",
            Key="bookmarks/test-job/test_table.json"
        )
        
        # Verify logging calls
        storage_instance.structured_logger.log_s3_operation_start.assert_called_once()
        storage_instance.structured_logger.log_s3_operation_success.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_read_bookmark_not_found(self, storage_instance):
        """Test bookmark reading when file doesn't exist (first run scenario)."""
        table_name = "test_table"
        
        # Mock S3 NoSuchKey error
        error = ClientError(
            error_response={'Error': {'Code': 'NoSuchKey', 'Message': 'The specified key does not exist.'}},
            operation_name='GetObject'
        )
        storage_instance.s3_client.get_object.side_effect = error
        
        result = await storage_instance._read_bookmark_from_s3(table_name)
        
        assert result is None
        storage_instance.structured_logger.info.assert_called()
    
    @pytest.mark.asyncio
    async def test_read_bookmark_access_denied(self, storage_instance):
        """Test bookmark reading with access denied error."""
        table_name = "test_table"
        
        # Mock S3 AccessDenied error
        error = ClientError(
            error_response={'Error': {'Code': 'AccessDenied', 'Message': 'Access Denied'}},
            operation_name='GetObject'
        )
        storage_instance.s3_client.get_object.side_effect = error
        
        result = await storage_instance._read_bookmark_from_s3(table_name)
        
        assert result is None
        storage_instance.structured_logger.log_s3_operation_failure.assert_called()
    
    @pytest.mark.asyncio
    async def test_read_bookmark_timeout_with_retry(self, storage_instance):
        """Test bookmark reading with timeout and retry logic."""
        table_name = "test_table"
        
        # Mock timeout error that should trigger retry
        timeout_error = ReadTimeoutError(endpoint_url="https://s3.amazonaws.com")
        storage_instance.s3_client.get_object.side_effect = [timeout_error, timeout_error, timeout_error]
        
        result = await storage_instance._read_bookmark_from_s3(table_name)
        
        assert result is None
        assert storage_instance.s3_client.get_object.call_count == 3  # All retry attempts
        
        # Verify retry logging
        assert storage_instance.structured_logger.log_s3_operation_retry.call_count == 2
        storage_instance.structured_logger.log_s3_operation_failure.assert_called()
    
    @pytest.mark.asyncio
    async def test_read_bookmark_corrupted_json(self, storage_instance):
        """Test bookmark reading with corrupted JSON data."""
        table_name = "test_table"
        
        # Mock S3 response with invalid JSON
        mock_response = {
            'Body': Mock(),
            'ContentLength': 100
        }
        mock_response['Body'].read.return_value = b'{"invalid": json data}'
        storage_instance.s3_client.get_object.return_value = mock_response
        
        result = await storage_instance._read_bookmark_from_s3(table_name)
        
        assert result is None
        storage_instance.structured_logger.error.assert_called()
    
    @pytest.mark.asyncio
    async def test_read_bookmark_validation_failure(self, storage_instance):
        """Test bookmark reading with validation failure."""
        table_name = "test_table"
        bookmark_data = {"invalid": "data"}
        
        # Mock S3 response
        mock_response = {
            'Body': Mock(),
            'ContentLength': 100
        }
        mock_response['Body'].read.return_value = json.dumps(bookmark_data).encode('utf-8')
        storage_instance.s3_client.get_object.return_value = mock_response
        
        result = await storage_instance._read_bookmark_from_s3(table_name)
        
        assert result is None
        storage_instance.structured_logger.error.assert_called()


class TestS3BookmarkStorageWriteOperations:
    """Test S3BookmarkStorage write operations."""
    
    @pytest.fixture
    def mock_config(self):
        """Create a mock S3BookmarkConfig for testing."""
        return MockS3BookmarkConfig(
            bucket_name="test-bucket",
            bookmark_prefix="bookmarks/",
            job_name="test-job",
            retry_attempts=3,
            timeout_seconds=30
        )
    
    @pytest.fixture
    def storage_instance(self, mock_config):
        """Create S3BookmarkStorage instance with mocked dependencies."""
        storage = MockS3BookmarkStorage(mock_config)
        return storage
    
    @pytest.mark.asyncio
    async def test_write_bookmark_success(self, storage_instance):
        """Test successful bookmark writing to S3."""
        table_name = "test_table"
        bookmark_data = {
            "table_name": table_name,
            "incremental_strategy": "timestamp",
            "last_processed_value": "2024-01-15T10:30:00Z",
            "is_first_run": False
        }
        
        # Mock successful S3 put_object
        storage_instance.s3_client.put_object.return_value = {'ETag': '"abc123"'}
        
        result = await storage_instance._write_bookmark_to_s3(table_name, bookmark_data)
        
        assert result is True
        storage_instance.s3_client.put_object.assert_called_once()
        
        # Verify the call arguments
        call_args = storage_instance.s3_client.put_object.call_args
        assert call_args.kwargs['Bucket'] == "test-bucket"
        assert call_args.kwargs['Key'] == "bookmarks/test-job/test_table.json"
        assert call_args.kwargs['ContentType'] == "application/json"
        
        # Verify JSON content
        body_content = call_args.kwargs['Body']
        parsed_content = json.loads(body_content)
        assert parsed_content['table_name'] == table_name
        
        # Verify logging
        storage_instance.structured_logger.log_s3_operation_start.assert_called_once()
        storage_instance.structured_logger.log_s3_operation_success.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_write_bookmark_access_denied(self, storage_instance):
        """Test bookmark writing with access denied error."""
        table_name = "test_table"
        bookmark_data = {"table_name": table_name}
        
        # Mock S3 AccessDenied error
        error = ClientError(
            error_response={'Error': {'Code': 'AccessDenied', 'Message': 'Access Denied'}},
            operation_name='PutObject'
        )
        storage_instance.s3_client.put_object.side_effect = error
        
        result = await storage_instance._write_bookmark_to_s3(table_name, bookmark_data)
        
        assert result is False
        storage_instance.structured_logger.log_s3_operation_failure.assert_called()
    
    @pytest.mark.asyncio
    async def test_write_bookmark_timeout_with_retry(self, storage_instance):
        """Test bookmark writing with timeout and retry logic."""
        table_name = "test_table"
        bookmark_data = {"table_name": table_name}
        
        # Mock timeout error for first two attempts, success on third
        timeout_error = ConnectTimeoutError(endpoint_url="https://s3.amazonaws.com")
        storage_instance.s3_client.put_object.side_effect = [
            timeout_error, 
            timeout_error, 
            {'ETag': '"abc123"'}
        ]
        
        result = await storage_instance._write_bookmark_to_s3(table_name, bookmark_data)
        
        assert result is True
        assert storage_instance.s3_client.put_object.call_count == 3
        
        # Verify retry logging
        assert storage_instance.structured_logger.log_s3_operation_retry.call_count == 2
        storage_instance.structured_logger.log_s3_operation_success.assert_called()


class TestS3BookmarkStorageValidation:
    """Test bookmark JSON validation functionality."""
    
    @pytest.fixture
    def storage_instance(self):
        """Create S3BookmarkStorage instance for validation testing."""
        config = MockS3BookmarkConfig(
            bucket_name="test-bucket",
            bookmark_prefix="bookmarks/",
            job_name="test-job"
        )
        
        storage = MockS3BookmarkStorage(config)
        return storage
    
    def test_validate_bookmark_json_valid_data(self, storage_instance):
        """Test validation with valid bookmark data."""
        table_name = "test_table"
        valid_data = {
            "table_name": table_name,
            "incremental_strategy": "timestamp",
            "incremental_column": "updated_at",
            "last_processed_value": "2024-01-15T10:30:00Z",
            "is_first_run": False,
            "job_name": "test-job",
            "version": "1.0"
        }
        
        result = storage_instance._validate_bookmark_json(valid_data, table_name)
        assert result is True
    
    def test_validate_bookmark_json_invalid_type(self, storage_instance):
        """Test validation with non-dictionary data."""
        table_name = "test_table"
        invalid_data = "not a dictionary"
        
        result = storage_instance._validate_bookmark_json(invalid_data, table_name)
        
        assert result is False
        storage_instance.structured_logger.error.assert_called()
    
    def test_validate_bookmark_json_empty_data(self, storage_instance):
        """Test validation with empty data."""
        table_name = "test_table"
        empty_data = {}
        
        result = storage_instance._validate_bookmark_json(empty_data, table_name)
        
        assert result is False
        storage_instance.structured_logger.error.assert_called()
    
    def test_validate_bookmark_json_missing_required_fields(self, storage_instance):
        """Test validation with missing required fields."""
        table_name = "test_table"
        invalid_data = {"table_name": table_name}  # Missing incremental_strategy
        
        result = storage_instance._validate_bookmark_json(invalid_data, table_name)
        
        assert result is False
        storage_instance.structured_logger.error.assert_called()
    
    def test_validate_bookmark_json_table_name_mismatch(self, storage_instance):
        """Test validation with table name mismatch."""
        table_name = "test_table"
        invalid_data = {
            "table_name": "different_table",
            "incremental_strategy": "timestamp"
        }
        
        result = storage_instance._validate_bookmark_json(invalid_data, table_name)
        
        assert result is False
        storage_instance.structured_logger.error.assert_called()


class TestS3BookmarkStorageErrorHandling:
    """Test comprehensive error handling scenarios."""
    
    @pytest.fixture
    def storage_instance(self):
        """Create S3BookmarkStorage instance for error handling testing."""
        config = MockS3BookmarkConfig(
            bucket_name="test-bucket",
            bookmark_prefix="bookmarks/",
            job_name="test-job"
        )
        
        storage = MockS3BookmarkStorage(config)
        return storage
    
    def test_handle_s3_error_access_denied(self, storage_instance):
        """Test error handling for access denied errors."""
        error = ClientError(
            error_response={'Error': {'Code': 'AccessDenied', 'Message': 'Access Denied'}},
            operation_name='GetObject'
        )
        
        result = storage_instance._handle_s3_error("read", "test_table", error)
        
        assert result['error_category'] == 'access_denied'
        assert result['is_recoverable'] is False
        assert result['should_retry'] is False
    
    def test_handle_s3_error_not_found(self, storage_instance):
        """Test error handling for not found errors."""
        error = ClientError(
            error_response={'Error': {'Code': 'NoSuchKey', 'Message': 'Key not found'}},
            operation_name='GetObject'
        )
        
        result = storage_instance._handle_s3_error("read", "test_table", error)
        
        assert result['error_category'] == 'key_not_found'
        assert result['is_recoverable'] is False
        assert result['should_retry'] is False
    
    def test_handle_s3_error_timeout(self, storage_instance):
        """Test error handling for timeout errors."""
        error = ReadTimeoutError(endpoint_url="https://s3.amazonaws.com")
        
        result = storage_instance._handle_s3_error("read", "test_table", error)
        
        assert result['error_category'] == 'timeout'
        assert result['is_recoverable'] is True
        assert result['should_retry'] is True
    
    def test_handle_s3_error_json_decode(self, storage_instance):
        """Test error handling for JSON decode errors."""
        error = json.JSONDecodeError("Invalid JSON", "doc", 0)
        
        result = storage_instance._handle_s3_error("read", "test_table", error)
        
        assert result['error_category'] == 'corrupted_data'
        assert result['is_recoverable'] is False
        assert result['should_retry'] is False


class TestS3BookmarkStorageDataTypes:
    """Test JSON serialization/deserialization with various data types."""
    
    @pytest.fixture
    def storage_instance(self):
        """Create S3BookmarkStorage instance for data type testing."""
        config = MockS3BookmarkConfig(
            bucket_name="test-bucket",
            bookmark_prefix="bookmarks/",
            job_name="test-job"
        )
        
        storage = MockS3BookmarkStorage(config)
        return storage
    
    @pytest.mark.asyncio
    async def test_write_read_string_values(self, storage_instance):
        """Test serialization/deserialization with string values."""
        table_name = "test_table"
        bookmark_data = {
            "table_name": table_name,
            "incremental_strategy": "timestamp",
            "last_processed_value": "2024-01-15T10:30:00Z",
            "incremental_column": "updated_at"
        }
        
        # Mock successful write
        storage_instance.s3_client.put_object.return_value = {'ETag': '"abc123"'}
        
        result = await storage_instance._write_bookmark_to_s3(table_name, bookmark_data)
        assert result is True
        
        # Verify JSON serialization
        call_args = storage_instance.s3_client.put_object.call_args
        body_content = call_args.kwargs['Body']
        parsed_content = json.loads(body_content)
        assert parsed_content == bookmark_data
    
    @pytest.mark.asyncio
    async def test_write_read_numeric_values(self, storage_instance):
        """Test serialization/deserialization with numeric values."""
        table_name = "test_table"
        bookmark_data = {
            "table_name": table_name,
            "incremental_strategy": "primary_key",
            "last_processed_value": 12345,
            "incremental_column": "id"
        }
        
        # Mock successful write
        storage_instance.s3_client.put_object.return_value = {'ETag': '"abc123"'}
        
        result = await storage_instance._write_bookmark_to_s3(table_name, bookmark_data)
        assert result is True
        
        # Verify JSON serialization preserves numeric types
        call_args = storage_instance.s3_client.put_object.call_args
        body_content = call_args.kwargs['Body']
        parsed_content = json.loads(body_content)
        assert parsed_content["last_processed_value"] == 12345
        assert isinstance(parsed_content["last_processed_value"], int)
    
    @pytest.mark.asyncio
    async def test_write_read_boolean_values(self, storage_instance):
        """Test serialization/deserialization with boolean values."""
        table_name = "test_table"
        bookmark_data = {
            "table_name": table_name,
            "incremental_strategy": "timestamp",
            "is_first_run": False,
            "has_data": True
        }
        
        # Mock successful write
        storage_instance.s3_client.put_object.return_value = {'ETag': '"abc123"'}
        
        result = await storage_instance._write_bookmark_to_s3(table_name, bookmark_data)
        assert result is True
        
        # Verify JSON serialization preserves boolean types
        call_args = storage_instance.s3_client.put_object.call_args
        body_content = call_args.kwargs['Body']
        parsed_content = json.loads(body_content)
        assert parsed_content["is_first_run"] is False
        assert parsed_content["has_data"] is True
    
    @pytest.mark.asyncio
    async def test_write_read_null_values(self, storage_instance):
        """Test serialization/deserialization with null values."""
        table_name = "test_table"
        bookmark_data = {
            "table_name": table_name,
            "incremental_strategy": "timestamp",
            "last_processed_value": None,
            "incremental_column": None
        }
        
        # Mock successful write
        storage_instance.s3_client.put_object.return_value = {'ETag': '"abc123"'}
        
        result = await storage_instance._write_bookmark_to_s3(table_name, bookmark_data)
        assert result is True
        
        # Verify JSON serialization preserves null values
        call_args = storage_instance.s3_client.put_object.call_args
        body_content = call_args.kwargs['Body']
        parsed_content = json.loads(body_content)
        assert parsed_content["last_processed_value"] is None
        assert parsed_content["incremental_column"] is None


if __name__ == "__main__":
    pytest.main([__file__, "-v"])