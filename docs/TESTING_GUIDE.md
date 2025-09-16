# Testing Guide

This comprehensive testing guide covers all testing aspects of the AWS Glue Data Replication project's modular architecture, including unit tests, integration tests, performance testing, and testing best practices.

## Table of Contents

1. [Testing Overview](#testing-overview)
2. [Modular Test Structure](#modular-test-structure)
3. [Running Tests](#running-tests)
4. [Unit Testing by Module](#unit-testing-by-module)
5. [Integration Testing](#integration-testing)
6. [Test Development Guidelines](#test-development-guidelines)
7. [Common Testing Scenarios](#common-testing-scenarios)
8. [Debugging and Troubleshooting](#debugging-and-troubleshooting)
9. [CI/CD Integration](#cicd-integration)

## Testing Overview

The AWS Glue Data Replication project employs a comprehensive testing strategy designed around its modular architecture:

- **Unit Tests**: Testing individual modules and classes in isolation
- **Integration Tests**: Testing module interactions and real AWS service integration
- **Performance Tests**: Validating performance characteristics and scalability
- **End-to-End Tests**: Complete workflow validation

### Testing Philosophy

1. **Modular Testing**: Each module (`config`, `storage`, `database`, `monitoring`, `network`, `utils`) has dedicated test suites
2. **Test Pyramid**: Emphasis on unit tests with supporting integration and E2E tests
3. **Isolation**: Tests use mocking to isolate dependencies and avoid external service calls
4. **Repeatability**: Tests produce consistent results across environments
5. **Comprehensive Coverage**: All critical paths and error scenarios are tested
6. **Performance Validation**: Performance characteristics are validated and monitored

### Test Architecture

The test suite mirrors the modular structure of the application:

```
tests/
├── test_config.py          # Configuration module tests
├── test_storage.py         # Storage and bookmark tests
├── test_database.py        # Database connection and migration tests
├── test_monitoring.py      # Logging and metrics tests
├── test_network.py         # Network error handling tests
├── test_utils.py           # Utility function tests
├── test_suite.py           # Comprehensive test runner
├── run_tests.py            # Main test execution script
├── validate_tests.py       # Import validation script
└── integration/            # Integration test files
    ├── test_s3_bookmark_integration_complete.py
    ├── test_end_to_end.py
    └── test_cloudformation_integration.py
```

## Modular Test Structure

The test suite is organized to match the modular architecture of the application. Each module has dedicated test files that focus on specific functionality.

### Test Migration and Consolidation

During the project refactoring, the test suite underwent significant reorganization:

**Import Path Updates**: All test files were updated from the old monolithic imports:
```python
# Old imports
from scripts.glue_data_replication import JobConfig, ConnectionConfig

# New modular imports  
from glue_job.config import JobConfig, ConnectionConfig
from glue_job.storage import S3BookmarkStorage, JobBookmarkManager
from glue_job.database import JdbcConnectionManager
```

**Test Consolidation**: Redundant and simple test files were consolidated into comprehensive test suites:
- **Removed**: `simple_test.py`, `test_minimal.py`, `test_network_simple.py`, `simple_network_test.py`
- **Created**: Comprehensive modular test files covering all functionality areas
- **Enhanced**: Integration tests updated for new module structure

**Validation Infrastructure**: New validation scripts ensure the modular structure works correctly:
- `validate_tests.py`: Validates all module imports and basic functionality
- `test_suite.py`: Comprehensive test runner for all test categories

### Test File Organization

| Test File | Module Tested | Key Components |
|-----------|---------------|----------------|
| `test_config.py` | `glue_job.config` | JobConfig, DatabaseEngineManager, ConnectionStringBuilder |
| `test_storage.py` | `glue_job.storage` | S3BookmarkStorage, JobBookmarkManager |
| `test_database.py` | `glue_job.database` | JdbcConnectionManager, SchemaValidator, DataMigrator |
| `test_monitoring.py` | `glue_job.monitoring` | StructuredLogger, CloudWatchMetrics, ProgressTracking |
| `test_network.py` | `glue_job.network` | ErrorHandler, RetryHandler, NetworkExceptions |
| `test_utils.py` | `glue_job.utils` | S3PathUtilities, EnhancedS3Operations |

### Mock Strategy

All tests use comprehensive mocking to avoid external dependencies:

```python
# Standard mock setup for all test files
mock_modules = [
    'awsglue', 'awsglue.utils', 'awsglue.context', 'awsglue.job',
    'pyspark', 'pyspark.context', 'pyspark.sql', 'pyspark.sql.types',
    'pyspark.sql.functions', 'boto3'
]

for module in mock_modules:
    sys.modules[module] = MagicMock()
```

## Running Tests

### Quick Start

```bash
# Run all unit tests
python tests/run_tests.py

# Run comprehensive test suite
python tests/test_suite.py

# Validate module imports
python tests/validate_tests.py
```

### Individual Module Testing

```bash
# Test specific modules
python -m pytest tests/test_config.py -v
python -m pytest tests/test_storage.py -v
python -m pytest tests/test_database.py -v
python -m pytest tests/test_monitoring.py -v
python -m pytest tests/test_network.py -v
python -m pytest tests/test_utils.py -v
```

### Test with Coverage

```bash
# Install coverage if not already installed
pip install coverage pytest-cov

# Run tests with coverage report
python -m pytest tests/ --cov=src/glue_job --cov-report=html --cov-report=term

# View detailed coverage report
open htmlcov/index.html  # On macOS
# or
xdg-open htmlcov/index.html  # On Linux
```

### Integration Tests

```bash
# Run S3 bookmark integration tests
python tests/run_s3_integration_tests.py

# Run end-to-end tests
python tests/run_end_to_end_tests.py

# Run CloudFormation integration tests
python tests/run_cloudformation_tests.py
```

## Unit Testing by Module

### Configuration Module Tests (`test_config.py`)

Tests all configuration-related functionality including dataclasses, database engine management, and configuration parsing.

#### Test Classes

##### TestConnectionConfig
```python
def test_connection_config_creation(self):
    """Test creating ConnectionConfig instances with various parameters"""
    
def test_connection_config_validation(self):
    """Test validation of connection parameters"""
```

##### TestJobConfig
```python
def test_job_config_initialization(self):
    """Test JobConfig dataclass initialization"""
    
def test_job_config_with_network_settings(self):
    """Test JobConfig with NetworkConfig integration"""
```

##### TestDatabaseEngineManager
```python
def test_get_driver_class_postgresql(self):
    """Test PostgreSQL driver class retrieval"""
    
def test_get_driver_class_sqlserver(self):
    """Test SQL Server driver class retrieval"""
    
def test_build_connection_url(self):
    """Test JDBC URL construction for different engines"""
```

##### TestJobConfigurationParser
```python
def test_parse_configuration_basic(self):
    """Test basic configuration parsing from CloudFormation parameters"""
    
def test_parse_configuration_with_network(self):
    """Test configuration parsing with network settings"""
```

**Coverage Areas**:
- Dataclass creation and validation
- Database engine configuration management
- JDBC URL construction for different database types
- Configuration parsing from CloudFormation parameters
- Network configuration handling

**Running Config Tests**:
```bash
python -m pytest tests/test_config.py::TestDatabaseEngineManager -v
python -m pytest tests/test_config.py::TestJobConfigurationParser -v
```

### Storage Module Tests (`test_storage.py`)

Tests bookmark storage functionality, S3 operations, and progress tracking.

#### Test Classes

##### TestS3BookmarkConfig
```python
def test_s3_bookmark_config_creation(self):
    """Test S3BookmarkConfig initialization with various parameters"""
    
def test_config_validation(self):
    """Test configuration validation for bucket names and settings"""
```

##### TestS3BookmarkStorage
```python
def test_read_bookmark_success(self):
    """Test successful bookmark reading from S3"""
    
def test_read_bookmark_not_found(self):
    """Test handling when bookmark doesn't exist (first run scenario)"""
    
def test_write_bookmark_success(self):
    """Test successful bookmark writing to S3"""
    
def test_s3_error_handling(self):
    """Test various S3 error scenarios (access denied, timeouts)"""
```

##### TestJobBookmarkManager
```python
def test_bookmark_manager_initialization(self):
    """Test JobBookmarkManager setup with different configurations"""
    
def test_get_bookmark_state_first_run(self):
    """Test bookmark state retrieval for first-time execution"""
    
def test_update_bookmark_state(self):
    """Test bookmark state updates and persistence"""
    
def test_fallback_to_memory_storage(self):
    """Test fallback behavior when S3 is unavailable"""
```

##### TestProgressTracking
```python
def test_full_load_progress(self):
    """Test FullLoadProgress tracking functionality"""
    
def test_incremental_load_progress(self):
    """Test IncrementalLoadProgress tracking functionality"""
```

**Coverage Areas**:
- S3 bookmark configuration and validation
- Bookmark CRUD operations with S3
- Error handling and fallback mechanisms
- Progress tracking for different load types
- State management across job executions

**Running Storage Tests**:
```bash
python -m pytest tests/test_storage.py::TestS3BookmarkStorage -v
python -m pytest tests/test_storage.py::TestJobBookmarkManager -v
```

### Database Module Tests (`test_database.py`)

Tests database connection management, schema validation, and data migration functionality.

#### Test Classes

##### TestJdbcConnectionManager
```python
def test_create_connection_postgresql(self):
    """Test JDBC connection creation for PostgreSQL"""
    
def test_create_connection_sqlserver(self):
    """Test JDBC connection creation for SQL Server"""
    
def test_validate_connection(self):
    """Test database connection validation"""
    
def test_connection_error_handling(self):
    """Test handling of connection failures"""
```

##### TestGlueConnectionManager
```python
def test_glue_connection_setup(self):
    """Test Glue connection configuration"""
    
def test_glue_connection_validation(self):
    """Test Glue connection validation"""
```

##### TestSchemaCompatibilityValidator
```python
def test_validate_schema_compatibility(self):
    """Test schema compatibility validation between source and target"""
    
def test_data_type_mapping(self):
    """Test data type mapping between different database engines"""
    
def test_schema_mismatch_detection(self):
    """Test detection of schema incompatibilities"""
```

##### TestDataMigrator
```python
def test_full_load_migration(self):
    """Test full-load data migration process"""
    
def test_incremental_migration(self):
    """Test incremental data migration process"""
    
def test_migration_error_handling(self):
    """Test error handling during migration"""
```

##### TestIncrementalColumnDetector
```python
def test_detect_incremental_column(self):
    """Test automatic detection of incremental columns"""
    
def test_validate_incremental_column(self):
    """Test validation of specified incremental columns"""
```

**Coverage Areas**:
- JDBC and Glue connection management
- Schema validation and compatibility checking
- Data type mapping between database engines
- Full-load and incremental migration processes
- Incremental column detection and validation
- Error handling for database operations

**Running Database Tests**:
```bash
python -m pytest tests/test_database.py::TestJdbcConnectionManager -v
python -m pytest tests/test_database.py::TestSchemaCompatibilityValidator -v
```

### Monitoring Module Tests (`test_monitoring.py`)

Tests logging, metrics publishing, and performance monitoring functionality.

#### Test Classes

##### TestStructuredLogger
```python
def test_logger_initialization(self):
    """Test StructuredLogger initialization with job context"""
    
def test_log_info_message(self):
    """Test structured info logging with context"""
    
def test_log_error_with_exception(self):
    """Test error logging with exception details"""
    
def test_log_performance_metrics(self):
    """Test performance metric logging"""
```

##### TestCloudWatchMetricsPublisher
```python
def test_publish_metrics_success(self):
    """Test successful metrics publishing to CloudWatch"""
    
def test_publish_metrics_error_handling(self):
    """Test error handling when CloudWatch is unavailable"""
    
def test_batch_metrics_publishing(self):
    """Test batch publishing of multiple metrics"""
```

##### TestProcessingMetrics
```python
def test_processing_metrics_calculation(self):
    """Test calculation of processing metrics (rows, duration, etc.)"""
    
def test_metrics_aggregation(self):
    """Test aggregation of metrics across multiple operations"""
```

##### TestPerformanceMonitor
```python
def test_performance_monitoring(self):
    """Test performance monitoring and threshold detection"""
    
def test_performance_alerts(self):
    """Test performance alert generation"""
```

**Coverage Areas**:
- Structured logging with job context
- CloudWatch metrics publishing
- Performance monitoring and alerting
- Metrics calculation and aggregation
- Error handling in monitoring operations

**Running Monitoring Tests**:
```bash
python -m pytest tests/test_monitoring.py::TestStructuredLogger -v
python -m pytest tests/test_monitoring.py::TestCloudWatchMetricsPublisher -v
```

### Network Module Tests (`test_network.py`)

Tests network error handling, retry logic, and connection recovery mechanisms.

#### Test Classes

##### TestNetworkExceptions
```python
def test_network_connectivity_error(self):
    """Test NetworkConnectivityError exception handling"""
    
def test_glue_connection_error(self):
    """Test GlueConnectionError exception handling"""
    
def test_vpc_endpoint_error(self):
    """Test VpcEndpointError exception handling"""
```

##### TestErrorClassifier
```python
def test_classify_retryable_errors(self):
    """Test classification of retryable network errors"""
    
def test_classify_non_retryable_errors(self):
    """Test classification of non-retryable errors"""
```

##### TestConnectionRetryHandler
```python
def test_retry_with_exponential_backoff(self):
    """Test retry logic with exponential backoff"""
    
def test_max_retry_limit(self):
    """Test maximum retry limit enforcement"""
    
def test_retry_success_after_failures(self):
    """Test successful operation after initial failures"""
```

##### TestErrorRecoveryManager
```python
def test_error_recovery_strategies(self):
    """Test different error recovery strategies"""
    
def test_fallback_mechanisms(self):
    """Test fallback mechanisms for critical failures"""
```

**Coverage Areas**:
- Custom network exception classes
- Error classification (retryable vs non-retryable)
- Retry logic with exponential backoff
- Error recovery strategies
- Fallback mechanisms

**Running Network Tests**:
```bash
python -m pytest tests/test_network.py::TestConnectionRetryHandler -v
python -m pytest tests/test_network.py::TestErrorClassifier -v
```

### Utils Module Tests (`test_utils.py`)

Tests utility functions for S3 operations and path handling.

#### Test Classes

##### TestS3PathUtilities
```python
def test_parse_s3_path_valid(self):
    """Test parsing of valid S3 paths"""
    
def test_parse_s3_path_invalid(self):
    """Test handling of invalid S3 path formats"""
    
def test_extract_bucket_from_jdbc_path(self):
    """Test bucket extraction from JDBC paths"""
    
def test_validate_bucket_access(self):
    """Test S3 bucket access validation"""
```

##### TestEnhancedS3ParallelOperations
```python
def test_parallel_s3_operations(self):
    """Test parallel S3 operations functionality"""
    
def test_s3_operation_error_handling(self):
    """Test error handling in S3 operations"""
    
def test_s3_performance_optimization(self):
    """Test S3 performance optimization features"""
```

**Coverage Areas**:
- S3 path parsing and validation
- Bucket access validation
- Parallel S3 operations
- S3 error handling
- Performance optimization

**Running Utils Tests**:
```bash
python -m pytest tests/test_utils.py::TestS3PathUtilities -v
python -m pytest tests/test_utils.py::TestEnhancedS3ParallelOperations -v
```

## Integration Testing

Integration tests validate the interaction between modules and with real AWS services.

### S3 Bookmark Integration Tests

**File**: `test_s3_bookmark_integration_complete.py`

#### Prerequisites
```bash
# Install test dependencies
pip install -r requirements-test.txt

# Configure AWS credentials
aws configure
# or set environment variables
export AWS_ACCESS_KEY_ID=your_key
export AWS_SECRET_ACCESS_KEY=your_secret
export AWS_DEFAULT_REGION=us-east-1

# Set test bucket (optional)
export TEST_S3_BUCKET=my-integration-test-bucket
```

#### Running Integration Tests
```bash
# Run all S3 integration tests
python tests/run_s3_integration_tests.py

# Run with custom configuration
python tests/run_s3_integration_tests.py --bucket my-test-bucket --region us-west-2

# Run specific integration test classes
python -m pytest tests/test_s3_bookmark_integration_complete.py::TestS3BookmarkIntegrationComplete -v
```

#### Test Scenarios
- Real S3 bucket operations (create, read, write, delete)
- IAM permission validation
- Cross-region S3 operations
- Large-scale bookmark operations
- Concurrent access patterns
- Error recovery and fallback mechanisms

### End-to-End Tests

**File**: `test_end_to_end.py`

```bash
# Run complete workflow tests
python tests/run_end_to_end_tests.py

# Test with specific database configurations
python tests/run_end_to_end_tests.py --source postgresql --target sqlserver
```

### Manual Bookmark Configuration Integration Tests

**File**: `test_manual_bookmark_config_integration.py`

#### Test Coverage
The manual bookmark configuration integration tests provide comprehensive validation of:

- **Real Database Connections**: Mock JDBC connections simulating PostgreSQL, Oracle, and SQL Server
- **JDBC Metadata Querying**: Real-time metadata validation and caching
- **Database Engine Compatibility**: Engine-specific type mapping and validation
- **Performance Testing**: Large-scale configuration performance and concurrent processing
- **Error Handling**: Invalid configuration handling and fallback mechanisms

#### Running Manual Bookmark Configuration Tests
```bash
# Run all manual bookmark configuration integration tests
python -m pytest tests/test_manual_bookmark_config_integration.py -v

# Run specific test categories
python -m pytest tests/test_manual_bookmark_config_integration.py::TestManualBookmarkConfigIntegration -v
python -m pytest tests/test_manual_bookmark_config_integration.py::TestManualBookmarkConfigPerformance -v

# Run with performance metrics
python -m pytest tests/test_manual_bookmark_config_integration.py -v --tb=short
```

#### Test Scenarios

**Database Engine Testing:**
- PostgreSQL metadata integration with BOOL, INTEGER, TIMESTAMP types
- Oracle metadata integration with NUMBER, DATE, CLOB types  
- SQL Server metadata integration with DATETIME2, BIT, DECIMAL types
- Database-specific type mapping validation

**Performance Testing:**
- JDBC metadata query performance impact (20+ tables)
- Concurrent metadata queries (10+ tables with ThreadPoolExecutor)
- Large-scale configuration performance (100+ tables)
- Memory usage with large cache (500+ entries)

**End-to-End Testing:**
- Complete manual configuration parsing workflow
- Bookmark state creation with manual configuration
- Integration with JobBookmarkManager
- Mixed manual and automatic detection scenarios

**Error Handling:**
- Invalid manual configuration handling
- Column not found scenarios
- JDBC connection error recovery
- Fallback to automatic detection

#### Performance Benchmarks
The integration tests validate performance within acceptable limits:
- Individual metadata queries: < 1ms each
- Sequential processing (20 tables): < 5 seconds
- Concurrent processing (10 tables): < 3 seconds
- Large-scale processing (100 tables): < 5 seconds
- Cache operations: < 100ms for cache clearing

#### Mock Infrastructure
The tests use sophisticated mock infrastructure:
- `MockJDBCConnection`: Simulates real database connections
- `MockJDBCMetadata`: Provides realistic metadata responses
- `MockJDBCResultSet`: Handles JDBC result set operations
- Database-specific type adjustments for different engines

### CloudFormation Integration Tests

**File**: `test_cloudformation_integration.py`

```bash
# Validate CloudFormation templates
python tests/run_cloudformation_tests.py

# Test template deployment
python tests/run_cloudformation_tests.py --deploy
```

## Test Development Guidelines

### Writing Unit Tests

#### Test Structure Template
```python
#!/usr/bin/env python3
"""
Unit tests for [module_name] module.

This test suite covers:
- [Component 1] functionality
- [Component 2] error handling
- [Component 3] integration points
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

# Import the classes to test
from glue_job.module_name import ClassToTest


class TestClassToTest(unittest.TestCase):
    """Test ClassToTest functionality."""
    
    def setUp(self):
        """Set up test fixtures before each test method."""
        self.test_instance = ClassToTest()
    
    def test_basic_functionality(self):
        """Test basic functionality with valid inputs."""
        # Arrange
        input_data = "test_input"
        expected_result = "expected_output"
        
        # Act
        result = self.test_instance.method_to_test(input_data)
        
        # Assert
        self.assertEqual(result, expected_result)
    
    def test_error_handling(self):
        """Test error handling with invalid inputs."""
        with self.assertRaises(ExpectedException):
            self.test_instance.method_to_test(invalid_input)
    
    def tearDown(self):
        """Clean up after each test method."""
        # Cleanup if needed
        pass


if __name__ == '__main__':
    unittest.main()
```

#### Mocking Best Practices

##### External Service Mocking
```python
@patch('boto3.client')
def test_s3_operations(self, mock_boto3_client):
    """Test S3 operations with mocked boto3 client."""
    # Configure mock
    mock_s3_client = Mock()
    mock_boto3_client.return_value = mock_s3_client
    mock_s3_client.get_object.return_value = {
        'Body': Mock(read=Mock(return_value=b'{"test": "data"}'))
    }
    
    # Test the functionality
    result = self.storage.read_bookmark("test_table")
    
    # Verify mock interactions
    mock_s3_client.get_object.assert_called_once_with(
        Bucket='test-bucket',
        Key='bookmarks/test_table.json'
    )
```

##### Database Connection Mocking
```python
@patch('glue_job.database.connection_manager.spark')
def test_jdbc_connection(self, mock_spark):
    """Test JDBC connection with mocked Spark."""
    # Configure mock DataFrame
    mock_df = Mock()
    mock_spark.read.format.return_value.options.return_value.load.return_value = mock_df
    
    # Test connection creation
    connection_manager = JdbcConnectionManager()
    result_df = connection_manager.create_connection(self.connection_config)
    
    # Verify connection parameters
    mock_spark.read.format.assert_called_with("jdbc")
    self.assertEqual(result_df, mock_df)
```

#### Error Testing Patterns

##### Exception Testing
```python
def test_specific_exception_handling(self):
    """Test handling of specific exceptions."""
    with self.assertRaises(NetworkConnectivityError) as context:
        self.network_handler.handle_connection_failure("timeout_error")
    
    self.assertIn("Connection timeout", str(context.exception))
    self.assertEqual(context.exception.error_code, "NET_001")
```

##### Retry Logic Testing
```python
@patch('time.sleep')  # Mock sleep to speed up tests
def test_retry_mechanism(self, mock_sleep):
    """Test retry mechanism with exponential backoff."""
    # Configure mock to fail twice, then succeed
    mock_operation = Mock(side_effect=[Exception("fail"), Exception("fail"), "success"])
    
    # Test retry logic
    result = self.retry_handler.execute_with_retry(mock_operation)
    
    # Verify results
    self.assertEqual(result, "success")
    self.assertEqual(mock_operation.call_count, 3)
    
    # Verify exponential backoff
    expected_delays = [1, 2, 4]  # 2^0, 2^1, 2^2
    actual_delays = [call[0][0] for call in mock_sleep.call_args_list]
    self.assertEqual(actual_delays, expected_delays)
```

### Integration Test Guidelines

#### Resource Management
```python
class TestIntegrationWithRealAWS(unittest.TestCase):
    """Integration tests with real AWS resources."""
    
    @classmethod
    def setUpClass(cls):
        """Set up resources for entire test class."""
        cls.test_bucket = f"integration-test-{uuid.uuid4()}"
        cls.s3_client = boto3.client('s3')
        
        # Create test bucket
        cls.s3_client.create_bucket(Bucket=cls.test_bucket)
        
        # Wait for bucket to be available
        waiter = cls.s3_client.get_waiter('bucket_exists')
        waiter.wait(Bucket=cls.test_bucket)
    
    @classmethod
    def tearDownClass(cls):
        """Clean up resources after test class."""
        # Delete all objects in bucket
        try:
            objects = cls.s3_client.list_objects_v2(Bucket=cls.test_bucket)
            if 'Contents' in objects:
                delete_keys = [{'Key': obj['Key']} for obj in objects['Contents']]
                cls.s3_client.delete_objects(
                    Bucket=cls.test_bucket,
                    Delete={'Objects': delete_keys}
                )
            
            # Delete bucket
            cls.s3_client.delete_bucket(Bucket=cls.test_bucket)
        except Exception as e:
            print(f"Warning: Failed to clean up test bucket {cls.test_bucket}: {e}")
```

#### Performance Testing
```python
def test_performance_benchmark(self):
    """Test performance against established benchmarks."""
    import time
    
    # Setup test data
    large_dataset = self.create_test_dataset(size=1000)
    
    # Measure performance
    start_time = time.time()
    result = self.processor.process_large_dataset(large_dataset)
    end_time = time.time()
    
    # Performance assertions
    processing_time = end_time - start_time
    self.assertLess(processing_time, 30.0, "Processing should complete within 30 seconds")
    self.assertEqual(len(result), 1000, "All records should be processed")
    
    # Log performance metrics
    print(f"Performance: Processed {len(result)} records in {processing_time:.2f} seconds")
```

## Common Testing Scenarios

### Testing Configuration Parsing

```python
def test_configuration_parsing_scenario(self):
    """Test realistic configuration parsing scenario."""
    # Simulate CloudFormation parameters
    cf_params = {
        'SourceEngine': 'postgresql',
        'SourceHost': 'source-db.example.com',
        'SourcePort': '5432',
        'SourceDatabase': 'source_db',
        'TargetEngine': 'sqlserver',
        'TargetHost': 'target-db.example.com',
        'TargetPort': '1433',
        'TargetDatabase': 'target_db',
        'JobName': 'test-replication-job'
    }
    
    # Test configuration parsing
    parser = JobConfigurationParser()
    config = parser.parse_configuration(cf_params)
    
    # Verify parsed configuration
    self.assertEqual(config.job_name, 'test-replication-job')
    self.assertEqual(config.source_config.engine_type, 'postgresql')
    self.assertEqual(config.target_config.engine_type, 'sqlserver')
```

### Testing Error Recovery

```python
def test_s3_error_recovery_scenario(self):
    """Test S3 error recovery with fallback to memory storage."""
    # Configure S3 client to simulate access denied
    with patch('boto3.client') as mock_boto3:
        mock_s3_client = Mock()
        mock_boto3.return_value = mock_s3_client
        mock_s3_client.get_object.side_effect = ClientError(
            error_response={'Error': {'Code': 'AccessDenied'}},
            operation_name='GetObject'
        )
        
        # Test bookmark manager fallback behavior
        bookmark_manager = JobBookmarkManager(
            glue_context=self.mock_glue_context,
            job_name="test-job",
            source_jdbc_path="s3://test-bucket/drivers/"
        )
        
        # Should fallback to memory storage
        bookmark_state = bookmark_manager.get_bookmark_state("test_table")
        
        # Verify fallback occurred
        self.assertFalse(bookmark_manager.s3_enabled)
        self.assertEqual(bookmark_state.processing_mode, "FIRST_RUN")
```

### Testing Database Connections

```python
def test_database_connection_scenario(self):
    """Test database connection with realistic configuration."""
    # Create realistic connection config
    connection_config = ConnectionConfig(
        engine_type="postgresql",
        connection_string="jdbc:postgresql://localhost:5432/testdb",
        database="testdb",
        schema="public",
        username="testuser",
        password="testpass",
        table_name="test_table"
    )
    
    # Mock Spark DataFrame
    with patch('glue_job.database.connection_manager.spark') as mock_spark:
        mock_df = Mock()
        mock_spark.read.format.return_value.options.return_value.load.return_value = mock_df
        
        # Test connection creation
        connection_manager = JdbcConnectionManager()
        result_df = connection_manager.create_connection(connection_config)
        
        # Verify connection parameters
        mock_spark.read.format.assert_called_with("jdbc")
        expected_options = {
            "url": "jdbc:postgresql://localhost:5432/testdb",
            "dbtable": "public.test_table",
            "user": "testuser",
            "password": "testpass",
            "driver": "org.postgresql.Driver"
        }
        mock_spark.read.format.return_value.options.assert_called_with(**expected_options)
```

### Testing Performance Scenarios

```python
def test_large_scale_bookmark_operations(self):
    """Test performance with large number of bookmarks."""
    import time
    
    # Create multiple table bookmarks
    table_count = 50
    table_names = [f"table_{i:03d}" for i in range(table_count)]
    
    # Measure bookmark operations performance
    start_time = time.time()
    
    for table_name in table_names:
        bookmark_state = self.bookmark_manager.get_bookmark_state(table_name)
        self.bookmark_manager.update_bookmark_state(table_name, bookmark_state)
    
    total_time = time.time() - start_time
    avg_time_per_table = total_time / table_count
    
    # Performance assertions
    self.assertLess(avg_time_per_table, 1.0, 
                   f"Average time per table {avg_time_per_table:.2f}s exceeds 1s limit")
    self.assertLess(total_time, 60.0, 
                   f"Total time {total_time:.2f}s exceeds 60s limit")
    
    print(f"Performance: {table_count} tables processed in {total_time:.2f}s ")
    print(f"Average: {avg_time_per_table:.3f}s per table")
```

## Debugging and Troubleshooting

### Common Test Issues

#### Import Errors

**Problem**: `ModuleNotFoundError` when running tests

**Solution**:
```bash
# Ensure PYTHONPATH includes project root
export PYTHONPATH="${PYTHONPATH}:$(pwd)"

# Or run tests from project root
cd /path/to/aws-glue-data-replication
python -m pytest tests/test_config.py -v
```

#### Mock Import Issues

**Problem**: Tests fail due to PySpark/Glue import errors

**Solution**: Ensure mock setup is correct in test files:
```python
# This should be at the top of every test file, before other imports
mock_modules = [
    'awsglue', 'awsglue.utils', 'awsglue.context', 'awsglue.job',
    'pyspark', 'pyspark.context', 'pyspark.sql', 'pyspark.sql.types',
    'pyspark.sql.functions', 'boto3'
]

for module in mock_modules:
    sys.modules[module] = MagicMock()
```

#### AWS Credential Issues

**Problem**: Integration tests fail with `NoCredentialsError`

**Solution**:
```bash
# Configure AWS credentials
aws configure

# Or set environment variables
export AWS_ACCESS_KEY_ID=your_access_key
export AWS_SECRET_ACCESS_KEY=your_secret_key
export AWS_DEFAULT_REGION=us-east-1

# Or use IAM role (recommended for EC2/Lambda)
# No additional configuration needed if running on AWS with proper IAM role
```

### Test Debugging Techniques

#### Verbose Test Output

```bash
# Run tests with maximum verbosity
python -m pytest tests/test_storage.py -v -s

# Show local variables in tracebacks
python -m pytest tests/test_storage.py -v -l

# Drop into debugger on failures
python -m pytest tests/test_storage.py --pdb

# Run only failed tests from last run
python -m pytest tests/test_storage.py --lf
```

#### Debug Logging in Tests

```python
import logging

class TestWithDebugLogging(unittest.TestCase):
    def setUp(self):
        # Enable debug logging for tests
        logging.basicConfig(level=logging.DEBUG)
        self.logger = logging.getLogger(__name__)
    
    def test_with_debug_info(self):
        self.logger.debug("Starting test with debug information")
        
        # Your test code here
        result = self.component.process_data(test_data)
        
        self.logger.debug(f"Test result: {result}")
        self.assertEqual(result, expected_value)
```

#### Mock Debugging

```python
def test_with_mock_debugging(self):
    """Test with detailed mock call verification."""
    with patch('boto3.client') as mock_boto3:
        mock_s3_client = Mock()
        mock_boto3.return_value = mock_s3_client
        
        # Execute test
        self.storage.read_bookmark("test_table")
        
        # Debug mock calls
        print(f"boto3.client called {mock_boto3.call_count} times")
        print(f"boto3.client call args: {mock_boto3.call_args_list}")
        print(f"s3_client.get_object called {mock_s3_client.get_object.call_count} times")
        print(f"s3_client.get_object call args: {mock_s3_client.get_object.call_args_list}")
        
        # Verify expected calls
        mock_s3_client.get_object.assert_called_with(
            Bucket='expected-bucket',
            Key='expected/key.json'
        )
```

### Performance Debugging

#### Test Execution Time Analysis

```bash
# Show test execution times
python -m pytest tests/ --durations=10

# Profile test execution
python -m pytest tests/test_storage.py --profile

# Run tests in parallel (requires pytest-xdist)
pip install pytest-xdist
python -m pytest tests/ -n auto
```

#### Memory Usage Monitoring

```python
import psutil
import os

def test_memory_usage_monitoring(self):
    """Monitor memory usage during test execution."""
    process = psutil.Process(os.getpid())
    initial_memory = process.memory_info().rss
    
    # Execute memory-intensive operation
    large_data = self.create_large_test_dataset()
    result = self.processor.process_large_dataset(large_data)
    
    final_memory = process.memory_info().rss
    memory_increase = final_memory - initial_memory
    
    # Log memory usage
    print(f"Memory usage: {memory_increase / 1024 / 1024:.2f} MB increase")
    
    # Assert memory usage is within acceptable limits
    self.assertLess(memory_increase, 100 * 1024 * 1024, "Memory increase should be < 100MB")
```

## CI/CD Integration

### GitHub Actions Integration

Create `.github/workflows/test.yml` for automated testing:

```yaml
name: Test Suite

on:
  push:
    branches: [ main, develop ]
  pull_request:
    branches: [ main ]

jobs:
  unit-tests:
    runs-on: ubuntu-latest
    strategy:
      matrix:
        python-version: [3.8, 3.9, '3.10']
    
    steps:
    - uses: actions/checkout@v3
    
    - name: Set up Python ${{ matrix.python-version }}
      uses: actions/setup-python@v3
      with:
        python-version: ${{ matrix.python-version }}
    
    - name: Install dependencies
      run: |
        python -m pip install --upgrade pip
        pip install -r requirements-test.txt
        pip install pytest pytest-cov
    
    - name: Run unit tests
      run: |
        python -m pytest tests/test_*.py -v --cov=src/glue_job --cov-report=xml
    
    - name: Upload coverage to Codecov
      uses: codecov/codecov-action@v3
      with:
        file: ./coverage.xml
        flags: unittests
        name: codecov-umbrella

  integration-tests:
    runs-on: ubuntu-latest
    needs: unit-tests
    if: github.event_name == 'push' && github.ref == 'refs/heads/main'
    
    steps:
    - uses: actions/checkout@v3
    
    - name: Set up Python
      uses: actions/setup-python@v3
      with:
        python-version: 3.9
    
    - name: Configure AWS credentials
      uses: aws-actions/configure-aws-credentials@v2
      with:
        aws-access-key-id: ${{ secrets.AWS_ACCESS_KEY_ID }}
        aws-secret-access-key: ${{ secrets.AWS_SECRET_ACCESS_KEY }}
        aws-region: us-east-1
    
    - name: Install dependencies
      run: |
        python -m pip install --upgrade pip
        pip install -r requirements-test.txt
    
    - name: Run integration tests
      run: |
        python tests/run_s3_integration_tests.py --verbose
      env:
        TEST_S3_BUCKET: glue-integration-test-${{ github.run_id }}
    
    - name: Cleanup test resources
      if: always()
      run: |
        aws s3 rb s3://glue-integration-test-${{ github.run_id }} --force || true
```

### Jenkins Pipeline Integration

Create `Jenkinsfile` for Jenkins CI/CD:

```groovy
pipeline {
    agent any
    
    environment {
        PYTHONPATH = "${WORKSPACE}/src"
        TEST_S3_BUCKET = "glue-integration-test-${BUILD_ID}"
    }
    
    stages {
        stage('Setup') {
            steps {
                sh 'python -m pip install --upgrade pip'
                sh 'pip install -r requirements-test.txt'
                sh 'pip install pytest pytest-cov pytest-html'
            }
        }
        
        stage('Unit Tests') {
            steps {
                sh '''
                    python -m pytest tests/test_*.py \
                        --junitxml=test-results.xml \
                        --cov=src/glue_job \
                        --cov-report=html \
                        --cov-report=xml \
                        --html=test-report.html \
                        --self-contained-html
                '''
            }
            post {
                always {
                    publishTestResults testResultsPattern: 'test-results.xml'
                    publishHTML([
                        allowMissing: false,
                        alwaysLinkToLastBuild: true,
                        keepAll: true,
                        reportDir: 'htmlcov',
                        reportFiles: 'index.html',
                        reportName: 'Coverage Report'
                    ])
                    publishHTML([
                        allowMissing: false,
                        alwaysLinkToLastBuild: true,
                        keepAll: true,
                        reportDir: '.',
                        reportFiles: 'test-report.html',
                        reportName: 'Test Report'
                    ])
                }
            }
        }
        
        stage('Integration Tests') {
            when {
                branch 'main'
            }
            steps {
                withAWS(credentials: 'aws-credentials', region: 'us-east-1') {
                    sh 'python tests/run_s3_integration_tests.py --verbose'
                }
            }
            post {
                always {
                    sh '''
                        aws s3 rb s3://${TEST_S3_BUCKET} --force || true
                    '''
                }
            }
        }
        
        stage('Performance Tests') {
            when {
                anyOf {
                    branch 'main'
                    changeRequest target: 'main'
                }
            }
            steps {
                sh '''
                    python -m pytest tests/test_*integration*.py::*Performance* \
                        --junitxml=performance-results.xml \
                        -v
                '''
            }
            post {
                always {
                    publishTestResults testResultsPattern: 'performance-results.xml'
                }
            }
        }
    }
    
    post {
        always {
            cleanWs()
        }
        failure {
            emailext (
                subject: "Test Failure: ${env.JOB_NAME} - ${env.BUILD_NUMBER}",
                body: "Test suite failed. Check console output at ${env.BUILD_URL}",
                to: "${env.CHANGE_AUTHOR_EMAIL}"
            )
        }
    }
}
```

### Local Development Workflow

#### Pre-commit Hooks

Create `.pre-commit-config.yaml`:

```yaml
repos:
  - repo: local
    hooks:
      - id: unit-tests
        name: Run unit tests
        entry: python -m pytest tests/test_*.py -x
        language: system
        pass_filenames: false
        always_run: true
      
      - id: import-validation
        name: Validate module imports
        entry: python tests/validate_tests.py
        language: system
        pass_filenames: false
        always_run: true
```

Install and setup:
```bash
pip install pre-commit
pre-commit install
```

#### Development Test Script

Create `scripts/dev-test.sh`:

```bash
#!/bin/bash
# Development testing script

set -e

echo "=== AWS Glue Data Replication - Development Test Suite ==="
echo

# Validate imports first
echo "1. Validating module imports..."
python tests/validate_tests.py
echo

# Run unit tests with coverage
echo "2. Running unit tests with coverage..."
python -m pytest tests/test_*.py -v --cov=src/glue_job --cov-report=term-missing
echo

# Run integration tests if AWS credentials are available
if aws sts get-caller-identity &>/dev/null; then
    echo "3. Running integration tests..."
    python tests/run_s3_integration_tests.py
else
    echo "3. Skipping integration tests (AWS credentials not configured)"
fi

echo
echo "✅ All tests completed successfully!"
```

Make executable and use:
```bash
chmod +x scripts/dev-test.sh
./scripts/dev-test.sh
```

### Test Reporting and Metrics

#### Coverage Requirements

- **Unit Tests**: Minimum 85% code coverage
- **Integration Tests**: All critical paths covered
- **Performance Tests**: Baseline performance metrics established

#### Quality Gates

1. All unit tests must pass
2. Code coverage must meet minimum threshold
3. Integration tests must pass for main branch
4. Performance tests must not exceed baseline by more than 20%
5. No critical security vulnerabilities in dependencies

#### Monitoring and Alerting

```python
# Example test metrics collection
def collect_test_metrics():
    """Collect and report test execution metrics."""
    metrics = {
        'test_execution_time': time.time() - start_time,
        'tests_passed': result.testsRun - len(result.failures) - len(result.errors),
        'tests_failed': len(result.failures),
        'tests_errors': len(result.errors),
        'coverage_percentage': coverage_percentage
    }
    
    # Send metrics to monitoring system
    send_metrics_to_cloudwatch(metrics)
    
    return metrics
```

This comprehensive testing guide provides complete documentation for understanding, running, and developing tests for the modular AWS Glue Data Replication project. It covers all aspects from basic unit testing to advanced CI/CD integration, ensuring contributors have clear instructions for maintaining high code quality and reliability.





#### Running Integration Tests

```bash
# Quick start with default settings
python run_s3_integration_tests.py

# Custom configuration
python run_s3_integration_tests.py --bucket my-test-bucket --region us-west-2 --verbose

# Environment variables
export TEST_S3_BUCKET=my-integration-test-bucket
export AWS_DEFAULT_REGION=us-west-2
python run_s3_integration_tests.py

# Direct pytest execution
pytest test_s3_bookmark_integration_complete.py::TestPerformanceWithMultipleTables -v
```

#### Prerequisites for Integration Tests

##### AWS Configuration
1. **AWS Credentials**: Configure via AWS CLI, environment variables, or IAM role
2. **S3 Permissions**: GetObject, PutObject, DeleteObject, ListBucket, CreateBucket
3. **AWS Region**: Set default region (optional)

##### Python Dependencies
```bash
pip install pytest boto3 botocore
```

#### Integration Test Configuration

**File**: `integration_test_config.py`

```python
class IntegrationTestConfig:
    """Centralized configuration for integration tests"""
    
    def __init__(self):
        self.test_bucket = os.getenv('TEST_S3_BUCKET', 'glue-integration-test-bucket')
        self.aws_region = os.getenv('AWS_DEFAULT_REGION', 'us-east-1')
        self.large_table_count = int(os.getenv('TEST_LARGE_TABLE_COUNT', '50'))
        self.performance_timeout = int(os.getenv('TEST_PERFORMANCE_TIMEOUT', '300'))
```







## Test Development Guidelines



