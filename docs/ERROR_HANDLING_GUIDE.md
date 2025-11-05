# Error Handling During Data Transfer

This document provides comprehensive details about how errors are handled during data transfer operations in the AWS Glue Data Replication system.

## Table of Contents

1. [Overview](#overview)
2. [Error Handling Philosophy](#error-handling-philosophy)
3. [Table-Level Error Isolation](#table-level-error-isolation)
4. [Error Classification](#error-classification)
5. [Retry Mechanisms](#retry-mechanisms)
6. [Record and Transaction Handling](#record-and-transaction-handling)
   - [Transaction-Based Processing: Detailed Scenario](#transaction-based-processing-detailed-scenario)
7. [Bookmark Management During Failures](#bookmark-management-during-failures)
8. [Job Completion Behavior](#job-completion-behavior)
9. [Monitoring and Alerting](#monitoring-and-alerting)
10. [Recovery Strategies](#recovery-strategies)
11. [Best Practices](#best-practices)

## Overview

The AWS Glue Data Replication system implements a robust error handling strategy designed to maximize data consistency while ensuring job resilience. The system follows a **table-level error isolation** approach where individual table failures don't stop the entire replication job.

### Key Principles

- **Data Consistency**: All-or-nothing approach per table ensures no partial writes
- **Job Resilience**: Individual table failures don't stop the entire job
- **Automatic Recovery**: Intelligent retry mechanisms with exponential backoff
- **Comprehensive Monitoring**: Detailed error tracking and alerting
- **Graceful Degradation**: System continues operating even when optional components fail

## Error Handling Philosophy

### Fail-Safe Design

The system is designed with a "fail-safe" philosophy:

1. **Preserve Data Integrity**: Never leave tables in an inconsistent state
2. **Maximize Throughput**: Process as many tables as possible in each run
3. **Enable Recovery**: Maintain state to allow seamless recovery in subsequent runs
4. **Provide Visibility**: Comprehensive logging and metrics for troubleshooting

### Error Boundaries

Error boundaries are established at multiple levels:

```
Job Level
├── Table Level (Primary Error Boundary)
│   ├── Connection Level
│   ├── Data Processing Level
│   └── Write Operation Level
└── Infrastructure Level
    ├── S3 Operations
    ├── Network Connectivity
    └── AWS Service Interactions
```

## Table-Level Error Isolation

### How It Works

Each table is processed independently within its own error boundary:

```python
# Simplified error isolation logic
successful_tables = 0
failed_tables = 0

for table_name in config.tables:
    try:
        # Process individual table
        progress = process_table(table_name, config)
        
        if progress.status == 'completed':
            successful_tables += 1
            logger.info(f"Successfully processed table {table_name}: {progress.processed_rows} rows")
        else:
            failed_tables += 1
            logger.error(f"Failed to process table {table_name}: {progress.error_message}")
            
    except Exception as e:
        # Table-level exception handling
        logger.error(f"Failed to process table {table_name}: {e}")
        failed_tables += 1

# Job continues regardless of individual table failures
logger.info(f"Job completed: {successful_tables} successful, {failed_tables} failed")
```

### Benefits

1. **Fault Isolation**: One table's failure doesn't affect others
2. **Partial Success**: Jobs can complete successfully even with some table failures
3. **Efficient Recovery**: Only failed tables need to be retried in subsequent runs
4. **Resource Optimization**: System resources aren't wasted on repeatedly failing operations

## Error Classification

The system classifies errors into specific categories to determine appropriate handling strategies:

### Error Categories

#### 1. Retryable Errors
Errors that are likely to succeed on retry:

- **Network Timeouts**: `connection timed out`, `read timeout`, `write timeout`
- **Connection Issues**: `connection refused`, `connection reset`, `connection failed`
- **Temporary Service Issues**: `service unavailable`, `throttling`, `rate limiting`
- **Resource Constraints**: `too many connections`, `memory pressure`
- **AWS Service Throttling**: `Throttling`, `ServiceUnavailable`, `InternalFailure`

```python
# Example retryable error patterns
RETRYABLE_PATTERNS = [
    'connection refused', 'connection timed out', 'connection reset',
    'network unreachable', 'service unavailable', 'throttling',
    'Throttling', 'ServiceUnavailable', 'InternalFailure'
]
```

#### 2. Non-Retryable Errors
Errors that require manual intervention:

- **Authentication Failures**: `authentication failed`, `invalid credentials`, `access denied`
- **Authorization Issues**: `permission denied`, `insufficient privileges`, `forbidden`
- **Configuration Errors**: `invalid connection string`, `missing parameters`
- **Schema Mismatches**: `column not found`, `table not found`, `data type mismatch`
- **Glue Connection Errors**: `connection not found`, `invalid connection type`, `connection validation failed`
- **Parameter Validation**: `mutually exclusive parameters`, `invalid parameter combination`

```python
# Example non-retryable error patterns
NON_RETRYABLE_PATTERNS = [
    'authentication failed', 'permission denied', 'column not found',
    'invalid credentials', 'table does not exist', 'connection not found',
    'invalid connection type', 'mutually exclusive parameters'
]
```

#### 3. Data Processing Errors
Errors related to data quality or format:

- **Data Type Issues**: `conversion error`, `invalid data format`, `data truncation`
- **Constraint Violations**: `duplicate key`, `foreign key violation`, `check constraint`
- **Data Corruption**: `invalid data`, `corrupted record`, `parsing error`

#### 4. Glue Connection Errors
Errors specific to AWS Glue Connection operations:

- **Connection Creation Failures**: `InvalidInputException`, `AlreadyExistsException`, `ResourceNumberLimitExceededException`
- **Connection Retrieval Failures**: `EntityNotFoundException`, `GlueEncryptionException`
- **Connection Validation Failures**: `connection test failed`, `network connectivity issues`
- **Parameter Validation Errors**: `mutually exclusive parameters`, `missing required parameters for Glue Connection`

```python
# Glue Connection specific error patterns
GLUE_CONNECTION_ERROR_PATTERNS = {
    'creation_errors': [
        'InvalidInputException', 'AlreadyExistsException', 
        'ResourceNumberLimitExceededException', 'OperationTimeoutException'
    ],
    'retrieval_errors': [
        'EntityNotFoundException', 'GlueEncryptionException',
        'AccessDeniedException'
    ],
    'validation_errors': [
        'connection test failed', 'network connectivity',
        'invalid connection properties'
    ],
    'parameter_errors': [
        'mutually exclusive parameters', 'createSourceConnection and useSourceConnection',
        'createTargetConnection and useTargetConnection'
    ]
}
```

### Error Classification Logic

```python
class ErrorClassifier:
    @classmethod
    def classify_error(cls, error: Exception) -> str:
        error_message = str(error).lower()
        
        # Check for Glue Connection specific errors first
        if cls._is_glue_connection_error(error):
            return cls._classify_glue_connection_error(error)
        
        # Standard error classification
        for category, patterns in cls.ERROR_PATTERNS.items():
            for pattern in patterns:
                if pattern in error_message:
                    return category
        
        return ErrorCategory.UNKNOWN
    
    @classmethod
    def is_retryable_error(cls, error: Exception) -> bool:
        category = cls.classify_error(error)
        retryable_categories = {
            ErrorCategory.CONNECTION,
            ErrorCategory.NETWORK,
            ErrorCategory.TIMEOUT,
            ErrorCategory.RESOURCE,
            ErrorCategory.GLUE_CONNECTION_TRANSIENT
        }
        return category in retryable_categories
    
    @classmethod
    def _is_glue_connection_error(cls, error: Exception) -> bool:
        """Check if error is related to Glue Connection operations"""
        if hasattr(error, 'response') and 'Error' in error.response:
            error_code = error.response['Error']['Code']
            return error_code in [
                'InvalidInputException', 'EntityNotFoundException', 
                'AlreadyExistsException', 'Throttling', 'ServiceUnavailable'
            ]
        return isinstance(error, (GlueConnectionError, GlueConnectionCreationError, 
                                GlueConnectionNotFoundError, GlueConnectionValidationError))
    
    @classmethod
    def _classify_glue_connection_error(cls, error: Exception) -> str:
        """Classify Glue Connection specific errors"""
        if hasattr(error, 'response') and 'Error' in error.response:
            error_code = error.response['Error']['Code']
            
            if error_code in ['Throttling', 'ServiceUnavailable', 'InternalFailure']:
                return ErrorCategory.GLUE_CONNECTION_TRANSIENT
            elif error_code in ['EntityNotFoundException']:
                return ErrorCategory.GLUE_CONNECTION_NOT_FOUND
            elif error_code in ['InvalidInputException', 'AlreadyExistsException']:
                return ErrorCategory.GLUE_CONNECTION_CONFIGURATION
            else:
                return ErrorCategory.GLUE_CONNECTION_UNKNOWN
        
        # Handle custom Glue Connection exceptions
        if isinstance(error, GlueConnectionNotFoundError):
            return ErrorCategory.GLUE_CONNECTION_NOT_FOUND
        elif isinstance(error, GlueConnectionCreationError):
            return ErrorCategory.GLUE_CONNECTION_CREATION
        elif isinstance(error, GlueConnectionValidationError):
            return ErrorCategory.GLUE_CONNECTION_VALIDATION
        
        return ErrorCategory.GLUE_CONNECTION_UNKNOWN
```

## Glue Connection Error Handling

### Glue Connection Error Categories

The system implements specialized error handling for AWS Glue Connection operations, with specific strategies for different types of failures.

#### Connection Creation Errors

**Common Scenarios**:
```python
# Scenario 1: Invalid connection parameters
try:
    connection_name = glue_connection_manager.create_glue_connection(
        connection_config, "my-connection"
    )
except GlueConnectionCreationError as e:
    if "InvalidInputException" in str(e):
        logger.error(f"Invalid Glue Connection parameters: {e}")
        # Non-retryable - requires parameter correction
        raise ConfigurationError(f"Fix connection parameters: {e}")
    elif "AlreadyExistsException" in str(e):
        logger.warning(f"Glue Connection already exists: {e}")
        # Use existing connection instead
        return glue_connection_manager.validate_glue_connection_exists(connection_name)

# Scenario 2: Network configuration issues
try:
    connection_name = glue_connection_manager.create_glue_connection(
        connection_config, "my-connection"
    )
except GlueConnectionCreationError as e:
    if "network" in str(e).lower():
        logger.error(f"Network configuration error: {e}")
        # Check VPC, subnet, and security group settings
        raise NetworkConfigurationError(f"Verify network settings: {e}")
```

**Error Handling Strategy**:
```python
class GlueConnectionCreationHandler:
    def handle_creation_error(self, error: ClientError, connection_config: ConnectionConfig) -> ErrorResponse:
        error_code = error.response['Error']['Code']
        error_message = error.response['Error']['Message']
        
        if error_code == 'InvalidInputException':
            return ErrorResponse(
                category='configuration',
                retryable=False,
                action='fix_parameters',
                message=f"Invalid Glue Connection parameters: {error_message}",
                suggested_fix="Review connection string, credentials, and network configuration"
            )
        
        elif error_code == 'AlreadyExistsException':
            return ErrorResponse(
                category='configuration',
                retryable=False,
                action='use_existing',
                message=f"Glue Connection already exists: {error_message}",
                suggested_fix="Use existing connection or choose different name"
            )
        
        elif error_code == 'ResourceNumberLimitExceededException':
            return ErrorResponse(
                category='resource_limit',
                retryable=False,
                action='cleanup_connections',
                message=f"Glue Connection limit exceeded: {error_message}",
                suggested_fix="Delete unused connections or request limit increase"
            )
        
        elif error_code in ['Throttling', 'ServiceUnavailable']:
            return ErrorResponse(
                category='transient',
                retryable=True,
                action='retry_with_backoff',
                message=f"Temporary Glue service issue: {error_message}",
                suggested_fix="Retry operation with exponential backoff"
            )
        
        else:
            return ErrorResponse(
                category='unknown',
                retryable=False,
                action='manual_investigation',
                message=f"Unknown Glue Connection creation error: {error_message}",
                suggested_fix="Check AWS service status and contact support if needed"
            )
```

#### Connection Retrieval Errors

**Common Scenarios**:
```python
# Scenario 1: Connection not found
try:
    connection_exists = glue_connection_manager.validate_glue_connection_exists("my-connection")
except GlueConnectionNotFoundError as e:
    logger.error(f"Glue Connection not found: {e}")
    # Check connection name, region, and account
    raise ConfigurationError(f"Verify connection name and region: {e}")

# Scenario 2: Permission issues
try:
    connection_properties = glue_connection_manager.get_glue_connection_properties("my-connection")
except ClientError as e:
    if e.response['Error']['Code'] == 'AccessDeniedException':
        logger.error(f"Insufficient permissions for Glue Connection: {e}")
        raise PermissionError(f"Add glue:GetConnection permission: {e}")
```

**Error Handling Strategy**:
```python
class GlueConnectionRetrievalHandler:
    def handle_retrieval_error(self, error: Exception, connection_name: str) -> ErrorResponse:
        if isinstance(error, ClientError):
            error_code = error.response['Error']['Code']
            
            if error_code == 'EntityNotFoundException':
                return ErrorResponse(
                    category='not_found',
                    retryable=False,
                    action='verify_connection',
                    message=f"Glue Connection '{connection_name}' not found",
                    suggested_fix="Verify connection name, region, and AWS account"
                )
            
            elif error_code == 'AccessDeniedException':
                return ErrorResponse(
                    category='permission',
                    retryable=False,
                    action='fix_permissions',
                    message=f"Access denied for Glue Connection '{connection_name}'",
                    suggested_fix="Add glue:GetConnection permission to IAM role"
                )
        
        return ErrorResponse(
            category='unknown',
            retryable=False,
            action='manual_investigation',
            message=f"Unknown error retrieving Glue Connection '{connection_name}': {str(error)}",
            suggested_fix="Check AWS service status and connection configuration"
        )
```

#### Parameter Validation Errors

**Common Scenarios**:
```python
# Scenario 1: Mutually exclusive parameters
try:
    glue_config = GlueConnectionConfig(
        create_connection=True,
        use_existing_connection="my-connection"
    )
    glue_config.validate()
except ValueError as e:
    logger.error(f"Invalid Glue Connection parameter combination: {e}")
    raise ConfigurationError(f"Fix parameter configuration: {e}")

# Scenario 2: Missing required parameters for creation
try:
    if connection_config.glue_connection_config.create_connection:
        required_params = ['connection_string', 'username', 'password']
        missing_params = [p for p in required_params if not getattr(connection_config, p)]
        if missing_params:
            raise ValueError(f"Missing required parameters for Glue Connection creation: {missing_params}")
except ValueError as e:
    logger.error(f"Missing Glue Connection parameters: {e}")
    raise ConfigurationError(f"Provide all required parameters: {e}")
```

#### Iceberg Engine Compatibility Warnings

**Handling Glue Connection Parameters with Iceberg**:
```python
class IcebergGlueConnectionHandler:
    def handle_iceberg_glue_parameters(self, connection_config: ConnectionConfig) -> None:
        """Handle Glue Connection parameters for Iceberg engines"""
        if connection_config.engine_type == 'iceberg' and connection_config.uses_glue_connection():
            warning_message = (
                f"Glue Connection parameters ignored for Iceberg engine. "
                f"Iceberg connections use existing connection mechanism. "
                f"Parameters: createConnection={connection_config.glue_connection_config.create_connection}, "
                f"useConnection={connection_config.glue_connection_config.use_existing_connection}"
            )
            
            logger.warning(warning_message)
            
            # Publish warning metric
            self.metrics_publisher.publish_metric(
                'IcebergGlueConnectionParametersIgnored', 1, 'Count',
                dimensions={'EngineType': 'iceberg'}
            )
            
            # Reset Glue Connection config for Iceberg
            connection_config.glue_connection_config = None
```

### Glue Connection Retry Mechanisms

#### Specialized Retry Logic for Glue Operations

```python
class GlueConnectionRetryHandler:
    def __init__(self):
        self.max_retries = 3
        self.base_delay = 2.0  # Longer base delay for AWS API calls
        self.max_delay = 120.0  # Longer max delay for service issues
        self.backoff_factor = 2.0
        
        # Glue-specific retryable error codes
        self.retryable_error_codes = {
            'Throttling', 'ServiceUnavailable', 'InternalFailure',
            'OperationTimeoutException', 'ConcurrentModificationException'
        }
    
    def execute_glue_operation_with_retry(self, operation: Callable, 
                                        operation_name: str, **kwargs) -> Any:
        """Execute Glue Connection operation with specialized retry logic"""
        
        for attempt in range(self.max_retries + 1):
            try:
                logger.info(f"Executing {operation_name}, attempt {attempt + 1}/{self.max_retries + 1}")
                result = operation(**kwargs)
                
                if attempt > 0:
                    logger.info(f"{operation_name} succeeded after {attempt + 1} attempts")
                
                return result
                
            except ClientError as e:
                error_code = e.response['Error']['Code']
                error_message = e.response['Error']['Message']
                
                if error_code not in self.retryable_error_codes:
                    logger.error(f"{operation_name} failed with non-retryable error: {error_code} - {error_message}")
                    raise GlueConnectionError(f"{operation_name} failed: {error_message}")
                
                if attempt < self.max_retries:
                    delay = self._calculate_glue_backoff_delay(attempt, error_code)
                    logger.warning(f"{operation_name} failed (attempt {attempt + 1}), retrying in {delay:.1f}s: {error_code}")
                    time.sleep(delay)
                else:
                    logger.error(f"{operation_name} failed after {self.max_retries + 1} attempts: {error_code} - {error_message}")
                    raise GlueConnectionError(f"{operation_name} failed after retries: {error_message}")
            
            except Exception as e:
                logger.error(f"{operation_name} failed with unexpected error: {str(e)}")
                raise GlueConnectionError(f"{operation_name} failed: {str(e)}")
    
    def _calculate_glue_backoff_delay(self, attempt: int, error_code: str) -> float:
        """Calculate backoff delay with error-code specific adjustments"""
        base_delay = self.base_delay
        
        # Longer delays for throttling
        if error_code == 'Throttling':
            base_delay *= 2.0
        
        # Calculate exponential backoff
        delay = min(base_delay * (self.backoff_factor ** attempt), self.max_delay)
        
        # Add jitter to prevent thundering herd
        jitter_factor = random.uniform(0.8, 1.2)
        delay *= jitter_factor
        
        return delay
```

### Glue Connection Monitoring and Alerting

#### Specialized Metrics for Glue Connections

```python
class GlueConnectionMetricsPublisher:
    def __init__(self, metrics_publisher: CloudWatchMetricsPublisher):
        self.metrics_publisher = metrics_publisher
        self.namespace = 'GlueJob/Connections'
    
    def publish_glue_connection_metrics(self, operation: str, success: bool, 
                                      duration: float, connection_name: str = None):
        """Publish Glue Connection specific metrics"""
        
        dimensions = {
            'Operation': operation,
            'ConnectionStrategy': 'glue_connection'
        }
        
        if connection_name:
            dimensions['ConnectionName'] = connection_name
        
        # Success/failure metrics
        self.metrics_publisher.publish_metric(
            f'GlueConnection{operation}Success' if success else f'GlueConnection{operation}Failure',
            1, 'Count', dimensions=dimensions
        )
        
        # Duration metrics
        self.metrics_publisher.publish_metric(
            f'GlueConnection{operation}Duration',
            duration, 'Seconds', dimensions=dimensions
        )
        
        # Overall success rate
        self.metrics_publisher.publish_metric(
            'GlueConnectionOperationSuccessRate',
            100.0 if success else 0.0, 'Percent', dimensions=dimensions
        )
    
    def publish_glue_connection_error_metrics(self, error_category: str, error_code: str = None):
        """Publish error-specific metrics for Glue Connections"""
        
        dimensions = {
            'ErrorCategory': error_category,
            'ConnectionStrategy': 'glue_connection'
        }
        
        if error_code:
            dimensions['ErrorCode'] = error_code
        
        self.metrics_publisher.publish_metric(
            'GlueConnectionErrors', 1, 'Count', dimensions=dimensions
        )
```

#### CloudWatch Alarms for Glue Connection Issues

```python
def create_glue_connection_alarms(cloudwatch_client, job_name: str):
    """Create CloudWatch alarms for Glue Connection monitoring"""
    
    alarms = [
        {
            'AlarmName': f'{job_name}-GlueConnectionCreationFailures',
            'MetricName': 'GlueConnectionCreateConnectionFailure',
            'Threshold': 3,
            'ComparisonOperator': 'GreaterThanOrEqualToThreshold',
            'EvaluationPeriods': 2,
            'AlarmDescription': 'High number of Glue Connection creation failures'
        },
        {
            'AlarmName': f'{job_name}-GlueConnectionNotFound',
            'MetricName': 'GlueConnectionErrors',
            'Dimensions': [{'Name': 'ErrorCategory', 'Value': 'not_found'}],
            'Threshold': 1,
            'ComparisonOperator': 'GreaterThanOrEqualToThreshold',
            'EvaluationPeriods': 1,
            'AlarmDescription': 'Glue Connection not found errors'
        },
        {
            'AlarmName': f'{job_name}-GlueConnectionThrottling',
            'MetricName': 'GlueConnectionErrors',
            'Dimensions': [{'Name': 'ErrorCode', 'Value': 'Throttling'}],
            'Threshold': 5,
            'ComparisonOperator': 'GreaterThanOrEqualToThreshold',
            'EvaluationPeriods': 3,
            'AlarmDescription': 'High rate of Glue Connection throttling'
        }
    ]
    
    for alarm_config in alarms:
        cloudwatch_client.put_metric_alarm(**alarm_config)
```

## Retry Mechanisms

### Exponential Backoff Strategy

The system implements intelligent retry logic with exponential backoff:

```python
class ConnectionRetryHandler:
    def __init__(self, max_retries: int = 3, base_delay: float = 1.0, 
                 max_delay: float = 60.0, backoff_factor: float = 2.0):
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.backoff_factor = backoff_factor
    
    def execute_with_retry(self, operation, operation_name: str):
        for attempt in range(self.max_retries + 1):
            try:
                return operation()
            except Exception as e:
                if not self.error_classifier.is_retryable_error(e):
                    raise RuntimeError(f"{operation_name} failed: {str(e)} [Non-retryable]")
                
                if attempt < self.max_retries:
                    delay = self._calculate_delay(attempt)
                    time.sleep(delay)
                else:
                    raise e
```

### Retry Configuration

| Parameter | Default Value | Description |
|-----------|---------------|-------------|
| `max_retries` | 3 | Maximum number of retry attempts |
| `base_delay` | 1.0 seconds | Initial delay between retries |
| `max_delay` | 60.0 seconds | Maximum delay between retries |
| `backoff_factor` | 2.0 | Exponential backoff multiplier |
| `jitter` | True | Add randomization to prevent thundering herd |

### Delay Calculation

```python
def _calculate_delay(self, attempt: int) -> float:
    # Calculate exponential backoff
    delay = min(self.base_delay * (self.backoff_factor ** attempt), self.max_delay)
    
    # Add jitter to prevent thundering herd
    if self.jitter:
        jitter_factor = random.uniform(0.5, 1.5)
        delay *= jitter_factor
    
    return delay
```

### Circuit Breaker Pattern

For repeated failures, the system implements a circuit breaker pattern:

```python
class CircuitBreaker:
    def __init__(self, failure_threshold: int = 5, timeout: int = 60):
        self.failure_threshold = failure_threshold
        self.timeout = timeout
        self.failure_count = 0
        self.state = CircuitState.CLOSED
    
    def call(self, operation: Callable):
        if self.state == CircuitState.OPEN:
            if time.time() - self.last_failure_time > self.timeout:
                self.state = CircuitState.HALF_OPEN
            else:
                raise CircuitBreakerOpenException()
        
        try:
            result = operation()
            self._on_success()
            return result
        except Exception as e:
            self._on_failure()
            raise e
```

## Record and Transaction Handling

### All-or-Nothing Approach

**Important**: The system does NOT discard individual failed records. Instead, it uses an **all-or-nothing approach per table**.

#### How Data Writes Work

1. **Spark DataFrame Operations**: Data is processed using Spark DataFrames
2. **Partition-Level Atomicity**: Operations are atomic at the partition level
3. **Table-Level Consistency**: If any partition fails, the entire table operation fails
4. **No Partial Writes**: Tables are never left in an inconsistent state

```python
def write_table_data(self, df: DataFrame, connection_config: ConnectionConfig, 
                    table_name: str, mode: str = 'append'):
    """Write data to database table using Spark DataFrame."""
    try:
        writer = df.write.format('jdbc')
        writer = writer.option('url', connection_config.connection_string)
        writer = writer.option('dbtable', f"{connection_config.schema}.{table_name}")
        writer = writer.mode(mode)
        
        # Add connection properties
        for key, value in properties.items():
            writer = writer.option(key, str(value))
        
        # Atomic write operation
        writer.save()
        
    except Exception as e:
        # Entire table write fails - no partial data is written
        logger.error(f"Failed to write data to table {table_name}: {str(e)}")
        raise
```

#### What Happens During Failures

1. **Network Failure During Write**: 
   - Spark retries the operation automatically
   - If retries fail, the entire table write is rolled back
   - No partial data is committed

2. **Database Constraint Violation**:
   - Database rejects the entire transaction
   - Table remains in its previous consistent state
   - Error is logged with specific constraint details

3. **Data Type Mismatch**:
   - Spark fails the operation before writing begins
   - No data is written to the target table
   - Schema validation error is reported

### Transaction-Based Processing: Detailed Scenario

This section provides a detailed explanation of what happens when a failure occurs during data processing, using a concrete example.

#### Scenario: 10,000 Records with Mid-Processing Failure

**Question**: If a table has 10,000 records to replicate and fails at record 5,001, how many records are written to the target?

**Answer**: **Zero (0) records** are written to the target table.

#### Why Zero Records?

The system uses **transaction-based processing** with the following characteristics:

1. **Single Transaction Per Table**: All records for a table are processed within a single database transaction
2. **Atomic Operations**: Either all records are committed, or none are committed
3. **Rollback on Failure**: Any failure during processing triggers a complete rollback
4. **Data Consistency**: Ensures target table never contains partial data

#### Processing Flow Example

```
Table: customer_orders (10,000 records to process)
Bookmark: last_processed_value = "2024-01-01 00:00:00"

Processing Steps:
1. ✅ Begin transaction
2. ✅ Read records 1-5,000 from source
3. ✅ Transform and stage records 1-5,000
4. ❌ Fail to process record 5,001 (e.g., data type error)
5. 🔄 Rollback entire transaction
6. 📊 Result: 0 records in target table
7. 📌 Bookmark: remains "2024-01-01 00:00:00" (unchanged)
```

#### Data Consistency Guarantees

| Aspect | State After Failure |
|--------|-------------------|
| **Target Table Records** | 0 (zero) |
| **Bookmark Value** | Unchanged from before processing |
| **Data Integrity** | Perfect - no partial data |
| **Recovery Position** | Exact same starting point for retry |

#### Technical Implementation

The transaction-based approach is implemented through:

```python
def process_table_with_transaction(self, table_name: str, records: List[Dict]):
    """Process table records within a single transaction."""
    transaction = None
    try:
        # Begin transaction
        transaction = self.connection.begin_transaction()
        
        # Process all records in the transaction
        for i, record in enumerate(records):
            try:
                self.insert_record(record, transaction)
            except Exception as record_error:
                logger.error(f"Failed to process record {i+1}/{len(records)}: {record_error}")
                # Rollback entire transaction
                transaction.rollback()
                raise ProcessingError(f"Table {table_name} processing failed at record {i+1}")
        
        # Commit only if all records succeed
        transaction.commit()
        
        # Update bookmark only after successful commit
        self.update_bookmark(table_name, self.get_max_bookmark_value(records))
        
        return ProcessingResult(
            status='success',
            records_processed=len(records),
            records_written=len(records)
        )
        
    except Exception as e:
        if transaction:
            transaction.rollback()
        
        return ProcessingResult(
            status='failed',
            records_processed=0,
            records_written=0,
            error_message=str(e)
        )
```

#### Comparison with Alternative Approaches

| Approach | Records Written on Failure | Data Consistency | Recovery Complexity |
|----------|---------------------------|------------------|-------------------|
| **Transaction-Based (Current)** | 0 | Perfect | Simple - retry from same bookmark |
| **Record-by-Record Commits** | 5,000 | Inconsistent | Complex - track individual record states |
| **Batch Commits (e.g., 1,000)** | 4,000 | Partially consistent | Moderate - track batch boundaries |
| **Skip Failed Records** | 9,999 | Incomplete | Complex - track skipped records |

#### Recovery Process

When the job runs again after a failure:

```
Recovery Run:
1. 📖 Read bookmark: "2024-01-01 00:00:00" (unchanged)
2. 🔍 Query source: WHERE timestamp > '2024-01-01 00:00:00'
3. 📊 Find: Same 10,000 records (including the previously failed record)
4. 🔄 Retry: Process all 10,000 records again
5. ✅ Success: All records committed, bookmark updated
```

#### Benefits of This Approach

1. **Data Integrity**: Target table always reflects a consistent point-in-time state
2. **Simple Recovery**: Just re-run the job - no complex state tracking needed
3. **Audit Trail**: Bookmark values directly correlate to data in target tables
4. **Predictable Behavior**: Easy to understand and troubleshoot

#### Important Limitation: Large Dataset Reality

**Critical Note**: The "single transaction per table" approach described above applies to **smaller datasets** (typically < 10 million records). For very large datasets (hundreds of millions or billions of records), Spark automatically partitions the data, and each partition becomes a separate database transaction.

This means:
- **Large tables may have partial commits** if failures occur mid-processing
- **True atomicity is only guaranteed for smaller tables** that fit within database transaction limits
- **Large table failures may require cleanup** before retry operations

See the [Performance Considerations and Large Dataset Limitations](#performance-considerations-and-large-dataset-limitations) section below for detailed information on how the system handles large datasets.

#### Performance Considerations and Large Dataset Limitations

While this approach prioritizes consistency, there are important limitations and considerations for large datasets:

##### Large Dataset Reality: Spark Partitioning

**Important Clarification**: For very large datasets (e.g., 1 billion records), the "single transaction per table" is actually implemented through **Spark partitioning**, not traditional database transactions.

Here's what actually happens:

1. **Spark DataFrame Partitioning**: Large datasets are automatically partitioned by Spark
2. **Partition-Level Atomicity**: Each partition is written as a separate database transaction
3. **Coordinated Failure**: If any partition fails, Spark fails the entire write operation
4. **Rollback Behavior**: Failed partitions may leave partial data that needs cleanup

##### Real-World Transaction Behavior

```python
# What actually happens with 1 billion records
def write_large_table(df: DataFrame, connection_config: ConnectionConfig):
    """
    Spark automatically partitions large DataFrames.
    Each partition becomes a separate database transaction.
    """
    try:
        # Spark may create 1000+ partitions for 1 billion records
        # Each partition = ~1 million records = 1 database transaction
        df.write.format('jdbc') \
          .option('url', connection_config.connection_string) \
          .option('dbtable', table_name) \
          .option('batchsize', '10000') \
          .mode('append') \
          .save()  # This creates multiple database transactions
          
    except Exception as e:
        # If any partition fails, entire operation fails
        # But some partitions may have already committed
        logger.error(f"Large table write failed: {e}")
        raise
```

##### Database Engine Transaction Limits

Different database engines have varying transaction limits:

| Database Engine | Typical Transaction Limits | 1 Billion Record Feasibility |
|----------------|---------------------------|------------------------------|
| **PostgreSQL** | ~2GB transaction log limit | ❌ Not feasible in single transaction |
| **Oracle** | Depends on undo tablespace | ❌ Would require massive undo space |
| **SQL Server** | Transaction log size limit | ❌ Would exhaust transaction log |
| **MySQL** | InnoDB buffer pool limits | ❌ Memory and log constraints |

##### Actual Behavior with Large Datasets

For tables with 1 billion records:

```
Spark Processing Flow:
1. 📊 DataFrame: 1 billion records
2. 🔀 Auto-partition: ~1000 partitions (1M records each)
3. 💾 Write Process:
   ├── Partition 1: Transaction 1 (1M records) ✅ COMMIT
   ├── Partition 2: Transaction 2 (1M records) ✅ COMMIT
   ├── ...
   ├── Partition 500: Transaction 500 (1M records) ❌ FAIL
   └── Result: Spark fails entire operation, but 499M records may be committed
```

##### Implications for Data Consistency

This reveals critical limitations that affect bookmark accuracy:

1. **Partial Commits Possible**: With very large datasets, some data may be committed before failure
2. **Cleanup Required**: Failed operations may leave partial data in target tables
3. **Bookmark Accuracy**: Bookmarks may not reflect actual data state after large table failures
4. **Recovery Complexity**: May require data cleanup before retry

##### Critical Issue: Bookmark Update Timing

**The bookmark is updated ONLY after the entire Spark write operation completes successfully.** This creates a serious consistency problem with large datasets:

```python
# Current implementation sequence
def process_incremental_data(table_name, source_df):
    try:
        # 1. Write data (may partially succeed with large datasets)
        spark_writer.save()  # This may commit some partitions before failing
        
        # 2. Update bookmark ONLY if entire operation succeeds
        new_max_value = get_max_value(source_df)
        bookmark_manager.update_bookmark_state(table_name, new_max_value)
        
    except Exception as e:
        # If ANY partition fails, bookmark is NOT updated
        # But some partitions may have already committed data
        logger.error(f"Write failed, bookmark not updated: {e}")
        raise
```

##### Bookmark Inconsistency Scenarios

**Scenario 1: Large Table Partial Failure**
```
Table: orders (1 billion records)
Bookmark before: "2024-01-01 00:00:00"
Processing: Records from "2024-01-01 00:00:00" to "2024-01-31 23:59:59"

Spark Partitioning:
├── Partition 1: 2024-01-01 to 2024-01-03 ✅ COMMITTED
├── Partition 2: 2024-01-04 to 2024-01-06 ✅ COMMITTED
├── ...
├── Partition 500: 2024-01-15 to 2024-01-17 ❌ FAILED
└── Result: 
    - Target table: Contains data up to 2024-01-14
    - Bookmark: Still "2024-01-01 00:00:00" (NOT UPDATED)
    - Inconsistency: Target has more data than bookmark indicates
```

**Scenario 2: Recovery Run Problems**
```
Next job run:
1. Reads bookmark: "2024-01-01 00:00:00"
2. Queries source: WHERE date > '2024-01-01 00:00:00'
3. Finds: Records from 2024-01-01 to 2024-01-31 (including already committed data)
4. Attempts to write: DUPLICATE KEY ERRORS for records 2024-01-01 to 2024-01-14
```

##### Per-Partition Bookmark Updates: Not Implemented

**Important**: The current system does NOT update bookmarks per partition. Bookmarks are updated only once per table, after the entire write operation succeeds.

```python
# What DOESN'T happen (per-partition updates)
for partition in spark_partitions:
    try:
        write_partition(partition)
        # This does NOT happen:
        # update_bookmark_for_partition(partition.max_value)
    except Exception:
        # Partition failure doesn't update bookmark
        continue

# What DOES happen (single table-level update)
try:
    write_all_partitions()  # May partially succeed
    # Only if ALL partitions succeed:
    update_bookmark_state(table_name, overall_max_value)
except Exception:
    # No bookmark update at all, even if some partitions succeeded
    pass
```

##### Mitigation Strategies for Large Tables

1. **Pre-Write Cleanup Strategy**:
   ```python
   def handle_large_table_with_cleanup(table_name: str, connection_config: ConnectionConfig, 
                                     bookmark_state: BookmarkState):
       """Clean up potential partial data before processing."""
       try:
           # Check if target table has data beyond bookmark
           max_target_value = get_max_incremental_value(connection_config, table_name)
           
           if max_target_value > bookmark_state.last_processed_value:
               logger.warning(f"Target table {table_name} has data beyond bookmark. Cleaning up.")
               
               # Delete data beyond bookmark to ensure consistency
               cleanup_sql = f"""
                   DELETE FROM {connection_config.schema}.{table_name} 
                   WHERE {bookmark_state.incremental_column} > '{bookmark_state.last_processed_value}'
               """
               execute_sql(cleanup_sql, connection_config)
               
       except Exception as e:
           logger.error(f"Failed to cleanup table {table_name}: {e}")
           raise
   ```

2. **Staging Table Approach with Bookmark Consistency**:
   ```python
   def write_large_table_with_staging(df: DataFrame, table_name: str, bookmark_state: BookmarkState):
       """Use staging table for large dataset atomicity."""
       staging_table = f"{table_name}_staging_{job_run_id}"
       
       try:
           # Write to staging table (can handle partial failures safely)
           df.write.format('jdbc') \
             .option('dbtable', staging_table) \
             .mode('overwrite') \
             .save()
           
           # Verify staging table completeness
           staging_count = get_table_count(staging_table)
           expected_count = df.count()
           
           if staging_count != expected_count:
               raise Exception(f"Staging table incomplete: {staging_count}/{expected_count}")
           
           # Atomic operations: swap tables and update bookmark together
           with database_transaction():
               swap_tables(staging_table, table_name)
               new_max_value = get_max_value_from_dataframe(df)
               update_bookmark_state(table_name, new_max_value)
           
       except Exception as e:
           # Cleanup staging table on failure
           drop_table_if_exists(staging_table)
           raise e
   ```

3. **Incremental Batch Processing with Consistent Bookmarks**:
   ```python
   def process_large_table_incrementally(table_name: str, bookmark_state: BookmarkState, 
                                       batch_size: int = 1000000):
       """Process large tables in smaller, manageable batches with consistent bookmarks."""
       current_bookmark = bookmark_state.last_processed_value
       
       while True:
           # Read batch from current bookmark position
           batch_df = read_incremental_batch(
               table_name, 
               current_bookmark, 
               batch_size,
               bookmark_state.incremental_column
           )
           
           if batch_df.count() == 0:
               break
           
           try:
               # Write batch (small enough for true transaction atomicity)
               write_batch_with_transaction(batch_df, table_name)
               
               # Update bookmark immediately after successful batch
               batch_max_value = get_max_value_from_dataframe(batch_df)
               update_bookmark_state(table_name, batch_max_value)
               
               # Update current position for next batch
               current_bookmark = batch_max_value
               
               logger.info(f"Processed batch for {table_name}: {batch_df.count()} records, "
                          f"bookmark updated to {batch_max_value}")
               
           except Exception as e:
               logger.error(f"Batch failed for {table_name} at bookmark {current_bookmark}: {e}")
               # Bookmark remains at last successful position
               raise e
   ```

4. **Idempotent Write Strategy**:
   ```python
   def write_with_idempotency(df: DataFrame, table_name: str, bookmark_state: BookmarkState):
       """Write data with built-in duplicate handling."""
       try:
           # Use MERGE/UPSERT instead of INSERT to handle duplicates
           df.write.format('jdbc') \
             .option('dbtable', table_name) \
             .option('writeMode', 'upsert') \
             .option('mergeKey', bookmark_state.incremental_column) \
             .mode('append') \
             .save()
           
           # Update bookmark after successful write
           new_max_value = get_max_value_from_dataframe(df)
           update_bookmark_state(table_name, new_max_value)
           
       except Exception as e:
           logger.error(f"Idempotent write failed for {table_name}: {e}")
           raise
   ```

5. **Bookmark Validation and Recovery**:
   ```python
   def validate_and_recover_bookmark(table_name: str, connection_config: ConnectionConfig,
                                   bookmark_state: BookmarkState):
       """Validate bookmark consistency and recover if needed."""
       try:
           # Get actual max value from target table
           actual_max_value = get_max_incremental_value(connection_config, table_name)
           bookmark_value = bookmark_state.last_processed_value
           
           if actual_max_value > bookmark_value:
               logger.warning(f"Bookmark inconsistency detected for {table_name}: "
                            f"target_max={actual_max_value}, bookmark={bookmark_value}")
               
               # Option 1: Update bookmark to match target (if data is valid)
               if validate_target_data_integrity(table_name, bookmark_value, actual_max_value):
                   logger.info(f"Updating bookmark to match target data: {actual_max_value}")
                   update_bookmark_state(table_name, actual_max_value)
               
               # Option 2: Clean target data to match bookmark (if data is suspect)
               else:
                   logger.info(f"Cleaning target data to match bookmark: {bookmark_value}")
                   cleanup_target_data_beyond_bookmark(table_name, bookmark_value)
           
       except Exception as e:
           logger.error(f"Bookmark validation failed for {table_name}: {e}")
           raise
   ```

##### Summary: Bookmark Consistency with Large Datasets

**Direct Answer to Key Questions:**

1. **Does the bookmark get updated per partition?** 
   - **No.** Bookmarks are updated only once per table, after the entire write operation succeeds.

2. **Can bookmark files show incorrect values after partition failures?**
   - **Yes.** If some partitions succeed but others fail, the target table will contain more data than the bookmark indicates.

3. **What happens on recovery?**
   - The job will attempt to reprocess data that may already exist in the target, potentially causing duplicate key errors.

**Critical Bookmark Behavior:**
```
Large Table Processing:
├── Partition 1: ✅ Data committed to database
├── Partition 2: ✅ Data committed to database  
├── Partition 3: ❌ Fails
└── Result:
    ├── Target Table: Contains data from partitions 1-2
    ├── Bookmark: NOT UPDATED (remains at previous value)
    └── Inconsistency: Target has more data than bookmark shows
```

##### Recommendations for Large Datasets

1. **Size Thresholds**: Define size thresholds for different processing strategies
   - < 10M records: Single transaction approach (bookmark consistency guaranteed)
   - 10M - 100M records: Staging table approach with atomic bookmark updates
   - > 100M records: Incremental batch processing with frequent bookmark updates

2. **Configuration Parameters**: Add parameters to control large table handling
   ```json
   {
     "LargeTableThreshold": 10000000,
     "BatchProcessingSize": 1000000,
     "UseStagingTables": true,
     "EnableTableCleanupOnFailure": true,
     "EnableBookmarkValidation": true,
     "EnableIdempotentWrites": true
   }
   ```

3. **Monitoring**: Enhanced monitoring for large table operations and bookmark consistency
   ```python
   # Additional metrics for large tables and bookmark consistency
   self.put_metric('PartitionCount', partition_count, 'Count')
   self.put_metric('PartialCommits', partial_commit_count, 'Count')
   self.put_metric('BookmarkInconsistencies', inconsistency_count, 'Count')
   self.put_metric('CleanupOperations', cleanup_count, 'Count')
   self.put_metric('BookmarkValidationFailures', validation_failures, 'Count')
   ```

4. **Operational Procedures**: Establish procedures for bookmark inconsistency detection and resolution
   - Pre-job bookmark validation
   - Post-failure cleanup procedures  
   - Regular bookmark consistency audits
   - Automated recovery workflows

##### Traditional Performance Considerations

For smaller datasets (< 10M records), the original considerations apply:

1. **Memory Usage**: All records for a table are held in memory during processing
2. **Lock Duration**: Database locks are held for the entire table processing duration
3. **Retry Cost**: Failed tables require complete reprocessing
4. **Transaction Timeouts**: Very large tables may need longer transaction timeouts

#### Monitoring Transaction-Based Processing

Key metrics to monitor:

```python
# CloudWatch metrics for transaction processing
self.put_metric('TransactionDuration', transaction_time, 'Seconds')
self.put_metric('RecordsInTransaction', record_count, 'Count')
self.put_metric('TransactionRollbacks', rollback_count, 'Count')
self.put_metric('TransactionCommits', commit_count, 'Count')
```

#### Best Practices for Transaction-Based Processing

1. **Batch Size Optimization**: Configure appropriate batch sizes for your data volume
2. **Timeout Configuration**: Set adequate transaction timeouts for large datasets
3. **Memory Management**: Ensure sufficient memory for processing large record sets
4. **Error Analysis**: Investigate root causes of failures to prevent repeated rollbacks
5. **Monitoring**: Track transaction success rates and processing times

### Transaction Boundaries

```
Job Level
├── Table 1 (Transaction Boundary)
│   ├── Read from Source ✓
│   ├── Transform Data ✓
│   └── Write to Target ✗ (ROLLBACK - No data written)
├── Table 2 (Transaction Boundary)
│   ├── Read from Source ✓
│   ├── Transform Data ✓
│   └── Write to Target ✓ (COMMIT - All data written)
└── Table 3 (Transaction Boundary)
    ├── Read from Source ✓
    ├── Transform Data ✓
    └── Write to Target ✓ (COMMIT - All data written)
```

## Bookmark Management During Failures

### Bookmark Update Logic

Bookmarks are only updated after successful table processing:

```python
# Bookmark update logic
if progress.status == 'completed':
    # Update bookmark only on success
    bookmark_manager.update_bookmark_state(
        table_name, 
        new_max_value, 
        progress.processed_rows
    )
    successful_tables += 1
else:
    # Bookmark remains unchanged on failure
    failed_tables += 1
    logger.error(f"Failed to process table {table_name}: {progress.error_message}")
```

### Recovery Behavior

| Scenario | Bookmark State | Next Run Behavior |
|----------|----------------|-------------------|
| **Successful Full Load** | Updated to max incremental column value | Switches to incremental mode |
| **Failed Full Load** | Remains at initial state | Retries full load from beginning |
| **Successful Incremental** | Updated with new max value | Continues from new position |
| **Failed Incremental** | Remains at previous value | Retries from last successful position |
| **Partial Processing** | Not updated | Restarts table from last bookmark |

### S3 Bookmark Resilience

The bookmark system includes multiple layers of resilience:

1. **In-Memory Fallback**: If S3 operations fail, bookmarks are maintained in memory
2. **Batch Operations**: Multiple bookmark updates are batched for efficiency
3. **Error Recovery**: S3 errors don't prevent job execution
4. **Consistency Checks**: Bookmark data is validated before use

```python
def _handle_s3_error(self, operation: str, table_name: str, error: Exception):
    """Handle S3 operation errors with graceful degradation."""
    error_info = {
        'operation': operation,
        'table_name': table_name,
        'error_type': type(error).__name__,
        'error_message': str(error),
        'should_retry': self._should_retry_s3_operation(error),
        'should_fallback': operation == 'read'  # Fallback only for reads
    }
    
    if error_info['should_fallback']:
        logger.warning(f"S3 {operation} failed for {table_name}, using in-memory fallback")
    
    return error_info
```

## Job Completion Behavior

### Success Criteria

The job completion behavior is designed to maximize data processing:

- **Job Success**: Job completes successfully even if some tables fail
- **Partial Success**: Processing continues for all tables regardless of individual failures
- **Comprehensive Reporting**: Detailed metrics on success/failure rates

### Completion Metrics

```python
# Job completion summary
logger.info(f"Job completed: {successful_tables} successful, {failed_tables} failed")

# CloudWatch metrics
self.put_metric('SuccessfulTables', successful_tables, 'Count')
self.put_metric('FailedTables', failed_tables, 'Count')
self.put_metric('SuccessRate', success_rate, 'Percent')
self.put_metric('TotalRowsProcessed', total_rows, 'Count')
```

### Exit Codes

| Scenario | Exit Code | Description |
|----------|-----------|-------------|
| **All Tables Successful** | 0 | Complete success |
| **Partial Success** | 0 | Some tables processed successfully |
| **All Tables Failed** | 0 | Job structure completed, but no data processed |
| **Critical Configuration Error** | 1 | Job cannot start due to configuration issues |
| **Infrastructure Failure** | 1 | AWS service or network failures prevent execution |

### When Jobs Actually Fail

The job only fails (exit code 1) in these scenarios:

1. **Configuration Errors**: Invalid connection strings, missing parameters
2. **Infrastructure Failures**: Cannot access AWS services, network connectivity issues
3. **Permission Errors**: Insufficient IAM permissions for required operations
4. **Critical Resource Issues**: Out of memory, disk space, or other system resources

## Monitoring and Alerting

### CloudWatch Metrics

The system publishes comprehensive metrics for monitoring:

#### Job-Level Metrics
- `JobDuration`: Total job execution time
- `TotalTables`: Number of tables processed
- `SuccessfulTables`: Number of successfully processed tables
- `FailedTables`: Number of failed tables
- `SuccessRate`: Percentage of successful tables

#### Table-Level Metrics
- `TableProcessingDuration`: Time to process individual tables
- `RowsProcessed`: Number of rows processed per table
- `ProcessingThroughput`: Rows processed per second

#### Error Metrics
- `ConnectionErrors`: Database connection failures
- `RetryAttempts`: Number of retry operations
- `S3Errors`: S3 bookmark operation failures
- `NetworkErrors`: Network connectivity issues

### Structured Logging

All errors are logged with structured context:

```json
{
  "timestamp": "2024-01-15T10:30:00.123Z",
  "level": "ERROR",
  "job_name": "data-replication-job",
  "job_run_id": "jr_2024011510302245",
  "component": "data_migrator",
  "operation": "write_table_data",
  "table_name": "users",
  "error_type": "ConnectionTimeoutException",
  "error_message": "Connection timed out after 30 seconds",
  "error_category": "network",
  "retry_attempt": 2,
  "is_retryable": true,
  "recovery_strategy": "retry_with_backoff"
}
```

### Alerting Strategies

#### Critical Alerts (Immediate Response)
- Job fails to start due to configuration errors
- All tables fail in a single job run
- Infrastructure connectivity issues

#### Warning Alerts (Monitor Closely)
- Success rate drops below 80%
- Individual table consistently fails across multiple runs
- High retry rates or extended processing times

#### Informational Alerts (Trend Monitoring)
- Processing throughput changes
- Bookmark operation patterns
- Resource utilization trends

## Recovery Strategies

### Automatic Recovery

The system includes several automatic recovery mechanisms:

1. **Retry Logic**: Automatic retries for transient errors
2. **Circuit Breakers**: Prevent cascading failures
3. **Graceful Degradation**: Continue operation when optional components fail
4. **Bookmark Persistence**: Maintain state for recovery in subsequent runs

### Manual Recovery

For non-retryable errors, manual intervention may be required:

#### Common Recovery Actions

1. **Configuration Issues**:
   ```bash
   # Update CloudFormation parameters
   aws cloudformation update-stack \
     --stack-name glue-data-replication \
     --parameters ParameterKey=SourceConnectionString,ParameterValue="new-value"
   ```

2. **Permission Issues**:
   ```bash
   # Update IAM role permissions
   aws iam attach-role-policy \
     --role-name GlueJobExecutionRole \
     --policy-arn arn:aws:iam::aws:policy/AmazonS3FullAccess
   ```

3. **Schema Mismatches**:
   ```sql
   -- Add missing columns to target table
   ALTER TABLE target_schema.users ADD COLUMN new_column VARCHAR(255);
   ```

4. **Network Connectivity**:
   ```bash
   # Update security group rules
   aws ec2 authorize-security-group-ingress \
     --group-id sg-12345678 \
     --protocol tcp \
     --port 1521 \
     --source-group sg-87654321
   ```

### Recovery Verification

After manual recovery actions:

1. **Test Connections**: Verify database connectivity
2. **Validate Configuration**: Ensure all parameters are correct
3. **Check Permissions**: Verify IAM roles and policies
4. **Monitor Next Run**: Watch for successful processing

## Best Practices

### Error Prevention

1. **Validate Configuration**: Test all connection strings and parameters before deployment
2. **Monitor Resource Usage**: Ensure adequate memory and CPU for data volumes
3. **Network Planning**: Verify security groups and network connectivity
4. **Schema Alignment**: Ensure source and target schemas are compatible

### Error Response

1. **Monitor Metrics**: Set up CloudWatch dashboards and alerts
2. **Review Logs**: Regularly check structured logs for error patterns
3. **Trend Analysis**: Track success rates and processing times over time
4. **Proactive Maintenance**: Address warning-level issues before they become critical

### Recovery Planning

1. **Document Procedures**: Maintain runbooks for common error scenarios
2. **Test Recovery**: Regularly test recovery procedures in non-production environments
3. **Backup Strategies**: Ensure bookmark and configuration data is backed up
4. **Escalation Paths**: Define clear escalation procedures for critical failures

### Performance Optimization

1. **Batch Sizing**: Optimize batch sizes for your data volumes and network conditions
2. **Parallel Processing**: Use parallel bookmark operations for large numbers of tables
3. **Resource Allocation**: Allocate appropriate Glue job resources (DPUs, memory)
4. **Connection Pooling**: Optimize database connection settings for your workload

## Conclusion

The AWS Glue Data Replication system's error handling strategy prioritizes data consistency and job resilience. By implementing table-level error isolation, intelligent retry mechanisms, and comprehensive monitoring, the system ensures that:

- **Data integrity is never compromised** through partial writes
- **Jobs continue processing** even when individual tables fail
- **Recovery is seamless** through persistent bookmark management
- **Operations teams have visibility** into all aspects of job execution

This approach enables reliable, large-scale data replication operations while minimizing the impact of transient failures and providing clear paths for recovery from persistent issues.