# Error Handling During Data Transfer

This document provides comprehensive details about how errors are handled during data transfer operations in the AWS Glue Data Replication system.

## Table of Contents

1. [Overview](#overview)
2. [Error Handling Philosophy](#error-handling-philosophy)
3. [Table-Level Error Isolation](#table-level-error-isolation)
4. [Error Classification](#error-classification)
5. [Retry Mechanisms](#retry-mechanisms)
6. [Record and Transaction Handling](#record-and-transaction-handling)
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

```python
# Example retryable error patterns
RETRYABLE_PATTERNS = [
    'connection refused', 'connection timed out', 'connection reset',
    'network unreachable', 'service unavailable', 'throttling'
]
```

#### 2. Non-Retryable Errors
Errors that require manual intervention:

- **Authentication Failures**: `authentication failed`, `invalid credentials`, `access denied`
- **Authorization Issues**: `permission denied`, `insufficient privileges`, `forbidden`
- **Configuration Errors**: `invalid connection string`, `missing parameters`
- **Schema Mismatches**: `column not found`, `table not found`, `data type mismatch`

```python
# Example non-retryable error patterns
NON_RETRYABLE_PATTERNS = [
    'authentication failed', 'permission denied', 'column not found',
    'invalid credentials', 'table does not exist'
]
```

#### 3. Data Processing Errors
Errors related to data quality or format:

- **Data Type Issues**: `conversion error`, `invalid data format`, `data truncation`
- **Constraint Violations**: `duplicate key`, `foreign key violation`, `check constraint`
- **Data Corruption**: `invalid data`, `corrupted record`, `parsing error`

### Error Classification Logic

```python
class ErrorClassifier:
    @classmethod
    def classify_error(cls, error: Exception) -> str:
        error_message = str(error).lower()
        
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
            ErrorCategory.RESOURCE
        }
        return category in retryable_categories
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
| **Successful Full Load** | Updated to "full_load_completed" | Switches to incremental mode |
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