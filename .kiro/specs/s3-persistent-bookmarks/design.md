# Design Document

## Overview

This design implements persistent bookmark storage on S3 for the AWS Glue data replication system. The enhancement replaces the current in-memory bookmark system with a robust S3-based solution that maintains state across job executions. The design leverages the existing S3 bucket used for JDBC drivers, ensuring no additional infrastructure or permissions are required. The implementation maintains full backward compatibility while providing true incremental loading capabilities.

## Architecture

### High-Level Architecture

```mermaid
graph TB
    subgraph "AWS Glue Job Execution"
        GJ[Glue Job<br/>PySpark Script]
        BM[Bookmark Manager<br/>Enhanced]
        S3C[S3 Client<br/>boto3]
    end
    
    subgraph "S3 Bucket (Existing)"
        subgraph "JDBC Drivers"
            SJD[Source Driver JAR]
            TJD[Target Driver JAR]
        end
        subgraph "Bookmarks (New)"
            BF1[job1/table1.json]
            BF2[job1/table2.json]
            BF3[job2/table1.json]
        end
    end
    
    subgraph "Database Sources/Targets"
        SRC[Source Database]
        TGT[Target Database]
    end
    
    GJ --> BM
    BM --> S3C
    S3C --> BF1
    S3C --> BF2
    S3C --> BF3
    GJ --> SJD
    GJ --> TJD
    GJ --> SRC
    GJ --> TGT
    
    style BF1 fill:#e8f5e8
    style BF2 fill:#e8f5e8
    style BF3 fill:#e8f5e8
    style BM fill:#fff3e0
```

### Bookmark Storage Architecture

```mermaid
graph TB
    subgraph "S3 Bucket Structure"
        subgraph "drivers/"
            D1[oracle-driver.jar]
            D2[sqlserver-driver.jar]
            D3[postgresql-driver.jar]
        end
        
        subgraph "bookmarks/"
            subgraph "job-name-1/"
                B1[customers.json]
                B2[orders.json]
                B3[products.json]
            end
            subgraph "job-name-2/"
                B4[users.json]
                B5[transactions.json]
            end
        end
    end
    
    subgraph "Bookmark JSON Structure"
        JSON["{<br/>  'table_name': 'customers',<br/>  'incremental_strategy': 'timestamp',<br/>  'incremental_column': 'updated_at',<br/>  'last_processed_value': '2024-01-15T10:30:00Z',<br/>  'last_update_timestamp': '2024-01-15T11:00:00Z',<br/>  'is_first_run': false,<br/>  'job_name': 'customer-replication',<br/>  'created_timestamp': '2024-01-01T09:00:00Z',<br/>  'updated_timestamp': '2024-01-15T11:00:00Z',<br/>  'version': '1.0'<br/>}"]
    end
    
    B1 --> JSON
    
    style B1 fill:#e8f5e8
    style B2 fill:#e8f5e8
    style B3 fill:#e8f5e8
    style B4 fill:#e8f5e8
    style B5 fill:#e8f5e8
```

## Components and Interfaces

### Enhanced JobBookmarkManager Class

**Purpose**: Manages persistent bookmark state using S3 storage

**Key Enhancements**:
- S3 bucket detection from JDBC driver paths
- Asynchronous bookmark read/write operations
- Error handling and fallback to in-memory bookmarks
- JSON serialization/deserialization with validation

**New Methods**:
```python
class JobBookmarkManager:
    def __init__(self, glue_context: GlueContext, job_name: str, 
                 source_jdbc_path: str, target_jdbc_path: str, job: Job = None):
        # Enhanced constructor with S3 path parameters
        
    def _extract_s3_bucket(self, s3_path: str) -> str:
        # Extract bucket name from S3 path
        
    def _get_bookmark_s3_key(self, table_name: str) -> str:
        # Generate S3 key for bookmark file
        
    async def _read_bookmark_from_s3(self, table_name: str) -> Optional[Dict[str, Any]]:
        # Asynchronously read bookmark from S3
        
    async def _write_bookmark_to_s3(self, table_name: str, bookmark_data: Dict[str, Any]) -> bool:
        # Asynchronously write bookmark to S3
        
    def _validate_bookmark_data(self, data: Dict[str, Any]) -> bool:
        # Validate bookmark JSON structure
        
    def _handle_s3_error(self, operation: str, table_name: str, error: Exception) -> None:
        # Handle S3 operation errors with appropriate fallback
```

### S3BookmarkStorage Class

**Purpose**: Dedicated class for S3 bookmark operations

**Interface**:
```python
@dataclass
class S3BookmarkConfig:
    bucket_name: str
    bookmark_prefix: str
    job_name: str
    retry_attempts: int = 3
    timeout_seconds: int = 30

class S3BookmarkStorage:
    def __init__(self, config: S3BookmarkConfig):
        self.config = config
        self.s3_client = boto3.client('s3', config=Config(
            retries={'max_attempts': config.retry_attempts},
            read_timeout=config.timeout_seconds
        ))
    
    async def read_bookmark(self, table_name: str) -> Optional[Dict[str, Any]]:
        # Read bookmark from S3 with error handling
        
    async def write_bookmark(self, table_name: str, bookmark_data: Dict[str, Any]) -> bool:
        # Write bookmark to S3 with error handling
        
    async def delete_bookmark(self, table_name: str) -> bool:
        # Delete corrupted bookmark from S3
        
    def list_bookmarks(self) -> List[str]:
        # List all bookmark files for the job
```

### Enhanced JobBookmarkState Class

**Purpose**: Extended bookmark state with S3 metadata

**Enhanced Structure**:
```python
@dataclass
class JobBookmarkState:
    # Existing fields
    table_name: str
    incremental_strategy: str
    incremental_column: Optional[str] = None
    last_processed_value: Optional[Any] = None
    last_update_timestamp: Optional[datetime] = None
    is_first_run: bool = True
    
    # New S3-specific fields
    job_name: str = ""
    created_timestamp: Optional[datetime] = None
    updated_timestamp: Optional[datetime] = None
    version: str = "1.0"
    s3_key: Optional[str] = None
    
    def to_s3_dict(self) -> Dict[str, Any]:
        # Convert to S3-compatible dictionary with ISO timestamps
        
    @classmethod
    def from_s3_dict(cls, data: Dict[str, Any]) -> 'JobBookmarkState':
        # Create from S3 dictionary with validation
        
    def validate_s3_data(self) -> bool:
        # Validate S3 bookmark data integrity
```

## Data Models

### Bookmark File Structure

**S3 Path Pattern**: `s3://{bucket}/bookmarks/{job_name}/{table_name}.json`

**JSON Schema**:
```json
{
  "type": "object",
  "required": ["table_name", "incremental_strategy", "job_name", "version"],
  "properties": {
    "table_name": {"type": "string"},
    "incremental_strategy": {"type": "string", "enum": ["timestamp", "primary_key", "hash"]},
    "incremental_column": {"type": ["string", "null"]},
    "last_processed_value": {"type": ["string", "number", "null"]},
    "last_update_timestamp": {"type": ["string", "null"], "format": "date-time"},
    "is_first_run": {"type": "boolean"},
    "job_name": {"type": "string"},
    "created_timestamp": {"type": "string", "format": "date-time"},
    "updated_timestamp": {"type": "string", "format": "date-time"},
    "version": {"type": "string"}
  }
}
```

### S3 Bucket Detection Logic

**Algorithm**:
1. Extract bucket name from source JDBC driver S3 path
2. If source and target use different buckets, prefer source bucket
3. Validate bucket accessibility before proceeding
4. Fall back to in-memory bookmarks if S3 access fails

**Implementation**:
```python
def extract_s3_bucket(s3_path: str) -> str:
    """
    Extract bucket name from S3 path.
    Example: s3://my-bucket/drivers/oracle.jar -> my-bucket
    """
    if not s3_path.startswith('s3://'):
        raise ValueError(f"Invalid S3 path: {s3_path}")
    
    # Remove s3:// prefix and split by /
    path_parts = s3_path[5:].split('/')
    if len(path_parts) < 1:
        raise ValueError(f"Invalid S3 path format: {s3_path}")
    
    return path_parts[0]
```

## Error Handling

### S3 Operation Error Handling

**Error Categories and Responses**:

1. **Access Denied (403)**:
   - Log warning about insufficient permissions
   - Fall back to in-memory bookmarks
   - Continue job execution

2. **Bucket Not Found (404)**:
   - Log error about invalid bucket configuration
   - Fall back to in-memory bookmarks
   - Continue job execution

3. **Network Timeouts**:
   - Retry with exponential backoff (3 attempts)
   - Log retry attempts and final outcome
   - Fall back to in-memory bookmarks on final failure

4. **Corrupted Bookmark Data**:
   - Log warning about data corruption
   - Delete corrupted file from S3
   - Perform full load and create new bookmark

**Error Handling Flow**:
```mermaid
graph TD
    A[S3 Operation] --> B{Success?}
    B -->|Yes| C[Continue Processing]
    B -->|No| D{Error Type}
    
    D -->|Access Denied| E[Log Warning]
    D -->|Not Found| F[Log Error]
    D -->|Timeout| G[Retry with Backoff]
    D -->|Corruption| H[Delete Corrupted File]
    
    E --> I[Fall back to In-Memory]
    F --> I
    G --> J{Retry Success?}
    H --> K[Perform Full Load]
    
    J -->|Yes| C
    J -->|No| I
    
    I --> L[Continue Job Execution]
    K --> M[Create New Bookmark]
    M --> C
```

### Resilience Patterns

**Circuit Breaker Pattern**:
- Track S3 operation failure rate
- Temporarily disable S3 operations if failure rate exceeds threshold
- Automatically re-enable after cooldown period

**Graceful Degradation**:
- Always prioritize job execution over bookmark persistence
- Log all fallback scenarios for monitoring
- Provide clear error messages for troubleshooting

## Testing Strategy

### Unit Testing

**S3BookmarkStorage Testing**:
- Mock S3 client for isolated testing
- Test all error scenarios (403, 404, timeouts)
- Validate JSON serialization/deserialization
- Test retry logic and exponential backoff

**JobBookmarkManager Testing**:
- Test S3 bucket extraction from various path formats
- Test bookmark state transitions (first run → incremental)
- Test fallback to in-memory bookmarks
- Test concurrent bookmark operations

### Integration Testing

**S3 Integration Testing**:
- Test with real S3 bucket and IAM permissions
- Test bookmark persistence across job executions
- Test with different S3 bucket configurations
- Test network failure scenarios

**End-to-End Testing**:
- Deploy enhanced system in test environment
- Execute multiple job runs with incremental data
- Verify bookmark state persistence and accuracy
- Test recovery from corrupted bookmark files

### Performance Testing

**S3 Operation Performance**:
- Measure bookmark read/write latency
- Test with multiple concurrent tables
- Validate asynchronous operation benefits
- Monitor S3 request costs and optimization opportunities

**Scalability Testing**:
- Test with large numbers of tables (100+)
- Test with high-frequency job executions
- Monitor S3 rate limiting and throttling
- Test bookmark file size growth over time

### Test Scenarios

**Scenario 1: First-Time Deployment**
- Deploy enhanced system to existing job
- Verify full load execution and bookmark creation
- Confirm S3 bookmark files are created correctly

**Scenario 2: Incremental Loading**
- Execute job multiple times with data changes
- Verify incremental loading using S3 bookmarks
- Confirm bookmark state updates correctly

**Scenario 3: Error Recovery**
- Simulate S3 access failures
- Verify fallback to in-memory bookmarks
- Confirm job continues execution successfully

**Scenario 4: Bookmark Corruption**
- Manually corrupt bookmark JSON files
- Verify detection and recovery mechanisms
- Confirm full load execution and bookmark recreation

## Implementation Phases

### Phase 1: Core S3 Integration
- Implement S3BookmarkStorage class
- Add S3 bucket detection logic
- Implement basic read/write operations
- Add comprehensive error handling

### Phase 2: Enhanced BookmarkManager
- Integrate S3BookmarkStorage into existing JobBookmarkManager
- Implement asynchronous operations
- Add fallback mechanisms
- Enhance logging and monitoring

### Phase 3: Testing and Validation
- Comprehensive unit and integration testing
- Performance testing and optimization
- Documentation updates
- Backward compatibility validation

### Phase 4: Deployment and Monitoring
- Deploy to test environments
- Monitor S3 operation metrics
- Validate cost implications
- Production deployment preparation

## Monitoring and Observability

### CloudWatch Metrics

**Custom Metrics**:
- `BookmarkS3ReadSuccess`: Successful S3 bookmark reads
- `BookmarkS3ReadFailure`: Failed S3 bookmark reads
- `BookmarkS3WriteSuccess`: Successful S3 bookmark writes
- `BookmarkS3WriteFailure`: Failed S3 bookmark writes
- `BookmarkFallbackToMemory`: Fallback to in-memory bookmarks

**Metric Dimensions**:
- JobName
- TableName
- ErrorType (for failure metrics)

### Structured Logging

**Log Events**:
- Bookmark S3 operations (read/write/delete)
- S3 bucket detection and validation
- Error conditions and fallback scenarios
- Performance metrics (operation latency)

**Log Format**:
```json
{
  "timestamp": "2024-01-15T11:00:00Z",
  "level": "INFO",
  "job_name": "customer-replication",
  "table_name": "customers",
  "operation": "bookmark_read",
  "s3_bucket": "my-glue-assets",
  "s3_key": "bookmarks/customer-replication/customers.json",
  "duration_ms": 150,
  "status": "success",
  "message": "Successfully read bookmark from S3"
}
```

### Alerting

**Alert Conditions**:
- High S3 operation failure rate (>10% over 5 minutes)
- Repeated fallback to in-memory bookmarks
- Bookmark corruption detection
- S3 access permission issues

## Security Considerations

### IAM Permissions

**Required S3 Permissions** (already granted via existing JDBC driver access):
- `s3:GetObject` on bookmark files
- `s3:PutObject` on bookmark files
- `s3:DeleteObject` on corrupted bookmark files
- `s3:ListBucket` for bookmark prefix

**Security Best Practices**:
- Use existing IAM role permissions (no additional grants needed)
- Encrypt bookmark files using S3 default encryption
- Implement least-privilege access patterns
- Log all S3 operations for audit trails

### Data Protection

**Bookmark Data Security**:
- No sensitive data stored in bookmark files
- Use S3 server-side encryption (SSE-S3)
- Implement data validation to prevent injection attacks
- Regular cleanup of old bookmark files

The design ensures robust, scalable, and secure persistent bookmark storage while maintaining full backward compatibility with existing deployments.