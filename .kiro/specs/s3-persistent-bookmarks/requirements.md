# Requirements Document

## Introduction

This feature enhances the existing AWS Glue data replication system by implementing persistent bookmark storage on S3. Currently, the system uses in-memory bookmarks that are lost between job runs, forcing full loads on every execution. This enhancement will store bookmark state in the same S3 bucket used for JDBC drivers, enabling true incremental loading across job executions while maintaining the existing PySpark DataFrame-based approach.

## Requirements

### Requirement 1: S3 Bookmark Persistence

**User Story:** As a data engineer, I want job bookmarks to persist in S3 between Glue job executions, so that incremental loads work correctly across multiple job runs without losing state.

#### Acceptance Criteria

1. WHEN a Glue job completes processing a table THEN the system SHALL store the bookmark state as a JSON file in S3
2. WHEN a Glue job starts processing a table THEN the system SHALL attempt to read existing bookmark state from S3
3. WHEN no bookmark state exists in S3 for a table THEN the system SHALL perform a full load and create initial bookmark state
4. WHEN bookmark state exists in S3 for a table THEN the system SHALL perform incremental loading using the stored state
5. WHEN bookmark state is corrupted or unreadable THEN the system SHALL log a warning, perform a full load, and overwrite the corrupted state

### Requirement 2: S3 Bucket Integration

**User Story:** As a DevOps engineer, I want bookmarks stored in the same S3 bucket as JDBC drivers, so that I can manage all job artifacts in a single location with consistent access policies.

#### Acceptance Criteria

1. WHEN storing bookmark files THEN the system SHALL use the same S3 bucket that contains the JDBC driver JARs
2. WHEN determining the S3 bucket THEN the system SHALL extract the bucket name from the existing JDBC driver S3 paths
3. WHEN storing bookmark files THEN the system SHALL use a dedicated prefix/folder structure for bookmarks (e.g., `bookmarks/{job_name}/`)
4. WHEN the JDBC driver S3 paths use different buckets THEN the system SHALL use the source JDBC driver bucket for bookmark storage
5. WHEN accessing S3 for bookmarks THEN the system SHALL use the existing IAM role permissions without requiring additional S3 access

### Requirement 3: Bookmark File Structure

**User Story:** As a data engineer, I want bookmark files to be organized and named consistently, so that I can easily identify and manage bookmark state for different jobs and tables.

#### Acceptance Criteria

1. WHEN storing bookmark files THEN the system SHALL use the path structure: `s3://{bucket}/bookmarks/{job_name}/{table_name}.json`
2. WHEN storing bookmark JSON THEN the system SHALL include: table_name, incremental_strategy, incremental_column, last_processed_value, last_update_timestamp, is_first_run
3. WHEN storing bookmark JSON THEN the system SHALL include metadata: job_name, created_timestamp, updated_timestamp, version
4. WHEN reading bookmark files THEN the system SHALL validate the JSON structure and handle missing or invalid fields gracefully
5. WHEN multiple tables are processed THEN each table SHALL have its own separate bookmark file

### Requirement 4: Backward Compatibility

**User Story:** As a data engineer, I want the S3 bookmark enhancement to work with existing job configurations, so that I can upgrade without changing CloudFormation parameters or job logic.

#### Acceptance Criteria

1. WHEN the enhanced bookmark system is deployed THEN existing job configurations SHALL continue to work without modification
2. WHEN existing jobs run for the first time with S3 bookmarks THEN they SHALL perform a full load and create initial bookmark state
3. WHEN the S3 bookmark system fails THEN the system SHALL fall back to in-memory bookmarks and log appropriate warnings
4. WHEN S3 access is unavailable THEN the system SHALL continue processing with full loads rather than failing the job
5. WHEN upgrading from in-memory to S3 bookmarks THEN the system SHALL not require changes to CloudFormation templates or job parameters

### Requirement 5: Error Handling and Resilience

**User Story:** As a data engineer, I want the bookmark system to be resilient to S3 failures and data corruption, so that temporary issues don't cause job failures or data inconsistencies.

#### Acceptance Criteria

1. WHEN S3 read operations fail THEN the system SHALL log the error, assume first run, and perform a full load
2. WHEN S3 write operations fail THEN the system SHALL log the error but continue job execution successfully
3. WHEN bookmark JSON is corrupted or invalid THEN the system SHALL log a warning, delete the corrupted file, and perform a full load
4. WHEN S3 access is denied THEN the system SHALL fall back to in-memory bookmarks and log appropriate warnings
5. WHEN network timeouts occur during S3 operations THEN the system SHALL retry with exponential backoff (3 attempts maximum)

### Requirement 6: Monitoring and Observability

**User Story:** As a data engineer, I want visibility into bookmark operations and S3 storage, so that I can monitor the health and performance of the incremental loading system.

#### Acceptance Criteria

1. WHEN bookmark operations occur THEN the system SHALL log structured messages including: operation type, table name, S3 path, success/failure status
2. WHEN bookmark state is loaded from S3 THEN the system SHALL log the last processed value and timestamp for each table
3. WHEN bookmark state is saved to S3 THEN the system SHALL log the new processed value and file size
4. WHEN S3 operations fail THEN the system SHALL log detailed error messages including S3 path, error code, and retry attempts
5. WHEN CloudWatch metrics are available THEN the system SHALL publish custom metrics for bookmark operations (read/write success/failure counts)

### Requirement 7: Performance Optimization

**User Story:** As a data engineer, I want bookmark S3 operations to be efficient and not significantly impact job performance, so that the persistence feature doesn't slow down data processing.

#### Acceptance Criteria

1. WHEN reading bookmarks THEN the system SHALL perform S3 operations in parallel for multiple tables
2. WHEN writing bookmarks THEN the system SHALL batch S3 write operations when possible
3. WHEN bookmark files are small THEN the system SHALL use S3 standard storage class for cost efficiency
4. WHEN S3 operations are performed THEN they SHALL not block data processing operations
5. WHEN multiple tables are processed THEN bookmark operations SHALL be performed asynchronously where possible

### Requirement 8: Manual Bookmark Configuration

**User Story:** As a data engineer, I want to manually configure bookmark strategies for specific tables, so that I can override the automatic detection mechanism and ensure optimal incremental loading for tables with known characteristics.

#### Acceptance Criteria

1. WHEN manual bookmark configuration is provided for a table THEN the system SHALL use the specified column instead of automatic detection
2. WHEN manual bookmark configuration contains a table name and column THEN the system SHALL query JDBC metadata to determine the column's data type
3. WHEN the manually configured column data type is determined THEN the system SHALL select the appropriate bookmark strategy (timestamp, primary_key, or hash) based on the data type
4. WHEN manual bookmark configuration is not provided for a table THEN the system SHALL fall back to the existing automatic detection mechanism
5. WHEN manual bookmark configuration is provided as a job parameter THEN the system SHALL support configuring multiple tables and their bookmark columns in a single parameter structure
6. WHEN manual bookmark configuration contains invalid table names or columns THEN the system SHALL log warnings and fall back to automatic detection for those tables
7. WHEN manual bookmark configuration specifies a column that doesn't exist in the table THEN the system SHALL log an error and fall back to automatic detection
8. WHEN manual bookmark configuration is used THEN the system SHALL log which tables are using manual vs automatic bookmark detection for observability