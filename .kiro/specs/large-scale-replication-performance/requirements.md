# Requirements Document

## Introduction

This feature enhances the AWS Glue Data Replication solution to efficiently handle large-scale data migrations (1TB+) by optimizing progress tracking, eliminating redundant data reads, and providing real-time monitoring capabilities. The current implementation reads source data twice (once for counting, once for writing), which doubles I/O operations and processing time for large datasets.

## Glossary

- **Migration System**: The AWS Glue-based data replication solution that transfers data between source and target databases
- **Progress Tracker**: Component responsible for tracking and reporting migration progress metrics
- **Source Database**: The database from which data is being read during migration
- **Target Database**: The database to which data is being written during migration
- **DataFrame**: Apache Spark distributed data structure representing the dataset being migrated
- **CloudWatch Metrics**: AWS monitoring service metrics for tracking job performance
- **Row Count Operation**: Spark action that triggers full dataset evaluation to count total rows
- **Streaming Progress**: Real-time progress updates emitted during data transfer operations
- **Batch Write**: Writing data in chunks rather than as a single operation
- **Deferred Counting**: Strategy of counting rows after write completion rather than before

## Requirements

### Requirement 1

**User Story:** As a data engineer migrating large datasets (1TB+), I want the migration to avoid reading source data multiple times, so that I can minimize processing time and resource costs.

#### Acceptance Criteria

1. WHEN THE Migration System performs a full load migration, THE Migration System SHALL read data from the Source Database exactly once
2. THE Migration System SHALL NOT execute a Row Count Operation on the source DataFrame before writing to the Target Database
3. WHEN THE Migration System completes writing data to the Target Database, THE Migration System SHALL obtain the total row count from the Target Database
4. THE Migration System SHALL update the Progress Tracker with the final row count after the write operation completes
5. THE Migration System SHALL log a warning message when deferred counting is used for datasets where immediate row counts are not available

### Requirement 2

**User Story:** As a data engineer monitoring active migrations, I want to see real-time progress updates during data transfer, so that I can track job status and estimate completion time.

#### Acceptance Criteria

1. WHEN THE Migration System writes data to the Target Database in full load or incremental load mode, THE Migration System SHALL emit progress updates at configurable intervals
2. THE Progress Tracker SHALL publish CloudWatch Metrics for rows processed at least every 60 seconds during active transfers for both full load and incremental load operations
3. THE Progress Tracker SHALL include timestamp, table name, rows processed, load type (full or incremental), and estimated completion time in each progress update
4. WHEN THE Migration System processes data in batches, THE Migration System SHALL update the Progress Tracker after each batch completes
5. THE Migration System SHALL log structured progress messages that include percentage completion when total rows are known for both full load and incremental load operations

### Requirement 3

**User Story:** As a DevOps engineer, I want comprehensive CloudWatch metrics for migration jobs, so that I can monitor performance, set up alarms, and troubleshoot issues.

#### Acceptance Criteria

1. THE Migration System SHALL publish a CloudWatch Metric for total rows processed per table with table name and load type (full or incremental) as dimensions
2. THE Migration System SHALL publish a CloudWatch Metric for data transfer rate in rows per second for both full load and incremental load operations
3. THE Migration System SHALL publish a CloudWatch Metric for migration duration in seconds per table for both full load and incremental load operations
4. THE Migration System SHALL publish a CloudWatch Metric for migration status (success, failed, in_progress) per table with load type as a dimension
5. THE Migration System SHALL publish CloudWatch Metrics to a configurable namespace with job name, table name, and load type dimensions

### Requirement 4

**User Story:** As a data engineer, I want the migration system to handle both small and large datasets efficiently, so that I can use the same solution regardless of data volume.

#### Acceptance Criteria

1. WHERE the source is a JDBC database (Oracle, SQL Server, PostgreSQL, DB2), THE Migration System SHALL use immediate row counting via SQL `SELECT COUNT(*)` regardless of dataset size
2. WHERE the source is an Iceberg table, THE Migration System SHALL use immediate row counting via Spark SQL `SELECT COUNT(*)` which leverages Iceberg metadata statistics
3. THE Migration System SHALL provide a configuration parameter to override the automatic counting strategy selection
4. THE Migration System SHALL log the selected counting strategy (immediate or deferred) at the start of each table migration
5. WHEN THE Migration System cannot execute the COUNT query (e.g., connection issues), THE Migration System SHALL fall back to deferred counting strategy

### Requirement 5

**User Story:** As a data engineer, I want detailed logging during migration operations, so that I can understand what the job is doing and troubleshoot any issues.

#### Acceptance Criteria

1. THE Migration System SHALL log the start of each table migration with source engine, target engine, and table name
2. THE Migration System SHALL log progress updates every 100,000 rows processed during write operations
3. THE Migration System SHALL log the completion of each table migration with total rows, duration, and transfer rate
4. WHEN THE Migration System encounters an error during migration, THE Migration System SHALL log the error with table name, operation type, and full error details
5. THE Migration System SHALL use structured logging format with consistent field names for all migration-related log entries

### Requirement 6

**User Story:** As a data engineer working with Iceberg tables, I want the same performance optimizations and monitoring capabilities, so that I can efficiently migrate data to and from data lakes.

#### Acceptance Criteria

1. WHEN THE Source Database is an Iceberg table, THE Migration System SHALL apply deferred counting strategy
2. WHEN THE Target Database is an Iceberg table, THE Migration System SHALL retrieve row counts using Iceberg metadata when available
3. THE Migration System SHALL publish the same CloudWatch Metrics for Iceberg migrations as for JDBC migrations
4. THE Migration System SHALL log Iceberg-specific operations with engine type indicators in structured logs
5. WHEN THE Migration System writes to an Iceberg Target Database, THE Migration System SHALL emit progress updates using Spark write callbacks

### Requirement 7

**User Story:** As a data engineer, I want to configure progress tracking behavior, so that I can optimize for my specific use case and infrastructure.

#### Acceptance Criteria

1. THE Migration System SHALL accept a configuration parameter for progress update interval in seconds with a default value of 60 seconds
2. THE Migration System SHALL accept a configuration parameter for batch size threshold with a default value of 1000000 rows
3. THE Migration System SHALL accept a configuration parameter to enable or disable CloudWatch metrics publishing with a default value of enabled
4. THE Migration System SHALL accept a configuration parameter for counting strategy (immediate, deferred, auto) with a default value of auto
5. THE Migration System SHALL validate all configuration parameters at job initialization and log warnings for invalid values


### Requirement 8

**User Story:** As a data engineer performing incremental loads, I want the same performance optimizations and monitoring capabilities as full loads, so that I can efficiently track and monitor incremental data synchronization.

#### Acceptance Criteria

1. WHEN THE Migration System performs an incremental load migration, THE Migration System SHALL use the StreamingProgressTracker to emit real-time progress updates
2. THE Migration System SHALL publish CloudWatch Metrics for incremental load operations with the same granularity as full load operations
3. THE Migration System SHALL log structured progress messages for incremental loads with delta rows processed and incremental column information
4. WHEN THE Migration System completes an incremental load, THE Migration System SHALL publish final metrics including delta rows, processing rate, and bookmark update status
5. THE Migration System SHALL include load type dimension (full or incremental) in all CloudWatch metrics to enable separate monitoring and alerting
