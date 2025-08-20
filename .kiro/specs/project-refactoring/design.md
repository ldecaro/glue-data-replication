# Design Document

## Overview

This design document outlines the refactoring approach for the AWS Glue Data Replication project. The current monolithic `glue_data_replication.py` script (~8000+ lines) will be decomposed into focused, maintainable modules. The project structure will be reorganized to separate infrastructure concerns from application logic, and comprehensive cleanup will remove redundant files while preserving essential information.

## Architecture

### Current State Analysis

The current `scripts/glue_data_replication.py` contains approximately 30+ classes handling diverse responsibilities:
- S3 bookmark storage and configuration
- Database connection management
- Error handling and recovery
- Performance monitoring and metrics
- Data migration (full-load and incremental)
- Schema validation and type mapping
- Network connectivity management

### Target Architecture

The refactored architecture will follow a modular design with clear separation of concerns:

```
src/
├── glue_job/
│   ├── __init__.py
│   ├── main.py                    # Entry point (< 200 lines)
│   ├── config/
│   │   ├── __init__.py
│   │   ├── job_config.py          # JobConfig, NetworkConfig, ConnectionConfig
│   │   └── database_engines.py    # DatabaseEngineManager, JdbcDriverLoader
│   ├── storage/
│   │   ├── __init__.py
│   │   ├── s3_bookmark.py         # S3BookmarkStorage, S3BookmarkConfig
│   │   └── bookmark_manager.py    # JobBookmarkManager
│   ├── database/
│   │   ├── __init__.py
│   │   ├── connection_manager.py  # JdbcConnectionManager, GlueConnectionManager
│   │   ├── schema_validator.py    # SchemaCompatibilityValidator, DataTypeMapper
│   │   └── migration.py           # FullLoadDataMigrator, IncrementalDataMigrator
│   ├── monitoring/
│   │   ├── __init__.py
│   │   ├── metrics.py             # CloudWatchMetricsPublisher, PerformanceMonitor
│   │   ├── logging.py             # StructuredLogger
│   │   └── progress.py            # ProcessingMetrics, Progress tracking classes
│   ├── network/
│   │   ├── __init__.py
│   │   ├── error_handler.py       # NetworkErrorHandler, custom exceptions
│   │   └── retry_handler.py       # ConnectionRetryHandler, ErrorRecoveryManager
│   └── utils/
│       ├── __init__.py
│       ├── s3_utils.py            # S3PathUtilities, EnhancedS3ParallelOperations
│       └── incremental_detector.py # IncrementalColumnDetector
```

## Components and Interfaces

### 1. Main Entry Point (`src/glue_job/main.py`)

**Responsibility**: Orchestrate the data replication process
**Key Functions**:
- Parse command-line arguments
- Initialize Glue context and Spark session
- Coordinate migration workflow
- Handle top-level error scenarios

**Interface**:
```python
def main() -> None:
    """Main entry point for Glue job execution"""

def setup_glue_context(args: Dict[str, Any]) -> Tuple[GlueContext, Job]:
    """Initialize Glue context and job"""

def execute_migration_workflow(config: JobConfig, glue_context: GlueContext) -> None:
    """Execute the complete migration workflow"""
```

### 2. Configuration Module (`src/glue_job/config/`)

**Responsibility**: Handle all configuration parsing and validation
**Key Classes**:
- `JobConfigurationParser`: Parse CloudFormation parameters
- `DatabaseEngineManager`: Manage database engine configurations
- `JdbcDriverLoader`: Handle JDBC driver loading

**Interface**:
```python
class JobConfigurationParser:
    def parse_configuration(self, args: Dict[str, Any]) -> JobConfig:
        """Parse job configuration from arguments"""

class DatabaseEngineManager:
    def get_driver_class(self, engine_type: str) -> str:
        """Get JDBC driver class for engine"""
    
    def build_connection_url(self, config: ConnectionConfig) -> str:
        """Build JDBC connection URL"""
```

### 3. Storage Module (`src/glue_job/storage/`)

**Responsibility**: Handle bookmark persistence and S3 operations
**Key Classes**:
- `S3BookmarkStorage`: S3 bookmark operations
- `JobBookmarkManager`: Bookmark lifecycle management

**Interface**:
```python
class S3BookmarkStorage:
    def read_bookmark(self, table_name: str) -> Optional[JobBookmarkState]:
        """Read bookmark from S3"""
    
    def write_bookmark(self, bookmark: JobBookmarkState) -> bool:
        """Write bookmark to S3"""

class JobBookmarkManager:
    def get_bookmark_state(self, table_name: str) -> JobBookmarkState:
        """Get current bookmark state"""
    
    def update_bookmark_state(self, table_name: str, state: JobBookmarkState) -> None:
        """Update bookmark state"""
```

### 4. Database Module (`src/glue_job/database/`)

**Responsibility**: Handle database connections, schema validation, and data migration
**Key Classes**:
- `JdbcConnectionManager`: JDBC connection management
- `GlueConnectionManager`: Glue connection management
- `SchemaCompatibilityValidator`: Schema validation
- `DataMigrator`: Data migration operations

**Interface**:
```python
class JdbcConnectionManager:
    def create_connection(self, config: ConnectionConfig) -> DataFrame:
        """Create JDBC connection and return DataFrame"""
    
    def validate_connection(self, config: ConnectionConfig) -> bool:
        """Validate database connection"""

class DataMigrator:
    def execute_full_load(self, source_df: DataFrame, target_config: ConnectionConfig) -> ProcessingMetrics:
        """Execute full-load migration"""
    
    def execute_incremental_load(self, source_df: DataFrame, target_config: ConnectionConfig, bookmark: JobBookmarkState) -> ProcessingMetrics:
        """Execute incremental migration"""
```

### 5. Monitoring Module (`src/glue_job/monitoring/`)

**Responsibility**: Handle logging, metrics, and performance monitoring
**Key Classes**:
- `StructuredLogger`: Enhanced logging with context
- `CloudWatchMetricsPublisher`: CloudWatch metrics publishing
- `PerformanceMonitor`: Performance tracking

### 6. Network Module (`src/glue_job/network/`)

**Responsibility**: Handle network-related error handling and retry logic
**Key Classes**:
- `NetworkErrorHandler`: Network-specific error handling
- `ConnectionRetryHandler`: Retry logic with exponential backoff

## Data Models

### Core Configuration Classes
```python
@dataclass
class JobConfig:
    job_name: str
    source_config: ConnectionConfig
    target_config: ConnectionConfig
    network_config: Optional[NetworkConfig]
    # ... other fields

@dataclass
class ConnectionConfig:
    engine_type: str
    host: str
    port: int
    database: str
    # ... other fields

@dataclass
class JobBookmarkState:
    table_name: str
    last_processed_value: Optional[str]
    processing_mode: str
    # ... other fields
```

## Error Handling

### Error Classification Strategy
- **Retryable Errors**: Network timeouts, temporary connection issues
- **Non-Retryable Errors**: Authentication failures, schema mismatches
- **Recovery Strategies**: Exponential backoff, fallback mechanisms

### Error Recovery Patterns
1. **Connection Failures**: Retry with exponential backoff
2. **S3 Access Issues**: Fallback to in-memory bookmarks
3. **Schema Validation Failures**: Detailed error reporting and job termination
4. **Network Connectivity**: VPC endpoint validation and ENI troubleshooting

## Testing Strategy

### Unit Testing Structure
```
tests/
├── unit/
│   ├── test_config/
│   │   ├── test_job_config.py
│   │   └── test_database_engines.py
│   ├── test_storage/
│   │   ├── test_s3_bookmark.py
│   │   └── test_bookmark_manager.py
│   ├── test_database/
│   │   ├── test_connection_manager.py
│   │   ├── test_schema_validator.py
│   │   └── test_migration.py
│   ├── test_monitoring/
│   │   ├── test_metrics.py
│   │   └── test_logging.py
│   └── test_network/
│       ├── test_error_handler.py
│       └── test_retry_handler.py
├── integration/
│   ├── test_s3_bookmark_integration.py
│   ├── test_database_integration.py
│   └── test_end_to_end.py
└── performance/
    ├── test_concurrent_operations.py
    └── test_large_dataset_migration.py
```

### Test Coverage Requirements
- **Unit Tests**: 90%+ coverage for all modules
- **Integration Tests**: End-to-end workflow validation
- **Performance Tests**: Concurrent operations and large dataset handling

## Project Structure Reorganization

### New Directory Structure
```
aws-glue-data-replication/
├── src/
│   └── glue_job/                  # Glue job Python modules
├── infrastructure/
│   ├── cloudformation/            # CloudFormation templates
│   ├── scripts/                   # Infrastructure deployment scripts
│   └── iam/                       # IAM policies
├── tests/                         # All test files
├── docs/                          # Documentation
├── examples/                      # Configuration examples
└── config/                        # Static configuration files
```

### Migration Strategy
1. **Phase 1**: Extract modules from monolithic script
2. **Phase 2**: Update import statements and dependencies
3. **Phase 3**: Reorganize directory structure
4. **Phase 4**: Update deployment scripts and documentation
5. **Phase 5**: Validate and test complete system

## File Cleanup Strategy

### Files to Remove
- `TASK_*.md` files (after extracting important information)
- Redundant `verify_task_*.py` files
- Duplicate test files
- Unused configuration files

### Information Preservation
Important information from TASK files will be integrated into:
- `docs/IMPLEMENTATION_HISTORY.md`: Historical implementation details
- `docs/TESTING_GUIDE.md`: Comprehensive testing documentation
- `docs/ARCHITECTURE.md`: Technical architecture documentation

## Deployment Integration

### CloudFormation Template Updates
- Update `GlueJobScriptS3Path` to point to new main script location
- Ensure all S3 paths reference the new structure
- Validate template functionality with new module structure

### Deployment Script Updates
- Update `deploy.sh` to handle new directory structure
- Modify upload scripts to deploy from new locations
- Update documentation to reflect new deployment process

### Backward Compatibility
- Maintain existing parameter interfaces
- Preserve all CloudFormation stack outputs
- Ensure existing deployment workflows continue to function