# AWS Glue Data Replication - API Reference

This document provides comprehensive API documentation for all public interfaces in the modular AWS Glue Data Replication solution.

## Overview

The solution is organized into focused modules, each with well-defined public interfaces:

- **config**: Configuration management and parsing
- **database**: Database operations and connections
- **storage**: Bookmark storage and management
- **monitoring**: Logging, metrics, and progress tracking
- **network**: Error handling and retry mechanisms
- **utils**: Utility functions and S3 operations

## Configuration Modules (`glue_job.config`)

### JobConfig

Core configuration dataclasses for job parameters.

```python
from glue_job.config.job_config import JobConfig, ConnectionConfig, NetworkConfig
```

#### JobConfig Class

```python
@dataclass
class JobConfig:
    job_name: str
    source_config: ConnectionConfig
    target_config: ConnectionConfig
    network_config: Optional[NetworkConfig] = None
    processing_mode: str = "full-load"
    worker_type: str = "G.1X"
    number_of_workers: int = 2
    max_concurrent_runs: int = 1
    timeout: int = 2880  # 48 hours in minutes
    
    def validate(self) -> bool:
        """Validate configuration parameters"""
        
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation"""
```

#### ConnectionConfig Class

```python
@dataclass
class ConnectionConfig:
    engine_type: str  # "oracle", "sqlserver", "postgresql", "db2"
    host: str
    port: int
    database: str
    username: str
    password: str
    jdbc_driver_s3_path: str
    connection_string: Optional[str] = None
    
    def build_connection_string(self) -> str:
        """Build JDBC connection string"""
        
    def validate_connection_params(self) -> bool:
        """Validate connection parameters"""
```

#### NetworkConfig Class

```python
@dataclass
class NetworkConfig:
    vpc_id: Optional[str] = None
    subnet_ids: Optional[List[str]] = None
    security_group_ids: Optional[List[str]] = None
    availability_zone: Optional[str] = None
    
    def is_vpc_enabled(self) -> bool:
        """Check if VPC configuration is enabled"""
```

### DatabaseEngineManager

Database engine configuration and JDBC driver management.

```python
from glue_job.config.database_engines import DatabaseEngineManager, JdbcDriverLoader
```

#### DatabaseEngineManager Class

```python
class DatabaseEngineManager:
    def __init__(self, config_path: Optional[str] = None):
        """Initialize with optional custom config path"""
        
    def get_driver_class(self, engine_type: str) -> str:
        """Get JDBC driver class for engine type"""
        
    def get_default_port(self, engine_type: str) -> int:
        """Get default port for engine type"""
        
    def build_connection_url(self, config: ConnectionConfig) -> str:
        """Build JDBC connection URL for engine"""
        
    def get_supported_engines(self) -> List[str]:
        """Get list of supported database engines"""
        
    def validate_engine_type(self, engine_type: str) -> bool:
        """Validate if engine type is supported"""
        
    @classmethod
    def is_iceberg_engine(cls, engine_type: str) -> bool:
        """Check if engine is Iceberg type"""
        
    @classmethod
    def validate_iceberg_config(cls, config: Dict[str, Any]) -> bool:
        """Validate Iceberg-specific configuration"""
```

#### JdbcDriverLoader Class

```python
class JdbcDriverLoader:
    def __init__(self, glue_context: GlueContext):
        """Initialize with Glue context"""
        
    def load_driver(self, driver_s3_path: str) -> bool:
        """Load JDBC driver from S3 path"""
        
    def validate_driver_path(self, s3_path: str) -> bool:
        """Validate S3 driver path exists and is accessible"""
```

### IcebergConnectionHandler

Iceberg table operations through Glue Data Catalog and Spark integration.

```python
from glue_job.config.iceberg_connection_handler import IcebergConnectionHandler
```

#### IcebergConnectionHandler Class

```python
class IcebergConnectionHandler:
    def __init__(self, spark_session: SparkSession, glue_context: GlueContext):
        """Initialize with Spark session and Glue context"""
        
    def configure_iceberg_catalog(self, warehouse_location: str, catalog_id: Optional[str] = None) -> None:
        """Configure Spark for Iceberg operations with Glue Data Catalog"""
        
    def table_exists(self, database: str, table: str) -> bool:
        """Check if Iceberg table exists in Glue Data Catalog"""
        
    def create_table_if_not_exists(self, database: str, table: str, source_schema: StructType, 
                                 bookmark_column: str, warehouse_location: str) -> None:
        """Create Iceberg table with identifier-field-ids if it doesn't exist"""
        
    def read_table(self, database: str, table: str) -> DataFrame:
        """Read data from Iceberg table"""
        
    def write_table(self, dataframe: DataFrame, database: str, table: str, 
                   mode: str = "append") -> None:
        """Write data to Iceberg table with proper job options"""
```

### IcebergSchemaManager

Schema operations and data type mapping for Iceberg tables.

```python
from glue_job.config.iceberg_schema_manager import IcebergSchemaManager
```

#### IcebergSchemaManager Class

```python
class IcebergSchemaManager:
    def __init__(self):
        """Initialize Iceberg schema manager"""
        
    def create_iceberg_schema_from_jdbc(self, jdbc_metadata: ResultSetMetaData, 
                                      bookmark_column: str) -> Dict[str, Any]:
        """Convert JDBC metadata to Iceberg schema with identifier-field-ids"""
        
    def map_jdbc_to_iceberg_types(self, jdbc_type: str, precision: int, scale: int) -> str:
        """Map JDBC data types to Iceberg data types"""
        
    def add_identifier_field_ids(self, schema: Dict[str, Any], bookmark_column: str) -> Dict[str, Any]:
        """Add identifier-field-ids to schema for bookmark management"""
        
    def validate_iceberg_schema(self, schema: Dict[str, Any]) -> bool:
        """Validate Iceberg schema structure"""
        
    def get_table_schema(self, spark: SparkSession, database: str, table: str) -> Optional[StructType]:
        """Retrieve existing Iceberg table schema from Glue Data Catalog"""
```

### IcebergModels

Data models and configuration classes for Iceberg operations.

```python
from glue_job.config.iceberg_models import IcebergConfig, IcebergTableMetadata
```

#### IcebergConfig Class

```python
@dataclass
class IcebergConfig:
    database_name: str
    table_name: str
    warehouse_location: str
    catalog_id: Optional[str] = None
    format_version: str = "2"
    enable_update_catalog: bool = True
    update_behavior: str = "UPDATE_IN_DATABASE"
    
    def validate(self) -> bool:
        """Validate Iceberg configuration parameters"""
        
    def to_spark_options(self) -> Dict[str, str]:
        """Convert to Spark job options for Iceberg operations"""
```

#### IcebergTableMetadata Class

```python
@dataclass
class IcebergTableMetadata:
    database: str
    table: str
    location: str
    schema: Dict[str, Any]
    identifier_field_ids: Optional[List[int]] = None
    bookmark_column: Optional[str] = None
    partition_spec: Optional[Dict[str, Any]] = None
    
    def get_bookmark_field_id(self) -> Optional[int]:
        """Get field ID for bookmark column"""
        
    def has_identifier_field_ids(self) -> bool:
        """Check if table has identifier-field-ids configured"""
```

### JobConfigurationParser

Parse and validate job configuration from CloudFormation parameters.

```python
from glue_job.config.parsers import JobConfigurationParser, ConnectionStringBuilder
```

#### JobConfigurationParser Class

```python
class JobConfigurationParser:
    def __init__(self):
        """Initialize configuration parser"""
        
    def parse_configuration(self, args: Dict[str, Any]) -> JobConfig:
        """Parse job configuration from Glue arguments"""
        
    def validate_required_parameters(self, args: Dict[str, Any]) -> bool:
        """Validate all required parameters are present"""
        
    def parse_network_configuration(self, args: Dict[str, Any]) -> Optional[NetworkConfig]:
        """Parse network configuration parameters"""
```

## Database Modules (`glue_job.database`)

### ConnectionManager

Database connection management and validation.

```python
from glue_job.database.connection_manager import JdbcConnectionManager, GlueConnectionManager
```

#### JdbcConnectionManager Class

```python
class JdbcConnectionManager:
    def __init__(self, glue_context: GlueContext):
        """Initialize with Glue context"""
        
    def create_connection(self, config: ConnectionConfig, query: Optional[str] = None) -> DataFrame:
        """Create JDBC connection and return DataFrame"""
        
    def validate_connection(self, config: ConnectionConfig) -> bool:
        """Validate database connection"""
        
    def test_connectivity(self, config: ConnectionConfig) -> Dict[str, Any]:
        """Test connection and return detailed results"""
        
    def get_table_schema(self, config: ConnectionConfig, table_name: str) -> Dict[str, Any]:
        """Get table schema information"""
```

#### GlueConnectionManager Class

```python
class GlueConnectionManager:
    def __init__(self, glue_context: GlueContext):
        """Initialize with Glue context"""
        
    def create_glue_connection(self, connection_name: str, config: ConnectionConfig) -> bool:
        """Create Glue connection"""
        
    def validate_glue_connection(self, connection_name: str) -> bool:
        """Validate existing Glue connection"""
        
    def use_glue_connection(self, connection_name: str, query: str) -> DataFrame:
        """Use existing Glue connection for query"""
```

### SchemaValidator

Schema compatibility validation and data type mapping.

```python
from glue_job.database.schema_validator import SchemaCompatibilityValidator, DataTypeMapper
```

#### SchemaCompatibilityValidator Class

```python
class SchemaCompatibilityValidator:
    def __init__(self):
        """Initialize schema validator"""
        
    def validate_schema_compatibility(self, source_schema: Dict, target_schema: Dict) -> Dict[str, Any]:
        """Validate schema compatibility between source and target"""
        
    def identify_schema_differences(self, source_schema: Dict, target_schema: Dict) -> List[Dict]:
        """Identify specific schema differences"""
        
    def suggest_schema_fixes(self, differences: List[Dict]) -> List[str]:
        """Suggest fixes for schema incompatibilities"""
```

#### DataTypeMapper Class

```python
class DataTypeMapper:
    def __init__(self):
        """Initialize data type mapper"""
        
    def map_data_type(self, source_type: str, source_engine: str, target_engine: str) -> str:
        """Map data type from source to target engine"""
        
    def get_type_mapping_rules(self, source_engine: str, target_engine: str) -> Dict[str, str]:
        """Get all type mapping rules for engine pair"""
        
    def validate_type_conversion(self, source_type: str, target_type: str) -> bool:
        """Validate if type conversion is safe"""
```

### DataMigrator

Data migration operations for full-load and incremental processing.

```python
from glue_job.database.migration import FullLoadDataMigrator, IncrementalDataMigrator
```

#### FullLoadDataMigrator Class

```python
class FullLoadDataMigrator:
    def __init__(self, glue_context: GlueContext):
        """Initialize full load migrator"""
        
    def execute_full_load(self, source_df: DataFrame, target_config: ConnectionConfig, 
                         table_name: str) -> ProcessingMetrics:
        """Execute full load migration"""
        
    def prepare_target_table(self, target_config: ConnectionConfig, table_name: str, 
                           source_schema: Dict) -> bool:
        """Prepare target table for full load"""
        
    def validate_migration_results(self, source_count: int, target_count: int) -> bool:
        """Validate migration results"""
```

#### IncrementalDataMigrator Class

```python
class IncrementalDataMigrator:
    def __init__(self, glue_context: GlueContext):
        """Initialize incremental migrator"""
        
    def execute_incremental_load(self, source_df: DataFrame, target_config: ConnectionConfig,
                               bookmark: JobBookmarkState) -> ProcessingMetrics:
        """Execute incremental load migration"""
        
    def identify_incremental_column(self, source_df: DataFrame) -> Optional[str]:
        """Identify column for incremental processing"""
        
    def filter_incremental_data(self, source_df: DataFrame, bookmark: JobBookmarkState) -> DataFrame:
        """Filter data based on bookmark state"""
```

### IncrementalDetector

Detect and manage incremental processing columns.

```python
from glue_job.database.incremental_detector import IncrementalColumnDetector
```

#### IncrementalColumnDetector Class

```python
class IncrementalColumnDetector:
    def __init__(self):
        """Initialize incremental column detector"""
        
    def detect_incremental_columns(self, df: DataFrame) -> List[str]:
        """Detect potential incremental columns"""
        
    def validate_incremental_column(self, df: DataFrame, column_name: str) -> bool:
        """Validate if column is suitable for incremental processing"""
        
    def get_column_statistics(self, df: DataFrame, column_name: str) -> Dict[str, Any]:
        """Get statistics for incremental column"""
```

## Storage Modules (`glue_job.storage`)

### BookmarkManager

Job bookmark lifecycle management.

```python
from glue_job.storage.bookmark_manager import JobBookmarkManager, JobBookmarkState
```

#### JobBookmarkState Class

```python
@dataclass
class JobBookmarkState:
    table_name: str
    last_processed_value: Optional[str]
    processing_mode: str
    incremental_column: Optional[str]
    last_update_time: datetime
    processing_status: str
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation"""
        
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'JobBookmarkState':
        """Create from dictionary representation"""
```

#### JobBookmarkManager Class

```python
class JobBookmarkManager:
    def __init__(self, s3_config: S3BookmarkConfig):
        """Initialize with S3 bookmark configuration"""
        
    def get_bookmark_state(self, table_name: str) -> JobBookmarkState:
        """Get current bookmark state for table"""
        
    def update_bookmark_state(self, table_name: str, new_max_value: Any,
                            processed_rows: int = 0, 
                            database: Optional[str] = None,
                            engine_type: Optional[str] = None) -> None:
        """
        Update job bookmark state after successful processing with S3 persistence and Iceberg support.
        
        Args:
            table_name: Name of the table to update bookmark for
            new_max_value: New maximum value processed
            processed_rows: Number of rows processed (default: 0)
            database: Optional database name for Iceberg table validation
            engine_type: Optional engine type for Iceberg detection
        """
        
    def reset_bookmark(self, table_name: str) -> bool:
        """Reset bookmark for table"""
        
    def list_bookmarks(self) -> List[str]:
        """List all available bookmarks"""
        
    def get_iceberg_bookmark_column(self, database: str, table: str) -> Optional[str]:
        """Get bookmark column from Iceberg table identifier-field-ids"""
        
    def extract_identifier_field_ids(self, table_metadata: Dict[str, Any]) -> Optional[str]:
        """Extract identifier-field-ids from Iceberg table metadata"""
        
    def fallback_to_traditional_bookmark(self, dataframe: DataFrame) -> Optional[str]:
        """Fallback to traditional bookmark detection for Iceberg tables"""
```

### Manual Bookmark Configuration

Manual bookmark configuration components for explicit column specification.

```python
from glue_job.storage.manual_bookmark_config import ManualBookmarkConfig, BookmarkStrategyResolver
```

#### ManualBookmarkConfig Class

```python
@dataclass
class ManualBookmarkConfig:
    table_name: str
    column_name: str
    
    def __post_init__(self):
        """Validate table and column names after initialization"""
        
    @classmethod
    def from_dict(cls, data: Dict[str, str]) -> 'ManualBookmarkConfig':
        """Create ManualBookmarkConfig instance from dictionary data"""
        
    def to_dict(self) -> Dict[str, str]:
        """Convert ManualBookmarkConfig to dictionary"""
        
    def __str__(self) -> str:
        """String representation of the configuration"""
        
    def __repr__(self) -> str:
        """Detailed string representation of the configuration"""
```

#### BookmarkStrategyResolver Class

```python
class BookmarkStrategyResolver:
    def __init__(self, manual_configs: Dict[str, ManualBookmarkConfig], structured_logger=None):
        """Initialize BookmarkStrategyResolver with manual configurations"""
        
    def resolve_strategy(self, table_name: str, connection) -> Tuple[str, Optional[str], bool]:
        """
        Resolve bookmark strategy for a table using manual config or automatic detection.
        
        Returns:
            Tuple of (strategy, column_name, is_manually_configured)
        """
        
    def clear_cache(self):
        """Clear the JDBC metadata cache"""
        
    def get_cache_stats(self) -> Dict[str, int]:
        """Get cache statistics"""
        
    def _get_manual_strategy(self, table_name: str, connection) -> Optional[Tuple[str, str, str]]:
        """Get bookmark strategy from manual configuration"""
        
    def _get_automatic_strategy(self, table_name: str, connection) -> Tuple[str, Optional[str]]:
        """Get bookmark strategy using automatic detection"""
        
    def _get_column_metadata(self, connection, table_name: str, column_name: str) -> Optional[Dict[str, Any]]:
        """Query JDBC metadata to get column information with caching"""
        
    def _map_jdbc_type_to_strategy(self, jdbc_data_type: str) -> str:
        """Map JDBC data type to bookmark strategy"""
        
    def _detect_timestamp_columns(self, connection, table_name: str) -> List[str]:
        """Detect timestamp columns using JDBC metadata"""
        
    def _detect_primary_key_columns(self, connection, table_name: str) -> List[str]:
        """Detect primary key columns using JDBC metadata"""
```

### S3BookmarkStorage

S3-based bookmark storage operations.

```python
from glue_job.storage.s3_bookmark import S3BookmarkStorage, S3BookmarkConfig
```

#### S3BookmarkConfig Class

```python
@dataclass
class S3BookmarkConfig:
    bucket_name: str
    key_prefix: str
    encryption_enabled: bool = True
    
    def get_bookmark_key(self, table_name: str) -> str:
        """Get S3 key for table bookmark"""
```

#### S3BookmarkStorage Class

```python
class S3BookmarkStorage:
    def __init__(self, config: S3BookmarkConfig):
        """Initialize with S3 configuration"""
        
    def read_bookmark(self, table_name: str) -> Optional[JobBookmarkState]:
        """Read bookmark from S3"""
        
    def write_bookmark(self, bookmark: JobBookmarkState) -> bool:
        """Write bookmark to S3"""
        
    def delete_bookmark(self, table_name: str) -> bool:
        """Delete bookmark from S3"""
        
    def list_bookmarks(self) -> List[str]:
        """List all bookmarks in S3"""
```

## Monitoring Modules (`glue_job.monitoring`)

### StructuredLogger

Enhanced logging with structured context.

```python
from glue_job.monitoring.logging import StructuredLogger
```

#### StructuredLogger Class

```python
class StructuredLogger:
    def __init__(self, job_name: str, log_level: str = "INFO"):
        """Initialize structured logger"""
        
    def info(self, message: str, extra: Optional[Dict[str, Any]] = None):
        """Log info message with structured context"""
        
    def warning(self, message: str, extra: Optional[Dict[str, Any]] = None):
        """Log warning message with structured context"""
        
    def error(self, message: str, extra: Optional[Dict[str, Any]] = None):
        """Log error message with structured context"""
        
    def debug(self, message: str, extra: Optional[Dict[str, Any]] = None):
        """Log debug message with structured context"""
        
    def set_context(self, context: Dict[str, Any]):
        """Set persistent logging context"""
```

### MetricsPublisher

CloudWatch metrics publishing and performance monitoring.

```python
from glue_job.monitoring.metrics import CloudWatchMetricsPublisher, PerformanceMonitor
```

#### CloudWatchMetricsPublisher Class

```python
class CloudWatchMetricsPublisher:
    def __init__(self, namespace: str = "AWS/Glue/DataReplication"):
        """Initialize metrics publisher"""
        
    def publish_metric(self, metric_name: str, value: float, unit: str = "Count",
                      dimensions: Optional[Dict[str, str]] = None):
        """Publish single metric to CloudWatch"""
        
    def publish_batch_metrics(self, metrics: List[Dict[str, Any]]):
        """Publish multiple metrics in batch"""
        
    def create_dashboard(self, dashboard_name: str, job_name: str) -> bool:
        """Create CloudWatch dashboard for job"""
```

#### PerformanceMonitor Class

```python
class PerformanceMonitor:
    def __init__(self, metrics_publisher: CloudWatchMetricsPublisher):
        """Initialize performance monitor"""
        
    def start_timing(self, operation_name: str):
        """Start timing an operation"""
        
    def end_timing(self, operation_name: str):
        """End timing and publish metric"""
        
    def record_throughput(self, operation_name: str, count: int, duration_seconds: float):
        """Record throughput metric"""
        
    def get_performance_summary(self) -> Dict[str, Any]:
        """Get performance summary"""
```

### ProgressTracker

Progress tracking for data processing operations.

```python
from glue_job.monitoring.progress import ProcessingMetrics, FullLoadProgress, IncrementalLoadProgress
```

#### ProcessingMetrics Class

```python
@dataclass
class ProcessingMetrics:
    operation_type: str
    start_time: datetime
    end_time: Optional[datetime]
    rows_processed: int
    rows_failed: int
    throughput_rows_per_second: float
    
    def calculate_duration(self) -> float:
        """Calculate operation duration in seconds"""
        
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation"""
```

## Network Modules (`glue_job.network`)

### ErrorHandler

Network error classification and handling.

```python
from glue_job.network.error_handler import NetworkErrorHandler, DatabaseConnectionError, S3AccessError
```

#### NetworkErrorHandler Class

```python
class NetworkErrorHandler:
    def __init__(self):
        """Initialize error handler"""
        
    def classify_error(self, error: Exception) -> str:
        """Classify error type"""
        
    def is_retryable_error(self, error: Exception) -> bool:
        """Determine if error is retryable"""
        
    def get_error_context(self, error: Exception) -> Dict[str, Any]:
        """Get detailed error context"""
        
    def suggest_resolution(self, error: Exception) -> List[str]:
        """Suggest resolution steps for error"""
```

### RetryHandler

Retry mechanisms with exponential backoff.

```python
from glue_job.network.retry_handler import ConnectionRetryHandler, ErrorRecoveryManager
```

#### ConnectionRetryHandler Class

```python
class ConnectionRetryHandler:
    def __init__(self, max_retries: int = 3, base_delay: float = 1.0, max_delay: float = 60.0):
        """Initialize retry handler"""
        
    def retry_with_backoff(self, func: Callable, *args, **kwargs) -> Any:
        """Execute function with exponential backoff retry"""
        
    def calculate_delay(self, attempt: int) -> float:
        """Calculate delay for retry attempt"""
        
    def should_retry(self, error: Exception, attempt: int) -> bool:
        """Determine if operation should be retried"""
```

## Utility Modules (`glue_job.utils`)

### S3Utilities

S3 operations and path utilities.

```python
from glue_job.utils.s3_utils import S3PathUtilities, EnhancedS3ParallelOperations
```

#### S3PathUtilities Class

```python
class S3PathUtilities:
    @staticmethod
    def validate_s3_path(s3_path: str) -> bool:
        """Validate S3 path format"""
        
    @staticmethod
    def parse_s3_path(s3_path: str) -> Tuple[str, str]:
        """Parse S3 path into bucket and key"""
        
    @staticmethod
    def build_s3_path(bucket: str, key: str) -> str:
        """Build S3 path from bucket and key"""
        
    @staticmethod
    def get_s3_object_info(s3_path: str) -> Dict[str, Any]:
        """Get S3 object information"""
```

#### EnhancedS3ParallelOperations Class

```python
class EnhancedS3ParallelOperations:
    def __init__(self, max_workers: int = 10):
        """Initialize parallel S3 operations"""
        
    def parallel_upload(self, files: List[str], bucket: str, key_prefix: str = "") -> Dict[str, Any]:
        """Upload multiple files in parallel"""
        
    def parallel_download(self, s3_paths: List[str], local_dir: str) -> Dict[str, Any]:
        """Download multiple files in parallel"""
        
    def batch_copy(self, source_paths: List[str], target_bucket: str, target_prefix: str) -> Dict[str, Any]:
        """Copy multiple S3 objects in parallel"""
```

## Usage Examples

### Basic Job Configuration

```python
from glue_job.config.job_config import JobConfig, ConnectionConfig
from glue_job.config.parsers import JobConfigurationParser

# Parse configuration from Glue arguments
parser = JobConfigurationParser()
config = parser.parse_configuration(args)

# Or create manually
source_config = ConnectionConfig(
    engine_type="sqlserver",
    host="source-db.example.com",
    port=1433,
    database="SourceDB",
    username="user",
    password="password",
    jdbc_driver_s3_path="s3://bucket/drivers/sqlserver.jar"
)

job_config = JobConfig(
    job_name="my-replication-job",
    source_config=source_config,
    target_config=target_config
)
```

### Database Operations

```python
from glue_job.database.connection_manager import JdbcConnectionManager
from glue_job.database.migration import FullLoadDataMigrator

# Create connection and read data
conn_manager = JdbcConnectionManager(glue_context)
source_df = conn_manager.create_connection(source_config, "SELECT * FROM users")

# Execute migration
migrator = FullLoadDataMigrator(glue_context)
metrics = migrator.execute_full_load(source_df, target_config, "users")
```

### Bookmark Management

```python
from glue_job.storage.bookmark_manager import JobBookmarkManager
from glue_job.storage.s3_bookmark import S3BookmarkConfig

# Configure S3 bookmark storage
s3_config = S3BookmarkConfig(
    bucket_name="my-bookmarks-bucket",
    key_prefix="glue-job-bookmarks/"
)

# Manage bookmarks
bookmark_manager = JobBookmarkManager(glue_context, job_name)
current_state = bookmark_manager.get_bookmark_state("users")

# Update bookmark with new maximum value after processing
max_id = 12345  # Maximum ID processed
rows_processed = 100
bookmark_manager.update_bookmark_state("users", max_id, rows_processed)
```

### Iceberg Operations

```python
from glue_job.config.iceberg_connection_handler import IcebergConnectionHandler
from glue_job.config.iceberg_models import IcebergConfig
from glue_job.config.iceberg_schema_manager import IcebergSchemaManager

# Configure Iceberg connection
iceberg_config = IcebergConfig(
    database_name="analytics_db",
    table_name="customer_data",
    warehouse_location="s3://my-datalake/warehouse/",
    catalog_id="123456789012"
)

# Initialize Iceberg handler
iceberg_handler = IcebergConnectionHandler(spark_session, glue_context)
iceberg_handler.configure_iceberg_catalog(
    warehouse_location=iceberg_config.warehouse_location,
    catalog_id=iceberg_config.catalog_id
)

# Create table if it doesn't exist
if not iceberg_handler.table_exists(iceberg_config.database_name, iceberg_config.table_name):
    schema_manager = IcebergSchemaManager()
    iceberg_schema = schema_manager.create_iceberg_schema_from_jdbc(
        jdbc_metadata, bookmark_column="created_timestamp"
    )
    iceberg_handler.create_table_if_not_exists(
        database=iceberg_config.database_name,
        table=iceberg_config.table_name,
        source_schema=source_df.schema,
        bookmark_column="created_timestamp",
        warehouse_location=iceberg_config.warehouse_location
    )

# Read from Iceberg table
iceberg_df = iceberg_handler.read_table(
    iceberg_config.database_name, 
    iceberg_config.table_name
)

# Write to Iceberg table
iceberg_handler.write_table(
    dataframe=processed_df,
    database=iceberg_config.database_name,
    table=iceberg_config.table_name,
    mode="append"
)
```

### Monitoring and Logging

```python
from glue_job.monitoring.logging import StructuredLogger
from glue_job.monitoring.metrics import CloudWatchMetricsPublisher

# Structured logging
logger = StructuredLogger("my-job")
logger.info("Processing started", extra={"table": "users", "mode": "full-load"})

# Metrics publishing
metrics = CloudWatchMetricsPublisher()
metrics.publish_metric("RowsProcessed", 1000, "Count", {"JobName": "my-job"})
```

## Error Handling

All modules include comprehensive error handling with custom exception types:

- `DatabaseConnectionError`: Database connectivity issues
- `S3AccessError`: S3 access and permission issues
- `SchemaValidationError`: Schema compatibility problems
- `BookmarkError`: Bookmark storage and retrieval issues
- `ConfigurationError`: Configuration validation failures

## Deployment

The enhanced deployment script provides automated deployment with S3 asset management:

```bash
# Complete deployment with automatic uploads
./deploy.sh -s stack-name -b bucket-name -p parameters-file.json

# Deploy without uploading (assets already in S3)
./deploy.sh -s stack-name -b bucket-name -p parameters-file.json --skip-upload

# Validate template only
./deploy.sh -s stack-name -b bucket-name -p parameters-file.json --validate-only
```

The script automatically:
- Creates S3 bucket if needed
- Uploads CloudFormation template and modular Glue job structure
- Updates parameter files with correct S3 paths
- Deploys using S3-hosted template

## Testing

Each module includes comprehensive unit tests. Run tests using:

```bash
# Run all tests
python -m pytest tests/

# Run specific module tests
python -m pytest tests/unit/test_config/
python -m pytest tests/unit/test_database/
python -m pytest tests/integration/
```

## Version Compatibility

- **Python**: 3.7+
- **PySpark**: 3.1+
- **AWS Glue**: 3.0+
- **Boto3**: 1.20+

## Support

For API questions or issues:
1. Check the module docstrings for detailed parameter information
2. Review the test files for usage examples
3. Consult the main documentation in the `docs/` directory