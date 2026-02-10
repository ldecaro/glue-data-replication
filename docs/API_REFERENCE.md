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

### MigrationPerformanceConfig

Configuration for migration performance optimizations including counting strategy and progress tracking.

```python
from glue_job.config.job_config import MigrationPerformanceConfig
```

#### MigrationPerformanceConfig Class

```python
@dataclass
class MigrationPerformanceConfig:
    """Configuration for migration performance optimizations."""
    
    # Counting strategy configuration
    counting_strategy: str = "auto"  # "immediate", "deferred", or "auto"
    counting_threshold_rows: int = 1_000_000
    
    # Progress tracking configuration
    progress_update_interval: int = 60  # seconds
    progress_batch_size: int = 100_000  # rows
    enable_progress_logging: bool = True
    
    # Metrics configuration
    enable_detailed_metrics: bool = True
    
    def validate(self) -> bool:
        """
        Validate configuration parameters.
        
        Returns:
            True if configuration is valid
            
        Raises:
            ValueError: If configuration parameters are invalid
        """
        
    def to_counting_strategy_config(self) -> CountingStrategyConfig:
        """Convert to CountingStrategyConfig for use with CountingStrategy"""
        
    def to_streaming_progress_config(self) -> StreamingProgressConfig:
        """Convert to StreamingProgressConfig for use with StreamingProgressTracker"""
```

**Configuration Parameters:**

**Counting Strategy:**
- `counting_strategy`: Strategy selection ("immediate", "deferred", or "auto")
  - `immediate`: Count rows before write (suitable for small datasets)
  - `deferred`: Count rows after write from target (optimal for large datasets)
  - `auto`: Automatically select based on dataset size
- `counting_threshold_rows`: Row count threshold for auto strategy selection (default: 1,000,000)

**Progress Tracking:**
- `progress_update_interval`: Seconds between progress updates (default: 60)
- `progress_batch_size`: Rows to process before checking for update (default: 100,000)
- `enable_progress_logging`: Enable structured logging of progress (default: True)

**Metrics:**
- `enable_detailed_metrics`: Enable detailed CloudWatch metrics publishing (default: True)

**Usage Example:**

```python
from glue_job.config.job_config import MigrationPerformanceConfig

# Create configuration for large dataset migration
perf_config = MigrationPerformanceConfig(
    counting_strategy="deferred",
    counting_threshold_rows=1_000_000,
    progress_update_interval=30,
    progress_batch_size=50_000,
    enable_progress_logging=True,
    enable_detailed_metrics=True
)

# Validate configuration
if perf_config.validate():
    # Use configuration in migration
    counting_config = perf_config.to_counting_strategy_config()
    progress_config = perf_config.to_streaming_progress_config()
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

### CountingStrategy

Determines and executes the optimal row counting strategy based on dataset characteristics and configuration to minimize redundant data reads.

```python
from glue_job.database.counting_strategy import CountingStrategy, CountingStrategyType, CountingStrategyConfig
```

#### CountingStrategyType Enum

```python
class CountingStrategyType(Enum):
    """Types of counting strategies."""
    IMMEDIATE = "immediate"  # Count before write (suitable for small datasets)
    DEFERRED = "deferred"    # Count after write from target (optimal for large datasets)
    AUTO = "auto"            # Automatically select based on dataset characteristics
```

#### CountingStrategyConfig Class

```python
@dataclass
class CountingStrategyConfig:
    """Configuration for counting strategy selection."""
    strategy_type: CountingStrategyType = CountingStrategyType.AUTO
    size_threshold_rows: int = 1_000_000  # Threshold for auto selection
    force_immediate: bool = False
    force_deferred: bool = False
    
    def validate(self):
        """
        Validate configuration.
        
        Raises:
            ValueError: If both force_immediate and force_deferred are True
        """
```

**Configuration Parameters:**
- `strategy_type`: Strategy selection mode (AUTO, IMMEDIATE, or DEFERRED)
- `size_threshold_rows`: Row count threshold for automatic strategy selection (default: 1,000,000)
- `force_immediate`: Force immediate counting regardless of dataset size (default: False)
- `force_deferred`: Force deferred counting regardless of dataset size (default: False)

#### CountingStrategy Class

```python
class CountingStrategy:
    """Manages row counting strategy selection and execution."""
    
    def __init__(self, config: CountingStrategyConfig, 
                 metrics_publisher: CloudWatchMetricsPublisher,
                 structured_logger: StructuredLogger):
        """
        Initialize counting strategy manager.
        
        Args:
            config: Counting strategy configuration
            metrics_publisher: CloudWatch metrics publisher
            structured_logger: Structured logger instance
        """
        
    def select_strategy(self, df: Optional[DataFrame], table_name: str, 
                       engine_type: str, estimated_size: Optional[int] = None) -> CountingStrategyType:
        """
        Select the optimal counting strategy based on dataset characteristics.
        
        Args:
            df: Source DataFrame (may be None for deferred-only scenarios)
            table_name: Name of the table being processed
            engine_type: Database engine type (e.g., 'oracle', 'iceberg')
            estimated_size: Estimated dataset size in rows (optional)
            
        Returns:
            Selected counting strategy type
            
        Strategy Selection Logic:
        - If force_immediate is True: Returns IMMEDIATE
        - If force_deferred is True: Returns DEFERRED
        - If engine is Iceberg: Returns DEFERRED (optimal for Iceberg metadata)
        - If estimated_size >= size_threshold_rows: Returns DEFERRED
        - Otherwise: Returns IMMEDIATE
        """
        
    def execute_immediate_count(self, df: DataFrame, table_name: str) -> int:
        """
        Execute immediate counting (before write).
        
        Args:
            df: Source DataFrame to count
            table_name: Name of the table
            
        Returns:
            Row count
            
        Note: Triggers Spark action that reads entire dataset
        """
        
    def execute_deferred_count(self, connection_manager: Any, 
                              target_config: ConnectionConfig,
                              table_name: str,
                              engine_type: str) -> int:
        """
        Execute deferred counting (after write from target).
        
        Args:
            connection_manager: Connection manager instance
            target_config: Target database configuration
            table_name: Name of the table
            engine_type: Target engine type
            
        Returns:
            Row count from target database
            
        Note: Uses lightweight metadata query on target database
        """
        
    def _count_from_jdbc_target(self, connection_manager: Any,
                               target_config: ConnectionConfig,
                               table_name: str) -> int:
        """Count rows from JDBC target using SELECT COUNT(*) query"""
        
    def _count_from_iceberg_target(self, connection_manager: Any,
                                  target_config: ConnectionConfig,
                                  table_name: str) -> int:
        """Count rows from Iceberg target using table metadata or Spark query"""
```

**Usage Example:**

```python
from glue_job.database.counting_strategy import CountingStrategy, CountingStrategyType, CountingStrategyConfig
from glue_job.monitoring.metrics import CloudWatchMetricsPublisher
from glue_job.monitoring.logging import StructuredLogger

# Configure counting strategy
config = CountingStrategyConfig(
    strategy_type=CountingStrategyType.AUTO,
    size_threshold_rows=1_000_000,
    force_immediate=False,
    force_deferred=False
)

# Initialize strategy manager
metrics_publisher = CloudWatchMetricsPublisher()
logger = StructuredLogger("migration-job")
strategy = CountingStrategy(config, metrics_publisher, logger)

# Select strategy based on dataset
selected_strategy = strategy.select_strategy(
    df=source_df,
    table_name="large_table",
    engine_type="oracle",
    estimated_size=5_000_000
)

# Execute counting based on selected strategy
if selected_strategy == CountingStrategyType.IMMEDIATE:
    row_count = strategy.execute_immediate_count(source_df, "large_table")
else:
    # Write data first, then count from target
    # ... perform write operation ...
    row_count = strategy.execute_deferred_count(
        connection_manager=conn_mgr,
        target_config=target_config,
        table_name="large_table",
        engine_type="postgresql"
    )
```

**Performance Considerations:**

For a 1TB dataset with 1 billion rows:
- **Immediate Counting**: Read (30 min) + Count (30 min) + Write (30 min) = 90 minutes
- **Deferred Counting**: Read+Write (30 min) + Target Count (30 sec) = 30.5 minutes
- **Improvement**: ~66% reduction in total processing time

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
        
    def publish_migration_progress_metrics(self, table_name: str,
                                          load_type: str,
                                          rows_processed: int,
                                          total_rows: Optional[int],
                                          rows_per_second: float,
                                          progress_percentage: float):
        """
        Publish real-time migration progress metrics.
        
        Args:
            table_name: Name of the table being migrated
            load_type: Type of load operation ('full' or 'incremental')
            rows_processed: Number of rows processed so far
            total_rows: Total rows (if known)
            rows_per_second: Current processing rate
            progress_percentage: Progress percentage (0-100)
            
        Published Metrics:
        - MigrationRowsProcessed: Number of rows processed (Count)
        - MigrationRowsPerSecond: Processing rate (Count/Second)
        - MigrationProgressPercentage: Progress percentage (Percent)
        
        Dimensions:
        - TableName: Name of the table
        - LoadType: 'full' or 'incremental'
        """
        
    def publish_counting_strategy_metrics(self, table_name: str,
                                         strategy_type: str,
                                         count_duration_seconds: float,
                                         row_count: int):
        """
        Publish metrics about counting strategy execution.
        
        Args:
            table_name: Name of the table
            strategy_type: Type of strategy used ('immediate' or 'deferred')
            count_duration_seconds: Time taken to count
            row_count: Number of rows counted
            
        Published Metrics:
        - CountingStrategyDuration: Time taken to count rows (Seconds)
        - CountingStrategyRowCount: Number of rows counted (Count)
        
        Dimensions:
        - TableName: Name of the table
        - StrategyType: 'immediate' or 'deferred'
        """
        
    def publish_migration_phase_metrics(self, table_name: str,
                                       load_type: str,
                                       phase: str,
                                       duration_seconds: float,
                                       rows_count: Optional[int] = None):
        """
        Publish metrics for individual migration phases.
        
        Args:
            table_name: Name of the table
            load_type: Type of load operation ('full' or 'incremental')
            phase: Phase name ('read', 'write', or 'count')
            duration_seconds: Phase duration
            rows_count: Number of rows (if applicable)
            
        Published Metrics:
        - MigrationPhaseDuration: Duration of the phase (Seconds)
        - MigrationPhaseRows: Number of rows in phase (Count, if applicable)
        
        Dimensions:
        - TableName: Name of the table
        - LoadType: 'full' or 'incremental'
        - Phase: 'read', 'write', or 'count'
        """
```

**New CloudWatch Metrics:**

**Migration Progress Metrics:**
- `MigrationRowsProcessed`: Real-time count of rows processed
  - Unit: Count
  - Dimensions: TableName, LoadType
  - Update Frequency: Configurable (default: every 60 seconds)

- `MigrationRowsPerSecond`: Current processing rate
  - Unit: Count/Second
  - Dimensions: TableName, LoadType
  - Use Case: Monitor throughput and identify performance bottlenecks

- `MigrationProgressPercentage`: Progress percentage (0-100)
  - Unit: Percent
  - Dimensions: TableName, LoadType
  - Use Case: Track completion status and estimate remaining time

**Counting Strategy Metrics:**
- `CountingStrategyDuration`: Time taken to count rows
  - Unit: Seconds
  - Dimensions: TableName, StrategyType
  - Use Case: Compare performance of immediate vs deferred counting

- `CountingStrategyRowCount`: Number of rows counted
  - Unit: Count
  - Dimensions: TableName, StrategyType
  - Use Case: Validate counting accuracy

**Migration Phase Metrics:**
- `MigrationPhaseDuration`: Duration of each migration phase
  - Unit: Seconds
  - Dimensions: TableName, LoadType, Phase
  - Phases: read, write, count
  - Use Case: Identify which phase is the bottleneck

- `MigrationPhaseRows`: Number of rows in each phase
  - Unit: Count
  - Dimensions: TableName, LoadType, Phase
  - Use Case: Validate data consistency across phases

**Usage Example:**

```python
from glue_job.monitoring.metrics import CloudWatchMetricsPublisher

# Initialize publisher
metrics = CloudWatchMetricsPublisher(namespace="AWS/Glue/DataReplication")

# Publish progress metrics during migration
metrics.publish_migration_progress_metrics(
    table_name="users",
    load_type="full",
    rows_processed=500_000,
    total_rows=1_000_000,
    rows_per_second=8_333.33,
    progress_percentage=50.0
)

# Publish counting strategy metrics
metrics.publish_counting_strategy_metrics(
    table_name="users",
    strategy_type="deferred",
    count_duration_seconds=0.5,
    row_count=1_000_000
)

# Publish phase metrics
metrics.publish_migration_phase_metrics(
    table_name="users",
    load_type="full",
    phase="write",
    duration_seconds=120.0,
    rows_count=1_000_000
)
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

### StreamingProgressTracker

Real-time progress tracking during data transfer operations with periodic metric emission for both full load and incremental load operations.

```python
from glue_job.monitoring.streaming_progress_tracker import StreamingProgressTracker, StreamingProgressConfig
```

#### StreamingProgressConfig Class

```python
@dataclass
class StreamingProgressConfig:
    """Configuration for streaming progress tracking."""
    update_interval_seconds: int = 60
    batch_size_rows: int = 100_000
    enable_metrics: bool = True
    enable_logging: bool = True
    
    def validate(self) -> bool:
        """Validate configuration parameters"""
```

**Configuration Parameters:**
- `update_interval_seconds`: Interval in seconds between progress updates (default: 60)
- `batch_size_rows`: Number of rows to process before checking for progress update (default: 100,000)
- `enable_metrics`: Enable CloudWatch metrics publishing (default: True)
- `enable_logging`: Enable structured logging of progress (default: True)

#### StreamingProgressTracker Class

```python
class StreamingProgressTracker:
    """Tracks real-time progress during data transfer operations."""
    
    def __init__(self, table_name: str, load_type: str, config: StreamingProgressConfig,
                 metrics_publisher: CloudWatchMetricsPublisher):
        """
        Initialize streaming progress tracker.
        
        Args:
            table_name: Name of the table being processed
            load_type: Type of load operation ('full' or 'incremental')
            config: Streaming progress configuration
            metrics_publisher: CloudWatch metrics publisher instance
        """
        
    def start_tracking(self, total_rows: Optional[int] = None):
        """
        Start progress tracking.
        
        Args:
            total_rows: Total number of rows to process (if known)
        """
        
    def update_progress(self, rows_processed: int):
        """
        Update progress with new row count.
        
        Args:
            rows_processed: Number of rows processed so far
        """
        
    def complete_tracking(self, final_row_count: int):
        """
        Complete tracking and emit final metrics.
        
        Args:
            final_row_count: Final total row count
        """
        
    def get_progress_percentage(self) -> float:
        """
        Get current progress percentage.
        
        Returns:
            Progress percentage (0-100), or 0 if total rows unknown
        """
        
    def get_rows_per_second(self) -> float:
        """
        Get current processing rate.
        
        Returns:
            Rows processed per second
        """
        
    def _should_emit_update(self) -> bool:
        """Check if progress update should be emitted based on interval"""
        
    def _emit_progress_update(self):
        """Emit progress update to CloudWatch and logs"""
        
    def _calculate_eta(self) -> Optional[float]:
        """Calculate estimated time to completion in seconds"""
```

**Usage Example:**

```python
from glue_job.monitoring.streaming_progress_tracker import StreamingProgressTracker, StreamingProgressConfig
from glue_job.monitoring.metrics import CloudWatchMetricsPublisher

# Configure streaming progress
config = StreamingProgressConfig(
    update_interval_seconds=60,
    batch_size_rows=100_000,
    enable_metrics=True,
    enable_logging=True
)

# Initialize tracker
metrics_publisher = CloudWatchMetricsPublisher()
tracker = StreamingProgressTracker(
    table_name="users",
    load_type="full",  # or "incremental"
    config=config,
    metrics_publisher=metrics_publisher
)

# Start tracking (with or without known total)
tracker.start_tracking(total_rows=1_000_000)

# Update progress during processing
for batch in process_batches():
    # Process batch...
    tracker.update_progress(rows_processed)

# Complete tracking
tracker.complete_tracking(final_row_count=1_000_000)
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

### Performance-Optimized Migration (Large Datasets)

Complete example of using the new performance optimization features for large-scale data migrations:

```python
from glue_job.config.job_config import MigrationPerformanceConfig
from glue_job.database.counting_strategy import CountingStrategy, CountingStrategyType, CountingStrategyConfig
from glue_job.monitoring.streaming_progress_tracker import StreamingProgressTracker, StreamingProgressConfig
from glue_job.monitoring.metrics import CloudWatchMetricsPublisher
from glue_job.monitoring.logging import StructuredLogger
from glue_job.database.connection_manager import JdbcConnectionManager

# Step 1: Configure performance optimizations
perf_config = MigrationPerformanceConfig(
    counting_strategy="auto",  # Automatically select based on dataset size
    counting_threshold_rows=1_000_000,
    progress_update_interval=60,
    progress_batch_size=100_000,
    enable_progress_logging=True,
    enable_detailed_metrics=True
)

# Step 2: Initialize components
logger = StructuredLogger("large-migration-job")
metrics_publisher = CloudWatchMetricsPublisher(namespace="AWS/Glue/DataReplication")

# Step 3: Set up counting strategy
counting_config = perf_config.to_counting_strategy_config()
counting_strategy = CountingStrategy(
    config=counting_config,
    metrics_publisher=metrics_publisher,
    structured_logger=logger
)

# Step 4: Set up streaming progress tracker
progress_config = perf_config.to_streaming_progress_config()
progress_tracker = StreamingProgressTracker(
    table_name="large_table",
    load_type="full",
    config=progress_config,
    metrics_publisher=metrics_publisher
)

# Step 5: Read source data (single read, no counting yet)
logger.info("Starting data read", extra={
    "table": "large_table",
    "source_engine": "oracle",
    "target_engine": "postgresql"
})

source_df = connection_manager.create_connection(
    source_config,
    "SELECT * FROM large_table"
)

# Step 6: Select counting strategy
selected_strategy = counting_strategy.select_strategy(
    df=source_df,
    table_name="large_table",
    engine_type="oracle",
    estimated_size=5_000_000  # 5 million rows estimated
)

logger.info(f"Selected counting strategy: {selected_strategy.value}", extra={
    "table": "large_table",
    "strategy": selected_strategy.value,
    "estimated_size": 5_000_000
})

# Step 7: Handle counting based on strategy
row_count = 0
if selected_strategy == CountingStrategyType.IMMEDIATE:
    # Count before write (for small datasets)
    row_count = counting_strategy.execute_immediate_count(source_df, "large_table")
    progress_tracker.start_tracking(total_rows=row_count)
else:
    # Deferred counting - start tracking without total
    progress_tracker.start_tracking(total_rows=None)

# Step 8: Write data with progress tracking
logger.info("Starting data write", extra={
    "table": "large_table",
    "strategy": selected_strategy.value
})

# Simulate batch writing with progress updates
batch_size = 100_000
rows_written = 0

for batch in source_df.toLocalIterator():
    # Write batch to target
    # ... write logic ...
    
    rows_written += batch_size
    progress_tracker.update_progress(rows_written)
    
    # Log progress every 100k rows
    if rows_written % 100_000 == 0:
        logger.info(f"Progress update", extra={
            "table": "large_table",
            "rows_processed": rows_written,
            "rows_per_second": progress_tracker.get_rows_per_second(),
            "progress_percentage": progress_tracker.get_progress_percentage()
        })

# Step 9: Execute deferred counting if needed
if selected_strategy == CountingStrategyType.DEFERRED:
    logger.info("Executing deferred count from target", extra={
        "table": "large_table"
    })
    
    row_count = counting_strategy.execute_deferred_count(
        connection_manager=connection_manager,
        target_config=target_config,
        table_name="large_table",
        engine_type="postgresql"
    )

# Step 10: Complete tracking and publish final metrics
progress_tracker.complete_tracking(final_row_count=row_count)

logger.info("Migration completed", extra={
    "table": "large_table",
    "total_rows": row_count,
    "duration_seconds": progress_tracker.get_rows_per_second(),
    "strategy_used": selected_strategy.value
})

# Step 11: Publish phase metrics
metrics_publisher.publish_migration_phase_metrics(
    table_name="large_table",
    load_type="full",
    phase="write",
    duration_seconds=120.0,
    rows_count=row_count
)
```

### Incremental Load with Progress Tracking

Example of using streaming progress tracker for incremental loads:

```python
from glue_job.monitoring.streaming_progress_tracker import StreamingProgressTracker, StreamingProgressConfig
from glue_job.monitoring.metrics import CloudWatchMetricsPublisher

# Configure progress tracking for incremental load
progress_config = StreamingProgressConfig(
    update_interval_seconds=30,  # More frequent updates for incremental
    batch_size_rows=50_000,
    enable_metrics=True,
    enable_logging=True
)

# Initialize tracker with incremental load type
metrics_publisher = CloudWatchMetricsPublisher()
tracker = StreamingProgressTracker(
    table_name="orders",
    load_type="incremental",  # Specify incremental load
    config=progress_config,
    metrics_publisher=metrics_publisher
)

# Get bookmark state
bookmark_state = bookmark_manager.get_bookmark_state("orders")
last_processed_value = bookmark_state.last_processed_value

# Read incremental data
incremental_df = connection_manager.create_connection(
    source_config,
    f"SELECT * FROM orders WHERE order_date > '{last_processed_value}'"
)

# Count delta rows (immediate for incremental)
delta_rows = incremental_df.count()

# Start tracking with known total
tracker.start_tracking(total_rows=delta_rows)

# Write incremental data with progress tracking
rows_written = 0
for batch in incremental_df.toLocalIterator():
    # Write batch
    # ... write logic ...
    
    rows_written += len(batch)
    tracker.update_progress(rows_written)

# Complete tracking
tracker.complete_tracking(final_row_count=delta_rows)

# Update bookmark
new_max_value = incremental_df.agg({"order_date": "max"}).collect()[0][0]
bookmark_manager.update_bookmark_state("orders", new_max_value, delta_rows)

# Publish incremental load metrics
metrics_publisher.publish_migration_progress_metrics(
    table_name="orders",
    load_type="incremental",
    rows_processed=delta_rows,
    total_rows=delta_rows,
    rows_per_second=tracker.get_rows_per_second(),
    progress_percentage=100.0
)
```

### Configuration from CloudFormation Parameters

Example of parsing performance configuration from CloudFormation parameters:

```python
from glue_job.config.parsers import JobConfigurationParser
from glue_job.config.job_config import MigrationPerformanceConfig

# Parse job arguments from Glue
parser = JobConfigurationParser()
job_config = parser.parse_configuration(args)

# Extract performance configuration from parameters
perf_config = MigrationPerformanceConfig(
    counting_strategy=args.get('CountingStrategy', 'auto'),
    counting_threshold_rows=int(args.get('BatchSizeThreshold', 1_000_000)),
    progress_update_interval=int(args.get('ProgressUpdateInterval', 60)),
    progress_batch_size=int(args.get('ProgressBatchSize', 100_000)),
    enable_progress_logging=args.get('EnableProgressLogging', 'true').lower() == 'true',
    enable_detailed_metrics=args.get('EnableDetailedMetrics', 'true').lower() == 'true'
)

# Validate configuration
if not perf_config.validate():
    raise ValueError("Invalid performance configuration")

# Use configuration in migration
counting_config = perf_config.to_counting_strategy_config()
progress_config = perf_config.to_streaming_progress_config()
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