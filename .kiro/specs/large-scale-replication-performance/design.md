# Design Document

## Overview

This design enhances the AWS Glue Data Replication solution to efficiently handle large-scale data migrations (1TB+) by implementing optimized row counting via SQL queries, real-time progress tracking, and comprehensive monitoring capabilities. The solution uses lightweight SQL `SELECT COUNT(*)` queries instead of Spark DataFrame counting, providing full progress visibility with no performance penalty.

### Key Design Goals

1. **Optimized Row Counting**: Use SQL `SELECT COUNT(*)` queries that leverage database/Iceberg metadata instead of scanning data
2. **Real-Time Progress Visibility**: Provide continuous progress updates during data transfer operations with accurate completion percentages
3. **Adaptive Strategy Selection**: Automatically choose immediate counting for all source types (JDBC and Iceberg)
4. **Comprehensive Monitoring**: Emit CloudWatch metrics for all migration operations with table-level granularity
5. **Backward Compatibility**: Maintain existing API contracts while adding new capabilities

## Counting Strategy Design

### Why SQL COUNT(*) Instead of DataFrame.count()

The previous implementation used `df.count()` which triggers a full Spark action that reads all source data just to count rows. This caused:
- **Double data reading**: Once for counting, once for writing
- **Performance penalty**: Up to 50% additional processing time for large datasets

The new approach uses SQL `SELECT COUNT(*)` queries which are fast because:
- **JDBC databases**: Execute count server-side using indexes and statistics
- **Iceberg tables**: Read from manifest metadata, not data files

### Strategy Selection Logic

```
┌─────────────────────────────────────────────────────────────────┐
│                    Counting Strategy Selection                   │
└─────────────────────────────────────────────────────────────────┘

                    ┌──────────────────┐
                    │ Select Strategy  │
                    │ (auto/immediate/ │
                    │  deferred)       │
                    └────────┬─────────┘
                             │
              ┌──────────────┼──────────────┐
              │              │              │
              ▼              ▼              ▼
     ┌────────────┐  ┌────────────┐  ┌────────────┐
     │   AUTO     │  │ IMMEDIATE  │  │  DEFERRED  │
     │ (default)  │  │  (forced)  │  │  (forced)  │
     └─────┬──────┘  └─────┬──────┘  └─────┬──────┘
           │               │               │
           ▼               │               │
     ┌────────────┐        │               │
     │ Use SQL    │        │               │
     │ COUNT(*)   │◄───────┘               │
     │ for ALL    │                        │
     │ sources    │                        │
     └─────┬──────┘                        │
           │                               │
           │    ┌──────────────────────────┘
           │    │
           ▼    ▼
     ┌────────────────────────────────────┐
     │ Progress tracking with total rows  │
     │ known from start (immediate) or    │
     │ after write completes (deferred)   │
     └────────────────────────────────────┘
```

### Counting Methods by Source Type

| Source Type | Counting Method | Performance | Progress Visibility |
|-------------|-----------------|-------------|---------------------|
| JDBC (Oracle, SQL Server, PostgreSQL, DB2) | `SELECT COUNT(*) FROM schema.table` | Fast (database-optimized) | Full from start |
| Iceberg | `SELECT COUNT(*) FROM catalog.db.table` | Fast (metadata-based) | Full from start |
| Fallback | Deferred counting from target | Fast | After write completes |

## Architecture

### High-Level Flow

#### Full Load Migration Flow

```
┌─────────────────────────────────────────────────────────────────┐
│                    Full Load Migration Flow                      │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
                    ┌──────────────────┐
                    │ Initialize       │
                    │ Progress Tracker │
                    └────────┬─────────┘
                             │
                             ▼
                    ┌──────────────────┐
                    │ Execute SQL      │
                    │ COUNT(*) Query   │
                    │ (Fast metadata)  │
                    └────────┬─────────┘
                             │
                             ▼
                    ┌──────────────────┐
                    │ Start Progress   │
                    │ Tracker with     │
                    │ Total Rows Known │
                    └────────┬─────────┘
                             │
                             ▼
                    ┌──────────────────┐
                    │ Read Source &    │
                    │ Write Target     │
                    │ (Single Pass)    │
                    │ with Progress    │
                    │ Updates          │
                    └────────┬─────────┘
                             │
                             ▼
                    ┌──────────────────┐
                    │ Publish Final    │
                    │ Metrics          │
                    └──────────────────┘
```

#### Incremental Load Migration Flow

```
┌─────────────────────────────────────────────────────────────────┐
│                 Incremental Load Migration Flow                  │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
                    ┌──────────────────┐
                    │ Initialize       │
                    │ Progress Tracker │
                    │ (Incremental)    │
                    └────────┬─────────┘
                             │
                             ▼
                    ┌──────────────────┐
                    │ Get Bookmark     │
                    │ State            │
                    └────────┬─────────┘
                             │
                             ▼
                    ┌──────────────────┐
                    │ Read Incremental │
                    │ Data (Filtered)  │
                    │ - Single Read    │
                    │ - WHERE clause   │
                    └────────┬─────────┘
                             │
                             ▼
                    ┌──────────────────┐
                    │ Count Delta Rows │
                    │ (Immediate)      │
                    └────────┬─────────┘
                             │
                             ▼
                    ┌──────────────────┐
                    │ Write to Target  │
                    │ with Progress    │
                    │ Tracking         │
                    │ (Append Mode)    │
                    └────────┬─────────┘
                             │
                             ▼
                    ┌──────────────────┐
                    │ Update Bookmark  │
                    │ State            │
                    └────────┬─────────┘
                             │
                             ▼
                    ┌──────────────────┐
                    │ Publish Final    │
                    │ Metrics          │
                    │ (with load_type) │
                    └──────────────────┘
```

### Component Interaction

```
┌──────────────────────────────────────────────────────────────────┐
│                     Component Architecture                        │
└──────────────────────────────────────────────────────────────────┘

┌─────────────────────┐
│ FullLoadDataMigrator│
│                     │
│ - perform_full_load │◄────┐
│ - _read_source      │     │
│ - _write_target     │     │
└──────────┬──────────┘     │
           │                │
           │ uses           │
           ▼                │
┌─────────────────────┐     │
│ ProgressTracker     │     │
│ (NEW)               │     │
│                     │     │
│ - start_tracking    │     │
│ - update_progress   │     │
│ - complete_tracking │     │
│ - emit_metrics      │     │
└──────────┬──────────┘     │
           │                │
           │ publishes to   │
           ▼                │
┌─────────────────────┐     │
│ CloudWatch          │     │
│ MetricsPublisher    │     │
│ (ENHANCED)          │     │
│                     │     │
│ - put_metric        │     │
│ - flush_metrics     │     │
└─────────────────────┘     │
                            │
┌─────────────────────┐     │
│ CountingStrategy    │     │
│ (NEW)               │     │
│                     │     │
│ - select_strategy   │─────┘
│ - immediate_count   │
│ - deferred_count    │
└─────────────────────┘
```

## Components and Interfaces

### 1. CountingStrategy (New Component)

**Purpose**: Executes optimized row counting using SQL queries instead of DataFrame operations, with fallback to deferred counting if needed.

**Interface**:

```python
from enum import Enum
from typing import Optional
from dataclasses import dataclass

class CountingStrategyType(Enum):
    """Types of counting strategies."""
    IMMEDIATE = "immediate"  # Count via SQL before write (default for auto)
    DEFERRED = "deferred"    # Count from target after write (fallback)
    AUTO = "auto"            # Automatically use immediate with SQL COUNT(*)

@dataclass
class CountingStrategyConfig:
    """Configuration for counting strategy selection."""
    strategy_type: CountingStrategyType = CountingStrategyType.AUTO
    force_immediate: bool = False
    force_deferred: bool = False
    
    def validate(self):
        """Validate configuration."""
        if self.force_immediate and self.force_deferred:
            raise ValueError("Cannot force both immediate and deferred strategies")

class CountingStrategy:
    """Manages row counting strategy selection and execution."""
    
    def __init__(self, config: CountingStrategyConfig):
        self.config = config
        self.structured_logger = StructuredLogger("CountingStrategy")
    
    def select_strategy(self, table_name: str, 
                       engine_type: str) -> CountingStrategyType:
        """
        Select the counting strategy.
        
        For AUTO mode, always returns IMMEDIATE since SQL COUNT(*) is fast
        for both JDBC and Iceberg sources.
        
        Args:
            table_name: Name of the table
            engine_type: Database engine type
            
        Returns:
            Selected counting strategy type
        """
        pass
    
    def execute_immediate_count_sql(self, source_config: ConnectionConfig,
                                   table_name: str,
                                   connection_manager: UnifiedConnectionManager) -> int:
        """
        Execute immediate counting via SQL SELECT COUNT(*).
        
        For JDBC: Executes SELECT COUNT(*) FROM schema.table
        For Iceberg: Executes SELECT COUNT(*) FROM catalog.db.table (uses metadata)
        
        Args:
            source_config: Source database connection configuration
            table_name: Name of the table
            connection_manager: Connection manager for database access
            
        Returns:
            Number of rows in the source table
        """
        pass
    
    def execute_deferred_count(self, target_config: ConnectionConfig, 
                              table_name: str, 
                              connection_manager: UnifiedConnectionManager) -> int:
        """Execute deferred counting from target (fallback)."""
        pass
```

### 2. StreamingProgressTracker (New Component)

**Purpose**: Tracks and reports real-time progress during data transfer operations with periodic metric emission for both full load and incremental load operations.

**Interface**:

```python
from dataclasses import dataclass
from typing import Optional, Callable
import time

@dataclass
class StreamingProgressConfig:
    """Configuration for streaming progress tracking."""
    update_interval_seconds: int = 60
    batch_size_rows: int = 100_000
    enable_metrics: bool = True
    enable_logging: bool = True

class StreamingProgressTracker:
    """Tracks real-time progress during data transfer operations."""
    
    def __init__(self, table_name: str, load_type: str, config: StreamingProgressConfig,
                 metrics_publisher: CloudWatchMetricsPublisher):
        self.table_name = table_name
        self.load_type = load_type  # 'full' or 'incremental'
        self.config = config
        self.metrics_publisher = metrics_publisher
        self.structured_logger = StructuredLogger("StreamingProgressTracker")
        
        # Progress state
        self.rows_processed = 0
        self.start_time = time.time()
        self.last_update_time = time.time()
        self.total_rows: Optional[int] = None
        self.is_active = False
    
    def start_tracking(self, total_rows: Optional[int] = None):
        """Start progress tracking."""
        pass
    
    def update_progress(self, rows_processed: int):
        """Update progress with new row count."""
        pass
    
    def complete_tracking(self, final_row_count: int):
        """Complete tracking and emit final metrics."""
        pass
    
    def _should_emit_update(self) -> bool:
        """Check if progress update should be emitted."""
        pass
    
    def _emit_progress_update(self):
        """Emit progress update to CloudWatch and logs."""
        pass
    
    def _calculate_eta(self) -> Optional[float]:
        """Calculate estimated time to completion."""
        pass
    
    def get_progress_percentage(self) -> float:
        """Get current progress percentage."""
        pass
    
    def get_rows_per_second(self) -> float:
        """Get current processing rate."""
        pass
```

### 3. Enhanced FullLoadProgress (Modified)

**Purpose**: Extended progress tracking with streaming capabilities and deferred counting support.

**Changes**:

```python
@dataclass
class FullLoadProgress:
    """Tracks progress of full-load operations with streaming support."""
    table_name: str
    total_rows: int = 0
    processed_rows: int = 0
    start_time: float = 0.0
    end_time: Optional[float] = None
    status: str = 'pending'
    error_message: Optional[str] = None
    
    # NEW: Streaming progress fields
    counting_strategy: str = 'auto'  # immediate, deferred, auto
    rows_counted_at: str = 'unknown'  # before_write, after_write
    last_progress_update: float = 0.0
    progress_updates_count: int = 0
    
    # NEW: Performance metrics
    read_duration_seconds: float = 0.0
    write_duration_seconds: float = 0.0
    count_duration_seconds: float = 0.0
    
    @property
    def total_duration_seconds(self) -> float:
        """Total operation duration including all phases."""
        return self.read_duration_seconds + self.write_duration_seconds + self.count_duration_seconds
    
    @property
    def effective_rows_per_second(self) -> float:
        """Effective throughput excluding counting overhead."""
        if self.write_duration_seconds == 0.0:
            return 0.0
        return self.processed_rows / self.write_duration_seconds
```

### 4. Enhanced CloudWatchMetricsPublisher (Modified)

**Purpose**: Extended metrics publishing with new migration-specific metrics.

**New Methods**:

```python
class CloudWatchMetricsPublisher:
    """Enhanced metrics publisher with migration-specific metrics."""
    
    # Existing methods remain unchanged
    
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
        """
        pass
    
    def publish_counting_strategy_metrics(self, table_name: str,
                                         strategy_type: str,
                                         count_duration_seconds: float,
                                         row_count: int):
        """
        Publish metrics about counting strategy execution.
        
        Args:
            table_name: Name of the table
            strategy_type: Type of strategy used (immediate/deferred)
            count_duration_seconds: Time taken to count
            row_count: Number of rows counted
        """
        pass
    
    def publish_migration_phase_metrics(self, table_name: str,
                                       phase: str,
                                       duration_seconds: float,
                                       rows_count: Optional[int] = None):
        """
        Publish metrics for individual migration phases.
        
        Args:
            table_name: Name of the table
            phase: Phase name (read, write, count)
            duration_seconds: Phase duration
            rows_count: Number of rows (if applicable)
        """
        pass
```

## Data Models

### Configuration Models

```python
@dataclass
class MigrationPerformanceConfig:
    """Configuration for migration performance optimizations."""
    
    # Counting strategy configuration
    counting_strategy: CountingStrategyConfig = field(
        default_factory=lambda: CountingStrategyConfig()
    )
    
    # Progress tracking configuration
    progress_tracking: StreamingProgressConfig = field(
        default_factory=lambda: StreamingProgressConfig()
    )
    
    # Metrics configuration
    enable_detailed_metrics: bool = True
    metrics_namespace: str = "AWS/Glue/DataReplication"
    
    def validate(self):
        """Validate all configuration components."""
        self.counting_strategy.validate()
```

### Progress State Models

```python
@dataclass
class MigrationPhaseMetrics:
    """Metrics for individual migration phases."""
    phase_name: str  # read, write, count
    start_time: float
    end_time: Optional[float] = None
    rows_processed: int = 0
    status: str = 'in_progress'
    
    @property
    def duration_seconds(self) -> float:
        if self.end_time is None:
            return time.time() - self.start_time
        return self.end_time - self.start_time

@dataclass
class DetailedMigrationProgress:
    """Detailed progress tracking for migration operations."""
    table_name: str
    overall_status: str = 'pending'
    
    # Phase-specific metrics
    read_phase: Optional[MigrationPhaseMetrics] = None
    write_phase: Optional[MigrationPhaseMetrics] = None
    count_phase: Optional[MigrationPhaseMetrics] = None
    
    # Streaming progress
    streaming_tracker: Optional[StreamingProgressTracker] = None
    
    def get_current_phase(self) -> Optional[str]:
        """Get the currently active phase."""
        if self.read_phase and self.read_phase.status == 'in_progress':
            return 'read'
        if self.write_phase and self.write_phase.status == 'in_progress':
            return 'write'
        if self.count_phase and self.count_phase.status == 'in_progress':
            return 'count'
        return None
```

## Error Handling

### Error Scenarios and Handling

1. **Deferred Count Failure**
   - **Scenario**: Unable to count rows from target after write
   - **Handling**: Log warning, use estimated count from write operation, mark progress as "count_unavailable"
   - **Metrics**: Publish error metric with error category "DEFERRED_COUNT_FAILED"

2. **Progress Tracking Failure**
   - **Scenario**: Unable to emit progress updates during write
   - **Handling**: Continue migration, log errors, disable progress tracking for remainder of operation
   - **Metrics**: Publish error metric, continue with basic progress tracking

3. **Metrics Publishing Failure**
   - **Scenario**: CloudWatch API calls fail
   - **Handling**: Buffer metrics, retry with exponential backoff, log locally if all retries fail
   - **Metrics**: Log metric publishing failures to structured logs

4. **Strategy Selection Failure**
   - **Scenario**: Unable to determine optimal counting strategy
   - **Handling**: Fall back to deferred counting (safest for large datasets)
   - **Metrics**: Log strategy selection failure, publish fallback metric

### Error Recovery Patterns

```python
class MigrationErrorHandler:
    """Handles errors during migration operations."""
    
    def handle_deferred_count_failure(self, table_name: str, error: Exception,
                                     estimated_rows: Optional[int]) -> int:
        """
        Handle failure in deferred counting.
        
        Returns estimated row count or 0 if unavailable.
        """
        self.structured_logger.warning(
            f"Deferred count failed for {table_name}, using estimated count",
            error=str(error),
            estimated_rows=estimated_rows
        )
        
        self.metrics_publisher.publish_error_metrics(
            error_category="DEFERRED_COUNT_FAILED",
            operation="count_from_target"
        )
        
        return estimated_rows if estimated_rows is not None else 0
    
    def handle_progress_tracking_failure(self, table_name: str, error: Exception):
        """Handle failure in progress tracking."""
        self.structured_logger.error(
            f"Progress tracking failed for {table_name}, disabling",
            error=str(error)
        )
        
        # Disable progress tracking for this table
        return False  # Indicates tracking should be disabled
```

## Testing Strategy

### Unit Tests

1. **CountingStrategy Tests**
   - Test strategy selection logic for various dataset sizes
   - Test immediate counting execution
   - Test deferred counting execution
   - Test configuration validation
   - Test fallback behavior

2. **StreamingProgressTracker Tests**
   - Test progress update emission at intervals
   - Test ETA calculation accuracy
   - Test progress percentage calculation
   - Test metric publishing integration
   - Test completion handling

3. **Enhanced Progress Models Tests**
   - Test new fields and properties
   - Test phase metrics calculation
   - Test progress state transitions

### Integration Tests

1. **End-to-End Migration with Deferred Counting**
   - Test full migration flow with large dataset (simulated)
   - Verify single source read
   - Verify target counting after write
   - Verify metrics published correctly

2. **Streaming Progress Integration**
   - Test progress updates during active migration
   - Verify CloudWatch metrics emission
   - Verify structured log output
   - Test with various update intervals

3. **Strategy Selection Integration**
   - Test auto strategy selection with small datasets
   - Test auto strategy selection with large datasets
   - Test manual strategy override
   - Test Iceberg-specific behavior

### Performance Tests

1. **Large Dataset Simulation**
   - Simulate 1TB+ dataset migration
   - Measure time savings from deferred counting
   - Verify memory usage remains constant
   - Verify progress updates don't impact throughput

2. **Metrics Publishing Performance**
   - Test metric buffering and batching
   - Verify no performance degradation from metrics
   - Test with high-frequency updates

## Implementation Phases

### Phase 1: Core Counting Strategy (Priority: High)
- Implement CountingStrategy component
- Implement deferred counting in FullLoadDataMigrator
- Add configuration support
- Unit tests for counting logic

### Phase 2: Streaming Progress Tracking (Priority: High)
- Implement StreamingProgressTracker
- Integrate with write operations
- Add progress update emission
- Unit tests for progress tracking

### Phase 3: Enhanced Metrics (Priority: Medium)
- Add new CloudWatch metrics
- Implement phase-specific metrics
- Add counting strategy metrics
- Integration tests for metrics

### Phase 4: Configuration and Documentation (Priority: Medium)
- Add configuration parameters to JobConfig
- Update parameter reference documentation
- Add usage examples
- Create migration guide

### Phase 5: Monitoring and Observability (Priority: Low)
- Create CloudWatch dashboard templates
- Add sample alarm configurations
- Document monitoring best practices
- Add troubleshooting guide

## Performance Considerations

### Memory Usage
- **SQL COUNT(*)**: No additional memory overhead (single integer result)
- **Streaming Progress**: Minimal overhead (~1KB per table for progress state)
- **Metrics Buffering**: Bounded buffer size (max 20 metrics per batch)

### Network I/O
- **SQL COUNT Query**: Single lightweight query returning one integer
- **No Double Reading**: Source data read exactly once for the actual transfer
- **Added**: Periodic CloudWatch API calls (batched, minimal overhead)

### Processing Time
- **SQL COUNT(*)**: Typically <1 second for JDBC (uses database statistics)
- **Iceberg COUNT(*)**: Typically <1 second (reads manifest metadata only)
- **Progress tracking overhead**: Negligible (<0.1%)

### Performance Comparison

| Approach | Count Time (1B rows) | Data Reads | Total Time |
|----------|---------------------|------------|------------|
| Old (df.count()) | ~30 minutes | 2x (count + write) | ~90 minutes |
| New (SQL COUNT) | <1 second | 1x (write only) | ~30 minutes |
| Improvement | 99.9% faster count | 50% less I/O | ~66% faster |

### Why SQL COUNT(*) is Fast

**JDBC Databases (Oracle, SQL Server, PostgreSQL, DB2)**:
- Database maintains table statistics and row count estimates
- COUNT(*) often uses index-only scans or cached statistics
- Query executed server-side, only result transmitted

**Iceberg Tables**:
- Row counts stored in manifest metadata files
- Spark SQL COUNT(*) reads metadata, not data files
- No full table scan required

## Backward Compatibility

### API Compatibility
- All existing method signatures remain unchanged
- New parameters added as optional with sensible defaults
- Existing progress tracking behavior preserved when new features disabled

### Configuration Compatibility
- New configuration parameters are optional
- Default behavior matches current implementation for small datasets
- Automatic strategy selection ensures optimal performance without configuration changes

### Metrics Compatibility
- All existing metrics continue to be published
- New metrics use distinct names to avoid conflicts
- Existing dashboards and alarms continue to work

## Security Considerations

### Data Access
- No changes to data access patterns or permissions
- Target counting uses existing connection credentials
- No additional data exposure

### Metrics and Logging
- Progress metrics do not contain sensitive data
- Row counts and table names are already logged
- No PII or credentials in new log messages

### Configuration
- Configuration parameters validated at initialization
- No security-sensitive configuration added
- Existing security controls remain in place
