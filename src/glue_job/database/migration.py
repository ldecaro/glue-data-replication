"""
Data migration operations for AWS Glue Data Replication.

This module provides full-load and incremental data migration capabilities
for database replication operations with enhanced Iceberg engine support.
"""

import time
import logging
from typing import Dict, Any, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from .counting_strategy import CountingStrategy
    from ..monitoring.streaming_progress_tracker import StreamingProgressTracker, StreamingProgressConfig
    from ..monitoring.metrics import CloudWatchMetricsPublisher
    from ..config.partitioned_read_config import PartitionedReadConfig
# Conditional imports for PySpark (only available in Glue runtime)
try:
    from pyspark.sql import SparkSession, DataFrame
    from pyspark.sql.functions import col, max as spark_max
except ImportError:
    # Mock classes for local development/testing
    class SparkSession:
        pass
    class DataFrame:
        pass
    def col(name):
        return name
    def spark_max(name):
        return name

# Import from other modules
from ..config.job_config import ConnectionConfig
from ..monitoring.progress import FullLoadProgress, IncrementalLoadProgress
from ..monitoring.logging import StructuredLogger
from ..storage.bookmark_manager import JobBookmarkManager
from .connection_manager import UnifiedConnectionManager
from .incremental_detector import IncrementalColumnDetector
from ..config.database_engines import DatabaseEngineManager
from ..config.iceberg_models import IcebergEngineError

logger = logging.getLogger(__name__)


class FullLoadDataMigrator:
    """Handles full-load data migration operations with Iceberg engine support."""
    
    def __init__(self, spark_session: SparkSession, connection_manager: UnifiedConnectionManager,
                 counting_strategy: Optional['CountingStrategy'] = None,
                 metrics_publisher: Optional['CloudWatchMetricsPublisher'] = None,
                 streaming_progress_config: Optional['StreamingProgressConfig'] = None,
                 partitioned_read_config: Optional['PartitionedReadConfig'] = None):
        self.spark = spark_session
        self.connection_manager = connection_manager
        self.structured_logger = StructuredLogger("FullLoadDataMigrator")
        
        # Initialize counting strategy with default config if not provided
        if counting_strategy is None:
            from .counting_strategy import CountingStrategy, CountingStrategyConfig
            counting_strategy = CountingStrategy(
                CountingStrategyConfig(),
                metrics_publisher=metrics_publisher
            )
        self.counting_strategy = counting_strategy
        
        # Store metrics publisher and streaming progress config for progress tracking
        self.metrics_publisher = metrics_publisher
        self.streaming_progress_config = streaming_progress_config
        
        # Store partitioned read config for parallel JDBC reads
        self.partitioned_read_config = partitioned_read_config
    
    def perform_full_load_migration(self, source_config: ConnectionConfig, 
                                  target_config: ConnectionConfig, table_name: str) -> FullLoadProgress:
        """
        Perform full-load migration for a table with Iceberg engine support.
        
        Args:
            source_config: Source database connection configuration
            target_config: Target database connection configuration
            table_name: Name of the table to migrate
            
        Returns:
            FullLoadProgress: Migration progress and results
            
        Raises:
            IcebergEngineError: If Iceberg-specific operations fail
        """
        from .counting_strategy import CountingStrategyType
        from ..monitoring.streaming_progress_tracker import StreamingProgressTracker, StreamingProgressConfig
        
        progress = FullLoadProgress(table_name=table_name)
        progress.start_time = time.time()
        progress.status = 'in_progress'
        
        # Check engine types for enhanced logging and error handling
        source_is_iceberg = DatabaseEngineManager.is_iceberg_engine(source_config.engine_type)
        target_is_iceberg = DatabaseEngineManager.is_iceberg_engine(target_config.engine_type)
        
        # Select counting strategy
        selected_strategy = self.counting_strategy.select_strategy(
            df=None,  # We don't have the DataFrame yet
            table_name=table_name,
            engine_type=source_config.engine_type,
            estimated_size=None
        )
        
        # Update progress with strategy information
        progress.counting_strategy = selected_strategy.value
        
        # Initialize StreamingProgressTracker if metrics publisher is available
        streaming_tracker = None
        if self.metrics_publisher is not None:
            # Use provided config or create default
            tracker_config = self.streaming_progress_config or StreamingProgressConfig()
            streaming_tracker = StreamingProgressTracker(
                table_name=table_name,
                load_type='full',
                config=tracker_config,
                metrics_publisher=self.metrics_publisher,
                engine_type=target_config.engine_type  # Pass target engine type for Iceberg-aware tracking
            )
        
        # Requirement 5.1: Log migration start with engine types
        self.structured_logger.log_migration_start(
            table_name=table_name,
            load_type='full',
            source_engine=source_config.engine_type,
            target_engine=target_config.engine_type,
            counting_strategy=selected_strategy.value,
            source_is_iceberg=source_is_iceberg,
            target_is_iceberg=target_is_iceberg,
            streaming_progress_enabled=streaming_tracker is not None
        )
        
        try:
            # Read all data from source with engine-specific handling
            read_start_time = time.time()
            source_df = self._read_source_data_with_engine_support(
                source_config, table_name, source_is_iceberg
            )
            progress.read_duration_seconds = time.time() - read_start_time
            
            # Publish read phase metrics
            if self.metrics_publisher:
                self.metrics_publisher.publish_migration_phase_metrics(
                    table_name=table_name,
                    phase='read',
                    load_type='full',
                    duration_seconds=progress.read_duration_seconds
                )
            
            # Immediate counting using SQL COUNT(*) (only if strategy is IMMEDIATE)
            if selected_strategy == CountingStrategyType.IMMEDIATE:
                count_start_time = time.time()
                
                # Use SQL COUNT(*) with fallback to deferred counting
                row_count, counting_method = self.counting_strategy.execute_immediate_count_sql_with_fallback(
                    source_config=source_config,
                    table_name=table_name,
                    connection_manager=self.connection_manager,
                    target_config=target_config
                )
                
                progress.count_duration_seconds = time.time() - count_start_time
                
                if counting_method == 'immediate_sql':
                    # SQL COUNT succeeded
                    progress.total_rows = row_count
                    progress.rows_counted_at = 'before_write'
                    
                    self.structured_logger.info(
                        f"Counted {progress.total_rows} rows from source {table_name} using SQL COUNT(*)",
                        source_engine=source_config.engine_type,
                        counting_strategy="immediate_sql",
                        count_duration_seconds=round(progress.count_duration_seconds, 2)
                    )
                    
                    # Publish count phase metrics
                    if self.metrics_publisher:
                        self.metrics_publisher.publish_migration_phase_metrics(
                            table_name=table_name,
                            phase='count',
                            load_type='full',
                            duration_seconds=progress.count_duration_seconds,
                            rows_count=progress.total_rows
                        )
                    
                    # Start streaming progress tracker with known total rows
                    if streaming_tracker is not None:
                        try:
                            streaming_tracker.start_tracking(total_rows=progress.total_rows)
                        except Exception as tracker_error:
                            # Requirement 2.1: Continue migration even if progress tracking fails
                            self.structured_logger.warning(
                                f"Failed to start progress tracking for {table_name}, continuing migration",
                                table_name=table_name,
                                error=str(tracker_error),
                                error_type=type(tracker_error).__name__
                            )
                else:
                    # SQL COUNT failed, will use deferred counting after write
                    selected_strategy = CountingStrategyType.DEFERRED
                    progress.counting_strategy = 'deferred'
                    
                    self.structured_logger.info(
                        f"SQL COUNT failed for {table_name}, switching to deferred counting",
                        source_engine=source_config.engine_type,
                        counting_strategy="deferred_fallback"
                    )
                    
                    # Start streaming progress tracker without total rows (deferred counting)
                    if streaming_tracker is not None:
                        try:
                            streaming_tracker.start_tracking(total_rows=None)
                        except Exception as tracker_error:
                            self.structured_logger.warning(
                                f"Failed to start progress tracking for {table_name}, continuing migration",
                                table_name=table_name,
                                error=str(tracker_error),
                                error_type=type(tracker_error).__name__
                            )
            else:
                # For deferred counting, we don't count yet
                self.structured_logger.info(
                    f"Skipping immediate count for {table_name} (deferred strategy)",
                    source_engine=source_config.engine_type,
                    counting_strategy="deferred"
                )
                
                # Start streaming progress tracker without total rows (deferred counting)
                if streaming_tracker is not None:
                    try:
                        streaming_tracker.start_tracking(total_rows=None)
                    except Exception as tracker_error:
                        # Requirement 2.1: Continue migration even if progress tracking fails
                        self.structured_logger.warning(
                            f"Failed to start progress tracking for {table_name}, continuing migration",
                            table_name=table_name,
                            error=str(tracker_error),
                            error_type=type(tracker_error).__name__
                        )
            
            # Write to target with engine-specific handling
            write_start_time = time.time()
            self._write_target_data_with_engine_support(
                source_df, target_config, table_name, target_is_iceberg, mode='overwrite',
                streaming_tracker=streaming_tracker
            )
            progress.write_duration_seconds = time.time() - write_start_time
            
            # Publish write phase metrics
            if self.metrics_publisher:
                self.metrics_publisher.publish_migration_phase_metrics(
                    table_name=table_name,
                    phase='write',
                    load_type='full',
                    duration_seconds=progress.write_duration_seconds
                )
            
            # Deferred counting (only if strategy is DEFERRED)
            if selected_strategy == CountingStrategyType.DEFERRED:
                count_start_time = time.time()
                try:
                    progress.total_rows = self.counting_strategy.execute_deferred_count(
                        target_config, table_name, self.connection_manager
                    )
                    progress.count_duration_seconds = time.time() - count_start_time
                    progress.rows_counted_at = 'after_write'
                    
                    self.structured_logger.info(
                        f"Counted {progress.total_rows} rows from target {table_name} (deferred)",
                        target_engine=target_config.engine_type,
                        counting_strategy="deferred",
                        count_duration_seconds=round(progress.count_duration_seconds, 2)
                    )
                    
                    # Publish count phase metrics
                    if self.metrics_publisher:
                        self.metrics_publisher.publish_migration_phase_metrics(
                            table_name=table_name,
                            phase='count',
                            load_type='full',
                            duration_seconds=progress.count_duration_seconds,
                            rows_count=progress.total_rows
                        )
                except Exception as count_error:
                    progress.count_duration_seconds = time.time() - count_start_time
                    
                    # Try to get estimated count from streaming tracker
                    estimated_rows = None
                    if streaming_tracker is not None:
                        estimated_rows = streaming_tracker.rows_processed
                    
                    # Fallback to estimated count
                    progress.total_rows = estimated_rows if estimated_rows is not None else 0
                    progress.rows_counted_at = 'estimated_fallback'
                    
                    # Log warning with full context
                    self.structured_logger.warning(
                        f"Deferred counting failed for {table_name}, using estimated count",
                        table_name=table_name,
                        target_engine=target_config.engine_type,
                        counting_strategy="deferred",
                        error=str(count_error),
                        error_type=type(count_error).__name__,
                        estimated_rows=progress.total_rows,
                        count_duration_seconds=round(progress.count_duration_seconds, 2),
                        fallback_source="streaming_tracker" if estimated_rows is not None else "zero_default"
                    )
                    
                    # Publish error metrics
                    if self.metrics_publisher:
                        self.metrics_publisher.publish_error_metrics(
                            error_category="DEFERRED_COUNT_FAILED",
                            error_operation="count_from_target"
                        )
            
            progress.processed_rows = progress.total_rows
            progress.end_time = time.time()
            progress.status = 'completed'
            
            # Complete streaming progress tracker
            if streaming_tracker is not None:
                try:
                    streaming_tracker.complete_tracking(final_row_count=progress.total_rows)
                    progress.progress_updates_count = streaming_tracker.progress_updates_emitted
                except Exception as tracker_error:
                    # Requirement 2.1: Continue migration even if progress tracking fails
                    self.structured_logger.warning(
                        f"Failed to complete progress tracking for {table_name}, continuing migration",
                        table_name=table_name,
                        error=str(tracker_error),
                        error_type=type(tracker_error).__name__
                    )
                    # Set progress updates count to 0 if we can't get it from tracker
                    progress.progress_updates_count = 0
            
            # Requirement 5.3: Log migration completion with metrics
            self.structured_logger.log_migration_completion(
                table_name=table_name,
                load_type='full',
                total_rows=progress.processed_rows,
                duration_seconds=progress.duration_seconds,
                rows_per_second=progress.effective_rows_per_second,
                read_duration_seconds=progress.read_duration_seconds,
                write_duration_seconds=progress.write_duration_seconds,
                count_duration_seconds=progress.count_duration_seconds,
                counting_strategy=progress.counting_strategy,
                rows_counted_at=progress.rows_counted_at,
                source_engine=source_config.engine_type,
                target_engine=target_config.engine_type
            )
            
            # Publish migration success status
            if self.metrics_publisher:
                self.metrics_publisher.publish_migration_phase_metrics(
                    table_name=table_name,
                    phase='write',  # Use write phase for status
                    load_type='full',
                    duration_seconds=0,  # Not relevant for status metric
                    migration_status='success'
                )
            
        except IcebergEngineError as e:
            progress.end_time = time.time()
            progress.status = 'failed'
            progress.error_message = f"Iceberg engine error: {str(e)}"
            
            # Requirement 5.4: Error logging with full context
            self.structured_logger.log_migration_error(
                table_name=table_name,
                load_type='full',
                error=str(e),
                error_type='IcebergEngineError',
                duration_seconds=time.time() - progress.start_time,
                rows_processed=progress.processed_rows,
                source_engine=source_config.engine_type,
                target_engine=target_config.engine_type,
                counting_strategy=progress.counting_strategy,
                source_is_iceberg=source_is_iceberg,
                target_is_iceberg=target_is_iceberg
            )
            
            # Publish migration failure status
            if self.metrics_publisher:
                self.metrics_publisher.publish_migration_phase_metrics(
                    table_name=table_name,
                    phase='write',
                    load_type='full',
                    duration_seconds=0,
                    migration_status='failed'
                )
        except Exception as e:
            progress.end_time = time.time()
            progress.status = 'failed'
            progress.error_message = str(e)
            
            # Requirement 5.4: Error logging with full context
            self.structured_logger.log_migration_error(
                table_name=table_name,
                load_type='full',
                error=str(e),
                error_type=type(e).__name__,
                duration_seconds=time.time() - progress.start_time,
                rows_processed=progress.processed_rows,
                source_engine=source_config.engine_type,
                target_engine=target_config.engine_type,
                counting_strategy=progress.counting_strategy
            )
            
            # Publish migration failure status
            if self.metrics_publisher:
                self.metrics_publisher.publish_migration_phase_metrics(
                    table_name=table_name,
                    phase='write',
                    load_type='full',
                    duration_seconds=0,
                    migration_status='failed'
                )
        
        return progress
    
    def _read_source_data_with_engine_support(self, source_config: ConnectionConfig, 
                                            table_name: str, source_is_iceberg: bool) -> DataFrame:
        """
        Read source data with engine-specific support.
        
        Args:
            source_config: Source connection configuration
            table_name: Name of the table to read
            source_is_iceberg: Whether source engine is Iceberg
            
        Returns:
            DataFrame: Source data
            
        Raises:
            IcebergEngineError: If Iceberg read operations fail
        """
        try:
            if source_is_iceberg:
                self.structured_logger.debug(f"Reading from Iceberg table: {table_name}")
                # For Iceberg sources, use the connection manager's Iceberg-aware read
                return self.connection_manager.read_table(
                    connection_config=source_config,
                    table_name=table_name
                )
            else:
                # For traditional databases, check if partitioned reads are enabled
                has_config = self.partitioned_read_config is not None
                should_partition = has_config and self.partitioned_read_config.should_use_partitioned_read(table_name)
                
                self.structured_logger.info(
                    f"Read strategy decision for {table_name}",
                    table_name=table_name,
                    has_partitioned_config=has_config,
                    partitioned_config_enabled=has_config and self.partitioned_read_config.enabled,
                    should_use_partitioned_read=should_partition
                )
                
                if should_partition:
                    return self._read_with_partitioned_jdbc(source_config, table_name)
                else:
                    self.structured_logger.info(f"Using standard JDBC read for {table_name}", table_name=table_name)
                    # For traditional databases, use standard read
                    return self.connection_manager.read_table_data(source_config, table_name)
        except Exception as e:
            if source_is_iceberg:
                raise IcebergEngineError(
                    f"Failed to read from Iceberg table {table_name}: {str(e)}",
                    error_code="READ_ERROR",
                    context={"table_name": table_name, "spark_error": str(e)}
                )
            else:
                raise
    
    def _read_with_partitioned_jdbc(self, source_config: ConnectionConfig, table_name: str) -> DataFrame:
        """
        Read data using parallel JDBC connections with partitioning.
        
        This method enables parallel reads by splitting the data across multiple
        JDBC connections based on a partition column (typically primary key).
        
        Args:
            source_config: Source connection configuration
            table_name: Name of the table to read
            
        Returns:
            DataFrame: Source data distributed across partitions
        """
        from .partition_detector import PartitionColumnDetector
        
        # Get table-specific config or use defaults
        table_config = self.partitioned_read_config.get_table_config(table_name)
        
        # Determine partition column
        partition_column = None
        lower_bound = None
        upper_bound = None
        num_partitions = self.partitioned_read_config.default_num_partitions
        fetch_size = self.partitioned_read_config.default_fetch_size
        
        if table_config:
            partition_column = table_config.partition_column
            lower_bound = table_config.lower_bound
            upper_bound = table_config.upper_bound
            num_partitions = table_config.num_partitions or num_partitions
            fetch_size = table_config.fetch_size
        
        # Auto-detect partition column if not specified
        if not partition_column:
            self.structured_logger.info(
                f"Auto-detecting partition column for {table_name}",
                table_name=table_name
            )
            detector = PartitionColumnDetector(self.spark, self.structured_logger)
            partition_info = detector.detect_partition_column(source_config, table_name)
            
            if partition_info:
                partition_column = partition_info.column_name
                lower_bound = partition_info.min_value
                upper_bound = partition_info.max_value
                
                # Auto-calculate partitions if not specified
                if num_partitions == 0:
                    num_partitions = partition_info.calculate_optimal_partitions()
                
                self.structured_logger.info(
                    f"Auto-detected partition column for {table_name}",
                    table_name=table_name,
                    partition_column=partition_column,
                    lower_bound=lower_bound,
                    upper_bound=upper_bound,
                    num_partitions=num_partitions,
                    row_count=partition_info.row_count
                )
            else:
                # No suitable partition column found, fall back to standard read
                self.structured_logger.warning(
                    f"No suitable partition column found for {table_name}, using standard read",
                    table_name=table_name
                )
                return self.connection_manager.read_table_data(source_config, table_name)
        
        # Get bounds if not provided
        if lower_bound is None or upper_bound is None:
            self.structured_logger.info(
                f"Detecting bounds for partition column {partition_column}",
                table_name=table_name,
                partition_column=partition_column
            )
            detector = PartitionColumnDetector(self.spark, self.structured_logger)
            partition_info = detector._get_column_stats(
                source_config, source_config.schema, table_name,
                partition_column, 'numeric'
            )
            if partition_info:
                lower_bound = partition_info.min_value
                upper_bound = partition_info.max_value
                if num_partitions == 0:
                    num_partitions = partition_info.calculate_optimal_partitions()
            else:
                self.structured_logger.warning(
                    f"Could not detect bounds for {partition_column}, using standard read",
                    table_name=table_name
                )
                return self.connection_manager.read_table_data(source_config, table_name)
        
        # Ensure we have valid partitions
        if num_partitions == 0:
            num_partitions = 10  # Default fallback
        
        self.structured_logger.info(
            f"Starting partitioned JDBC read for {table_name}",
            table_name=table_name,
            partition_column=partition_column,
            lower_bound=lower_bound,
            upper_bound=upper_bound,
            num_partitions=num_partitions,
            fetch_size=fetch_size
        )
        
        # Use partitioned read
        df = self.connection_manager.read_table_data_partitioned(
            connection_config=source_config,
            table_name=table_name,
            partition_column=partition_column,
            lower_bound=lower_bound,
            upper_bound=upper_bound,
            num_partitions=num_partitions,
            fetch_size=fetch_size
        )
        
        # Log completion with actual partition count for visibility
        actual_partitions = df.rdd.getNumPartitions()
        self.structured_logger.info(
            f"Partitioned JDBC read complete for {table_name} | {actual_partitions} parallel connections used",
            table_name=table_name,
            partition_column=partition_column,
            requested_partitions=num_partitions,
            actual_partitions=actual_partitions,
            parallel_connections=actual_partitions
        )
        
        return df
    
    def _write_target_data_with_engine_support(self, df: DataFrame, target_config: ConnectionConfig,
                                             table_name: str, target_is_iceberg: bool, mode: str = 'overwrite',
                                             streaming_tracker: Optional['StreamingProgressTracker'] = None) -> None:
        """
        Write target data with engine-specific support and streaming progress tracking.
        
        Args:
            df: DataFrame to write
            target_config: Target connection configuration
            table_name: Name of the target table
            target_is_iceberg: Whether target engine is Iceberg
            mode: Write mode (overwrite, append)
            streaming_tracker: Optional streaming progress tracker for real-time updates
            
        Raises:
            IcebergEngineError: If Iceberg write operations fail
        """
        try:
            # If streaming tracker is available, we'll track progress
            # Note: Spark write operations are atomic, so we simulate progress tracking
            # by updating after write completes. For more granular tracking, we could
            # use foreachPartition, but that adds complexity.
            
            if target_is_iceberg:
                self.structured_logger.debug(f"Writing to Iceberg table: {table_name}")
                # For Iceberg targets, use partition-level progress tracking with SimpleSparkProgressTracker
                # This provides consistent progress updates for both full load and incremental load
                self._write_with_partition_progress(
                    df=df,
                    target_config=target_config,
                    table_name=table_name,
                    mode=mode,
                    is_iceberg=True,
                    streaming_tracker=streaming_tracker
                )
            else:
                self.structured_logger.debug(f"Writing to JDBC table: {table_name}")
                # For traditional databases, use partition-level progress tracking
                self._write_with_partition_progress(
                    df=df,
                    target_config=target_config,
                    table_name=table_name,
                    mode=mode,
                    is_iceberg=False,
                    streaming_tracker=streaming_tracker
                )
        except Exception as e:
            if target_is_iceberg:
                raise IcebergEngineError(
                    f"Failed to write to Iceberg table {table_name}: {str(e)}",
                    error_code="WRITE_ERROR",
                    context={"table_name": table_name, "spark_error": str(e)}
                )
            else:
                raise
    
    def _write_with_partition_progress(self, df: DataFrame, target_config: ConnectionConfig,
                                      table_name: str, mode: str, is_iceberg: bool,
                                      streaming_tracker: Optional['StreamingProgressTracker'] = None) -> None:
        """
        Write data with partition-level progress tracking.
        
        This method provides granular progress updates by tracking each partition as it's written.
        For JDBC targets, it uses foreachPartition to write and track progress incrementally.
        For Iceberg targets, it uses Spark progress polling to track write progress.
        
        Args:
            df: DataFrame to write
            target_config: Target connection configuration
            table_name: Name of the target table
            mode: Write mode ('append' or 'overwrite')
            is_iceberg: Whether target is Iceberg
            streaming_tracker: Optional streaming progress tracker for real-time updates
        """
        try:
            if is_iceberg:
                # For Iceberg, use Spark progress tracker with periodic polling
                self._write_iceberg_with_spark_progress(
                    df=df,
                    target_config=target_config,
                    table_name=table_name,
                    mode=mode,
                    streaming_tracker=streaming_tracker
                )
            else:
                # For JDBC, use foreachPartition for granular progress tracking
                self._write_jdbc_with_partition_progress(
                    df=df,
                    target_config=target_config,
                    table_name=table_name,
                    mode=mode,
                    streaming_tracker=streaming_tracker
                )
        except Exception as e:
            self.structured_logger.error(
                f"Failed to write data with partition progress for {table_name}",
                table_name=table_name,
                error=str(e),
                error_type=type(e).__name__
            )
            raise
    
    def _write_iceberg_with_spark_progress(self, df: DataFrame, target_config: ConnectionConfig,
                                          table_name: str, mode: str,
                                          streaming_tracker: Optional['StreamingProgressTracker'] = None) -> None:
        """
        Write Iceberg data with Spark progress tracking.
        
        This method uses SimpleSparkProgressTracker to poll Spark job progress
        during the Iceberg write operation, providing real-time visibility.
        
        Args:
            df: DataFrame to write
            target_config: Target connection configuration
            table_name: Name of the target table
            mode: Write mode ('append' or 'overwrite')
            streaming_tracker: Optional streaming progress tracker for real-time updates
        """
        from ..monitoring.spark_progress_listener import SimpleSparkProgressTracker
        
        # Get estimated row count for progress tracking
        total_rows_estimate = None
        if streaming_tracker and streaming_tracker.total_rows:
            total_rows_estimate = streaming_tracker.total_rows
        
        # Initialize Spark progress tracker
        spark_progress_tracker = None
        if streaming_tracker is not None:
            try:
                spark_progress_tracker = SimpleSparkProgressTracker(
                    spark_session=self.spark,
                    table_name=table_name,
                    streaming_tracker=streaming_tracker
                )
                spark_progress_tracker.start_tracking(total_rows_estimate=total_rows_estimate)
                
                self.structured_logger.info(
                    f"Started Spark progress tracking for Iceberg write",
                    table_name=table_name,
                    total_rows_estimate=total_rows_estimate
                )
            except Exception as tracker_error:
                self.structured_logger.warning(
                    f"Failed to start Spark progress tracker for {table_name}, continuing without progress tracking",
                    table_name=table_name,
                    error=str(tracker_error),
                    error_type=type(tracker_error).__name__
                )
                spark_progress_tracker = None
        
        try:
            # Repartition DataFrame for parallel writes if it has too few partitions
            current_partitions = df.rdd.getNumPartitions()
            
            self.structured_logger.info(
                f"DataFrame partition check for {table_name}",
                table_name=table_name,
                current_partitions=current_partitions,
                total_rows_estimate=total_rows_estimate,
                streaming_tracker_available=streaming_tracker is not None
            )
            
            if total_rows_estimate and total_rows_estimate > 0:
                # Calculate optimal partition count based on data size
                if total_rows_estimate > 5_000_000:
                    target_partitions = 20
                elif total_rows_estimate > 1_000_000:
                    target_partitions = 10
                elif total_rows_estimate > 100_000:
                    target_partitions = 5
                else:
                    target_partitions = 2
                
                if current_partitions < target_partitions:
                    self.structured_logger.info(
                        f"Repartitioning DataFrame for parallel writes",
                        table_name=table_name,
                        current_partitions=current_partitions,
                        target_partitions=target_partitions,
                        total_rows=total_rows_estimate
                    )
                    df = df.repartition(target_partitions)
                    
                    # Verify repartitioning worked
                    new_partitions = df.rdd.getNumPartitions()
                    self.structured_logger.info(
                        f"Repartitioning complete for {table_name}",
                        table_name=table_name,
                        previous_partitions=current_partitions,
                        new_partitions=new_partitions
                    )
                else:
                    self.structured_logger.info(
                        f"DataFrame already has sufficient partitions",
                        table_name=table_name,
                        current_partitions=current_partitions,
                        total_rows=total_rows_estimate
                    )
            else:
                # Fallback: If we don't have row count, use a default repartitioning for large reads
                # This ensures parallelism even when SQL COUNT fails
                if current_partitions == 1:
                    default_partitions = 10
                    self.structured_logger.info(
                        f"No row count available, applying default repartitioning for {table_name}",
                        table_name=table_name,
                        current_partitions=current_partitions,
                        default_partitions=default_partitions
                    )
                    df = df.repartition(default_partitions)
            
            # Perform Iceberg write
            self.connection_manager.write_table(
                df=df,
                connection_config=target_config,
                table_name=table_name,
                mode=mode
            )
            
            # Update progress after write completes
            if streaming_tracker is not None and total_rows_estimate is not None:
                try:
                    streaming_tracker.update_progress(total_rows_estimate)
                except Exception as update_error:
                    self.structured_logger.warning(
                        f"Failed to update streaming progress for {table_name}",
                        table_name=table_name,
                        error=str(update_error)
                    )
        
        finally:
            if spark_progress_tracker is not None:
                try:
                    spark_progress_tracker.complete_tracking()
                except Exception as tracker_error:
                    self.structured_logger.warning(
                        f"Failed to stop Spark progress tracker for {table_name}",
                        table_name=table_name,
                        error=str(tracker_error)
                    )
    
    def _write_jdbc_with_partition_progress(self, df: DataFrame, target_config: ConnectionConfig,
                                           table_name: str, mode: str,
                                           streaming_tracker: Optional['StreamingProgressTracker'] = None) -> None:
        """
        Write JDBC data with Spark progress polling.
        
        This method uses SimpleSparkProgressTracker to poll Spark job progress
        during the JDBC write operation, providing real-time visibility without
        serialization issues.
        
        Args:
            df: DataFrame to write
            target_config: Target connection configuration
            table_name: Name of the target table
            mode: Write mode ('append' or 'overwrite')
            streaming_tracker: Optional streaming progress tracker for real-time updates
        """
        from ..monitoring.spark_progress_listener import SimpleSparkProgressTracker
        
        total_partitions = df.rdd.getNumPartitions()
        
        # Get estimated row count for progress tracking
        total_rows_estimate = None
        if streaming_tracker and streaming_tracker.total_rows:
            total_rows_estimate = streaming_tracker.total_rows
        
        self.structured_logger.info(
            f"Starting JDBC write with Spark progress tracking",
            table_name=table_name,
            total_partitions=total_partitions,
            mode=mode,
            total_rows_estimate=total_rows_estimate
        )
        
        # Initialize Spark progress tracker
        spark_progress_tracker = None
        if streaming_tracker is not None:
            try:
                spark_progress_tracker = SimpleSparkProgressTracker(
                    spark_session=self.spark,
                    table_name=table_name,
                    streaming_tracker=streaming_tracker
                )
                spark_progress_tracker.start_tracking(total_rows_estimate=total_rows_estimate)
                
                self.structured_logger.info(
                    f"Started Spark progress tracking for JDBC write",
                    table_name=table_name,
                    total_rows_estimate=total_rows_estimate
                )
            except Exception as tracker_error:
                self.structured_logger.warning(
                    f"Failed to start Spark progress tracker for {table_name}, continuing without progress tracking",
                    table_name=table_name,
                    error=str(tracker_error),
                    error_type=type(tracker_error).__name__
                )
                spark_progress_tracker = None
        
        try:
            # Repartition DataFrame for parallel writes if it has too few partitions
            current_partitions = df.rdd.getNumPartitions()
            
            if total_rows_estimate and total_rows_estimate > 0:
                if total_rows_estimate > 5_000_000:
                    target_partitions = 20
                elif total_rows_estimate > 1_000_000:
                    target_partitions = 10
                elif total_rows_estimate > 100_000:
                    target_partitions = 5
                else:
                    target_partitions = 2
                
                if current_partitions < target_partitions:
                    self.structured_logger.info(
                        f"Repartitioning DataFrame for parallel writes",
                        table_name=table_name,
                        current_partitions=current_partitions,
                        target_partitions=target_partitions,
                        total_rows=total_rows_estimate
                    )
                    df = df.repartition(target_partitions)
                else:
                    self.structured_logger.info(
                        f"DataFrame already has sufficient partitions",
                        table_name=table_name,
                        current_partitions=current_partitions,
                        total_rows=total_rows_estimate
                    )
            
            # Use standard JDBC write
            self.connection_manager.write_table_data(df, target_config, table_name, mode=mode)
            
            # Update progress after write completes
            if streaming_tracker is not None and total_rows_estimate is not None:
                try:
                    streaming_tracker.update_progress(total_rows_estimate)
                    
                    self.structured_logger.info(
                        f"Completed JDBC write with progress tracking",
                        table_name=table_name,
                        total_rows=total_rows_estimate,
                        total_partitions=total_partitions
                    )
                except Exception as update_error:
                    self.structured_logger.warning(
                        f"Failed to update streaming progress for {table_name}",
                        table_name=table_name,
                        error=str(update_error),
                        error_type=type(update_error).__name__
                    )
        
        finally:
            if spark_progress_tracker is not None:
                try:
                    spark_progress_tracker.complete_tracking()
                except Exception as tracker_error:
                    self.structured_logger.warning(
                        f"Failed to stop Spark progress tracker for {table_name}",
                        table_name=table_name,
                        error=str(tracker_error)
                    )


class IncrementalDataMigrator:
    """Handles incremental data migration operations with Iceberg engine support."""
    
    def __init__(self, spark_session: SparkSession, connection_manager: UnifiedConnectionManager, 
                 bookmark_manager: JobBookmarkManager,
                 counting_strategy: Optional['CountingStrategy'] = None,
                 metrics_publisher: Optional['CloudWatchMetricsPublisher'] = None,
                 streaming_progress_config: Optional['StreamingProgressConfig'] = None,
                 partitioned_read_config: Optional['PartitionedReadConfig'] = None):
        self.spark = spark_session
        self.connection_manager = connection_manager
        self.bookmark_manager = bookmark_manager
        self.structured_logger = StructuredLogger("IncrementalDataMigrator")
        
        # Initialize counting strategy with default config if not provided
        if counting_strategy is None:
            from .counting_strategy import CountingStrategy, CountingStrategyConfig
            counting_strategy = CountingStrategy(
                CountingStrategyConfig(),
                metrics_publisher=metrics_publisher
            )
        self.counting_strategy = counting_strategy
        
        # Store metrics publisher and streaming progress config for progress tracking
        self.metrics_publisher = metrics_publisher
        self.streaming_progress_config = streaming_progress_config
        
        # Store partitioned read config for parallel JDBC reads
        self.partitioned_read_config = partitioned_read_config
    
    def perform_incremental_load_migration(self, source_config: ConnectionConfig, 
                                         target_config: ConnectionConfig, table_name: str) -> IncrementalLoadProgress:
        """
        Perform incremental migration for a table with Iceberg engine support.
        
        Args:
            source_config: Source database connection configuration
            target_config: Target database connection configuration
            table_name: Name of the table to migrate
            
        Returns:
            IncrementalLoadProgress: Migration progress and results
            
        Raises:
            IcebergEngineError: If Iceberg-specific operations fail
        """
        from ..monitoring.streaming_progress_tracker import StreamingProgressTracker, StreamingProgressConfig
        
        # Check engine types for enhanced processing
        source_is_iceberg = DatabaseEngineManager.is_iceberg_engine(source_config.engine_type)
        target_is_iceberg = DatabaseEngineManager.is_iceberg_engine(target_config.engine_type)
        
        # Get existing bookmark state first to include in start log
        bookmark_state = None
        try:
            bookmark_state = self.bookmark_manager.get_bookmark_state(table_name)
        except Exception:
            pass  # Will be handled later in the flow
        
        # Initialize StreamingProgressTracker if metrics publisher is available
        # Requirement 8.1: Use StreamingProgressTracker for incremental loads
        streaming_tracker = None
        if self.metrics_publisher is not None:
            # Use provided config or create default
            tracker_config = self.streaming_progress_config or StreamingProgressConfig()
            streaming_tracker = StreamingProgressTracker(
                table_name=table_name,
                load_type='incremental',  # Requirement 8.1: Set load_type to 'incremental'
                config=tracker_config,
                metrics_publisher=self.metrics_publisher,
                engine_type=target_config.engine_type  # Pass target engine type for Iceberg-aware tracking
            )
        
        # Requirement 5.1: Log migration start with engine types
        self.structured_logger.log_migration_start(
            table_name=table_name,
            load_type='incremental',
            source_engine=source_config.engine_type,
            target_engine=target_config.engine_type,
            source_is_iceberg=source_is_iceberg,
            target_is_iceberg=target_is_iceberg,
            incremental_column=bookmark_state.incremental_column if bookmark_state else None,
            last_processed_value=bookmark_state.last_processed_value if bookmark_state else None,
            streaming_progress_enabled=streaming_tracker is not None
        )
        
        try:
            # Get existing bookmark state (which should already be initialized in the main workflow)
            # Note: We already retrieved this above for logging, but get it again to ensure we have the latest state
            bookmark_state = self.bookmark_manager.get_bookmark_state(table_name)
            
            if bookmark_state is None:
                # This should not happen if the main workflow is working correctly
                raise RuntimeError(f"No bookmark state found for table {table_name}. The bookmark state should have been initialized in the main workflow before calling the incremental migrator.")
            
            # Log the bookmark state being used
            self.structured_logger.info(f"Using existing bookmark state for incremental migration: table={table_name}, strategy={bookmark_state.incremental_strategy}, column={bookmark_state.incremental_column}, is_manually_configured={getattr(bookmark_state, 'is_manually_configured', False)}")
            
            progress = IncrementalLoadProgress(
                table_name=table_name,
                incremental_strategy=bookmark_state.incremental_strategy,
                incremental_column=bookmark_state.incremental_column
            )
            progress.start_time = time.time()
            progress.status = 'in_progress'
            
            # Count incremental rows using SQL COUNT(*) with WHERE clause (fast)
            # This avoids reading all data just to count
            count_start_time = time.time()
            try:
                delta_rows, counting_method = self.counting_strategy.execute_incremental_count_sql_with_fallback(
                    source_config=source_config,
                    table_name=table_name,
                    connection_manager=self.connection_manager,
                    incremental_column=bookmark_state.incremental_column,
                    last_processed_value=bookmark_state.last_processed_value,
                    df=None,  # We'll read the DataFrame after counting
                    incremental_strategy=bookmark_state.incremental_strategy
                )
                count_duration = time.time() - count_start_time
                
                if counting_method == 'incremental_sql':
                    progress.delta_rows = delta_rows
                    self.structured_logger.info(
                        f"Counted {progress.delta_rows} incremental rows using SQL COUNT(*) for {table_name}",
                        incremental_column=bookmark_state.incremental_column,
                        last_processed_value=bookmark_state.last_processed_value,
                        delta_rows=progress.delta_rows,
                        count_duration_seconds=round(count_duration, 2),
                        counting_method="incremental_sql"
                    )
                else:
                    # SQL COUNT failed, we'll count after reading the DataFrame
                    progress.delta_rows = None
                    self.structured_logger.info(
                        f"SQL COUNT failed for {table_name}, will count after reading DataFrame",
                        incremental_column=bookmark_state.incremental_column,
                        counting_method="deferred_to_dataframe"
                    )
            except Exception as count_error:
                # SQL COUNT failed, we'll count after reading the DataFrame
                progress.delta_rows = None
                self.structured_logger.warning(
                    f"Incremental SQL COUNT failed for {table_name}, will count after reading",
                    error=str(count_error),
                    incremental_column=bookmark_state.incremental_column
                )
            
            # Read incremental data with engine-specific handling
            read_start_time = time.time()
            source_df = self._read_incremental_data_with_engine_support(
                source_config, table_name, bookmark_state, source_is_iceberg
            )
            
            # If SQL COUNT failed, fall back to DataFrame count
            if progress.delta_rows is None:
                progress.delta_rows = source_df.count()
                self.structured_logger.info(
                    f"Counted {progress.delta_rows} incremental rows using DataFrame count for {table_name}",
                    incremental_column=bookmark_state.incremental_column,
                    delta_rows=progress.delta_rows,
                    counting_method="dataframe_fallback"
                )
            
            read_duration = time.time() - read_start_time
            
            # Requirement 8.3: Log incremental data read with delta rows and bookmark state
            self.structured_logger.info(
                f"Read {progress.delta_rows} incremental rows from {table_name}",
                incremental_column=bookmark_state.incremental_column,
                last_processed_value=bookmark_state.last_processed_value,
                delta_rows=progress.delta_rows,
                read_duration_seconds=round(read_duration, 2)
            )
            
            # Publish read phase metrics
            if self.metrics_publisher:
                self.metrics_publisher.publish_migration_phase_metrics(
                    table_name=table_name,
                    phase='read',
                    load_type='incremental',
                    duration_seconds=read_duration,
                    rows_count=progress.delta_rows
                )
            
            # Requirement 8.1: Start streaming progress tracker with known delta rows
            if streaming_tracker is not None:
                try:
                    streaming_tracker.start_tracking(total_rows=progress.delta_rows)
                except Exception as tracker_error:
                    # Requirement 2.1: Continue migration even if progress tracking fails
                    self.structured_logger.warning(
                        f"Failed to start progress tracking for {table_name}, continuing migration",
                        table_name=table_name,
                        error=str(tracker_error),
                        error_type=type(tracker_error).__name__
                    )
            
            if progress.delta_rows > 0:
                # Write to target with engine-specific handling and progress tracking
                # Requirement 8.1: Integrate progress updates during incremental write operations
                write_start_time = time.time()
                self._write_incremental_data_with_engine_support(
                    source_df, target_config, table_name, target_is_iceberg,
                    streaming_tracker=streaming_tracker
                )
                write_duration = time.time() - write_start_time
                
                # Publish write phase metrics
                if self.metrics_publisher:
                    self.metrics_publisher.publish_migration_phase_metrics(
                        table_name=table_name,
                        phase='write',
                        load_type='incremental',
                        duration_seconds=write_duration,
                        rows_count=progress.delta_rows
                    )
                
                # Update bookmark with new maximum value
                new_max_value = self._get_new_bookmark_value(source_df, bookmark_state.incremental_column)
                self.bookmark_manager.update_bookmark_state(table_name, new_max_value, progress.delta_rows)
                
                self.structured_logger.info(
                    f"Updated bookmark for {table_name}",
                    new_max_value=new_max_value,
                    incremental_column=bookmark_state.incremental_column
                )
            
            progress.processed_rows = progress.delta_rows
            progress.end_time = time.time()
            progress.status = 'completed'
            
            # Requirement 8.1: Complete streaming progress tracker
            if streaming_tracker is not None:
                try:
                    streaming_tracker.complete_tracking(final_row_count=progress.delta_rows)
                except Exception as tracker_error:
                    # Requirement 2.1: Continue migration even if progress tracking fails
                    self.structured_logger.warning(
                        f"Failed to complete progress tracking for {table_name}, continuing migration",
                        table_name=table_name,
                        error=str(tracker_error),
                        error_type=type(tracker_error).__name__
                    )
            
            # Requirement 5.3, 8.3, 8.4: Log migration completion with metrics and bookmark update
            new_max_value = None
            bookmark_update_status = 'no_data'
            if progress.delta_rows > 0:
                # Get the new bookmark value for logging
                try:
                    new_max_value = self._get_new_bookmark_value(source_df, bookmark_state.incremental_column)
                    bookmark_update_status = 'success'
                except Exception as e:
                    bookmark_update_status = 'failed'
                    self.structured_logger.warning(
                        f"Failed to retrieve new bookmark value for logging: {str(e)}",
                        table_name=table_name,
                        incremental_column=bookmark_state.incremental_column
                    )
            
            self.structured_logger.log_migration_completion(
                table_name=table_name,
                load_type='incremental',
                total_rows=progress.processed_rows,
                duration_seconds=progress.duration_seconds,
                rows_per_second=progress.processed_rows / progress.duration_seconds if progress.duration_seconds > 0 else 0,
                source_engine=source_config.engine_type,
                target_engine=target_config.engine_type,
                incremental_column=bookmark_state.incremental_column,
                new_bookmark_value=new_max_value,
                delta_rows=progress.delta_rows,
                bookmark_update_status=bookmark_update_status
            )
            
            # Requirement 8.2, 8.4, 8.5: Publish incremental load metrics
            if self.metrics_publisher is not None:
                try:
                    processing_rate = progress.processed_rows / progress.duration_seconds if progress.duration_seconds > 0 else 0
                    self.metrics_publisher.publish_incremental_load_metrics(
                        table_name=table_name,
                        delta_rows=progress.delta_rows,
                        incremental_column=bookmark_state.incremental_column,
                        bookmark_update_status='success',
                        processing_rate=processing_rate,
                        duration_seconds=progress.duration_seconds
                    )
                    
                    # Publish migration success status
                    self.metrics_publisher.publish_migration_phase_metrics(
                        table_name=table_name,
                        phase='write',
                        load_type='incremental',
                        duration_seconds=0,
                        migration_status='success'
                    )
                except Exception as metrics_error:
                    # Don't fail migration if metrics publishing fails
                    self.structured_logger.warning(
                        f"Failed to publish incremental load metrics for {table_name}",
                        table_name=table_name,
                        error=str(metrics_error),
                        error_type=type(metrics_error).__name__
                    )
            
        except IcebergEngineError as e:
            progress.end_time = time.time()
            progress.status = 'failed'
            progress.error_message = f"Iceberg engine error: {str(e)}"
            
            # Requirement 5.4: Error logging with full context
            self.structured_logger.log_migration_error(
                table_name=table_name,
                load_type='incremental',
                error=str(e),
                error_type='IcebergEngineError',
                duration_seconds=time.time() - progress.start_time,
                rows_processed=progress.processed_rows,
                source_engine=source_config.engine_type,
                target_engine=target_config.engine_type,
                source_is_iceberg=source_is_iceberg,
                target_is_iceberg=target_is_iceberg,
                incremental_column=bookmark_state.incremental_column if bookmark_state else None,
                last_processed_value=bookmark_state.last_processed_value if bookmark_state else None
            )
            
            # Requirement 8.2, 8.4: Publish incremental load metrics even on failure
            if self.metrics_publisher is not None and bookmark_state is not None:
                try:
                    duration = time.time() - progress.start_time
                    processing_rate = progress.processed_rows / duration if duration > 0 else 0
                    self.metrics_publisher.publish_incremental_load_metrics(
                        table_name=table_name,
                        delta_rows=progress.delta_rows,
                        incremental_column=bookmark_state.incremental_column,
                        bookmark_update_status='failed',
                        processing_rate=processing_rate,
                        duration_seconds=duration
                    )
                    
                    # Publish migration failure status
                    self.metrics_publisher.publish_migration_phase_metrics(
                        table_name=table_name,
                        phase='write',
                        load_type='incremental',
                        duration_seconds=0,
                        migration_status='failed'
                    )
                except Exception:
                    pass  # Don't fail on metrics publishing
        except Exception as e:
            progress.end_time = time.time()
            progress.status = 'failed'
            progress.error_message = str(e)
            
            # Requirement 5.4: Error logging with full context
            self.structured_logger.log_migration_error(
                table_name=table_name,
                load_type='incremental',
                error=str(e),
                error_type=type(e).__name__,
                duration_seconds=time.time() - progress.start_time,
                rows_processed=progress.processed_rows,
                source_engine=source_config.engine_type,
                target_engine=target_config.engine_type,
                incremental_column=bookmark_state.incremental_column if bookmark_state else None,
                last_processed_value=bookmark_state.last_processed_value if bookmark_state else None
            )
            
            # Requirement 8.2, 8.4: Publish incremental load metrics even on failure
            if self.metrics_publisher is not None and bookmark_state is not None:
                try:
                    duration = time.time() - progress.start_time
                    processing_rate = progress.processed_rows / duration if duration > 0 else 0
                    self.metrics_publisher.publish_incremental_load_metrics(
                        table_name=table_name,
                        delta_rows=progress.delta_rows,
                        incremental_column=bookmark_state.incremental_column,
                        bookmark_update_status='failed',
                        processing_rate=processing_rate,
                        duration_seconds=duration
                    )
                    
                    # Publish migration failure status
                    self.metrics_publisher.publish_migration_phase_metrics(
                        table_name=table_name,
                        phase='write',
                        load_type='incremental',
                        duration_seconds=0,
                        migration_status='failed'
                    )
                except Exception:
                    pass  # Don't fail on metrics publishing
        
        return progress
    
    def _get_table_schema_with_engine_support(self, source_config: ConnectionConfig, 
                                            table_name: str, source_is_iceberg: bool):
        """
        Get table schema with engine-specific support.
        
        Args:
            source_config: Source connection configuration
            table_name: Name of the table
            source_is_iceberg: Whether source engine is Iceberg
            
        Returns:
            Table schema structure
            
        Raises:
            IcebergEngineError: If Iceberg schema retrieval fails
        """
        try:
            return self.connection_manager.get_table_schema(source_config, table_name)
        except Exception as e:
            if source_is_iceberg:
                raise IcebergEngineError(
                    f"Failed to retrieve Iceberg table schema for {table_name}: {str(e)}",
                    error_code="SCHEMA_RETRIEVAL_ERROR",
                    context={"table_name": table_name, "spark_error": str(e)}
                )
            else:
                raise
    
    def _read_incremental_data_with_engine_support(self, source_config: ConnectionConfig, 
                                                 table_name: str, bookmark_state, 
                                                 source_is_iceberg: bool) -> DataFrame:
        """
        Read incremental data with engine-specific support.
        
        Args:
            source_config: Source connection configuration
            table_name: Name of the table
            bookmark_state: Current bookmark state
            source_is_iceberg: Whether source engine is Iceberg
            
        Returns:
            DataFrame: Incremental data
            
        Raises:
            IcebergEngineError: If Iceberg read operations fail
        """
        try:
            if source_is_iceberg:
                # For Iceberg sources, use DataFrame filtering instead of SQL queries
                self.structured_logger.debug(f"Reading incremental data from Iceberg table: {table_name}")
                
                # Read full table first
                full_df = self.connection_manager.read_table(
                    connection_config=source_config,
                    table_name=table_name
                )
                
                # Apply incremental filter if bookmark exists
                if bookmark_state.last_processed_value:
                    incremental_df = full_df.filter(
                        col(bookmark_state.incremental_column) > bookmark_state.last_processed_value
                    )
                else:
                    incremental_df = full_df
                
                return incremental_df
            else:
                # For traditional databases, check if partitioned read is configured
                has_config = self.partitioned_read_config is not None
                config_enabled = has_config and self.partitioned_read_config.enabled
                
                # Check if partitioned read is configured for this table
                partition_config = None
                if self.partitioned_read_config:
                    partition_config = self.partitioned_read_config.get_table_config(table_name)
                
                should_partition = config_enabled and (partition_config is not None or self.partitioned_read_config.should_use_partitioned_read(table_name))
                
                self.structured_logger.info(
                    f"Incremental read strategy decision for {table_name}",
                    table_name=table_name,
                    has_partitioned_config=has_config,
                    partitioned_config_enabled=config_enabled,
                    has_table_specific_config=partition_config is not None,
                    should_use_partitioned_read=should_partition
                )
                
                if partition_config:
                    # Use partitioned read with explicit table config
                    return self._read_incremental_data_partitioned(
                        source_config, table_name, bookmark_state, partition_config
                    )
                elif config_enabled:
                    # Auto-detect partition column for incremental read
                    return self._read_incremental_data_with_auto_partition(
                        source_config, table_name, bookmark_state
                    )
                else:
                    self.structured_logger.info(f"Using standard JDBC read for incremental {table_name}", table_name=table_name)
                    # Use standard query-based read (single connection)
                    return self._read_incremental_data_with_query(
                        source_config, table_name, bookmark_state
                    )
                
        except Exception as e:
            if source_is_iceberg:
                raise IcebergEngineError(
                    f"Failed to read incremental data from Iceberg table {table_name}: {str(e)}",
                    error_code="INCREMENTAL_READ_ERROR",
                    context={"table_name": table_name, "spark_error": str(e)}
                )
            else:
                raise
    
    def _read_incremental_data_with_query(self, source_config: ConnectionConfig,
                                         table_name: str, bookmark_state) -> DataFrame:
        """Read incremental data using SQL query (single JDBC connection)."""
        if bookmark_state.last_processed_value:
            # Format the bookmark value properly for SQL query based on engine type
            formatted_value = self._format_bookmark_value_for_sql(
                bookmark_state.last_processed_value,
                source_config.engine_type,
                bookmark_state.incremental_column
            )
            query = f"SELECT * FROM {source_config.schema}.{table_name} WHERE {bookmark_state.incremental_column} > {formatted_value}"
        else:
            query = f"SELECT * FROM {source_config.schema}.{table_name}"
        
        self.structured_logger.info(
            f"Executing incremental data read query (single connection)",
            table_name=table_name,
            incremental_column=bookmark_state.incremental_column,
            last_processed_value=str(bookmark_state.last_processed_value)[:100] if bookmark_state.last_processed_value else None,
            formatted_value=formatted_value if bookmark_state.last_processed_value else None,
            query=query,
            read_mode="single_connection"
        )
        
        return self.connection_manager.read_table_data(source_config, table_name, query=query)
    
    def _read_incremental_data_partitioned(self, source_config: ConnectionConfig,
                                          table_name: str, bookmark_state,
                                          partition_config) -> DataFrame:
        """
        Read incremental data using partitioned JDBC read with subquery for parallel connections.
        
        This method uses a subquery with the WHERE clause as the dbtable option, which pushes
        the filter down to the database. Each parallel JDBC connection only reads the filtered
        incremental data, not the full table.
        
        Args:
            source_config: Source connection configuration
            table_name: Name of the table
            bookmark_state: Current bookmark state
            partition_config: Partition configuration for the table
            
        Returns:
            DataFrame: Incremental data read in parallel with filter pushed to database
        """
        from .partition_detector import PartitionColumnDetector
        
        partition_column = partition_config.partition_column
        num_partitions = partition_config.num_partitions
        lower_bound = partition_config.lower_bound
        upper_bound = partition_config.upper_bound
        fetch_size = partition_config.fetch_size
        
        # Build the WHERE clause for incremental filter
        where_clause = ""
        formatted_value = None
        if bookmark_state.last_processed_value:
            formatted_value = self._format_bookmark_value_for_sql(
                bookmark_state.last_processed_value,
                source_config.engine_type,
                bookmark_state.incremental_column
            )
            where_clause = f"WHERE {bookmark_state.incremental_column} > {formatted_value}"
        
        # Build subquery that will be used as dbtable - filter is pushed to database
        full_table_name = f"{source_config.schema}.{table_name}"
        if where_clause:
            # Use subquery syntax: (SELECT * FROM table WHERE condition) AS alias
            subquery = f"(SELECT * FROM {full_table_name} {where_clause}) AS incremental_subquery"
        else:
            subquery = full_table_name
        
        self.structured_logger.info(
            f"Using partitioned read with subquery for incremental load of {table_name}",
            table_name=table_name,
            partition_column=partition_column,
            num_partitions=num_partitions,
            incremental_column=bookmark_state.incremental_column,
            last_processed_value=str(bookmark_state.last_processed_value)[:100] if bookmark_state.last_processed_value else None,
            subquery=subquery,
            read_mode="partitioned_parallel_with_pushdown"
        )
        
        # Auto-detect bounds if not specified - query the filtered data for accurate bounds
        if lower_bound is None or upper_bound is None:
            self.structured_logger.info(
                f"Auto-detecting partition bounds for incremental data of {table_name}",
                table_name=table_name,
                partition_column=partition_column
            )
            
            # Get bounds from the incremental data subset
            bounds_query = f"SELECT MIN({partition_column}) as min_val, MAX({partition_column}) as max_val FROM {full_table_name}"
            if where_clause:
                bounds_query += f" {where_clause}"
            try:
                bounds_df = self.connection_manager.read_table_data(
                    source_config, table_name, 
                    query=bounds_query
                )
                bounds_row = bounds_df.collect()[0]
                if bounds_row['min_val'] is not None and bounds_row['max_val'] is not None:
                    lower_bound = int(bounds_row['min_val'])
                    upper_bound = int(bounds_row['max_val'])
                    self.structured_logger.info(
                        f"Detected partition bounds for incremental data",
                        table_name=table_name,
                        lower_bound=lower_bound,
                        upper_bound=upper_bound
                    )
                else:
                    # No incremental data found
                    self.structured_logger.info(
                        f"No incremental data found for {table_name}, returning empty DataFrame",
                        table_name=table_name
                    )
                    # Return empty DataFrame with correct schema
                    schema_query = f"SELECT * FROM {full_table_name} WHERE 1=0"
                    return self.connection_manager.read_table_data(source_config, table_name, query=schema_query)
            except Exception as e:
                self.structured_logger.warning(
                    f"Could not detect bounds for incremental data, falling back to query-based read",
                    table_name=table_name,
                    error=str(e)
                )
                return self._read_incremental_data_with_query(source_config, table_name, bookmark_state)
        
        # Ensure we have valid partitions
        if num_partitions == 0:
            num_partitions = 4  # Default for incremental loads
        
        self.structured_logger.info(
            f"Starting partitioned JDBC read for incremental load of {table_name}",
            table_name=table_name,
            partition_column=partition_column,
            lower_bound=lower_bound,
            upper_bound=upper_bound,
            num_partitions=num_partitions,
            fetch_size=fetch_size,
            filter_pushed_to_database=True
        )
        
        # Check if we should use Glue connection strategy
        if source_config.glue_connection_config and source_config.uses_glue_connection():
            properties = self.connection_manager.jdbc_manager.glue_connection_manager.setup_jdbc_with_glue_connection_strategy(source_config)
        else:
            properties = self.connection_manager.jdbc_manager.create_connection_properties(source_config)
        
        # Get URL from properties if available (from Glue connection), otherwise use connection_config
        url = properties.pop('url', source_config.connection_string)
        
        # Build reader with partitioning options using subquery as dbtable
        reader = self.spark.read.format('jdbc') \
            .option('url', url) \
            .option('dbtable', subquery) \
            .option('partitionColumn', partition_column) \
            .option('lowerBound', str(lower_bound)) \
            .option('upperBound', str(upper_bound)) \
            .option('numPartitions', str(num_partitions)) \
            .option('fetchsize', str(fetch_size))
        
        # Add connection properties (skip internal metadata keys)
        for key, value in properties.items():
            if not key.startswith('_'):
                reader = reader.option(key, str(value))
        
        df = reader.load()
        
        # Log completion with actual partition count
        actual_partitions = df.rdd.getNumPartitions()
        self.structured_logger.info(
            f"Partitioned JDBC read complete for incremental load of {table_name} | {actual_partitions} parallel connections used",
            table_name=table_name,
            partition_column=partition_column,
            requested_partitions=num_partitions,
            actual_partitions=actual_partitions,
            parallel_connections=actual_partitions,
            filter_pushed_to_database=True
        )
        
        return df
    
    def _read_incremental_data_with_auto_partition(
        self, 
        source_config: ConnectionConfig, 
        table_name: str, 
        bookmark_state
    ) -> DataFrame:
        """
        Read incremental data using the bookmark's incremental column for partitioning.
        
        This method attempts to use the bookmark's incremental column for parallel JDBC reads.
        However, JDBC partitioning only works with numeric columns. If the incremental column
        is a datetime/timestamp type, it falls back to standard single-connection read.
        
        For parallel reads with datetime incremental columns, use PartitionedReadConfig to
        explicitly specify a numeric partition column.
        
        Args:
            source_config: Source connection configuration
            table_name: Name of the table
            bookmark_state: Current bookmark state with incremental column info
            
        Returns:
            DataFrame: Incremental data (parallel if numeric column, single-connection otherwise)
        """
        # Use the bookmark's incremental column as the partition column
        incremental_column = bookmark_state.incremental_column
        
        if not incremental_column:
            self.structured_logger.warning(
                f"No incremental column in bookmark for {table_name}, using standard read",
                table_name=table_name
            )
            return self._read_incremental_data_with_query(source_config, table_name, bookmark_state)
        
        # Check if the incremental column value is numeric (required for JDBC partitioning)
        # JDBC partitioned reads require integer lowerBound/upperBound values
        last_value = bookmark_state.last_processed_value
        is_numeric_column = False
        
        if last_value is not None:
            # Check the type of the last processed value to determine column type
            is_numeric_column = isinstance(last_value, (int, float)) or (
                isinstance(last_value, str) and last_value.replace('-', '').replace('.', '').isdigit()
            )
        
        if not is_numeric_column:
            self.structured_logger.info(
                f"Incremental column '{incremental_column}' is not numeric (type: {type(last_value).__name__}), "
                f"falling back to standard read. To enable parallel reads with a datetime incremental column, "
                f"use PartitionedReadConfig to specify a numeric partition column.",
                table_name=table_name,
                incremental_column=incremental_column,
                column_value_type=type(last_value).__name__ if last_value else 'None',
                recommendation="Use PartitionedReadConfig with a numeric partition_column for parallel reads"
            )
            return self._read_incremental_data_with_query(source_config, table_name, bookmark_state)
        
        self.structured_logger.info(
            f"Using bookmark incremental column as partition column for {table_name}",
            table_name=table_name,
            partition_column=incremental_column,
            last_processed_value=str(last_value)[:100] if last_value else None
        )
        
        # Get bounds for the incremental column from the incremental data subset
        full_table_name = f"{source_config.schema}.{table_name}"
        where_clause = ""
        if last_value:
            formatted_value = self._format_bookmark_value_for_sql(
                last_value,
                source_config.engine_type,
                incremental_column
            )
            where_clause = f"WHERE {incremental_column} > {formatted_value}"
        
        bounds_query = f"SELECT MIN({incremental_column}) as min_val, MAX({incremental_column}) as max_val, COUNT(*) as row_count FROM {full_table_name}"
        if where_clause:
            bounds_query += f" {where_clause}"
        
        try:
            from ..config.database_engines import DatabaseEngineManager
            
            bounds_df = self.spark.read.format("jdbc") \
                .option("url", source_config.connection_string) \
                .option("user", source_config.username) \
                .option("password", source_config.password) \
                .option("driver", DatabaseEngineManager.get_driver_class(source_config.engine_type)) \
                .option("query", bounds_query) \
                .load()
            
            bounds_row = bounds_df.collect()[0]
            row_count = int(bounds_row['row_count']) if bounds_row['row_count'] else 0
            
            if row_count == 0:
                self.structured_logger.info(
                    f"No incremental data found for {table_name}, returning empty DataFrame",
                    table_name=table_name
                )
                # Return empty DataFrame with correct schema
                schema_query = f"SELECT * FROM {full_table_name} WHERE 1=0"
                return self.spark.read.format("jdbc") \
                    .option("url", source_config.connection_string) \
                    .option("user", source_config.username) \
                    .option("password", source_config.password) \
                    .option("driver", DatabaseEngineManager.get_driver_class(source_config.engine_type)) \
                    .option("query", schema_query) \
                    .load()
            
            lower_bound = int(bounds_row['min_val']) if bounds_row['min_val'] is not None else 0
            upper_bound = int(bounds_row['max_val']) if bounds_row['max_val'] is not None else 0
            
            # Calculate optimal partitions based on row count
            num_partitions = self.partitioned_read_config.default_num_partitions
            if num_partitions == 0:
                # Target ~500K rows per partition, min 2, max 100
                num_partitions = max(2, min(100, row_count // 500000)) if row_count > 0 else 2
            
            self.structured_logger.info(
                f"Incremental partition bounds detected for {table_name}",
                table_name=table_name,
                partition_column=incremental_column,
                lower_bound=lower_bound,
                upper_bound=upper_bound,
                row_count=row_count,
                num_partitions=num_partitions
            )
            
        except Exception as e:
            self.structured_logger.warning(
                f"Could not get bounds for incremental column, falling back to standard read: {str(e)}",
                table_name=table_name,
                error=str(e)
            )
            return self._read_incremental_data_with_query(source_config, table_name, bookmark_state)
        
        # Create partition config using the incremental column
        from ..config.partitioned_read_config import TablePartitionConfig
        auto_config = TablePartitionConfig(
            table_name=table_name,
            partition_column=incremental_column,
            num_partitions=num_partitions,
            fetch_size=self.partitioned_read_config.default_fetch_size,
            lower_bound=lower_bound,
            upper_bound=upper_bound
        )
        
        # Use the existing partitioned read method
        return self._read_incremental_data_partitioned(
            source_config, table_name, bookmark_state, auto_config
        )
    
    def _write_incremental_data_with_engine_support(self, df: DataFrame, target_config: ConnectionConfig,
                                                  table_name: str, target_is_iceberg: bool,
                                                  streaming_tracker: Optional['StreamingProgressTracker'] = None) -> None:
        """
        Write incremental data with engine-specific support and streaming progress tracking.
        
        Args:
            df: DataFrame to write
            target_config: Target connection configuration
            table_name: Name of the target table
            target_is_iceberg: Whether target engine is Iceberg
            streaming_tracker: Optional streaming progress tracker for real-time updates
            
        Raises:
            IcebergEngineError: If Iceberg write operations fail
        """
        try:
            # Requirement 8.1: Integrate progress updates during incremental write operations
            
            if target_is_iceberg:
                self.structured_logger.debug(f"Writing incremental data to Iceberg table: {table_name}")
                # For Iceberg targets, use standard write with partition-level progress tracking
                self._write_with_partition_progress(
                    df=df,
                    target_config=target_config,
                    table_name=table_name,
                    mode='append',
                    is_iceberg=True,
                    streaming_tracker=streaming_tracker
                )
            else:
                self.structured_logger.debug(f"Writing incremental data to JDBC table: {table_name}")
                # For traditional databases, use foreachPartition for granular progress tracking
                self._write_with_partition_progress(
                    df=df,
                    target_config=target_config,
                    table_name=table_name,
                    mode='append',
                    is_iceberg=False,
                    streaming_tracker=streaming_tracker
                )
        except Exception as e:
            if target_is_iceberg:
                raise IcebergEngineError(
                    f"Failed to write incremental data to Iceberg table {table_name}: {str(e)}",
                    error_code="INCREMENTAL_WRITE_ERROR",
                    context={"table_name": table_name, "spark_error": str(e)}
                )
            else:
                raise
    
    def _write_with_partition_progress(self, df: DataFrame, target_config: ConnectionConfig,
                                      table_name: str, mode: str, is_iceberg: bool,
                                      streaming_tracker: Optional['StreamingProgressTracker'] = None) -> None:
        """
        Write data with partition-level progress tracking.
        
        This method provides granular progress updates by tracking each partition as it's written.
        For JDBC targets, it uses foreachPartition to write and track progress incrementally.
        For Iceberg targets, it uses Spark progress polling to track write progress.
        
        Args:
            df: DataFrame to write
            target_config: Target connection configuration
            table_name: Name of the target table
            mode: Write mode ('append' or 'overwrite')
            is_iceberg: Whether target is Iceberg
            streaming_tracker: Optional streaming progress tracker for real-time updates
        """
        try:
            if is_iceberg:
                # For Iceberg, use Spark progress tracker with periodic polling
                self._write_iceberg_with_spark_progress(
                    df=df,
                    target_config=target_config,
                    table_name=table_name,
                    mode=mode,
                    streaming_tracker=streaming_tracker
                )
            else:
                # For JDBC, use foreachPartition for granular progress tracking
                self._write_jdbc_with_partition_progress(
                    df=df,
                    target_config=target_config,
                    table_name=table_name,
                    mode=mode,
                    streaming_tracker=streaming_tracker
                )
        except Exception as e:
            self.structured_logger.error(
                f"Failed to write data with partition progress for {table_name}",
                table_name=table_name,
                error=str(e),
                error_type=type(e).__name__
            )
            raise
    
    def _write_iceberg_with_spark_progress(self, df: DataFrame, target_config: ConnectionConfig,
                                          table_name: str, mode: str,
                                          streaming_tracker: Optional['StreamingProgressTracker'] = None) -> None:
        """
        Write Iceberg data with Spark progress tracking.
        
        This method uses SimpleSparkProgressTracker to poll Spark job progress
        during the Iceberg write operation, providing real-time visibility.
        
        Args:
            df: DataFrame to write
            target_config: Target connection configuration
            table_name: Name of the target table
            mode: Write mode ('append' or 'overwrite')
            streaming_tracker: Optional streaming progress tracker for real-time updates
        """
        from ..monitoring.spark_progress_listener import SimpleSparkProgressTracker
        
        # Get estimated row count for progress tracking
        total_rows_estimate = None
        if streaming_tracker and streaming_tracker.total_rows:
            total_rows_estimate = streaming_tracker.total_rows
        
        # Initialize Spark progress tracker
        spark_progress_tracker = None
        if streaming_tracker is not None:
            try:
                spark_progress_tracker = SimpleSparkProgressTracker(
                    spark_session=self.spark,
                    table_name=table_name,
                    streaming_tracker=streaming_tracker
                )
                spark_progress_tracker.start_tracking(total_rows_estimate=total_rows_estimate)
                
                self.structured_logger.info(
                    f"Started Spark progress tracking for Iceberg write",
                    table_name=table_name,
                    total_rows_estimate=total_rows_estimate
                )
            except Exception as tracker_error:
                self.structured_logger.warning(
                    f"Failed to start Spark progress tracker for {table_name}, continuing without progress tracking",
                    table_name=table_name,
                    error=str(tracker_error),
                    error_type=type(tracker_error).__name__
                )
                spark_progress_tracker = None
        
        try:
            # Repartition DataFrame for parallel writes if it has too few partitions
            current_partitions = df.rdd.getNumPartitions()
            
            self.structured_logger.info(
                f"DataFrame partition check for {table_name} (incremental)",
                table_name=table_name,
                current_partitions=current_partitions,
                total_rows_estimate=total_rows_estimate,
                streaming_tracker_available=streaming_tracker is not None
            )
            
            if total_rows_estimate and total_rows_estimate > 0:
                # Calculate optimal partition count based on data size
                # Target: ~500K rows per partition for good parallelism
                if total_rows_estimate > 5_000_000:
                    target_partitions = 20  # 20 parallel writes for very large datasets
                elif total_rows_estimate > 1_000_000:
                    target_partitions = 10  # 10 parallel writes for large datasets
                elif total_rows_estimate > 100_000:
                    target_partitions = 5   # 5 parallel writes for medium datasets
                else:
                    target_partitions = 2   # 2 parallel writes for small datasets
                
                # Only repartition if current partition count is too low
                if current_partitions < target_partitions:
                    self.structured_logger.info(
                        f"Repartitioning DataFrame for parallel writes",
                        table_name=table_name,
                        current_partitions=current_partitions,
                        target_partitions=target_partitions,
                        total_rows=total_rows_estimate
                    )
                    df = df.repartition(target_partitions)
                    
                    # Verify repartitioning worked
                    new_partitions = df.rdd.getNumPartitions()
                    self.structured_logger.info(
                        f"Repartitioning complete for {table_name}",
                        table_name=table_name,
                        previous_partitions=current_partitions,
                        new_partitions=new_partitions
                    )
                else:
                    self.structured_logger.info(
                        f"DataFrame already has sufficient partitions",
                        table_name=table_name,
                        current_partitions=current_partitions,
                        total_rows=total_rows_estimate
                    )
            else:
                # Fallback: If we don't have row count, use a default repartitioning for large reads
                if current_partitions == 1:
                    default_partitions = 10
                    self.structured_logger.info(
                        f"No row count available, applying default repartitioning for {table_name}",
                        table_name=table_name,
                        current_partitions=current_partitions,
                        default_partitions=default_partitions
                    )
                    df = df.repartition(default_partitions)
            
            # Perform Iceberg write
            self.connection_manager.write_table(
                df=df,
                connection_config=target_config,
                table_name=table_name,
                mode=mode
            )
            
            # Update progress after write completes using known total_rows (avoid df.count())
            if streaming_tracker is not None and total_rows_estimate is not None:
                try:
                    # Use the total_rows from streaming tracker to avoid re-scanning data
                    streaming_tracker.update_progress(total_rows_estimate)
                except Exception as update_error:
                    self.structured_logger.warning(
                        f"Failed to update streaming progress for {table_name}",
                        table_name=table_name,
                        error=str(update_error)
                    )
        
        finally:
            # Stop Spark progress tracker
            if spark_progress_tracker is not None:
                try:
                    spark_progress_tracker.complete_tracking()
                except Exception as tracker_error:
                    self.structured_logger.warning(
                        f"Failed to stop Spark progress tracker for {table_name}",
                        table_name=table_name,
                        error=str(tracker_error)
                    )
    
    def _write_jdbc_with_partition_progress(self, df: DataFrame, target_config: ConnectionConfig,
                                           table_name: str, mode: str,
                                           streaming_tracker: Optional['StreamingProgressTracker'] = None) -> None:
        """
        Write JDBC data with Spark progress polling (similar to Iceberg approach).
        
        This method uses SimpleSparkProgressTracker to poll Spark job progress
        during the JDBC write operation, providing real-time visibility without
        serialization issues.
        
        Args:
            df: DataFrame to write
            target_config: Target connection configuration
            table_name: Name of the target table
            mode: Write mode ('append' or 'overwrite')
            streaming_tracker: Optional streaming progress tracker for real-time updates
        """
        from ..monitoring.spark_progress_listener import SimpleSparkProgressTracker
        
        total_partitions = df.rdd.getNumPartitions()
        
        # Get estimated row count for progress tracking
        total_rows_estimate = None
        if streaming_tracker and streaming_tracker.total_rows:
            total_rows_estimate = streaming_tracker.total_rows
        
        self.structured_logger.info(
            f"Starting JDBC write with Spark progress tracking",
            table_name=table_name,
            total_partitions=total_partitions,
            mode=mode,
            total_rows_estimate=total_rows_estimate
        )
        
        # Initialize Spark progress tracker
        spark_progress_tracker = None
        if streaming_tracker is not None:
            try:
                spark_progress_tracker = SimpleSparkProgressTracker(
                    spark_session=self.spark,
                    table_name=table_name,
                    streaming_tracker=streaming_tracker
                )
                spark_progress_tracker.start_tracking(total_rows_estimate=total_rows_estimate)
                
                self.structured_logger.info(
                    f"Started Spark progress tracking for JDBC write",
                    table_name=table_name,
                    total_rows_estimate=total_rows_estimate
                )
            except Exception as tracker_error:
                self.structured_logger.warning(
                    f"Failed to start Spark progress tracker for {table_name}, continuing without progress tracking",
                    table_name=table_name,
                    error=str(tracker_error),
                    error_type=type(tracker_error).__name__
                )
                spark_progress_tracker = None
        
        try:
            # Repartition DataFrame for parallel writes if it has too few partitions
            current_partitions = df.rdd.getNumPartitions()
            
            if total_rows_estimate and total_rows_estimate > 0:
                # Calculate optimal partition count based on data size
                # Target: ~500K rows per partition for good parallelism
                if total_rows_estimate > 5_000_000:
                    target_partitions = 20  # 20 parallel writes for very large datasets
                elif total_rows_estimate > 1_000_000:
                    target_partitions = 10  # 10 parallel writes for large datasets
                elif total_rows_estimate > 100_000:
                    target_partitions = 5   # 5 parallel writes for medium datasets
                else:
                    target_partitions = 2   # 2 parallel writes for small datasets
                
                # Only repartition if current partition count is too low
                if current_partitions < target_partitions:
                    self.structured_logger.info(
                        f"Repartitioning DataFrame for parallel writes",
                        table_name=table_name,
                        current_partitions=current_partitions,
                        target_partitions=target_partitions,
                        total_rows=total_rows_estimate
                    )
                    df = df.repartition(target_partitions)
                else:
                    self.structured_logger.info(
                        f"DataFrame already has sufficient partitions",
                        table_name=table_name,
                        current_partitions=current_partitions,
                        total_rows=total_rows_estimate
                    )
            
            # Use standard JDBC write
            self.connection_manager.write_table_data(df, target_config, table_name, mode=mode)
            
            # Update progress after write completes using known total_rows (avoid df.count())
            if streaming_tracker is not None and total_rows_estimate is not None:
                try:
                    # Use the total_rows from streaming tracker to avoid re-scanning data
                    streaming_tracker.update_progress(total_rows_estimate)
                    
                    self.structured_logger.info(
                        f"Completed JDBC write with progress tracking",
                        table_name=table_name,
                        total_rows=total_rows_estimate,
                        total_partitions=total_partitions
                    )
                except Exception as update_error:
                    self.structured_logger.warning(
                        f"Failed to update streaming progress for {table_name}",
                        table_name=table_name,
                        error=str(update_error),
                        error_type=type(update_error).__name__
                    )
        
        finally:
            # Stop Spark progress tracker
            if spark_progress_tracker is not None:
                try:
                    spark_progress_tracker.complete_tracking()
                except Exception as tracker_error:
                    self.structured_logger.warning(
                        f"Failed to stop Spark progress tracker for {table_name}",
                        table_name=table_name,
                        error=str(tracker_error)
                    )
    
    def _get_new_bookmark_value(self, df: DataFrame, incremental_column: str):
        """
        Get the new bookmark value from the DataFrame.
        
        Args:
            df: DataFrame containing the data
            incremental_column: Name of the incremental column
            
        Returns:
            New maximum value for bookmark
        """
        try:
            # Use Spark's max function to get the maximum value
            max_value_row = df.agg(spark_max(col(incremental_column)).alias("max_value")).collect()[0]
            return max_value_row["max_value"]
        except Exception as e:
            self.structured_logger.warning(
                f"Failed to get new bookmark value using Spark max, falling back to original method: {str(e)}"
            )
            # Fallback to original method
            return df.agg({incremental_column: "max"}).collect()[0][f"max({incremental_column})"]
    
    def _format_bookmark_value_for_sql(self, value: Any, engine_type: str, column_name: str) -> str:
        """
        Format bookmark value for SQL query based on database engine type.
        
        This method handles proper formatting of datetime and other data types
        to prevent SQL conversion errors, especially for SQL Server datetime columns.
        
        Args:
            value: The bookmark value to format
            engine_type: Database engine type (sqlserver, oracle, postgresql, etc.)
            column_name: Name of the column (for logging purposes)
            
        Returns:
            Properly formatted SQL value string
        """
        from datetime import datetime, date
        
        # Handle None/NULL values
        if value is None:
            return "NULL"
        
        # Check if value is numeric (int or float) - return without quotes
        if isinstance(value, (int, float)):
            return str(value)
        
        # Convert to string if it's not already
        value_str = str(value).strip()
        
        # Check if the string value is purely numeric - return without quotes
        try:
            # Try to parse as float (handles both int and float strings)
            float(value_str)
            return value_str
        except ValueError:
            pass  # Not numeric, continue with other formatting
        
        # Check if the value looks like a corrupted datetime (multiple dates concatenated)
        # This is a defensive check for the bug where values get concatenated
        if len(value_str) > 30 and value_str.count('-') >= 4:
            self.structured_logger.warning(
                f"Detected potentially corrupted bookmark value for {column_name}: {value_str[:50]}...",
                column_name=column_name,
                value_length=len(value_str)
            )
            # Try to extract just the first datetime portion
            # Format: YYYY-MM-DD HH:MM:SS.ffffff
            # Look for the pattern where a second YYYY appears (4 digits starting with 20)
            try:
                import re
                # Find where the second date starts (look for pattern like "2025" after the first complete datetime)
                # A complete datetime with microseconds is: YYYY-MM-DD HH:MM:SS.ffffff (26 chars)
                # But the corruption happens right after microseconds, so look for a 4-digit year pattern
                match = re.search(r'(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}(?:\.\d+)?)\d{4}', value_str)
                if match:
                    value_str = match.group(1).strip()
                    self.structured_logger.info(
                        f"Extracted datetime portion using regex: {value_str}",
                        column_name=column_name
                    )
                else:
                    # Fallback: take first 26 characters
                    value_str = value_str[:26].strip()
                    self.structured_logger.info(
                        f"Extracted datetime portion using length: {value_str}",
                        column_name=column_name
                    )
            except Exception as e:
                self.structured_logger.error(
                    f"Failed to extract datetime from corrupted value: {str(e)}",
                    column_name=column_name
                )
        
        # For SQL Server, use CONVERT function for datetime values
        if engine_type.lower() == 'sqlserver':
            # Check if value looks like a datetime
            if any(char in value_str for char in ['-', ':', ' ']) and len(value_str) >= 10:
                # Use CONVERT with style 121 (ODBC canonical with milliseconds)
                # This is the most reliable format for SQL Server
                return f"CONVERT(DATETIME2, '{value_str}', 121)"
            else:
                # For non-datetime string values, use simple quoting
                return f"'{value_str}'"
        
        # For Oracle, use TO_TIMESTAMP for datetime values
        elif engine_type.lower() == 'oracle':
            if any(char in value_str for char in ['-', ':', ' ']) and len(value_str) >= 10:
                # Determine the format mask based on the value
                if '.' in value_str:
                    # Has fractional seconds
                    return f"TO_TIMESTAMP('{value_str}', 'YYYY-MM-DD HH24:MI:SS.FF')"
                else:
                    return f"TO_TIMESTAMP('{value_str}', 'YYYY-MM-DD HH24:MI:SS')"
            else:
                return f"'{value_str}'"
        
        # For PostgreSQL, use explicit casting
        elif engine_type.lower() == 'postgresql':
            if any(char in value_str for char in ['-', ':', ' ']) and len(value_str) >= 10:
                return f"'{value_str}'::timestamp"
            else:
                return f"'{value_str}'"
        
        # For other databases, use simple quoting
        else:
            return f"'{value_str}'"