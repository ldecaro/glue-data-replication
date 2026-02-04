#!/usr/bin/env python3
"""
Main entry point for AWS Glue Data Replication.

This module provides the main orchestration logic for the data replication process,
coordinating between configuration parsing, database connections, and data migration.
Enhanced with Iceberg engine support for modern data lake operations.
"""

import sys
import logging
import asyncio
from typing import Dict, Any, Tuple

# Conditional imports for AWS Glue and PySpark (only available in Glue runtime)
try:
    from awsglue.utils import getResolvedOptions
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from pyspark.context import SparkContext
    from pyspark.sql import SparkSession
except ImportError:
    # Mock classes for local development/testing
    class GlueContext:
        pass
    class Job:
        pass
    class SparkContext:
        pass
    class SparkSession:
        pass
    def getResolvedOptions(argv, options):
        return {opt: f"mock_{opt.lower()}" for opt in options}

# Import configuration modules
from glue_job.config.parsers import JobConfigurationParser
from glue_job.config.job_config import JobConfig, ConnectionConfig

# Import database modules
from glue_job.database.connection_manager import UnifiedConnectionManager
from glue_job.database.incremental_detector import IncrementalColumnDetector
from glue_job.database.migration import FullLoadDataMigrator, IncrementalDataMigrator
from glue_job.config.database_engines import DatabaseEngineManager

# Import storage modules
from glue_job.storage.bookmark_manager import JobBookmarkManager, JobBookmarkState

# Import monitoring modules
from glue_job.monitoring.logging import StructuredLogger

# Import utility modules
from glue_job.utils.s3_utils import EnhancedS3ParallelOperations

# Import Iceberg-specific modules
from glue_job.config.iceberg_models import IcebergEngineError, IcebergConnectionError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - [%(funcName)s:%(lineno)d] - %(message)s'
)
logger = logging.getLogger(__name__)


def setup_glue_context(args: Dict[str, Any]) -> Tuple[GlueContext, Job]:
    """
    Initialize Glue context and job.
    
    Args:
        args: Job arguments dictionary
        
    Returns:
        Tuple of (GlueContext, Job)
        
    Raises:
        RuntimeError: If Glue context initialization fails
    """
    try:
        # Initialize Spark context
        sc = SparkContext()
        
        # Initialize Glue context
        glue_context = GlueContext(sc)
        
        # Initialize Glue job with proper bookmark configuration
        job = Job(glue_context)
        job_name = args.get('JOB_NAME', 'glue-data-replication')
        job.init(job_name, {
            '--job-bookmark-option': 'job-bookmark-enable',
            '--enable-job-bookmark': 'true'
        })
        
        logger.info(f"Initialized Glue context for job: {job_name} with job bookmarks enabled")
        return glue_context, job
        
    except Exception as e:
        logger.error(f"Failed to initialize Glue context: {str(e)}")
        raise RuntimeError(f"Glue context initialization failed: {str(e)}")


def _validate_iceberg_configuration(spark: SparkSession, config: JobConfig) -> None:
    """
    Validate Iceberg configuration early to catch common issues.
    
    Args:
        spark: Spark session
        config: Job configuration
        
    Raises:
        RuntimeError: If Iceberg configuration is invalid
    """
    # Check if any engine is Iceberg
    source_is_iceberg = DatabaseEngineManager.is_iceberg_engine(config.source_connection.engine_type)
    target_is_iceberg = DatabaseEngineManager.is_iceberg_engine(config.target_connection.engine_type)
    
    if not (source_is_iceberg or target_is_iceberg):
        return  # No Iceberg engines, skip validation
    
    logger.info("Validating Iceberg configuration...")
    
    try:
        spark_conf = spark.conf
        
        # Check essential Spark extensions
        extensions = spark_conf.get("spark.sql.extensions", "")
        if "IcebergSparkSessionExtensions" not in extensions:
            error_msg = (
                "Iceberg Spark extensions not configured. "
                "Expected: org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions. "
                "This will cause 'Couldn't find a catalog to handle the identifier' errors."
            )
            logger.error(error_msg)
            raise RuntimeError(error_msg)
        
        # Check catalog configuration
        catalog_name = "glue_catalog"
        catalog_class = spark_conf.get(f"spark.sql.catalog.{catalog_name}", "")
        if not catalog_class:
            logger.warning(
                f"Iceberg catalog '{catalog_name}' not configured in Spark session. "
                f"Expected: spark.sql.catalog.{catalog_name}=org.apache.iceberg.spark.SparkCatalog. "
                f"The job will attempt to configure it dynamically."
            )
            # Don't fail here - let the connection handler configure it dynamically
        else:
            logger.info(f"✓ Iceberg catalog '{catalog_name}' is configured: {catalog_class}")
        
        # Validate warehouse locations
        if source_is_iceberg:
            source_config = config.source_connection.get_iceberg_config()
            if not source_config or not source_config.get('warehouse_location'):
                raise RuntimeError("Source Iceberg engine missing warehouse_location configuration")
        
        if target_is_iceberg:
            target_config = config.target_connection.get_iceberg_config()
            if not target_config or not target_config.get('warehouse_location'):
                raise RuntimeError("Target Iceberg engine missing warehouse_location configuration")
        
        logger.info("✓ Iceberg configuration validation passed")
        
    except Exception as e:
        logger.error(f"Iceberg configuration validation failed: {str(e)}")
        logger.error("This will likely cause table creation failures")
        logger.error("Please check your CloudFormation parameters and Glue job configuration")
        raise


def execute_migration_workflow(config: JobConfig, glue_context: GlueContext) -> None:
    """
    Execute the complete migration workflow with Iceberg engine support.
    
    Args:
        config: Job configuration
        glue_context: Initialized Glue context
        
    Raises:
        Exception: If migration workflow fails
        IcebergEngineError: If Iceberg-specific operations fail
    """
    spark = glue_context.spark_session
    
    # Log engine types for debugging and monitoring
    logger.info(f"Source engine: {config.source_connection.engine_type}")
    logger.info(f"Target engine: {config.target_connection.engine_type}")
    
    # Validate Iceberg configuration early if using Iceberg engines
    _validate_iceberg_configuration(spark, config)
    
    # Check if either source or target is Iceberg for engine-aware processing
    source_is_iceberg = DatabaseEngineManager.is_iceberg_engine(config.source_connection.engine_type)
    target_is_iceberg = DatabaseEngineManager.is_iceberg_engine(config.target_connection.engine_type)
    
    logger.info(f"Source is Iceberg: {source_is_iceberg}, Target is Iceberg: {target_is_iceberg}")
    
    # Validate engine compatibility and configuration
    try:
        _validate_engine_configuration(config, source_is_iceberg, target_is_iceberg)
    except Exception as e:
        logger.error(f"Engine configuration validation failed: {str(e)}")
        raise
    
    # Set connection roles for Glue Connection naming ({job_name}-source/target)
    if config.source_connection.glue_connection_config:
        config.source_connection.glue_connection_config.connection_role = "source"
    if config.target_connection.glue_connection_config:
        config.target_connection.glue_connection_config.connection_role = "target"
    
    # Initialize managers with engine-aware configuration and job_name for connection naming
    connection_manager = UnifiedConnectionManager(spark, glue_context, job_name=config.job_name)
    
    # Initialize bookmark manager with engine-aware JDBC paths
    # Iceberg engines don't use JDBC drivers, so set paths to None
    source_jdbc_path = None if source_is_iceberg else config.source_connection.jdbc_driver_path
    target_jdbc_path = None if target_is_iceberg else config.target_connection.jdbc_driver_path
    
    # Get manual bookmark configuration from job config
    manual_bookmark_config = config.manual_bookmark_config
    logger.info(f"Manual bookmark config from job config: {manual_bookmark_config}")
    
    bookmark_manager = JobBookmarkManager(
        glue_context, 
        config.job_name, 
        None,  # Job will be set later
        source_jdbc_path=source_jdbc_path,
        target_jdbc_path=target_jdbc_path,
        manual_bookmark_config=manual_bookmark_config
    )
    
    # Log the parsed manual configurations
    if hasattr(bookmark_manager, 'manual_bookmark_configs'):
        logger.info(f"Parsed manual bookmark configs: {list(bookmark_manager.manual_bookmark_configs.keys())}")
    else:
        logger.info("No manual_bookmark_configs attribute found on bookmark_manager")
    
    # Get migration performance configuration
    perf_config = config.migration_performance_config
    
    # Initialize CloudWatch metrics publisher if detailed metrics are enabled
    from glue_job.monitoring.metrics import CloudWatchMetricsPublisher
    metrics_publisher = None
    
    logger.info(f"Performance config - enable_detailed_metrics: {perf_config.enable_detailed_metrics}, "
               f"metrics_namespace: {perf_config.metrics_namespace}")
    
    if perf_config.enable_detailed_metrics:
        try:
            logger.info(f"Attempting to initialize CloudWatch metrics publisher for job: {config.job_name}")
            metrics_publisher = CloudWatchMetricsPublisher(
                job_name=config.job_name,
                namespace=perf_config.metrics_namespace
            )
            logger.info(f"✓ Successfully initialized CloudWatch metrics publisher with namespace: {perf_config.metrics_namespace}")
        except Exception as e:
            logger.error(f"✗ Failed to initialize CloudWatch metrics publisher: {str(e)}", exc_info=True)
            logger.warning("Continuing without metrics publishing")
            metrics_publisher = None
    else:
        logger.info("CloudWatch metrics publishing disabled by configuration")
    
    # Initialize counting strategy with configuration and metrics publisher
    from glue_job.database.counting_strategy import CountingStrategy
    counting_strategy = CountingStrategy(
        config=perf_config.get_counting_strategy_config(),
        metrics_publisher=metrics_publisher
    )
    
    # Initialize streaming progress configuration
    streaming_progress_config = perf_config.get_streaming_progress_config()
    
    # Log performance configuration being used
    logger.info(f"Using migration performance configuration: counting_strategy={perf_config.counting_strategy}, "
               f"progress_tracking={perf_config.enable_progress_tracking}, "
               f"detailed_metrics={perf_config.enable_detailed_metrics}")
    
    # Initialize partitioned read configuration for large dataset optimization
    from glue_job.config.partitioned_read_config import PartitionedReadConfig
    partitioned_read_config = config.partitioned_read_config
    
    if partitioned_read_config and partitioned_read_config.enabled:
        table_config_count = len(partitioned_read_config.table_configs)
        if table_config_count > 0:
            logger.info(f"Parallel JDBC reads ENABLED with explicit table configurations: "
                       f"tables_configured={list(partitioned_read_config.table_configs.keys())}, "
                       f"default_partitions={partitioned_read_config.default_num_partitions}, "
                       f"default_fetch_size={partitioned_read_config.default_fetch_size}")
        else:
            logger.info(f"Parallel JDBC reads ENABLED with auto-detection: "
                       f"default_partitions={partitioned_read_config.default_num_partitions} (0=auto-calculate), "
                       f"default_fetch_size={partitioned_read_config.default_fetch_size}. "
                       f"Partition columns will be auto-detected from primary keys or indexes.")
    else:
        logger.info("Parallel JDBC reads DISABLED (default). Using single-connection reads. "
                   "To enable parallel reads for large datasets, set EnablePartitionedReads=auto and optionally "
                   "configure PartitionedReadConfig. See docs/PARAMETER_REFERENCE.md for details.")
    
    # Initialize migrators with enhanced error handling for Iceberg and performance configuration
    full_migrator = FullLoadDataMigrator(
        spark, 
        connection_manager,
        counting_strategy=counting_strategy,
        metrics_publisher=metrics_publisher,
        streaming_progress_config=streaming_progress_config,
        partitioned_read_config=partitioned_read_config
    )
    incremental_migrator = IncrementalDataMigrator(
        spark, 
        connection_manager, 
        bookmark_manager,
        counting_strategy=counting_strategy,
        metrics_publisher=metrics_publisher,
        streaming_progress_config=streaming_progress_config,
        partitioned_read_config=partitioned_read_config
    )
    
    successful_tables = 0
    failed_tables = 0
    
    # Pre-load bookmark states for parallel processing if available
    bookmark_states = {}
    if (bookmark_manager.s3_enabled and 
        hasattr(bookmark_manager, 'enhanced_parallel_ops') and 
        bookmark_manager.enhanced_parallel_ops and 
        len(config.tables) > 1):
        
        logger.info(f"Pre-loading bookmark states for {len(config.tables)} tables using parallel S3 operations")
        try:
            bookmark_states = asyncio.run(
                bookmark_manager.enhanced_parallel_ops.read_bookmarks_chunked_parallel(
                    config.tables, chunk_size=10
                )
            )
            logger.info(f"Successfully pre-loaded {len([k for k, v in bookmark_states.items() if v is not None])} bookmark states from S3")
        except Exception as e:
            logger.warning(f"Failed to pre-load bookmark states in parallel: {e}. Falling back to individual reads.")
    
    # Process each table with enhanced error handling for Iceberg operations
    for table_name in config.tables:
        try:
            logger.info(f"Processing table: {table_name}")
            
            # Initialize or use pre-loaded bookmark state with manual configuration support
            try:
                if table_name in bookmark_states and bookmark_states[table_name] is not None:
                    logger.info(f"Using pre-loaded bookmark state for {table_name} from parallel S3 read")
                    bookmark_state = JobBookmarkState.from_s3_dict(bookmark_states[table_name])
                    logger.info(f"Pre-loaded bookmark for {table_name}: is_first_run={bookmark_state.is_first_run}, last_processed_value={bookmark_state.last_processed_value}")
                    bookmark_manager.bookmark_states[table_name] = bookmark_state
                else:
                    logger.info(f"No pre-loaded bookmark for {table_name}, will read from S3 via initialize_bookmark_state")
                    # Use the new auto-detection method that considers manual configuration
                    if source_is_iceberg:
                        # For Iceberg tables, use Iceberg-specific initialization
                        bookmark_state = _initialize_bookmark_state_with_iceberg_support_and_manual_config(
                            bookmark_manager, table_name, connection_manager, 
                            config.source_connection, source_is_iceberg
                        )
                    else:
                        # For JDBC tables, use manual configuration if available, otherwise fall back to automatic detection
                        logger.info(f"Checking manual configuration for {table_name}")
                        logger.info(f"Has manual_bookmark_configs: {hasattr(bookmark_manager, 'manual_bookmark_configs')}")
                        if hasattr(bookmark_manager, 'manual_bookmark_configs'):
                            logger.info(f"Manual configs available: {list(bookmark_manager.manual_bookmark_configs.keys())}")
                            logger.info(f"Table {table_name} in manual configs: {table_name in bookmark_manager.manual_bookmark_configs}")
                        
                        if (hasattr(bookmark_manager, 'manual_bookmark_configs') and 
                            table_name in bookmark_manager.manual_bookmark_configs):
                            # Use manual configuration
                            manual_config = bookmark_manager.manual_bookmark_configs[table_name]
                            column_name = manual_config.column_name
                            
                            # For manual config, assume primary_key strategy for ID columns, timestamp for others
                            if 'id' in column_name.lower() or column_name.lower().endswith('_id'):
                                strategy = 'primary_key'
                            elif any(keyword in column_name.lower() for keyword in ['date', 'time', 'timestamp', 'created', 'updated', 'modified']):
                                strategy = 'timestamp'
                            else:
                                strategy = 'hash'  # Safe fallback
                            
                            logger.info(f"Using manual bookmark configuration for {table_name}: column={column_name}, strategy={strategy}")
                            
                            bookmark_state = bookmark_manager.initialize_bookmark_state(
                                table_name=table_name,
                                incremental_strategy=strategy,
                                incremental_column=column_name,
                                database=config.source_connection.database,
                                engine_type=config.source_connection.engine_type
                            )
                        else:
                            # Fall back to automatic detection using existing logic
                            schema = _get_table_schema_with_engine_support(
                                connection_manager, config.source_connection, table_name, source_is_iceberg
                            )
                            strategy_info = IncrementalColumnDetector.detect_incremental_strategy(schema, table_name)
                            
                            bookmark_state = bookmark_manager.initialize_bookmark_state(
                                table_name=table_name,
                                incremental_strategy=strategy_info['strategy'],
                                incremental_column=strategy_info['column'],
                                database=config.source_connection.database,
                                engine_type=config.source_connection.engine_type
                            )
                    
                    # Log the bookmark state after initialization
                    logger.info(f"Bookmark state after initialize_bookmark_state for {table_name}: is_first_run={bookmark_state.is_first_run}, last_processed_value={bookmark_state.last_processed_value}")
            except Exception as e:
                logger.error(f"Bookmark state initialization failed for {table_name}: {str(e)}")
                failed_tables += 1
                continue
            
            # Execute migration based on bookmark state with enhanced error handling
            try:
                if bookmark_state.is_first_run:
                    logger.info(f"First run detected for {table_name} - performing full load")
                    progress = _perform_full_load_with_engine_support(
                        full_migrator, config, table_name, source_is_iceberg, target_is_iceberg
                    )
                    if progress.status == 'completed':
                        # After successful full load, set bookmark to actual max value of incremental column
                        # from target database to ensure we capture what was actually transferred
                        max_bookmark_value = _get_max_incremental_value_after_full_load(
                            connection_manager, config.target_connection, table_name, bookmark_state.incremental_column, target_is_iceberg
                        )
                        bookmark_manager.update_bookmark_state(table_name, max_bookmark_value, progress.processed_rows)
                else:
                    logger.info(f"Incremental load for {table_name} using bookmark: last_processed_value={bookmark_state.last_processed_value}")
                    progress = _perform_incremental_load_with_engine_support(
                        incremental_migrator, config, table_name, source_is_iceberg, target_is_iceberg
                    )
                
                # Track results
                if progress.status == 'completed':
                    successful_tables += 1
                    logger.info(f"Successfully processed table {table_name}: {progress.processed_rows} rows")
                else:
                    failed_tables += 1
                    logger.error(f"Failed to process table {table_name}: {progress.error_message}")
                    
            except IcebergEngineError as e:
                logger.error(f"Iceberg operation failed for table {table_name}: {str(e)}")
                failed_tables += 1
            except Exception as e:
                logger.error(f"Migration failed for table {table_name}: {str(e)}")
                failed_tables += 1
                
        except Exception as e:
            logger.error(f"Unexpected error processing table {table_name}: {e}")
            failed_tables += 1
    
    # Final batch operations for S3 bookmarks
    if (bookmark_manager.s3_enabled and 
        hasattr(bookmark_manager, 'enhanced_parallel_ops') and 
        bookmark_manager.enhanced_parallel_ops and 
        successful_tables > 0):
        
        try:
            final_bookmark_batch = {}
            for table_name in config.tables:
                if table_name in bookmark_manager.bookmark_states:
                    bookmark_state = bookmark_manager.bookmark_states[table_name]
                    final_bookmark_batch[table_name] = bookmark_state.to_s3_dict()
            
            if final_bookmark_batch:
                batch_results = asyncio.run(
                    bookmark_manager.enhanced_parallel_ops.write_bookmarks_batch_optimized(
                        final_bookmark_batch, batch_size=10
                    )
                )
                successful_writes = sum(1 for success in batch_results.values() if success)
                logger.info(f"Final S3 bookmark batch operation completed: {successful_writes}/{len(final_bookmark_batch)} successful")
                
        except Exception as e:
            logger.warning(f"Failed to perform final S3 bookmark batch operations: {e}")
    
    # Log bookmark detection summary for observability
    try:
        bookmark_manager.log_bookmark_detection_summary()
    except Exception as e:
        logger.warning(f"Failed to log bookmark detection summary: {e}")
    
    logger.info(f"Job completed: {successful_tables} successful, {failed_tables} failed")
    
    # Flush all buffered metrics to CloudWatch before job completion
    if metrics_publisher is not None:
        try:
            logger.info("Flushing buffered metrics to CloudWatch...")
            metrics_publisher.flush_metrics()
            logger.info("✓ Successfully flushed all metrics to CloudWatch")
        except Exception as e:
            logger.error(f"✗ Failed to flush metrics to CloudWatch: {str(e)}", exc_info=True)
    
    # Fail the job if no tables were successfully processed due to errors
    if successful_tables == 0 and failed_tables > 0:
        raise RuntimeError(f"Job failed: All {failed_tables} tables failed to process. No data was migrated.")
    
    # Fail the job if some tables failed (optional - you can comment this out if you want partial success)
    if failed_tables > 0:
        raise RuntimeError(f"Job partially failed: {failed_tables} out of {successful_tables + failed_tables} tables failed to process.")


def _validate_engine_configuration(config: JobConfig, source_is_iceberg: bool, target_is_iceberg: bool) -> None:
    """
    Validate engine configuration for compatibility and required parameters.
    
    Args:
        config: Job configuration
        source_is_iceberg: Whether source engine is Iceberg
        target_is_iceberg: Whether target engine is Iceberg
        
    Raises:
        ValueError: If configuration is invalid
        IcebergEngineError: If Iceberg configuration is invalid
    """
    # Validate Iceberg-specific configuration
    if source_is_iceberg:
        _validate_iceberg_connection_config(config.source_connection, "source")
    
    if target_is_iceberg:
        _validate_iceberg_connection_config(config.target_connection, "target")
    
    # Log configuration validation results
    logger.info("Engine configuration validation completed successfully")


def _validate_iceberg_connection_config(connection_config, connection_type: str) -> None:
    """
    Validate Iceberg connection configuration.
    
    Args:
        connection_config: Connection configuration to validate
        connection_type: Type of connection (source/target) for error messages
        
    Raises:
        IcebergEngineError: If Iceberg configuration is invalid
    """
    # Get Iceberg configuration from the connection config
    iceberg_config = connection_config.get_iceberg_config()
    if not iceberg_config:
        raise IcebergEngineError(
            f"Iceberg configuration is missing for {connection_type} engine",
            error_code="MISSING_ICEBERG_CONFIG",
            context={"connection_type": connection_type}
        )
    
    # Check for required Iceberg parameters
    warehouse_location = iceberg_config.get('warehouse_location')
    if not warehouse_location:
        raise IcebergEngineError(
            f"Warehouse location is required for Iceberg {connection_type} engine",
            error_code="MISSING_WAREHOUSE_LOCATION",
            context={"warehouse_location": warehouse_location or "None", "connection_type": connection_type}
        )
    
    # Validate warehouse location format (should be S3 path)
    if not warehouse_location.startswith('s3://'):
        raise IcebergEngineError(
            f"Warehouse location must be an S3 path for Iceberg {connection_type} engine",
            error_code="INVALID_WAREHOUSE_LOCATION",
            context={"warehouse_location": warehouse_location, "connection_type": connection_type}
        )
    
    logger.debug(f"Iceberg {connection_type} configuration validated successfully")


def _get_table_schema_with_engine_support(connection_manager, source_config, table_name: str, source_is_iceberg: bool):
    """
    Get table schema with engine-specific support.
    
    Args:
        connection_manager: Connection manager instance
        source_config: Source connection configuration
        table_name: Name of the table
        source_is_iceberg: Whether source is Iceberg engine
        
    Returns:
        Table schema structure
        
    Raises:
        IcebergEngineError: If Iceberg schema retrieval fails
    """
    try:
        if source_is_iceberg:
            # For Iceberg tables, use the connection manager's Iceberg-aware schema retrieval
            logger.debug(f"Retrieving Iceberg table schema for {table_name}")
            return connection_manager.get_table_schema(source_config, table_name)
        else:
            # For traditional databases, use standard schema retrieval
            logger.debug(f"Retrieving JDBC table schema for {table_name}")
            return connection_manager.get_table_schema(source_config, table_name)
    except Exception as e:
        if source_is_iceberg:
            raise IcebergEngineError(
                f"Failed to retrieve Iceberg table schema for {table_name}: {str(e)}",
                error_code="SCHEMA_RETRIEVAL_ERROR",
                context={"table_name": table_name, "spark_error": str(e)}
            )
        else:
            raise


def _initialize_bookmark_state_with_iceberg_support_and_manual_config(bookmark_manager, table_name: str, 
                                                                     connection_manager, source_config, source_is_iceberg: bool):
    """
    Initialize bookmark state with Iceberg support and manual configuration.
    
    Args:
        bookmark_manager: Bookmark manager instance
        table_name: Name of the table
        connection_manager: Connection manager for getting connections
        source_config: Source connection configuration
        source_is_iceberg: Whether source is Iceberg engine
        
    Returns:
        Initialized bookmark state
    """
    if source_is_iceberg:
        # For Iceberg tables, check for manual configuration first
        if (hasattr(bookmark_manager, 'manual_bookmark_configs') and 
            table_name in bookmark_manager.manual_bookmark_configs):
            # Use manual configuration for Iceberg
            manual_config = bookmark_manager.manual_bookmark_configs[table_name]
            column_name = manual_config.column_name
            
            # For manual config, assume primary_key strategy for ID columns, timestamp for others
            if 'id' in column_name.lower() or column_name.lower().endswith('_id'):
                strategy = 'primary_key'
            elif any(keyword in column_name.lower() for keyword in ['date', 'time', 'timestamp', 'created', 'updated', 'modified']):
                strategy = 'timestamp'
            else:
                strategy = 'hash'  # Safe fallback
            
            logger.info(f"Using manual bookmark configuration for Iceberg table {table_name}: column={column_name}, strategy={strategy}")
            
            return bookmark_manager.initialize_bookmark_state(
                table_name=table_name,
                incremental_strategy=strategy,
                incremental_column=column_name,
                database=source_config.database,
                engine_type=source_config.engine_type
            )
        else:
            # For Iceberg without manual config, use hash strategy as fallback
            logger.info(f"No manual configuration for Iceberg table {table_name}, using hash strategy")
            return bookmark_manager.initialize_bookmark_state(
                table_name=table_name,
                incremental_strategy='hash',
                incremental_column=None,
                database=source_config.database,
                engine_type=source_config.engine_type
            )
    else:
        # This shouldn't be called for non-Iceberg engines, but handle it gracefully
        logger.warning(f"_initialize_bookmark_state_with_iceberg_support_and_manual_config called for non-Iceberg engine")
        # Use the same logic as the main JDBC path
        if (hasattr(bookmark_manager, 'manual_bookmark_configs') and 
            table_name in bookmark_manager.manual_bookmark_configs):
            manual_config = bookmark_manager.manual_bookmark_configs[table_name]
            column_name = manual_config.column_name
            
            if 'id' in column_name.lower() or column_name.lower().endswith('_id'):
                strategy = 'primary_key'
            elif any(keyword in column_name.lower() for keyword in ['date', 'time', 'timestamp', 'created', 'updated', 'modified']):
                strategy = 'timestamp'
            else:
                strategy = 'hash'
            
            return bookmark_manager.initialize_bookmark_state(
                table_name=table_name,
                incremental_strategy=strategy,
                incremental_column=column_name,
                database=source_config.database,
                engine_type=source_config.engine_type
            )
        else:
            # Fall back to automatic detection
            schema = _get_table_schema_with_engine_support(
                connection_manager, source_config, table_name, False
            )
            strategy_info = IncrementalColumnDetector.detect_incremental_strategy(schema, table_name)
            
            return bookmark_manager.initialize_bookmark_state(
                table_name=table_name,
                incremental_strategy=strategy_info['strategy'],
                incremental_column=strategy_info['column'],
                database=source_config.database,
                engine_type=source_config.engine_type
            )


def _get_max_incremental_value_after_full_load(connection_manager, target_config: ConnectionConfig, table_name: str, 
                                             incremental_column: str, target_is_iceberg: bool):
    """
    Get the maximum value of the incremental column from the target database after a full load.
    
    This function queries the target database to get the actual maximum value that was 
    transferred during the full load. This ensures:
    1. We capture what was actually migrated to the target
    2. We avoid conflicts with records inserted in source during transfer
    3. Subsequent incremental loads start from the correct position
    
    Args:
        connection_manager: Existing connection manager instance
        target_config: Target database connection configuration
        table_name: Name of the table
        incremental_column: Name of the incremental column
        target_is_iceberg: Whether target engine is Iceberg
        
    Returns:
        Maximum value of the incremental column from the target database (as string with full precision)
    """
    try:
        if target_is_iceberg:
            # For Iceberg targets, use Spark SQL to get max value with full timestamp precision
            # This avoids the warehousePath issue with read_table()
            iceberg_config = target_config.get_iceberg_config()
            if iceberg_config:
                database = iceberg_config.get('database_name', target_config.database)
                catalog_name = iceberg_config.get('catalog_name', 'glue_catalog')
                full_table_name = f"{catalog_name}.{database}.{table_name}"
                
                # Use CAST to string to preserve full timestamp precision
                query = f"SELECT CAST(MAX({incremental_column}) AS STRING) as max_value FROM {full_table_name}"
                logger.info(f"Executing Iceberg max value query: {query}")
                
                spark = connection_manager.spark
                result_df = spark.sql(query)
                max_value = result_df.collect()[0]["max_value"]
            else:
                # Fallback if no Iceberg config - this shouldn't happen
                logger.warning(f"No Iceberg config found for {table_name}, returning None")
                return None
        else:
            # For traditional databases, use SQL query to get max value from target
            # Cast to string to preserve full timestamp precision
            engine_type = target_config.engine_type.lower()
            
            if engine_type == 'sqlserver':
                # SQL Server: Use CONVERT to get full precision string
                query = f"SELECT CONVERT(VARCHAR(30), MAX({incremental_column}), 121) as max_value FROM {target_config.schema}.{table_name}"
            elif engine_type == 'oracle':
                # Oracle: Use TO_CHAR for full precision
                query = f"SELECT TO_CHAR(MAX({incremental_column}), 'YYYY-MM-DD HH24:MI:SS.FF6') as max_value FROM {target_config.schema}.{table_name}"
            elif engine_type == 'postgresql':
                # PostgreSQL: Cast to text
                query = f"SELECT MAX({incremental_column})::text as max_value FROM {target_config.schema}.{table_name}"
            else:
                # Default: standard MAX query
                query = f"SELECT MAX({incremental_column}) as max_value FROM {target_config.schema}.{table_name}"
            
            logger.info(f"Executing max value query: {query}")
            result_df = connection_manager.read_table_data(target_config, table_name, query=query)
            max_value = result_df.collect()[0]["max_value"]
            
            # Ensure it's a string
            if max_value is not None and not isinstance(max_value, str):
                max_value = str(max_value)
        
        logger.info(f"Retrieved max incremental value from target {table_name}.{incremental_column}: {max_value}")
        return max_value
        
    except Exception as e:
        logger.error(f"Failed to get max incremental value from target {table_name}.{incremental_column}: {str(e)}")
        # Fallback to a safe default that won't cause SQL conversion errors
        # Return None so the next incremental load will do a full scan
        logger.warning(f"Falling back to None bookmark value for {table_name} - next run will perform full scan")
        return None


def _perform_full_load_with_engine_support(full_migrator, config: JobConfig, table_name: str, 
                                         source_is_iceberg: bool, target_is_iceberg: bool):
    """
    Perform full load migration with engine-specific support.
    
    Args:
        full_migrator: Full load migrator instance
        config: Job configuration
        table_name: Name of the table
        source_is_iceberg: Whether source is Iceberg engine
        target_is_iceberg: Whether target is Iceberg engine
        
    Returns:
        Migration progress result
        
    Raises:
        IcebergEngineError: If Iceberg operations fail
    """
    try:
        logger.info(f"Performing full load migration for {table_name} (source_iceberg={source_is_iceberg}, target_iceberg={target_is_iceberg})")
        
        # The migrator should handle engine-specific operations internally
        return full_migrator.perform_full_load_migration(
            config.source_connection,
            config.target_connection,
            table_name
        )
    except Exception as e:
        if source_is_iceberg or target_is_iceberg:
            raise IcebergEngineError(
                f"Iceberg full load migration failed for {table_name}: {str(e)}",
                error_code="FULL_LOAD_ERROR",
                context={"table_name": table_name, "spark_error": str(e)}
            )
        else:
            raise


def _perform_incremental_load_with_engine_support(incremental_migrator, config: JobConfig, table_name: str,
                                                source_is_iceberg: bool, target_is_iceberg: bool):
    """
    Perform incremental load migration with engine-specific support.
    
    Args:
        incremental_migrator: Incremental migrator instance
        config: Job configuration
        table_name: Name of the table
        source_is_iceberg: Whether source is Iceberg engine
        target_is_iceberg: Whether target is Iceberg engine
        
    Returns:
        Migration progress result
        
    Raises:
        IcebergEngineError: If Iceberg operations fail
    """
    try:
        logger.info(f"Performing incremental load migration for {table_name} (source_iceberg={source_is_iceberg}, target_iceberg={target_is_iceberg})")
        
        # The migrator should handle engine-specific operations internally
        return incremental_migrator.perform_incremental_load_migration(
            config.source_connection,
            config.target_connection,
            table_name
        )
    except Exception as e:
        if source_is_iceberg or target_is_iceberg:
            raise IcebergEngineError(
                f"Iceberg incremental load migration failed for {table_name}: {str(e)}",
                error_code="INCREMENTAL_LOAD_ERROR",
                context={"table_name": table_name, "spark_error": str(e)}
            )
        else:
            raise


def _setup_kerberos_environment_if_needed(job_config: JobConfig) -> None:
    """Set up Kerberos environment for source and/or target connections if configured.
    
    This MUST be called before any JDBC connections are made to ensure the krb5.conf
    file exists and Java system properties are set for Kerberos authentication.
    
    Args:
        job_config: Job configuration containing connection configs
    """
    from glue_job.config.kerberos_environment import KerberosEnvironmentManager
    
    kerberos_manager = KerberosEnvironmentManager()
    
    # Set up Kerberos for source connection if configured
    if job_config.source_connection.uses_kerberos_authentication():
        kerberos_config = job_config.source_connection.get_kerberos_config()
        logger.info(f"Setting up Kerberos environment for SOURCE connection: domain={kerberos_config.domain}, kdc={kerberos_config.kdc}")
        
        kerberos_manager.setup_kerberos_environment(
            kerberos_config=kerberos_config,
            username=job_config.source_connection.username,
            password=job_config.source_connection.password,
            keytab_s3_path=job_config.source_connection.kerberos_keytab_s3_path
        )
        logger.info("✓ Kerberos environment setup completed for SOURCE connection")
    
    # Set up Kerberos for target connection if configured (and different from source)
    if job_config.target_connection.uses_kerberos_authentication():
        target_kerberos_config = job_config.target_connection.get_kerberos_config()
        
        # Check if target uses a different Kerberos realm than source
        source_kerberos_config = job_config.source_connection.get_kerberos_config()
        if source_kerberos_config is None or target_kerberos_config.domain != source_kerberos_config.domain:
            logger.info(f"Setting up Kerberos environment for TARGET connection: domain={target_kerberos_config.domain}, kdc={target_kerberos_config.kdc}")
            
            kerberos_manager.setup_kerberos_environment(
                kerberos_config=target_kerberos_config,
                username=job_config.target_connection.username,
                password=job_config.target_connection.password,
                keytab_s3_path=job_config.target_connection.kerberos_keytab_s3_path
            )
            logger.info("✓ Kerberos environment setup completed for TARGET connection")
        else:
            logger.info("TARGET connection uses same Kerberos realm as SOURCE - reusing environment")


def main() -> None:
    """Main entry point for Glue job execution with Iceberg support."""
    try:
        # Parse job arguments
        args = JobConfigurationParser.parse_job_arguments()
        job_config = JobConfigurationParser.create_job_config(args)
        JobConfigurationParser.validate_configuration(job_config)
        
        # Initialize Glue context and job
        glue_context, job = setup_glue_context(args)
        
        # CRITICAL: Set up Kerberos environment BEFORE any database connections
        # This creates krb5.conf and sets Java system properties needed for Kerberos auth
        _setup_kerberos_environment_if_needed(job_config)
        
        # Execute migration workflow with Iceberg support
        execute_migration_workflow(job_config, glue_context)
        
        # Commit job bookmark
        job.commit()
        
    except IcebergEngineError as e:
        logger.error(f"Iceberg engine error: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Job failed: {str(e)}")
        raise


if __name__ == "__main__":
    main()