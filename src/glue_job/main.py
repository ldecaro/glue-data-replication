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
from glue_job.config.job_config import JobConfig

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
    
    # Initialize managers with engine-aware configuration
    connection_manager = UnifiedConnectionManager(spark, glue_context)
    
    # Initialize bookmark manager with engine-aware JDBC paths
    # Iceberg engines don't use JDBC drivers, so set paths to None
    source_jdbc_path = None if source_is_iceberg else config.source_connection.jdbc_driver_path
    target_jdbc_path = None if target_is_iceberg else config.target_connection.jdbc_driver_path
    
    bookmark_manager = JobBookmarkManager(
        glue_context, 
        config.job_name, 
        None,  # Job will be set later
        source_jdbc_path=source_jdbc_path,
        target_jdbc_path=target_jdbc_path
    )
    
    # Initialize migrators with enhanced error handling for Iceberg
    full_migrator = FullLoadDataMigrator(spark, connection_manager)
    incremental_migrator = IncrementalDataMigrator(spark, connection_manager, bookmark_manager)
    
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
            
            # Handle schema detection with engine-specific logic
            try:
                schema = _get_table_schema_with_engine_support(
                    connection_manager, config.source_connection, table_name, source_is_iceberg
                )
                strategy_info = IncrementalColumnDetector.detect_incremental_strategy(schema, table_name)
            except IcebergEngineError as e:
                logger.error(f"Iceberg schema detection failed for {table_name}: {str(e)}")
                failed_tables += 1
                continue
            except Exception as e:
                logger.error(f"Schema detection failed for {table_name}: {str(e)}")
                failed_tables += 1
                continue
            
            # Initialize or use pre-loaded bookmark state with Iceberg support
            try:
                if table_name in bookmark_states and bookmark_states[table_name] is not None:
                    bookmark_state = JobBookmarkState.from_s3_dict(bookmark_states[table_name])
                    bookmark_manager.bookmark_states[table_name] = bookmark_state
                else:
                    bookmark_state = _initialize_bookmark_state_with_iceberg_support(
                        bookmark_manager, table_name, strategy_info, 
                        config.source_connection, source_is_iceberg
                    )
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
                        bookmark_manager.update_bookmark_state(table_name, "full_load_completed", progress.processed_rows)
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
    
    logger.info(f"Job completed: {successful_tables} successful, {failed_tables} failed")
    
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


def _initialize_bookmark_state_with_iceberg_support(bookmark_manager, table_name: str, strategy_info: Dict[str, Any], 
                                                   source_config, source_is_iceberg: bool):
    """
    Initialize bookmark state with Iceberg support.
    
    Args:
        bookmark_manager: Bookmark manager instance
        table_name: Name of the table
        strategy_info: Incremental strategy information
        source_config: Source connection configuration
        source_is_iceberg: Whether source is Iceberg engine
        
    Returns:
        Initialized bookmark state
    """
    if source_is_iceberg:
        # For Iceberg tables, try to use identifier-field-ids for bookmark management
        logger.debug(f"Initializing Iceberg bookmark state for {table_name}")
        try:
            # The bookmark manager should handle Iceberg-specific bookmark extraction
            return bookmark_manager.initialize_bookmark_state(
                table_name, strategy_info['strategy'], strategy_info['column']
            )
        except Exception as e:
            logger.warning(f"Failed to initialize Iceberg bookmark for {table_name}, falling back to traditional: {str(e)}")
            # Fallback to traditional bookmark management
            return bookmark_manager.initialize_bookmark_state(
                table_name, strategy_info['strategy'], strategy_info['column']
            )
    else:
        # For traditional databases, use standard bookmark initialization
        return bookmark_manager.initialize_bookmark_state(
            table_name, strategy_info['strategy'], strategy_info['column']
        )


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


def main() -> None:
    """Main entry point for Glue job execution with Iceberg support."""
    try:
        # Parse job arguments
        args = JobConfigurationParser.parse_job_arguments()
        job_config = JobConfigurationParser.create_job_config(args)
        JobConfigurationParser.validate_configuration(job_config)
        
        # Initialize Glue context and job
        glue_context, job = setup_glue_context(args)
        
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