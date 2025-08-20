#!/usr/bin/env python3
"""
Main entry point for AWS Glue Data Replication.

This module provides the main orchestration logic for the data replication process,
coordinating between configuration parsing, database connections, and data migration.
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
from glue_job.database.connection_manager import JdbcConnectionManager
from glue_job.database.incremental_detector import IncrementalColumnDetector
from glue_job.database.migration import FullLoadDataMigrator, IncrementalDataMigrator

# Import storage modules
from glue_job.storage.bookmark_manager import JobBookmarkManager, JobBookmarkState

# Import monitoring modules
from glue_job.monitoring.logging import StructuredLogger

# Import utility modules
from glue_job.utils.s3_utils import EnhancedS3ParallelOperations

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
    Execute the complete migration workflow.
    
    Args:
        config: Job configuration
        glue_context: Initialized Glue context
        
    Raises:
        Exception: If migration workflow fails
    """
    spark = glue_context.spark_session
    
    # Initialize managers
    connection_manager = JdbcConnectionManager(spark, glue_context)
    bookmark_manager = JobBookmarkManager(
        glue_context, 
        config.job_name, 
        None,  # Job will be set later
        source_jdbc_path=config.source_connection.jdbc_driver_path,
        target_jdbc_path=config.target_connection.jdbc_driver_path
    )
    
    # Initialize migrators
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
    
    # Process each table
    for table_name in config.tables:
        try:
            logger.info(f"Processing table: {table_name}")
            
            # Auto-detect incremental strategy
            schema = connection_manager.get_table_schema(config.source_connection, table_name)
            strategy_info = IncrementalColumnDetector.detect_incremental_strategy(schema, table_name)
            
            # Initialize or use pre-loaded bookmark state
            if table_name in bookmark_states and bookmark_states[table_name] is not None:
                bookmark_state = JobBookmarkState.from_s3_dict(bookmark_states[table_name])
                bookmark_manager.bookmark_states[table_name] = bookmark_state
            else:
                bookmark_state = bookmark_manager.initialize_bookmark_state(
                    table_name, strategy_info['strategy'], strategy_info['column']
                )
            
            # Execute migration based on bookmark state
            if bookmark_state.is_first_run:
                logger.info(f"First run detected for {table_name} - performing full load")
                progress = full_migrator.perform_full_load_migration(
                    config.source_connection,
                    config.target_connection,
                    table_name
                )
                if progress.status == 'completed':
                    bookmark_manager.update_bookmark_state(table_name, "full_load_completed", progress.processed_rows)
            else:
                logger.info(f"Incremental load for {table_name} using bookmark: last_processed_value={bookmark_state.last_processed_value}")
                progress = incremental_migrator.perform_incremental_load_migration(
                    config.source_connection,
                    config.target_connection,
                    table_name
                )
            
            # Track results
            if progress.status == 'completed':
                successful_tables += 1
                logger.info(f"Successfully processed table {table_name}: {progress.processed_rows} rows")
            else:
                failed_tables += 1
                logger.error(f"Failed to process table {table_name}: {progress.error_message}")
                
        except Exception as e:
            logger.error(f"Failed to process table {table_name}: {e}")
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


def main() -> None:
    """Main entry point for Glue job execution."""
    try:
        # Parse job arguments
        args = JobConfigurationParser.parse_job_arguments()
        job_config = JobConfigurationParser.create_job_config(args)
        JobConfigurationParser.validate_configuration(job_config)
        
        # Initialize Glue context and job
        glue_context, job = setup_glue_context(args)
        
        # Execute migration workflow
        execute_migration_workflow(job_config, glue_context)
        
        # Commit job bookmark
        job.commit()
        
    except Exception as e:
        logger.error(f"Job failed: {str(e)}")
        raise


if __name__ == "__main__":
    main()