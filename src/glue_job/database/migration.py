"""
Data migration operations for AWS Glue Data Replication.

This module provides full-load and incremental data migration capabilities
for database replication operations.
"""

import time
import logging
from typing import Dict, Any
# Conditional imports for PySpark (only available in Glue runtime)
try:
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col
except ImportError:
    # Mock classes for local development/testing
    class SparkSession:
        pass
    def col(name):
        return name

# Import from other modules
from ..config.job_config import ConnectionConfig
from ..monitoring.progress import FullLoadProgress, IncrementalLoadProgress
from ..monitoring.logging import StructuredLogger
from ..storage.bookmark_manager import JobBookmarkManager
from .connection_manager import JdbcConnectionManager
from .incremental_detector import IncrementalColumnDetector

logger = logging.getLogger(__name__)


class FullLoadDataMigrator:
    """Handles full-load data migration operations."""
    
    def __init__(self, spark_session: SparkSession, connection_manager: JdbcConnectionManager):
        self.spark = spark_session
        self.connection_manager = connection_manager
        self.structured_logger = StructuredLogger("FullLoadDataMigrator")
    
    def perform_full_load_migration(self, source_config: ConnectionConfig, 
                                  target_config: ConnectionConfig, table_name: str) -> FullLoadProgress:
        """Perform full-load migration for a table."""
        progress = FullLoadProgress(table_name=table_name)
        progress.start_time = time.time()
        progress.status = 'in_progress'
        
        try:
            # Read all data from source
            source_df = self.connection_manager.read_table_data(source_config, table_name)
            progress.total_rows = source_df.count()
            
            # Write to target
            self.connection_manager.write_table_data(source_df, target_config, table_name, mode='overwrite')
            
            progress.processed_rows = progress.total_rows
            progress.end_time = time.time()
            progress.status = 'completed'
            
            self.structured_logger.info(f"Full load completed for {table_name}", 
                                      rows_processed=progress.processed_rows,
                                      duration=progress.duration_seconds)
            
        except Exception as e:
            progress.end_time = time.time()
            progress.status = 'failed'
            progress.error_message = str(e)
            self.structured_logger.error(f"Full load failed for {table_name}", error=str(e))
        
        return progress


class IncrementalDataMigrator:
    """Handles incremental data migration operations."""
    
    def __init__(self, spark_session: SparkSession, connection_manager: JdbcConnectionManager, 
                 bookmark_manager: JobBookmarkManager):
        self.spark = spark_session
        self.connection_manager = connection_manager
        self.bookmark_manager = bookmark_manager
        self.structured_logger = StructuredLogger("IncrementalDataMigrator")
    
    def perform_incremental_load_migration(self, source_config: ConnectionConfig, 
                                         target_config: ConnectionConfig, table_name: str) -> IncrementalLoadProgress:
        """Perform incremental migration for a table."""
        # Auto-detect incremental strategy
        schema = self.connection_manager.get_table_schema(source_config, table_name)
        strategy_info = IncrementalColumnDetector.detect_incremental_strategy(schema, table_name)
        
        # Initialize bookmark state with detected strategy
        bookmark_state = self.bookmark_manager.initialize_bookmark_state(
            table_name, strategy_info['strategy'], strategy_info['column']
        )
        
        progress = IncrementalLoadProgress(
            table_name=table_name,
            incremental_strategy=bookmark_state.incremental_strategy,
            incremental_column=bookmark_state.incremental_column
        )
        progress.start_time = time.time()
        progress.status = 'in_progress'
        
        try:
            # Build incremental query
            if bookmark_state.last_processed_value:
                query = f"SELECT * FROM {source_config.schema}.{table_name} WHERE {bookmark_state.incremental_column} > '{bookmark_state.last_processed_value}'"
            else:
                query = f"SELECT * FROM {source_config.schema}.{table_name}"
            
            # Read incremental data
            source_df = self.connection_manager.read_table_data(source_config, table_name, query=query)
            progress.delta_rows = source_df.count()
            
            if progress.delta_rows > 0:
                # Write to target
                self.connection_manager.write_table_data(source_df, target_config, table_name, mode='append')
                
                # Update bookmark
                new_max_value = source_df.agg({bookmark_state.incremental_column: "max"}).collect()[0][f"max({bookmark_state.incremental_column})"]
                self.bookmark_manager.update_bookmark_state(table_name, new_max_value, progress.delta_rows)
            
            progress.processed_rows = progress.delta_rows
            progress.end_time = time.time()
            progress.status = 'completed'
            
            self.structured_logger.info(f"Incremental load completed for {table_name}", 
                                      rows_processed=progress.processed_rows,
                                      duration=progress.duration_seconds)
            
        except Exception as e:
            progress.end_time = time.time()
            progress.status = 'failed'
            progress.error_message = str(e)
            self.structured_logger.error(f"Incremental load failed for {table_name}", error=str(e))
        
        return progress