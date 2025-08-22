"""
Data migration operations for AWS Glue Data Replication.

This module provides full-load and incremental data migration capabilities
for database replication operations with enhanced Iceberg engine support.
"""

import time
import logging
from typing import Dict, Any, Optional
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
    
    def __init__(self, spark_session: SparkSession, connection_manager: UnifiedConnectionManager):
        self.spark = spark_session
        self.connection_manager = connection_manager
        self.structured_logger = StructuredLogger("FullLoadDataMigrator")
    
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
        progress = FullLoadProgress(table_name=table_name)
        progress.start_time = time.time()
        progress.status = 'in_progress'
        
        # Check engine types for enhanced logging and error handling
        source_is_iceberg = DatabaseEngineManager.is_iceberg_engine(source_config.engine_type)
        target_is_iceberg = DatabaseEngineManager.is_iceberg_engine(target_config.engine_type)
        
        self.structured_logger.info(
            f"Starting full load migration for {table_name}",
            source_engine=source_config.engine_type,
            target_engine=target_config.engine_type,
            source_is_iceberg=source_is_iceberg,
            target_is_iceberg=target_is_iceberg
        )
        
        try:
            # Read all data from source with engine-specific handling
            source_df = self._read_source_data_with_engine_support(
                source_config, table_name, source_is_iceberg
            )
            progress.total_rows = source_df.count()
            
            self.structured_logger.info(
                f"Read {progress.total_rows} rows from source {table_name}",
                source_engine=source_config.engine_type
            )
            
            # Write to target with engine-specific handling
            self._write_target_data_with_engine_support(
                source_df, target_config, table_name, target_is_iceberg, mode='overwrite'
            )
            
            progress.processed_rows = progress.total_rows
            progress.end_time = time.time()
            progress.status = 'completed'
            
            self.structured_logger.info(
                f"Full load completed for {table_name}", 
                rows_processed=progress.processed_rows,
                duration=progress.duration_seconds,
                source_engine=source_config.engine_type,
                target_engine=target_config.engine_type
            )
            
        except IcebergEngineError as e:
            progress.end_time = time.time()
            progress.status = 'failed'
            progress.error_message = f"Iceberg engine error: {str(e)}"
            self.structured_logger.error(
                f"Iceberg full load failed for {table_name}",
                error=str(e),
                source_is_iceberg=source_is_iceberg,
                target_is_iceberg=target_is_iceberg
            )
        except Exception as e:
            progress.end_time = time.time()
            progress.status = 'failed'
            progress.error_message = str(e)
            self.structured_logger.error(f"Full load failed for {table_name}", error=str(e))
        
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
                self.structured_logger.debug(f"Reading from JDBC table: {table_name}")
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
    
    def _write_target_data_with_engine_support(self, df: DataFrame, target_config: ConnectionConfig,
                                             table_name: str, target_is_iceberg: bool, mode: str = 'overwrite') -> None:
        """
        Write target data with engine-specific support.
        
        Args:
            df: DataFrame to write
            target_config: Target connection configuration
            table_name: Name of the target table
            target_is_iceberg: Whether target engine is Iceberg
            mode: Write mode (overwrite, append)
            
        Raises:
            IcebergEngineError: If Iceberg write operations fail
        """
        try:
            if target_is_iceberg:
                self.structured_logger.debug(f"Writing to Iceberg table: {table_name}")
                print(f"=== MIGRATION CALLING CONNECTION MANAGER FOR {table_name} ===")
                self.structured_logger.info(f"=== ABOUT TO CALL CONNECTION MANAGER WRITE_TABLE ===")
                
                # PROACTIVE APPROACH: Create table BEFORE calling connection manager
                try:
                    print(f"=== PROACTIVE TABLE CREATION FOR {table_name} ===")
                    
                    # Get Iceberg configuration
                    iceberg_config = target_config.get_iceberg_config()
                    warehouse_location = iceberg_config.get('warehouse_location', '') if iceberg_config else ''
                    
                    # Create full table name
                    full_table_name = f"glue_catalog.{target_config.database}.{table_name}"
                    
                    print(f"=== ATTEMPTING TO CREATE TABLE {full_table_name} PROACTIVELY ===")
                    
                    # Try to create the table using DataFrame operations
                    df.write \
                        .format("iceberg") \
                        .mode("overwrite") \
                        .option("path", f"{warehouse_location}/{target_config.database}/{table_name}") \
                        .saveAsTable(full_table_name)
                    
                    print(f"=== PROACTIVELY CREATED TABLE {full_table_name} SUCCESSFULLY ===")
                    self.structured_logger.info(f"Proactively created Iceberg table: {table_name}")
                    
                except Exception as proactive_error:
                    print(f"=== PROACTIVE TABLE CREATION FAILED: {str(proactive_error)} ===")
                    self.structured_logger.warning(f"Proactive table creation failed, will try connection manager: {str(proactive_error)}")
                    
                    # Continue with connection manager approach
                    pass
                
                # DIRECT WORKAROUND: Create table using DataFrame operations if it doesn't exist
                try:
                    # For Iceberg targets, use the connection manager's Iceberg-aware write
                    self.connection_manager.write_table(
                        df=df,
                        connection_config=target_config,
                        table_name=table_name,
                        mode=mode
                    )
                except Exception as write_error:
                    # COMPREHENSIVE EXCEPTION ANALYSIS
                    import traceback
                    print(f"=== FULL EXCEPTION DETAILS FOR {table_name} ===")
                    print(f"Exception type: {type(write_error).__name__}")
                    print(f"Exception message: {str(write_error)}")
                    print(f"Exception module: {type(write_error).__module__}")
                    print("=== FULL TRACEBACK ===")
                    traceback.print_exc()
                    print("=== END TRACEBACK ===")
                    
                    self.structured_logger.error(f"FULL EXCEPTION ANALYSIS: {str(write_error)}")
                    self.structured_logger.error(f"Exception type: {type(write_error).__name__}")
                    
                    print(f"=== CONNECTION MANAGER FAILED, TRYING DIRECT APPROACH ===")
                    self.structured_logger.error(f"Connection manager failed: {str(write_error)}")
                    
                    # If the error is about table not found, try direct DataFrame approach
                    if "Table not found" in str(write_error) or "not found" in str(write_error).lower():
                        print(f"=== TABLE NOT FOUND ERROR DETECTED, CREATING TABLE DIRECTLY ===")
                        self.structured_logger.info(f"Attempting direct table creation for {table_name}")
                        
                        try:
                            # Get warehouse location from target config
                            iceberg_config = target_config.get_iceberg_config()
                            warehouse_location = iceberg_config.get('warehouse_location', '') if iceberg_config else ''
                            
                            # Create full table name
                            full_table_name = f"glue_catalog.{target_config.database}.{table_name}"
                            
                            print(f"=== CREATING TABLE {full_table_name} USING DATAFRAME OPERATIONS ===")
                            
                            # Use DataFrame.write.saveAsTable() to create and populate the table
                            df.write \
                                .format("iceberg") \
                                .mode("overwrite") \
                                .option("path", f"{warehouse_location}/{target_config.database}/{table_name}") \
                                .saveAsTable(full_table_name)
                            
                            print(f"=== SUCCESSFULLY CREATED TABLE {full_table_name} ===")
                            self.structured_logger.info(f"Successfully created Iceberg table using direct approach: {table_name}")
                            
                        except Exception as direct_error:
                            print(f"=== DIRECT APPROACH ALSO FAILED: {str(direct_error)} ===")
                            self.structured_logger.error(f"Direct table creation also failed: {str(direct_error)}")
                            raise IcebergEngineError(
                                f"Both connection manager and direct approaches failed for {table_name}: {str(direct_error)}",
                                error_code="WRITE_ERROR",
                                context={"table_name": table_name, "original_error": str(write_error), "direct_error": str(direct_error)}
                            )
                    else:
                        # Re-raise the original error if it's not a table not found error
                        raise
                
                print(f"=== CONNECTION MANAGER CALL COMPLETED FOR {table_name} ===")
                self.structured_logger.info(f"=== CONNECTION MANAGER CALL COMPLETED ===")
                
            else:
                self.structured_logger.debug(f"Writing to JDBC table: {table_name}")
                # For traditional databases, use standard write
                self.connection_manager.write_table_data(df, target_config, table_name, mode=mode)
        except Exception as e:
            if target_is_iceberg:
                raise IcebergEngineError(
                    f"Failed to write to Iceberg table {table_name}: {str(e)}",
                    error_code="WRITE_ERROR",
                    context={"table_name": table_name, "spark_error": str(e)}
                )
            else:
                raise


class IncrementalDataMigrator:
    """Handles incremental data migration operations with Iceberg engine support."""
    
    def __init__(self, spark_session: SparkSession, connection_manager: UnifiedConnectionManager, 
                 bookmark_manager: JobBookmarkManager):
        self.spark = spark_session
        self.connection_manager = connection_manager
        self.bookmark_manager = bookmark_manager
        self.structured_logger = StructuredLogger("IncrementalDataMigrator")
    
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
        # Check engine types for enhanced processing
        source_is_iceberg = DatabaseEngineManager.is_iceberg_engine(source_config.engine_type)
        target_is_iceberg = DatabaseEngineManager.is_iceberg_engine(target_config.engine_type)
        
        self.structured_logger.info(
            f"Starting incremental load migration for {table_name}",
            source_engine=source_config.engine_type,
            target_engine=target_config.engine_type,
            source_is_iceberg=source_is_iceberg,
            target_is_iceberg=target_is_iceberg
        )
        
        try:
            # Auto-detect incremental strategy with engine-specific handling
            schema = self._get_table_schema_with_engine_support(
                source_config, table_name, source_is_iceberg
            )
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
            
            # Read incremental data with engine-specific handling
            source_df = self._read_incremental_data_with_engine_support(
                source_config, table_name, bookmark_state, source_is_iceberg
            )
            progress.delta_rows = source_df.count()
            
            self.structured_logger.info(
                f"Read {progress.delta_rows} incremental rows from {table_name}",
                incremental_column=bookmark_state.incremental_column,
                last_processed_value=bookmark_state.last_processed_value
            )
            
            if progress.delta_rows > 0:
                # Write to target with engine-specific handling
                self._write_incremental_data_with_engine_support(
                    source_df, target_config, table_name, target_is_iceberg
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
            
            self.structured_logger.info(
                f"Incremental load completed for {table_name}", 
                rows_processed=progress.processed_rows,
                duration=progress.duration_seconds,
                source_engine=source_config.engine_type,
                target_engine=target_config.engine_type
            )
            
        except IcebergEngineError as e:
            progress.end_time = time.time()
            progress.status = 'failed'
            progress.error_message = f"Iceberg engine error: {str(e)}"
            self.structured_logger.error(
                f"Iceberg incremental load failed for {table_name}",
                error=str(e),
                source_is_iceberg=source_is_iceberg,
                target_is_iceberg=target_is_iceberg
            )
        except Exception as e:
            progress.end_time = time.time()
            progress.status = 'failed'
            progress.error_message = str(e)
            self.structured_logger.error(f"Incremental load failed for {table_name}", error=str(e))
        
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
                # For traditional databases, use SQL query approach
                self.structured_logger.debug(f"Reading incremental data from JDBC table: {table_name}")
                
                if bookmark_state.last_processed_value:
                    query = f"SELECT * FROM {source_config.schema}.{table_name} WHERE {bookmark_state.incremental_column} > '{bookmark_state.last_processed_value}'"
                else:
                    query = f"SELECT * FROM {source_config.schema}.{table_name}"
                
                return self.connection_manager.read_table_data(source_config, table_name, query=query)
                
        except Exception as e:
            if source_is_iceberg:
                raise IcebergEngineError(
                    f"Failed to read incremental data from Iceberg table {table_name}: {str(e)}",
                    error_code="INCREMENTAL_READ_ERROR",
                    context={"table_name": table_name, "spark_error": str(e)}
                )
            else:
                raise
    
    def _write_incremental_data_with_engine_support(self, df: DataFrame, target_config: ConnectionConfig,
                                                  table_name: str, target_is_iceberg: bool) -> None:
        """
        Write incremental data with engine-specific support.
        
        Args:
            df: DataFrame to write
            target_config: Target connection configuration
            table_name: Name of the target table
            target_is_iceberg: Whether target engine is Iceberg
            
        Raises:
            IcebergEngineError: If Iceberg write operations fail
        """
        try:
            if target_is_iceberg:
                self.structured_logger.debug(f"Writing incremental data to Iceberg table: {table_name}")
                # For Iceberg targets, use append mode
                self.connection_manager.write_table(
                    df=df,
                    connection_config=target_config,
                    table_name=table_name,
                    mode='append'
                )
            else:
                self.structured_logger.debug(f"Writing incremental data to JDBC table: {table_name}")
                # For traditional databases, use append mode
                self.connection_manager.write_table_data(df, target_config, table_name, mode='append')
        except Exception as e:
            if target_is_iceberg:
                raise IcebergEngineError(
                    f"Failed to write incremental data to Iceberg table {table_name}: {str(e)}",
                    error_code="INCREMENTAL_WRITE_ERROR",
                    context={"table_name": table_name, "spark_error": str(e)}
                )
            else:
                raise
    
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