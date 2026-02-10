"""
Counting strategy module for AWS Glue Data Replication.

This module provides adaptive row counting strategies to optimize performance
for large-scale data migrations by eliminating redundant source data reads.
"""

import time
from enum import Enum
from typing import Optional
from dataclasses import dataclass

# Conditional imports for PySpark (only available in Glue runtime)
try:
    from pyspark.sql import DataFrame
except ImportError:
    # Mock class for local development/testing
    class DataFrame:
        pass

from ..config.job_config import ConnectionConfig
from ..monitoring.logging import StructuredLogger


class CountingStrategyType(Enum):
    """Types of counting strategies."""
    IMMEDIATE = "immediate"  # Count before write
    DEFERRED = "deferred"    # Count after write
    AUTO = "auto"            # Automatically select based on dataset


@dataclass
class CountingStrategyConfig:
    """Configuration for counting strategy selection."""
    strategy_type: CountingStrategyType = CountingStrategyType.AUTO
    size_threshold_rows: int = 1_000_000  # Threshold for auto selection
    force_immediate: bool = False
    force_deferred: bool = False
    
    def validate(self):
        """Validate configuration.
        
        Raises:
            ValueError: If configuration is invalid
        """
        if self.force_immediate and self.force_deferred:
            raise ValueError("Cannot force both immediate and deferred strategies")
        
        if self.size_threshold_rows <= 0:
            raise ValueError("Size threshold must be positive")


class CountingStrategy:
    """Manages row counting strategy selection and execution."""
    
    def __init__(self, config: CountingStrategyConfig, metrics_publisher=None):
        """Initialize counting strategy.
        
        Args:
            config: Configuration for counting strategy
            metrics_publisher: Optional CloudWatchMetricsPublisher for publishing metrics
        """
        self.config = config
        self.structured_logger = StructuredLogger("CountingStrategy")
        self.metrics_publisher = metrics_publisher
    
    def select_strategy(self, df: Optional[DataFrame], table_name: str, 
                       engine_type: str, estimated_size: Optional[int] = None,
                       source_config=None, connection_manager=None) -> CountingStrategyType:
        """Select the optimal counting strategy.
        
        For AUTO mode, always returns IMMEDIATE since SQL COUNT(*) is fast
        for both JDBC and Iceberg sources (uses database statistics/metadata).
        
        Args:
            df: Source DataFrame (may be None for deferred counting)
            table_name: Name of the table
            engine_type: Database engine type
            estimated_size: Estimated dataset size in rows (if known) - deprecated, ignored
            source_config: Optional source connection config for SQL COUNT
            connection_manager: Optional connection manager for SQL COUNT
            
        Returns:
            Selected counting strategy type
        """
        # Check for forced strategies
        if self.config.force_immediate:
            self.structured_logger.info(
                f"Using forced immediate counting strategy for {table_name}",
                table_name=table_name,
                engine_type=engine_type,
                strategy="immediate",
                reason="forced_by_configuration"
            )
            return CountingStrategyType.IMMEDIATE
        
        if self.config.force_deferred:
            self.structured_logger.info(
                f"Using forced deferred counting strategy for {table_name}",
                table_name=table_name,
                engine_type=engine_type,
                strategy="deferred",
                reason="forced_by_configuration"
            )
            return CountingStrategyType.DEFERRED
        
        # Auto selection logic - always use IMMEDIATE with SQL COUNT(*)
        if self.config.strategy_type == CountingStrategyType.AUTO:
            return self._auto_select_strategy(table_name, engine_type, estimated_size)
        
        # Use configured strategy
        self.structured_logger.info(
            f"Using configured counting strategy for {table_name}",
            table_name=table_name,
            engine_type=engine_type,
            strategy=self.config.strategy_type.value,
            reason="explicit_configuration"
        )
        return self.config.strategy_type
    
    def _auto_select_strategy(self, table_name: str, engine_type: str, 
                            estimated_size: Optional[int]) -> CountingStrategyType:
        """Automatically select counting strategy based on dataset characteristics.
        
        For AUTO mode, always use IMMEDIATE counting with SQL COUNT(*) since:
        - JDBC databases: COUNT(*) uses database statistics/indexes (fast)
        - Iceberg tables: COUNT(*) reads manifest metadata only (fast)
        
        This eliminates the need for size-based strategy selection.
        
        Args:
            table_name: Name of the table
            engine_type: Database engine type
            estimated_size: Estimated dataset size in rows (deprecated, ignored)
            
        Returns:
            Selected counting strategy type (always IMMEDIATE for AUTO)
        """
        # Import here to avoid circular imports
        from ..config.database_engines import DatabaseEngineManager
        
        # For Iceberg engines, use IMMEDIATE with Spark SQL COUNT(*)
        if DatabaseEngineManager.is_iceberg_engine(engine_type):
            self.structured_logger.info(
                f"Auto-selected immediate counting for Iceberg table {table_name}",
                table_name=table_name,
                engine_type="iceberg",
                strategy="immediate",
                reason="sql_count_optimization",
                optimization="spark_sql_metadata_count"
            )
            return CountingStrategyType.IMMEDIATE
        
        # For JDBC databases, use IMMEDIATE with SQL COUNT(*)
        self.structured_logger.info(
            f"Auto-selected immediate counting for JDBC table {table_name}",
            table_name=table_name,
            engine_type=engine_type,
            strategy="immediate",
            reason="sql_count_optimization",
            optimization="database_statistics_count"
        )
        return CountingStrategyType.IMMEDIATE
    
    def execute_immediate_count(self, df: DataFrame, table_name: str) -> int:
        """Execute immediate counting (before write).
        
        Args:
            df: Source DataFrame to count
            table_name: Name of the table
            
        Returns:
            Number of rows in the DataFrame
        """
        start_time = time.time()
        
        self.structured_logger.info(
            f"Starting immediate row count for {table_name}",
            table_name=table_name,
            counting_strategy="immediate"
        )
        
        try:
            row_count = df.count()
            duration = time.time() - start_time
            
            self.structured_logger.info(
                f"Immediate row count completed for {table_name}",
                table_name=table_name,
                row_count=row_count,
                count_duration_seconds=round(duration, 2),
                counting_strategy="immediate"
            )
            
            # Publish counting strategy metrics
            if self.metrics_publisher:
                self.metrics_publisher.publish_counting_strategy_metrics(
                    table_name=table_name,
                    strategy_type="immediate",
                    count_duration_seconds=duration,
                    row_count=row_count
                )
            
            return row_count
            
        except Exception as e:
            duration = time.time() - start_time
            self.structured_logger.error(
                f"Immediate row count failed for {table_name}",
                table_name=table_name,
                error=str(e),
                count_duration_seconds=round(duration, 2),
                counting_strategy="immediate"
            )
            raise
    
    def execute_immediate_count_sql(self, source_config: ConnectionConfig,
                                   table_name: str,
                                   connection_manager) -> int:
        """Execute immediate counting via SQL SELECT COUNT(*).
        
        This method uses SQL COUNT(*) queries which are fast because:
        - JDBC databases: Execute count server-side using indexes and statistics
        - Iceberg tables: Read from manifest metadata, not data files
        
        Args:
            source_config: Source database connection configuration
            table_name: Name of the table
            connection_manager: UnifiedConnectionManager instance for database access
            
        Returns:
            Number of rows in the source table
            
        Raises:
            Exception: If SQL COUNT fails (caller should fall back to deferred counting)
        """
        start_time = time.time()
        
        # Import here to avoid circular imports
        from ..config.database_engines import DatabaseEngineManager
        
        # Determine if source is Iceberg
        is_iceberg = DatabaseEngineManager.is_iceberg_engine(source_config.engine_type)
        
        self.structured_logger.info(
            f"Starting SQL COUNT(*) for {table_name}",
            table_name=table_name,
            source_engine=source_config.engine_type,
            is_iceberg=is_iceberg,
            counting_strategy="immediate_sql",
            counting_method="spark_sql_metadata" if is_iceberg else "jdbc_sql_count"
        )
        
        try:
            if is_iceberg:
                row_count = self._execute_iceberg_count_sql(
                    connection_manager, source_config, table_name
                )
            else:
                row_count = self._execute_jdbc_count_sql(
                    connection_manager, source_config, table_name
                )
            
            duration = time.time() - start_time
            
            self.structured_logger.info(
                f"SQL COUNT(*) completed for {table_name}",
                table_name=table_name,
                row_count=row_count,
                count_duration_seconds=round(duration, 2),
                source_engine=source_config.engine_type,
                is_iceberg=is_iceberg,
                counting_strategy="immediate_sql",
                counting_method="spark_sql_metadata" if is_iceberg else "jdbc_sql_count"
            )
            
            # Publish counting strategy metrics
            if self.metrics_publisher:
                self.metrics_publisher.publish_counting_strategy_metrics(
                    table_name=table_name,
                    strategy_type="immediate_sql",
                    count_duration_seconds=duration,
                    row_count=row_count
                )
            
            return row_count
            
        except Exception as e:
            duration = time.time() - start_time
            self.structured_logger.error(
                f"SQL COUNT(*) failed for {table_name}",
                table_name=table_name,
                error=str(e),
                count_duration_seconds=round(duration, 2),
                source_engine=source_config.engine_type,
                is_iceberg=is_iceberg,
                counting_strategy="immediate_sql"
            )
            raise
    
    def _execute_iceberg_count_sql(self, connection_manager, source_config: ConnectionConfig,
                                  table_name: str) -> int:
        """Execute SQL COUNT(*) for Iceberg table using Spark SQL.
        
        Iceberg tables maintain row count statistics in their metadata,
        so COUNT(*) reads from manifest files rather than scanning data.
        
        Args:
            connection_manager: UnifiedConnectionManager instance
            source_config: Source connection configuration
            table_name: Name of the table
            
        Returns:
            Number of rows in the table
        """
        self.structured_logger.debug(
            f"Executing Spark SQL COUNT(*) for Iceberg table: {table_name}",
            table_name=table_name,
            engine_type="iceberg"
        )
        
        # Get Iceberg configuration
        iceberg_config = source_config.get_iceberg_config()
        if not iceberg_config:
            raise ValueError(f"No Iceberg config found for {table_name}")
        
        # Construct full table name for Iceberg
        database = iceberg_config.get('database_name', source_config.database)
        catalog_name = iceberg_config.get('catalog_name', 'glue_catalog')
        full_table_name = f"{catalog_name}.{database}.{table_name}"
        
        self.structured_logger.debug(
            f"Using Iceberg table identifier: {full_table_name}",
            table_name=table_name,
            full_table_name=full_table_name,
            engine_type="iceberg"
        )
        
        # Get the Spark session from connection manager
        spark = connection_manager.spark
        
        # Execute Spark SQL COUNT(*) - reads from Iceberg metadata
        count_query = f"SELECT COUNT(*) as row_count FROM {full_table_name}"
        
        self.structured_logger.debug(
            f"Executing Iceberg count query: {count_query}",
            table_name=table_name,
            engine_type="iceberg"
        )
        
        count_df = spark.sql(count_query)
        row_count = count_df.collect()[0]['row_count']
        
        self.structured_logger.info(
            f"Iceberg SQL COUNT(*) completed: {table_name}",
            table_name=table_name,
            row_count=row_count,
            engine_type="iceberg",
            counting_method="spark_sql_metadata"
        )
        
        return row_count
    
    def _execute_jdbc_count_sql(self, connection_manager, source_config: ConnectionConfig,
                               table_name: str) -> int:
        """Execute SQL COUNT(*) for JDBC table.
        
        JDBC databases execute COUNT(*) server-side using indexes and statistics,
        making it much faster than reading all data to count rows.
        
        Args:
            connection_manager: UnifiedConnectionManager instance
            source_config: Source connection configuration
            table_name: Name of the table
            
        Returns:
            Number of rows in the table
        """
        self.structured_logger.debug(
            f"Executing JDBC SQL COUNT(*) for table: {table_name}",
            table_name=table_name,
            engine_type=source_config.engine_type
        )
        
        # Build SQL COUNT query
        count_query = f"SELECT COUNT(*) as row_count FROM {source_config.schema}.{table_name}"
        
        self.structured_logger.debug(
            f"Executing JDBC count query: {count_query}",
            table_name=table_name,
            engine_type=source_config.engine_type
        )
        
        # Execute count query via connection manager
        count_df = connection_manager.read_table_data(
            source_config, 
            table_name, 
            query=count_query
        )
        
        # Extract count from result
        count_result = count_df.collect()[0]
        row_count = count_result['row_count']
        
        self.structured_logger.info(
            f"JDBC SQL COUNT(*) completed: {table_name}",
            table_name=table_name,
            row_count=row_count,
            engine_type=source_config.engine_type,
            counting_method="jdbc_sql_count"
        )
        
        return row_count
    
    def execute_deferred_count(self, target_config: ConnectionConfig, 
                              table_name: str, 
                              connection_manager) -> int:
        """Execute deferred counting (after write from target).
        
        For Iceberg tables, this uses metadata-based counting which is significantly
        faster than scanning the entire table. For JDBC tables, this uses SQL COUNT queries.
        
        Args:
            target_config: Target database connection configuration
            table_name: Name of the table
            connection_manager: UnifiedConnectionManager instance for database access
            
        Returns:
            Number of rows in the target table
        """
        start_time = time.time()
        
        # Import here to avoid circular imports
        from ..config.database_engines import DatabaseEngineManager
        
        # Determine if target is Iceberg
        is_iceberg = DatabaseEngineManager.is_iceberg_engine(target_config.engine_type)
        
        self.structured_logger.info(
            f"Starting deferred row count for {table_name}",
            table_name=table_name,
            target_engine=target_config.engine_type,
            engine_type=target_config.engine_type,
            is_iceberg=is_iceberg,
            counting_strategy="deferred",
            counting_method="metadata_optimized" if is_iceberg else "sql_query"
        )
        
        try:
            # Check if target is Iceberg
            if is_iceberg:
                self.structured_logger.debug(
                    f"Using Iceberg metadata-based counting for {table_name}",
                    table_name=table_name,
                    engine_type="iceberg"
                )
                row_count = self._count_iceberg_table(
                    connection_manager, target_config, table_name
                )
            else:
                self.structured_logger.debug(
                    f"Using JDBC SQL query counting for {table_name}",
                    table_name=table_name,
                    engine_type=target_config.engine_type
                )
                row_count = self._count_jdbc_table(
                    connection_manager, target_config, table_name
                )
            
            duration = time.time() - start_time
            
            self.structured_logger.info(
                f"Deferred row count completed for {table_name}",
                table_name=table_name,
                row_count=row_count,
                count_duration_seconds=round(duration, 2),
                target_engine=target_config.engine_type,
                engine_type=target_config.engine_type,
                is_iceberg=is_iceberg,
                counting_strategy="deferred",
                counting_method="metadata_optimized" if is_iceberg else "sql_query"
            )
            
            # Publish counting strategy metrics
            if self.metrics_publisher:
                self.metrics_publisher.publish_counting_strategy_metrics(
                    table_name=table_name,
                    strategy_type="deferred",
                    count_duration_seconds=duration,
                    row_count=row_count
                )
            
            return row_count
            
        except Exception as e:
            duration = time.time() - start_time
            self.structured_logger.error(
                f"Deferred row count failed for {table_name}",
                table_name=table_name,
                error=str(e),
                count_duration_seconds=round(duration, 2),
                target_engine=target_config.engine_type,
                engine_type=target_config.engine_type,
                is_iceberg=is_iceberg,
                counting_strategy="deferred"
            )
            raise
    
    def _count_iceberg_table(self, connection_manager, target_config: ConnectionConfig, 
                           table_name: str) -> int:
        """Count rows in Iceberg table using metadata.
        
        This method attempts to use Iceberg's metadata for efficient row counting.
        Iceberg tables maintain statistics in their metadata that can be used for
        fast row counting without scanning the entire table.
        
        Args:
            connection_manager: UnifiedConnectionManager instance
            target_config: Target connection configuration
            table_name: Name of the table
            
        Returns:
            Number of rows in the table
        """
        self.structured_logger.info(
            f"Counting Iceberg table using metadata-optimized approach: {table_name}",
            table_name=table_name,
            engine_type="iceberg",
            counting_method="metadata_optimized"
        )
        
        try:
            # Get Iceberg configuration
            iceberg_config = target_config.get_iceberg_config()
            if not iceberg_config:
                self.structured_logger.warning(
                    f"No Iceberg config found for {table_name}, falling back to standard count",
                    table_name=table_name,
                    engine_type="iceberg"
                )
                df = connection_manager.read_table(
                    connection_config=target_config,
                    table_name=table_name
                )
                return df.count()
            
            # Construct full table name for Iceberg
            database = iceberg_config.get('database_name', target_config.database)
            catalog_name = iceberg_config.get('catalog_name', 'glue_catalog')
            full_table_name = f"{catalog_name}.{database}.{table_name}"
            
            self.structured_logger.debug(
                f"Using Iceberg table identifier: {full_table_name}",
                table_name=table_name,
                full_table_name=full_table_name,
                engine_type="iceberg"
            )
            
            # Try to use Iceberg metadata for counting
            # Iceberg maintains row count statistics in table metadata
            try:
                # Get the Spark session from connection manager
                spark = connection_manager.spark
                
                # Use Spark SQL to query Iceberg metadata
                # This is much faster than scanning the entire table
                metadata_query = f"SELECT COUNT(*) as row_count FROM {full_table_name}"
                
                self.structured_logger.debug(
                    f"Executing metadata-optimized count query: {metadata_query}",
                    table_name=table_name,
                    engine_type="iceberg"
                )
                
                count_df = spark.sql(metadata_query)
                row_count = count_df.collect()[0]['row_count']
                
                self.structured_logger.info(
                    f"Successfully counted Iceberg table using metadata: {table_name}",
                    table_name=table_name,
                    row_count=row_count,
                    engine_type="iceberg",
                    counting_method="metadata_optimized"
                )
                
                return row_count
                
            except Exception as metadata_error:
                self.structured_logger.warning(
                    f"Metadata-based counting failed for {table_name}, falling back to DataFrame count",
                    table_name=table_name,
                    error=str(metadata_error),
                    engine_type="iceberg",
                    counting_method="fallback_to_dataframe"
                )
                
                # Fallback to standard DataFrame count
                df = connection_manager.read_table(
                    connection_config=target_config,
                    table_name=table_name
                )
                row_count = df.count()
                
                self.structured_logger.info(
                    f"Counted Iceberg table using DataFrame fallback: {table_name}",
                    table_name=table_name,
                    row_count=row_count,
                    engine_type="iceberg",
                    counting_method="dataframe_fallback"
                )
                
                return row_count
                
        except Exception as e:
            self.structured_logger.error(
                f"Failed to count Iceberg table {table_name}",
                table_name=table_name,
                error=str(e),
                engine_type="iceberg"
            )
            raise
    
    def _count_jdbc_table(self, connection_manager, target_config: ConnectionConfig, 
                        table_name: str) -> int:
        """Count rows in JDBC table using SQL query.
        
        Args:
            connection_manager: UnifiedConnectionManager instance
            target_config: Target connection configuration
            table_name: Name of the table
            
        Returns:
            Number of rows in the table
        """
        self.structured_logger.debug(
            f"Counting JDBC table using SQL query: {table_name}",
            table_name=table_name
        )
        
        # Use SQL COUNT query for JDBC databases
        count_query = f"SELECT COUNT(*) as row_count FROM {target_config.schema}.{table_name}"
        
        # Execute count query
        count_df = connection_manager.read_table_data(
            target_config, 
            table_name, 
            query=count_query
        )
        
        # Extract count from result
        count_result = count_df.collect()[0]
        return count_result['row_count']

    def execute_immediate_count_sql_with_fallback(self, source_config: ConnectionConfig,
                                                  table_name: str,
                                                  connection_manager,
                                                  target_config: ConnectionConfig = None) -> tuple:
        """Execute immediate counting via SQL COUNT(*) with fallback to deferred counting.
        
        This method attempts SQL COUNT(*) first (fast for all sizes), and if it fails,
        falls back to deferred counting strategy.
        
        Args:
            source_config: Source database connection configuration
            table_name: Name of the table
            connection_manager: UnifiedConnectionManager instance for database access
            target_config: Target connection config (required for deferred fallback)
            
        Returns:
            Tuple of (row_count, counting_method) where counting_method is:
            - 'immediate_sql': SQL COUNT(*) succeeded
            - 'deferred_fallback': Fell back to deferred counting
        """
        try:
            row_count = self.execute_immediate_count_sql(
                source_config, table_name, connection_manager
            )
            return row_count, 'immediate_sql'
            
        except Exception as sql_error:
            self.structured_logger.warning(
                f"SQL COUNT(*) failed for {table_name}, falling back to deferred counting",
                table_name=table_name,
                source_engine=source_config.engine_type,
                error=str(sql_error),
                error_type=type(sql_error).__name__,
                fallback_strategy="deferred"
            )
            
            # Publish fallback metric
            if self.metrics_publisher:
                self.metrics_publisher.publish_error_metrics(
                    error_category="SQL_COUNT_FALLBACK",
                    error_operation="immediate_count_sql"
                )
            
            # Return None to indicate deferred counting should be used
            # The caller will handle deferred counting after write completes
            return None, 'deferred_fallback'

    def execute_incremental_count_sql(self, source_config: ConnectionConfig,
                                      table_name: str,
                                      connection_manager,
                                      incremental_column: str,
                                      last_processed_value,
                                      incremental_strategy: str = None) -> int:
        """Execute SQL COUNT(*) with WHERE clause for incremental loads.
        
        This method counts only the incremental rows (delta) using a WHERE clause,
        which is much faster than counting the entire filtered DataFrame.
        
        For JDBC: SELECT COUNT(*) FROM schema.table WHERE incremental_column > last_value
        For Iceberg: SELECT COUNT(*) FROM catalog.db.table WHERE incremental_column > last_value
        
        Args:
            source_config: Source database connection configuration
            table_name: Name of the table
            connection_manager: UnifiedConnectionManager instance for database access
            incremental_column: Name of the incremental column
            last_processed_value: Last processed value for the incremental column
            incremental_strategy: Strategy type ('timestamp', 'primary_key', 'hash') for value formatting
            
        Returns:
            Number of incremental rows (delta count)
            
        Raises:
            Exception: If SQL COUNT fails (caller should fall back to df.count())
        """
        import time
        start_time = time.time()
        
        # Import here to avoid circular imports
        from ..config.database_engines import DatabaseEngineManager
        
        # Determine if source is Iceberg
        is_iceberg = DatabaseEngineManager.is_iceberg_engine(source_config.engine_type)
        
        self.structured_logger.info(
            f"Starting incremental SQL COUNT(*) for {table_name}",
            table_name=table_name,
            source_engine=source_config.engine_type,
            is_iceberg=is_iceberg,
            incremental_column=incremental_column,
            last_processed_value=last_processed_value,
            incremental_strategy=incremental_strategy,
            counting_strategy="incremental_sql",
            counting_method="spark_sql_metadata" if is_iceberg else "jdbc_sql_count"
        )
        
        try:
            if is_iceberg:
                row_count = self._execute_iceberg_incremental_count_sql(
                    connection_manager, source_config, table_name,
                    incremental_column, last_processed_value, incremental_strategy
                )
            else:
                row_count = self._execute_jdbc_incremental_count_sql(
                    connection_manager, source_config, table_name,
                    incremental_column, last_processed_value, incremental_strategy
                )
            
            duration = time.time() - start_time
            
            self.structured_logger.info(
                f"Incremental SQL COUNT(*) completed for {table_name}",
                table_name=table_name,
                row_count=row_count,
                count_duration_seconds=round(duration, 2),
                source_engine=source_config.engine_type,
                is_iceberg=is_iceberg,
                incremental_column=incremental_column,
                counting_strategy="incremental_sql",
                counting_method="spark_sql_metadata" if is_iceberg else "jdbc_sql_count"
            )
            
            # Publish counting strategy metrics
            if self.metrics_publisher:
                self.metrics_publisher.publish_counting_strategy_metrics(
                    table_name=table_name,
                    strategy_type="incremental_sql",
                    count_duration_seconds=duration,
                    row_count=row_count
                )
            
            return row_count
            
        except Exception as e:
            duration = time.time() - start_time
            self.structured_logger.error(
                f"Incremental SQL COUNT(*) failed for {table_name}",
                table_name=table_name,
                error=str(e),
                count_duration_seconds=round(duration, 2),
                source_engine=source_config.engine_type,
                is_iceberg=is_iceberg,
                incremental_column=incremental_column,
                counting_strategy="incremental_sql"
            )
            raise
    
    def _execute_iceberg_incremental_count_sql(self, connection_manager, source_config: ConnectionConfig,
                                               table_name: str, incremental_column: str,
                                               last_processed_value, incremental_strategy: str = None) -> int:
        """Execute incremental SQL COUNT(*) for Iceberg table using Spark SQL.
        
        Args:
            connection_manager: UnifiedConnectionManager instance
            source_config: Source connection configuration
            table_name: Name of the table
            incremental_column: Name of the incremental column
            last_processed_value: Last processed value
            incremental_strategy: Strategy type ('timestamp', 'primary_key', 'hash')
            
        Returns:
            Number of incremental rows
        """
        # Get Iceberg configuration
        iceberg_config = source_config.get_iceberg_config()
        if not iceberg_config:
            raise ValueError(f"No Iceberg config found for {table_name}")
        
        # Construct full table name for Iceberg
        database = iceberg_config.get('database_name', source_config.database)
        catalog_name = iceberg_config.get('catalog_name', 'glue_catalog')
        full_table_name = f"{catalog_name}.{database}.{table_name}"
        
        # Get the Spark session from connection manager
        spark = connection_manager.spark
        
        # Build WHERE clause based on strategy and value type
        if last_processed_value is None:
            # No previous value, count all rows
            count_query = f"SELECT COUNT(*) as row_count FROM {full_table_name}"
        elif incremental_strategy == 'primary_key':
            # Primary key is numeric - use value directly without quotes
            count_query = f"SELECT COUNT(*) as row_count FROM {full_table_name} WHERE {incremental_column} > {last_processed_value}"
        elif incremental_strategy == 'timestamp':
            # Timestamp value - use quotes
            count_query = f"SELECT COUNT(*) as row_count FROM {full_table_name} WHERE {incremental_column} > '{last_processed_value}'"
        else:
            # Fallback: detect if value is numeric
            if self._is_numeric_string(str(last_processed_value)):
                count_query = f"SELECT COUNT(*) as row_count FROM {full_table_name} WHERE {incremental_column} > {last_processed_value}"
            else:
                count_query = f"SELECT COUNT(*) as row_count FROM {full_table_name} WHERE {incremental_column} > '{last_processed_value}'"
        
        self.structured_logger.debug(
            f"Executing Iceberg incremental count query: {count_query}",
            table_name=table_name,
            engine_type="iceberg"
        )
        
        count_df = spark.sql(count_query)
        row_count = count_df.collect()[0]['row_count']
        
        return row_count
    
    def _execute_jdbc_incremental_count_sql(self, connection_manager, source_config: ConnectionConfig,
                                            table_name: str, incremental_column: str,
                                            last_processed_value, incremental_strategy: str = None) -> int:
        """Execute incremental SQL COUNT(*) for JDBC table.
        
        Args:
            connection_manager: UnifiedConnectionManager instance
            source_config: Source connection configuration
            table_name: Name of the table
            incremental_column: Name of the incremental column
            last_processed_value: Last processed value
            incremental_strategy: Strategy type ('timestamp', 'primary_key', 'hash')
            
        Returns:
            Number of incremental rows
        """
        # Build WHERE clause based on strategy and value type
        if last_processed_value is None:
            # No previous value, count all rows
            count_query = f"SELECT COUNT(*) as row_count FROM {source_config.schema}.{table_name}"
        elif incremental_strategy == 'primary_key':
            # Primary key is numeric - use value directly without quotes or datetime formatting
            count_query = f"SELECT COUNT(*) as row_count FROM {source_config.schema}.{table_name} WHERE {incremental_column} > {last_processed_value}"
        elif incremental_strategy == 'timestamp':
            # Timestamp value - format properly for the database engine
            formatted_value = self._format_datetime_for_sql(str(last_processed_value), source_config.engine_type)
            count_query = f"SELECT COUNT(*) as row_count FROM {source_config.schema}.{table_name} WHERE {incremental_column} > {formatted_value}"
        else:
            # Fallback: detect if value is numeric
            if self._is_numeric_string(str(last_processed_value)):
                # Numeric value - use directly
                count_query = f"SELECT COUNT(*) as row_count FROM {source_config.schema}.{table_name} WHERE {incremental_column} > {last_processed_value}"
            else:
                # Non-numeric string - assume datetime and format
                formatted_value = self._format_datetime_for_sql(str(last_processed_value), source_config.engine_type)
                count_query = f"SELECT COUNT(*) as row_count FROM {source_config.schema}.{table_name} WHERE {incremental_column} > {formatted_value}"
        
        self.structured_logger.info(
            f"Executing JDBC incremental count query",
            table_name=table_name,
            engine_type=source_config.engine_type,
            incremental_strategy=incremental_strategy,
            count_query=count_query
        )
        
        # Execute count query via connection manager
        count_df = connection_manager.read_table_data(
            source_config, 
            table_name, 
            query=count_query
        )
        
        # Extract count from result
        count_result = count_df.collect()[0]
        return count_result['row_count']
    
    def _is_numeric_string(self, value: str) -> bool:
        """Check if a string represents a numeric value.
        
        Args:
            value: The string value to check
            
        Returns:
            True if the string is numeric (int or float), False otherwise
        """
        try:
            float(value.strip())
            return True
        except (ValueError, AttributeError):
            return False
    
    def _format_datetime_for_sql(self, value: str, engine_type: str) -> str:
        """Format a datetime string for use in SQL queries based on database engine.
        
        Args:
            value: The datetime value as a string
            engine_type: The database engine type (sqlserver, oracle, postgresql, etc.)
            
        Returns:
            Properly formatted datetime string for SQL query
        """
        # Clean up the value
        clean_value = value.strip()
        
        if engine_type.lower() == 'sqlserver':
            # SQL Server: Truncate to 3 decimal places (milliseconds) for JDBC compatibility
            # The JDBC driver can fail with "Conversion failed" if microseconds are present
            if '.' in clean_value:
                parts = clean_value.split('.')
                if len(parts) == 2:
                    # Truncate fractional seconds to 3 digits (milliseconds)
                    fractional = parts[1][:3] if len(parts[1]) >= 3 else parts[1]
                    clean_value = f"{parts[0]}.{fractional}"
            return f"'{clean_value}'"
        
        elif engine_type.lower() == 'oracle':
            # Oracle uses TO_TIMESTAMP for datetime with fractional seconds
            return f"TO_TIMESTAMP('{clean_value}', 'YYYY-MM-DD HH24:MI:SS.FF6')"
        
        elif engine_type.lower() == 'postgresql':
            # PostgreSQL accepts ISO format directly
            return f"'{clean_value}'::timestamp"
        
        elif engine_type.lower() == 'db2':
            # DB2 uses TIMESTAMP function
            return f"TIMESTAMP('{clean_value}')"
        
        else:
            # Default: use simple string format
            return f"'{clean_value}'"
    
    def execute_incremental_count_sql_with_fallback(self, source_config: ConnectionConfig,
                                                    table_name: str,
                                                    connection_manager,
                                                    incremental_column: str,
                                                    last_processed_value,
                                                    df=None,
                                                    incremental_strategy: str = None) -> tuple:
        """Execute incremental SQL COUNT(*) with fallback to DataFrame count.
        
        This method attempts SQL COUNT(*) with WHERE clause first (fast),
        and if it fails, falls back to df.count() on the filtered DataFrame.
        
        Args:
            source_config: Source database connection configuration
            table_name: Name of the table
            connection_manager: UnifiedConnectionManager instance for database access
            incremental_column: Name of the incremental column
            last_processed_value: Last processed value for the incremental column
            df: Optional DataFrame to count if SQL fails (fallback)
            incremental_strategy: Strategy type ('timestamp', 'primary_key', 'hash')
            
        Returns:
            Tuple of (row_count, counting_method) where counting_method is:
            - 'incremental_sql': SQL COUNT(*) with WHERE succeeded
            - 'dataframe_fallback': Fell back to df.count()
        """
        try:
            row_count = self.execute_incremental_count_sql(
                source_config, table_name, connection_manager,
                incremental_column, last_processed_value, incremental_strategy
            )
            return row_count, 'incremental_sql'
            
        except Exception as sql_error:
            self.structured_logger.warning(
                f"Incremental SQL COUNT(*) failed for {table_name}, falling back to DataFrame count",
                table_name=table_name,
                source_engine=source_config.engine_type,
                incremental_column=incremental_column,
                error=str(sql_error),
                error_type=type(sql_error).__name__,
                fallback_strategy="dataframe_count"
            )
            
            # Publish fallback metric
            if self.metrics_publisher:
                self.metrics_publisher.publish_error_metrics(
                    error_category="INCREMENTAL_SQL_COUNT_FALLBACK",
                    error_operation="incremental_count_sql"
                )
            
            # Fall back to DataFrame count if DataFrame is provided
            if df is not None:
                row_count = df.count()
                return row_count, 'dataframe_fallback'
            else:
                # No DataFrame provided, return 0 and let caller handle
                return 0, 'dataframe_fallback'
