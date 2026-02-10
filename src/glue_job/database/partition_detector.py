"""
Partition Column Detector

This module provides automatic detection of suitable partition columns
for parallel JDBC reads across different database engines.
"""

import logging
from typing import Optional, Tuple, Dict, Any
from dataclasses import dataclass

from ..config.database_engines import DatabaseEngineManager

logger = logging.getLogger(__name__)


@dataclass
class PartitionColumnInfo:
    """Information about a detected partition column."""
    
    column_name: str
    data_type: str
    min_value: int
    max_value: int
    row_count: int
    is_primary_key: bool = False
    is_indexed: bool = False
    
    @property
    def value_range(self) -> int:
        """Calculate the range of values."""
        return self.max_value - self.min_value + 1
    
    def calculate_optimal_partitions(self, target_rows_per_partition: int = 500000) -> int:
        """
        Calculate optimal number of partitions based on row count.
        
        Args:
            target_rows_per_partition: Target rows per partition (default 500K)
            
        Returns:
            Recommended number of partitions (between 2 and 100)
        """
        if self.row_count <= 0:
            return 4
        
        optimal = max(2, min(100, self.row_count // target_rows_per_partition))
        return optimal


class PartitionColumnDetector:
    """
    Detects suitable partition columns for parallel JDBC reads.
    
    Supports: SQL Server, Oracle, PostgreSQL, DB2
    """
    
    # SQL queries for detecting partition columns by engine type
    # Note: These queries avoid ORDER BY to prevent SQL Server subquery errors
    PRIMARY_KEY_QUERIES = {
        'sqlserver': """
            SELECT TOP 1 c.COLUMN_NAME, c.DATA_TYPE
            FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
            JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE ccu 
                ON tc.CONSTRAINT_NAME = ccu.CONSTRAINT_NAME
            JOIN INFORMATION_SCHEMA.COLUMNS c 
                ON c.TABLE_SCHEMA = ccu.TABLE_SCHEMA 
                AND c.TABLE_NAME = ccu.TABLE_NAME 
                AND c.COLUMN_NAME = ccu.COLUMN_NAME
            WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
                AND tc.TABLE_SCHEMA = '{schema}'
                AND tc.TABLE_NAME = '{table}'
                AND c.DATA_TYPE IN ('int', 'bigint', 'smallint', 'tinyint', 'numeric', 'decimal')
        """,
        'oracle': """
            SELECT column_name, data_type FROM (
                SELECT cols.column_name, cols.data_type,
                       ROW_NUMBER() OVER (ORDER BY cols.position) as rn
                FROM all_constraints cons
                JOIN all_cons_columns cols 
                    ON cons.constraint_name = cols.constraint_name
                    AND cons.owner = cols.owner
                WHERE cons.constraint_type = 'P'
                    AND cons.owner = UPPER('{schema}')
                    AND cons.table_name = UPPER('{table}')
                    AND cols.data_type IN ('NUMBER', 'INTEGER', 'SMALLINT', 'FLOAT')
            ) WHERE rn = 1
        """,
        'postgresql': """
            SELECT a.attname as column_name, format_type(a.atttypid, a.atttypmod) as data_type
            FROM pg_index i
            JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
            JOIN pg_class c ON c.oid = i.indrelid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE i.indisprimary
                AND n.nspname = '{schema}'
                AND c.relname = '{table}'
                AND format_type(a.atttypid, a.atttypmod) IN ('integer', 'bigint', 'smallint', 'numeric', 'decimal')
            LIMIT 1
        """,
        'db2': """
            SELECT c.COLNAME as column_name, c.TYPENAME as data_type
            FROM SYSCAT.KEYCOLUSE k
            JOIN SYSCAT.COLUMNS c 
                ON k.TABSCHEMA = c.TABSCHEMA 
                AND k.TABNAME = c.TABNAME 
                AND k.COLNAME = c.COLNAME
            JOIN SYSCAT.TABCONST tc 
                ON k.CONSTNAME = tc.CONSTNAME 
                AND k.TABSCHEMA = tc.TABSCHEMA
            WHERE tc.TYPE = 'P'
                AND k.TABSCHEMA = UPPER('{schema}')
                AND k.TABNAME = UPPER('{table}')
                AND c.TYPENAME IN ('INTEGER', 'BIGINT', 'SMALLINT', 'DECIMAL', 'NUMERIC')
            FETCH FIRST 1 ROWS ONLY
        """
    }
    
    # SQL queries for getting indexed numeric columns (fallback)
    INDEXED_COLUMN_QUERIES = {
        'sqlserver': """
            SELECT TOP 1 c.name as column_name, t.name as data_type
            FROM sys.indexes i
            JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
            JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
            JOIN sys.types t ON c.user_type_id = t.user_type_id
            WHERE i.object_id = OBJECT_ID('{schema}.{table}')
                AND ic.key_ordinal = 1
                AND t.name IN ('int', 'bigint', 'smallint', 'tinyint', 'numeric', 'decimal')
            ORDER BY i.is_primary_key DESC, i.is_unique DESC
        """,
        'oracle': """
            SELECT column_name, data_type
            FROM (
                SELECT ic.column_name, tc.data_type,
                       ROW_NUMBER() OVER (ORDER BY i.uniqueness DESC) as rn
                FROM all_indexes i
                JOIN all_ind_columns ic ON i.index_name = ic.index_name AND i.owner = ic.index_owner
                JOIN all_tab_columns tc ON ic.table_name = tc.table_name 
                    AND ic.column_name = tc.column_name AND ic.table_owner = tc.owner
                WHERE i.table_owner = UPPER('{schema}')
                    AND i.table_name = UPPER('{table}')
                    AND ic.column_position = 1
                    AND tc.data_type IN ('NUMBER', 'INTEGER', 'SMALLINT', 'FLOAT')
            ) WHERE rn = 1
        """,
        'postgresql': """
            SELECT a.attname as column_name, format_type(a.atttypid, a.atttypmod) as data_type
            FROM pg_index i
            JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = i.indkey[0]
            JOIN pg_class c ON c.oid = i.indrelid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = '{schema}'
                AND c.relname = '{table}'
                AND format_type(a.atttypid, a.atttypmod) IN ('integer', 'bigint', 'smallint', 'numeric', 'decimal')
            ORDER BY i.indisunique DESC
            LIMIT 1
        """,
        'db2': """
            SELECT c.COLNAME as column_name, c.TYPENAME as data_type
            FROM SYSCAT.INDEXCOLUSE ic
            JOIN SYSCAT.INDEXES i ON ic.INDSCHEMA = i.INDSCHEMA AND ic.INDNAME = i.INDNAME
            JOIN SYSCAT.COLUMNS c ON i.TABSCHEMA = c.TABSCHEMA AND i.TABNAME = c.TABNAME AND ic.COLNAME = c.COLNAME
            WHERE i.TABSCHEMA = UPPER('{schema}')
                AND i.TABNAME = UPPER('{table}')
                AND ic.COLSEQ = 1
                AND c.TYPENAME IN ('INTEGER', 'BIGINT', 'SMALLINT', 'DECIMAL', 'NUMERIC')
            ORDER BY i.UNIQUERULE
            FETCH FIRST 1 ROWS ONLY
        """
    }
    
    def __init__(self, spark_session, structured_logger=None):
        """
        Initialize the partition column detector.
        
        Args:
            spark_session: Active Spark session
            structured_logger: Optional structured logger
        """
        self.spark = spark_session
        self.logger = structured_logger or logger
    
    def detect_partition_column(
        self,
        connection_config,
        table_name: str
    ) -> Optional[PartitionColumnInfo]:
        """
        Detect the best partition column for a table.
        
        Strategy:
        1. Try to find primary key column (numeric)
        2. Fall back to first indexed numeric column
        3. Return None if no suitable column found
        
        Args:
            connection_config: Database connection configuration
            table_name: Name of the table
            
        Returns:
            PartitionColumnInfo if found, None otherwise
        """
        engine_type = connection_config.engine_type.lower()
        schema = connection_config.schema
        
        self.logger.info(
            f"Detecting partition column for {schema}.{table_name}",
            engine_type=engine_type
        )
        
        # Try primary key first
        pk_column = self._detect_primary_key_column(
            connection_config, schema, table_name, engine_type
        )
        if pk_column:
            return self._get_column_stats(
                connection_config, schema, table_name, 
                pk_column['column_name'], pk_column['data_type'],
                is_primary_key=True
            )
        
        # Fall back to indexed column
        idx_column = self._detect_indexed_column(
            connection_config, schema, table_name, engine_type
        )
        if idx_column:
            return self._get_column_stats(
                connection_config, schema, table_name,
                idx_column['column_name'], idx_column['data_type'],
                is_indexed=True
            )
        
        self.logger.warning(
            f"No suitable partition column found for {schema}.{table_name}",
            table_name=table_name
        )
        return None
    
    def _detect_primary_key_column(
        self,
        connection_config,
        schema: str,
        table_name: str,
        engine_type: str
    ) -> Optional[Dict[str, str]]:
        """Detect primary key column."""
        query_template = self.PRIMARY_KEY_QUERIES.get(engine_type)
        if not query_template:
            self.logger.warning(f"No PK detection query for engine: {engine_type}")
            return None
        
        query = query_template.format(schema=schema, table=table_name)
        return self._execute_metadata_query(connection_config, query, "primary key")
    
    def _detect_indexed_column(
        self,
        connection_config,
        schema: str,
        table_name: str,
        engine_type: str
    ) -> Optional[Dict[str, str]]:
        """Detect indexed numeric column."""
        query_template = self.INDEXED_COLUMN_QUERIES.get(engine_type)
        if not query_template:
            self.logger.warning(f"No index detection query for engine: {engine_type}")
            return None
        
        query = query_template.format(schema=schema, table=table_name)
        return self._execute_metadata_query(connection_config, query, "indexed column")
    
    def _execute_metadata_query(
        self,
        connection_config,
        query: str,
        query_type: str
    ) -> Optional[Dict[str, str]]:
        """Execute a metadata query and return first result."""
        try:
            self.logger.info(
                f"Executing {query_type} detection query",
                query_type=query_type,
                engine_type=connection_config.engine_type
            )
            
            df = self.spark.read.format("jdbc") \
                .option("url", connection_config.connection_string) \
                .option("user", connection_config.username) \
                .option("password", connection_config.password) \
                .option("driver", DatabaseEngineManager.get_driver_class(connection_config.engine_type)) \
                .option("query", query) \
                .load()
            
            rows = df.collect()
            if rows:
                row = rows[0]
                self.logger.info(
                    f"Found {query_type} column",
                    query_type=query_type,
                    column_name=row[0],
                    data_type=row[1]
                )
                return {
                    'column_name': row[0],
                    'data_type': row[1]
                }
            else:
                self.logger.info(
                    f"No {query_type} column found (query returned no rows)",
                    query_type=query_type
                )
        except Exception as e:
            self.logger.warning(
                f"Failed to detect {query_type}: {str(e)}",
                query_type=query_type,
                error_type=type(e).__name__,
                error_message=str(e)
            )
        return None
    
    def _get_column_stats(
        self,
        connection_config,
        schema: str,
        table_name: str,
        column_name: str,
        data_type: str,
        is_primary_key: bool = False,
        is_indexed: bool = False
    ) -> Optional[PartitionColumnInfo]:
        """Get statistics for a partition column."""
        try:
            stats_query = f"""
                SELECT 
                    MIN({column_name}) as min_val,
                    MAX({column_name}) as max_val,
                    COUNT(*) as row_count
                FROM {schema}.{table_name}
            """
            
            df = self.spark.read.format("jdbc") \
                .option("url", connection_config.connection_string) \
                .option("user", connection_config.username) \
                .option("password", connection_config.password) \
                .option("driver", DatabaseEngineManager.get_driver_class(connection_config.engine_type)) \
                .option("query", stats_query) \
                .load()
            
            row = df.collect()[0]
            min_val = int(row['min_val']) if row['min_val'] is not None else 0
            max_val = int(row['max_val']) if row['max_val'] is not None else 0
            row_count = int(row['row_count']) if row['row_count'] is not None else 0
            
            info = PartitionColumnInfo(
                column_name=column_name,
                data_type=data_type,
                min_value=min_val,
                max_value=max_val,
                row_count=row_count,
                is_primary_key=is_primary_key,
                is_indexed=is_indexed
            )
            
            self.logger.info(
                f"Detected partition column: {column_name}",
                table_name=table_name,
                column_name=column_name,
                data_type=data_type,
                min_value=min_val,
                max_value=max_val,
                row_count=row_count,
                is_primary_key=is_primary_key,
                is_indexed=is_indexed,
                optimal_partitions=info.calculate_optimal_partitions()
            )
            
            return info
            
        except Exception as e:
            self.logger.warning(
                f"Failed to get column stats for {column_name}: {str(e)}",
                table_name=table_name,
                column_name=column_name,
                error_type=type(e).__name__
            )
            return None
