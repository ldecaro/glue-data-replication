"""
Incremental column detection for AWS Glue Data Replication.

This module provides automatic detection of suitable columns for incremental
loading strategies based on table schema analysis.
"""

import logging
from typing import Dict, Any
# Conditional imports for PySpark (only available in Glue runtime)
try:
    from pyspark.sql.types import StructType, TimestampType, DateType, IntegerType, LongType
except ImportError:
    # Mock classes for local development/testing
    class StructType:
        pass
    class TimestampType:
        pass
    class DateType:
        pass
    class IntegerType:
        pass
    class LongType:
        pass

# Import from other modules
from ..config.job_config import ConnectionConfig
from .connection_manager import UnifiedConnectionManager

logger = logging.getLogger(__name__)


class IncrementalColumnDetector:
    """Detects suitable columns for incremental loading strategies."""
    
    # Common timestamp column names
    TIMESTAMP_COLUMN_NAMES = [
        'updated_at', 'modified_at', 'last_modified', 'last_updated',
        'created_at', 'insert_date', 'update_date', 'modified_date',
        'timestamp', 'last_change', 'change_date', 'mod_time'
    ]
    
    # Common primary key column names
    PRIMARY_KEY_COLUMN_NAMES = [
        'id', 'pk', 'primary_key', 'key', 'seq', 'sequence',
        'row_id', 'record_id', 'unique_id'
    ]
    
    @classmethod
    def detect_incremental_strategy(cls, schema: StructType, table_name: str) -> Dict[str, Any]:
        """Detect the best incremental loading strategy for a table."""
        column_names = [field.name.lower() for field in schema.fields]
        column_types = {field.name.lower(): field.dataType for field in schema.fields}
        
        strategy_info = {
            'strategy': 'hash',  # Default fallback
            'column': None,
            'confidence': 0.0,
            'reason': 'No suitable incremental column found, using hash-based strategy'
        }
        
        # Strategy 1: Look for timestamp columns
        timestamp_candidates = []
        for col_name in column_names:
            col_type = column_types[col_name]
            
            # Check if it's a timestamp/date type
            type_name = type(col_type).__name__
            is_timestamp_type = False
            try:
                is_timestamp_type = (isinstance(col_type, (TimestampType, DateType)) or 
                                   type_name in ['MockTimestampType', 'MockDateType'] or
                                   'TimestampType' in type_name or 'DateType' in type_name)
            except TypeError:
                # Handle isinstance issues with mock types
                is_timestamp_type = (type_name in ['MockTimestampType', 'MockDateType'] or
                                   'TimestampType' in type_name or 'DateType' in type_name)
            
            if is_timestamp_type:
                confidence = 0.5  # Base confidence for timestamp types
                
                # Boost confidence for common timestamp column names
                for pattern in cls.TIMESTAMP_COLUMN_NAMES:
                    if pattern in col_name:
                        confidence += 0.3
                        break
                
                # Prefer 'updated_at' or 'modified_at' patterns
                if any(pattern in col_name for pattern in ['updated', 'modified', 'last_modified']):
                    confidence += 0.2
                
                timestamp_candidates.append((col_name, confidence))
        
        # Select best timestamp candidate
        if timestamp_candidates:
            best_timestamp = max(timestamp_candidates, key=lambda x: x[1])
            if best_timestamp[1] > strategy_info['confidence']:
                strategy_info = {
                    'strategy': 'timestamp',
                    'column': best_timestamp[0],
                    'confidence': best_timestamp[1],
                    'reason': f'Found timestamp column: {best_timestamp[0]}'
                }
        
        # Strategy 2: Look for auto-incrementing primary key columns
        pk_candidates = []
        for col_name in column_names:
            col_type = column_types[col_name]
            
            # Check if it's an integer type (potential auto-increment)
            type_name = type(col_type).__name__
            is_integer_type = False
            try:
                is_integer_type = (isinstance(col_type, (IntegerType, LongType)) or 
                                 type_name in ['MockIntegerType', 'MockLongType'] or
                                 'IntegerType' in type_name or 'LongType' in type_name)
            except TypeError:
                # Handle isinstance issues with mock types
                is_integer_type = (type_name in ['MockIntegerType', 'MockLongType'] or
                                 'IntegerType' in type_name or 'LongType' in type_name)
            
            if is_integer_type:
                confidence = 0.3  # Base confidence for integer types
                
                # Boost confidence for common primary key column names
                for pattern in cls.PRIMARY_KEY_COLUMN_NAMES:
                    if pattern == col_name or col_name.endswith('_' + pattern):
                        confidence += 0.4
                        break
                
                # Special boost for 'id' columns
                if col_name == 'id' or col_name.endswith('_id'):
                    confidence += 0.2
                
                pk_candidates.append((col_name, confidence))
        
        # Select best primary key candidate
        if pk_candidates:
            best_pk = max(pk_candidates, key=lambda x: x[1])
            if best_pk[1] > strategy_info['confidence']:
                strategy_info = {
                    'strategy': 'primary_key',
                    'column': best_pk[0],
                    'confidence': best_pk[1],
                    'reason': f'Found auto-increment primary key column: {best_pk[0]}'
                }
        
        logger.info(
            f"Incremental strategy for table {table_name}: {strategy_info['strategy']} "
            f"(column: {strategy_info['column']}, confidence: {strategy_info['confidence']:.2f}) - "
            f"{strategy_info['reason']}"
        )
        
        return strategy_info
    
    @classmethod
    def validate_incremental_column(cls, connection_manager: UnifiedConnectionManager,
                                  connection_config: ConnectionConfig, table_name: str,
                                  column_name: str, strategy: str) -> bool:
        """Validate that the incremental column is suitable for the strategy."""
        try:
            # Build validation query based on strategy
            if strategy == 'timestamp':
                # Check if column has reasonable timestamp values
                validation_query = f"""
                SELECT 
                    COUNT(*) as total_rows,
                    COUNT({column_name}) as non_null_rows,
                    MIN({column_name}) as min_value,
                    MAX({column_name}) as max_value
                FROM {connection_config.schema}.{table_name}
                """
            elif strategy == 'primary_key':
                # Check if column has sequential values
                validation_query = f"""
                SELECT 
                    COUNT(*) as total_rows,
                    COUNT(DISTINCT {column_name}) as unique_rows,
                    MIN({column_name}) as min_value,
                    MAX({column_name}) as max_value
                FROM {connection_config.schema}.{table_name}
                """
            else:
                return True  # Hash strategy doesn't need column validation
            
            df = connection_manager.read_table_data(
                connection_config=connection_config,
                table_name=table_name,
                query=validation_query
            )
            
            result = df.collect()[0]
            total_rows = result['total_rows']
            
            if strategy == 'timestamp':
                non_null_rows = result['non_null_rows']
                if non_null_rows < total_rows * 0.95:  # At least 95% non-null
                    logger.warning(
                        f"Timestamp column {column_name} has too many null values: "
                        f"{non_null_rows}/{total_rows} ({non_null_rows/total_rows*100:.1f}%)"
                    )
                    return False
            
            elif strategy == 'primary_key':
                unique_rows = result['unique_rows']
                if unique_rows < total_rows * 0.95:  # At least 95% unique
                    logger.warning(
                        f"Primary key column {column_name} has too many duplicate values: "
                        f"{unique_rows}/{total_rows} unique ({unique_rows/total_rows*100:.1f}%)"
                    )
                    return False
            
            logger.info(f"Incremental column validation passed for {table_name}.{column_name}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to validate incremental column {table_name}.{column_name}: {str(e)}")
            return False