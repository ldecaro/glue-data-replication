"""
Manual bookmark configuration module for AWS Glue Data Replication.

This module provides manual bookmark configuration functionality including:
- ManualBookmarkConfig: Dataclass for manual bookmark configuration with validation
- BookmarkStrategyResolver: Resolves bookmark strategies using manual config or automatic detection
"""

import re
import logging
from dataclasses import dataclass
from typing import Dict, Optional, Tuple, Any, List


@dataclass
class ManualBookmarkConfig:
    """
    Configuration structure for manual bookmark settings.
    
    This dataclass represents manual bookmark configuration for a specific table,
    allowing users to override automatic bookmark detection by specifying the
    exact column to use for incremental loading.
    
    Attributes:
        table_name: Name of the table for bookmark configuration
        column_name: Name of the column to use for bookmark tracking
    """
    table_name: str
    column_name: str
    
    def __post_init__(self):
        """
        Validate table and column names after initialization.
        
        Validates that:
        - Both table_name and column_name are provided and non-empty
        - Names follow proper naming conventions (alphanumeric and underscores)
        - Names don't start with numbers
        
        Raises:
            ValueError: If validation fails for any field
        """
        # Validate required fields are provided and non-empty
        if not self.table_name or not isinstance(self.table_name, str):
            raise ValueError("table_name is required and must be a non-empty string")
        
        if not self.column_name or not isinstance(self.column_name, str):
            raise ValueError("column_name is required and must be a non-empty string")
        
        # Strip whitespace from names
        self.table_name = self.table_name.strip()
        self.column_name = self.column_name.strip()
        
        # Validate names are not empty after stripping
        if not self.table_name:
            raise ValueError("table_name cannot be empty or whitespace only")
        
        if not self.column_name:
            raise ValueError("column_name cannot be empty or whitespace only")
        
        # Validate naming conventions (alphanumeric and underscores, cannot start with number)
        name_pattern = re.compile(r'^[a-zA-Z_][a-zA-Z0-9_]*$')
        
        if not name_pattern.match(self.table_name):
            raise ValueError(
                f"table_name '{self.table_name}' must contain only alphanumeric characters "
                "and underscores, and cannot start with a number"
            )
        
        if not name_pattern.match(self.column_name):
            raise ValueError(
                f"column_name '{self.column_name}' must contain only alphanumeric characters "
                "and cannot start with a number"
            )
    
    @classmethod
    def from_dict(cls, data: Dict[str, str]) -> 'ManualBookmarkConfig':
        """
        Create ManualBookmarkConfig instance from dictionary data.
        
        Args:
            data: Dictionary containing table_name and column_name keys
            
        Returns:
            ManualBookmarkConfig instance
            
        Raises:
            ValueError: If required keys are missing or invalid
            TypeError: If data is not a dictionary
        """
        if not isinstance(data, dict):
            raise TypeError("Configuration data must be a dictionary")
        
        # Extract and validate required fields
        table_name = data.get('table_name')
        column_name = data.get('column_name')
        
        if table_name is None:
            raise ValueError("Missing required field 'table_name' in configuration data")
        
        if column_name is None:
            raise ValueError("Missing required field 'column_name' in configuration data")
        
        # Create instance (validation happens in __post_init__)
        return cls(
            table_name=str(table_name),
            column_name=str(column_name)
        )
    
    def to_dict(self) -> Dict[str, str]:
        """
        Convert ManualBookmarkConfig to dictionary.
        
        Returns:
            Dictionary representation of the configuration
        """
        return {
            'table_name': self.table_name,
            'column_name': self.column_name
        }
    
    def __str__(self) -> str:
        """String representation of the configuration."""
        return f"ManualBookmarkConfig(table='{self.table_name}', column='{self.column_name}')"
    
    def __repr__(self) -> str:
        """Detailed string representation of the configuration."""
        return f"ManualBookmarkConfig(table_name='{self.table_name}', column_name='{self.column_name}')"


class BookmarkStrategyResolver:
    """
    Resolves bookmark strategies using manual configuration or automatic detection.
    
    This class handles the resolution of bookmark strategies by prioritizing manual
    configuration over automatic detection. It integrates with JDBC metadata querying
    to determine appropriate strategies based on column data types.
    """
    
    # JDBC data type to bookmark strategy mapping
    JDBC_TYPE_TO_STRATEGY = {
        # Timestamp types -> timestamp strategy
        'TIMESTAMP': 'timestamp',
        'TIMESTAMP_WITH_TIMEZONE': 'timestamp',
        'DATE': 'timestamp',
        'DATETIME': 'timestamp',
        'TIME': 'timestamp',
        
        # Integer types -> primary_key strategy  
        'INTEGER': 'primary_key',
        'BIGINT': 'primary_key',
        'SMALLINT': 'primary_key',
        'TINYINT': 'primary_key',
        'SERIAL': 'primary_key',
        'BIGSERIAL': 'primary_key',
        
        # String and other types -> hash strategy
        'VARCHAR': 'hash',
        'CHAR': 'hash',
        'TEXT': 'hash',
        'CLOB': 'hash',
        'DECIMAL': 'hash',
        'NUMERIC': 'hash',
        'FLOAT': 'hash',
        'DOUBLE': 'hash',
        'BOOLEAN': 'hash',
        'BINARY': 'hash',
        'VARBINARY': 'hash',
        'BLOB': 'hash'
    }
    
    def __init__(self, manual_configs: Dict[str, ManualBookmarkConfig], structured_logger=None):
        """
        Initialize BookmarkStrategyResolver with manual configurations.
        
        Args:
            manual_configs: Dictionary mapping table names to ManualBookmarkConfig instances
            structured_logger: Optional structured logger instance for enhanced logging
        """
        self.manual_configs = manual_configs or {}
        self.jdbc_metadata_cache = {}
        self.logger = logging.getLogger(__name__)
        self.structured_logger = structured_logger or self.logger
    
    def resolve_strategy(self, table_name: str, connection) -> Tuple[str, Optional[str], bool]:
        """
        Resolve bookmark strategy for a table using manual config or automatic detection.
        
        Args:
            table_name: Name of the table to resolve strategy for
            connection: JDBC connection for metadata queries
            
        Returns:
            Tuple of (strategy, column_name, is_manually_configured)
        """
        import time
        start_time = time.time()
        
        has_manual_config = table_name in self.manual_configs
        
        # Log start of strategy resolution
        if hasattr(self.structured_logger, 'log_bookmark_strategy_resolution_start'):
            self.structured_logger.log_bookmark_strategy_resolution_start(table_name, has_manual_config)
        
        # Check for manual configuration first
        manual_result = self._get_manual_strategy(table_name, connection)
        if manual_result:
            strategy, column_name, data_type = manual_result
            resolution_duration_ms = (time.time() - start_time) * 1000
            
            # Log successful manual strategy resolution
            if hasattr(self.structured_logger, 'log_bookmark_strategy_resolution_success'):
                self.structured_logger.log_bookmark_strategy_resolution_success(
                    table_name, strategy, column_name, True, resolution_duration_ms, data_type
                )
            else:
                self.logger.info(
                    f"Manual bookmark configuration resolved for table '{table_name}': "
                    f"column='{column_name}', strategy='{strategy}', data_type='{data_type}'"
                )
            
            return strategy, column_name, True
        
        # Fall back to automatic detection
        strategy, column_name = self._get_automatic_strategy(table_name, connection)
        resolution_duration_ms = (time.time() - start_time) * 1000
        
        # Log fallback to automatic detection if manual config was attempted
        if has_manual_config and hasattr(self.structured_logger, 'log_bookmark_strategy_resolution_fallback'):
            self.structured_logger.log_bookmark_strategy_resolution_fallback(
                table_name, "manual", "automatic", "manual_config_validation_failed", strategy, column_name
            )
        
        # Log successful automatic strategy resolution
        if hasattr(self.structured_logger, 'log_bookmark_strategy_resolution_success'):
            self.structured_logger.log_bookmark_strategy_resolution_success(
                table_name, strategy, column_name, False, resolution_duration_ms
            )
        else:
            self.logger.info(
                f"Automatic bookmark detection for table '{table_name}': "
                f"column='{column_name}', strategy='{strategy}'"
            )
        
        return strategy, column_name, False
    
    def _get_manual_strategy(self, table_name: str, connection) -> Optional[Tuple[str, str, str]]:
        """
        Get bookmark strategy from manual configuration.
        
        Args:
            table_name: Name of the table
            connection: JDBC connection for metadata queries
            
        Returns:
            Tuple of (strategy, column_name, data_type) if manual config exists and is valid, None otherwise
        """
        if table_name not in self.manual_configs:
            return None
        
        manual_config = self.manual_configs[table_name]
        column_name = manual_config.column_name
        
        import time
        validation_start_time = time.time()
        
        # Log start of manual configuration validation
        if hasattr(self.structured_logger, 'log_manual_config_validation_start'):
            self.structured_logger.log_manual_config_validation_start(table_name, column_name)
        
        try:
            # Query JDBC metadata to get column data type
            column_metadata = self._get_column_metadata(connection, table_name, column_name)
            
            if not column_metadata:
                validation_duration_ms = (time.time() - validation_start_time) * 1000
                
                # Log column not found error
                if hasattr(self.structured_logger, 'log_manual_config_column_not_found'):
                    self.structured_logger.log_manual_config_column_not_found(table_name, column_name)
                
                if hasattr(self.structured_logger, 'log_manual_config_validation_failure'):
                    self.structured_logger.log_manual_config_validation_failure(
                        table_name, column_name, f"Column '{column_name}' not found in table", 
                        validation_duration_ms, "automatic_detection"
                    )
                else:
                    self.logger.error(
                        f"Manual configuration column '{column_name}' not found in table '{table_name}', "
                        "falling back to automatic detection"
                    )
                return None
            
            # Map data type to strategy
            data_type = column_metadata.get('data_type', '').upper()
            strategy = self._map_jdbc_type_to_strategy(data_type)
            
            validation_duration_ms = (time.time() - validation_start_time) * 1000
            
            # Log successful validation
            if hasattr(self.structured_logger, 'log_manual_config_validation_success'):
                self.structured_logger.log_manual_config_validation_success(
                    table_name, column_name, validation_duration_ms
                )
            
            # Log data type mapping
            if hasattr(self.structured_logger, 'log_data_type_mapping_success'):
                self.structured_logger.log_data_type_mapping_success(
                    data_type, strategy, table_name, column_name, "direct"
                )
            else:
                self.logger.info(
                    f"Manual bookmark configuration validated for table '{table_name}': "
                    f"column='{column_name}', data_type='{data_type}', strategy='{strategy}'"
                )
            
            return strategy, column_name, data_type
            
        except Exception as e:
            validation_duration_ms = (time.time() - validation_start_time) * 1000
            
            # Log validation failure
            if hasattr(self.structured_logger, 'log_manual_config_validation_failure'):
                self.structured_logger.log_manual_config_validation_failure(
                    table_name, column_name, str(e), validation_duration_ms, "automatic_detection"
                )
            else:
                self.logger.error(
                    f"Error processing manual configuration for table '{table_name}': {e}, "
                    "falling back to automatic detection"
                )
            return None
    
    def _get_automatic_strategy(self, table_name: str, connection) -> Tuple[str, Optional[str]]:
        """
        Get bookmark strategy using automatic detection (existing logic).
        
        This method integrates with the existing IncrementalColumnDetector to provide
        automatic detection as a fallback when manual configuration is not available.
        
        Args:
            table_name: Name of the table
            connection: JDBC connection for metadata queries
            
        Returns:
            Tuple of (strategy, column_name)
        """
        try:
            self.logger.info(f"Using automatic detection for table '{table_name}'")
            
            # Basic automatic detection logic (simplified)
            # Try to detect common timestamp columns via JDBC metadata
            timestamp_columns = self._detect_timestamp_columns(connection, table_name)
            if timestamp_columns:
                best_column = self._select_best_timestamp_column(timestamp_columns)
                self.logger.info(f"Automatic detection found timestamp column '{best_column}' for table '{table_name}'")
                return 'timestamp', best_column
            
            # Try to detect primary key columns
            pk_columns = self._detect_primary_key_columns(connection, table_name)
            if pk_columns:
                best_column = self._select_best_primary_key_column(pk_columns)
                self.logger.info(f"Automatic detection found primary key column '{best_column}' for table '{table_name}'")
                return 'primary_key', best_column
            
            # Fallback to hash strategy
            self.logger.info(f"Automatic detection defaulting to hash strategy for table '{table_name}'")
            return 'hash', None
            
        except Exception as e:
            self.logger.error(f"Error in automatic detection for table '{table_name}': {e}")
            # Fallback to hash strategy on any error
            return 'hash', None
    
    def _detect_timestamp_columns(self, connection, table_name: str) -> List[str]:
        """
        Detect timestamp columns using JDBC metadata.
        
        Args:
            connection: JDBC connection
            table_name: Name of the table
            
        Returns:
            List of timestamp column names
        """
        timestamp_columns = []
        
        try:
            metadata = connection.getMetaData()
            result_set = metadata.getColumns(None, None, table_name, None)
            
            # Common timestamp column name patterns
            timestamp_patterns = [
                'updated_at', 'modified_at', 'last_modified', 'last_updated',
                'created_at', 'insert_date', 'update_date', 'modified_date',
                'timestamp', 'last_change', 'change_date', 'mod_time'
            ]
            
            # Add safety counter to prevent infinite loops in testing
            max_iterations = 1000
            iteration_count = 0
            
            while result_set.next() and iteration_count < max_iterations:
                iteration_count += 1
                column_name = result_set.getString('COLUMN_NAME').lower()
                data_type = result_set.getString('TYPE_NAME').upper()
                
                # Check if it's a timestamp type
                is_timestamp_type = any(ts_type in data_type for ts_type in 
                                      ['TIMESTAMP', 'DATE', 'TIME', 'DATETIME'])
                
                # Check if name matches common patterns
                matches_pattern = any(pattern in column_name for pattern in timestamp_patterns)
                
                if is_timestamp_type and (matches_pattern or 'date' in column_name or 'time' in column_name):
                    timestamp_columns.append(column_name)
            
            if iteration_count >= max_iterations:
                self.logger.warning(f"Reached maximum iterations ({max_iterations}) while detecting timestamp columns for {table_name}")
            
        except Exception as e:
            self.logger.error(f"Error detecting timestamp columns for {table_name}: {e}")
        
        return timestamp_columns
    
    def _detect_primary_key_columns(self, connection, table_name: str) -> List[str]:
        """
        Detect primary key columns using JDBC metadata.
        
        Args:
            connection: JDBC connection
            table_name: Name of the table
            
        Returns:
            List of primary key column names
        """
        pk_columns = []
        
        try:
            metadata = connection.getMetaData()
            
            # First, try to get actual primary keys
            pk_result_set = metadata.getPrimaryKeys(None, None, table_name)
            
            # Add safety counter to prevent infinite loops in testing
            max_iterations = 1000
            iteration_count = 0
            
            while pk_result_set.next() and iteration_count < max_iterations:
                iteration_count += 1
                column_name = pk_result_set.getString('COLUMN_NAME').lower()
                pk_columns.append(column_name)
            
            # If no primary keys found, look for integer columns with common PK names
            if not pk_columns:
                result_set = metadata.getColumns(None, None, table_name, None)
                
                pk_patterns = [
                    'id', 'pk', 'primary_key', 'key', 'seq', 'sequence',
                    'row_id', 'record_id', 'unique_id'
                ]
                
                iteration_count = 0
                while result_set.next() and iteration_count < max_iterations:
                    iteration_count += 1
                    column_name = result_set.getString('COLUMN_NAME').lower()
                    data_type = result_set.getString('TYPE_NAME').upper()
                    
                    # Check if it's an integer type
                    is_integer_type = any(int_type in data_type for int_type in 
                                        ['INT', 'BIGINT', 'SMALLINT', 'SERIAL'])
                    
                    # Check if name matches common patterns
                    matches_pattern = any(pattern == column_name or column_name.endswith('_' + pattern) 
                                        for pattern in pk_patterns)
                    
                    if is_integer_type and matches_pattern:
                        pk_columns.append(column_name)
                
                if iteration_count >= max_iterations:
                    self.logger.warning(f"Reached maximum iterations ({max_iterations}) while detecting primary key columns for {table_name}")
            
        except Exception as e:
            self.logger.error(f"Error detecting primary key columns for {table_name}: {e}")
        
        return pk_columns
    
    def _select_best_timestamp_column(self, timestamp_columns: List[str]) -> str:
        """
        Select the best timestamp column from candidates.
        
        Args:
            timestamp_columns: List of timestamp column candidates
            
        Returns:
            Best timestamp column name
        """
        if not timestamp_columns:
            return timestamp_columns[0] if timestamp_columns else None
        
        # Preference order for timestamp columns
        preferred_patterns = [
            'updated_at', 'last_updated', 'modified_at', 'last_modified',
            'update_date', 'modified_date', 'last_change', 'change_date'
        ]
        
        # Look for preferred patterns first
        for pattern in preferred_patterns:
            for column in timestamp_columns:
                if pattern in column:
                    return column
        
        # If no preferred pattern found, return the first one
        return timestamp_columns[0]
    
    def _select_best_primary_key_column(self, pk_columns: List[str]) -> str:
        """
        Select the best primary key column from candidates.
        
        Args:
            pk_columns: List of primary key column candidates
            
        Returns:
            Best primary key column name
        """
        if not pk_columns:
            return None
        
        # Preference order for primary key columns
        preferred_patterns = ['id', 'pk', 'primary_key', 'key']
        
        # Look for exact matches first
        for pattern in preferred_patterns:
            for column in pk_columns:
                if column == pattern:
                    return column
        
        # Then look for columns ending with the pattern
        for pattern in preferred_patterns:
            for column in pk_columns:
                if column.endswith('_' + pattern):
                    return column
        
        # If no preferred pattern found, return the first one
        return pk_columns[0]
    
    def _get_column_metadata(self, connection, table_name: str, column_name: str) -> Optional[Dict[str, Any]]:
        """
        Query JDBC metadata to get column information with caching.
        
        Args:
            connection: JDBC connection
            table_name: Name of the table
            column_name: Name of the column
            
        Returns:
            Dictionary with column metadata or None if column not found
        """
        import time
        
        # Create cache key
        cache_key = f"{table_name}.{column_name}"
        
        # Check cache first
        if cache_key in self.jdbc_metadata_cache:
            cached_result = self.jdbc_metadata_cache[cache_key]
            if cached_result and hasattr(self.structured_logger, 'log_jdbc_metadata_cache_hit'):
                self.structured_logger.log_jdbc_metadata_cache_hit(
                    table_name, column_name, cached_result.get('data_type', 'unknown')
                )
            return cached_result
        
        query_start_time = time.time()
        
        # Log start of JDBC metadata query
        if hasattr(self.structured_logger, 'log_jdbc_metadata_query_start'):
            self.structured_logger.log_jdbc_metadata_query_start(table_name, column_name, "column_metadata")
        
        try:
            # Get database metadata
            metadata = connection.getMetaData()
            
            # Query column information
            result_set = metadata.getColumns(None, None, table_name, column_name)
            
            column_info = None
            if result_set.next():
                column_info = {
                    'column_name': result_set.getString('COLUMN_NAME'),
                    'data_type': result_set.getString('TYPE_NAME'),
                    'jdbc_type': result_set.getInt('DATA_TYPE'),
                    'column_size': result_set.getInt('COLUMN_SIZE'),
                    'nullable': result_set.getInt('NULLABLE')
                }
            
            query_duration_ms = (time.time() - query_start_time) * 1000
            
            # Cache the result (even if None)
            self.jdbc_metadata_cache[cache_key] = column_info
            
            if column_info:
                # Log successful query
                if hasattr(self.structured_logger, 'log_jdbc_metadata_query_success'):
                    self.structured_logger.log_jdbc_metadata_query_success(
                        table_name, column_name, column_info['data_type'], 
                        column_info['jdbc_type'], query_duration_ms, False
                    )
                else:
                    self.logger.debug(
                        f"Retrieved metadata for {table_name}.{column_name}: "
                        f"type={column_info['data_type']}, jdbc_type={column_info['jdbc_type']}"
                    )
            else:
                # Log column not found
                if hasattr(self.structured_logger, 'log_jdbc_metadata_query_failure'):
                    self.structured_logger.log_jdbc_metadata_query_failure(
                        table_name, column_name, f"Column '{column_name}' not found in table '{table_name}'",
                        query_duration_ms, "return_none"
                    )
                else:
                    self.logger.warning(f"Column {column_name} not found in table {table_name}")
            
            return column_info
            
        except Exception as e:
            query_duration_ms = (time.time() - query_start_time) * 1000
            
            # Log query failure
            if hasattr(self.structured_logger, 'log_jdbc_metadata_query_failure'):
                self.structured_logger.log_jdbc_metadata_query_failure(
                    table_name, column_name, str(e), query_duration_ms, "cache_failure_and_return_none"
                )
            else:
                self.logger.error(f"Failed to query metadata for {table_name}.{column_name}: {e}")
            
            # Cache the failure to avoid repeated queries
            self.jdbc_metadata_cache[cache_key] = None
            return None
    
    def _map_jdbc_type_to_strategy(self, jdbc_data_type: str) -> str:
        """
        Map JDBC data type to bookmark strategy.
        
        Args:
            jdbc_data_type: JDBC data type name (e.g., 'TIMESTAMP', 'INTEGER')
            
        Returns:
            Bookmark strategy ('timestamp', 'primary_key', or 'hash')
        """
        # Log start of data type mapping
        if hasattr(self.structured_logger, 'log_data_type_mapping_start'):
            self.structured_logger.log_data_type_mapping_start(jdbc_data_type, "unknown", "unknown")
        
        # Normalize the data type (uppercase, remove extra spaces)
        normalized_type = jdbc_data_type.upper().strip()
        
        # Direct mapping lookup
        strategy = self.JDBC_TYPE_TO_STRATEGY.get(normalized_type)
        
        if strategy:
            # Log successful direct mapping
            if hasattr(self.structured_logger, 'log_data_type_mapping_success'):
                self.structured_logger.log_data_type_mapping_success(
                    jdbc_data_type, strategy, "unknown", "unknown", "direct"
                )
            return strategy
        
        # Fallback logic for partial matches or database-specific variations
        if any(ts_type in normalized_type for ts_type in ['TIMESTAMP', 'DATE', 'TIME']):
            fallback_strategy = 'timestamp'
            if hasattr(self.structured_logger, 'log_data_type_mapping_success'):
                self.structured_logger.log_data_type_mapping_success(
                    jdbc_data_type, fallback_strategy, "unknown", "unknown", "partial_match"
                )
            return fallback_strategy
        elif any(int_type in normalized_type for int_type in ['INT', 'SERIAL', 'NUMBER']):
            fallback_strategy = 'primary_key'
            if hasattr(self.structured_logger, 'log_data_type_mapping_success'):
                self.structured_logger.log_data_type_mapping_success(
                    jdbc_data_type, fallback_strategy, "unknown", "unknown", "partial_match"
                )
            return fallback_strategy
        else:
            # Default to hash strategy for unknown types
            fallback_strategy = 'hash'
            if hasattr(self.structured_logger, 'log_data_type_mapping_fallback'):
                self.structured_logger.log_data_type_mapping_fallback(
                    jdbc_data_type, fallback_strategy, "unknown", "unknown", "unknown_type"
                )
            else:
                self.logger.warning(
                    f"Unknown JDBC data type '{jdbc_data_type}', defaulting to 'hash' strategy"
                )
            return fallback_strategy
    
    def clear_cache(self):
        """Clear the JDBC metadata cache."""
        self.jdbc_metadata_cache.clear()
        self.logger.debug("JDBC metadata cache cleared")
    
    def get_cache_stats(self) -> Dict[str, int]:
        """
        Get cache statistics.
        
        Returns:
            Dictionary with cache statistics
        """
        return {
            'cached_entries': len(self.jdbc_metadata_cache),
            'manual_configs': len(self.manual_configs)
        }