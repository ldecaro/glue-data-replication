"""
Partitioned Read Configuration Handler

This module provides configuration management for parallel JDBC reads,
supporting both auto-detection of partition columns and manual overrides.
"""

import json
import logging
from dataclasses import dataclass, field
from typing import Dict, Any, Optional, List

logger = logging.getLogger(__name__)


@dataclass
class TablePartitionConfig:
    """Configuration for partitioned reads on a specific table."""
    
    table_name: str
    partition_column: Optional[str] = None  # None = auto-detect
    num_partitions: Optional[int] = None    # None = auto-calculate
    fetch_size: int = 10000
    lower_bound: Optional[int] = None       # None = auto-detect from data
    upper_bound: Optional[int] = None       # None = auto-detect from data
    
    def __post_init__(self):
        """Validate configuration after initialization."""
        if self.num_partitions is not None and self.num_partitions < 0:
            raise ValueError(f"num_partitions must be non-negative, got {self.num_partitions}")
        if self.fetch_size < 100:
            raise ValueError(f"fetch_size must be at least 100, got {self.fetch_size}")


@dataclass
class PartitionedReadConfig:
    """
    Global configuration for partitioned JDBC reads.
    
    Supports:
    - Auto-detection of partition columns from primary keys/indexes
    - Per-table override configuration via JSON
    - Default settings for all tables
    """
    
    enabled: bool = True
    default_num_partitions: int = 0  # 0 = auto-calculate based on row count
    default_fetch_size: int = 10000
    table_configs: Dict[str, TablePartitionConfig] = field(default_factory=dict)
    
    @classmethod
    def from_args(
        cls,
        enable_partitioned_reads: str = 'auto',
        partitioned_read_config_json: str = '',
        default_num_partitions: int = 0,
        default_fetch_size: int = 10000
    ) -> 'PartitionedReadConfig':
        """
        Create configuration from Glue job arguments.
        
        Args:
            enable_partitioned_reads: 'auto' or 'disabled'
            partitioned_read_config_json: JSON string with per-table overrides
            default_num_partitions: Default partition count (0 = auto)
            default_fetch_size: Default JDBC fetch size
            
        Returns:
            PartitionedReadConfig instance
        """
        enabled = enable_partitioned_reads.lower() != 'disabled'
        
        table_configs = {}
        if partitioned_read_config_json and partitioned_read_config_json.strip():
            try:
                config_dict = json.loads(partitioned_read_config_json)
                for table_name, table_config in config_dict.items():
                    table_configs[table_name.lower()] = TablePartitionConfig(
                        table_name=table_name,
                        partition_column=table_config.get('partition_column'),
                        num_partitions=table_config.get('num_partitions'),
                        fetch_size=table_config.get('fetch_size', default_fetch_size),
                        lower_bound=table_config.get('lower_bound'),
                        upper_bound=table_config.get('upper_bound')
                    )
                logger.info(f"Loaded partitioned read config for {len(table_configs)} tables")
            except json.JSONDecodeError as e:
                logger.warning(f"Failed to parse partitioned read config JSON: {e}")
            except Exception as e:
                logger.warning(f"Error processing partitioned read config: {e}")
        
        return cls(
            enabled=enabled,
            default_num_partitions=default_num_partitions,
            default_fetch_size=default_fetch_size,
            table_configs=table_configs
        )
    
    def get_table_config(self, table_name: str) -> Optional[TablePartitionConfig]:
        """
        Get partition configuration for a specific table.
        
        Args:
            table_name: Name of the table
            
        Returns:
            TablePartitionConfig if configured, None otherwise
        """
        return self.table_configs.get(table_name.lower())
    
    def should_use_partitioned_read(self, table_name: str) -> bool:
        """
        Determine if partitioned reads should be used for a table.
        
        Args:
            table_name: Name of the table
            
        Returns:
            True if partitioned reads are enabled (globally or per-table)
        """
        if not self.enabled:
            return False
        
        # Check if table has explicit config
        table_config = self.get_table_config(table_name)
        if table_config and table_config.partition_column:
            return True
        
        # Auto-detection is enabled
        return self.enabled
