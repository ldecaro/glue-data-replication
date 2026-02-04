"""
Job configuration dataclasses for AWS Glue Data Replication

This module contains the core configuration dataclasses that define
job parameters, network settings, and database connection configurations.
"""

import logging
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any


@dataclass
class GlueConnectionConfig:
    """Configuration for Glue Connection operations."""
    create_connection: bool = False
    use_existing_connection: Optional[str] = None
    
    @property
    def connection_strategy(self) -> str:
        """Determine the connection strategy based on configuration.
        
        Returns:
            str: Connection strategy - 'create_glue', 'use_glue', or 'direct_jdbc'
        """
        if self.create_connection:
            return "create_glue"
        elif self.use_existing_connection:
            return "use_glue"
        else:
            return "direct_jdbc"
    
    def uses_glue_connection(self) -> bool:
        """Check if this configuration uses Glue Connection.
        
        Returns:
            bool: True if using Glue Connection (create or use existing), False otherwise
        """
        return self.create_connection or bool(self.use_existing_connection)
    
    def validate(self) -> None:
        """Validate Glue Connection configuration.
        
        Raises:
            ValueError: If configuration is invalid
        """
        # Check for mutually exclusive parameters
        if self.create_connection and self.use_existing_connection:
            raise ValueError("Cannot both create and use existing Glue Connection - parameters are mutually exclusive")
        
        # Validate use_existing_connection parameter
        if self.use_existing_connection is not None:
            if not isinstance(self.use_existing_connection, str):
                raise ValueError("use_existing_connection must be a string")
            if not self.use_existing_connection.strip():
                raise ValueError("use_existing_connection cannot be empty when specified")
        
        # Validate create_connection parameter
        if not isinstance(self.create_connection, bool):
            raise ValueError("create_connection must be a boolean")


@dataclass
class NetworkConfig:
    """Network configuration for cross-VPC database connections."""
    vpc_id: Optional[str] = None
    subnet_ids: Optional[List[str]] = None
    security_group_ids: Optional[List[str]] = None
    glue_connection_name: Optional[str] = None
    create_s3_vpc_endpoint: bool = False
    
    def has_network_config(self) -> bool:
        """Check if network configuration is provided."""
        return bool(self.vpc_id and self.subnet_ids and self.security_group_ids)
    
    def requires_glue_connection(self) -> bool:
        """Check if Glue connection is required for cross-VPC access."""
        return self.has_network_config() and bool(self.glue_connection_name)
    
    def validate(self) -> None:
        """Validate network configuration parameters."""
        if self.vpc_id and not self.subnet_ids:
            raise ValueError("subnet_ids are required when vpc_id is specified")
        if self.vpc_id and not self.security_group_ids:
            raise ValueError("security_group_ids are required when vpc_id is specified")
        if self.glue_connection_name and not self.has_network_config():
            raise ValueError("Network configuration is required when glue_connection_name is specified")


@dataclass
class ConnectionConfig:
    """Configuration for database connection."""
    engine_type: str
    connection_string: str
    database: str
    schema: str
    username: str
    password: str
    jdbc_driver_path: str
    network_config: Optional[NetworkConfig] = None
    iceberg_config: Optional[Dict[str, Any]] = None
    glue_connection_config: Optional[GlueConnectionConfig] = None
    
    def __post_init__(self):
        """Validate connection configuration after initialization."""
        self.validate()
    
    def validate(self) -> None:
        """Validate connection configuration parameters."""
        if not self.engine_type:
            raise ValueError("Engine type cannot be empty")
        
        # Import here to avoid circular imports
        from .database_engines import DatabaseEngineManager
        
        # Validate Glue Connection configuration if present
        if self.glue_connection_config:
            self.glue_connection_config.validate()
            
            # Warn if Glue Connection parameters are provided for Iceberg engines
            if DatabaseEngineManager.is_iceberg_engine(self.engine_type):
                if self.glue_connection_config.uses_glue_connection():
                    logger = logging.getLogger(__name__)
                    logger.warning(
                        f"Glue Connection parameters provided for Iceberg engine '{self.engine_type}' "
                        "will be ignored. Iceberg connections use existing connection mechanism."
                    )
        
        # Skip JDBC validation for Iceberg engines
        if DatabaseEngineManager.is_iceberg_engine(self.engine_type):
            self._validate_iceberg_config()
        else:
            self._validate_jdbc_config()
    
    def _validate_iceberg_config(self) -> None:
        """Validate Iceberg-specific configuration."""
        if not self.database:
            raise ValueError("Database name cannot be empty for Iceberg engine")
        if not self.schema:  # schema field contains table_name for Iceberg
            raise ValueError("Table name cannot be empty for Iceberg engine")
        if not self.iceberg_config:
            raise ValueError("Iceberg configuration is required for Iceberg engine")
        
        # Validate Iceberg configuration using DatabaseEngineManager
        from .database_engines import DatabaseEngineManager
        if not DatabaseEngineManager.validate_iceberg_config(self.iceberg_config):
            raise ValueError("Invalid Iceberg configuration parameters")
    
    def _validate_jdbc_config(self) -> None:
        """Validate JDBC-specific configuration."""
        # When using existing Glue Connection, some JDBC parameters may not be required
        # as they will be retrieved from the Glue Connection metadata
        using_existing_glue_connection = (
            self.glue_connection_config and 
            self.glue_connection_config.use_existing_connection
        )
        
        # When creating a new Glue Connection, all JDBC parameters are required
        creating_glue_connection = (
            self.glue_connection_config and 
            self.glue_connection_config.create_connection
        )
        
        # These are always required regardless of connection strategy
        if not self.database:
            raise ValueError("Database name cannot be empty")
        if not self.schema:
            raise ValueError("Schema name cannot be empty")
        
        # Additional validation for Glue Connection creation (check first for specific error messages)
        if creating_glue_connection:
            if not self.connection_string:
                raise ValueError("Connection string is required when creating Glue Connection")
            if not self.username:
                raise ValueError("Username is required when creating Glue Connection")
            if not self.password:
                raise ValueError("Password is required when creating Glue Connection")
        
        # For direct JDBC or creating Glue Connection, validate all parameters
        elif not using_existing_glue_connection:
            if not self.connection_string:
                raise ValueError("Connection string cannot be empty")
            if not self.username:
                raise ValueError("Username cannot be empty")
            if not self.password:
                raise ValueError("Password cannot be empty")
            if not self.jdbc_driver_path:
                raise ValueError("JDBC driver path cannot be empty")
    
    def is_iceberg_engine(self) -> bool:
        """Check if this connection uses Iceberg engine."""
        from .database_engines import DatabaseEngineManager
        return DatabaseEngineManager.is_iceberg_engine(self.engine_type)
    
    def get_iceberg_config(self) -> Optional[Dict[str, Any]]:
        """Get Iceberg configuration if available."""
        return self.iceberg_config if self.is_iceberg_engine() else None
    
    def requires_cross_vpc_connection(self) -> bool:
        """Check if this connection requires cross-VPC connectivity."""
        return self.network_config and self.network_config.requires_glue_connection()
    
    def get_glue_connection_name(self) -> Optional[str]:
        """Get the Glue connection name if configured."""
        return self.network_config.glue_connection_name if self.network_config else None
    
    def uses_glue_connection(self) -> bool:
        """Check if this connection uses Glue Connection.
        
        Returns:
            bool: True if using Glue Connection (create or use existing), False otherwise
        """
        return (self.glue_connection_config and 
                self.glue_connection_config.uses_glue_connection())
    
    def get_glue_connection_strategy(self) -> str:
        """Get the Glue Connection strategy.
        
        Returns:
            str: Connection strategy - 'create_glue', 'use_glue', or 'direct_jdbc'
        """
        if self.glue_connection_config:
            return self.glue_connection_config.connection_strategy
        return "direct_jdbc"
    
    def get_glue_connection_name_for_creation(self) -> Optional[str]:
        """Get the Glue connection name for use with existing connections.
        
        Returns:
            Optional[str]: Connection name if using existing connection, None otherwise
        """
        if (self.glue_connection_config and 
            self.glue_connection_config.use_existing_connection):
            return self.glue_connection_config.use_existing_connection
        return None
    
    def should_create_glue_connection(self) -> bool:
        """Check if a new Glue Connection should be created.
        
        Returns:
            bool: True if should create new Glue Connection, False otherwise
        """
        return (self.glue_connection_config and 
                self.glue_connection_config.create_connection)
    
    def should_use_existing_glue_connection(self) -> bool:
        """Check if an existing Glue Connection should be used.
        
        Returns:
            bool: True if should use existing Glue Connection, False otherwise
        """
        return (self.glue_connection_config and 
                bool(self.glue_connection_config.use_existing_connection))


@dataclass
class MigrationPerformanceConfig:
    """Configuration for migration performance optimizations.
    
    This configuration controls counting strategies, progress tracking,
    and metrics publishing for large-scale data migrations.
    """
    
    # Counting strategy configuration
    counting_strategy: str = "auto"  # Options: "immediate", "deferred", "auto"
    size_threshold_rows: int = 1_000_000  # Threshold for auto strategy selection
    force_immediate_counting: bool = False
    force_deferred_counting: bool = False
    
    # Progress tracking configuration
    progress_update_interval_seconds: int = 60
    progress_batch_size_rows: int = 100_000
    enable_progress_tracking: bool = True
    enable_progress_logging: bool = True
    
    # Metrics configuration
    enable_detailed_metrics: bool = True
    metrics_namespace: str = "AWS/Glue/DataReplication"
    
    def validate(self):
        """Validate all configuration parameters.
        
        Raises:
            ValueError: If configuration is invalid
        """
        # Validate counting strategy
        valid_strategies = ["immediate", "deferred", "auto"]
        if self.counting_strategy not in valid_strategies:
            raise ValueError(
                f"Invalid counting_strategy '{self.counting_strategy}'. "
                f"Must be one of: {', '.join(valid_strategies)}"
            )
        
        # Validate mutually exclusive force flags
        if self.force_immediate_counting and self.force_deferred_counting:
            raise ValueError(
                "Cannot force both immediate and deferred counting strategies. "
                "Only one force flag can be True."
            )
        
        # Validate size threshold
        if self.size_threshold_rows <= 0:
            raise ValueError(
                f"size_threshold_rows must be positive, got {self.size_threshold_rows}"
            )
        
        # Validate progress tracking intervals
        if self.progress_update_interval_seconds <= 0:
            raise ValueError(
                f"progress_update_interval_seconds must be positive, "
                f"got {self.progress_update_interval_seconds}"
            )
        
        if self.progress_batch_size_rows <= 0:
            raise ValueError(
                f"progress_batch_size_rows must be positive, "
                f"got {self.progress_batch_size_rows}"
            )
        
        # Validate metrics namespace
        if not self.metrics_namespace or not self.metrics_namespace.strip():
            raise ValueError("metrics_namespace cannot be empty")
        
        # Warn if progress tracking is disabled but detailed metrics are enabled
        if self.enable_detailed_metrics and not self.enable_progress_tracking:
            logger = logging.getLogger(__name__)
            logger.warning(
                "Detailed metrics are enabled but progress tracking is disabled. "
                "Some metrics may not be available."
            )
    
    def get_counting_strategy_config(self):
        """Get CountingStrategyConfig from this configuration.
        
        Returns:
            CountingStrategyConfig instance
        """
        # Import here to avoid circular imports
        from ..database.counting_strategy import CountingStrategyConfig, CountingStrategyType
        
        # Map string strategy to enum
        strategy_map = {
            "immediate": CountingStrategyType.IMMEDIATE,
            "deferred": CountingStrategyType.DEFERRED,
            "auto": CountingStrategyType.AUTO
        }
        
        return CountingStrategyConfig(
            strategy_type=strategy_map[self.counting_strategy],
            size_threshold_rows=self.size_threshold_rows,
            force_immediate=self.force_immediate_counting,
            force_deferred=self.force_deferred_counting
        )
    
    def get_streaming_progress_config(self):
        """Get StreamingProgressConfig from this configuration.
        
        Returns:
            StreamingProgressConfig instance
        """
        # Import here to avoid circular imports
        from ..monitoring.streaming_progress_tracker import StreamingProgressConfig
        
        return StreamingProgressConfig(
            update_interval_seconds=self.progress_update_interval_seconds,
            batch_size_rows=self.progress_batch_size_rows,
            enable_metrics=self.enable_detailed_metrics,
            enable_logging=self.enable_progress_logging
        )


@dataclass
class JobConfig:
    """Main job configuration containing all parameters."""
    job_name: str
    source_connection: ConnectionConfig
    target_connection: ConnectionConfig
    tables: List[str]
    validate_connections: bool = True
    connection_timeout_seconds: int = 30
    manual_bookmark_config: Optional[str] = None
    migration_performance_config: MigrationPerformanceConfig = field(
        default_factory=MigrationPerformanceConfig
    )
    partitioned_read_config: Optional[Any] = None  # PartitionedReadConfig, imported dynamically
    
    def __post_init__(self):
        """Validate job configuration after initialization."""
        self.validate()
    
    def validate(self) -> None:
        """Validate job configuration parameters."""
        if not self.job_name:
            raise ValueError("Job name cannot be empty")
        if not self.tables:
            raise ValueError("Table list cannot be empty")
        
        # Validate connection configurations
        self.source_connection.validate()
        self.target_connection.validate()
        
        # Validate migration performance configuration
        self.migration_performance_config.validate()
    
    def has_cross_vpc_connections(self) -> bool:
        """Check if any connections require cross-VPC connectivity."""
        return (self.source_connection.requires_cross_vpc_connection() or 
                self.target_connection.requires_cross_vpc_connection())
    
    def get_network_summary(self) -> Dict[str, Any]:
        """Get summary of network configuration."""
        return {
            'source_cross_vpc': self.source_connection.requires_cross_vpc_connection(),
            'target_cross_vpc': self.target_connection.requires_cross_vpc_connection(),
            'source_glue_connection': self.source_connection.get_glue_connection_name(),
            'target_glue_connection': self.target_connection.get_glue_connection_name(),
            'validate_connections': self.validate_connections,
            'connection_timeout': self.connection_timeout_seconds
        }
    
    def get_performance_summary(self) -> Dict[str, Any]:
        """Get summary of migration performance configuration.
        
        Returns:
            Dictionary containing performance configuration summary
        """
        return {
            'counting_strategy': self.migration_performance_config.counting_strategy,
            'size_threshold_rows': self.migration_performance_config.size_threshold_rows,
            'progress_tracking_enabled': self.migration_performance_config.enable_progress_tracking,
            'progress_update_interval': self.migration_performance_config.progress_update_interval_seconds,
            'detailed_metrics_enabled': self.migration_performance_config.enable_detailed_metrics,
            'metrics_namespace': self.migration_performance_config.metrics_namespace
        }