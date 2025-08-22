"""
Job configuration dataclasses for AWS Glue Data Replication

This module contains the core configuration dataclasses that define
job parameters, network settings, and database connection configurations.
"""

from dataclasses import dataclass
from typing import Dict, List, Optional, Any


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
    
    def __post_init__(self):
        """Validate connection configuration after initialization."""
        self.validate()
    
    def validate(self) -> None:
        """Validate connection configuration parameters."""
        if not self.engine_type:
            raise ValueError("Engine type cannot be empty")
        
        # Import here to avoid circular imports
        from .database_engines import DatabaseEngineManager
        
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
        if not self.connection_string:
            raise ValueError("Connection string cannot be empty")
        if not self.database:
            raise ValueError("Database name cannot be empty")
        if not self.schema:
            raise ValueError("Schema name cannot be empty")
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


@dataclass
class JobConfig:
    """Main job configuration containing all parameters."""
    job_name: str
    source_connection: ConnectionConfig
    target_connection: ConnectionConfig
    tables: List[str]
    validate_connections: bool = True
    connection_timeout_seconds: int = 30
    
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