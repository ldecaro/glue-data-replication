"""
Database engine management and JDBC driver loading

This module provides utilities for managing database engine configurations
and loading JDBC drivers for different database types.
"""

import logging
from typing import List, Dict, Any
from urllib.parse import urlparse

# Conditional import for PySpark (only available in Glue runtime)
try:
    from pyspark.context import SparkContext
except ImportError:
    # Mock SparkContext for local development/testing
    class SparkContext:
        def addPyFile(self, path: str) -> None:
            pass

logger = logging.getLogger(__name__)


class DatabaseEngineManager:
    """Manages database engine configurations and JDBC driver loading."""
    
    # Database engine configurations
    ENGINE_CONFIGS = {
        'oracle': {
            'driver_class': 'oracle.jdbc.OracleDriver',
            'url_template': 'jdbc:oracle:thin:@{host}:{port}:{database}',
            'default_port': 1521,
            'requires_jdbc': True
        },
        'sqlserver': {
            'driver_class': 'com.microsoft.sqlserver.jdbc.SQLServerDriver',
            'url_template': 'jdbc:sqlserver://{host}:{port};databaseName={database}',
            'default_port': 1433,
            'requires_jdbc': True
        },
        'postgresql': {
            'driver_class': 'org.postgresql.Driver',
            'url_template': 'jdbc:postgresql://{host}:{port}/{database}',
            'default_port': 5432,
            'requires_jdbc': True
        },
        'db2': {
            'driver_class': 'com.ibm.db2.jcc.DB2Driver',
            'url_template': 'jdbc:db2://{host}:{port}/{database}',
            'default_port': 50000,
            'requires_jdbc': True
        },
        'iceberg': {
            'catalog_name': 'glue_catalog',
            'warehouse_location': 's3://{bucket}/{prefix}/',
            'format_version': '2',
            'requires_jdbc': False,
            'supported_operations': ['read', 'write', 'create', 'append'],
            'required_parameters': ['database_name', 'table_name', 'warehouse_location'],
            'optional_parameters': ['catalog_id', 'format_version'],
            'excluded_source_params': ['SOURCE_DB_PASSWORD', 'SOURCE_DB_USER', 'SOURCE_SCHEMA', 'SOURCE_JDBC_DRIVER_S3_PATH', 'SOURCE_CONNECTION_STRING'],
            'excluded_target_params': ['TARGET_DB_PASSWORD', 'TARGET_DB_USER', 'TARGET_SCHEMA', 'TARGET_JDBC_DRIVER_S3_PATH', 'TARGET_CONNECTION_STRING'],
            'required_source_params': ['SOURCE_DATABASE', 'SOURCE_WAREHOUSE_LOCATION'],
            'required_target_params': ['TARGET_DATABASE', 'TARGET_WAREHOUSE_LOCATION']
        }
    }
    
    @classmethod
    def get_supported_engines(cls) -> List[str]:
        """Get list of supported database engines."""
        return list(cls.ENGINE_CONFIGS.keys())
    
    @classmethod
    def is_engine_supported(cls, engine_type: str) -> bool:
        """Check if database engine is supported."""
        return engine_type.lower() in cls.ENGINE_CONFIGS
    
    @classmethod
    def get_driver_class(cls, engine_type: str) -> str:
        """Get JDBC driver class for the specified engine.
        
        Args:
            engine_type: Database engine type
            
        Returns:
            str: JDBC driver class name
            
        Raises:
            ValueError: If engine type is not supported or doesn't use JDBC
        """
        engine_type = engine_type.lower()
        if not cls.is_engine_supported(engine_type):
            raise ValueError(f"Unsupported database engine: {engine_type}")
        
        # Iceberg doesn't use JDBC drivers
        if cls.is_iceberg_engine(engine_type):
            raise ValueError(f"Iceberg engine does not use JDBC drivers")
        
        driver_class = cls.ENGINE_CONFIGS[engine_type].get('driver_class')
        if not driver_class:
            raise ValueError(f"No JDBC driver class defined for engine: {engine_type}")
        
        return driver_class
    
    @classmethod
    def is_iceberg_engine(cls, engine_type: str) -> bool:
        """Check if engine is Iceberg type.
        
        Args:
            engine_type: Database engine type to check
            
        Returns:
            bool: True if engine is Iceberg, False otherwise
        """
        return engine_type.lower() == 'iceberg'
    
    @classmethod
    def get_engine_config(cls, engine_type: str) -> Dict[str, Any]:
        """Get configuration for the specified engine type.
        
        Args:
            engine_type: Database engine type
            
        Returns:
            Dict[str, Any]: Engine configuration dictionary
            
        Raises:
            ValueError: If engine type is not supported
        """
        engine_type = engine_type.lower()
        if not cls.is_engine_supported(engine_type):
            raise ValueError(f"Unsupported database engine: {engine_type}")
        return cls.ENGINE_CONFIGS[engine_type].copy()
    
    @classmethod
    def requires_jdbc(cls, engine_type: str) -> bool:
        """Check if engine requires JDBC connection.
        
        Args:
            engine_type: Database engine type
            
        Returns:
            bool: True if engine requires JDBC, False otherwise
        """
        engine_type = engine_type.lower()
        if not cls.is_engine_supported(engine_type):
            return True  # Default to requiring JDBC for unknown engines
        return cls.ENGINE_CONFIGS[engine_type].get('requires_jdbc', True)
    
    @classmethod
    def validate_iceberg_config(cls, config: dict) -> bool:
        """Validate Iceberg-specific configuration parameters.
        
        Args:
            config: Dictionary containing Iceberg configuration parameters
            
        Returns:
            bool: True if configuration is valid, False otherwise
        """
        if not isinstance(config, dict):
            logger.error("Iceberg configuration must be a dictionary")
            return False
        
        # Get required parameters for Iceberg engine
        iceberg_config = cls.ENGINE_CONFIGS.get('iceberg', {})
        required_params = iceberg_config.get('required_parameters', [])
        
        # Check if all required parameters are present and not empty
        for param in required_params:
            if param not in config:
                logger.error(f"Missing required Iceberg parameter: {param}")
                return False
            if not config[param] or (isinstance(config[param], str) and not config[param].strip()):
                logger.error(f"Required Iceberg parameter '{param}' cannot be empty")
                return False
        
        # Validate warehouse_location format (must be S3 URL)
        warehouse_location = config.get('warehouse_location', '')
        if warehouse_location:
            if not warehouse_location.startswith('s3://'):
                logger.error(f"Invalid warehouse_location format. Must be S3 URL: {warehouse_location}")
                return False
            # Validate S3 URL structure
            try:
                parsed = urlparse(warehouse_location)
                if not parsed.netloc or not parsed.path:
                    logger.error(f"Invalid S3 URL structure in warehouse_location: {warehouse_location}")
                    return False
            except Exception as e:
                logger.error(f"Failed to parse warehouse_location URL: {warehouse_location}, error: {str(e)}")
                return False
        
        # Validate format_version if provided
        format_version = config.get('format_version')
        if format_version and str(format_version) not in ['1', '2']:
            logger.error(f"Invalid format_version. Must be '1' or '2': {format_version}")
            return False
        
        # Validate database_name format (no special characters except underscore)
        database_name = config.get('database_name', '')
        if database_name and not database_name.replace('_', '').replace('-', '').isalnum():
            logger.error(f"Invalid database_name format. Only alphanumeric characters, underscores, and hyphens allowed: {database_name}")
            return False
        
        # Validate table_name format (no special characters except underscore)
        table_name = config.get('table_name', '')
        if table_name and not table_name.replace('_', '').replace('-', '').isalnum():
            logger.error(f"Invalid table_name format. Only alphanumeric characters, underscores, and hyphens allowed: {table_name}")
            return False
        
        # Validate catalog_id if provided (should be AWS account ID format)
        catalog_id = config.get('catalog_id')
        if catalog_id:
            if not isinstance(catalog_id, str) or not catalog_id.isdigit() or len(catalog_id) != 12:
                logger.error(f"Invalid catalog_id format. Must be a 12-digit AWS account ID: {catalog_id}")
                return False
        
        logger.info("Iceberg configuration validation passed")
        return True
    
    @classmethod
    def get_excluded_parameters(cls, engine_type: str, engine_role: str) -> List[str]:
        """Get list of parameters to exclude from validation for specific engine type and role.
        
        Args:
            engine_type: Database engine type (e.g., 'iceberg', 'oracle')
            engine_role: Engine role - 'source' or 'target'
            
        Returns:
            List[str]: List of parameter names to exclude from validation
        """
        engine_type = engine_type.lower()
        if not cls.is_engine_supported(engine_type):
            return []
        
        engine_config = cls.ENGINE_CONFIGS.get(engine_type, {})
        
        if engine_role.lower() == 'source':
            return engine_config.get('excluded_source_params', [])
        elif engine_role.lower() == 'target':
            return engine_config.get('excluded_target_params', [])
        
        return []
    
    @classmethod
    def get_required_parameters(cls, engine_type: str, engine_role: str) -> List[str]:
        """Get list of required parameters for specific engine type and role.
        
        Args:
            engine_type: Database engine type (e.g., 'iceberg', 'oracle')
            engine_role: Engine role - 'source' or 'target'
            
        Returns:
            List[str]: List of required parameter names for the engine and role
        """
        engine_type = engine_type.lower()
        if not cls.is_engine_supported(engine_type):
            return []
        
        engine_config = cls.ENGINE_CONFIGS.get(engine_type, {})
        
        if engine_role.lower() == 'source':
            return engine_config.get('required_source_params', [])
        elif engine_role.lower() == 'target':
            return engine_config.get('required_target_params', [])
        
        return []
    
    @classmethod
    def validate_connection_string(cls, engine_type: str, connection_string: str) -> bool:
        """Validate connection string format for the specified engine.
        
        Args:
            engine_type: Database engine type (e.g., 'oracle', 'postgresql', 'iceberg')
            connection_string: Connection string to validate
            
        Returns:
            bool: True if connection string is valid for the engine type
        """
        engine_type = engine_type.lower()
        if not cls.is_engine_supported(engine_type):
            logger.error(f"Unsupported database engine type: {engine_type}")
            return False
        
        # Bypass JDBC validation for Iceberg engine
        if cls.is_iceberg_engine(engine_type):
            # For Iceberg, we don't use traditional JDBC connection strings
            # Validation is handled by validate_iceberg_config method
            logger.info("Iceberg engine detected - bypassing JDBC connection string validation")
            return True
        
        # Validate connection string is not empty for JDBC engines
        if not connection_string or not connection_string.strip():
            logger.error(f"Connection string cannot be empty for {engine_type} engine")
            return False
        
        # Basic validation - check if connection string starts with expected JDBC URL prefix
        expected_prefixes = {
            'oracle': 'jdbc:oracle:thin:@',
            'sqlserver': 'jdbc:sqlserver://',
            'postgresql': 'jdbc:postgresql://',
            'db2': 'jdbc:db2://'
        }
        
        expected_prefix = expected_prefixes.get(engine_type)
        if not expected_prefix:
            logger.error(f"No JDBC prefix defined for engine type: {engine_type}")
            return False
        
        is_valid = connection_string.startswith(expected_prefix)
        if not is_valid:
            logger.error(f"Invalid connection string format for {engine_type}. Expected to start with: {expected_prefix}")
        
        return is_valid


class JdbcDriverLoader:
    """Handles JDBC driver loading and validation."""
    
    def __init__(self, spark_context: SparkContext):
        self.spark_context = spark_context
        self.loaded_drivers = set()
    
    def load_driver(self, engine_type: str, driver_s3_path: str) -> None:
        """Load JDBC driver from S3 path."""
        if not driver_s3_path.startswith('s3://'):
            raise ValueError(f"JDBC driver path must be an S3 URL: {driver_s3_path}")
        
        if driver_s3_path in self.loaded_drivers:
            logger.info(f"JDBC driver already loaded: {driver_s3_path}")
            return
        
        try:
            # Add the JAR file to Spark context
            self.spark_context.addPyFile(driver_s3_path)
            self.loaded_drivers.add(driver_s3_path)
            logger.info(f"Successfully loaded JDBC driver for {engine_type}: {driver_s3_path}")
        except Exception as e:
            raise RuntimeError(f"Failed to load JDBC driver from {driver_s3_path}: {str(e)}")
    
    def validate_driver_path(self, driver_s3_path: str) -> bool:
        """Validate S3 driver path format."""
        if not driver_s3_path.startswith('s3://'):
            return False
        
        # Parse S3 URL to validate format
        try:
            parsed = urlparse(driver_s3_path)
            return bool(parsed.netloc and parsed.path and parsed.path.endswith('.jar'))
        except Exception:
            return False