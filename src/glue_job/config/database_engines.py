"""
Database engine management and JDBC driver loading

This module provides utilities for managing database engine configurations
and loading JDBC drivers for different database types.
"""

import logging
from typing import List
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
            'default_port': 1521
        },
        'sqlserver': {
            'driver_class': 'com.microsoft.sqlserver.jdbc.SQLServerDriver',
            'url_template': 'jdbc:sqlserver://{host}:{port};databaseName={database}',
            'default_port': 1433
        },
        'postgresql': {
            'driver_class': 'org.postgresql.Driver',
            'url_template': 'jdbc:postgresql://{host}:{port}/{database}',
            'default_port': 5432
        },
        'db2': {
            'driver_class': 'com.ibm.db2.jcc.DB2Driver',
            'url_template': 'jdbc:db2://{host}:{port}/{database}',
            'default_port': 50000
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
        """Get JDBC driver class for the specified engine."""
        engine_type = engine_type.lower()
        if not cls.is_engine_supported(engine_type):
            raise ValueError(f"Unsupported database engine: {engine_type}")
        return cls.ENGINE_CONFIGS[engine_type]['driver_class']
    
    @classmethod
    def validate_connection_string(cls, engine_type: str, connection_string: str) -> bool:
        """Validate connection string format for the specified engine."""
        engine_type = engine_type.lower()
        if not cls.is_engine_supported(engine_type):
            return False
        
        # Basic validation - check if connection string starts with expected JDBC URL prefix
        expected_prefixes = {
            'oracle': 'jdbc:oracle:thin:@',
            'sqlserver': 'jdbc:sqlserver://',
            'postgresql': 'jdbc:postgresql://',
            'db2': 'jdbc:db2://'
        }
        
        return connection_string.startswith(expected_prefixes[engine_type])


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