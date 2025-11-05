"""
Unit tests for UnifiedConnectionManager

Tests the routing logic between JDBC and Iceberg connections,
configuration validation, and error handling.
"""

import unittest
import pytest
from unittest.mock import Mock, patch, MagicMock
from dataclasses import dataclass
from typing import Optional

# Mock PySpark and Glue classes for testing
class MockSparkSession:
    def __init__(self):
        self.conf = MockSparkConf()
    
    def sql(self, query: str):
        return MockDataFrame()

class MockDataFrame:
    def __init__(self):
        self.schema = MockStructType()
    
    def limit(self, n: int):
        return self
    
    def collect(self):
        return [{'test_column': 1}]

class MockStructType:
    def __init__(self):
        self.fields = []

class MockGlueContext:
    def create_data_frame_from_catalog(self, database, table_name, additional_options=None):
        return MockDataFrame()

class MockSparkConf:
    def set(self, key: str, value: str):
        return self

# Mock connection configuration for testing
@dataclass
class MockNetworkConfig:
    vpc_id: Optional[str] = None
    subnet_ids: Optional[list] = None
    security_group_ids: Optional[list] = None
    glue_connection_name: Optional[str] = None
    create_s3_vpc_endpoint: bool = False
    
    def has_network_config(self) -> bool:
        return bool(self.vpc_id and self.subnet_ids and self.security_group_ids)
    
    def requires_glue_connection(self) -> bool:
        return self.has_network_config() and bool(self.glue_connection_name)

# Mock Glue Connection configuration for testing
@dataclass
class MockGlueConnectionConfig:
    create_connection: bool = False
    use_existing_connection: Optional[str] = None
    
    @property
    def connection_strategy(self) -> str:
        if self.create_connection:
            return "create_glue"
        elif self.use_existing_connection:
            return "use_glue"
        else:
            return "direct_jdbc"
    
    def uses_glue_connection(self) -> bool:
        return self.create_connection or bool(self.use_existing_connection)
    
    def validate(self) -> None:
        if self.create_connection and self.use_existing_connection:
            raise ValueError("Cannot both create and use existing Glue Connection")

@dataclass
class MockConnectionConfig:
    engine_type: str
    connection_string: str
    database: str
    schema: str
    username: str
    password: str
    jdbc_driver_path: str
    network_config: Optional[MockNetworkConfig] = None
    warehouse_location: str = ''
    catalog_id: Optional[str] = None
    table_name: str = ''
    format_version: str = '2'
    glue_connection_config: Optional[MockGlueConnectionConfig] = None
    
    def validate(self):
        pass
    
    def requires_cross_vpc_connection(self) -> bool:
        return self.network_config and self.network_config.requires_glue_connection()
    
    def get_glue_connection_name(self) -> Optional[str]:
        return self.network_config.glue_connection_name if self.network_config else None
    
    def get_iceberg_config(self):
        """Return Iceberg configuration as a dictionary for compatibility."""
        return {
            'warehouse_location': self.warehouse_location,
            'catalog_id': self.catalog_id,
            'format_version': self.format_version
        }
    
    def uses_glue_connection(self) -> bool:
        """Check if this connection uses Glue Connection."""
        return (self.glue_connection_config and 
                self.glue_connection_config.uses_glue_connection())
    
    def get_glue_connection_strategy(self) -> str:
        """Get the Glue Connection strategy."""
        if self.glue_connection_config:
            return self.glue_connection_config.connection_strategy
        return "direct_jdbc"
    
    def get_glue_connection_name_for_creation(self) -> Optional[str]:
        """Get the Glue connection name for use with existing connections."""
        if (self.glue_connection_config and 
            self.glue_connection_config.use_existing_connection):
            return self.glue_connection_config.use_existing_connection
        return None
    
    def should_create_glue_connection(self) -> bool:
        """Check if a new Glue Connection should be created."""
        return (self.glue_connection_config and 
                self.glue_connection_config.create_connection)
    
    def should_use_existing_glue_connection(self) -> bool:
        """Check if an existing Glue Connection should be used."""
        return (self.glue_connection_config and 
                bool(self.glue_connection_config.use_existing_connection))

# Import the class under test
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from glue_job.database.connection_manager import UnifiedConnectionManager


class TestUnifiedConnectionManager:
    """Test cases for UnifiedConnectionManager."""
    
    @pytest.fixture
    def mock_spark_session(self):
        """Create mock Spark session."""
        return MockSparkSession()
    
    @pytest.fixture
    def mock_glue_context(self):
        """Create mock Glue context."""
        return MockGlueContext()
    
    @pytest.fixture
    def unified_manager(self, mock_spark_session, mock_glue_context):
        """Create UnifiedConnectionManager instance for testing."""
        with patch('glue_job.database.connection_manager.JdbcConnectionManager'), \
             patch('glue_job.database.connection_manager.IcebergConnectionHandler'), \
             patch('glue_job.database.connection_manager.ConnectionRetryHandler'), \
             patch('glue_job.database.connection_manager.StructuredLogger'):
            
            manager = UnifiedConnectionManager(
                spark_session=mock_spark_session,
                glue_context=mock_glue_context
            )
            return manager
    
    @pytest.fixture
    def jdbc_connection_config(self):
        """Create JDBC connection configuration for testing."""
        return MockConnectionConfig(
            engine_type='postgresql',
            connection_string='jdbc:postgresql://localhost:5432/testdb',
            database='testdb',
            schema='public',
            username='testuser',
            password='testpass',
            jdbc_driver_path='s3://bucket/postgresql.jar'
        )
    
    @pytest.fixture
    def iceberg_connection_config(self):
        """Create Iceberg connection configuration for testing."""
        return MockConnectionConfig(
            engine_type='iceberg',
            connection_string='',  # Not used for Iceberg
            database='test_database',
            schema='',  # Not used for Iceberg
            username='',  # Not used for Iceberg
            password='',  # Not used for Iceberg
            jdbc_driver_path='',  # Not used for Iceberg
            warehouse_location='s3://test-bucket/warehouse/',
            catalog_id='123456789012',
            table_name='test_table'
        )
    
    def test_is_iceberg_engine_detection(self, unified_manager):
        """Test Iceberg engine type detection."""
        # Test Iceberg engine detection
        assert unified_manager.is_iceberg_engine('iceberg') is True
        assert unified_manager.is_iceberg_engine('ICEBERG') is True
        
        # Test non-Iceberg engines
        assert unified_manager.is_iceberg_engine('postgresql') is False
        assert unified_manager.is_iceberg_engine('oracle') is False
        assert unified_manager.is_iceberg_engine('sqlserver') is False
    
    @patch('glue_job.database.connection_manager.DatabaseEngineManager.is_engine_supported')
    def test_validate_connection_config_unsupported_engine(self, mock_is_supported, unified_manager):
        """Test validation with unsupported engine type."""
        mock_is_supported.return_value = False
        
        config = MockConnectionConfig(
            engine_type='unsupported',
            connection_string='',
            database='test',
            schema='test',
            username='test',
            password='test',
            jdbc_driver_path=''
        )
        
        with pytest.raises(ValueError, match="Unsupported database engine"):
            unified_manager.validate_connection_config(config)
    
    @patch('glue_job.database.connection_manager.DatabaseEngineManager.is_engine_supported')
    @patch('glue_job.database.connection_manager.DatabaseEngineManager.validate_iceberg_config')
    def test_validate_iceberg_connection_config_success(self, mock_validate_iceberg, 
                                                       mock_is_supported, unified_manager, 
                                                       iceberg_connection_config):
        """Test successful Iceberg connection configuration validation."""
        mock_is_supported.return_value = True
        mock_validate_iceberg.return_value = True
        
        result = unified_manager.validate_connection_config(iceberg_connection_config)
        
        assert result is True
        mock_validate_iceberg.assert_called_once()
    
    @patch('glue_job.database.connection_manager.DatabaseEngineManager.is_engine_supported')
    @patch('glue_job.database.connection_manager.DatabaseEngineManager.validate_iceberg_config')
    def test_validate_iceberg_connection_config_failure(self, mock_validate_iceberg, 
                                                       mock_is_supported, unified_manager, 
                                                       iceberg_connection_config):
        """Test failed Iceberg connection configuration validation."""
        mock_is_supported.return_value = True
        mock_validate_iceberg.return_value = False
        
        with pytest.raises(Exception):  # Should raise IcebergValidationError
            unified_manager.validate_connection_config(iceberg_connection_config)
    
    @patch('glue_job.database.connection_manager.DatabaseEngineManager.is_engine_supported')
    @patch('glue_job.database.connection_manager.DatabaseEngineManager.validate_connection_string')
    def test_validate_jdbc_connection_config_success(self, mock_validate_string, 
                                                    mock_is_supported, unified_manager, 
                                                    jdbc_connection_config):
        """Test successful JDBC connection configuration validation."""
        mock_is_supported.return_value = True
        mock_validate_string.return_value = True
        
        result = unified_manager.validate_connection_config(jdbc_connection_config)
        
        assert result is True
        mock_validate_string.assert_called_once_with(
            jdbc_connection_config.engine_type,
            jdbc_connection_config.connection_string
        )
    
    @patch('glue_job.database.connection_manager.DatabaseEngineManager.is_engine_supported')
    @patch('glue_job.database.connection_manager.DatabaseEngineManager.validate_connection_string')
    def test_validate_jdbc_connection_config_failure(self, mock_validate_string, 
                                                    mock_is_supported, unified_manager, 
                                                    jdbc_connection_config):
        """Test failed JDBC connection configuration validation."""
        mock_is_supported.return_value = True
        mock_validate_string.return_value = False
        
        with pytest.raises(ValueError, match="Invalid connection string format"):
            unified_manager.validate_connection_config(jdbc_connection_config)
    
    def test_validate_connection_routing_to_iceberg(self, unified_manager, iceberg_connection_config):
        """Test connection validation routing to Iceberg handler."""
        with patch.object(unified_manager, '_validate_iceberg_connection', return_value=True) as mock_validate:
            result = unified_manager.validate_connection(iceberg_connection_config)
            
            assert result is True
            mock_validate.assert_called_once_with(iceberg_connection_config, 30)
    
    def test_validate_connection_routing_to_jdbc(self, unified_manager, jdbc_connection_config):
        """Test connection validation routing to JDBC handler."""
        with patch.object(unified_manager, '_validate_jdbc_connection', return_value=True) as mock_validate:
            result = unified_manager.validate_connection(jdbc_connection_config)
            
            assert result is True
            mock_validate.assert_called_once_with(jdbc_connection_config, 30)
    
    def test_validate_connection_caching(self, unified_manager, jdbc_connection_config):
        """Test connection validation result caching."""
        with patch.object(unified_manager, '_validate_jdbc_connection', return_value=True) as mock_validate:
            # First call should execute validation
            result1 = unified_manager.validate_connection(jdbc_connection_config)
            assert result1 is True
            assert mock_validate.call_count == 1
            
            # Second call should use cached result
            result2 = unified_manager.validate_connection(jdbc_connection_config)
            assert result2 is True
            assert mock_validate.call_count == 1  # Should not be called again
    
    @patch('boto3.client')
    def test_validate_iceberg_connection_success(self, mock_boto_client, unified_manager, 
                                               iceberg_connection_config):
        """Test successful Iceberg connection validation."""
        # Mock Glue client
        mock_glue_client = Mock()
        mock_glue_client.get_database.return_value = {'Database': {'Name': 'test_database'}}
        mock_boto_client.return_value = mock_glue_client
        
        # Mock Iceberg handler configuration
        unified_manager.iceberg_handler.configure_iceberg_catalog = Mock()
        
        result = unified_manager._validate_iceberg_connection(iceberg_connection_config, 30)
        
        assert result is True
        unified_manager.iceberg_handler.configure_iceberg_catalog.assert_called_once_with(
            warehouse_location='s3://test-bucket/warehouse/',
            catalog_id='123456789012'
        )
        mock_glue_client.get_database.assert_called_once_with(
            Name='test_database',
            CatalogId='123456789012'
        )
    
    @patch('boto3.client')
    def test_validate_iceberg_connection_database_not_found(self, mock_boto_client, unified_manager, 
                                                          iceberg_connection_config):
        """Test Iceberg connection validation when database doesn't exist."""
        from botocore.exceptions import ClientError
        
        # Mock Glue client to raise EntityNotFoundException
        mock_glue_client = Mock()
        error_response = {'Error': {'Code': 'EntityNotFoundException'}}
        mock_glue_client.get_database.side_effect = ClientError(error_response, 'GetDatabase')
        mock_boto_client.return_value = mock_glue_client
        
        # Mock Iceberg handler configuration
        unified_manager.iceberg_handler.configure_iceberg_catalog = Mock()
        
        # Should still return True (catalog access works, database just doesn't exist)
        result = unified_manager._validate_iceberg_connection(iceberg_connection_config, 30)
        
        assert result is True
    
    def test_create_connection_routing_to_iceberg(self, unified_manager, iceberg_connection_config):
        """Test connection creation routing to Iceberg handler."""
        with patch.object(unified_manager, '_create_iceberg_connection', 
                         return_value=unified_manager.iceberg_handler) as mock_create:
            result = unified_manager.create_connection(iceberg_connection_config)
            
            assert result == unified_manager.iceberg_handler
            mock_create.assert_called_once_with(iceberg_connection_config)
    
    def test_create_connection_routing_to_jdbc(self, unified_manager, jdbc_connection_config):
        """Test connection creation routing to JDBC handler."""
        mock_df_reader = Mock()
        with patch.object(unified_manager, '_create_jdbc_connection', 
                         return_value=mock_df_reader) as mock_create:
            result = unified_manager.create_connection(jdbc_connection_config)
            
            assert result == mock_df_reader
            mock_create.assert_called_once_with(jdbc_connection_config)
    
    def test_create_iceberg_connection(self, unified_manager, iceberg_connection_config):
        """Test Iceberg connection creation."""
        unified_manager.iceberg_handler.configure_iceberg_catalog = Mock()
        
        result = unified_manager._create_iceberg_connection(iceberg_connection_config)
        
        assert result == unified_manager.iceberg_handler
        unified_manager.iceberg_handler.configure_iceberg_catalog.assert_called_once_with(
            warehouse_location='s3://test-bucket/warehouse/',
            catalog_id='123456789012'
        )
    
    def test_create_jdbc_connection(self, unified_manager, jdbc_connection_config):
        """Test JDBC connection creation."""
        mock_df_reader = Mock()
        unified_manager.jdbc_manager.create_connection_with_glue_support = Mock(return_value=mock_df_reader)
        
        result = unified_manager._create_jdbc_connection(jdbc_connection_config)
        
        assert result == mock_df_reader
        unified_manager.jdbc_manager.create_connection_with_glue_support.assert_called_once_with(
            connection_config=jdbc_connection_config,
            glue_connection_name=''
        )
    
    def test_read_table_routing_to_iceberg(self, unified_manager, iceberg_connection_config):
        """Test table read routing to Iceberg handler."""
        mock_df = Mock()
        with patch.object(unified_manager, '_read_iceberg_table', return_value=mock_df) as mock_read:
            result = unified_manager.read_table(iceberg_connection_config, 'test_table')
            
            assert result == mock_df
            mock_read.assert_called_once_with(iceberg_connection_config, 'test_table')
    
    def test_read_table_routing_to_jdbc(self, unified_manager, jdbc_connection_config):
        """Test table read routing to JDBC handler."""
        mock_df = Mock()
        with patch.object(unified_manager, '_read_jdbc_table', return_value=mock_df) as mock_read:
            result = unified_manager.read_table(jdbc_connection_config, 'test_table', 'SELECT * FROM test_table')
            
            assert result == mock_df
            mock_read.assert_called_once_with(jdbc_connection_config, 'test_table', 'SELECT * FROM test_table')
    
    def test_read_iceberg_table(self, unified_manager, iceberg_connection_config):
        """Test reading from Iceberg table."""
        mock_df = Mock()
        unified_manager.iceberg_handler.read_table = Mock(return_value=mock_df)
        
        result = unified_manager._read_iceberg_table(iceberg_connection_config, 'test_table', option1='value1')
        
        assert result == mock_df
        unified_manager.iceberg_handler.read_table.assert_called_once_with(
            database='test_database',
            table='test_table',
            catalog_id='123456789012',
            additional_options={'option1': 'value1'}
        )
    
    def test_read_jdbc_table(self, unified_manager, jdbc_connection_config):
        """Test reading from JDBC table."""
        mock_df = Mock()
        unified_manager.jdbc_manager.read_table_data = Mock(return_value=mock_df)
        
        result = unified_manager._read_jdbc_table(jdbc_connection_config, 'test_table', 
                                                'SELECT * FROM test_table', option1='value1')
        
        assert result == mock_df
        unified_manager.jdbc_manager.read_table_data.assert_called_once_with(
            connection_config=jdbc_connection_config,
            table_name='test_table',
            query='SELECT * FROM test_table',
            option1='value1'
        )
    
    def test_write_table_routing_to_iceberg(self, unified_manager, iceberg_connection_config):
        """Test table write routing to Iceberg handler."""
        mock_df = Mock()
        with patch.object(unified_manager, '_write_iceberg_table') as mock_write:
            unified_manager.write_table(mock_df, iceberg_connection_config, 'test_table', 'append')
            
            mock_write.assert_called_once_with(mock_df, iceberg_connection_config, 'test_table', 'append')
    
    def test_write_table_routing_to_jdbc(self, unified_manager, jdbc_connection_config):
        """Test table write routing to JDBC handler."""
        mock_df = Mock()
        with patch.object(unified_manager, '_write_jdbc_table') as mock_write:
            unified_manager.write_table(mock_df, jdbc_connection_config, 'test_table', 'append')
            
            mock_write.assert_called_once_with(mock_df, jdbc_connection_config, 'test_table', 'append')
    
    def test_get_table_schema_routing_to_iceberg(self, unified_manager, iceberg_connection_config):
        """Test schema retrieval routing to Iceberg handler."""
        mock_schema = Mock()
        with patch.object(unified_manager, '_get_iceberg_table_schema', return_value=mock_schema) as mock_get:
            result = unified_manager.get_table_schema(iceberg_connection_config, 'test_table')
            
            assert result == mock_schema
            mock_get.assert_called_once_with(iceberg_connection_config, 'test_table')
    
    def test_get_table_schema_routing_to_jdbc(self, unified_manager, jdbc_connection_config):
        """Test schema retrieval routing to JDBC handler."""
        mock_schema = Mock()
        with patch.object(unified_manager, '_get_jdbc_table_schema', return_value=mock_schema) as mock_get:
            result = unified_manager.get_table_schema(jdbc_connection_config, 'test_table')
            
            assert result == mock_schema
            mock_get.assert_called_once_with(jdbc_connection_config, 'test_table')
    
    def test_validation_cache_key_generation_iceberg(self, unified_manager, iceberg_connection_config):
        """Test validation cache key generation for Iceberg."""
        cache_key = unified_manager._get_validation_cache_key(iceberg_connection_config)
        
        expected_key = "iceberg_test_database_s3://test-bucket/warehouse/_123456789012"
        assert cache_key == expected_key
    
    def test_validation_cache_key_generation_jdbc(self, unified_manager, jdbc_connection_config):
        """Test validation cache key generation for JDBC."""
        cache_key = unified_manager._get_validation_cache_key(jdbc_connection_config)
        
        expected_key = "jdbc_postgresql_testdb_public_testuser_direct_jdbc_"
        assert cache_key == expected_key
    
    @pytest.fixture
    def jdbc_connection_config_with_create_glue(self):
        """Create JDBC connection configuration with create Glue Connection."""
        glue_config = MockGlueConnectionConfig(create_connection=True)
        return MockConnectionConfig(
            engine_type='postgresql',
            connection_string='jdbc:postgresql://localhost:5432/testdb',
            database='testdb',
            schema='public',
            username='testuser',
            password='testpass',
            jdbc_driver_path='s3://bucket/postgresql.jar',
            glue_connection_config=glue_config
        )
    
    @pytest.fixture
    def jdbc_connection_config_with_use_glue(self):
        """Create JDBC connection configuration with use existing Glue Connection."""
        glue_config = MockGlueConnectionConfig(use_existing_connection='my-glue-connection')
        return MockConnectionConfig(
            engine_type='postgresql',
            connection_string='jdbc:postgresql://localhost:5432/testdb',
            database='testdb',
            schema='public',
            username='testuser',
            password='testpass',
            jdbc_driver_path='s3://bucket/postgresql.jar',
            glue_connection_config=glue_config
        )
    
    def test_determine_connection_strategy_iceberg(self, unified_manager, iceberg_connection_config):
        """Test connection strategy determination for Iceberg."""
        strategy = unified_manager._determine_connection_strategy(iceberg_connection_config)
        assert strategy == "iceberg"
    
    def test_determine_connection_strategy_direct_jdbc(self, unified_manager, jdbc_connection_config):
        """Test connection strategy determination for direct JDBC."""
        strategy = unified_manager._determine_connection_strategy(jdbc_connection_config)
        assert strategy == "direct_jdbc"
    
    def test_determine_connection_strategy_create_glue(self, unified_manager, jdbc_connection_config_with_create_glue):
        """Test connection strategy determination for create Glue Connection."""
        strategy = unified_manager._determine_connection_strategy(jdbc_connection_config_with_create_glue)
        assert strategy == "create_glue"
    
    def test_determine_connection_strategy_use_glue(self, unified_manager, jdbc_connection_config_with_use_glue):
        """Test connection strategy determination for use existing Glue Connection."""
        strategy = unified_manager._determine_connection_strategy(jdbc_connection_config_with_use_glue)
        assert strategy == "use_glue"
    
    def test_create_connection_with_glue_strategy(self, unified_manager, jdbc_connection_config_with_create_glue):
        """Test connection creation with Glue Connection strategy."""
        mock_df_reader = Mock()
        with patch.object(unified_manager, '_create_jdbc_connection_with_glue', 
                         return_value=mock_df_reader) as mock_create:
            result = unified_manager.create_connection(jdbc_connection_config_with_create_glue)
            
            assert result == mock_df_reader
            mock_create.assert_called_once_with(jdbc_connection_config_with_create_glue)
    
    def test_validate_connection_with_glue_strategy(self, unified_manager, jdbc_connection_config_with_use_glue):
        """Test connection validation with Glue Connection strategy."""
        with patch.object(unified_manager, '_validate_jdbc_connection_with_glue_strategy', 
                         return_value=True) as mock_validate:
            result = unified_manager.validate_connection(jdbc_connection_config_with_use_glue)
            
            assert result is True
            mock_validate.assert_called_once_with(jdbc_connection_config_with_use_glue, 30)
    
    def test_validation_cache_key_with_glue_strategy(self, unified_manager, jdbc_connection_config_with_use_glue):
        """Test validation cache key generation with Glue Connection strategy."""
        cache_key = unified_manager._get_validation_cache_key(jdbc_connection_config_with_use_glue)
        
        expected_key = "jdbc_postgresql_testdb_public_testuser_use_glue_my-glue-connection"
        assert cache_key == expected_key


if __name__ == '__main__':
    pytest.main([__file__])