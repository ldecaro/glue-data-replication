"""
Integration test for Connection Manager Iceberg routing

This test verifies that the UnifiedConnectionManager correctly routes
requests between JDBC and Iceberg handlers based on engine type.
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from unittest.mock import Mock, patch, MagicMock
from dataclasses import dataclass
from typing import Optional

# Mock classes for testing
class MockSparkSession:
    def __init__(self):
        self.conf = Mock()

class MockGlueContext:
    pass

@dataclass
class MockConnectionConfig:
    engine_type: str
    connection_string: str = ''
    database: str = 'test_db'
    schema: str = 'test_schema'
    username: str = 'test_user'
    password: str = 'test_pass'
    jdbc_driver_path: str = 's3://bucket/driver.jar'
    network_config: Optional[object] = None
    warehouse_location: str = 's3://test-bucket/warehouse/'
    catalog_id: Optional[str] = None
    table_name: str = 'test_table'
    format_version: str = '2'
    
    def validate(self):
        pass
    
    def requires_cross_vpc_connection(self) -> bool:
        return False
    
    def get_glue_connection_name(self) -> Optional[str]:
        return None
    
    def uses_glue_connection(self) -> bool:
        """Return whether this connection uses Glue Connection."""
        return False
    
    def get_glue_connection_name_for_creation(self) -> Optional[str]:
        """Return the Glue connection name for creation."""
        return None
    
    def get_iceberg_config(self):
        """Return Iceberg configuration as a dictionary for compatibility."""
        return {
            'warehouse_location': self.warehouse_location,
            'catalog_id': self.catalog_id,
            'database_name': self.database,
            'table_name': self.table_name,
            'format_version': self.format_version
        }


def test_iceberg_engine_detection():
    """Test that Iceberg engine is correctly detected."""
    from glue_job.config.database_engines import DatabaseEngineManager
    
    # Test Iceberg detection
    assert DatabaseEngineManager.is_iceberg_engine('iceberg') is True
    assert DatabaseEngineManager.is_iceberg_engine('ICEBERG') is True
    
    # Test non-Iceberg engines
    assert DatabaseEngineManager.is_iceberg_engine('postgresql') is False
    assert DatabaseEngineManager.is_iceberg_engine('oracle') is False
    
    print("✓ Iceberg engine detection test passed")


def test_unified_connection_manager_routing():
    """Test that UnifiedConnectionManager routes correctly based on engine type."""
    
    # Mock dependencies
    with patch('glue_job.database.connection_manager.JdbcConnectionManager') as mock_jdbc_mgr, \
         patch('glue_job.database.connection_manager.IcebergConnectionHandler') as mock_iceberg_handler, \
         patch('glue_job.database.connection_manager.ConnectionRetryHandler'), \
         patch('glue_job.database.connection_manager.StructuredLogger'):
        
        from glue_job.database.connection_manager import UnifiedConnectionManager
        
        # Create manager instance
        spark_session = MockSparkSession()
        glue_context = MockGlueContext()
        manager = UnifiedConnectionManager(spark_session, glue_context)
        
        # Test Iceberg engine detection
        iceberg_config = MockConnectionConfig(engine_type='iceberg')
        assert manager.is_iceberg_engine(iceberg_config.engine_type) is True
        
        # Test JDBC engine detection
        jdbc_config = MockConnectionConfig(engine_type='postgresql')
        assert manager.is_iceberg_engine(jdbc_config.engine_type) is False
        
        print("✓ UnifiedConnectionManager routing test passed")


def test_connection_validation_routing():
    """Test that connection validation routes to the correct handler."""
    
    with patch('glue_job.database.connection_manager.JdbcConnectionManager') as mock_jdbc_mgr, \
         patch('glue_job.database.connection_manager.IcebergConnectionHandler') as mock_iceberg_handler, \
         patch('glue_job.database.connection_manager.ConnectionRetryHandler'), \
         patch('glue_job.database.connection_manager.StructuredLogger'):
        
        from glue_job.database.connection_manager import UnifiedConnectionManager
        
        # Create manager instance
        spark_session = MockSparkSession()
        glue_context = MockGlueContext()
        manager = UnifiedConnectionManager(spark_session, glue_context)
        
        # Mock the validation methods
        manager._validate_iceberg_connection = Mock(return_value=True)
        manager._validate_jdbc_connection = Mock(return_value=True)
        
        # Test Iceberg validation routing
        iceberg_config = MockConnectionConfig(engine_type='iceberg')
        result = manager.validate_connection(iceberg_config)
        assert result is True
        manager._validate_iceberg_connection.assert_called_once()
        
        # Reset mocks
        manager._validate_iceberg_connection.reset_mock()
        manager._validate_jdbc_connection.reset_mock()
        
        # Test JDBC validation routing
        jdbc_config = MockConnectionConfig(engine_type='postgresql')
        result = manager.validate_connection(jdbc_config)
        assert result is True
        manager._validate_jdbc_connection.assert_called_once()
        
        print("✓ Connection validation routing test passed")


def test_connection_creation_routing():
    """Test that connection creation routes to the correct handler."""
    
    with patch('glue_job.database.connection_manager.JdbcConnectionManager') as mock_jdbc_mgr, \
         patch('glue_job.database.connection_manager.IcebergConnectionHandler') as mock_iceberg_handler, \
         patch('glue_job.database.connection_manager.ConnectionRetryHandler'), \
         patch('glue_job.database.connection_manager.StructuredLogger'):
        
        from glue_job.database.connection_manager import UnifiedConnectionManager
        
        # Create manager instance
        spark_session = MockSparkSession()
        glue_context = MockGlueContext()
        manager = UnifiedConnectionManager(spark_session, glue_context)
        
        # Mock the creation methods
        mock_iceberg_conn = Mock()
        mock_jdbc_conn = Mock()
        manager._create_iceberg_connection = Mock(return_value=mock_iceberg_conn)
        manager._create_jdbc_connection = Mock(return_value=mock_jdbc_conn)
        
        # Test Iceberg connection creation routing
        iceberg_config = MockConnectionConfig(engine_type='iceberg')
        result = manager.create_connection(iceberg_config)
        assert result == mock_iceberg_conn
        manager._create_iceberg_connection.assert_called_once_with(iceberg_config)
        
        # Reset mocks
        manager._create_iceberg_connection.reset_mock()
        manager._create_jdbc_connection.reset_mock()
        
        # Test JDBC connection creation routing
        jdbc_config = MockConnectionConfig(engine_type='postgresql')
        result = manager.create_connection(jdbc_config)
        assert result == mock_jdbc_conn
        manager._create_jdbc_connection.assert_called_once_with(jdbc_config)
        
        print("✓ Connection creation routing test passed")


def test_table_operations_routing():
    """Test that table operations route to the correct handler."""
    
    with patch('glue_job.database.connection_manager.JdbcConnectionManager') as mock_jdbc_mgr, \
         patch('glue_job.database.connection_manager.IcebergConnectionHandler') as mock_iceberg_handler, \
         patch('glue_job.database.connection_manager.ConnectionRetryHandler'), \
         patch('glue_job.database.connection_manager.StructuredLogger'):
        
        from glue_job.database.connection_manager import UnifiedConnectionManager
        
        # Create manager instance
        spark_session = MockSparkSession()
        glue_context = MockGlueContext()
        manager = UnifiedConnectionManager(spark_session, glue_context)
        
        # Mock the table operation methods
        mock_df = Mock()
        manager._read_iceberg_table = Mock(return_value=mock_df)
        manager._read_jdbc_table = Mock(return_value=mock_df)
        manager._write_iceberg_table = Mock()
        manager._write_jdbc_table = Mock()
        manager._get_iceberg_table_schema = Mock(return_value=Mock())
        manager._get_jdbc_table_schema = Mock(return_value=Mock())
        
        # Test Iceberg table operations
        iceberg_config = MockConnectionConfig(engine_type='iceberg')
        
        # Read operation
        result = manager.read_table(iceberg_config, 'test_table')
        assert result == mock_df
        manager._read_iceberg_table.assert_called_once()
        
        # Write operation
        manager.write_table(mock_df, iceberg_config, 'test_table')
        manager._write_iceberg_table.assert_called_once()
        
        # Schema operation
        manager.get_table_schema(iceberg_config, 'test_table')
        manager._get_iceberg_table_schema.assert_called_once()
        
        # Reset mocks
        for mock_method in [manager._read_iceberg_table, manager._read_jdbc_table,
                           manager._write_iceberg_table, manager._write_jdbc_table,
                           manager._get_iceberg_table_schema, manager._get_jdbc_table_schema]:
            mock_method.reset_mock()
        
        # Test JDBC table operations
        jdbc_config = MockConnectionConfig(engine_type='postgresql')
        
        # Read operation
        result = manager.read_table(jdbc_config, 'test_table')
        assert result == mock_df
        manager._read_jdbc_table.assert_called_once()
        
        # Write operation
        manager.write_table(mock_df, jdbc_config, 'test_table')
        manager._write_jdbc_table.assert_called_once()
        
        # Schema operation
        manager.get_table_schema(jdbc_config, 'test_table')
        manager._get_jdbc_table_schema.assert_called_once()
        
        print("✓ Table operations routing test passed")


def run_all_tests():
    """Run all integration tests."""
    print("Running Connection Manager Integration Tests...")
    print("=" * 50)
    
    try:
        test_iceberg_engine_detection()
        test_unified_connection_manager_routing()
        test_connection_validation_routing()
        test_connection_creation_routing()
        test_table_operations_routing()
        
        print("=" * 50)
        print("✓ All integration tests passed successfully!")
        return True
        
    except Exception as e:
        print(f"✗ Integration test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == '__main__':
    success = run_all_tests()
    exit(0 if success else 1)