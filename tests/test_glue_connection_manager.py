"""
Tests for Glue Connection Manager functionality

This module tests the Glue Connection operations including:
- Validating existing Glue Connections
- Setting up JDBC with Glue Connection strategies
- Connection retrieval and validation
- Error handling for nonexistent connections
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

import unittest
from unittest.mock import Mock, patch, MagicMock
from botocore.exceptions import ClientError

from glue_job.database.connection_manager import GlueConnectionManager
from glue_job.config.job_config import ConnectionConfig, GlueConnectionConfig, NetworkConfig
from glue_job.network.error_handler import GlueConnectionError


class TestGlueConnectionManager(unittest.TestCase):
    """Test GlueConnectionManager functionality."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.mock_glue_context = Mock()
        self.manager = GlueConnectionManager(self.mock_glue_context)
        
        # Mock the Glue client
        self.mock_glue_client = Mock()
        self.manager.glue_client = self.mock_glue_client
        
        # Sample connection configuration
        self.connection_config = ConnectionConfig(
            engine_type="postgresql",
            connection_string="jdbc:postgresql://localhost:5432/testdb",
            database="testdb",
            schema="public",
            username="testuser",
            password="testpass",
            jdbc_driver_path="s3://bucket/postgresql.jar"
        )
    
    def test_validate_glue_connection_exists_success(self):
        """Test successful validation of existing Glue Connection."""
        # Mock successful response
        self.mock_glue_client.get_connection.return_value = {
            'Connection': {
                'Name': 'test-connection',
                'ConnectionType': 'JDBC',
                'ConnectionProperties': {
                    'JDBC_CONNECTION_URL': 'jdbc:postgresql://localhost:5432/testdb'
                },
                'PhysicalConnectionRequirements': {
                    'SubnetId': 'subnet-123',
                    'SecurityGroupIdList': ['sg-123']
                }
            }
        }
        
        result = self.manager.validate_glue_connection_exists('test-connection')
        
        self.assertTrue(result)
        self.mock_glue_client.get_connection.assert_called_once_with(Name='test-connection')
    
    def test_validate_glue_connection_exists_not_found(self):
        """Test validation when Glue Connection does not exist."""
        # Mock EntityNotFoundException
        error_response = {
            'Error': {
                'Code': 'EntityNotFoundException',
                'Message': 'Connection not found'
            }
        }
        self.mock_glue_client.get_connection.side_effect = ClientError(
            error_response, 'GetConnection'
        )
        
        result = self.manager.validate_glue_connection_exists('nonexistent-connection')
        
        self.assertFalse(result)
    
    def test_validate_glue_connection_exists_access_denied(self):
        """Test validation when access is denied to Glue Connection."""
        # Mock AccessDeniedException
        error_response = {
            'Error': {
                'Code': 'AccessDeniedException',
                'Message': 'Access denied'
            }
        }
        self.mock_glue_client.get_connection.side_effect = ClientError(
            error_response, 'GetConnection'
        )
        
        with self.assertRaises(GlueConnectionError) as context:
            self.manager.validate_glue_connection_exists('test-connection')
        
        self.assertIn('Access denied', str(context.exception))
    
    def test_validate_glue_connection_exists_invalid_connection_type(self):
        """Test validation fails for non-JDBC connection type."""
        # Mock response with invalid connection type
        self.mock_glue_client.get_connection.return_value = {
            'Connection': {
                'Name': 'test-connection',
                'ConnectionType': 'MONGODB',  # Invalid type
                'ConnectionProperties': {},
                'PhysicalConnectionRequirements': {}
            }
        }
        
        with self.assertRaises(GlueConnectionError) as context:
            self.manager.validate_glue_connection_exists('test-connection')
        
        self.assertIn('not a JDBC or NETWORK connection', str(context.exception))
    
    def test_validate_glue_connection_exists_missing_jdbc_url(self):
        """Test validation fails for JDBC connection without URL."""
        # Mock response with missing JDBC URL
        self.mock_glue_client.get_connection.return_value = {
            'Connection': {
                'Name': 'test-connection',
                'ConnectionType': 'JDBC',
                'ConnectionProperties': {},  # Missing JDBC_CONNECTION_URL
                'PhysicalConnectionRequirements': {
                    'SubnetId': 'subnet-123',
                    'SecurityGroupIdList': ['sg-123']
                }
            }
        }
        
        with self.assertRaises(GlueConnectionError) as context:
            self.manager.validate_glue_connection_exists('test-connection')
        
        self.assertIn('lacks required JDBC_CONNECTION_URL', str(context.exception))
    
    def test_validate_glue_connection_exists_empty_name(self):
        """Test validation fails for empty connection name."""
        with self.assertRaises(ValueError) as context:
            self.manager.validate_glue_connection_exists('')
        
        self.assertIn('cannot be empty', str(context.exception))
    
    def test_setup_jdbc_with_glue_connection_strategy_direct_jdbc(self):
        """Test JDBC setup with direct JDBC strategy."""
        # No Glue Connection config
        result = self.manager.setup_jdbc_with_glue_connection_strategy(self.connection_config)
        
        # Should return JDBC properties for direct connection
        self.assertIn('url', result)
        self.assertIn('user', result)
        self.assertIn('password', result)
        self.assertEqual(result['url'], self.connection_config.connection_string)
    
    def test_setup_jdbc_with_glue_connection_strategy_create_glue(self):
        """Test JDBC setup with create Glue Connection strategy."""
        # Add Glue Connection config for creation
        glue_config = GlueConnectionConfig(create_connection=True)
        self.connection_config.glue_connection_config = glue_config
        
        # Mock the create_glue_connection method
        with patch.object(self.manager, 'create_glue_connection') as mock_create, \
             patch.object(self.manager, 'setup_jdbc_with_connection') as mock_setup:
            
            mock_create.return_value = 'created-connection'
            mock_setup.return_value = {'url': 'test', 'user': 'test', 'password': 'test'}
            
            result = self.manager.setup_jdbc_with_glue_connection_strategy(self.connection_config)
            
            # Should create connection and then use it
            mock_create.assert_called_once()
            mock_setup.assert_called_once_with(self.connection_config, 'created-connection')
    
    def test_setup_jdbc_with_glue_connection_strategy_use_existing(self):
        """Test JDBC setup with use existing Glue Connection strategy."""
        # Add Glue Connection config for using existing
        glue_config = GlueConnectionConfig(use_existing_connection='existing-connection')
        self.connection_config.glue_connection_config = glue_config
        
        # Mock the validation and setup methods
        with patch.object(self.manager, 'validate_glue_connection_exists') as mock_validate, \
             patch.object(self.manager, 'setup_jdbc_with_connection') as mock_setup:
            
            mock_validate.return_value = True
            mock_setup.return_value = {'url': 'test', 'user': 'test', 'password': 'test'}
            
            result = self.manager.setup_jdbc_with_glue_connection_strategy(self.connection_config)
            
            # Should validate existence and then use connection
            mock_validate.assert_called_once_with('existing-connection')
            mock_setup.assert_called_once_with(self.connection_config, 'existing-connection')
    
    def test_setup_jdbc_with_glue_connection_strategy_use_nonexistent(self):
        """Test JDBC setup fails when using nonexistent Glue Connection."""
        # Add Glue Connection config for using existing
        glue_config = GlueConnectionConfig(use_existing_connection='nonexistent-connection')
        self.connection_config.glue_connection_config = glue_config
        
        # Mock validation to return False (connection doesn't exist)
        with patch.object(self.manager, 'validate_glue_connection_exists') as mock_validate:
            mock_validate.return_value = False
            
            with self.assertRaises(GlueConnectionError) as context:
                self.manager.setup_jdbc_with_glue_connection_strategy(self.connection_config)
            
            self.assertIn('does not exist or is not accessible', str(context.exception))
    
    def test_setup_jdbc_with_connection_direct_jdbc(self):
        """Test setup JDBC with direct connection (no Glue Connection)."""
        result = self.manager.setup_jdbc_with_connection(self.connection_config, '')
        
        # Should return direct JDBC properties
        self.assertIn('url', result)
        self.assertIn('user', result)
        self.assertIn('password', result)
        self.assertEqual(result['url'], self.connection_config.connection_string)
        self.assertEqual(result['user'], self.connection_config.username)
        self.assertEqual(result['password'], self.connection_config.password)
    
    def test_setup_jdbc_with_connection_glue_connection(self):
        """Test setup JDBC with Glue Connection."""
        # Mock get_glue_connection to return connection details
        with patch.object(self.manager, 'get_glue_connection') as mock_get:
            mock_get.return_value = {
                'name': 'test-connection',
                'connection_type': 'JDBC',
                'connection_properties': {
                    'JDBC_CONNECTION_URL': 'jdbc:postgresql://glue-host:5432/gluedb',
                    'USERNAME': 'glueuser',
                    'PASSWORD': 'gluepass'
                },
                'physical_connection_requirements': {
                    'SubnetId': 'subnet-123',
                    'SecurityGroupIdList': ['sg-123']
                }
            }
            
            result = self.manager.setup_jdbc_with_connection(self.connection_config, 'test-connection')
            
            # Should use Glue Connection properties
            self.assertEqual(result['url'], 'jdbc:postgresql://glue-host:5432/gluedb')
            self.assertEqual(result['user'], 'glueuser')
            self.assertEqual(result['password'], 'gluepass')
            self.assertIn('_glue_connection_metadata', result)
    
    def test_setup_jdbc_with_connection_network_connection(self):
        """Test setup JDBC with NETWORK type Glue Connection."""
        # Mock get_glue_connection to return NETWORK connection
        with patch.object(self.manager, 'get_glue_connection') as mock_get:
            mock_get.return_value = {
                'name': 'test-network-connection',
                'connection_type': 'NETWORK',
                'connection_properties': {},
                'physical_connection_requirements': {
                    'SubnetId': 'subnet-123',
                    'SecurityGroupIdList': ['sg-123']
                }
            }
            
            result = self.manager.setup_jdbc_with_connection(self.connection_config, 'test-network-connection')
            
            # Should use original JDBC properties with network metadata
            self.assertEqual(result['url'], self.connection_config.connection_string)
            self.assertEqual(result['user'], self.connection_config.username)
            self.assertEqual(result['password'], self.connection_config.password)
            self.assertIn('_glue_connection_metadata', result)
            self.assertEqual(result['_glue_connection_metadata']['connection_type'], 'NETWORK')
    
    def test_setup_jdbc_with_connection_retrieval_error(self):
        """Test setup JDBC handles Glue Connection retrieval errors."""
        # Mock get_glue_connection to raise an error
        with patch.object(self.manager, 'get_glue_connection') as mock_get:
            mock_get.side_effect = GlueConnectionError(
                "Connection not found", 'test-connection', {'error_type': 'not_found'}
            )
            
            with self.assertRaises(GlueConnectionError):
                self.manager.setup_jdbc_with_connection(self.connection_config, 'test-connection')


class TestGlueConnectionManagerIntegration(unittest.TestCase):
    """Integration tests for Glue Connection Manager with real AWS SDK calls."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.mock_glue_context = Mock()
        self.manager = GlueConnectionManager(self.mock_glue_context)
    
    @patch('boto3.client')
    def test_validate_glue_connection_exists_with_real_client(self, mock_boto_client):
        """Test validation with real boto3 client (mocked)."""
        # Mock the boto3 client
        mock_client = Mock()
        mock_boto_client.return_value = mock_client
        
        # Create new manager to use mocked client
        manager = GlueConnectionManager(self.mock_glue_context)
        
        # Mock successful response
        mock_client.get_connection.return_value = {
            'Connection': {
                'Name': 'test-connection',
                'ConnectionType': 'JDBC',
                'ConnectionProperties': {
                    'JDBC_CONNECTION_URL': 'jdbc:postgresql://localhost:5432/testdb'
                },
                'PhysicalConnectionRequirements': {
                    'SubnetId': 'subnet-123',
                    'SecurityGroupIdList': ['sg-123']
                }
            }
        }
        
        result = manager.validate_glue_connection_exists('test-connection')
        
        self.assertTrue(result)
        mock_client.get_connection.assert_called_once_with(Name='test-connection')
    
    @patch('boto3.client')
    @patch('glue_job.database.connection_manager.SecretsManagerHandler')
    def test_create_and_use_glue_connection_workflow(self, mock_sm_handler_class, mock_boto_client):
        """Test complete workflow of creating and using Glue Connection."""
        # Mock the boto3 client
        mock_client = Mock()
        mock_boto_client.return_value = mock_client
        
        # Mock the SecretsManagerHandler
        mock_secrets_manager_handler = Mock()
        mock_sm_handler_class.return_value = mock_secrets_manager_handler
        
        # Mock successful secret operations
        secret_arn = "arn:aws:secretsmanager:us-east-1:123456789012:secret:/aws-glue/created-connection-AbCdEf"
        mock_secrets_manager_handler.validate_secret_permissions.return_value = True
        mock_secrets_manager_handler.create_secret.return_value = secret_arn
        
        # Create new manager to use mocked client
        manager = GlueConnectionManager(self.mock_glue_context)
        
        # Mock successful connection creation
        mock_client.create_connection.return_value = {}
        
        # Mock successful connection retrieval
        mock_client.get_connection.return_value = {
            'Connection': {
                'Name': 'created-connection',
                'ConnectionType': 'JDBC',
                'ConnectionProperties': {
                    'JDBC_CONNECTION_URL': 'jdbc:postgresql://localhost:5432/testdb',
                    'SECRET_ID': secret_arn
                },
                'PhysicalConnectionRequirements': {
                    'SubnetId': 'subnet-123',
                    'SecurityGroupIdList': ['sg-123']
                }
            }
        }
        
        # Create connection configuration
        connection_config = ConnectionConfig(
            engine_type="postgresql",
            connection_string="jdbc:postgresql://localhost:5432/testdb",
            database="testdb",
            schema="public",
            username="testuser",
            password="testpass",
            jdbc_driver_path="s3://bucket/postgresql.jar"
        )
        
        # Test connection creation
        created_name = manager.create_glue_connection(connection_config, 'created-connection')
        self.assertEqual(created_name, 'created-connection')
        
        # Verify secret creation was called
        mock_secrets_manager_handler.create_secret.assert_called_once()
        
        # Mock the get_glue_connection method to return connection details
        with patch.object(manager, 'get_glue_connection') as mock_get_connection:
            mock_get_connection.return_value = {
                'name': 'created-connection',
                'connection_type': 'JDBC',
                'connection_properties': {
                    'JDBC_CONNECTION_URL': 'jdbc:postgresql://localhost:5432/testdb',
                    'SECRET_ID': secret_arn
                },
                'physical_connection_requirements': {
                    'SubnetId': 'subnet-123',
                    'SecurityGroupIdList': ['sg-123']
                }
            }
            
            # Test using the created connection
            jdbc_properties = manager.setup_jdbc_with_connection(connection_config, created_name)
            
            self.assertIn('url', jdbc_properties)
            self.assertIn('user', jdbc_properties)
            self.assertIn('password', jdbc_properties)
            self.assertIn('_glue_connection_metadata', jdbc_properties)


if __name__ == '__main__':
    unittest.main()