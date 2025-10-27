"""
Tests for Glue Connection Manager with Secrets Manager integration

This module tests the enhanced Glue Connection operations including:
- Creating Glue Connections with Secrets Manager integration
- Validating required JDBC parameters for connection creation
- Error handling for connection creation failures with secret cleanup
- Secrets Manager permission validation
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

import unittest
from unittest.mock import Mock, patch, MagicMock
from botocore.exceptions import ClientError

from glue_job.database.connection_manager import GlueConnectionManager
from glue_job.config.job_config import ConnectionConfig, GlueConnectionConfig, NetworkConfig
from glue_job.config.secrets_manager_handler import (
    SecretsManagerError, SecretCreationError, SecretsManagerPermissionError
)
from glue_job.network.glue_connection_errors import (
    GlueConnectionCreationError, GlueConnectionParameterError, GlueConnectionValidationError
)


class TestGlueConnectionManagerSecretsManager(unittest.TestCase):
    """Test GlueConnectionManager with Secrets Manager integration."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.mock_glue_context = Mock()
        
        # Mock the SecretsManagerHandler
        with patch('glue_job.database.connection_manager.SecretsManagerHandler') as mock_sm_handler_class:
            self.mock_secrets_manager_handler = Mock()
            mock_sm_handler_class.return_value = self.mock_secrets_manager_handler
            
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
    
    def test_create_glue_connection_with_secrets_manager_success(self):
        """Test successful Glue Connection creation with Secrets Manager integration."""
        # Mock successful secret creation
        secret_arn = "arn:aws:secretsmanager:us-east-1:123456789012:secret:/aws-glue/test-connection-AbCdEf"
        self.mock_secrets_manager_handler.validate_secret_permissions.return_value = True
        self.mock_secrets_manager_handler.create_secret.return_value = secret_arn
        
        # Mock successful Glue Connection creation
        self.mock_glue_client.create_connection.return_value = {}
        
        result = self.manager.create_glue_connection(self.connection_config, 'test-connection')
        
        self.assertEqual(result, 'test-connection')
        
        # Verify secret creation was called
        self.mock_secrets_manager_handler.create_secret.assert_called_once_with(
            connection_name='test-connection',
            username='testuser',
            password='testpass',
            description="Database credentials for Glue Connection 'test-connection' (postgresql)",
            tags={
                'EngineType': 'postgresql',
                'Database': 'testdb',
                'Schema': 'public'
            }
        )
        
        # Verify Glue Connection creation was called
        self.mock_glue_client.create_connection.assert_called_once()
        
        # Verify the connection input structure uses secret reference
        call_args = self.mock_glue_client.create_connection.call_args
        connection_input = call_args[1]['ConnectionInput']
        
        self.assertEqual(connection_input['Name'], 'test-connection')
        self.assertEqual(connection_input['ConnectionType'], 'JDBC')
        self.assertEqual(
            connection_input['ConnectionProperties']['JDBC_CONNECTION_URL'],
            self.connection_config.connection_string
        )
        self.assertEqual(
            connection_input['ConnectionProperties']['SECRET_ID'],
            secret_arn
        )
        # Verify that USERNAME and PASSWORD are not in the connection properties
        self.assertNotIn('USERNAME', connection_input['ConnectionProperties'])
        self.assertNotIn('PASSWORD', connection_input['ConnectionProperties'])
    
    def test_create_glue_connection_secret_creation_failure(self):
        """Test Glue Connection creation when secret creation fails."""
        # Mock secret creation failure
        self.mock_secrets_manager_handler.validate_secret_permissions.return_value = True
        self.mock_secrets_manager_handler.create_secret.side_effect = SecretCreationError(
            "Failed to create secret", "test-connection"
        )
        
        with self.assertRaises(SecretCreationError):
            self.manager.create_glue_connection(self.connection_config, 'test-connection')
        
        # Verify Glue Connection creation was not called
        self.mock_glue_client.create_connection.assert_not_called()
    
    def test_create_glue_connection_glue_creation_failure_with_cleanup(self):
        """Test Glue Connection creation failure with secret cleanup."""
        # Mock successful secret creation
        secret_arn = "arn:aws:secretsmanager:us-east-1:123456789012:secret:/aws-glue/test-connection-AbCdEf"
        self.mock_secrets_manager_handler.validate_secret_permissions.return_value = True
        self.mock_secrets_manager_handler.create_secret.return_value = secret_arn
        
        # Mock Glue Connection creation failure
        error_response = {
            'Error': {
                'Code': 'InvalidInputException',
                'Message': 'Invalid connection parameters'
            }
        }
        self.mock_glue_client.create_connection.side_effect = ClientError(
            error_response, 'CreateConnection'
        )
        
        # Mock successful cleanup
        self.mock_secrets_manager_handler.cleanup_secret_on_failure.return_value = True
        
        with self.assertRaises(Exception) as context:
            self.manager.create_glue_connection(self.connection_config, 'test-connection')
        
        # The error could be GlueConnectionValidationError (for InvalidInputException) 
        # or GlueConnectionCreationError (for other errors)
        self.assertTrue(
            isinstance(context.exception, (GlueConnectionCreationError, GlueConnectionValidationError)),
            f"Expected GlueConnectionCreationError or GlueConnectionValidationError, got {type(context.exception)}"
        )
        
        # Verify secret cleanup was called
        self.mock_secrets_manager_handler.cleanup_secret_on_failure.assert_called_once_with(
            'test-connection'
        )
    
    def test_create_glue_connection_permission_validation_failure(self):
        """Test Glue Connection creation when Secrets Manager permissions are insufficient."""
        # Mock permission validation failure
        self.mock_secrets_manager_handler.validate_secret_permissions.side_effect = SecretsManagerPermissionError(
            "Insufficient permissions", operation="create_secret"
        )
        
        with self.assertRaises(SecretsManagerPermissionError):
            self.manager.create_glue_connection(self.connection_config, 'test-connection')
        
        # Verify secret creation was not called
        self.mock_secrets_manager_handler.create_secret.assert_not_called()
        
        # Verify Glue Connection creation was not called
        self.mock_glue_client.create_connection.assert_not_called()
    
    def test_create_glue_connection_with_network_config(self):
        """Test Glue Connection creation with network configuration and Secrets Manager."""
        # Add network config to connection
        network_config = NetworkConfig(
            vpc_id='vpc-123',
            subnet_ids=['subnet-123', 'subnet-456'],
            security_group_ids=['sg-123']
        )
        self.connection_config.network_config = network_config
        
        # Mock successful secret creation
        secret_arn = "arn:aws:secretsmanager:us-east-1:123456789012:secret:/aws-glue/test-connection-AbCdEf"
        self.mock_secrets_manager_handler.validate_secret_permissions.return_value = True
        self.mock_secrets_manager_handler.create_secret.return_value = secret_arn
        
        # Mock successful Glue Connection creation
        self.mock_glue_client.create_connection.return_value = {}
        
        result = self.manager.create_glue_connection(self.connection_config, 'test-connection')
        
        self.assertEqual(result, 'test-connection')
        
        # Verify network configuration was included
        call_args = self.mock_glue_client.create_connection.call_args
        connection_input = call_args[1]['ConnectionInput']
        
        self.assertIn('PhysicalConnectionRequirements', connection_input)
        physical_reqs = connection_input['PhysicalConnectionRequirements']
        self.assertEqual(physical_reqs['SubnetId'], 'subnet-123')
        self.assertEqual(physical_reqs['SecurityGroupIdList'], ['sg-123'])
    
    def test_validate_glue_connection_creation_params_missing_username(self):
        """Test parameter validation when username is missing."""
        self.connection_config.username = ""
        
        with self.assertRaises(GlueConnectionParameterError) as context:
            self.manager._validate_glue_connection_creation_params(
                self.connection_config, 'test-connection'
            )
        
        self.assertIn("Username is required", str(context.exception))
    
    def test_validate_glue_connection_creation_params_missing_password(self):
        """Test parameter validation when password is missing."""
        self.connection_config.password = ""
        
        with self.assertRaises(GlueConnectionParameterError) as context:
            self.manager._validate_glue_connection_creation_params(
                self.connection_config, 'test-connection'
            )
        
        self.assertIn("Password is required", str(context.exception))
    
    def test_validate_glue_connection_creation_params_missing_connection_string(self):
        """Test parameter validation when connection string is missing."""
        self.connection_config.connection_string = ""
        
        with self.assertRaises(GlueConnectionParameterError) as context:
            self.manager._validate_glue_connection_creation_params(
                self.connection_config, 'test-connection'
            )
        
        self.assertIn("Connection string is required", str(context.exception))
    
    def test_validate_glue_connection_creation_params_invalid_connection_name(self):
        """Test parameter validation when connection name is invalid."""
        with self.assertRaises(GlueConnectionParameterError) as context:
            self.manager._validate_glue_connection_creation_params(
                self.connection_config, 'invalid@connection#name'
            )
        
        self.assertIn("must contain only alphanumeric characters", str(context.exception))
    
    def test_validate_glue_connection_creation_params_empty_connection_name(self):
        """Test parameter validation when connection name is empty."""
        with self.assertRaises(GlueConnectionParameterError) as context:
            self.manager._validate_glue_connection_creation_params(
                self.connection_config, ''
            )
        
        self.assertIn("Connection name cannot be empty", str(context.exception))
    
    def test_validate_glue_connection_creation_params_unsupported_engine(self):
        """Test parameter validation when engine type is unsupported."""
        self.connection_config.engine_type = "unsupported_engine"
        
        with self.assertRaises(GlueConnectionParameterError) as context:
            self.manager._validate_glue_connection_creation_params(
                self.connection_config, 'test-connection'
            )
        
        self.assertIn("is not supported for Glue Connections", str(context.exception))
    
    def test_validate_glue_connection_creation_params_whitespace_only_username(self):
        """Test parameter validation when username is whitespace only."""
        self.connection_config.username = "   "
        
        with self.assertRaises(GlueConnectionParameterError) as context:
            self.manager._validate_glue_connection_creation_params(
                self.connection_config, 'test-connection'
            )
        
        self.assertIn("Username cannot be empty or whitespace only", str(context.exception))
    
    def test_validate_glue_connection_creation_params_whitespace_only_password(self):
        """Test parameter validation when password is whitespace only."""
        self.connection_config.password = "   "
        
        with self.assertRaises(GlueConnectionParameterError) as context:
            self.manager._validate_glue_connection_creation_params(
                self.connection_config, 'test-connection'
            )
        
        self.assertIn("Password cannot be empty or whitespace only", str(context.exception))
    
    def test_build_glue_connection_input_with_secrets(self):
        """Test building Glue Connection input with Secrets Manager reference."""
        secret_arn = "arn:aws:secretsmanager:us-east-1:123456789012:secret:/aws-glue/test-connection-AbCdEf"
        
        connection_input = self.manager._build_glue_connection_input_with_secrets(
            self.connection_config, 'test-connection', secret_arn
        )
        
        # Verify the structure
        self.assertEqual(connection_input['Name'], 'test-connection')
        self.assertEqual(connection_input['ConnectionType'], 'JDBC')
        self.assertEqual(
            connection_input['ConnectionProperties']['JDBC_CONNECTION_URL'],
            self.connection_config.connection_string
        )
        self.assertEqual(
            connection_input['ConnectionProperties']['SECRET_ID'],
            secret_arn
        )
        
        # Verify that USERNAME and PASSWORD are not in the connection properties
        self.assertNotIn('USERNAME', connection_input['ConnectionProperties'])
        self.assertNotIn('PASSWORD', connection_input['ConnectionProperties'])
        
        # Verify description mentions Secrets Manager
        self.assertIn('Secrets Manager integration', connection_input['Description'])
    
    def test_cleanup_secret_on_failure_success(self):
        """Test successful secret cleanup on failure."""
        self.mock_secrets_manager_handler.cleanup_secret_on_failure.return_value = True
        
        # This should not raise an exception
        self.manager._cleanup_secret_on_failure('test-connection')
        
        self.mock_secrets_manager_handler.cleanup_secret_on_failure.assert_called_once_with(
            'test-connection'
        )
    
    def test_cleanup_secret_on_failure_error(self):
        """Test secret cleanup when cleanup fails."""
        self.mock_secrets_manager_handler.cleanup_secret_on_failure.side_effect = Exception(
            "Cleanup failed"
        )
        
        # This should not raise an exception (cleanup errors are logged but not raised)
        self.manager._cleanup_secret_on_failure('test-connection')
        
        self.mock_secrets_manager_handler.cleanup_secret_on_failure.assert_called_once_with(
            'test-connection'
        )


if __name__ == '__main__':
    unittest.main()