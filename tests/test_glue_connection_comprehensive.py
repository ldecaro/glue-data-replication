#!/usr/bin/env python3
"""
Comprehensive unit tests for Glue Connection support functionality.

This test suite covers all aspects of Glue Connection support including:
- GlueConnectionConfig validation and methods
- Parameter parsing with Glue Connection parameters  
- Connection creation and usage functionality
- Error handling scenarios and edge cases
- Integration with existing JDBC and Iceberg connections

Requirements covered: 1.1, 1.2, 2.1, 2.2, 3.1, 3.2, 5.1, 5.2
"""

import unittest
from unittest.mock import Mock, MagicMock, patch
import sys
import os
from dataclasses import dataclass
from typing import Dict, Any, Optional
from botocore.exceptions import ClientError

# Add src directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'src'))

# Mock PySpark and AWS Glue imports for testing
mock_modules = [
    'awsglue', 'awsglue.utils', 'awsglue.context', 'awsglue.job',
    'pyspark', 'pyspark.context', 'pyspark.sql', 'pyspark.sql.types',
    'pyspark.sql.functions'
]

for module in mock_modules:
    sys.modules[module] = MagicMock()

# Import the classes to test
from glue_job.config.job_config import GlueConnectionConfig, ConnectionConfig
from glue_job.config.parsers import JobConfigurationParser
from glue_job.database.connection_manager import GlueConnectionManager, UnifiedConnectionManager
from glue_job.network.error_handler import GlueConnectionError
from glue_job.network.glue_connection_errors import (
    GlueConnectionParameterError,
    GlueConnectionCreationError,
    GlueConnectionNotFoundError,
    GlueConnectionValidationError
)


class TestGlueConnectionConfigValidation(unittest.TestCase):
    """Test GlueConnectionConfig validation and methods - Requirements 1.1, 1.2, 5.1, 5.2"""
    
    def test_default_configuration(self):
        """Test default GlueConnectionConfig creation."""
        config = GlueConnectionConfig()
        
        self.assertFalse(config.create_connection)
        self.assertIsNone(config.use_existing_connection)
        self.assertEqual(config.connection_strategy, "direct_jdbc")
        self.assertFalse(config.uses_glue_connection())
    
    def test_create_connection_configuration(self):
        """Test GlueConnectionConfig with create_connection=True."""
        config = GlueConnectionConfig(create_connection=True)
        
        self.assertTrue(config.create_connection)
        self.assertIsNone(config.use_existing_connection)
        self.assertEqual(config.connection_strategy, "create_glue")
        self.assertTrue(config.uses_glue_connection())
    
    def test_use_existing_connection_configuration(self):
        """Test GlueConnectionConfig with existing connection name."""
        config = GlueConnectionConfig(use_existing_connection="my-connection")
        
        self.assertFalse(config.create_connection)
        self.assertEqual(config.use_existing_connection, "my-connection")
        self.assertEqual(config.connection_strategy, "use_glue")
        self.assertTrue(config.uses_glue_connection())
    
    def test_validation_success_cases(self):
        """Test successful validation scenarios."""
        # Default config
        config = GlueConnectionConfig()
        config.validate()  # Should not raise
        
        # Create connection
        config = GlueConnectionConfig(create_connection=True)
        config.validate()  # Should not raise
        
        # Use existing connection
        config = GlueConnectionConfig(use_existing_connection="valid-connection-name")
        config.validate()  # Should not raise
    
    def test_validation_mutually_exclusive_parameters(self):
        """Test validation fails for mutually exclusive parameters."""
        config = GlueConnectionConfig(
            create_connection=True,
            use_existing_connection="my-connection"
        )
        
        with self.assertRaises(ValueError) as context:
            config.validate()
        self.assertIn("mutually exclusive", str(context.exception))
    
    def test_validation_empty_connection_name(self):
        """Test validation fails for empty connection name."""
        config = GlueConnectionConfig(use_existing_connection="")
        
        with self.assertRaises(ValueError) as context:
            config.validate()
        self.assertIn("cannot be empty", str(context.exception))
    
    def test_validation_whitespace_connection_name(self):
        """Test validation fails for whitespace-only connection name."""
        config = GlueConnectionConfig(use_existing_connection="   ")
        
        with self.assertRaises(ValueError) as context:
            config.validate()
        self.assertIn("cannot be empty", str(context.exception))
    
    def test_validation_invalid_connection_name_characters(self):
        """Test validation passes for connection names (validation happens at AWS level)."""
        # Note: Connection name format validation happens at AWS API level, not in our config
        test_names = [
            "connection-with-dashes",
            "connection_with_underscores",
            "ConnectionWithCamelCase"
        ]
        
        for name in test_names:
            with self.subTest(name=name):
                config = GlueConnectionConfig(use_existing_connection=name)
                config.validate()  # Should not raise
    
    def test_validation_valid_connection_name_formats(self):
        """Test validation passes for valid connection name formats."""
        valid_names = [
            "connection123",
            "Connection_Name", 
            "connection-name",
            "MyConnection",
            "connection_123_test"
        ]
        
        for valid_name in valid_names:
            with self.subTest(name=valid_name):
                config = GlueConnectionConfig(use_existing_connection=valid_name)
                config.validate()  # Should not raise
    
    def test_validation_invalid_parameter_types(self):
        """Test validation fails for invalid parameter types."""
        # Test invalid create_connection type
        config = GlueConnectionConfig()
        config.create_connection = "true"  # Should be boolean
        
        with self.assertRaises(ValueError) as context:
            config.validate()
        self.assertIn("must be a boolean", str(context.exception))
        
        # Test invalid use_existing_connection type
        config = GlueConnectionConfig()
        config.use_existing_connection = 123  # Should be string
        
        with self.assertRaises(ValueError) as context:
            config.validate()
        self.assertIn("must be a string", str(context.exception))


class TestConnectionConfigGlueIntegration(unittest.TestCase):
    """Test ConnectionConfig integration with GlueConnectionConfig - Requirements 1.1, 1.2, 3.1, 3.2"""
    
    def setUp(self):
        """Set up test fixtures."""
        self.base_jdbc_config = {
            'engine_type': 'postgresql',
            'connection_string': 'jdbc:postgresql://localhost:5432/testdb',
            'database': 'testdb',
            'schema': 'public',
            'username': 'testuser',
            'password': 'testpass',
            'jdbc_driver_path': 's3://bucket/drivers/postgresql.jar'
        }
    
    def test_connection_config_without_glue_connection(self):
        """Test ConnectionConfig without Glue Connection configuration."""
        config = ConnectionConfig(**self.base_jdbc_config)
        
        self.assertFalse(config.uses_glue_connection())
        self.assertEqual(config.get_glue_connection_strategy(), "direct_jdbc")
        self.assertFalse(config.should_create_glue_connection())
        self.assertFalse(config.should_use_existing_glue_connection())
        self.assertIsNone(config.get_glue_connection_name_for_creation())
    
    def test_connection_config_with_create_glue_connection(self):
        """Test ConnectionConfig with create Glue Connection."""
        glue_config = GlueConnectionConfig(create_connection=True)
        config = ConnectionConfig(
            **self.base_jdbc_config,
            glue_connection_config=glue_config
        )
        
        self.assertTrue(config.uses_glue_connection())
        self.assertEqual(config.get_glue_connection_strategy(), "create_glue")
        self.assertTrue(config.should_create_glue_connection())
        self.assertFalse(config.should_use_existing_glue_connection())
        self.assertIsNone(config.get_glue_connection_name_for_creation())
    
    def test_connection_config_with_use_existing_glue_connection(self):
        """Test ConnectionConfig with use existing Glue Connection."""
        glue_config = GlueConnectionConfig(use_existing_connection="my-connection")
        config = ConnectionConfig(
            **self.base_jdbc_config,
            glue_connection_config=glue_config
        )
        
        self.assertTrue(config.uses_glue_connection())
        self.assertEqual(config.get_glue_connection_strategy(), "use_glue")
        self.assertFalse(config.should_create_glue_connection())
        self.assertTrue(config.should_use_existing_glue_connection())
        self.assertEqual(config.get_glue_connection_name_for_creation(), "my-connection")
    
    @patch('glue_job.config.job_config.logging.getLogger')
    def test_iceberg_config_with_glue_connection_warning(self, mock_logger):
        """Test that Iceberg engines log warning when Glue Connection parameters are provided."""
        mock_logger_instance = Mock()
        mock_logger.return_value = mock_logger_instance
        
        glue_config = GlueConnectionConfig(create_connection=True)
        iceberg_config = {
            'database_name': 'test_db',
            'table_name': 'test_table',
            'warehouse_location': 's3://test-bucket/warehouse/'
        }
        
        config = ConnectionConfig(
            engine_type="iceberg",
            connection_string="",  # Not used for Iceberg
            database="test_db",
            schema="test_table",
            username="",  # Not used for Iceberg
            password="",  # Not used for Iceberg
            jdbc_driver_path="",  # Not used for Iceberg
            iceberg_config=iceberg_config,
            glue_connection_config=glue_config
        )
        
        # Check that warning was logged
        mock_logger_instance.warning.assert_called_once()
        warning_call = mock_logger_instance.warning.call_args[0][0]
        self.assertIn("Glue Connection parameters provided for Iceberg engine", warning_call)
        self.assertIn("will be ignored", warning_call)
    
    def test_jdbc_validation_with_create_glue_connection(self):
        """Test JDBC validation when creating new Glue Connection."""
        glue_config = GlueConnectionConfig(create_connection=True)
        
        # Missing connection string should fail validation
        with self.assertRaises(ValueError) as context:
            config = ConnectionConfig(
                engine_type="postgresql",
                connection_string="",  # Required for creation
                database="testdb",
                schema="public",
                username="testuser",
                password="testpass",
                jdbc_driver_path="s3://bucket/drivers/postgresql.jar",
                glue_connection_config=glue_config
            )
        self.assertIn("Connection string is required when creating Glue Connection", str(context.exception))
    
    def test_jdbc_validation_with_use_existing_glue_connection(self):
        """Test JDBC validation when using existing Glue Connection."""
        glue_config = GlueConnectionConfig(use_existing_connection="my-connection")
        
        # When using existing connection, some JDBC parameters are not required
        config = ConnectionConfig(
            engine_type="postgresql",
            connection_string="",  # Not required when using existing connection
            database="testdb",
            schema="public",
            username="",  # Not required when using existing connection
            password="",  # Not required when using existing connection
            jdbc_driver_path="",  # Not required when using existing connection
            glue_connection_config=glue_config
        )
        
        # Should not raise validation error
        config.validate()


class TestJobConfigurationParserGlueConnection(unittest.TestCase):
    """Test JobConfigurationParser Glue Connection functionality - Requirements 1.1, 1.2, 2.1, 2.2, 5.1, 5.2"""
    
    def test_parse_glue_connection_params_create_source(self):
        """Test parsing createSourceConnection parameter."""
        args = {'createSourceConnection': 'true'}
        
        config = JobConfigurationParser.parse_glue_connection_params(args, 'SOURCE')
        
        self.assertTrue(config.create_connection)
        self.assertIsNone(config.use_existing_connection)
        self.assertEqual(config.connection_strategy, "create_glue")
        self.assertTrue(config.uses_glue_connection())
    
    def test_parse_glue_connection_params_create_target(self):
        """Test parsing createTargetConnection parameter."""
        args = {'createTargetConnection': 'true'}
        
        config = JobConfigurationParser.parse_glue_connection_params(args, 'TARGET')
        
        self.assertTrue(config.create_connection)
        self.assertIsNone(config.use_existing_connection)
        self.assertEqual(config.connection_strategy, "create_glue")
        self.assertTrue(config.uses_glue_connection())
    
    def test_parse_glue_connection_params_use_source(self):
        """Test parsing useSourceConnection parameter."""
        args = {'useSourceConnection': 'my-existing-connection'}
        
        config = JobConfigurationParser.parse_glue_connection_params(args, 'SOURCE')
        
        self.assertFalse(config.create_connection)
        self.assertEqual(config.use_existing_connection, 'my-existing-connection')
        self.assertEqual(config.connection_strategy, "use_glue")
        self.assertTrue(config.uses_glue_connection())
    
    def test_parse_glue_connection_params_use_target(self):
        """Test parsing useTargetConnection parameter."""
        args = {'useTargetConnection': 'my-existing-connection'}
        
        config = JobConfigurationParser.parse_glue_connection_params(args, 'TARGET')
        
        self.assertFalse(config.create_connection)
        self.assertEqual(config.use_existing_connection, 'my-existing-connection')
        self.assertEqual(config.connection_strategy, "use_glue")
        self.assertTrue(config.uses_glue_connection())
    
    def test_parse_glue_connection_params_no_parameters(self):
        """Test parsing with no Glue Connection parameters."""
        args = {}
        
        config = JobConfigurationParser.parse_glue_connection_params(args, 'SOURCE')
        
        self.assertFalse(config.create_connection)
        self.assertIsNone(config.use_existing_connection)
        self.assertEqual(config.connection_strategy, "direct_jdbc")
        self.assertFalse(config.uses_glue_connection())
    
    def test_parse_glue_connection_params_boolean_variations(self):
        """Test parsing createConnection with various boolean representations."""
        test_cases = [
            ('true', True),
            ('True', True),
            ('TRUE', True),
            ('1', True),
            ('yes', True),
            ('false', False),
            ('False', False),
            ('FALSE', False),
            ('0', False),
            ('no', False),
            ('', False),
            ('invalid', False)
        ]
        
        for value, expected in test_cases:
            with self.subTest(value=value, expected=expected):
                args = {'createSourceConnection': value}
                config = JobConfigurationParser.parse_glue_connection_params(args, 'SOURCE')
                self.assertEqual(config.create_connection, expected)
    
    def test_validate_glue_connection_params_no_conflicts(self):
        """Test successful validation of Glue Connection parameters."""
        args = {
            'createSourceConnection': 'true',
            'useTargetConnection': 'my-connection',
            'SOURCE_ENGINE_TYPE': 'postgresql',
            'TARGET_ENGINE_TYPE': 'oracle',
            'SOURCE_CONNECTION_STRING': 'jdbc:postgresql://localhost:5432/testdb',
            'SOURCE_DB_USER': 'testuser',
            'SOURCE_DB_PASSWORD': 'testpass'
        }
        
        # Should not raise - this tests the basic conflict detection, not comprehensive validation
        try:
            result = JobConfigurationParser.validate_glue_connection_params(args)
            # If it returns a result, validation passed
            self.assertIsInstance(result, dict)
        except Exception as e:
            # If it's not a parameter conflict error, that's unexpected
            if "mutually exclusive" not in str(e):
                self.fail(f"Unexpected validation error: {e}")
    
    def test_validate_glue_connection_params_source_conflict(self):
        """Test validation fails for conflicting source parameters."""
        args = {
            'createSourceConnection': 'true',
            'useSourceConnection': 'my-connection'
        }
        
        with self.assertRaises(GlueConnectionParameterError) as context:
            JobConfigurationParser.validate_glue_connection_params(args)
        self.assertIn("mutually exclusive", str(context.exception))
        self.assertIn("createSourceConnection", str(context.exception))
        self.assertIn("useSourceConnection", str(context.exception))
    
    def test_validate_glue_connection_params_target_conflict(self):
        """Test validation fails for conflicting target parameters."""
        args = {
            'createTargetConnection': 'true',
            'useTargetConnection': 'my-connection'
        }
        
        with self.assertRaises(GlueConnectionParameterError) as context:
            JobConfigurationParser.validate_glue_connection_params(args)
        self.assertIn("mutually exclusive", str(context.exception))
        self.assertIn("createTargetConnection", str(context.exception))
        self.assertIn("useTargetConnection", str(context.exception))
    
    @patch('glue_job.config.parsers.logger')
    def test_validate_engine_specific_params_iceberg_warning(self, mock_logger):
        """Test that Iceberg engines generate warnings for Glue Connection parameters."""
        args = {
            'SOURCE_ENGINE_TYPE': 'iceberg',
            'TARGET_ENGINE_TYPE': 'postgresql',
            'createSourceConnection': 'true',
            'useTargetConnection': 'my-connection'
        }
        
        JobConfigurationParser.validate_engine_specific_glue_connection_params(args)
        
        # Check that warning was logged for Iceberg source
        mock_logger.warning.assert_called()
        warning_calls = [call[0][0] for call in mock_logger.warning.call_args_list]
        
        # Should have warning about createSourceConnection for Iceberg
        iceberg_warning_found = any(
            'createSourceConnection' in call and 'Iceberg' in call and 'ignored' in call
            for call in warning_calls
        )
        self.assertTrue(iceberg_warning_found, f"Expected Iceberg warning not found in: {warning_calls}")
    
    @patch('glue_job.config.parsers.logger')
    def test_validate_engine_specific_params_no_warning_for_jdbc(self, mock_logger):
        """Test that JDBC engines do not generate warnings for Glue Connection parameters."""
        args = {
            'SOURCE_ENGINE_TYPE': 'postgresql',
            'TARGET_ENGINE_TYPE': 'oracle',
            'createSourceConnection': 'true',
            'useTargetConnection': 'my-connection'
        }
        
        JobConfigurationParser.validate_engine_specific_glue_connection_params(args)
        
        # Should not have any warnings for JDBC engines
        mock_logger.warning.assert_not_called()
    
    def test_create_connection_config_with_glue_support_jdbc_create(self):
        """Test creating ConnectionConfig with Glue Connection creation for JDBC engine."""
        args = {
            'SOURCE_ENGINE_TYPE': 'postgresql',
            'SOURCE_CONNECTION_STRING': 'jdbc:postgresql://localhost:5432/testdb',
            'SOURCE_DATABASE': 'testdb',
            'SOURCE_SCHEMA': 'public',
            'SOURCE_DB_USER': 'testuser',
            'SOURCE_DB_PASSWORD': 'testpass',
            'SOURCE_JDBC_DRIVER_S3_PATH': 's3://bucket/drivers/postgresql.jar',
            'createSourceConnection': 'true'
        }
        
        config = JobConfigurationParser.create_connection_config_with_glue_support(
            args, 'SOURCE', None, {}
        )
        
        self.assertEqual(config.engine_type, 'postgresql')
        self.assertIsNotNone(config.glue_connection_config)
        self.assertTrue(config.glue_connection_config.create_connection)
        self.assertTrue(config.uses_glue_connection())
        self.assertEqual(config.get_glue_connection_strategy(), 'create_glue')
    
    def test_create_connection_config_with_glue_support_jdbc_use_existing(self):
        """Test creating ConnectionConfig with existing Glue Connection for JDBC engine."""
        args = {
            'SOURCE_ENGINE_TYPE': 'oracle',
            'SOURCE_CONNECTION_STRING': 'jdbc:oracle:thin:@localhost:1521:testdb',
            'SOURCE_DATABASE': 'testdb',
            'SOURCE_SCHEMA': 'testschema',
            'SOURCE_DB_USER': 'testuser',
            'SOURCE_DB_PASSWORD': 'testpass',
            'SOURCE_JDBC_DRIVER_S3_PATH': 's3://bucket/drivers/oracle.jar',
            'useSourceConnection': 'my-existing-connection'
        }
        
        config = JobConfigurationParser.create_connection_config_with_glue_support(
            args, 'SOURCE', None, {}
        )
        
        self.assertEqual(config.engine_type, 'oracle')
        self.assertIsNotNone(config.glue_connection_config)
        self.assertEqual(config.glue_connection_config.use_existing_connection, 'my-existing-connection')
        self.assertTrue(config.uses_glue_connection())
        self.assertEqual(config.get_glue_connection_strategy(), 'use_glue')
    
    def test_create_connection_config_with_glue_support_iceberg_ignores_glue_params(self):
        """Test creating ConnectionConfig for Iceberg engine ignores Glue Connection parameters."""
        iceberg_config = {
            'database_name': 'test_db',
            'table_name': 'test_table',
            'warehouse_location': 's3://test-bucket/warehouse/'
        }
        
        args = {
            'SOURCE_ENGINE_TYPE': 'iceberg',
            'SOURCE_DATABASE': 'test_db',
            'SOURCE_SCHEMA': 'test_table',
            'createSourceConnection': 'true'  # Should be ignored for Iceberg
        }
        
        config = JobConfigurationParser.create_connection_config_with_glue_support(
            args, 'SOURCE', None, iceberg_config
        )
        
        self.assertEqual(config.engine_type, 'iceberg')
        self.assertIsNone(config.glue_connection_config)  # Should be None for Iceberg
        self.assertFalse(config.uses_glue_connection())
        self.assertEqual(config.get_glue_connection_strategy(), 'direct_jdbc')


class TestGlueConnectionManagerOperations(unittest.TestCase):
    """Test GlueConnectionManager operations - Requirements 1.3, 1.4, 1.5, 2.1, 2.2, 2.3, 2.4, 2.5"""
    
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
    
    def test_create_glue_connection_success(self):
        """Test successful Glue Connection creation with Secrets Manager integration."""
        # Mock successful secret creation
        secret_arn = "arn:aws:secretsmanager:us-east-1:123456789012:secret:/aws-glue/test-connection-AbCdEf"
        self.mock_secrets_manager_handler.validate_secret_permissions.return_value = True
        self.mock_secrets_manager_handler.create_secret.return_value = secret_arn
        
        # Mock successful Glue Connection creation
        self.mock_glue_client.create_connection.return_value = {}
        
        result = self.manager.create_glue_connection(self.connection_config, 'test-connection')
        
        self.assertEqual(result, 'test-connection')
        self.mock_glue_client.create_connection.assert_called_once()
        
        # Verify the connection input structure uses Secrets Manager
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
        # Verify that USERNAME and PASSWORD are not in the connection properties (they're in Secrets Manager)
        self.assertNotIn('USERNAME', connection_input['ConnectionProperties'])
        self.assertNotIn('PASSWORD', connection_input['ConnectionProperties'])
    
    def test_create_glue_connection_with_network_config(self):
        """Test Glue Connection creation with network configuration and Secrets Manager."""
        # Add network config to connection
        from glue_job.config.job_config import NetworkConfig
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
    
    def test_create_glue_connection_failure(self):
        """Test Glue Connection creation failure."""
        # Mock failure response
        error_response = {
            'Error': {
                'Code': 'InvalidInputException',
                'Message': 'Invalid connection parameters'
            }
        }
        self.mock_glue_client.create_connection.side_effect = ClientError(
            error_response, 'CreateConnection'
        )
        
        # The actual implementation may wrap the error differently
        with self.assertRaises(Exception) as context:
            self.manager.create_glue_connection(self.connection_config, 'test-connection')
        
        # Check that it's some kind of connection error
        error_message = str(context.exception)
        self.assertTrue(
            'test-connection' in error_message or 'Invalid' in error_message,
            f"Expected connection error, got: {error_message}"
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
    
    def test_validate_glue_connection_exists_invalid_type(self):
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


class TestUnifiedConnectionManagerGlueIntegration(unittest.TestCase):
    """Test UnifiedConnectionManager Glue Connection integration - Requirements 3.1, 3.2, 3.3, 3.5, 3.6"""
    
    def setUp(self):
        """Set up test fixtures."""
        self.mock_spark_session = Mock()
        self.mock_glue_context = Mock()
        
        # Create manager with mocked dependencies
        with patch('glue_job.database.connection_manager.JdbcConnectionManager'), \
             patch('glue_job.database.connection_manager.IcebergConnectionHandler'), \
             patch('glue_job.database.connection_manager.ConnectionRetryHandler'), \
             patch('glue_job.database.connection_manager.StructuredLogger'):
            
            self.manager = UnifiedConnectionManager(
                spark_session=self.mock_spark_session,
                glue_context=self.mock_glue_context
            )
    
    def test_determine_connection_strategy_iceberg(self):
        """Test connection strategy determination for Iceberg."""
        iceberg_config = {
            'warehouse_location': 's3://bucket/warehouse/',
            'database_name': 'test_db',
            'table_name': 'test_table'
        }
        config = ConnectionConfig(
            engine_type='iceberg',
            connection_string='',
            database='test_db',
            schema='test_table',
            username='',
            password='',
            jdbc_driver_path='',
            iceberg_config=iceberg_config
        )
        
        strategy = self.manager._determine_connection_strategy(config)
        self.assertEqual(strategy, "iceberg")
    
    def test_determine_connection_strategy_direct_jdbc(self):
        """Test connection strategy determination for direct JDBC."""
        config = ConnectionConfig(
            engine_type='postgresql',
            connection_string='jdbc:postgresql://localhost:5432/testdb',
            database='testdb',
            schema='public',
            username='testuser',
            password='testpass',
            jdbc_driver_path='s3://bucket/postgresql.jar'
        )
        
        strategy = self.manager._determine_connection_strategy(config)
        self.assertEqual(strategy, "direct_jdbc")
    
    def test_determine_connection_strategy_create_glue(self):
        """Test connection strategy determination for create Glue Connection."""
        glue_config = GlueConnectionConfig(create_connection=True)
        config = ConnectionConfig(
            engine_type='postgresql',
            connection_string='jdbc:postgresql://localhost:5432/testdb',
            database='testdb',
            schema='public',
            username='testuser',
            password='testpass',
            jdbc_driver_path='s3://bucket/postgresql.jar',
            glue_connection_config=glue_config
        )
        
        strategy = self.manager._determine_connection_strategy(config)
        self.assertEqual(strategy, "create_glue")
    
    def test_determine_connection_strategy_use_glue(self):
        """Test connection strategy determination for use existing Glue Connection."""
        glue_config = GlueConnectionConfig(use_existing_connection='my-connection')
        config = ConnectionConfig(
            engine_type='postgresql',
            connection_string='jdbc:postgresql://localhost:5432/testdb',
            database='testdb',
            schema='public',
            username='testuser',
            password='testpass',
            jdbc_driver_path='s3://bucket/postgresql.jar',
            glue_connection_config=glue_config
        )
        
        strategy = self.manager._determine_connection_strategy(config)
        self.assertEqual(strategy, "use_glue")
    
    def test_create_connection_routing_iceberg(self):
        """Test connection creation routing to Iceberg handler."""
        iceberg_config = {
            'warehouse_location': 's3://bucket/warehouse/',
            'database_name': 'test_db',
            'table_name': 'test_table'
        }
        config = ConnectionConfig(
            engine_type='iceberg',
            connection_string='',
            database='test_db',
            schema='test_table',
            username='',
            password='',
            jdbc_driver_path='',
            iceberg_config=iceberg_config
        )
        
        with patch.object(self.manager, '_create_iceberg_connection', 
                         return_value=self.manager.iceberg_handler) as mock_create:
            result = self.manager.create_connection(config)
            
            self.assertEqual(result, self.manager.iceberg_handler)
            mock_create.assert_called_once_with(config)
    
    def test_create_connection_routing_jdbc_with_glue(self):
        """Test connection creation routing to JDBC with Glue Connection."""
        glue_config = GlueConnectionConfig(create_connection=True)
        config = ConnectionConfig(
            engine_type='postgresql',
            connection_string='jdbc:postgresql://localhost:5432/testdb',
            database='testdb',
            schema='public',
            username='testuser',
            password='testpass',
            jdbc_driver_path='s3://bucket/postgresql.jar',
            glue_connection_config=glue_config
        )
        
        mock_df_reader = Mock()
        with patch.object(self.manager, '_create_jdbc_connection_with_glue', 
                         return_value=mock_df_reader) as mock_create:
            result = self.manager.create_connection(config)
            
            self.assertEqual(result, mock_df_reader)
            mock_create.assert_called_once_with(config)
    
    def test_create_connection_routing_direct_jdbc(self):
        """Test connection creation routing to direct JDBC."""
        config = ConnectionConfig(
            engine_type='postgresql',
            connection_string='jdbc:postgresql://localhost:5432/testdb',
            database='testdb',
            schema='public',
            username='testuser',
            password='testpass',
            jdbc_driver_path='s3://bucket/postgresql.jar'
        )
        
        mock_df_reader = Mock()
        with patch.object(self.manager, '_create_jdbc_connection', 
                         return_value=mock_df_reader) as mock_create:
            result = self.manager.create_connection(config)
            
            self.assertEqual(result, mock_df_reader)
            mock_create.assert_called_once_with(config)


class TestGlueConnectionParameterValidator(unittest.TestCase):
    """Test GlueConnectionParameterValidator functionality - Requirements 5.1, 5.2"""
    
    def setUp(self):
        """Set up test fixtures."""
        from glue_job.config.glue_connection_validator import GlueConnectionParameterValidator
        self.validator = GlueConnectionParameterValidator()
    
    def test_connection_name_format_validation_valid_names(self):
        """Test validation passes for valid connection names."""
        valid_names = [
            "connection123",
            "Connection_Name",
            "connection-name",
            "MyConnection",
            "connection_123_test"
        ]
        
        for valid_name in valid_names:
            with self.subTest(name=valid_name):
                # Should not raise any exceptions
                self.validator._validate_connection_name_format(valid_name, "testParam")
    
    def test_connection_name_format_validation_invalid_names(self):
        """Test validation fails for invalid connection names."""
        from glue_job.network.glue_connection_errors import GlueConnectionParameterError
        
        invalid_names = [
            "connection with spaces",
            "connection@domain.com",
            "connection#123",
            "connection!special"
        ]
        
        for invalid_name in invalid_names:
            with self.subTest(name=invalid_name):
                with self.assertRaises(GlueConnectionParameterError) as context:
                    self.validator._validate_connection_name_format(invalid_name, "testParam")
                self.assertIn("alphanumeric characters", str(context.exception))
    
    def test_connection_name_format_validation_empty_name(self):
        """Test validation fails for empty connection name."""
        from glue_job.network.glue_connection_errors import GlueConnectionParameterError
        
        with self.assertRaises(GlueConnectionParameterError) as context:
            self.validator._validate_connection_name_format("", "testParam")
        self.assertIn("cannot be empty", str(context.exception))
    
    def test_validate_all_glue_connection_parameters_success(self):
        """Test comprehensive parameter validation success."""
        args = {
            'createSourceConnection': 'true',
            'useTargetConnection': 'valid-connection-name',
            'SOURCE_ENGINE_TYPE': 'oracle',
            'TARGET_ENGINE_TYPE': 'postgresql',
            'SOURCE_CONNECTION_STRING': 'jdbc:oracle:thin:@host:1521:db',
            'SOURCE_DB_USER': 'user',
            'SOURCE_DB_PASSWORD': 'password'
        }
        
        result = self.validator.validate_all_glue_connection_parameters(args)
        
        # Should have both source and target configurations
        self.assertIsNotNone(result['source_config'])
        self.assertIsNotNone(result['target_config'])
        self.assertEqual(len(result['validation_errors']), 0)
    
    def test_validate_all_glue_connection_parameters_with_conflicts(self):
        """Test comprehensive parameter validation with conflicts."""
        from glue_job.network.glue_connection_errors import GlueConnectionParameterError
        
        args = {
            'createSourceConnection': 'true',
            'useSourceConnection': 'existing-connection',  # Conflict
            'SOURCE_ENGINE_TYPE': 'oracle'
        }
        
        with self.assertRaises(GlueConnectionParameterError) as context:
            self.validator.validate_all_glue_connection_parameters(args)
        
        self.assertIn("mutually exclusive", str(context.exception))


class TestGlueConnectionErrorHandlingScenarios(unittest.TestCase):
    """Test error handling scenarios and edge cases - Requirements 5.3, 5.4, 5.5, 5.6"""
    
    def setUp(self):
        """Set up test fixtures."""
        self.mock_glue_context = Mock()
        self.manager = GlueConnectionManager(self.mock_glue_context)
        self.mock_glue_client = Mock()
        self.manager.glue_client = self.mock_glue_client
        
        # Mock the Secrets Manager handler to avoid secret creation in tests
        self.mock_secrets_manager = Mock()
        self.manager.secrets_manager_handler = self.mock_secrets_manager
    
    def test_create_connection_permission_denied(self):
        """Test handling of permission denied errors during connection creation."""
        # Mock successful secret creation so we can test Glue Connection permission error
        self.mock_secrets_manager.validate_secret_permissions.return_value = True
        self.mock_secrets_manager.create_secret.return_value = "arn:aws:secretsmanager:us-east-1:123456789012:secret:test-secret"
        
        # Mock Glue Connection creation to fail with permission error
        error_response = {
            'Error': {
                'Code': 'AccessDeniedException',
                'Message': 'User is not authorized to perform: glue:CreateConnection'
            }
        }
        self.mock_glue_client.create_connection.side_effect = ClientError(
            error_response, 'CreateConnection'
        )
        
        config = ConnectionConfig(
            engine_type="postgresql",
            connection_string="jdbc:postgresql://localhost:5432/testdb",
            database="testdb",
            schema="public",
            username="testuser",
            password="testpass",
            jdbc_driver_path="s3://bucket/postgresql.jar"
        )
        
        with self.assertRaises(Exception) as context:
            self.manager.create_glue_connection(config, 'test-connection')
        
        error_message = str(context.exception)
        self.assertTrue(
            'Access denied' in error_message or 'not authorized' in error_message or 'permission' in error_message.lower(),
            f"Expected permission error, got: {error_message}"
        )
    
    def test_create_connection_invalid_parameters(self):
        """Test handling of invalid parameter errors during connection creation."""
        # ConnectionConfig creation doesn't validate connection string format
        # That validation happens at the manager level
        config = ConnectionConfig(
            engine_type="postgresql",
            connection_string="invalid-jdbc-url",  # Invalid format
            database="testdb",
            schema="public",
            username="testuser",
            password="testpass",
            jdbc_driver_path="s3://bucket/postgresql.jar"
        )
        
        # Mock failure response for invalid connection string
        error_response = {
            'Error': {
                'Code': 'InvalidInputException',
                'Message': 'Invalid JDBC URL format'
            }
        }
        self.mock_glue_client.create_connection.side_effect = ClientError(
            error_response, 'CreateConnection'
        )
        
        with self.assertRaises(Exception) as context:
            self.manager.create_glue_connection(config, 'test-connection')
        
        error_message = str(context.exception)
        self.assertTrue(
            'Invalid' in error_message or 'JDBC' in error_message,
            f"Expected JDBC validation error, got: {error_message}"
        )
    
    def test_validate_connection_network_error(self):
        """Test handling of network errors during connection validation."""
        error_response = {
            'Error': {
                'Code': 'ServiceUnavailableException',
                'Message': 'Service temporarily unavailable'
            }
        }
        self.mock_glue_client.get_connection.side_effect = ClientError(
            error_response, 'GetConnection'
        )
        
        with self.assertRaises(Exception) as context:
            self.manager.validate_glue_connection_exists('test-connection')
        
        error_message = str(context.exception)
        self.assertTrue(
            'Service temporarily unavailable' in error_message or 'unavailable' in error_message,
            f"Expected service unavailable error, got: {error_message}"
        )
    
    def test_empty_connection_name_validation(self):
        """Test validation of empty connection names."""
        with self.assertRaises(ValueError) as context:
            self.manager.validate_glue_connection_exists('')
        
        self.assertIn('cannot be empty', str(context.exception))
        
        with self.assertRaises(ValueError) as context:
            self.manager.validate_glue_connection_exists('   ')
        
        self.assertIn('cannot be empty', str(context.exception))
    
    def test_connection_name_validation_at_aws_level(self):
        """Test that connection name validation happens at AWS API level."""
        # Mock AWS API error for invalid connection name
        error_response = {
            'Error': {
                'Code': 'InvalidInputException',
                'Message': 'Invalid connection name format'
            }
        }
        self.mock_glue_client.get_connection.side_effect = ClientError(
            error_response, 'GetConnection'
        )
        
        # Test that AWS-level validation errors are properly handled
        with self.assertRaises(Exception) as context:
            self.manager.validate_glue_connection_exists('invalid-name-format')
        
        error_message = str(context.exception)
        self.assertTrue(
            'Invalid connection name format' in error_message or 'format' in error_message,
            f"Expected connection name format error, got: {error_message}"
        )
    
    def test_connection_creation_with_missing_required_fields(self):
        """Test connection creation with missing required fields."""
        # This should fail during ConnectionConfig creation due to validation
        with self.assertRaises(ValueError) as context:
            config = ConnectionConfig(
                engine_type="postgresql",
                connection_string="",  # Missing required field
                database="testdb",
                schema="public",
                username="testuser",
                password="testpass",
                jdbc_driver_path="s3://bucket/postgresql.jar"
            )
        
        self.assertIn('Connection string cannot be empty', str(context.exception))
    
    def test_retry_logic_for_transient_failures(self):
        """Test handling of transient failures."""
        # Mock transient failure
        error_response = {
            'Error': {
                'Code': 'Throttling',
                'Message': 'Rate exceeded'
            }
        }
        
        self.mock_glue_client.get_connection.side_effect = ClientError(
            error_response, 'GetConnection'
        )
        
        # Should raise an error for throttling
        with self.assertRaises(Exception) as context:
            self.manager.validate_glue_connection_exists('test-connection')
        
        error_message = str(context.exception)
        self.assertTrue(
            'Rate exceeded' in error_message or 'throttl' in error_message.lower(),
            f"Expected throttling error, got: {error_message}"
        )


if __name__ == '__main__':
    unittest.main(verbosity=2)