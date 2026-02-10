#!/usr/bin/env python3
"""
Unit tests for configuration modules.

This test suite covers:
- JobConfig, NetworkConfig, ConnectionConfig dataclasses
- DatabaseEngineManager and JdbcDriverLoader functionality
- JobConfigurationParser and ConnectionStringBuilder
"""

import unittest
from unittest.mock import Mock, MagicMock, patch
import sys
import os
from dataclasses import dataclass
from typing import Dict, Any

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

# Create a mock ClientError class for testing
class MockClientError(Exception):
    """Mock ClientError that behaves like botocore.exceptions.ClientError"""
    def __init__(self, error_response, operation_name):
        self.response = error_response
        self.operation_name = operation_name
        error_code = error_response.get('Error', {}).get('Code', 'Unknown')
        super().__init__(f"An error occurred ({error_code})")

# Import the classes to test from new modular structure
from glue_job.config import (
    JobConfig, NetworkConfig, ConnectionConfig, GlueConnectionConfig,
    DatabaseEngineManager, JdbcDriverLoader,
    JobConfigurationParser, ConnectionStringBuilder,
    SecretsManagerHandler, SecretsManagerError, SecretCreationError,
    SecretsManagerPermissionError, SecretsManagerRetryableError
)


class TestGlueConnectionConfig(unittest.TestCase):
    """Test GlueConnectionConfig dataclass."""
    
    def test_glue_connection_config_default(self):
        """Test default GlueConnectionConfig creation."""
        config = GlueConnectionConfig()
        
        self.assertFalse(config.create_connection)
        self.assertIsNone(config.use_existing_connection)
        self.assertEqual(config.connection_strategy, "direct_jdbc")
        self.assertFalse(config.uses_glue_connection())
    
    def test_glue_connection_config_create_connection(self):
        """Test GlueConnectionConfig with create_connection=True."""
        config = GlueConnectionConfig(create_connection=True)
        
        self.assertTrue(config.create_connection)
        self.assertIsNone(config.use_existing_connection)
        self.assertEqual(config.connection_strategy, "create_glue")
        self.assertTrue(config.uses_glue_connection())
    
    def test_glue_connection_config_use_existing(self):
        """Test GlueConnectionConfig with existing connection name."""
        config = GlueConnectionConfig(use_existing_connection="my-connection")
        
        self.assertFalse(config.create_connection)
        self.assertEqual(config.use_existing_connection, "my-connection")
        self.assertEqual(config.connection_strategy, "use_glue")
        self.assertTrue(config.uses_glue_connection())
    
    def test_glue_connection_config_validation_success(self):
        """Test successful validation of GlueConnectionConfig."""
        # Test default config
        config = GlueConnectionConfig()
        config.validate()  # Should not raise
        
        # Test create connection
        config = GlueConnectionConfig(create_connection=True)
        config.validate()  # Should not raise
        
        # Test use existing connection
        config = GlueConnectionConfig(use_existing_connection="my-connection")
        config.validate()  # Should not raise
    
    def test_glue_connection_config_validation_mutually_exclusive(self):
        """Test validation fails for mutually exclusive parameters."""
        config = GlueConnectionConfig(
            create_connection=True,
            use_existing_connection="my-connection"
        )
        
        with self.assertRaises(ValueError) as context:
            config.validate()
        self.assertIn("mutually exclusive", str(context.exception))
    
    def test_glue_connection_config_validation_empty_connection_name(self):
        """Test validation fails for empty connection name."""
        config = GlueConnectionConfig(use_existing_connection="")
        
        with self.assertRaises(ValueError) as context:
            config.validate()
        self.assertIn("cannot be empty", str(context.exception))
    
    def test_glue_connection_config_validation_whitespace_connection_name(self):
        """Test validation fails for whitespace-only connection name."""
        config = GlueConnectionConfig(use_existing_connection="   ")
        
        with self.assertRaises(ValueError) as context:
            config.validate()
        self.assertIn("cannot be empty", str(context.exception))
    
    def test_glue_connection_config_validation_invalid_types(self):
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


class TestConnectionConfig(unittest.TestCase):
    """Test ConnectionConfig dataclass."""
    
    def test_connection_config_creation(self):
        """Test creating a ConnectionConfig instance."""
        config = ConnectionConfig(
            engine_type="postgresql",
            connection_string="jdbc:postgresql://localhost:5432/testdb",
            database="testdb",
            schema="public",
            username="testuser",
            password="testpass",
            jdbc_driver_path="s3://bucket/drivers/postgresql.jar"
        )
        
        self.assertEqual(config.engine_type, "postgresql")
        self.assertEqual(config.database, "testdb")
        self.assertEqual(config.schema, "public")
        self.assertEqual(config.username, "testuser")
        self.assertEqual(config.password, "testpass")
    
    def test_connection_config_with_glue_connection_config(self):
        """Test ConnectionConfig with GlueConnectionConfig."""
        glue_config = GlueConnectionConfig(create_connection=True)
        
        config = ConnectionConfig(
            engine_type="postgresql",
            connection_string="jdbc:postgresql://localhost:5432/testdb",
            database="testdb",
            schema="public",
            username="testuser",
            password="testpass",
            jdbc_driver_path="s3://bucket/drivers/postgresql.jar",
            glue_connection_config=glue_config
        )
        
        self.assertEqual(config.glue_connection_config, glue_config)
        self.assertTrue(config.uses_glue_connection())
        self.assertEqual(config.get_glue_connection_strategy(), "create_glue")
        self.assertTrue(config.should_create_glue_connection())
        self.assertFalse(config.should_use_existing_glue_connection())
        self.assertIsNone(config.get_glue_connection_name_for_creation())
    
    def test_connection_config_glue_connection_methods_default(self):
        """Test Glue Connection methods with default ConnectionConfig."""
        config = ConnectionConfig(
            engine_type="postgresql",
            connection_string="jdbc:postgresql://localhost:5432/testdb",
            database="testdb",
            schema="public",
            username="testuser",
            password="testpass",
            jdbc_driver_path="s3://bucket/drivers/postgresql.jar"
        )
        
        self.assertFalse(config.uses_glue_connection())
        self.assertEqual(config.get_glue_connection_strategy(), "direct_jdbc")
        self.assertFalse(config.should_create_glue_connection())
        self.assertFalse(config.should_use_existing_glue_connection())
        self.assertIsNone(config.get_glue_connection_name_for_creation())
    
    def test_connection_config_use_existing_glue_connection(self):
        """Test ConnectionConfig with existing Glue Connection."""
        glue_config = GlueConnectionConfig(use_existing_connection="my-connection")
        
        config = ConnectionConfig(
            engine_type="postgresql",
            connection_string="jdbc:postgresql://localhost:5432/testdb",
            database="testdb",
            schema="public",
            username="testuser",
            password="testpass",
            jdbc_driver_path="s3://bucket/drivers/postgresql.jar",
            glue_connection_config=glue_config
        )
        
        self.assertTrue(config.uses_glue_connection())
        self.assertEqual(config.get_glue_connection_strategy(), "use_glue")
        self.assertFalse(config.should_create_glue_connection())
        self.assertTrue(config.should_use_existing_glue_connection())
        self.assertEqual(config.get_glue_connection_name_for_creation(), "my-connection")
    
    @patch('glue_job.config.job_config.logging.getLogger')
    def test_connection_config_iceberg_with_glue_connection_warning(self, mock_logger):
        """Test that Iceberg engines log warning when Glue Connection parameters are provided."""
        mock_logger_instance = Mock()
        mock_logger.return_value = mock_logger_instance
        
        glue_config = GlueConnectionConfig(create_connection=True)
        iceberg_config = {
            'database_name': 'test_db',
            'table_name': 'test_table',
            'warehouse_location': 's3://test-bucket/warehouse/'
        }
        
        # Creating the config will trigger validation in __post_init__
        config = ConnectionConfig(
            engine_type="iceberg",
            connection_string="",  # Not used for Iceberg
            database="test_db",
            schema="test_table",  # table_name for Iceberg
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
    
    def test_connection_config_jdbc_validation_with_existing_glue_connection(self):
        """Test JDBC validation when using existing Glue Connection."""
        glue_config = GlueConnectionConfig(use_existing_connection="my-connection")
        
        # When using existing Glue Connection, some JDBC parameters are not required
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
    
    def test_connection_config_jdbc_validation_with_create_glue_connection(self):
        """Test JDBC validation when creating new Glue Connection."""
        glue_config = GlueConnectionConfig(create_connection=True)
        
        # When creating Glue Connection, all JDBC parameters are required
        # The validation happens in __post_init__, so we catch the exception during object creation
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


class TestJobConfigurationParserGlueConnection(unittest.TestCase):
    """Test JobConfigurationParser Glue Connection functionality."""
    
    def test_parse_glue_connection_params_create_source(self):
        """Test parsing createSourceConnection parameter."""
        args = {'createSourceConnection': 'true'}
        
        config = JobConfigurationParser.parse_glue_connection_params(args, 'SOURCE')
        
        self.assertTrue(config.create_connection)
        self.assertIsNone(config.use_existing_connection)
        self.assertEqual(config.connection_strategy, "create_glue")
        self.assertTrue(config.uses_glue_connection())
    
    def test_parse_glue_connection_params_use_target(self):
        """Test parsing useTargetConnection parameter."""
        args = {'useTargetConnection': 'my-existing-connection'}
        
        config = JobConfigurationParser.parse_glue_connection_params(args, 'TARGET')
        
        self.assertFalse(config.create_connection)
        self.assertEqual(config.use_existing_connection, 'my-existing-connection')
        self.assertEqual(config.connection_strategy, "use_glue")
        self.assertTrue(config.uses_glue_connection())
    
    def test_parse_glue_connection_params_default(self):
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
    
    def test_validate_glue_connection_params_success(self):
        """Test successful validation of Glue Connection parameters."""
        # Test no conflicts
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
        from glue_job.network.glue_connection_errors import GlueConnectionParameterError
        
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
        from glue_job.network.glue_connection_errors import GlueConnectionParameterError
        
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
    def test_validate_engine_specific_glue_connection_params_iceberg_warning(self, mock_logger):
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
    def test_validate_engine_specific_glue_connection_params_no_warning_for_jdbc(self, mock_logger):
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


class TestJobConfig(unittest.TestCase):
    """Test JobConfig dataclass."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.source_config = ConnectionConfig(
            engine_type="postgresql",
            connection_string="jdbc:postgresql://source.example.com:5432/sourcedb",
            database="sourcedb",
            schema="public",
            username="sourceuser",
            password="sourcepass",
            jdbc_driver_path="s3://bucket/drivers/postgresql.jar"
        )
        
        self.target_config = ConnectionConfig(
            engine_type="postgresql",
            connection_string="jdbc:postgresql://target.example.com:5432/targetdb",
            database="targetdb",
            schema="public",
            username="targetuser",
            password="targetpass",
            jdbc_driver_path="s3://bucket/drivers/postgresql.jar"
        )
    
    def test_job_config_creation(self):
        """Test creating a JobConfig instance."""
        job_config = JobConfig(
            job_name="test-job",
            source_connection=self.source_config,
            target_connection=self.target_config,
            tables=["test_table"]
        )
        
        self.assertEqual(job_config.job_name, "test-job")
        self.assertEqual(job_config.source_connection, self.source_config)
        self.assertEqual(job_config.target_connection, self.target_config)
        self.assertEqual(job_config.tables, ["test_table"])


class TestDatabaseEngineManager(unittest.TestCase):
    """Test DatabaseEngineManager functionality."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.engine_manager = DatabaseEngineManager()
    
    def test_get_driver_class_postgresql(self):
        """Test getting PostgreSQL driver class."""
        driver_class = self.engine_manager.get_driver_class("postgresql")
        self.assertEqual(driver_class, "org.postgresql.Driver")
    
    def test_get_driver_class_mysql_unsupported(self):
        """Test that MySQL is not supported."""
        with self.assertRaises(ValueError) as context:
            self.engine_manager.get_driver_class("mysql")
        self.assertIn("Unsupported database engine: mysql", str(context.exception))
    
    def test_get_driver_class_sqlserver(self):
        """Test getting SQL Server driver class."""
        driver_class = self.engine_manager.get_driver_class("sqlserver")
        self.assertEqual(driver_class, "com.microsoft.sqlserver.jdbc.SQLServerDriver")
    
    def test_get_driver_class_oracle(self):
        """Test getting Oracle driver class."""
        driver_class = self.engine_manager.get_driver_class("oracle")
        self.assertEqual(driver_class, "oracle.jdbc.OracleDriver")
    
    def test_get_driver_class_unknown(self):
        """Test getting driver class for unknown engine."""
        with self.assertRaises(ValueError):
            self.engine_manager.get_driver_class("unknown_engine")


class TestConnectionStringBuilder(unittest.TestCase):
    """Test ConnectionStringBuilder functionality."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.builder = ConnectionStringBuilder()
    
    def test_build_postgresql_connection_string(self):
        """Test building PostgreSQL connection string."""
        connection_string = self.builder.build_connection_string(
            engine_type="postgresql",
            host="localhost",
            port=5432,
            database="testdb"
        )
        expected = "jdbc:postgresql://localhost:5432/testdb"
        self.assertEqual(connection_string, expected)
    
    def test_build_mysql_connection_string_unsupported(self):
        """Test that MySQL connection string building is not supported."""
        with self.assertRaises(ValueError):
            self.builder.build_connection_string(
                engine_type="mysql",
                host="localhost",
                port=3306,
                database="testdb"
            )


class TestSecretsManagerHandler(unittest.TestCase):
    """Test SecretsManagerHandler functionality."""
    
    def setUp(self):
        """Set up test fixtures."""
        # Mock boto3 to avoid actual AWS calls during testing
        self.boto3_patcher = patch('glue_job.config.secrets_manager_handler.boto3')
        self.mock_boto3 = self.boto3_patcher.start()
        
        # Mock the client
        self.mock_client = Mock()
        self.mock_boto3.client.return_value = self.mock_client
        
        # Create handler instance
        self.handler = SecretsManagerHandler(region_name="us-east-1", job_name="test-job")
    
    def tearDown(self):
        """Clean up test fixtures."""
        self.boto3_patcher.stop()
    
    def test_secrets_manager_handler_initialization(self):
        """Test SecretsManagerHandler initialization."""
        self.assertEqual(self.handler.region_name, "us-east-1")
        self.assertEqual(self.handler.job_name, "test-job")
        self.assertIsNotNone(self.handler.structured_logger)
        self.assertIsNotNone(self.handler.retry_handler)
        self.mock_boto3.client.assert_called_once_with('secretsmanager', region_name='us-east-1')
    
    def test_generate_secret_name(self):
        """Test secret name generation."""
        connection_name = "my-test-connection"
        expected_name = "/aws-glue/my-test-connection"
        
        secret_name = self.handler.generate_secret_name(connection_name)
        self.assertEqual(secret_name, expected_name)
    
    def test_generate_secret_name_with_special_characters(self):
        """Test secret name generation with special characters."""
        connection_name = "my test connection!"
        expected_name = "/aws-glue/my-test-connection-"
        
        secret_name = self.handler.generate_secret_name(connection_name)
        self.assertEqual(secret_name, expected_name)
    
    def test_validate_secret_inputs_success(self):
        """Test successful secret input validation."""
        # Should not raise any exception
        self.handler._validate_secret_inputs("test-connection", "username", "password")
    
    def test_validate_secret_inputs_empty_connection_name(self):
        """Test validation fails for empty connection name."""
        with self.assertRaises(SecretsManagerError) as context:
            self.handler._validate_secret_inputs("", "username", "password")
        self.assertIn("Connection name cannot be empty", str(context.exception))
    
    def test_validate_secret_inputs_empty_username(self):
        """Test validation fails for empty username."""
        with self.assertRaises(SecretsManagerError) as context:
            self.handler._validate_secret_inputs("test-connection", "", "password")
        self.assertIn("Username cannot be empty", str(context.exception))
    
    def test_validate_secret_inputs_empty_password(self):
        """Test validation fails for empty password."""
        with self.assertRaises(SecretsManagerError) as context:
            self.handler._validate_secret_inputs("test-connection", "username", "")
        self.assertIn("Password cannot be empty", str(context.exception))
    
    def test_validate_secret_inputs_invalid_connection_name_format(self):
        """Test validation fails for invalid connection name format."""
        with self.assertRaises(SecretsManagerError) as context:
            self.handler._validate_secret_inputs("test connection!", "username", "password")
        self.assertIn("must contain only alphanumeric characters", str(context.exception))
    
    @patch('glue_job.config.secrets_manager_handler.json.dumps')
    def test_create_secret_success(self, mock_json_dumps):
        """Test successful secret creation."""
        # Mock successful AWS response
        mock_response = {'ARN': 'arn:aws:secretsmanager:us-east-1:123456789012:secret:/aws-glue/test-connection-AbCdEf'}
        self.mock_client.create_secret.return_value = mock_response
        mock_json_dumps.return_value = '{"username": "testuser", "password": "testpass"}'
        
        # Call create_secret
        secret_arn = self.handler.create_secret("test-connection", "testuser", "testpass")
        
        # Verify result
        self.assertEqual(secret_arn, mock_response['ARN'])
        
        # Verify AWS client was called correctly
        self.mock_client.create_secret.assert_called_once()
        call_args = self.mock_client.create_secret.call_args[1]
        
        self.assertEqual(call_args['Name'], '/aws-glue/test-connection')
        self.assertIn('Database credentials for Glue Connection', call_args['Description'])
        self.assertEqual(call_args['SecretString'], '{"username": "testuser", "password": "testpass"}')
        self.assertIsInstance(call_args['Tags'], list)
        
        # Verify tags
        tags_dict = {tag['Key']: tag['Value'] for tag in call_args['Tags']}
        self.assertEqual(tags_dict['CreatedBy'], 'AWS-Glue-Data-Replication')
        self.assertEqual(tags_dict['JobName'], 'test-job')
        self.assertEqual(tags_dict['ConnectionName'], 'test-connection')
        self.assertIn('CreatedAt', tags_dict)
    
    def test_create_secret_resource_exists_error(self):
        """Test secret creation fails when secret already exists."""
        # Mock AWS error response
        error_response = {
            'Error': {
                'Code': 'ResourceExistsException',
                'Message': 'The request failed because a resource with the specified name already exists.'
            }
        }
        
        # Mock ClientError in the handler
        with patch('glue_job.config.secrets_manager_handler.ClientError', MockClientError):
            client_error = MockClientError(error_response, 'CreateSecret')
            self.mock_client.create_secret.side_effect = client_error
            
            # Call create_secret and expect SecretCreationError
            with self.assertRaises(SecretCreationError) as context:
                self.handler.create_secret("test-connection", "testuser", "testpass")
            
            self.assertIn("Secret already exists", str(context.exception))
            self.assertEqual(context.exception.aws_error_code, 'ResourceExistsException')
    
    def test_create_secret_permission_error(self):
        """Test secret creation fails with permission error."""
        # Mock AWS permission error response
        error_response = {
            'Error': {
                'Code': 'AccessDeniedException',
                'Message': 'User is not authorized to perform: secretsmanager:CreateSecret'
            }
        }
        
        # Mock ClientError in the handler
        with patch('glue_job.config.secrets_manager_handler.ClientError', MockClientError):
            client_error = MockClientError(error_response, 'CreateSecret')
            self.mock_client.create_secret.side_effect = client_error
            
            # Call create_secret and expect SecretsManagerPermissionError
            with self.assertRaises(SecretsManagerPermissionError) as context:
                self.handler.create_secret("test-connection", "testuser", "testpass")
            
            self.assertIn("Insufficient permissions", str(context.exception))
            self.assertEqual(context.exception.operation, 'create_secret')
    
    def test_create_secret_retryable_error(self):
        """Test secret creation fails with retryable error."""
        # Mock AWS throttling error response
        error_response = {
            'Error': {
                'Code': 'ThrottlingException',
                'Message': 'Rate exceeded'
            }
        }
        
        # Mock ClientError in the handler
        with patch('glue_job.config.secrets_manager_handler.ClientError', MockClientError):
            client_error = MockClientError(error_response, 'CreateSecret')
            self.mock_client.create_secret.side_effect = client_error
            
            # Call create_secret and expect SecretsManagerRetryableError
            with self.assertRaises(SecretsManagerRetryableError) as context:
                self.handler.create_secret("test-connection", "testuser", "testpass")
            
            self.assertIn("Transient error", str(context.exception))
            self.assertEqual(context.exception.retry_after_seconds, 5)
    
    def test_validate_secret_permissions_success(self):
        """Test successful permissions validation."""
        # Mock successful list_secrets call
        self.mock_client.list_secrets.return_value = {'SecretList': []}
        
        result = self.handler.validate_secret_permissions()
        
        self.assertTrue(result)
        self.mock_client.list_secrets.assert_called_once_with(MaxResults=1)
    
    def test_validate_secret_permissions_access_denied(self):
        """Test permissions validation fails with access denied."""
        # Mock AWS permission error response
        error_response = {
            'Error': {
                'Code': 'AccessDeniedException',
                'Message': 'User is not authorized to perform: secretsmanager:ListSecrets'
            }
        }
        
        # Mock ClientError in the handler
        with patch('glue_job.config.secrets_manager_handler.ClientError', MockClientError):
            client_error = MockClientError(error_response, 'ListSecrets')
            self.mock_client.list_secrets.side_effect = client_error
            
            # Call validate_secret_permissions and expect SecretsManagerPermissionError
            with self.assertRaises(SecretsManagerPermissionError) as context:
                self.handler.validate_secret_permissions()
            
            self.assertIn("Insufficient permissions", str(context.exception))
            self.assertEqual(context.exception.operation, 'list_secrets')
    
    def test_cleanup_secret_on_failure_success(self):
        """Test successful secret cleanup."""
        # Mock successful delete_secret call
        self.mock_client.delete_secret.return_value = {}
        
        result = self.handler.cleanup_secret_on_failure("test-connection")
        
        self.assertTrue(result)
        self.mock_client.delete_secret.assert_called_once_with(
            SecretId='/aws-glue/test-connection',
            ForceDeleteWithoutRecovery=True
        )
    
    def test_cleanup_secret_on_failure_not_found(self):
        """Test secret cleanup when secret doesn't exist."""
        # Mock AWS not found error response
        error_response = {
            'Error': {
                'Code': 'ResourceNotFoundException',
                'Message': 'Secrets Manager can\'t find the specified secret.'
            }
        }
        
        # Mock ClientError in the handler
        with patch('glue_job.config.secrets_manager_handler.ClientError', MockClientError):
            client_error = MockClientError(error_response, 'DeleteSecret')
            self.mock_client.delete_secret.side_effect = client_error
            
            result = self.handler.cleanup_secret_on_failure("test-connection")
            
            # Should return True because cleanup is successful (secret doesn't exist)
            self.assertTrue(result)
    
    def test_cleanup_secret_on_failure_error(self):
        """Test secret cleanup fails with other error."""
        # Mock AWS error response
        error_response = {
            'Error': {
                'Code': 'InternalServiceError',
                'Message': 'An error occurred on the server side.'
            }
        }
        
        # Mock ClientError in the handler
        with patch('glue_job.config.secrets_manager_handler.ClientError', MockClientError):
            client_error = MockClientError(error_response, 'DeleteSecret')
            self.mock_client.delete_secret.side_effect = client_error
            
            result = self.handler.cleanup_secret_on_failure("test-connection")
            
            # Should return False because cleanup failed
            self.assertFalse(result)
    
    def test_get_secret_arn_for_connection(self):
        """Test getting secret ARN format for connection."""
        connection_name = "test-connection"
        
        arn = self.handler.get_secret_arn_for_connection(connection_name)
        
        expected_arn = "arn:aws:secretsmanager:us-east-1:123456789012:secret:/aws-glue/test-connection"
        self.assertEqual(arn, expected_arn)


if __name__ == '__main__':
    unittest.main(verbosity=2)


class TestMigrationPerformanceConfig(unittest.TestCase):
    """Test MigrationPerformanceConfig dataclass."""
    
    def test_migration_performance_config_default(self):
        """Test default MigrationPerformanceConfig creation."""
        from glue_job.config.job_config import MigrationPerformanceConfig
        
        config = MigrationPerformanceConfig()
        
        # Test counting strategy defaults
        self.assertEqual(config.counting_strategy, "auto")
        self.assertEqual(config.size_threshold_rows, 1_000_000)
        self.assertFalse(config.force_immediate_counting)
        self.assertFalse(config.force_deferred_counting)
        
        # Test progress tracking defaults
        self.assertEqual(config.progress_update_interval_seconds, 60)
        self.assertEqual(config.progress_batch_size_rows, 100_000)
        self.assertTrue(config.enable_progress_tracking)
        self.assertTrue(config.enable_progress_logging)
        
        # Test metrics defaults
        self.assertTrue(config.enable_detailed_metrics)
        self.assertEqual(config.metrics_namespace, "AWS/Glue/DataReplication")
    
    def test_migration_performance_config_custom_values(self):
        """Test MigrationPerformanceConfig with custom values."""
        from glue_job.config.job_config import MigrationPerformanceConfig
        
        config = MigrationPerformanceConfig(
            counting_strategy="deferred",
            size_threshold_rows=500_000,
            force_deferred_counting=True,
            progress_update_interval_seconds=30,
            progress_batch_size_rows=50_000,
            enable_progress_tracking=False,
            enable_detailed_metrics=False,
            metrics_namespace="Custom/Namespace"
        )
        
        self.assertEqual(config.counting_strategy, "deferred")
        self.assertEqual(config.size_threshold_rows, 500_000)
        self.assertTrue(config.force_deferred_counting)
        self.assertEqual(config.progress_update_interval_seconds, 30)
        self.assertEqual(config.progress_batch_size_rows, 50_000)
        self.assertFalse(config.enable_progress_tracking)
        self.assertFalse(config.enable_detailed_metrics)
        self.assertEqual(config.metrics_namespace, "Custom/Namespace")
    
    def test_migration_performance_config_validation_success(self):
        """Test successful validation of MigrationPerformanceConfig."""
        from glue_job.config.job_config import MigrationPerformanceConfig
        
        # Test with valid "auto" strategy
        config = MigrationPerformanceConfig(counting_strategy="auto")
        config.validate()  # Should not raise
        
        # Test with valid "immediate" strategy
        config = MigrationPerformanceConfig(counting_strategy="immediate")
        config.validate()  # Should not raise
        
        # Test with valid "deferred" strategy
        config = MigrationPerformanceConfig(counting_strategy="deferred")
        config.validate()  # Should not raise
    
    def test_migration_performance_config_validation_invalid_strategy(self):
        """Test validation fails for invalid counting strategy."""
        from glue_job.config.job_config import MigrationPerformanceConfig
        
        config = MigrationPerformanceConfig(counting_strategy="invalid")
        
        with self.assertRaises(ValueError) as context:
            config.validate()
        self.assertIn("Invalid counting_strategy", str(context.exception))
        self.assertIn("invalid", str(context.exception))
    
    def test_migration_performance_config_validation_mutually_exclusive_force_flags(self):
        """Test validation fails for mutually exclusive force flags."""
        from glue_job.config.job_config import MigrationPerformanceConfig
        
        config = MigrationPerformanceConfig(
            force_immediate_counting=True,
            force_deferred_counting=True
        )
        
        with self.assertRaises(ValueError) as context:
            config.validate()
        self.assertIn("Cannot force both immediate and deferred", str(context.exception))
    
    def test_migration_performance_config_validation_negative_threshold(self):
        """Test validation fails for negative size threshold."""
        from glue_job.config.job_config import MigrationPerformanceConfig
        
        config = MigrationPerformanceConfig(size_threshold_rows=-1000)
        
        with self.assertRaises(ValueError) as context:
            config.validate()
        self.assertIn("size_threshold_rows must be positive", str(context.exception))
    
    def test_migration_performance_config_validation_zero_threshold(self):
        """Test validation fails for zero size threshold."""
        from glue_job.config.job_config import MigrationPerformanceConfig
        
        config = MigrationPerformanceConfig(size_threshold_rows=0)
        
        with self.assertRaises(ValueError) as context:
            config.validate()
        self.assertIn("size_threshold_rows must be positive", str(context.exception))
    
    def test_migration_performance_config_validation_negative_update_interval(self):
        """Test validation fails for negative update interval."""
        from glue_job.config.job_config import MigrationPerformanceConfig
        
        config = MigrationPerformanceConfig(progress_update_interval_seconds=-30)
        
        with self.assertRaises(ValueError) as context:
            config.validate()
        self.assertIn("progress_update_interval_seconds must be positive", str(context.exception))
    
    def test_migration_performance_config_validation_zero_update_interval(self):
        """Test validation fails for zero update interval."""
        from glue_job.config.job_config import MigrationPerformanceConfig
        
        config = MigrationPerformanceConfig(progress_update_interval_seconds=0)
        
        with self.assertRaises(ValueError) as context:
            config.validate()
        self.assertIn("progress_update_interval_seconds must be positive", str(context.exception))
    
    def test_migration_performance_config_validation_negative_batch_size(self):
        """Test validation fails for negative batch size."""
        from glue_job.config.job_config import MigrationPerformanceConfig
        
        config = MigrationPerformanceConfig(progress_batch_size_rows=-50000)
        
        with self.assertRaises(ValueError) as context:
            config.validate()
        self.assertIn("progress_batch_size_rows must be positive", str(context.exception))
    
    def test_migration_performance_config_validation_zero_batch_size(self):
        """Test validation fails for zero batch size."""
        from glue_job.config.job_config import MigrationPerformanceConfig
        
        config = MigrationPerformanceConfig(progress_batch_size_rows=0)
        
        with self.assertRaises(ValueError) as context:
            config.validate()
        self.assertIn("progress_batch_size_rows must be positive", str(context.exception))
    
    def test_migration_performance_config_validation_empty_namespace(self):
        """Test validation fails for empty metrics namespace."""
        from glue_job.config.job_config import MigrationPerformanceConfig
        
        config = MigrationPerformanceConfig(metrics_namespace="")
        
        with self.assertRaises(ValueError) as context:
            config.validate()
        self.assertIn("metrics_namespace cannot be empty", str(context.exception))
    
    def test_migration_performance_config_validation_whitespace_namespace(self):
        """Test validation fails for whitespace-only metrics namespace."""
        from glue_job.config.job_config import MigrationPerformanceConfig
        
        config = MigrationPerformanceConfig(metrics_namespace="   ")
        
        with self.assertRaises(ValueError) as context:
            config.validate()
        self.assertIn("metrics_namespace cannot be empty", str(context.exception))
    
    @patch('glue_job.config.job_config.logging.getLogger')
    def test_migration_performance_config_validation_warning_for_metrics_without_tracking(self, mock_logger):
        """Test validation logs warning when metrics enabled but tracking disabled."""
        from glue_job.config.job_config import MigrationPerformanceConfig
        
        mock_logger_instance = Mock()
        mock_logger.return_value = mock_logger_instance
        
        config = MigrationPerformanceConfig(
            enable_detailed_metrics=True,
            enable_progress_tracking=False
        )
        
        config.validate()
        
        # Check that warning was logged
        mock_logger_instance.warning.assert_called_once()
        warning_call = mock_logger_instance.warning.call_args[0][0]
        self.assertIn("Detailed metrics are enabled but progress tracking is disabled", warning_call)
    
    def test_migration_performance_config_get_counting_strategy_config(self):
        """Test getting CountingStrategyConfig from MigrationPerformanceConfig."""
        from glue_job.config.job_config import MigrationPerformanceConfig
        from glue_job.database.counting_strategy import CountingStrategyType
        
        config = MigrationPerformanceConfig(
            counting_strategy="deferred",
            size_threshold_rows=2_000_000,
            force_deferred_counting=True
        )
        
        counting_config = config.get_counting_strategy_config()
        
        self.assertEqual(counting_config.strategy_type, CountingStrategyType.DEFERRED)
        self.assertEqual(counting_config.size_threshold_rows, 2_000_000)
        self.assertTrue(counting_config.force_deferred)
        self.assertFalse(counting_config.force_immediate)
    
    def test_migration_performance_config_get_counting_strategy_config_auto(self):
        """Test getting CountingStrategyConfig with auto strategy."""
        from glue_job.config.job_config import MigrationPerformanceConfig
        from glue_job.database.counting_strategy import CountingStrategyType
        
        config = MigrationPerformanceConfig(counting_strategy="auto")
        
        counting_config = config.get_counting_strategy_config()
        
        self.assertEqual(counting_config.strategy_type, CountingStrategyType.AUTO)
    
    def test_migration_performance_config_get_counting_strategy_config_immediate(self):
        """Test getting CountingStrategyConfig with immediate strategy."""
        from glue_job.config.job_config import MigrationPerformanceConfig
        from glue_job.database.counting_strategy import CountingStrategyType
        
        config = MigrationPerformanceConfig(
            counting_strategy="immediate",
            force_immediate_counting=True
        )
        
        counting_config = config.get_counting_strategy_config()
        
        self.assertEqual(counting_config.strategy_type, CountingStrategyType.IMMEDIATE)
        self.assertTrue(counting_config.force_immediate)
    
    def test_migration_performance_config_get_streaming_progress_config(self):
        """Test getting StreamingProgressConfig from MigrationPerformanceConfig."""
        from glue_job.config.job_config import MigrationPerformanceConfig
        
        config = MigrationPerformanceConfig(
            progress_update_interval_seconds=45,
            progress_batch_size_rows=75_000,
            enable_detailed_metrics=False,
            enable_progress_logging=False
        )
        
        progress_config = config.get_streaming_progress_config()
        
        self.assertEqual(progress_config.update_interval_seconds, 45)
        self.assertEqual(progress_config.batch_size_rows, 75_000)
        self.assertFalse(progress_config.enable_metrics)
        self.assertFalse(progress_config.enable_logging)


class TestJobConfigWithMigrationPerformance(unittest.TestCase):
    """Test JobConfig with MigrationPerformanceConfig integration."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.source_config = ConnectionConfig(
            engine_type="postgresql",
            connection_string="jdbc:postgresql://source.example.com:5432/sourcedb",
            database="sourcedb",
            schema="public",
            username="sourceuser",
            password="sourcepass",
            jdbc_driver_path="s3://bucket/drivers/postgresql.jar"
        )
        
        self.target_config = ConnectionConfig(
            engine_type="postgresql",
            connection_string="jdbc:postgresql://target.example.com:5432/targetdb",
            database="targetdb",
            schema="public",
            username="targetuser",
            password="targetpass",
            jdbc_driver_path="s3://bucket/drivers/postgresql.jar"
        )
    
    def test_job_config_with_default_migration_performance_config(self):
        """Test JobConfig with default MigrationPerformanceConfig."""
        from glue_job.config.job_config import JobConfig
        
        job_config = JobConfig(
            job_name="test-job",
            source_connection=self.source_config,
            target_connection=self.target_config,
            tables=["test_table"]
        )
        
        # Should have default MigrationPerformanceConfig
        self.assertIsNotNone(job_config.migration_performance_config)
        self.assertEqual(job_config.migration_performance_config.counting_strategy, "auto")
        self.assertEqual(job_config.migration_performance_config.size_threshold_rows, 1_000_000)
    
    def test_job_config_with_custom_migration_performance_config(self):
        """Test JobConfig with custom MigrationPerformanceConfig."""
        from glue_job.config.job_config import JobConfig, MigrationPerformanceConfig
        
        perf_config = MigrationPerformanceConfig(
            counting_strategy="deferred",
            size_threshold_rows=500_000,
            progress_update_interval_seconds=30
        )
        
        job_config = JobConfig(
            job_name="test-job",
            source_connection=self.source_config,
            target_connection=self.target_config,
            tables=["test_table"],
            migration_performance_config=perf_config
        )
        
        self.assertEqual(job_config.migration_performance_config, perf_config)
        self.assertEqual(job_config.migration_performance_config.counting_strategy, "deferred")
        self.assertEqual(job_config.migration_performance_config.size_threshold_rows, 500_000)
    
    def test_job_config_validates_migration_performance_config(self):
        """Test JobConfig validates MigrationPerformanceConfig during initialization."""
        from glue_job.config.job_config import JobConfig, MigrationPerformanceConfig
        
        # Create invalid performance config
        perf_config = MigrationPerformanceConfig(
            counting_strategy="invalid_strategy"
        )
        
        # JobConfig should validate and raise error
        with self.assertRaises(ValueError) as context:
            job_config = JobConfig(
                job_name="test-job",
                source_connection=self.source_config,
                target_connection=self.target_config,
                tables=["test_table"],
                migration_performance_config=perf_config
            )
        self.assertIn("Invalid counting_strategy", str(context.exception))
    
    def test_job_config_get_performance_summary(self):
        """Test JobConfig.get_performance_summary() method."""
        from glue_job.config.job_config import JobConfig, MigrationPerformanceConfig
        
        perf_config = MigrationPerformanceConfig(
            counting_strategy="deferred",
            size_threshold_rows=2_000_000,
            enable_progress_tracking=True,
            progress_update_interval_seconds=45,
            enable_detailed_metrics=True,
            metrics_namespace="Custom/Namespace"
        )
        
        job_config = JobConfig(
            job_name="test-job",
            source_connection=self.source_config,
            target_connection=self.target_config,
            tables=["test_table"],
            migration_performance_config=perf_config
        )
        
        summary = job_config.get_performance_summary()
        
        self.assertEqual(summary['counting_strategy'], "deferred")
        self.assertEqual(summary['size_threshold_rows'], 2_000_000)
        self.assertTrue(summary['progress_tracking_enabled'])
        self.assertEqual(summary['progress_update_interval'], 45)
        self.assertTrue(summary['detailed_metrics_enabled'])
        self.assertEqual(summary['metrics_namespace'], "Custom/Namespace")
