"""
Tests for Glue Connection error handling and validation.

This module tests the comprehensive error handling, retry logic,
and parameter validation for Glue Connection operations.
"""

import unittest
from unittest.mock import Mock, patch, MagicMock
import pytest
from botocore.exceptions import ClientError

# Import the modules we're testing
from src.glue_job.network.glue_connection_errors import (
    GlueConnectionBaseError,
    GlueConnectionCreationError,
    GlueConnectionNotFoundError,
    GlueConnectionValidationError,
    GlueConnectionParameterError,
    GlueConnectionPermissionError,
    GlueConnectionNetworkError,
    GlueConnectionRetryableError,
    GlueConnectionEngineCompatibilityError
)

from src.glue_job.network.glue_connection_retry_handler import (
    GlueConnectionErrorClassifier,
    GlueConnectionRetryHandler
)

from src.glue_job.config.glue_connection_validator import (
    GlueConnectionParameterValidator,
    validate_glue_connection_parameters_comprehensive
)

from src.glue_job.monitoring.glue_connection_logger import GlueConnectionOperationLogger


class TestGlueConnectionErrors(unittest.TestCase):
    """Test Glue Connection error classes."""
    
    def test_glue_connection_base_error(self):
        """Test base Glue Connection error."""
        error = GlueConnectionBaseError(
            "Test error message",
            connection_name="test-connection",
            error_code="TEST_ERROR",
            context={"key": "value"}
        )
        
        self.assertEqual(str(error), "[TEST_ERROR] Test error message")
        self.assertEqual(error.connection_name, "test-connection")
        self.assertEqual(error.error_code, "TEST_ERROR")
        self.assertEqual(error.context["key"], "value")
    
    def test_glue_connection_creation_error(self):
        """Test Glue Connection creation error."""
        error = GlueConnectionCreationError(
            "Creation failed",
            connection_name="test-connection",
            aws_error_code="InvalidInputException",
            aws_error_message="Invalid input provided"
        )
        
        self.assertIn("Failed to create Glue Connection 'test-connection'", str(error))
        self.assertIn("Invalid input provided", str(error))
        self.assertEqual(error.aws_error_code, "InvalidInputException")
    
    def test_glue_connection_not_found_error(self):
        """Test Glue Connection not found error."""
        error = GlueConnectionNotFoundError("test-connection")
        
        self.assertIn("does not exist", str(error))
        self.assertEqual(error.connection_name, "test-connection")
        self.assertEqual(error.error_code, "CONNECTION_NOT_FOUND")
    
    def test_glue_connection_parameter_error(self):
        """Test Glue Connection parameter error."""
        error = GlueConnectionParameterError(
            "Invalid parameter combination",
            parameter_name="createSourceConnection",
            conflicting_parameter="useSourceConnection"
        )
        
        self.assertIn("parameter error", str(error))
        self.assertIn("createSourceConnection", str(error))
        self.assertEqual(error.parameter_name, "createSourceConnection")
        self.assertEqual(error.conflicting_parameter, "useSourceConnection")


class TestGlueConnectionErrorClassifier(unittest.TestCase):
    """Test Glue Connection error classifier."""
    
    def setUp(self):
        self.classifier = GlueConnectionErrorClassifier()
    
    def test_classify_retryable_error(self):
        """Test classification of retryable errors."""
        error = GlueConnectionRetryableError(
            "Temporary failure",
            retry_after_seconds=5,
            max_retries=3
        )
        
        classification = self.classifier.classify_glue_connection_error(error)
        
        self.assertTrue(classification['is_retryable'])
        self.assertEqual(classification['retry_strategy'], 'exponential_backoff')
        self.assertEqual(classification['retry_delay'], 5.0)
        self.assertEqual(classification['max_retries'], 3)
    
    def test_classify_aws_throttling_error(self):
        """Test classification of AWS throttling errors."""
        client_error = ClientError(
            error_response={
                'Error': {
                    'Code': 'Throttling',
                    'Message': 'Rate exceeded'
                }
            },
            operation_name='CreateConnection'
        )
        
        classification = self.classifier.classify_glue_connection_error(client_error)
        
        self.assertTrue(classification['is_retryable'])
        self.assertEqual(classification['retry_strategy'], 'exponential_backoff')
        self.assertEqual(classification['retry_delay'], 2.0)
        self.assertEqual(classification['max_retries'], 5)
    
    def test_classify_non_retryable_error(self):
        """Test classification of non-retryable errors."""
        error = GlueConnectionParameterError("Invalid parameter")
        
        classification = self.classifier.classify_glue_connection_error(error)
        
        self.assertFalse(classification['is_retryable'])
        self.assertEqual(classification['retry_strategy'], 'fail_immediately')


class TestGlueConnectionRetryHandler(unittest.TestCase):
    """Test Glue Connection retry handler."""
    
    def setUp(self):
        self.retry_handler = GlueConnectionRetryHandler(
            max_retries=2,
            base_delay=0.1,  # Short delay for testing
            max_delay=1.0
        )
    
    def test_successful_operation_no_retry(self):
        """Test successful operation without retry."""
        mock_operation = Mock(return_value="success")
        
        result = self.retry_handler.execute_with_retry(
            mock_operation,
            "test_operation",
            connection_name="test-connection"
        )
        
        self.assertEqual(result, "success")
        self.assertEqual(mock_operation.call_count, 1)
    
    def test_operation_succeeds_after_retry(self):
        """Test operation that succeeds after retry."""
        mock_operation = Mock(side_effect=[
            GlueConnectionRetryableError("Temporary failure"),
            "success"
        ])
        
        result = self.retry_handler.execute_with_retry(
            mock_operation,
            "test_operation",
            connection_name="test-connection"
        )
        
        self.assertEqual(result, "success")
        self.assertEqual(mock_operation.call_count, 2)
    
    def test_non_retryable_error_fails_immediately(self):
        """Test that non-retryable errors fail immediately."""
        mock_operation = Mock(side_effect=GlueConnectionParameterError("Invalid parameter"))
        
        with self.assertRaises(GlueConnectionParameterError):
            self.retry_handler.execute_with_retry(
                mock_operation,
                "test_operation",
                connection_name="test-connection"
            )
        
        self.assertEqual(mock_operation.call_count, 1)
    
    @patch('time.sleep')  # Mock sleep to speed up test
    def test_retry_exhausted(self, mock_sleep):
        """Test behavior when all retries are exhausted."""
        mock_operation = Mock(side_effect=GlueConnectionRetryableError("Persistent failure"))
        
        with self.assertRaises(GlueConnectionRetryableError):
            self.retry_handler.execute_with_retry(
                mock_operation,
                "test_operation",
                connection_name="test-connection"
            )
        
        # Should be called max_retries + 1 times (initial + retries)
        self.assertEqual(mock_operation.call_count, 3)


class TestGlueConnectionParameterValidator(unittest.TestCase):
    """Test Glue Connection parameter validator."""
    
    def setUp(self):
        self.validator = GlueConnectionParameterValidator()
    
    def test_valid_create_source_connection(self):
        """Test validation of valid createSourceConnection parameters."""
        args = {
            'createSourceConnection': 'true',
            'SOURCE_ENGINE_TYPE': 'oracle',
            'SOURCE_CONNECTION_STRING': 'jdbc:oracle:thin:@host:1521:db',
            'SOURCE_DB_USER': 'user',
            'SOURCE_DB_PASSWORD': 'password'
        }
        
        # Should not raise any exceptions
        result = self.validator.validate_all_glue_connection_parameters(args)
        
        self.assertIsNotNone(result['source_config'])
        self.assertEqual(result['source_config']['strategy'], 'create_glue')
        self.assertEqual(len(result['validation_errors']), 0)
    
    def test_valid_use_target_connection(self):
        """Test validation of valid useTargetConnection parameters."""
        args = {
            'useTargetConnection': 'my-existing-connection',
            'TARGET_ENGINE_TYPE': 'postgresql'
        }
        
        # Should not raise any exceptions
        result = self.validator.validate_all_glue_connection_parameters(args)
        
        self.assertIsNotNone(result['target_config'])
        self.assertEqual(result['target_config']['strategy'], 'use_glue')
        self.assertEqual(result['target_config']['connection_name'], 'my-existing-connection')
    
    def test_mutually_exclusive_parameters(self):
        """Test validation fails for mutually exclusive parameters."""
        args = {
            'createSourceConnection': 'true',
            'useSourceConnection': 'existing-connection',
            'SOURCE_ENGINE_TYPE': 'oracle'
        }
        
        with self.assertRaises(GlueConnectionParameterError) as context:
            self.validator.validate_all_glue_connection_parameters(args)
        
        self.assertIn("mutually exclusive", str(context.exception))
    
    def test_iceberg_engine_warning(self):
        """Test that Iceberg engines generate warnings for Glue Connection parameters."""
        args = {
            'createSourceConnection': 'true',
            'SOURCE_ENGINE_TYPE': 'iceberg',
            'SOURCE_CONNECTION_STRING': 'jdbc:oracle:thin:@host:1521:db',
            'SOURCE_DB_USER': 'user',
            'SOURCE_DB_PASSWORD': 'password'
        }
        
        result = self.validator.validate_all_glue_connection_parameters(args)
        
        # Should have warnings but no errors
        self.assertGreater(len(result['validation_warnings']), 0)
        self.assertEqual(len(result['validation_errors']), 0)
        
        # Check that warning mentions Iceberg
        warning_text = ' '.join(result['validation_warnings'])
        self.assertIn('Iceberg', warning_text)
        self.assertIn('ignored', warning_text.lower())
    
    def test_invalid_connection_name_format(self):
        """Test validation fails for invalid connection name format."""
        args = {
            'useSourceConnection': 'invalid connection name!',  # Contains spaces and special chars
            'SOURCE_ENGINE_TYPE': 'oracle'
        }
        
        with self.assertRaises(GlueConnectionParameterError) as context:
            self.validator.validate_all_glue_connection_parameters(args)
        
        self.assertIn("alphanumeric characters", str(context.exception))
    
    def test_missing_required_parameters_for_creation(self):
        """Test validation fails when required parameters are missing for creation."""
        args = {
            'createSourceConnection': 'true',
            'SOURCE_ENGINE_TYPE': 'oracle',
            # Missing SOURCE_CONNECTION_STRING, SOURCE_DB_USER, SOURCE_DB_PASSWORD
        }
        
        with self.assertRaises(GlueConnectionParameterError) as context:
            self.validator.validate_all_glue_connection_parameters(args)
        
        self.assertIn("Missing required parameters", str(context.exception))


class TestGlueConnectionOperationLogger(unittest.TestCase):
    """Test Glue Connection operation logger."""
    
    def setUp(self):
        self.logger = GlueConnectionOperationLogger("TestLogger")
    
    @patch('src.glue_job.monitoring.glue_connection_logger.StructuredLogger')
    def test_log_strategy_decision(self, mock_structured_logger):
        """Test logging of strategy decisions."""
        mock_logger_instance = Mock()
        mock_structured_logger.return_value = mock_logger_instance
        
        logger = GlueConnectionOperationLogger("TestLogger")
        
        context = {
            'engine_type': 'oracle',
            'connection_name': 'test-connection',
            'parameter_source': 'createSourceConnection'
        }
        
        logger.log_strategy_decision('source', 'create_glue', context)
        
        # Verify that structured logger was called
        mock_logger_instance.info.assert_called_once()
        call_args = mock_logger_instance.info.call_args
        
        # Check that the log message contains expected information
        self.assertIn('Create new Glue Connection', call_args[0][0])
    
    @patch('src.glue_job.monitoring.glue_connection_logger.StructuredLogger')
    def test_log_connection_creation_success(self, mock_structured_logger):
        """Test logging of successful connection creation."""
        mock_logger_instance = Mock()
        mock_structured_logger.return_value = mock_logger_instance
        
        logger = GlueConnectionOperationLogger("TestLogger")
        
        logger.log_connection_creation_success(
            "test-connection",
            duration_seconds=2.5,
            retry_attempts=1
        )
        
        # Verify that structured logger was called
        mock_logger_instance.info.assert_called_once()
        call_args = mock_logger_instance.info.call_args
        
        # Check that the log contains expected information
        self.assertIn('created successfully', call_args[0][0])


class TestComprehensiveValidationFunction(unittest.TestCase):
    """Test the comprehensive validation function."""
    
    def test_comprehensive_validation_success(self):
        """Test comprehensive validation with valid parameters."""
        args = {
            'createSourceConnection': 'true',
            'useTargetConnection': 'existing-target-connection',
            'SOURCE_ENGINE_TYPE': 'oracle',
            'TARGET_ENGINE_TYPE': 'postgresql',
            'SOURCE_CONNECTION_STRING': 'jdbc:oracle:thin:@host:1521:db',
            'SOURCE_DB_USER': 'user',
            'SOURCE_DB_PASSWORD': 'password'
        }
        
        result = validate_glue_connection_parameters_comprehensive(args)
        
        # Should have both source and target configurations
        self.assertIsNotNone(result['source_config'])
        self.assertIsNotNone(result['target_config'])
        
        # Check strategies
        self.assertEqual(result['source_config']['strategy'], 'create_glue')
        self.assertEqual(result['target_config']['strategy'], 'use_glue')
        
        # Should have no errors
        self.assertEqual(len(result['validation_errors']), 0)
    
    def test_comprehensive_validation_with_mixed_engines(self):
        """Test comprehensive validation with mixed engine types."""
        args = {
            'createSourceConnection': 'true',
            'SOURCE_ENGINE_TYPE': 'iceberg',  # Should generate warning
            'TARGET_ENGINE_TYPE': 'postgresql',
            'SOURCE_CONNECTION_STRING': 'jdbc:oracle:thin:@host:1521:db',
            'SOURCE_DB_USER': 'user',
            'SOURCE_DB_PASSWORD': 'password'
        }
        
        result = validate_glue_connection_parameters_comprehensive(args)
        
        # Should have warnings for Iceberg engine
        self.assertGreater(len(result['validation_warnings']), 0)
        
        # Source config should be None for Iceberg
        self.assertIsNone(result['source_config'])
        
        # Target config should also be None (no Glue Connection params)
        self.assertIsNone(result['target_config'])


if __name__ == '__main__':
    unittest.main()