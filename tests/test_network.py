#!/usr/bin/env python3
"""
Unit tests for network modules.

This test suite covers:
- Network error handling and custom exceptions
- Connection retry logic and error recovery
- Error classification and recovery strategies
"""

import unittest
from unittest.mock import Mock, MagicMock, patch
import sys
import os
import time
from botocore.exceptions import ClientError

# Custom mock class that prevents async behavior
class SyncMock(Mock):
    def __init__(self, *args, **kwargs):
        # Explicitly prevent async behavior
        kwargs['spec'] = object
        super().__init__(*args, **kwargs)
    
    def __await__(self):
        # Prevent this mock from being awaitable
        raise TypeError("SyncMock object is not awaitable")
    
    async def __aenter__(self):
        # Prevent async context manager behavior
        raise TypeError("SyncMock object cannot be used as async context manager")
    
    async def __aexit__(self, *args):
        # Prevent async context manager behavior
        raise TypeError("SyncMock object cannot be used as async context manager")

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

# Import the classes to test from new modular structure
from glue_job.network import (
    ErrorCategory, NetworkConnectivityError, GlueConnectionError,
    VpcEndpointError, ENICreationError, NetworkErrorHandler,
    ErrorClassifier, ConnectionRetryHandler, ErrorRecoveryManager
)


class TestNetworkExceptions(unittest.TestCase):
    """Test custom network exception classes."""
    
    def test_network_connectivity_error(self):
        """Test NetworkConnectivityError exception."""
        error = NetworkConnectivityError("Connection failed", "connection_timeout", "test-connection")
        
        self.assertEqual(str(error), "Connection failed")
        self.assertEqual(error.error_type, "connection_timeout")
        self.assertEqual(error.connection_name, "test-connection")
    
    def test_glue_connection_error(self):
        """Test GlueConnectionError exception."""
        error = GlueConnectionError("Glue connection failed", "test-connection", {"error_code": "GLUE_001"})
        
        self.assertEqual(str(error), "Glue connection failed")
        self.assertEqual(error.connection_name, "test-connection")
        self.assertEqual(error.error_details["error_code"], "GLUE_001")
    
    def test_vpc_endpoint_error(self):
        """Test VpcEndpointError exception."""
        error = VpcEndpointError("VPC endpoint not accessible", "vpc-12345", "s3")
        
        self.assertEqual(str(error), "VPC endpoint not accessible")
        self.assertEqual(error.vpc_id, "vpc-12345")
        self.assertEqual(error.endpoint_type, "s3")
    
    def test_eni_creation_error(self):
        """Test ENICreationError exception."""
        error = ENICreationError("ENI creation failed", "subnet-12345", "ENI_001")
        
        self.assertEqual(str(error), "ENI creation failed")
        self.assertEqual(error.subnet_id, "subnet-12345")
        self.assertEqual(error.error_code, "ENI_001")


class TestErrorClassifier(unittest.TestCase):
    """Test ErrorClassifier functionality."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.classifier = ErrorClassifier()
    
    def test_classify_network_timeout_error(self):
        """Test classifying network timeout errors."""
        error = Exception("connection timed out")
        
        category = self.classifier.classify_error(error)
        
        self.assertEqual(category, ErrorCategory.CONNECTION)
    
    def test_classify_connection_refused_error(self):
        """Test classifying connection refused errors."""
        error = Exception("connection refused")
        
        category = self.classifier.classify_error(error)
        
        self.assertEqual(category, ErrorCategory.CONNECTION)
    
    def test_classify_glue_error(self):
        """Test classifying Glue-specific errors."""
        error = Exception("Glue job failed")
        
        category = self.classifier.classify_error(error)
        
        self.assertEqual(category, ErrorCategory.UNKNOWN)
    
    def test_classify_vpc_error(self):
        """Test classifying VPC-related errors."""
        error = Exception("network unreachable")
        
        category = self.classifier.classify_error(error)
        
        # "network unreachable" is classified as CONNECTION error in production code
        self.assertEqual(category, ErrorCategory.CONNECTION)
    
    def test_classify_unknown_error(self):
        """Test classifying unknown errors."""
        error = Exception("Some unknown error")
        
        category = self.classifier.classify_error(error)
        
        self.assertEqual(category, ErrorCategory.UNKNOWN)
    
    def test_is_retryable_network_error(self):
        """Test identifying retryable network errors."""
        error = Exception("Connection timed out")
        
        is_retryable = self.classifier.is_retryable_error(error)
        
        self.assertTrue(is_retryable)
    
    def test_is_not_retryable_authentication_error(self):
        """Test identifying non-retryable authentication errors."""
        error = Exception("Authentication failed")
        
        is_retryable = self.classifier.is_retryable_error(error)
        
        self.assertFalse(is_retryable)


class TestConnectionRetryHandler(unittest.TestCase):
    """Test ConnectionRetryHandler functionality."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.retry_handler = ConnectionRetryHandler(
            max_retries=3,
            base_delay=1.0,
            max_delay=10.0
        )
    
    def test_successful_operation_no_retry(self):
        """Test successful operation without retries."""
        # Mock successful operation
        mock_operation = SyncMock(return_value="success")
        
        # Test operation execution
        result = self.retry_handler.execute_with_retry(mock_operation, "test_operation")
        
        self.assertEqual(result, "success")
        self.assertEqual(mock_operation.call_count, 1)
    
    def test_operation_with_retries(self):
        """Test operation that succeeds after retries."""
        # Mock operation that fails twice then succeeds
        mock_operation = SyncMock(side_effect=[
            Exception("connection timed out"),
            Exception("connection timed out"),
            "success"
        ])
        
        # Test operation execution with retries
        result = self.retry_handler.execute_with_retry(mock_operation, "test_operation")
        
        self.assertEqual(result, "success")
        self.assertEqual(mock_operation.call_count, 3)
    
    def test_operation_exceeds_max_retries(self):
        """Test operation that exceeds maximum retries."""
        # Mock operation that always fails
        mock_operation = SyncMock(side_effect=Exception("connection timed out"))
        
        # Test operation execution that should fail
        with self.assertRaises(Exception):
            self.retry_handler.execute_with_retry(mock_operation, "test_operation")
        
        # Should have tried max_retries + 1 times (initial + retries)
        self.assertEqual(mock_operation.call_count, 4)
    
    def test_non_retryable_error(self):
        """Test handling of non-retryable errors."""
        # Mock operation with non-retryable error
        mock_operation = SyncMock(side_effect=Exception("authentication failed"))
        
        # Test operation execution that should fail immediately
        with self.assertRaises(Exception):
            self.retry_handler.execute_with_retry(mock_operation, "test_operation")
        
        # Should only try once for non-retryable errors
        self.assertEqual(mock_operation.call_count, 1)
    
    @patch('time.sleep')
    def test_exponential_backoff_delay(self, mock_sleep):
        """Test exponential backoff delay calculation."""
        # Mock operation that fails then succeeds
        mock_operation = SyncMock(side_effect=[
            Exception("connection timed out"),
            "success"
        ])
        
        # Test operation execution
        result = self.retry_handler.execute_with_retry(mock_operation, "test_operation")
        
        self.assertEqual(result, "success")
        # Should have slept once (delay may vary due to jitter)
        mock_sleep.assert_called_once()
        # Verify the delay is reasonable (base delay with possible jitter)
        call_args = mock_sleep.call_args[0][0]
        self.assertGreater(call_args, 0.5)  # At least half the base delay
        self.assertLess(call_args, 2.0)     # At most double the base delay


class TestNetworkErrorHandler(unittest.TestCase):
    """Test NetworkErrorHandler functionality."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.error_handler = NetworkErrorHandler()
    
    def test_handle_network_connectivity_error(self):
        """Test handling network connectivity errors."""
        error = NetworkConnectivityError("Connection failed", "connection_timeout", "test-connection")
        
        # Test error diagnostics
        diagnostics = self.error_handler.diagnose_glue_connection_failure("test-connection", error)
        
        self.assertIsInstance(diagnostics, dict)
        self.assertEqual(diagnostics["connection_name"], "test-connection")
    
    def test_handle_generic_exception(self):
        """Test handling generic exceptions."""
        error = Exception("Generic error")
        
        # Test error diagnostics
        diagnostics = self.error_handler.diagnose_glue_connection_failure("test-connection", error)
        
        # Should return diagnostics dict
        self.assertIsInstance(diagnostics, dict)
        self.assertEqual(diagnostics["error_message"], "Generic error")
    
    def test_get_error_context(self):
        """Test getting error context information."""
        error = NetworkConnectivityError("Connection failed", "connection_timeout", "test-connection")
        
        diagnostics = self.error_handler.diagnose_glue_connection_failure("test-connection", error)
        
        self.assertIsInstance(diagnostics, dict)
        self.assertIn("error_type", diagnostics)
        self.assertIn("error_message", diagnostics)
        self.assertIn("connection_name", diagnostics)


class TestErrorRecoveryManager(unittest.TestCase):
    """Test ErrorRecoveryManager functionality."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.recovery_manager = ErrorRecoveryManager("test-job")
    
    def test_suggest_recovery_for_network_error(self):
        """Test suggesting recovery for network errors."""
        error = NetworkConnectivityError("Connection failed", "connection_timeout", "test-connection")
        
        # Mock connection config
        mock_config = SyncMock()
        mock_config.engine_type = "postgresql"
        mock_config.database = "test_db"
        mock_config.schema = "public"
        mock_config.connection_string = "postgresql://user:pass@host:5432/db"
        
        error_info = self.recovery_manager.handle_database_connection_error(error, mock_config, "test_operation")
        
        self.assertIsInstance(error_info, dict)
        self.assertEqual(error_info["error_message"], "Connection failed")
        # Should contain network-specific recovery information
        self.assertIn("recovery_strategy", error_info)
    
    def test_suggest_recovery_for_vpc_error(self):
        """Test suggesting recovery for VPC errors."""
        error = VpcEndpointError("VPC endpoint not accessible", "vpc-12345", "s3")
        
        # Mock connection config
        mock_config = SyncMock()
        mock_config.engine_type = "postgresql"
        mock_config.database = "test_db"
        mock_config.schema = "public"
        mock_config.connection_string = "postgresql://user:pass@host:5432/db"
        
        error_info = self.recovery_manager.handle_database_connection_error(error, mock_config, "test_operation")
        
        self.assertIsInstance(error_info, dict)
        self.assertEqual(error_info["error_message"], "VPC endpoint not accessible")
        # Should contain recovery strategy information
        self.assertIn("recovery_strategy", error_info)
    
    def test_suggest_recovery_for_glue_error(self):
        """Test suggesting recovery for Glue errors."""
        error = GlueConnectionError("Glue connection failed", "test-connection", {"error_code": "GLUE_001"})
        
        # Mock connection config
        mock_config = SyncMock()
        mock_config.engine_type = "postgresql"
        mock_config.database = "test_db"
        mock_config.schema = "public"
        mock_config.connection_string = "postgresql://user:pass@host:5432/db"
        
        error_info = self.recovery_manager.handle_database_connection_error(error, mock_config, "test_operation")
        
        self.assertIsInstance(error_info, dict)
        self.assertEqual(error_info["error_message"], "Glue connection failed")
        # Should contain recovery strategy information
        self.assertIn("recovery_strategy", error_info)
    
    def test_suggest_recovery_for_unknown_error(self):
        """Test suggesting recovery for unknown errors."""
        error = Exception("Unknown error")
        
        # Mock connection config
        mock_config = SyncMock()
        mock_config.engine_type = "postgresql"
        mock_config.database = "test_db"
        mock_config.schema = "public"
        mock_config.connection_string = "postgresql://user:pass@host:5432/db"
        
        error_info = self.recovery_manager.handle_database_connection_error(error, mock_config, "test_operation")
        
        self.assertIsInstance(error_info, dict)
        self.assertEqual(error_info["error_message"], "Unknown error")
        # Should contain recovery strategy information
        self.assertIn("recovery_strategy", error_info)
        
        # Check that recovery strategy is provided
        self.assertIn("recovery_strategy", error_info)
        recovery_strategy = error_info["recovery_strategy"]
        self.assertIsInstance(recovery_strategy, str)
        self.assertGreater(len(recovery_strategy), 0)


if __name__ == '__main__':
    unittest.main(verbosity=2)