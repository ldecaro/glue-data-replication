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

# Add src directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'src'))

# Mock PySpark and AWS Glue imports for testing
mock_modules = [
    'awsglue', 'awsglue.utils', 'awsglue.context', 'awsglue.job',
    'pyspark', 'pyspark.context', 'pyspark.sql', 'pyspark.sql.types',
    'pyspark.sql.functions', 'boto3'
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
        error = NetworkConnectivityError("Connection failed", "CONN_001")
        
        self.assertEqual(str(error), "Connection failed")
        self.assertEqual(error.error_code, "CONN_001")
        self.assertEqual(error.category, ErrorCategory.NETWORK)
    
    def test_glue_connection_error(self):
        """Test GlueConnectionError exception."""
        error = GlueConnectionError("Glue connection failed", "GLUE_001")
        
        self.assertEqual(str(error), "Glue connection failed")
        self.assertEqual(error.error_code, "GLUE_001")
        self.assertEqual(error.category, ErrorCategory.GLUE)
    
    def test_vpc_endpoint_error(self):
        """Test VpcEndpointError exception."""
        error = VpcEndpointError("VPC endpoint not accessible", "VPC_001")
        
        self.assertEqual(str(error), "VPC endpoint not accessible")
        self.assertEqual(error.error_code, "VPC_001")
        self.assertEqual(error.category, ErrorCategory.VPC)
    
    def test_eni_creation_error(self):
        """Test ENICreationError exception."""
        error = ENICreationError("ENI creation failed", "ENI_001")
        
        self.assertEqual(str(error), "ENI creation failed")
        self.assertEqual(error.error_code, "ENI_001")
        self.assertEqual(error.category, ErrorCategory.ENI)


class TestErrorClassifier(unittest.TestCase):
    """Test ErrorClassifier functionality."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.classifier = ErrorClassifier()
    
    def test_classify_network_timeout_error(self):
        """Test classifying network timeout errors."""
        error = Exception("Connection timed out")
        
        category = self.classifier.classify_error(error)
        
        self.assertEqual(category, ErrorCategory.NETWORK)
    
    def test_classify_connection_refused_error(self):
        """Test classifying connection refused errors."""
        error = Exception("Connection refused")
        
        category = self.classifier.classify_error(error)
        
        self.assertEqual(category, ErrorCategory.NETWORK)
    
    def test_classify_glue_error(self):
        """Test classifying Glue-specific errors."""
        error = Exception("Glue job failed")
        
        category = self.classifier.classify_error(error)
        
        self.assertEqual(category, ErrorCategory.GLUE)
    
    def test_classify_vpc_error(self):
        """Test classifying VPC-related errors."""
        error = Exception("VPC endpoint unreachable")
        
        category = self.classifier.classify_error(error)
        
        self.assertEqual(category, ErrorCategory.VPC)
    
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
        mock_operation = Mock(return_value="success")
        
        # Test operation execution
        result = self.retry_handler.execute_with_retry(mock_operation)
        
        self.assertEqual(result, "success")
        self.assertEqual(mock_operation.call_count, 1)
    
    def test_operation_with_retries(self):
        """Test operation that succeeds after retries."""
        # Mock operation that fails twice then succeeds
        mock_operation = Mock(side_effect=[
            Exception("Connection failed"),
            Exception("Connection failed"),
            "success"
        ])
        
        # Test operation execution with retries
        result = self.retry_handler.execute_with_retry(mock_operation)
        
        self.assertEqual(result, "success")
        self.assertEqual(mock_operation.call_count, 3)
    
    def test_operation_exceeds_max_retries(self):
        """Test operation that exceeds maximum retries."""
        # Mock operation that always fails
        mock_operation = Mock(side_effect=Exception("Connection failed"))
        
        # Test operation execution that should fail
        with self.assertRaises(Exception):
            self.retry_handler.execute_with_retry(mock_operation)
        
        # Should have tried max_retries + 1 times (initial + retries)
        self.assertEqual(mock_operation.call_count, 4)
    
    def test_non_retryable_error(self):
        """Test handling of non-retryable errors."""
        # Mock operation with non-retryable error
        mock_operation = Mock(side_effect=Exception("Authentication failed"))
        
        # Mock classifier to return non-retryable
        with patch.object(self.retry_handler.error_classifier, 'is_retryable_error', return_value=False):
            with self.assertRaises(Exception):
                self.retry_handler.execute_with_retry(mock_operation)
        
        # Should only try once for non-retryable errors
        self.assertEqual(mock_operation.call_count, 1)
    
    @patch('time.sleep')
    def test_exponential_backoff_delay(self, mock_sleep):
        """Test exponential backoff delay calculation."""
        # Mock operation that fails then succeeds
        mock_operation = Mock(side_effect=[
            Exception("Connection failed"),
            "success"
        ])
        
        # Test operation execution
        result = self.retry_handler.execute_with_retry(mock_operation)
        
        self.assertEqual(result, "success")
        # Should have slept once with base delay
        mock_sleep.assert_called_once_with(1.0)


class TestNetworkErrorHandler(unittest.TestCase):
    """Test NetworkErrorHandler functionality."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.error_handler = NetworkErrorHandler()
    
    def test_handle_network_connectivity_error(self):
        """Test handling network connectivity errors."""
        error = NetworkConnectivityError("Connection failed", "CONN_001")
        
        # Test error handling
        handled_error = self.error_handler.handle_error(error)
        
        self.assertIsInstance(handled_error, NetworkConnectivityError)
        self.assertEqual(handled_error.error_code, "CONN_001")
    
    def test_handle_generic_exception(self):
        """Test handling generic exceptions."""
        error = Exception("Generic error")
        
        # Test error handling
        handled_error = self.error_handler.handle_error(error)
        
        # Should wrap in appropriate network error type
        self.assertIsInstance(handled_error, Exception)
    
    def test_get_error_context(self):
        """Test getting error context information."""
        error = NetworkConnectivityError("Connection failed", "CONN_001")
        
        context = self.error_handler.get_error_context(error)
        
        self.assertIsInstance(context, dict)
        self.assertIn("error_type", context)
        self.assertIn("error_message", context)
        self.assertIn("error_code", context)


class TestErrorRecoveryManager(unittest.TestCase):
    """Test ErrorRecoveryManager functionality."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.recovery_manager = ErrorRecoveryManager()
    
    def test_suggest_recovery_for_network_error(self):
        """Test suggesting recovery for network errors."""
        error = NetworkConnectivityError("Connection failed", "CONN_001")
        
        suggestions = self.recovery_manager.suggest_recovery(error)
        
        self.assertIsInstance(suggestions, list)
        self.assertGreater(len(suggestions), 0)
        # Should contain network-specific recovery suggestions
        self.assertTrue(any("network" in suggestion.lower() for suggestion in suggestions))
    
    def test_suggest_recovery_for_vpc_error(self):
        """Test suggesting recovery for VPC errors."""
        error = VpcEndpointError("VPC endpoint not accessible", "VPC_001")
        
        suggestions = self.recovery_manager.suggest_recovery(error)
        
        self.assertIsInstance(suggestions, list)
        self.assertGreater(len(suggestions), 0)
        # Should contain VPC-specific recovery suggestions
        self.assertTrue(any("vpc" in suggestion.lower() for suggestion in suggestions))
    
    def test_suggest_recovery_for_glue_error(self):
        """Test suggesting recovery for Glue errors."""
        error = GlueConnectionError("Glue connection failed", "GLUE_001")
        
        suggestions = self.recovery_manager.suggest_recovery(error)
        
        self.assertIsInstance(suggestions, list)
        self.assertGreater(len(suggestions), 0)
        # Should contain Glue-specific recovery suggestions
        self.assertTrue(any("glue" in suggestion.lower() for suggestion in suggestions))
    
    def test_suggest_recovery_for_unknown_error(self):
        """Test suggesting recovery for unknown errors."""
        error = Exception("Unknown error")
        
        suggestions = self.recovery_manager.suggest_recovery(error)
        
        self.assertIsInstance(suggestions, list)
        self.assertGreater(len(suggestions), 0)
        # Should contain generic recovery suggestions
        self.assertTrue(any("check" in suggestion.lower() for suggestion in suggestions))


if __name__ == '__main__':
    unittest.main(verbosity=2)