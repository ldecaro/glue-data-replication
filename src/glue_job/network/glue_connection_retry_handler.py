"""
Specialized retry handler for Glue Connection operations.

This module provides intelligent retry logic specifically designed for
Glue Connection operations with appropriate error classification and
recovery strategies.
"""

import time
import logging
from typing import Dict, Any, Optional, Callable
from botocore.exceptions import ClientError

from .glue_connection_errors import (
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
from .retry_handler import ConnectionRetryHandler, ErrorClassifier
from ..monitoring.logging import StructuredLogger
from ..config.secrets_manager_handler import (
    SecretsManagerError,
    SecretCreationError,
    SecretsManagerPermissionError,
    SecretsManagerRetryableError
)

logger = logging.getLogger(__name__)


class GlueConnectionErrorClassifier:
    """Specialized error classifier for Glue Connection and Secrets Manager operations."""
    
    # AWS error codes that are retryable
    RETRYABLE_AWS_ERROR_CODES = {
        'Throttling',
        'ThrottlingException', 
        'ServiceUnavailable',
        'InternalServiceException',
        'TooManyRequestsException',
        'ConcurrentModificationException'
    }
    
    # AWS error codes that are not retryable
    NON_RETRYABLE_AWS_ERROR_CODES = {
        'AlreadyExistsException',
        'EntityNotFoundException',
        'InvalidInputException',
        'AccessDeniedException',
        'UnauthorizedOperation',
        'ResourceNumberLimitExceededException'
    }
    
    # Secrets Manager specific retryable error codes
    SECRETS_MANAGER_RETRYABLE_CODES = {
        'ThrottlingException',
        'ServiceUnavailableException',
        'InternalServiceException',
        'RequestTimeoutException'
    }
    
    # Secrets Manager specific non-retryable error codes
    SECRETS_MANAGER_NON_RETRYABLE_CODES = {
        'ResourceExistsException',
        'ResourceNotFoundException',
        'InvalidParameterException',
        'AccessDeniedException',
        'UnauthorizedOperation',
        'LimitExceededException'
    }
    
    @classmethod
    def classify_glue_connection_error(cls, error: Exception) -> Dict[str, Any]:
        """Classify Glue Connection specific errors.
        
        Args:
            error: Exception to classify
            
        Returns:
            Dict containing error classification information
        """
        classification = {
            'error_type': type(error).__name__,
            'error_message': str(error),
            'is_retryable': False,
            'retry_strategy': 'fail_immediately',
            'retry_delay': 1.0,
            'max_retries': 0,
            'error_category': 'unknown'
        }
        
        # Handle Glue Connection specific errors
        if isinstance(error, GlueConnectionBaseError):
            classification.update(cls._classify_glue_connection_base_error(error))
        
        # Handle Secrets Manager specific errors
        elif isinstance(error, SecretsManagerError):
            classification.update(cls._classify_secrets_manager_error(error))
        
        # Handle AWS ClientError (boto3 errors)
        elif isinstance(error, ClientError):
            classification.update(cls._classify_aws_client_error(error))
        
        # Handle general exceptions
        else:
            classification.update(cls._classify_general_error(error))
        
        return classification
    
    @classmethod
    def _classify_glue_connection_base_error(cls, error: GlueConnectionBaseError) -> Dict[str, Any]:
        """Classify Glue Connection base errors."""
        if isinstance(error, GlueConnectionRetryableError):
            return {
                'is_retryable': True,
                'retry_strategy': 'exponential_backoff',
                'retry_delay': error.retry_after_seconds or 2.0,
                'max_retries': error.max_retries or 3,
                'error_category': 'retryable'
            }
        
        elif isinstance(error, GlueConnectionCreationError):
            # Creation errors may be retryable depending on the cause
            if error.aws_error_code in cls.RETRYABLE_AWS_ERROR_CODES:
                return {
                    'is_retryable': True,
                    'retry_strategy': 'exponential_backoff',
                    'retry_delay': 2.0,
                    'max_retries': 3,
                    'error_category': 'creation_retryable'
                }
            else:
                return {
                    'is_retryable': False,
                    'retry_strategy': 'fail_immediately',
                    'error_category': 'creation_failed'
                }
        
        elif isinstance(error, GlueConnectionNetworkError):
            return {
                'is_retryable': True,
                'retry_strategy': 'exponential_backoff_with_jitter',
                'retry_delay': 5.0,  # Longer delay for network issues
                'max_retries': 2,    # Fewer retries for network issues
                'error_category': 'network'
            }
        
        elif isinstance(error, (GlueConnectionNotFoundError, 
                               GlueConnectionValidationError,
                               GlueConnectionParameterError,
                               GlueConnectionPermissionError,
                               GlueConnectionEngineCompatibilityError)):
            return {
                'is_retryable': False,
                'retry_strategy': 'fail_immediately',
                'error_category': 'configuration'
            }
        
        else:
            return {
                'is_retryable': False,
                'retry_strategy': 'fail_immediately',
                'error_category': 'unknown_glue_connection'
            }
    
    @classmethod
    def _classify_secrets_manager_error(cls, error: SecretsManagerError) -> Dict[str, Any]:
        """Classify Secrets Manager specific errors."""
        if isinstance(error, SecretsManagerRetryableError):
            return {
                'is_retryable': True,
                'retry_strategy': 'exponential_backoff',
                'retry_delay': error.retry_after_seconds or 2.0,
                'max_retries': error.max_retries or 3,
                'error_category': 'secrets_manager_retryable'
            }
        
        elif isinstance(error, SecretCreationError):
            # Check if the underlying AWS error is retryable
            if hasattr(error, 'aws_error_code') and error.aws_error_code in cls.SECRETS_MANAGER_RETRYABLE_CODES:
                return {
                    'is_retryable': True,
                    'retry_strategy': 'exponential_backoff',
                    'retry_delay': 3.0,  # Longer delay for secret creation
                    'max_retries': 3,
                    'error_category': 'secret_creation_retryable'
                }
            else:
                return {
                    'is_retryable': False,
                    'retry_strategy': 'fail_immediately',
                    'error_category': 'secret_creation_failed'
                }
        
        elif isinstance(error, SecretsManagerPermissionError):
            return {
                'is_retryable': False,
                'retry_strategy': 'fail_immediately',
                'error_category': 'secrets_manager_permission'
            }
        
        else:
            # Generic Secrets Manager error - be conservative
            return {
                'is_retryable': False,
                'retry_strategy': 'fail_immediately',
                'error_category': 'secrets_manager_unknown'
            }
    
    @classmethod
    def _classify_aws_client_error(cls, error: ClientError) -> Dict[str, Any]:
        """Classify AWS ClientError exceptions."""
        error_code = error.response.get('Error', {}).get('Code', '')
        error_message = error.response.get('Error', {}).get('Message', '')
        service_name = error.response.get('ResponseMetadata', {}).get('HTTPHeaders', {}).get('x-amzn-service', '')
        
        # Handle Secrets Manager specific errors
        if 'secretsmanager' in service_name.lower() or error_code in cls.SECRETS_MANAGER_RETRYABLE_CODES or error_code in cls.SECRETS_MANAGER_NON_RETRYABLE_CODES:
            return cls._classify_secrets_manager_client_error(error_code, error_message)
        
        # Handle general AWS errors
        if error_code in cls.RETRYABLE_AWS_ERROR_CODES:
            retry_delay = 1.0
            max_retries = 3
            
            # Adjust retry parameters based on specific error codes
            if error_code in ['Throttling', 'ThrottlingException', 'TooManyRequestsException']:
                retry_delay = 2.0
                max_retries = 5
            elif error_code == 'ServiceUnavailable':
                retry_delay = 5.0
                max_retries = 3
            
            return {
                'is_retryable': True,
                'retry_strategy': 'exponential_backoff',
                'retry_delay': retry_delay,
                'max_retries': max_retries,
                'error_category': 'aws_retryable',
                'aws_error_code': error_code
            }
        
        elif error_code in cls.NON_RETRYABLE_AWS_ERROR_CODES:
            return {
                'is_retryable': False,
                'retry_strategy': 'fail_immediately',
                'error_category': 'aws_non_retryable',
                'aws_error_code': error_code
            }
        
        else:
            # Unknown AWS error - be conservative and don't retry
            return {
                'is_retryable': False,
                'retry_strategy': 'fail_immediately',
                'error_category': 'aws_unknown',
                'aws_error_code': error_code
            }
    
    @classmethod
    def _classify_secrets_manager_client_error(cls, error_code: str, error_message: str) -> Dict[str, Any]:
        """Classify Secrets Manager specific ClientError exceptions."""
        if error_code in cls.SECRETS_MANAGER_RETRYABLE_CODES:
            retry_delay = 2.0
            max_retries = 3
            
            # Adjust retry parameters for specific Secrets Manager errors
            if error_code == 'ThrottlingException':
                retry_delay = 3.0
                max_retries = 5
            elif error_code in ['ServiceUnavailableException', 'RequestTimeoutException']:
                retry_delay = 5.0
                max_retries = 3
            
            return {
                'is_retryable': True,
                'retry_strategy': 'exponential_backoff',
                'retry_delay': retry_delay,
                'max_retries': max_retries,
                'error_category': 'secrets_manager_retryable',
                'aws_error_code': error_code
            }
        
        elif error_code in cls.SECRETS_MANAGER_NON_RETRYABLE_CODES:
            return {
                'is_retryable': False,
                'retry_strategy': 'fail_immediately',
                'error_category': 'secrets_manager_non_retryable',
                'aws_error_code': error_code
            }
        
        else:
            # Unknown Secrets Manager error - be conservative
            return {
                'is_retryable': False,
                'retry_strategy': 'fail_immediately',
                'error_category': 'secrets_manager_unknown',
                'aws_error_code': error_code
            }
    
    @classmethod
    def _classify_general_error(cls, error: Exception) -> Dict[str, Any]:
        """Classify general exceptions."""
        error_message = str(error).lower()
        
        # Check for network-related errors
        network_indicators = ['timeout', 'connection', 'network', 'dns', 'socket']
        if any(indicator in error_message for indicator in network_indicators):
            return {
                'is_retryable': True,
                'retry_strategy': 'exponential_backoff',
                'retry_delay': 2.0,
                'max_retries': 3,
                'error_category': 'network_general'
            }
        
        # Check for permission-related errors
        permission_indicators = ['permission', 'access denied', 'unauthorized', 'forbidden']
        if any(indicator in error_message for indicator in permission_indicators):
            return {
                'is_retryable': False,
                'retry_strategy': 'fail_immediately',
                'error_category': 'permission_general'
            }
        
        # Default to non-retryable for unknown errors
        return {
            'is_retryable': False,
            'retry_strategy': 'fail_immediately',
            'error_category': 'unknown_general'
        }


class GlueConnectionRetryHandler:
    """Specialized retry handler for Glue Connection operations."""
    
    def __init__(self, max_retries: int = 3, base_delay: float = 1.0,
                 max_delay: float = 30.0, backoff_factor: float = 2.0):
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.backoff_factor = backoff_factor
        self.structured_logger = StructuredLogger("GlueConnectionRetryHandler")
        self.error_classifier = GlueConnectionErrorClassifier()
    
    def execute_with_retry(self, operation: Callable, operation_name: str,
                          connection_name: Optional[str] = None,
                          *args, **kwargs) -> Any:
        """Execute Glue Connection operation with intelligent retry logic.
        
        Args:
            operation: Function to execute
            operation_name: Name of the operation for logging
            connection_name: Optional Glue Connection name for context
            *args: Arguments to pass to operation
            **kwargs: Keyword arguments to pass to operation
            
        Returns:
            Result of the operation
            
        Raises:
            GlueConnectionBaseError: For Glue Connection specific failures
            Exception: For other failures after retry attempts
        """
        last_exception = None
        operation_context = {
            'operation_name': operation_name,
            'connection_name': connection_name,
            'max_retries': self.max_retries
        }
        
        for attempt in range(self.max_retries + 1):
            try:
                self.structured_logger.info(
                    f"Executing Glue Connection operation: {operation_name}",
                    attempt=attempt + 1,
                    max_attempts=self.max_retries + 1,
                    connection_name=connection_name
                )
                
                result = operation(*args, **kwargs)
                
                if attempt > 0:
                    self.structured_logger.info(
                        f"Glue Connection operation succeeded after retry",
                        operation_name=operation_name,
                        connection_name=connection_name,
                        successful_attempt=attempt + 1,
                        total_attempts=attempt + 1
                    )
                
                return result
                
            except Exception as e:
                last_exception = e
                
                # Classify the error
                error_classification = self.error_classifier.classify_glue_connection_error(e)
                
                self.structured_logger.warning(
                    f"Glue Connection operation failed: {operation_name}",
                    attempt=attempt + 1,
                    error_type=error_classification['error_type'],
                    error_message=error_classification['error_message'],
                    error_category=error_classification['error_category'],
                    is_retryable=error_classification['is_retryable'],
                    connection_name=connection_name
                )
                
                # Check if error is retryable
                if not error_classification['is_retryable']:
                    self.structured_logger.error(
                        f"Glue Connection operation failed with non-retryable error",
                        operation_name=operation_name,
                        connection_name=connection_name,
                        error_category=error_classification['error_category'],
                        error_message=str(e)
                    )
                    
                    # Convert to appropriate Glue Connection error if needed
                    if not isinstance(e, GlueConnectionBaseError):
                        raise self._convert_to_glue_connection_error(e, connection_name, operation_name)
                    else:
                        raise e
                
                # Don't retry on last attempt
                if attempt < self.max_retries:
                    delay = self._calculate_retry_delay(
                        attempt, 
                        error_classification['retry_strategy'],
                        error_classification.get('retry_delay', self.base_delay)
                    )
                    
                    self.structured_logger.info(
                        f"Retrying Glue Connection operation in {delay:.1f} seconds",
                        operation_name=operation_name,
                        connection_name=connection_name,
                        retry_attempt=attempt + 1,
                        retry_delay=delay,
                        retry_strategy=error_classification['retry_strategy']
                    )
                    
                    time.sleep(delay)
                else:
                    self.structured_logger.error(
                        f"Glue Connection operation failed after all retry attempts",
                        operation_name=operation_name,
                        connection_name=connection_name,
                        total_attempts=self.max_retries + 1,
                        final_error=str(e)
                    )
        
        # All retries exhausted
        if isinstance(last_exception, GlueConnectionBaseError):
            raise last_exception
        else:
            raise self._convert_to_glue_connection_error(
                last_exception, connection_name, operation_name, 
                f"after {self.max_retries + 1} attempts"
            )
    
    def _calculate_retry_delay(self, attempt: int, retry_strategy: str, 
                             base_delay: float) -> float:
        """Calculate delay for retry attempt based on strategy."""
        if retry_strategy == 'exponential_backoff':
            delay = min(base_delay * (self.backoff_factor ** attempt), self.max_delay)
        
        elif retry_strategy == 'exponential_backoff_with_jitter':
            delay = min(base_delay * (self.backoff_factor ** attempt), self.max_delay)
            # Add jitter to prevent thundering herd
            import random
            jitter_factor = random.uniform(0.5, 1.5)
            delay *= jitter_factor
        
        elif retry_strategy == 'fixed_delay':
            delay = base_delay
        
        else:  # Default to exponential backoff
            delay = min(base_delay * (self.backoff_factor ** attempt), self.max_delay)
        
        return delay
    
    def _convert_to_glue_connection_error(self, error: Exception, 
                                        connection_name: Optional[str],
                                        operation_name: str,
                                        additional_context: str = "") -> GlueConnectionBaseError:
        """Convert general exception to appropriate Glue Connection error."""
        error_message = str(error)
        
        if isinstance(error, ClientError):
            error_code = error.response.get('Error', {}).get('Code', '')
            aws_message = error.response.get('Error', {}).get('Message', '')
            
            if error_code == 'EntityNotFoundException':
                return GlueConnectionNotFoundError(connection_name or "unknown")
            
            elif error_code in ['AccessDeniedException', 'UnauthorizedOperation']:
                return GlueConnectionPermissionError(
                    f"Access denied for operation '{operation_name}': {aws_message}",
                    connection_name=connection_name,
                    operation=operation_name
                )
            
            elif error_code == 'AlreadyExistsException':
                return GlueConnectionCreationError(
                    f"Connection already exists {additional_context}",
                    connection_name or "unknown",
                    aws_error_code=error_code,
                    aws_error_message=aws_message
                )
            
            elif error_code == 'InvalidInputException':
                return GlueConnectionValidationError(
                    f"Invalid input for operation '{operation_name}': {aws_message} {additional_context}",
                    connection_name or "unknown"
                )
            
            else:
                return GlueConnectionCreationError(
                    f"AWS error during operation '{operation_name}' {additional_context}: {aws_message}",
                    connection_name or "unknown",
                    aws_error_code=error_code,
                    aws_error_message=aws_message
                )
        
        else:
            # For general exceptions, create a base error
            return GlueConnectionBaseError(
                f"Operation '{operation_name}' failed {additional_context}: {error_message}",
                connection_name=connection_name,
                error_code="OPERATION_FAILED",
                context={'original_error': error_message, 'operation': operation_name}
            )
    
    def create_connection_with_retry(self, glue_client, connection_input: Dict[str, Any],
                                   connection_name: str) -> Dict[str, Any]:
        """Create Glue Connection with retry logic.
        
        Args:
            glue_client: Boto3 Glue client
            connection_input: Glue Connection input dictionary
            connection_name: Name of the connection to create
            
        Returns:
            Response from create_connection API call
            
        Raises:
            GlueConnectionCreationError: If creation fails after retries
        """
        def _create_operation():
            return glue_client.create_connection(ConnectionInput=connection_input)
        
        return self.execute_with_retry(
            _create_operation,
            "create_glue_connection",
            connection_name=connection_name
        )
    
    def get_connection_with_retry(self, glue_client, connection_name: str) -> Dict[str, Any]:
        """Get Glue Connection with retry logic.
        
        Args:
            glue_client: Boto3 Glue client
            connection_name: Name of the connection to retrieve
            
        Returns:
            Response from get_connection API call
            
        Raises:
            GlueConnectionNotFoundError: If connection doesn't exist
            GlueConnectionBaseError: If retrieval fails after retries
        """
        def _get_operation():
            return glue_client.get_connection(Name=connection_name)
        
        return self.execute_with_retry(
            _get_operation,
            "get_glue_connection",
            connection_name=connection_name
        )
    
    def validate_connection_with_retry(self, validation_func: Callable,
                                     connection_name: str, *args, **kwargs) -> bool:
        """Validate Glue Connection with retry logic.
        
        Args:
            validation_func: Function to perform validation
            connection_name: Name of the connection to validate
            *args: Arguments to pass to validation function (connection_name will be appended)
            **kwargs: Keyword arguments to pass to validation function
            
        Returns:
            True if validation passes
            
        Raises:
            GlueConnectionValidationError: If validation fails after retries
        """
        # Create a wrapper that includes connection_name in the call
        # The validation function expects (connection_details, connection_name)
        def _validation_wrapper():
            return validation_func(*args, connection_name)
        
        return self.execute_with_retry(
            _validation_wrapper,
            "validate_glue_connection",
            connection_name=connection_name
        )
    
    def create_secret_with_retry(self, secrets_manager_handler, connection_name: str,
                               username: str, password: str, **kwargs) -> str:
        """Create Secrets Manager secret with retry logic.
        
        Args:
            secrets_manager_handler: SecretsManagerHandler instance
            connection_name: Name of the Glue connection
            username: Database username
            password: Database password
            **kwargs: Additional arguments for secret creation
            
        Returns:
            Secret ARN
            
        Raises:
            SecretCreationError: If secret creation fails after retries
            SecretsManagerPermissionError: If insufficient permissions
        """
        def _create_secret_operation():
            return secrets_manager_handler.create_secret(
                connection_name=connection_name,
                username=username,
                password=password,
                **kwargs
            )
        
        return self.execute_with_retry(
            _create_secret_operation,
            "create_secrets_manager_secret",
            connection_name=connection_name
        )
    
    def validate_secrets_manager_permissions_with_retry(self, secrets_manager_handler) -> bool:
        """Validate Secrets Manager permissions with retry logic.
        
        Args:
            secrets_manager_handler: SecretsManagerHandler instance
            
        Returns:
            True if permissions are valid
            
        Raises:
            SecretsManagerPermissionError: If permissions are insufficient
        """
        def _validate_permissions_operation():
            return secrets_manager_handler.validate_secret_permissions()
        
        return self.execute_with_retry(
            _validate_permissions_operation,
            "validate_secrets_manager_permissions"
        )