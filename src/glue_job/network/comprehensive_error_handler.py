"""
Comprehensive error handling manager for Glue Connection and Secrets Manager operations.

This module provides a unified error handling interface that integrates
Glue Connection errors, Secrets Manager errors, retry logic, and detailed logging.
"""

import logging
from typing import Dict, Any, Optional, Callable, Union
from datetime import datetime, timezone

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
from .glue_connection_retry_handler import GlueConnectionRetryHandler, GlueConnectionErrorClassifier
from ..config.secrets_manager_handler import (
    SecretsManagerError,
    SecretCreationError,
    SecretsManagerPermissionError,
    SecretsManagerRetryableError
)
from ..monitoring.logging import StructuredLogger
from ..monitoring.connection_strategy_logger import ConnectionStrategyLogger


class ComprehensiveErrorHandler:
    """Unified error handler for Glue Connection and Secrets Manager operations."""
    
    def __init__(self, job_name: str = "glue-data-replication", 
                 enable_detailed_logging: bool = True):
        self.job_name = job_name
        self.enable_detailed_logging = enable_detailed_logging
        
        # Initialize loggers
        self.structured_logger = StructuredLogger(f"ComprehensiveErrorHandler-{job_name}")
        self.connection_strategy_logger = ConnectionStrategyLogger(job_name)
        self.logger = logging.getLogger(__name__)
        
        # Initialize retry handler
        self.retry_handler = GlueConnectionRetryHandler()
        
        # Error tracking
        self.error_history = []
        self.operation_metrics = {}
        
    def handle_glue_connection_operation(self, operation: Callable, operation_name: str,
                                       connection_name: Optional[str] = None,
                                       connection_type: Optional[str] = None,
                                       **kwargs) -> Any:
        """Handle Glue Connection operation with comprehensive error handling.
        
        Args:
            operation: Function to execute
            operation_name: Name of the operation for logging
            connection_name: Optional Glue Connection name
            connection_type: Optional connection type ('source' or 'target')
            **kwargs: Keyword arguments to pass to operation
            
        Returns:
            Result of the operation
            
        Raises:
            GlueConnectionBaseError: For Glue Connection specific failures
        """
        operation_start = datetime.now(timezone.utc)
        operation_context = {
            'operation_name': operation_name,
            'connection_name': connection_name,
            'connection_type': connection_type,
            'start_time': operation_start.isoformat()
        }
        
        try:
            self.structured_logger.info(
                f"Starting Glue Connection operation: {operation_name}",
                **operation_context
            )
            
            # Execute operation with retry logic
            # Note: We pass connection_name as keyword arg and merge any additional kwargs
            # We don't pass *args to avoid "multiple values for argument" errors
            result = self.retry_handler.execute_with_retry(
                operation, operation_name, connection_name=connection_name, **kwargs
            )
            
            # Log successful operation
            operation_duration = (datetime.now(timezone.utc) - operation_start).total_seconds()
            self._log_operation_success(operation_name, operation_context, operation_duration)
            self._update_operation_metrics(operation_name, True, operation_duration)
            
            return result
            
        except GlueConnectionBaseError as e:
            # Handle Glue Connection specific errors
            operation_duration = (datetime.now(timezone.utc) - operation_start).total_seconds()
            error_info = self._create_error_info(e, operation_context, operation_duration)
            self._log_glue_connection_error(error_info)
            self._update_operation_metrics(operation_name, False, operation_duration, str(e))
            raise
            
        except Exception as e:
            # Handle unexpected errors
            operation_duration = (datetime.now(timezone.utc) - operation_start).total_seconds()
            error_info = self._create_error_info(e, operation_context, operation_duration)
            self._log_unexpected_error(error_info)
            self._update_operation_metrics(operation_name, False, operation_duration, str(e))
            
            # Convert to appropriate Glue Connection error
            raise self._convert_to_glue_connection_error(e, connection_name, operation_name)
    
    def handle_secrets_manager_operation(self, operation: Callable, operation_name: str,
                                       connection_name: Optional[str] = None,
                                       **kwargs) -> Any:
        """Handle Secrets Manager operation with comprehensive error handling.
        
        Args:
            operation: Function to execute
            operation_name: Name of the operation for logging
            connection_name: Optional connection name for context
            **kwargs: Keyword arguments to pass to operation
            
        Returns:
            Result of the operation
            
        Raises:
            SecretsManagerError: For Secrets Manager specific failures
        """
        operation_start = datetime.now(timezone.utc)
        operation_context = {
            'operation_name': operation_name,
            'connection_name': connection_name,
            'start_time': operation_start.isoformat(),
            'service': 'secrets_manager'
        }
        
        try:
            self.structured_logger.info(
                f"Starting Secrets Manager operation: {operation_name}",
                **operation_context
            )
            
            # Execute operation with retry logic for Secrets Manager
            result = self._execute_secrets_manager_with_retry(
                operation, operation_name, connection_name, **kwargs
            )
            
            # Log successful operation
            operation_duration = (datetime.now(timezone.utc) - operation_start).total_seconds()
            self._log_operation_success(operation_name, operation_context, operation_duration)
            self._update_operation_metrics(f"secrets_manager_{operation_name}", True, operation_duration)
            
            # Log to connection strategy logger
            self.connection_strategy_logger.log_secrets_manager_operation(
                operation_name, connection_name or "unknown", True
            )
            
            return result
            
        except SecretsManagerError as e:
            # Handle Secrets Manager specific errors
            operation_duration = (datetime.now(timezone.utc) - operation_start).total_seconds()
            error_info = self._create_error_info(e, operation_context, operation_duration)
            self._log_secrets_manager_error(error_info)
            self._update_operation_metrics(f"secrets_manager_{operation_name}", False, operation_duration, str(e))
            
            # Log to connection strategy logger
            self.connection_strategy_logger.log_secrets_manager_operation(
                operation_name, connection_name or "unknown", False, {'error': str(e)}
            )
            
            raise
            
        except Exception as e:
            # Handle unexpected errors
            operation_duration = (datetime.now(timezone.utc) - operation_start).total_seconds()
            error_info = self._create_error_info(e, operation_context, operation_duration)
            self._log_unexpected_error(error_info)
            self._update_operation_metrics(f"secrets_manager_{operation_name}", False, operation_duration, str(e))
            
            # Log to connection strategy logger
            self.connection_strategy_logger.log_secrets_manager_operation(
                operation_name, connection_name or "unknown", False, {'error': str(e)}
            )
            
            # Convert to appropriate Secrets Manager error
            raise self._convert_to_secrets_manager_error(e, connection_name, operation_name)
    
    def validate_parameters_with_detailed_errors(self, validation_func: Callable,
                                               parameters: Dict[str, Any],
                                               validation_context: str = "parameter_validation") -> Dict[str, Any]:
        """Validate parameters with detailed error reporting.
        
        Args:
            validation_func: Function to perform validation
            parameters: Parameters to validate
            validation_context: Context for validation (for logging)
            
        Returns:
            Validation results
            
        Raises:
            GlueConnectionParameterError: If validation fails
        """
        try:
            self.structured_logger.info(
                f"Starting parameter validation: {validation_context}",
                parameter_count=len(parameters),
                validation_context=validation_context
            )
            
            result = validation_func(parameters)
            
            self.structured_logger.info(
                f"Parameter validation successful: {validation_context}",
                validation_result=result
            )
            
            return result
            
        except GlueConnectionParameterError as e:
            # Enhanced parameter error logging
            self.structured_logger.error(
                f"Parameter validation failed: {validation_context}",
                error_message=str(e),
                parameter_name=e.parameter_name,
                parameter_value=e.parameter_value,
                conflicting_parameter=e.conflicting_parameter,
                validation_context=validation_context
            )
            
            # Log user-friendly error message
            self.logger.error(
                f"Parameter Validation Error: {str(e)}\n"
                f"Context: {validation_context}\n"
                f"Please check your parameter configuration and try again."
            )
            
            raise
            
        except Exception as e:
            self.structured_logger.error(
                f"Unexpected error during parameter validation: {validation_context}",
                error=str(e),
                validation_context=validation_context
            )
            
            raise GlueConnectionParameterError(
                f"Parameter validation failed due to unexpected error: {str(e)}",
                parameter_name="unknown"
            )
    
    def log_connection_strategy_decision(self, connection_type: str, engine_type: str,
                                       strategy: str, config: Dict[str, Any],
                                       reasoning: str) -> None:
        """Log connection strategy decision with comprehensive details.
        
        Args:
            connection_type: 'source' or 'target'
            engine_type: Database engine type
            strategy: Chosen strategy
            config: Connection configuration
            reasoning: Reasoning for the decision
        """
        self.connection_strategy_logger.log_strategy_decision(
            connection_type, engine_type, strategy, config, reasoning
        )
        
        # Also log to structured logger for analysis
        self.structured_logger.info(
            "Connection strategy decision made",
            connection_type=connection_type,
            engine_type=engine_type,
            strategy=strategy,
            reasoning=reasoning,
            timestamp=datetime.now(timezone.utc).isoformat()
        )
    
    def get_error_summary(self) -> Dict[str, Any]:
        """Get comprehensive error summary for the job.
        
        Returns:
            Dictionary containing error statistics and metrics
        """
        total_errors = len(self.error_history)
        
        if total_errors == 0:
            return {
                'total_errors': 0,
                'error_categories': {},
                'operation_metrics': self.operation_metrics,
                'success_rate': 1.0
            }
        
        # Categorize errors
        error_categories = {}
        for error in self.error_history:
            category = error.get('error_category', 'unknown')
            error_categories[category] = error_categories.get(category, 0) + 1
        
        # Calculate success rates
        total_operations = sum(
            metrics.get('total_attempts', 0) 
            for metrics in self.operation_metrics.values()
        )
        successful_operations = sum(
            metrics.get('successful_attempts', 0) 
            for metrics in self.operation_metrics.values()
        )
        
        success_rate = successful_operations / total_operations if total_operations > 0 else 1.0
        
        return {
            'total_errors': total_errors,
            'error_categories': error_categories,
            'operation_metrics': self.operation_metrics,
            'success_rate': success_rate,
            'latest_errors': self.error_history[-5:] if total_errors > 5 else self.error_history
        }
    
    def _execute_secrets_manager_with_retry(self, operation: Callable, operation_name: str,
                                          connection_name: Optional[str],
                                          **kwargs) -> Any:
        """Execute Secrets Manager operation with appropriate retry logic."""
        # Use the same retry handler but with Secrets Manager specific classification
        # Note: We only pass **kwargs to avoid "multiple values for argument" errors
        return self.retry_handler.execute_with_retry(
            operation, f"secrets_manager_{operation_name}", connection_name=connection_name, **kwargs
        )
    
    def _create_error_info(self, error: Exception, operation_context: Dict[str, Any],
                          operation_duration: float) -> Dict[str, Any]:
        """Create comprehensive error information dictionary."""
        error_classification = GlueConnectionErrorClassifier.classify_glue_connection_error(error)
        
        return {
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'error_type': type(error).__name__,
            'error_message': str(error),
            'error_category': error_classification.get('error_category', 'unknown'),
            'is_retryable': error_classification.get('is_retryable', False),
            'operation_context': operation_context,
            'operation_duration': operation_duration,
            'error_classification': error_classification
        }
    
    def _log_operation_success(self, operation_name: str, operation_context: Dict[str, Any],
                             operation_duration: float) -> None:
        """Log successful operation completion."""
        self.structured_logger.info(
            f"Operation completed successfully: {operation_name}",
            operation_duration=operation_duration,
            **operation_context
        )
    
    def _log_glue_connection_error(self, error_info: Dict[str, Any]) -> None:
        """Log Glue Connection specific error with detailed information."""
        self.structured_logger.error(
            "Glue Connection operation failed",
            **error_info
        )
        
        self.error_history.append(error_info)
        
        if self.enable_detailed_logging:
            self.logger.error(
                f"Glue Connection Error: {error_info['error_message']} "
                f"[Category: {error_info['error_category']}, "
                f"Retryable: {error_info['is_retryable']}]"
            )
    
    def _log_secrets_manager_error(self, error_info: Dict[str, Any]) -> None:
        """Log Secrets Manager specific error with detailed information."""
        self.structured_logger.error(
            "Secrets Manager operation failed",
            **error_info
        )
        
        self.error_history.append(error_info)
        
        if self.enable_detailed_logging:
            self.logger.error(
                f"Secrets Manager Error: {error_info['error_message']} "
                f"[Category: {error_info['error_category']}, "
                f"Retryable: {error_info['is_retryable']}]"
            )
    
    def _log_unexpected_error(self, error_info: Dict[str, Any]) -> None:
        """Log unexpected error with detailed information."""
        self.structured_logger.error(
            "Unexpected error occurred",
            **error_info
        )
        
        self.error_history.append(error_info)
        
        if self.enable_detailed_logging:
            self.logger.error(
                f"Unexpected Error: {error_info['error_message']} "
                f"[Operation: {error_info['operation_context']['operation_name']}]"
            )
    
    def _update_operation_metrics(self, operation_name: str, success: bool,
                                duration: float, error_message: str = None) -> None:
        """Update operation metrics for monitoring."""
        if operation_name not in self.operation_metrics:
            self.operation_metrics[operation_name] = {
                'total_attempts': 0,
                'successful_attempts': 0,
                'failed_attempts': 0,
                'total_duration': 0.0,
                'average_duration': 0.0,
                'last_error': None
            }
        
        metrics = self.operation_metrics[operation_name]
        metrics['total_attempts'] += 1
        metrics['total_duration'] += duration
        metrics['average_duration'] = metrics['total_duration'] / metrics['total_attempts']
        
        if success:
            metrics['successful_attempts'] += 1
        else:
            metrics['failed_attempts'] += 1
            metrics['last_error'] = error_message
    
    def _convert_to_glue_connection_error(self, error: Exception, connection_name: Optional[str],
                                        operation_name: str) -> GlueConnectionBaseError:
        """Convert general exception to appropriate Glue Connection error."""
        return self.retry_handler._convert_to_glue_connection_error(
            error, connection_name, operation_name
        )
    
    def _convert_to_secrets_manager_error(self, error: Exception, connection_name: Optional[str],
                                        operation_name: str) -> SecretsManagerError:
        """Convert general exception to appropriate Secrets Manager error."""
        error_message = str(error)
        
        if 'permission' in error_message.lower() or 'access denied' in error_message.lower():
            return SecretsManagerPermissionError(
                f"Permission error during {operation_name}: {error_message}",
                secret_name=connection_name,
                operation=operation_name
            )
        elif 'timeout' in error_message.lower() or 'throttl' in error_message.lower():
            return SecretsManagerRetryableError(
                f"Transient error during {operation_name}: {error_message}",
                secret_name=connection_name
            )
        else:
            return SecretsManagerError(
                f"Secrets Manager operation '{operation_name}' failed: {error_message}",
                secret_name=connection_name,
                error_code="OPERATION_FAILED"
            )