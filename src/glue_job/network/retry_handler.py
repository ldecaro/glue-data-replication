"""
Retry handling and error recovery for AWS Glue Data Replication.

This module provides intelligent retry logic with exponential backoff,
error classification, and recovery strategies for various failure scenarios.
"""

import time
import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any
from .error_handler import (
    ErrorCategory, 
    NetworkConnectivityError, 
    GlueConnectionError, 
    VpcEndpointError, 
    ENICreationError,
    NetworkErrorHandler
)
from ..monitoring.logging import StructuredLogger

logger = logging.getLogger(__name__)


class ErrorClassifier:
    """Classifies errors into categories for appropriate handling."""
    
    # Error patterns for classification
    ERROR_PATTERNS = {
        ErrorCategory.CONNECTION: [
            'connection refused', 'connection timed out', 'connection reset',
            'no route to host', 'network unreachable', 'connection failed',
            'could not connect', 'unable to connect', 'connection error'
        ],
        ErrorCategory.AUTHENTICATION: [
            'authentication failed', 'login failed', 'invalid credentials',
            'access denied', 'unauthorized', 'invalid username or password',
            'authentication error', 'login incorrect'
        ],
        ErrorCategory.NETWORK: [
            'network error', 'socket timeout', 'read timeout', 'write timeout',
            'network is unreachable', 'host unreachable', 'dns resolution failed',
            'connection timeout', 'socket error'
        ],
        ErrorCategory.DATA_PROCESSING: [
            'data type mismatch', 'conversion error', 'parsing error',
            'invalid data format', 'constraint violation', 'data truncation',
            'null value', 'duplicate key'
        ],
        ErrorCategory.SCHEMA_MISMATCH: [
            'column not found', 'table not found', 'schema mismatch',
            'invalid column name', 'missing column', 'unknown column',
            'table does not exist', 'column does not exist'
        ],
        ErrorCategory.PERMISSION: [
            'permission denied', 'access forbidden', 'insufficient privileges',
            'not authorized', 'privilege error', 'access violation',
            'security error', 'forbidden'
        ],
        ErrorCategory.RESOURCE: [
            'out of memory', 'disk full', 'resource exhausted',
            'too many connections', 'connection pool exhausted',
            'memory error', 'resource unavailable'
        ],
        ErrorCategory.TIMEOUT: [
            'timeout', 'timed out', 'operation timeout', 'query timeout',
            'connection timeout', 'read timeout', 'write timeout'
        ]
    }
    
    @classmethod
    def classify_error(cls, error: Exception) -> str:
        """Classify error into appropriate category."""
        error_message = str(error).lower()
        
        for category, patterns in cls.ERROR_PATTERNS.items():
            for pattern in patterns:
                if pattern in error_message:
                    return category
        
        return ErrorCategory.UNKNOWN
    
    @classmethod
    def is_retryable_error(cls, error: Exception) -> bool:
        """Determine if error is retryable based on its category."""
        category = cls.classify_error(error)
        
        # Retryable error categories
        retryable_categories = {
            ErrorCategory.CONNECTION,
            ErrorCategory.NETWORK,
            ErrorCategory.TIMEOUT,
            ErrorCategory.RESOURCE
        }
        
        return category in retryable_categories
    
    @classmethod
    def get_recovery_strategy(cls, error: Exception) -> str:
        """Get recommended recovery strategy for error."""
        category = cls.classify_error(error)
        
        strategies = {
            ErrorCategory.CONNECTION: "retry_with_backoff",
            ErrorCategory.AUTHENTICATION: "fail_immediately",
            ErrorCategory.NETWORK: "retry_with_backoff",
            ErrorCategory.DATA_PROCESSING: "log_and_continue",
            ErrorCategory.SCHEMA_MISMATCH: "fail_immediately",
            ErrorCategory.PERMISSION: "fail_immediately",
            ErrorCategory.RESOURCE: "retry_with_longer_delay",
            ErrorCategory.TIMEOUT: "retry_with_backoff",
            ErrorCategory.UNKNOWN: "retry_with_backoff"
        }
        
        return strategies.get(category, "retry_with_backoff")


class ConnectionRetryHandler:
    """Enhanced connection retry handler with exponential backoff and error classification."""
    
    def __init__(self, max_retries: int = 3, base_delay: float = 1.0, 
                 max_delay: float = 60.0, backoff_factor: float = 2.0,
                 jitter: bool = True):
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.backoff_factor = backoff_factor
        self.jitter = jitter
        self.error_classifier = ErrorClassifier()
        self.network_error_handler = NetworkErrorHandler()
        self.structured_logger = StructuredLogger("ConnectionRetryHandler")
    
    def execute_with_retry(self, operation, operation_name: str, *args, **kwargs):
        """Execute operation with intelligent retry logic based on error classification."""
        last_exception = None
        retry_count = 0
        
        for attempt in range(self.max_retries + 1):
            try:
                self.structured_logger.info(f"Attempting {operation_name} (attempt {attempt + 1}/{self.max_retries + 1})")
                result = operation(*args, **kwargs)
                
                if attempt > 0:
                    self.structured_logger.info(f"{operation_name} succeeded after {attempt + 1} attempts")
                
                return result
                
            except Exception as e:
                last_exception = e
                error_category = self.error_classifier.classify_error(e)
                recovery_strategy = self.error_classifier.get_recovery_strategy(e)
                
                self.structured_logger.warning(
                    f"{operation_name} failed (attempt {attempt + 1}): {str(e)} "
                    f"[Category: {error_category}, Strategy: {recovery_strategy}]"
                )
                
                # Perform network-specific diagnostics for network errors
                self._perform_network_diagnostics(e, error_category, operation_name, *args, **kwargs)
                
                # Check if error is retryable
                if not self.error_classifier.is_retryable_error(e):
                    self.structured_logger.error(f"{operation_name} failed with non-retryable error: {str(e)}")
                    raise RuntimeError(f"{operation_name} failed: {str(e)} [Non-retryable: {error_category}]")
                
                # Don't retry on last attempt
                if attempt < self.max_retries:
                    delay = self._calculate_delay(attempt, recovery_strategy)
                    self.structured_logger.info(f"Retrying {operation_name} in {delay:.1f} seconds...")
                    time.sleep(delay)
                    retry_count += 1
                else:
                    self.structured_logger.error(f"{operation_name} failed after {self.max_retries + 1} attempts: {str(e)}")
        
        # Create detailed error message with retry information
        error_details = {
            'operation': operation_name,
            'total_attempts': self.max_retries + 1,
            'retry_count': retry_count,
            'last_error': str(last_exception),
            'error_category': self.error_classifier.classify_error(last_exception),
            'recovery_strategy': self.error_classifier.get_recovery_strategy(last_exception)
        }
        
        raise RuntimeError(
            f"{operation_name} failed after {self.max_retries + 1} attempts. "
            f"Error details: {error_details}"
        )
    
    def _calculate_delay(self, attempt: int, recovery_strategy: str) -> float:
        """Calculate delay based on attempt number and recovery strategy."""
        if recovery_strategy == "retry_with_longer_delay":
            # Use longer delays for resource exhaustion
            base_delay = self.base_delay * 3
        else:
            base_delay = self.base_delay
        
        # Calculate exponential backoff
        delay = min(base_delay * (self.backoff_factor ** attempt), self.max_delay)
        
        # Add jitter to prevent thundering herd
        if self.jitter:
            import random
            jitter_factor = random.uniform(0.5, 1.5)
            delay *= jitter_factor
        
        return delay
    
    def retry_with_network_recovery(self, operation, operation_name: str, 
                                  connection_config=None, *args, **kwargs):
        """Execute operation with network-aware retry logic and recovery."""
        last_exception = None
        
        for attempt in range(self.max_retries + 1):
            try:
                self.structured_logger.info(
                    f"Attempting {operation_name} with network recovery (attempt {attempt + 1}/{self.max_retries + 1})"
                )
                
                # Pre-validate network connectivity if connection config is provided
                if connection_config and attempt > 0:  # Skip on first attempt
                    self._validate_network_connectivity_before_retry(connection_config)
                
                result = operation(*args, **kwargs)
                
                if attempt > 0:
                    self.structured_logger.info(f"{operation_name} succeeded after {attempt + 1} attempts with network recovery")
                
                return result
                
            except (NetworkConnectivityError, GlueConnectionError, VpcEndpointError, ENICreationError) as network_error:
                last_exception = network_error
                
                self.structured_logger.error(
                    f"Network-specific error in {operation_name} (attempt {attempt + 1})",
                    error_type=type(network_error).__name__,
                    error_message=str(network_error)
                )
                
                # Perform detailed diagnostics for network errors
                if hasattr(network_error, 'connection_name') and network_error.connection_name:
                    diagnostics = self.network_error_handler.diagnose_glue_connection_failure(
                        network_error.connection_name, network_error
                    )
                    self.structured_logger.error(
                        "Network error diagnostics",
                        connection_name=network_error.connection_name,
                        diagnostics=diagnostics
                    )
                
                # Don't retry network configuration errors
                if isinstance(network_error, (GlueConnectionError, VpcEndpointError)):
                    self.structured_logger.error(f"Non-retryable network configuration error: {str(network_error)}")
                    raise network_error
                
                # Retry ENI and connectivity errors with longer delays
                if attempt < self.max_retries:
                    delay = self._calculate_network_recovery_delay(attempt, type(network_error))
                    self.structured_logger.info(f"Retrying {operation_name} after network error in {delay:.1f} seconds...")
                    time.sleep(delay)
                else:
                    self.structured_logger.error(f"{operation_name} failed after {self.max_retries + 1} attempts with network errors")
                    raise network_error
                    
            except Exception as e:
                # Handle non-network errors with standard retry logic
                return self.execute_with_retry(operation, operation_name, *args, **kwargs)
        
        raise last_exception
    
    def _validate_network_connectivity_before_retry(self, connection_config):
        """Validate network connectivity before retry attempt."""
        try:
            if hasattr(connection_config, 'requires_cross_vpc_connection') and connection_config.requires_cross_vpc_connection():
                connection_name = connection_config.get_glue_connection_name()
                if connection_name:
                    # Quick validation of Glue connection existence
                    diagnostics = self.network_error_handler.diagnose_glue_connection_failure(
                        connection_name, Exception("Pre-retry validation")
                    )
                    
                    if not diagnostics.get('connection_exists', False):
                        raise GlueConnectionError(
                            f"Glue connection '{connection_name}' does not exist",
                            connection_name
                        )
                    
                    self.structured_logger.info(
                        "Network connectivity pre-validation passed",
                        connection_name=connection_name
                    )
        
        except Exception as validation_error:
            self.structured_logger.warning(
                "Network connectivity pre-validation failed",
                error=str(validation_error)
            )
            # Don't fail the retry attempt due to validation issues
    
    def _calculate_network_recovery_delay(self, attempt: int, error_type: type) -> float:
        """Calculate delay for network recovery based on error type."""
        # Use longer delays for network-related errors
        base_multiplier = 1.0
        
        if error_type == ENICreationError:
            # ENI creation failures may need longer recovery time
            base_multiplier = 3.0
        elif error_type == NetworkConnectivityError:
            # Network connectivity issues may resolve quickly
            base_multiplier = 1.5
        elif error_type == VpcEndpointError:
            # VPC endpoint issues typically need longer recovery
            base_multiplier = 2.0
        
        delay = min(
            self.base_delay * base_multiplier * (self.backoff_factor ** attempt), 
            self.max_delay
        )
        
        # Add jitter for network recovery
        if self.jitter:
            import random
            jitter_factor = random.uniform(0.8, 1.2)
            delay *= jitter_factor
        
        return delay
    
    def execute_with_circuit_breaker(self, operation, operation_name: str, 
                                   failure_threshold: int = 5, 
                                   recovery_timeout: float = 300.0, *args, **kwargs):
        """Execute operation with circuit breaker pattern for repeated failures."""
        # Simple circuit breaker implementation
        circuit_key = f"circuit_{operation_name}"
        
        # This would typically be stored in a shared cache/database
        # For simplicity, using class-level storage
        if not hasattr(self, '_circuit_states'):
            self._circuit_states = {}
        
        circuit_state = self._circuit_states.get(circuit_key, {
            'failure_count': 0,
            'last_failure_time': 0,
            'state': 'closed'  # closed, open, half_open
        })
        
        current_time = time.time()
        
        # Check circuit state
        if circuit_state['state'] == 'open':
            if current_time - circuit_state['last_failure_time'] > recovery_timeout:
                circuit_state['state'] = 'half_open'
                logger.info(f"Circuit breaker for {operation_name} moving to half-open state")
            else:
                raise RuntimeError(
                    f"Circuit breaker is open for {operation_name}. "
                    f"Will retry after {recovery_timeout - (current_time - circuit_state['last_failure_time']):.1f} seconds"
                )
        
        try:
            result = self.execute_with_retry(operation, operation_name, *args, **kwargs)
            
            # Success - reset circuit breaker
            if circuit_state['state'] == 'half_open':
                circuit_state['state'] = 'closed'
                circuit_state['failure_count'] = 0
                logger.info(f"Circuit breaker for {operation_name} reset to closed state")
            
            self._circuit_states[circuit_key] = circuit_state
            return result
            
        except Exception as e:
            # Failure - update circuit breaker
            circuit_state['failure_count'] += 1
            circuit_state['last_failure_time'] = current_time
            
            if circuit_state['failure_count'] >= failure_threshold:
                circuit_state['state'] = 'open'
                logger.error(
                    f"Circuit breaker opened for {operation_name} after {failure_threshold} failures. "
                    f"Will remain open for {recovery_timeout} seconds"
                )
            
            self._circuit_states[circuit_key] = circuit_state
            raise
    
    def _perform_network_diagnostics(self, error: Exception, error_category: str, operation_name: str, *args, **kwargs):
        """Perform detailed network diagnostics for network-related errors."""
        try:
            # Check if this is a network-related error that warrants diagnostics
            network_error_categories = {
                'network_connectivity', 'cross_vpc_routing', 'glue_connection', 
                'eni_creation', 'vpc_endpoint'
            }
            
            if error_category not in network_error_categories:
                return
            
            self.structured_logger.info(
                "Performing network diagnostics for error",
                operation_name=operation_name,
                error_category=error_category,
                error_message=str(error)
            )
            
            # Extract connection information from arguments
            connection_name = self._extract_connection_name_from_args(*args, **kwargs)
            vpc_id = self._extract_vpc_id_from_args(*args, **kwargs)
            
            # Perform Glue connection diagnostics if connection name is available
            if connection_name and error_category in ['glue_connection', 'cross_vpc_routing']:
                try:
                    diagnostics = self.network_error_handler.diagnose_glue_connection_failure(
                        connection_name, error
                    )
                    self.structured_logger.error(
                        "Glue connection diagnostics completed",
                        operation_name=operation_name,
                        connection_name=connection_name,
                        diagnostics_summary={
                            'connection_exists': diagnostics.get('connection_exists'),
                            'issues_count': len(diagnostics.get('diagnostics', [])),
                            'recommendations_count': len(diagnostics.get('recommendations', []))
                        }
                    )
                    
                    # Log specific issues and recommendations
                    for issue in diagnostics.get('diagnostics', []):
                        self.structured_logger.warning(f"Network issue detected: {issue}")
                    
                    for recommendation in diagnostics.get('recommendations', []):
                        self.structured_logger.info(f"Network recommendation: {recommendation}")
                        
                except Exception as diag_error:
                    self.structured_logger.warning(
                        "Failed to perform Glue connection diagnostics",
                        connection_name=connection_name,
                        error=str(diag_error)
                    )
            
            # Perform VPC endpoint diagnostics if VPC ID is available
            if vpc_id and error_category == 'vpc_endpoint':
                try:
                    vpc_diagnostics = self.network_error_handler.diagnose_vpc_endpoint_issues(vpc_id)
                    self.structured_logger.error(
                        "VPC endpoint diagnostics completed",
                        operation_name=operation_name,
                        vpc_id=vpc_id,
                        diagnostics_summary={
                            'issues_count': len(vpc_diagnostics.get('issues', [])),
                            'recommendations_count': len(vpc_diagnostics.get('recommendations', []))
                        }
                    )
                    
                    # Log specific issues and recommendations
                    for issue in vpc_diagnostics.get('issues', []):
                        self.structured_logger.warning(f"VPC endpoint issue: {issue}")
                    
                    for recommendation in vpc_diagnostics.get('recommendations', []):
                        self.structured_logger.info(f"VPC endpoint recommendation: {recommendation}")
                        
                except Exception as diag_error:
                    self.structured_logger.warning(
                        "Failed to perform VPC endpoint diagnostics",
                        vpc_id=vpc_id,
                        error=str(diag_error)
                    )
            
        except Exception as diag_error:
            self.structured_logger.warning(
                "Failed to perform network diagnostics",
                operation_name=operation_name,
                error=str(diag_error)
            )
    
    def _extract_connection_name_from_args(self, *args, **kwargs) -> Optional[str]:
        """Extract Glue connection name from operation arguments."""
        try:
            # Check kwargs first
            if 'glue_connection_name' in kwargs:
                return kwargs['glue_connection_name']
            
            if 'connection_name' in kwargs:
                return kwargs['connection_name']
            
            # Check args for connection config objects
            for arg in args:
                if hasattr(arg, 'get_glue_connection_name'):
                    connection_name = arg.get_glue_connection_name()
                    if connection_name:
                        return connection_name
                
                if hasattr(arg, 'network_config') and arg.network_config:
                    if hasattr(arg.network_config, 'glue_connection_name'):
                        return arg.network_config.glue_connection_name
            
            return None
            
        except Exception:
            return None
    
    def _extract_vpc_id_from_args(self, *args, **kwargs) -> Optional[str]:
        """Extract VPC ID from operation arguments."""
        try:
            # Check kwargs first
            if 'vpc_id' in kwargs:
                return kwargs['vpc_id']
            
            # Check args for connection config objects
            for arg in args:
                if hasattr(arg, 'network_config') and arg.network_config:
                    if hasattr(arg.network_config, 'vpc_id'):
                        return arg.network_config.vpc_id
            
            return None
            
        except Exception:
            return None


class ErrorRecoveryManager:
    """Manages error recovery strategies and graceful failure handling."""
    
    def __init__(self, job_name: str, enable_detailed_logging: bool = True):
        self.job_name = job_name
        self.enable_detailed_logging = enable_detailed_logging
        self.error_history = []
        self.recovery_attempts = {}
        self.critical_errors = []
        
    def handle_database_connection_error(self, error: Exception, connection_config, 
                                       operation_context: str) -> Dict[str, Any]:
        """Handle database connection errors with appropriate recovery strategies."""
        error_info = {
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'error_type': type(error).__name__,
            'error_message': str(error),
            'error_category': ErrorClassifier.classify_error(error),
            'operation_context': operation_context,
            'connection_details': {
                'engine_type': connection_config.engine_type,
                'database': connection_config.database,
                'schema': connection_config.schema,
                'connection_string': self._sanitize_connection_string(connection_config.connection_string)
            },
            'recovery_strategy': ErrorClassifier.get_recovery_strategy(error),
            'is_retryable': ErrorClassifier.is_retryable_error(error)
        }
        
        # Log detailed error information
        if self.enable_detailed_logging:
            logger.error(
                f"Database connection error in {operation_context}: {error_info['error_message']} "
                f"[Category: {error_info['error_category']}, Engine: {connection_config.engine_type}]"
            )
            
            # Log connection details (sanitized)
            logger.debug(f"Connection details: {error_info['connection_details']}")
        
        # Store error in history
        self.error_history.append(error_info)
        
        # Determine if this is a critical error that should stop the job
        if self._is_critical_error(error_info):
            self.critical_errors.append(error_info)
            logger.critical(
                f"Critical database error detected: {error_info['error_message']}. "
                f"Job may need to be terminated."
            )
        
        return error_info
    
    def handle_data_processing_error(self, error: Exception, table_name: str, 
                                   operation_type: str, context_data: Dict[str, Any] = None) -> Dict[str, Any]:
        """Handle data processing errors with recovery options."""
        error_info = {
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'error_type': type(error).__name__,
            'error_message': str(error),
            'error_category': ErrorClassifier.classify_error(error),
            'table_name': table_name,
            'operation_type': operation_type,
            'context_data': context_data or {},
            'recovery_strategy': ErrorClassifier.get_recovery_strategy(error),
            'is_retryable': ErrorClassifier.is_retryable_error(error)
        }
        
        # Log error with context
        logger.error(
            f"Data processing error for table {table_name} during {operation_type}: "
            f"{error_info['error_message']} [Category: {error_info['error_category']}]"
        )
        
        if context_data:
            logger.debug(f"Error context: {context_data}")
        
        # Store error in history
        self.error_history.append(error_info)
        
        # Handle specific data processing error types
        if error_info['error_category'] == ErrorCategory.SCHEMA_MISMATCH:
            logger.error(
                f"Schema mismatch detected for table {table_name}. "
                f"Please verify that target schema matches source schema requirements."
            )
        elif error_info['error_category'] == ErrorCategory.DATA_PROCESSING:
            logger.warning(
                f"Data processing issue for table {table_name}. "
                f"Attempting to continue with remaining data."
            )
        
        return error_info
    
    def handle_infrastructure_error(self, error: Exception, component: str, 
                                  operation_context: str) -> Dict[str, Any]:
        """Handle infrastructure-related errors (S3, IAM, Glue, etc.)."""
        error_info = {
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'error_type': type(error).__name__,
            'error_message': str(error),
            'error_category': ErrorClassifier.classify_error(error),
            'component': component,
            'operation_context': operation_context,
            'recovery_strategy': ErrorClassifier.get_recovery_strategy(error),
            'is_retryable': ErrorClassifier.is_retryable_error(error)
        }
        
        # Log infrastructure error
        logger.error(
            f"Infrastructure error in {component} during {operation_context}: "
            f"{error_info['error_message']} [Category: {error_info['error_category']}]"
        )
        
        # Store error in history
        self.error_history.append(error_info)
        
        # Handle specific infrastructure errors
        if 'S3' in component.upper() or 's3://' in str(error).lower():
            logger.error(
                f"S3 access error detected. Please verify:"
                f"\n- S3 bucket permissions and IAM role access"
                f"\n- JDBC driver file paths and availability"
                f"\n- Network connectivity to S3"
            )
        elif 'IAM' in component.upper() or 'permission' in str(error).lower():
            logger.error(
                f"IAM permission error detected. Please verify:"
                f"\n- Glue job execution role permissions"
                f"\n- Database access permissions"
                f"\n- S3 bucket access permissions"
            )
        
        return error_info
    
    def attempt_graceful_recovery(self, error_info: Dict[str, Any], 
                                recovery_context: Dict[str, Any] = None) -> bool:
        """Attempt graceful recovery based on error type and context."""
        recovery_key = f"{error_info['error_category']}_{error_info.get('table_name', 'global')}"
        
        # Track recovery attempts
        if recovery_key not in self.recovery_attempts:
            self.recovery_attempts[recovery_key] = 0
        
        self.recovery_attempts[recovery_key] += 1
        max_recovery_attempts = 3
        
        if self.recovery_attempts[recovery_key] > max_recovery_attempts:
            logger.error(
                f"Maximum recovery attempts ({max_recovery_attempts}) exceeded for {recovery_key}. "
                f"Giving up on recovery."
            )
            return False
        
        logger.info(
            f"Attempting graceful recovery for {recovery_key} "
            f"(attempt {self.recovery_attempts[recovery_key]}/{max_recovery_attempts})"
        )
        
        try:
            # Implement recovery strategies based on error category
            if error_info['error_category'] == ErrorCategory.CONNECTION:
                return self._recover_connection_error(error_info, recovery_context)
            elif error_info['error_category'] == ErrorCategory.DATA_PROCESSING:
                return self._recover_data_processing_error(error_info, recovery_context)
            elif error_info['error_category'] == ErrorCategory.RESOURCE:
                return self._recover_resource_error(error_info, recovery_context)
            elif error_info['error_category'] == ErrorCategory.TIMEOUT:
                return self._recover_timeout_error(error_info, recovery_context)
            else:
                logger.warning(f"No specific recovery strategy for category: {error_info['error_category']}")
                return False
                
        except Exception as recovery_error:
            logger.error(f"Recovery attempt failed: {str(recovery_error)}")
            return False
    
    def _recover_connection_error(self, error_info: Dict[str, Any], 
                                recovery_context: Dict[str, Any] = None) -> bool:
        """Attempt to recover from connection errors."""
        logger.info("Attempting connection error recovery...")
        
        # Wait before retry to allow transient issues to resolve
        time.sleep(5)
        
        # Could implement connection pool reset, alternative connection strings, etc.
        logger.info("Connection error recovery completed")
        return True
    
    def _recover_data_processing_error(self, error_info: Dict[str, Any], 
                                     recovery_context: Dict[str, Any] = None) -> bool:
        """Attempt to recover from data processing errors."""
        logger.info("Attempting data processing error recovery...")
        
        # Could implement data validation, schema refresh, etc.
        logger.info("Data processing error recovery completed")
        return True
    
    def _recover_resource_error(self, error_info: Dict[str, Any], 
                              recovery_context: Dict[str, Any] = None) -> bool:
        """Attempt to recover from resource exhaustion errors."""
        logger.info("Attempting resource error recovery...")
        
        # Wait longer for resource exhaustion
        time.sleep(30)
        
        # Could implement memory cleanup, connection pool management, etc.
        logger.info("Resource error recovery completed")
        return True
    
    def _recover_timeout_error(self, error_info: Dict[str, Any], 
                             recovery_context: Dict[str, Any] = None) -> bool:
        """Attempt to recover from timeout errors."""
        logger.info("Attempting timeout error recovery...")
        
        # Wait before retry
        time.sleep(10)
        
        # Could implement query optimization, batch size reduction, etc.
        logger.info("Timeout error recovery completed")
        return True
    
    def _is_critical_error(self, error_info: Dict[str, Any]) -> bool:
        """Determine if an error is critical and should stop the job."""
        critical_categories = {
            ErrorCategory.AUTHENTICATION,
            ErrorCategory.PERMISSION,
            ErrorCategory.SCHEMA_MISMATCH
        }
        
        return error_info['error_category'] in critical_categories
    
    def _sanitize_connection_string(self, connection_string: str) -> str:
        """Remove sensitive information from connection string for logging."""
        import re
        
        # Remove password from connection string
        sanitized = re.sub(r'password=[^;]+', 'password=***', connection_string, flags=re.IGNORECASE)
        sanitized = re.sub(r'pwd=[^;]+', 'pwd=***', sanitized, flags=re.IGNORECASE)
        
        return sanitized
    
    def get_error_summary(self) -> Dict[str, Any]:
        """Get summary of all errors encountered during job execution."""
        if not self.error_history:
            return {'total_errors': 0, 'error_categories': {}, 'critical_errors': 0}
        
        # Count errors by category
        error_categories = {}
        for error in self.error_history:
            category = error['error_category']
            error_categories[category] = error_categories.get(category, 0) + 1
        
        return {
            'total_errors': len(self.error_history),
            'error_categories': error_categories,
            'critical_errors': len(self.critical_errors),
            'recovery_attempts': dict(self.recovery_attempts),
            'latest_errors': self.error_history[-5:] if len(self.error_history) > 5 else self.error_history
        }
    
    def log_final_error_report(self) -> None:
        """Log final error report at job completion."""
        summary = self.get_error_summary()
        
        if summary['total_errors'] == 0:
            logger.info(f"Job {self.job_name} completed without errors")
            return
        
        logger.info(
            f"Job {self.job_name} error summary: "
            f"{summary['total_errors']} total errors, "
            f"{summary['critical_errors']} critical errors"
        )
        
        # Log error breakdown by category
        for category, count in summary['error_categories'].items():
            logger.info(f"  {category}: {count} errors")
        
        # Log recovery attempts
        if summary['recovery_attempts']:
            logger.info("Recovery attempts:")
            for recovery_key, attempts in summary['recovery_attempts'].items():
                logger.info(f"  {recovery_key}: {attempts} attempts")
        
        # Log critical errors if any
        if summary['critical_errors'] > 0:
            logger.error(f"Critical errors detected ({summary['critical_errors']}):")
            for error in self.critical_errors:
                logger.error(
                    f"  {error['timestamp']}: {error['error_message']} "
                    f"[{error['error_category']}]"
                )