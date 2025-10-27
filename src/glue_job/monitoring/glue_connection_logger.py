"""
Specialized logging for Glue Connection operations.

This module provides comprehensive logging for Glue Connection strategy decisions,
operations, and error handling with structured logging support.
"""

import logging
from typing import Dict, Any, Optional
from .logging import StructuredLogger


class GlueConnectionOperationLogger:
    """Specialized logger for Glue Connection operations with detailed context."""
    
    def __init__(self, component_name: str = "GlueConnectionOperations"):
        self.structured_logger = StructuredLogger(component_name)
        self.logger = logging.getLogger(component_name)
    
    def log_strategy_decision(self, connection_type: str, strategy: str, 
                            context: Dict[str, Any]) -> None:
        """Log connection strategy decision with full context.
        
        Args:
            connection_type: 'source' or 'target'
            strategy: Connection strategy ('create_glue', 'use_glue', 'direct_jdbc')
            context: Additional context information
        """
        strategy_descriptions = {
            'create_glue': 'Create new Glue Connection',
            'use_glue': 'Use existing Glue Connection',
            'direct_jdbc': 'Direct JDBC connection (no Glue Connection)'
        }
        
        description = strategy_descriptions.get(strategy, f'Unknown strategy: {strategy}')
        
        self.structured_logger.info(
            f"Connection strategy decision: {description}",
            connection_type=connection_type,
            strategy=strategy,
            engine_type=context.get('engine_type'),
            connection_name=context.get('connection_name'),
            parameter_source=context.get('parameter_source'),
            **{k: v for k, v in context.items() if k not in ['engine_type', 'connection_name', 'parameter_source']}
        )
    
    def log_connection_creation_start(self, connection_name: str, 
                                    connection_config: Dict[str, Any]) -> None:
        """Log the start of Glue Connection creation.
        
        Args:
            connection_name: Name of the connection being created
            connection_config: Connection configuration details
        """
        self.structured_logger.info(
            "Starting Glue Connection creation",
            operation="create_glue_connection",
            connection_name=connection_name,
            engine_type=connection_config.get('engine_type'),
            has_network_config=bool(connection_config.get('network_config')),
            connection_type=connection_config.get('connection_type', 'JDBC')
        )
    
    def log_connection_creation_success(self, connection_name: str, 
                                      duration_seconds: float,
                                      retry_attempts: int = 0) -> None:
        """Log successful Glue Connection creation.
        
        Args:
            connection_name: Name of the created connection
            duration_seconds: Time taken to create the connection
            retry_attempts: Number of retry attempts made
        """
        self.structured_logger.info(
            "Glue Connection created successfully",
            operation="create_glue_connection",
            connection_name=connection_name,
            duration_seconds=round(duration_seconds, 2),
            retry_attempts=retry_attempts,
            status="success"
        )
    
    def log_connection_creation_failure(self, connection_name: str, 
                                      error: Exception,
                                      duration_seconds: float,
                                      retry_attempts: int = 0) -> None:
        """Log failed Glue Connection creation.
        
        Args:
            connection_name: Name of the connection that failed to create
            error: Exception that caused the failure
            duration_seconds: Time spent attempting to create the connection
            retry_attempts: Number of retry attempts made
        """
        self.structured_logger.error(
            "Glue Connection creation failed",
            operation="create_glue_connection",
            connection_name=connection_name,
            error_type=type(error).__name__,
            error_message=str(error),
            duration_seconds=round(duration_seconds, 2),
            retry_attempts=retry_attempts,
            status="failed"
        )
    
    def log_connection_retrieval_start(self, connection_name: str) -> None:
        """Log the start of Glue Connection retrieval.
        
        Args:
            connection_name: Name of the connection being retrieved
        """
        self.structured_logger.info(
            "Starting Glue Connection retrieval",
            operation="get_glue_connection",
            connection_name=connection_name
        )
    
    def log_connection_retrieval_success(self, connection_name: str,
                                       connection_details: Dict[str, Any],
                                       duration_seconds: float,
                                       retry_attempts: int = 0) -> None:
        """Log successful Glue Connection retrieval.
        
        Args:
            connection_name: Name of the retrieved connection
            connection_details: Retrieved connection details
            duration_seconds: Time taken to retrieve the connection
            retry_attempts: Number of retry attempts made
        """
        self.structured_logger.info(
            "Glue Connection retrieved successfully",
            operation="get_glue_connection",
            connection_name=connection_name,
            connection_type=connection_details.get('connection_type'),
            has_physical_requirements=bool(connection_details.get('physical_connection_requirements')),
            duration_seconds=round(duration_seconds, 2),
            retry_attempts=retry_attempts,
            status="success"
        )
    
    def log_connection_retrieval_failure(self, connection_name: str,
                                       error: Exception,
                                       duration_seconds: float,
                                       retry_attempts: int = 0) -> None:
        """Log failed Glue Connection retrieval.
        
        Args:
            connection_name: Name of the connection that failed to retrieve
            error: Exception that caused the failure
            duration_seconds: Time spent attempting to retrieve the connection
            retry_attempts: Number of retry attempts made
        """
        self.structured_logger.error(
            "Glue Connection retrieval failed",
            operation="get_glue_connection",
            connection_name=connection_name,
            error_type=type(error).__name__,
            error_message=str(error),
            duration_seconds=round(duration_seconds, 2),
            retry_attempts=retry_attempts,
            status="failed"
        )
    
    def log_connection_validation_start(self, connection_name: str,
                                      validation_type: str) -> None:
        """Log the start of Glue Connection validation.
        
        Args:
            connection_name: Name of the connection being validated
            validation_type: Type of validation being performed
        """
        self.structured_logger.info(
            "Starting Glue Connection validation",
            operation="validate_glue_connection",
            connection_name=connection_name,
            validation_type=validation_type
        )
    
    def log_connection_validation_success(self, connection_name: str,
                                        validation_type: str,
                                        validation_results: Dict[str, Any],
                                        duration_seconds: float) -> None:
        """Log successful Glue Connection validation.
        
        Args:
            connection_name: Name of the validated connection
            validation_type: Type of validation performed
            validation_results: Results of the validation
            duration_seconds: Time taken for validation
        """
        self.structured_logger.info(
            "Glue Connection validation completed successfully",
            operation="validate_glue_connection",
            connection_name=connection_name,
            validation_type=validation_type,
            duration_seconds=round(duration_seconds, 2),
            status="success",
            **validation_results
        )
    
    def log_connection_validation_failure(self, connection_name: str,
                                        validation_type: str,
                                        error: Exception,
                                        duration_seconds: float) -> None:
        """Log failed Glue Connection validation.
        
        Args:
            connection_name: Name of the connection that failed validation
            validation_type: Type of validation that failed
            error: Exception that caused the failure
            duration_seconds: Time spent on validation
        """
        self.structured_logger.error(
            "Glue Connection validation failed",
            operation="validate_glue_connection",
            connection_name=connection_name,
            validation_type=validation_type,
            error_type=type(error).__name__,
            error_message=str(error),
            duration_seconds=round(duration_seconds, 2),
            status="failed"
        )
    
    def log_parameter_validation_start(self, validation_scope: str) -> None:
        """Log the start of parameter validation.
        
        Args:
            validation_scope: Scope of validation ('comprehensive', 'basic', etc.)
        """
        self.structured_logger.info(
            "Starting Glue Connection parameter validation",
            operation="validate_parameters",
            validation_scope=validation_scope
        )
    
    def log_parameter_validation_success(self, validation_results: Dict[str, Any],
                                       duration_seconds: float) -> None:
        """Log successful parameter validation.
        
        Args:
            validation_results: Results of parameter validation
            duration_seconds: Time taken for validation
        """
        source_strategy = None
        target_strategy = None
        
        if validation_results.get('source_config'):
            source_strategy = validation_results['source_config'].get('strategy')
        
        if validation_results.get('target_config'):
            target_strategy = validation_results['target_config'].get('strategy')
        
        self.structured_logger.info(
            "Glue Connection parameter validation completed successfully",
            operation="validate_parameters",
            source_strategy=source_strategy,
            target_strategy=target_strategy,
            warnings_count=len(validation_results.get('validation_warnings', [])),
            errors_count=len(validation_results.get('validation_errors', [])),
            duration_seconds=round(duration_seconds, 2),
            status="success"
        )
        
        # Log individual warnings
        for warning in validation_results.get('validation_warnings', []):
            self.structured_logger.warning(f"Parameter validation warning: {warning}")
    
    def log_parameter_validation_failure(self, error: Exception,
                                       duration_seconds: float) -> None:
        """Log failed parameter validation.
        
        Args:
            error: Exception that caused the failure
            duration_seconds: Time spent on validation
        """
        self.structured_logger.error(
            "Glue Connection parameter validation failed",
            operation="validate_parameters",
            error_type=type(error).__name__,
            error_message=str(error),
            duration_seconds=round(duration_seconds, 2),
            status="failed"
        )
    
    def log_retry_attempt(self, operation: str, connection_name: Optional[str],
                         attempt_number: int, max_attempts: int,
                         error: Exception, retry_delay: float) -> None:
        """Log retry attempt for Glue Connection operation.
        
        Args:
            operation: Name of the operation being retried
            connection_name: Name of the connection (if applicable)
            attempt_number: Current attempt number
            max_attempts: Maximum number of attempts
            error: Exception that caused the retry
            retry_delay: Delay before next retry
        """
        self.structured_logger.warning(
            f"Glue Connection operation failed, retrying in {retry_delay:.1f}s",
            operation=operation,
            connection_name=connection_name,
            attempt_number=attempt_number,
            max_attempts=max_attempts,
            error_type=type(error).__name__,
            error_message=str(error),
            retry_delay=retry_delay
        )
    
    def log_retry_exhausted(self, operation: str, connection_name: Optional[str],
                          total_attempts: int, final_error: Exception,
                          total_duration: float) -> None:
        """Log when all retry attempts are exhausted.
        
        Args:
            operation: Name of the operation that failed
            connection_name: Name of the connection (if applicable)
            total_attempts: Total number of attempts made
            final_error: Final exception that caused failure
            total_duration: Total time spent on all attempts
        """
        self.structured_logger.error(
            f"Glue Connection operation failed after all retry attempts",
            operation=operation,
            connection_name=connection_name,
            total_attempts=total_attempts,
            error_type=type(final_error).__name__,
            error_message=str(final_error),
            total_duration=round(total_duration, 2),
            status="retry_exhausted"
        )
    
    def log_engine_compatibility_warning(self, engine_type: str, 
                                       connection_type: str,
                                       ignored_parameters: list) -> None:
        """Log engine compatibility warnings.
        
        Args:
            engine_type: Engine type that doesn't support Glue Connections
            connection_type: 'source' or 'target'
            ignored_parameters: List of parameters that will be ignored
        """
        self.structured_logger.warning(
            f"Glue Connection parameters ignored for {engine_type} engine",
            engine_type=engine_type,
            connection_type=connection_type,
            ignored_parameters=ignored_parameters,
            reason="Iceberg engines use existing connection mechanism"
        )
    
    def log_connection_strategy_summary(self, source_strategy: Optional[str],
                                      target_strategy: Optional[str],
                                      job_context: Dict[str, Any]) -> None:
        """Log summary of connection strategies for the job.
        
        Args:
            source_strategy: Source connection strategy
            target_strategy: Target connection strategy
            job_context: Additional job context information
        """
        self.structured_logger.info(
            "Glue Connection strategy summary for job execution",
            source_strategy=source_strategy or "direct_jdbc",
            target_strategy=target_strategy or "direct_jdbc",
            job_name=job_context.get('job_name'),
            source_engine=job_context.get('source_engine'),
            target_engine=job_context.get('target_engine'),
            uses_glue_connections=bool(source_strategy or target_strategy)
        )
    
    def log_operation_metrics(self, operation: str, connection_name: Optional[str],
                            success: bool, duration_seconds: float,
                            retry_count: int = 0, error_type: Optional[str] = None) -> None:
        """Log operation metrics for monitoring and analysis.
        
        Args:
            operation: Name of the operation
            connection_name: Name of the connection (if applicable)
            success: Whether the operation succeeded
            duration_seconds: Duration of the operation
            retry_count: Number of retries performed
            error_type: Type of error if operation failed
        """
        self.structured_logger.info(
            "Glue Connection operation metrics",
            operation=operation,
            connection_name=connection_name,
            success=success,
            duration_seconds=round(duration_seconds, 2),
            retry_count=retry_count,
            error_type=error_type,
            metric_type="operation_performance"
        )