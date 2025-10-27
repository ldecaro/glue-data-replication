"""
Custom exception classes for Glue Connection operations.

This module provides specialized exception classes for Glue Connection
error handling, following the same pattern as Iceberg error classes.
"""

from typing import Dict, Any, Optional


class GlueConnectionBaseError(Exception):
    """Base exception for Glue Connection operations.

    This is the base exception class for all Glue Connection-related errors.
    It provides a consistent error handling interface for Glue Connection operations.
    """

    def __init__(self, message: str, connection_name: Optional[str] = None,
                 error_code: Optional[str] = None, context: Optional[Dict[str, Any]] = None):
        super().__init__(message)
        self.message = message
        self.connection_name = connection_name
        self.error_code = error_code
        self.context = context or {}

    def __str__(self) -> str:
        if self.error_code:
            return f"[{self.error_code}] {self.message}"
        return self.message


class GlueConnectionCreationError(GlueConnectionBaseError):
    """Raised when Glue Connection creation fails.

    This exception is raised when there are errors creating a new
    Glue Connection through the AWS Glue API.
    """

    def __init__(self, message: str, connection_name: str, 
                 aws_error_code: Optional[str] = None, 
                 aws_error_message: Optional[str] = None):
        self.aws_error_code = aws_error_code
        self.aws_error_message = aws_error_message

        full_message = f"Failed to create Glue Connection '{connection_name}': {message}"
        if aws_error_message:
            full_message += f" (AWS error: {aws_error_message})"

        super().__init__(
            message=full_message,
            connection_name=connection_name,
            error_code="CONNECTION_CREATION_ERROR",
            context={
                "aws_error_code": aws_error_code,
                "aws_error_message": aws_error_message
            }
        )


class GlueConnectionNotFoundError(GlueConnectionBaseError):
    """Raised when specified Glue Connection does not exist.

    This exception is raised when attempting to use a Glue Connection
    that doesn't exist in the AWS Glue service.
    """

    def __init__(self, connection_name: str):
        message = f"Glue Connection '{connection_name}' does not exist"
        
        super().__init__(
            message=message,
            connection_name=connection_name,
            error_code="CONNECTION_NOT_FOUND",
            context={}
        )


class GlueConnectionValidationError(GlueConnectionBaseError):
    """Raised when Glue Connection configuration validation fails.

    This exception is raised when a Glue Connection exists but has
    invalid or incomplete configuration for the intended use.
    """

    def __init__(self, message: str, connection_name: str, 
                 validation_field: Optional[str] = None,
                 expected_value: Optional[str] = None,
                 actual_value: Optional[str] = None):
        self.validation_field = validation_field
        self.expected_value = expected_value
        self.actual_value = actual_value

        full_message = f"Glue Connection '{connection_name}' validation failed: {message}"
        if validation_field:
            full_message += f" (field: {validation_field})"
        if expected_value and actual_value:
            full_message += f" (expected: {expected_value}, actual: {actual_value})"

        super().__init__(
            message=full_message,
            connection_name=connection_name,
            error_code="CONNECTION_VALIDATION_ERROR",
            context={
                "validation_field": validation_field,
                "expected_value": expected_value,
                "actual_value": actual_value
            }
        )


class GlueConnectionParameterError(GlueConnectionBaseError):
    """Raised when Glue Connection parameters are invalid or conflicting.

    This exception is raised when there are issues with Glue Connection
    parameter combinations or values during job configuration parsing.
    """

    def __init__(self, message: str, parameter_name: Optional[str] = None,
                 parameter_value: Optional[str] = None,
                 conflicting_parameter: Optional[str] = None):
        self.parameter_name = parameter_name
        self.parameter_value = parameter_value
        self.conflicting_parameter = conflicting_parameter

        full_message = f"Glue Connection parameter error: {message}"
        if parameter_name:
            full_message += f" (parameter: {parameter_name})"
        if conflicting_parameter:
            full_message += f" (conflicts with: {conflicting_parameter})"

        super().__init__(
            message=full_message,
            connection_name=None,
            error_code="PARAMETER_ERROR",
            context={
                "parameter_name": parameter_name,
                "parameter_value": parameter_value,
                "conflicting_parameter": conflicting_parameter
            }
        )


class GlueConnectionPermissionError(GlueConnectionBaseError):
    """Raised when there are insufficient permissions for Glue Connection operations.

    This exception is raised when the job execution role lacks the necessary
    IAM permissions to perform Glue Connection operations.
    """

    def __init__(self, message: str, connection_name: Optional[str] = None,
                 operation: Optional[str] = None, 
                 required_permissions: Optional[list] = None):
        self.operation = operation
        self.required_permissions = required_permissions or []

        full_message = f"Insufficient permissions for Glue Connection operation: {message}"
        if operation:
            full_message += f" (operation: {operation})"
        if connection_name:
            full_message += f" (connection: {connection_name})"

        super().__init__(
            message=full_message,
            connection_name=connection_name,
            error_code="PERMISSION_ERROR",
            context={
                "operation": operation,
                "required_permissions": required_permissions
            }
        )


class GlueConnectionNetworkError(GlueConnectionBaseError):
    """Raised when there are network-related issues with Glue Connections.

    This exception is raised when Glue Connection operations fail due to
    network configuration issues, VPC connectivity problems, or ENI creation failures.
    """

    def __init__(self, message: str, connection_name: str,
                 network_component: Optional[str] = None,
                 subnet_id: Optional[str] = None,
                 security_group_ids: Optional[list] = None):
        self.network_component = network_component
        self.subnet_id = subnet_id
        self.security_group_ids = security_group_ids or []

        full_message = f"Network error with Glue Connection '{connection_name}': {message}"
        if network_component:
            full_message += f" (component: {network_component})"
        if subnet_id:
            full_message += f" (subnet: {subnet_id})"

        super().__init__(
            message=full_message,
            connection_name=connection_name,
            error_code="NETWORK_ERROR",
            context={
                "network_component": network_component,
                "subnet_id": subnet_id,
                "security_group_ids": security_group_ids
            }
        )


class GlueConnectionRetryableError(GlueConnectionBaseError):
    """Raised for transient Glue Connection errors that can be retried.

    This exception is raised for temporary failures that may resolve
    with retry attempts, such as service throttling or temporary network issues.
    """

    def __init__(self, message: str, connection_name: Optional[str] = None,
                 retry_after_seconds: Optional[int] = None,
                 max_retries: Optional[int] = None):
        self.retry_after_seconds = retry_after_seconds
        self.max_retries = max_retries

        full_message = f"Retryable Glue Connection error: {message}"
        if retry_after_seconds:
            full_message += f" (retry after: {retry_after_seconds}s)"

        super().__init__(
            message=full_message,
            connection_name=connection_name,
            error_code="RETRYABLE_ERROR",
            context={
                "retry_after_seconds": retry_after_seconds,
                "max_retries": max_retries
            }
        )


class GlueConnectionEngineCompatibilityError(GlueConnectionBaseError):
    """Raised when Glue Connection parameters are used with incompatible engines.

    This exception is raised when Glue Connection parameters are provided
    for engine types that don't support Glue Connections (e.g., Iceberg).
    """

    def __init__(self, message: str, engine_type: str,
                 unsupported_parameters: Optional[list] = None):
        self.engine_type = engine_type
        self.unsupported_parameters = unsupported_parameters or []

        full_message = f"Glue Connection not compatible with engine '{engine_type}': {message}"
        if unsupported_parameters:
            full_message += f" (unsupported parameters: {', '.join(unsupported_parameters)})"

        super().__init__(
            message=full_message,
            connection_name=None,
            error_code="ENGINE_COMPATIBILITY_ERROR",
            context={
                "engine_type": engine_type,
                "unsupported_parameters": unsupported_parameters
            }
        )