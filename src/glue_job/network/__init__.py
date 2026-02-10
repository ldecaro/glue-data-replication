"""
Network and error handling modules for AWS Glue Data Replication.

This package contains modules for handling network connectivity issues,
error classification, retry logic, and recovery strategies.
"""

from .error_handler import (
    ErrorCategory,
    NetworkConnectivityError,
    GlueConnectionError,
    VpcEndpointError,
    ENICreationError,
    NetworkErrorHandler
)

from .retry_handler import (
    ErrorClassifier,
    ConnectionRetryHandler,
    ErrorRecoveryManager
)

from .kerberos_error_handler import (
    KerberosErrorHandler,
    KerberosErrorCategory
)

__all__ = [
    'ErrorCategory',
    'NetworkConnectivityError',
    'GlueConnectionError',
    'VpcEndpointError',
    'ENICreationError',
    'NetworkErrorHandler',
    'ErrorClassifier',
    'ConnectionRetryHandler',
    'ErrorRecoveryManager',
    'KerberosErrorHandler',
    'KerberosErrorCategory'
]