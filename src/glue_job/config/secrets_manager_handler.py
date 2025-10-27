"""
AWS Secrets Manager integration for secure credential storage.

This module provides the SecretsManagerHandler class for managing database
credentials in AWS Secrets Manager, following the same patterns as other
AWS service integrations in the project.
"""

import json
import logging
import time
from datetime import datetime, timezone
from typing import Dict, Any, Optional, Tuple
from ..monitoring.logging import StructuredLogger
from ..network.retry_handler import ConnectionRetryHandler

# Conditional import for AWS SDK
try:
    import boto3
    from botocore.exceptions import ClientError, BotoCoreError
    AWS_SDK_AVAILABLE = True
except ImportError:
    # Mock for local development/testing
    boto3 = None
    ClientError = Exception
    BotoCoreError = Exception
    AWS_SDK_AVAILABLE = False

logger = logging.getLogger(__name__)


class SecretsManagerError(Exception):
    """Base class for Secrets Manager related errors."""
    
    def __init__(self, message: str, secret_name: Optional[str] = None,
                 error_code: Optional[str] = None, context: Optional[Dict[str, Any]] = None):
        super().__init__(message)
        self.message = message
        self.secret_name = secret_name
        self.error_code = error_code
        self.context = context or {}

    def __str__(self) -> str:
        if self.error_code:
            return f"[{self.error_code}] {self.message}"
        return self.message


class SecretCreationError(SecretsManagerError):
    """Raised when secret creation fails."""
    
    def __init__(self, message: str, secret_name: str, 
                 aws_error_code: Optional[str] = None, 
                 aws_error_message: Optional[str] = None):
        self.aws_error_code = aws_error_code
        self.aws_error_message = aws_error_message

        full_message = f"Failed to create secret '{secret_name}': {message}"
        if aws_error_message:
            full_message += f" (AWS error: {aws_error_message})"

        super().__init__(
            message=full_message,
            secret_name=secret_name,
            error_code="SECRET_CREATION_ERROR",
            context={
                "aws_error_code": aws_error_code,
                "aws_error_message": aws_error_message
            }
        )


class SecretsManagerPermissionError(SecretsManagerError):
    """Raised when there are insufficient permissions for Secrets Manager operations."""
    
    def __init__(self, message: str, secret_name: Optional[str] = None,
                 operation: Optional[str] = None, 
                 required_permissions: Optional[list] = None):
        self.operation = operation
        self.required_permissions = required_permissions or []

        full_message = f"Insufficient permissions for Secrets Manager operation: {message}"
        if operation:
            full_message += f" (operation: {operation})"
        if secret_name:
            full_message += f" (secret: {secret_name})"

        super().__init__(
            message=full_message,
            secret_name=secret_name,
            error_code="PERMISSION_ERROR",
            context={
                "operation": operation,
                "required_permissions": required_permissions
            }
        )


class SecretsManagerRetryableError(SecretsManagerError):
    """Raised for transient Secrets Manager errors that can be retried."""
    
    def __init__(self, message: str, secret_name: Optional[str] = None,
                 retry_after_seconds: Optional[int] = None,
                 max_retries: Optional[int] = None):
        self.retry_after_seconds = retry_after_seconds
        self.max_retries = max_retries

        full_message = f"Retryable Secrets Manager error: {message}"
        if retry_after_seconds:
            full_message += f" (retry after: {retry_after_seconds}s)"

        super().__init__(
            message=full_message,
            secret_name=secret_name,
            error_code="RETRYABLE_ERROR",
            context={
                "retry_after_seconds": retry_after_seconds,
                "max_retries": max_retries
            }
        )


class SecretsManagerHandler:
    """Handler for AWS Secrets Manager operations for database credentials.
    
    This class provides secure credential storage and retrieval for Glue Connections,
    following AWS security best practices and the project's error handling patterns.
    """
    
    # Secret name prefix for Glue connections
    SECRET_NAME_PREFIX = "/aws-glue/"
    
    # Standard secret structure keys
    SECRET_KEYS = {
        'username': 'username',
        'password': 'password'
    }
    
    # Required IAM permissions for Secrets Manager operations
    REQUIRED_PERMISSIONS = [
        'secretsmanager:CreateSecret',
        'secretsmanager:GetSecretValue',
        'secretsmanager:DescribeSecret',
        'secretsmanager:TagResource'
    ]
    
    def __init__(self, region_name: str = None, job_name: str = "glue-data-replication"):
        """Initialize Secrets Manager handler.
        
        Args:
            region_name: AWS region name. If None, uses default region
            job_name: Job name for logging context
        """
        self.region_name = region_name
        self.job_name = job_name
        self.structured_logger = StructuredLogger(f"SecretsManagerHandler-{job_name}")
        self.retry_handler = ConnectionRetryHandler(
            max_retries=3,
            base_delay=2.0,
            max_delay=30.0,
            backoff_factor=2.0
        )
        
        # Initialize AWS client
        self._client = None
        self._initialize_client()
    
    def _initialize_client(self) -> None:
        """Initialize AWS Secrets Manager client with error handling."""
        try:
            if not AWS_SDK_AVAILABLE:
                raise SecretsManagerError(
                    "AWS SDK (boto3) is not available. Cannot initialize Secrets Manager client.",
                    error_code="SDK_NOT_AVAILABLE"
                )
            
            self.structured_logger.info(
                "Initializing AWS Secrets Manager client",
                region_name=self.region_name or "default"
            )
            
            if self.region_name:
                self._client = boto3.client('secretsmanager', region_name=self.region_name)
            else:
                self._client = boto3.client('secretsmanager')
            
            self.structured_logger.info("AWS Secrets Manager client initialized successfully")
            
        except Exception as e:
            self.structured_logger.error(
                "Failed to initialize AWS Secrets Manager client",
                error=str(e)
            )
            raise SecretsManagerError(
                f"Failed to initialize Secrets Manager client: {str(e)}",
                error_code="CLIENT_INITIALIZATION_ERROR"
            )
    
    def create_secret(self, connection_name: str, username: str, password: str,
                     description: Optional[str] = None, tags: Optional[Dict[str, str]] = None) -> str:
        """Create a new secret with database credentials.
        
        Args:
            connection_name: Name of the Glue connection (used to generate secret name)
            username: Database username
            password: Database password
            description: Optional description for the secret
            tags: Optional tags for the secret
            
        Returns:
            Secret ARN for Glue Connection reference
            
        Raises:
            SecretCreationError: If secret creation fails
            SecretsManagerPermissionError: If insufficient permissions
            SecretsManagerRetryableError: For transient failures
        """
        secret_name = self.generate_secret_name(connection_name)
        
        self.structured_logger.info(
            "Starting secret creation for Glue Connection",
            connection_name=connection_name,
            secret_name=secret_name,
            has_username=bool(username),
            has_password=bool(password)
        )
        
        # Validate inputs
        self._validate_secret_inputs(connection_name, username, password)
        
        # Prepare secret value
        secret_value = {
            self.SECRET_KEYS['username']: username,
            self.SECRET_KEYS['password']: password
        }
        
        # Prepare tags
        default_tags = {
            'CreatedBy': 'AWS-Glue-Data-Replication',
            'JobName': self.job_name,
            'ConnectionName': connection_name,
            'CreatedAt': datetime.now(timezone.utc).isoformat()
        }
        if tags:
            default_tags.update(tags)
        
        try:
            # Create secret directly without retry handler to avoid double exception wrapping
            secret_arn = self._create_secret_with_client(
                secret_name=secret_name,
                secret_value=secret_value,
                description=description or f"Database credentials for Glue Connection '{connection_name}'",
                tags=default_tags
            )
            
            self.structured_logger.info(
                "Secret created successfully for Glue Connection",
                connection_name=connection_name,
                secret_name=secret_name,
                secret_arn=secret_arn
            )
            
            return secret_arn
            
        except (SecretCreationError, SecretsManagerPermissionError, SecretsManagerRetryableError) as e:
            # Re-raise our custom exceptions as-is
            self.structured_logger.error(
                "Failed to create secret for Glue Connection",
                connection_name=connection_name,
                secret_name=secret_name,
                error=str(e)
            )
            raise
        except Exception as e:
            self.structured_logger.error(
                "Failed to create secret for Glue Connection",
                connection_name=connection_name,
                secret_name=secret_name,
                error=str(e)
            )
            
            raise SecretCreationError(
                f"Unexpected error during secret creation: {str(e)}",
                secret_name
            )
    
    def _create_secret_with_client(self, secret_name: str, secret_value: Dict[str, str],
                                  description: str, tags: Dict[str, str]) -> str:
        """Create secret using AWS client with proper error handling.
        
        Args:
            secret_name: Name of the secret
            secret_value: Secret value dictionary
            description: Secret description
            tags: Secret tags
            
        Returns:
            Secret ARN
            
        Raises:
            SecretCreationError: If creation fails
            SecretsManagerPermissionError: If insufficient permissions
            SecretsManagerRetryableError: For transient failures
        """
        try:
            response = self._client.create_secret(
                Name=secret_name,
                Description=description,
                SecretString=json.dumps(secret_value),
                Tags=[{'Key': k, 'Value': v} for k, v in tags.items()]
            )
            
            return response['ARN']
            
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', 'Unknown')
            error_message = e.response.get('Error', {}).get('Message', str(e))
            
            self.structured_logger.error(
                "AWS Secrets Manager API error during secret creation",
                secret_name=secret_name,
                aws_error_code=error_code,
                aws_error_message=error_message
            )
            
            # Handle specific AWS error codes
            if error_code == 'ResourceExistsException':
                error = SecretCreationError(
                    f"Secret already exists: {error_message}",
                    secret_name,
                    error_code,
                    error_message
                )
                raise error
            elif error_code in ['AccessDeniedException', 'UnauthorizedOperation']:
                error = SecretsManagerPermissionError(
                    f"Insufficient permissions to create secret: {error_message}",
                    secret_name,
                    "create_secret",
                    self.REQUIRED_PERMISSIONS
                )
                raise error
            elif error_code in ['ThrottlingException', 'ServiceUnavailableException']:
                error = SecretsManagerRetryableError(
                    f"Transient error creating secret: {error_message}",
                    secret_name,
                    retry_after_seconds=5
                )
                raise error
            else:
                error = SecretCreationError(
                    f"AWS error creating secret: {error_message}",
                    secret_name,
                    error_code,
                    error_message
                )
                raise error
                
        except BotoCoreError as e:
            self.structured_logger.error(
                "BotoCore error during secret creation",
                secret_name=secret_name,
                error=str(e)
            )
            error = SecretsManagerRetryableError(
                f"Network or client error creating secret: {str(e)}",
                secret_name,
                retry_after_seconds=10
            )
            raise error
        except Exception as e:
            self.structured_logger.error(
                "Unexpected error during secret creation",
                secret_name=secret_name,
                error=str(e)
            )
            error = SecretCreationError(
                f"Unexpected error creating secret: {str(e)}",
                secret_name
            )
            raise error
    
    def validate_secret_permissions(self) -> bool:
        """Validate IAM permissions for Secrets Manager operations.
        
        Returns:
            True if permissions are valid, False otherwise
            
        Raises:
            SecretsManagerPermissionError: If validation fails with permission errors
        """
        self.structured_logger.info("Starting Secrets Manager permissions validation")
        
        try:
            # Test permissions directly without retry handler
            test_result = self._test_permissions_with_client()
            
            if test_result:
                self.structured_logger.info("Secrets Manager permissions validation successful")
                return True
            else:
                self.structured_logger.error("Secrets Manager permissions validation failed")
                return False
                
        except SecretsManagerPermissionError:
            raise
        except Exception as e:
            self.structured_logger.error(
                "Unexpected error during permissions validation",
                error=str(e)
            )
            raise SecretsManagerPermissionError(
                f"Failed to validate permissions: {str(e)}",
                operation="validate_permissions",
                required_permissions=self.REQUIRED_PERMISSIONS
            )
    
    def _test_permissions_with_client(self) -> bool:
        """Test permissions using AWS client.
        
        Returns:
            True if permissions are valid
            
        Raises:
            SecretsManagerPermissionError: If insufficient permissions
        """
        try:
            # Test basic list operation (minimal permissions required)
            self._client.list_secrets(MaxResults=1)
            return True
            
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', 'Unknown')
            error_message = e.response.get('Error', {}).get('Message', str(e))
            
            if error_code in ['AccessDeniedException', 'UnauthorizedOperation']:
                error = SecretsManagerPermissionError(
                    f"Insufficient permissions for Secrets Manager operations: {error_message}",
                    operation="list_secrets",
                    required_permissions=self.REQUIRED_PERMISSIONS
                )
                raise error
            else:
                # Other errors don't necessarily indicate permission issues
                self.structured_logger.warning(
                    "Non-permission error during validation",
                    aws_error_code=error_code,
                    aws_error_message=error_message
                )
                return True
                
        except Exception as e:
            self.structured_logger.warning(
                "Unexpected error during permission test",
                error=str(e)
            )
            # Don't fail validation for unexpected errors
            return True
    
    def generate_secret_name(self, connection_name: str) -> str:
        """Generate standardized secret name for Glue Connection.
        
        Args:
            connection_name: Name of the Glue connection
            
        Returns:
            Standardized secret name with /aws-glue/ prefix
        """
        # Clean connection name for use in secret name
        import re
        clean_connection_name = re.sub(r'[^a-zA-Z0-9_-]', '-', connection_name)
        
        # Generate secret name with prefix
        secret_name = f"{self.SECRET_NAME_PREFIX}{clean_connection_name}"
        
        self.structured_logger.debug(
            "Generated secret name for connection",
            connection_name=connection_name,
            secret_name=secret_name
        )
        
        return secret_name
    
    def _validate_secret_inputs(self, connection_name: str, username: str, password: str) -> None:
        """Validate inputs for secret creation.
        
        Args:
            connection_name: Name of the Glue connection
            username: Database username
            password: Database password
            
        Raises:
            SecretsManagerError: If inputs are invalid
        """
        if not connection_name or not connection_name.strip():
            raise SecretsManagerError(
                "Connection name cannot be empty",
                error_code="INVALID_INPUT"
            )
        
        if not username or not username.strip():
            raise SecretsManagerError(
                "Username cannot be empty",
                secret_name=self.generate_secret_name(connection_name),
                error_code="INVALID_INPUT"
            )
        
        if not password:
            raise SecretsManagerError(
                "Password cannot be empty",
                secret_name=self.generate_secret_name(connection_name),
                error_code="INVALID_INPUT"
            )
        
        # Validate connection name format
        import re
        if not re.match(r'^[a-zA-Z0-9_-]+$', connection_name):
            raise SecretsManagerError(
                "Connection name must contain only alphanumeric characters, hyphens, and underscores",
                secret_name=self.generate_secret_name(connection_name),
                error_code="INVALID_INPUT"
            )
    
    def get_secret_arn_for_connection(self, connection_name: str) -> str:
        """Get the secret ARN that would be used for a given connection name.
        
        This is a utility method to get the ARN format without creating the secret.
        
        Args:
            connection_name: Name of the Glue connection
            
        Returns:
            Expected secret ARN format
        """
        secret_name = self.generate_secret_name(connection_name)
        
        # Generate ARN format (actual ARN would be returned by AWS after creation)
        # This is for reference/logging purposes
        region = self.region_name or "us-east-1"  # Default region
        account_id = "123456789012"  # Placeholder - actual account ID would be determined at runtime
        
        arn = f"arn:aws:secretsmanager:{region}:{account_id}:secret:{secret_name}"
        
        return arn
    
    def cleanup_secret_on_failure(self, connection_name: str) -> bool:
        """Clean up secret if Glue Connection creation fails.
        
        This method attempts to delete a secret that was created but is no longer
        needed due to Glue Connection creation failure.
        
        Args:
            connection_name: Name of the Glue connection
            
        Returns:
            True if cleanup was successful or secret didn't exist, False otherwise
        """
        secret_name = self.generate_secret_name(connection_name)
        
        self.structured_logger.info(
            "Starting secret cleanup after Glue Connection failure",
            connection_name=connection_name,
            secret_name=secret_name
        )
        
        try:
            # Attempt to delete the secret
            self._client.delete_secret(
                SecretId=secret_name,
                ForceDeleteWithoutRecovery=True  # Immediate deletion for cleanup
            )
            
            self.structured_logger.info(
                "Secret cleaned up successfully after Glue Connection failure",
                connection_name=connection_name,
                secret_name=secret_name
            )
            return True
            
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', 'Unknown')
            
            if error_code == 'ResourceNotFoundException':
                # Secret doesn't exist, cleanup is successful
                self.structured_logger.info(
                    "Secret does not exist, cleanup not needed",
                    connection_name=connection_name,
                    secret_name=secret_name
                )
                return True
            else:
                self.structured_logger.warning(
                    "Failed to cleanup secret after Glue Connection failure",
                    connection_name=connection_name,
                    secret_name=secret_name,
                    aws_error_code=error_code,
                    error=str(e)
                )
                return False
                
        except Exception as e:
            self.structured_logger.warning(
                "Unexpected error during secret cleanup",
                connection_name=connection_name,
                secret_name=secret_name,
                error=str(e)
            )
            # Return False for any unexpected errors during cleanup
            return False