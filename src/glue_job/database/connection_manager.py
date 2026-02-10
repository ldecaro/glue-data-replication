"""
Database connection management for AWS Glue Data Replication.

This module provides JDBC and Glue connection management capabilities
for cross-VPC database access and connection validation.
"""

import time
import boto3
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass
from botocore.config import Config
from botocore.exceptions import ClientError
# Conditional imports for PySpark and AWS Glue (only available in Glue runtime)
try:
    from pyspark.sql import SparkSession, DataFrame
    from pyspark.sql.types import StructType
    from awsglue.context import GlueContext
except ImportError:
    # Mock classes for local development/testing
    class SparkSession:
        pass
    class DataFrame:
        pass
    class StructType:
        pass
    class GlueContext:
        pass

# Import from other modules
from ..config.job_config import ConnectionConfig
from ..config.database_engines import DatabaseEngineManager
from ..config.secrets_manager_handler import (
    SecretsManagerHandler, SecretsManagerError, SecretCreationError, 
    SecretsManagerPermissionError, SecretsManagerRetryableError
)
from ..monitoring.logging import StructuredLogger
from ..network.error_handler import (
    NetworkConnectivityError, GlueConnectionError, VpcEndpointError, 
    ENICreationError, ErrorCategory
)
from ..network.retry_handler import ConnectionRetryHandler, ErrorClassifier
from ..network.glue_connection_errors import (
    GlueConnectionBaseError,
    GlueConnectionCreationError,
    GlueConnectionNotFoundError,
    GlueConnectionValidationError,
    GlueConnectionParameterError,
    GlueConnectionPermissionError,
    GlueConnectionNetworkError,
    GlueConnectionRetryableError
)
from ..network.glue_connection_retry_handler import GlueConnectionRetryHandler
from ..config.iceberg_connection_handler import IcebergConnectionHandler
from ..config.iceberg_models import (
    IcebergConfig, IcebergEngineError, IcebergConnectionError, IcebergValidationError
)


class GlueConnectionManager:
    """Manages Glue connections for cross-VPC database access."""
    
    def __init__(self, glue_context: GlueContext, job_name: Optional[str] = None):
        self.glue_context = glue_context
        self.job_name = job_name  # Used for connection naming: {job_name}-{source|target}
        # Configure Glue client with aggressive timeout settings
        config = Config(
            read_timeout=30,  # Reduced from 60 to 30 seconds
            connect_timeout=10,  # Reduced from 30 to 10 seconds
            retries={'max_attempts': 2}  # Reduced from 3 to 2 attempts
        )
        self.glue_client = boto3.client('glue', config=config)
        self.structured_logger = StructuredLogger("GlueConnectionManager")
        
        # Initialize specialized retry handler for Glue Connection operations
        self.retry_handler = GlueConnectionRetryHandler(
            max_retries=3,
            base_delay=1.0,
            max_delay=30.0,
            backoff_factor=2.0
        )
        
        # Initialize Secrets Manager handler for credential storage
        self.secrets_manager_handler = SecretsManagerHandler(
            region_name=None,  # Use default region
            job_name=job_name or "glue-data-replication"
        )
    
    def get_glue_connection(self, connection_name: str) -> Optional[Dict[str, Any]]:
        """Retrieve Glue connection details for cross-VPC database access with enhanced error handling.
        
        Args:
            connection_name: Name of the Glue connection
            
        Returns:
            Dictionary containing connection details or None if not found
            
        Raises:
            GlueConnectionError: For Glue connection specific issues
        """
        if not connection_name or connection_name.strip() == '':
            self.structured_logger.debug("No Glue connection name provided")
            return None
        
        try:
            self.structured_logger.info("Retrieving Glue connection with retry logic", connection_name=connection_name)
            
            # Use retry handler for connection retrieval
            response = self.retry_handler.get_connection_with_retry(
                self.glue_client, 
                connection_name
            )
            connection = response.get('Connection', {})
            
            connection_details = {
                'name': connection.get('Name'),
                'connection_type': connection.get('ConnectionType'),
                'connection_properties': connection.get('ConnectionProperties', {}),
                'physical_connection_requirements': connection.get('PhysicalConnectionRequirements', {})
            }
            
            # Validate connection configuration with retry logic
            # The validation function expects (connection_details, connection_name)
            # We pass them as positional args after the connection_name parameter
            self.retry_handler.validate_connection_with_retry(
                self._validate_glue_connection_config,
                connection_name,
                connection_details
            )
            
            self.structured_logger.info(
                "Successfully retrieved and validated Glue connection",
                connection_name=connection_name,
                connection_type=connection_details['connection_type']
            )
            
            return connection_details
            
        except GlueConnectionBaseError:
            # Re-raise Glue Connection specific errors
            raise
        except Exception as e:
            self.structured_logger.error(
                "Failed to retrieve Glue connection",
                connection_name=connection_name,
                error=str(e),
                error_type=type(e).__name__
            )
            # Convert to appropriate Glue Connection error
            if "EntityNotFoundException" in str(e):
                raise GlueConnectionNotFoundError(connection_name)
            elif "AccessDenied" in str(e) or "permission" in str(e).lower():
                raise GlueConnectionPermissionError(
                    f"Access denied retrieving connection: {str(e)}",
                    connection_name=connection_name,
                    operation="get_connection"
                )
            else:
                raise GlueConnectionBaseError(
                    f"Failed to retrieve Glue connection '{connection_name}': {str(e)}",
                    connection_name=connection_name,
                    error_code="CONNECTION_RETRIEVAL_ERROR",
                    context={'original_error': str(e)}
                )
    
    def _validate_glue_connection_config(self, connection_details: Dict[str, Any], connection_name: str):
        """Validate Glue connection configuration with enhanced error handling.
        
        Note: Physical connection requirements are only required for cross-VPC access.
        Connections without physical requirements can still be valid for same-VPC scenarios.
        """
        connection_type = connection_details.get('connection_type')
        if connection_type not in ['JDBC', 'NETWORK']:
            raise GlueConnectionValidationError(
                f"Connection type '{connection_type}' is not supported (must be JDBC or NETWORK)",
                connection_name=connection_name,
                validation_field='connection_type',
                expected_value='JDBC or NETWORK',
                actual_value=connection_type
            )
        
        physical_reqs = connection_details.get('physical_connection_requirements', {})
        
        # Physical connection requirements are optional - only needed for cross-VPC access
        # Log a warning if not present, but don't fail validation
        if not physical_reqs:
            self.structured_logger.info(
                "Glue Connection has no physical connection requirements - using for same-VPC access",
                connection_name=connection_name,
                connection_type=connection_type
            )
            return  # Valid connection without cross-VPC requirements
        
        # If physical requirements are present, validate them
        subnet_id = physical_reqs.get('SubnetId')
        security_groups = physical_reqs.get('SecurityGroupIdList', [])
        
        if not subnet_id:
            raise GlueConnectionNetworkError(
                "Missing subnet configuration in physical connection requirements",
                connection_name=connection_name,
                network_component='subnet'
            )
        
        if not security_groups:
            raise GlueConnectionNetworkError(
                "Missing security group configuration in physical connection requirements",
                connection_name=connection_name,
                network_component='security_groups'
            )
        
        self.structured_logger.debug(
            "Glue connection configuration validation passed",
            connection_name=connection_name,
            connection_type=connection_type,
            subnet_id=subnet_id,
            security_groups_count=len(security_groups)
        )
    
    def validate_network_connectivity(self, connection_name: str, 
                                    connection_string: str, 
                                    timeout_seconds: int = 30) -> bool:
        """Test database connectivity before processing with enhanced error handling.
        
        Args:
            connection_name: Name of the Glue connection (empty for same-VPC)
            connection_string: JDBC connection string
            timeout_seconds: Connection timeout in seconds
            
        Returns:
            True if connectivity test passes, False otherwise
            
        Raises:
            NetworkConnectivityError: For network connectivity issues
            GlueConnectionError: For Glue connection specific issues
            VpcEndpointError: For VPC endpoint issues
        """
        try:
            # Check for environment variable to skip Glue connection validation
            import os
            skip_glue_validation = os.environ.get('SKIP_GLUE_CONNECTION_VALIDATION', 'false').lower() == 'true'
            
            if skip_glue_validation:
                self.structured_logger.warning(
                    "Skipping Glue connection validation due to environment variable SKIP_GLUE_CONNECTION_VALIDATION=true",
                    connection_name=connection_name or "same-vpc"
                )
                return True
            
            self.structured_logger.info(
                "Starting network connectivity validation",
                connection_name=connection_name or "same-vpc",
                timeout_seconds=timeout_seconds
            )
            
            start_time = time.time()
            
            # If no Glue connection name, assume same-VPC connectivity
            if not connection_name or connection_name.strip() == '':
                self.structured_logger.info("Using same-VPC connectivity (no Glue connection)")
                # For same-VPC, we'll validate during actual connection attempt
                return True
            
            # Retrieve Glue connection details with error handling and fallback
            try:
                self.structured_logger.info("Attempting to retrieve Glue connection for validation", connection_name=connection_name)
                connection_details = self.get_glue_connection(connection_name)
                if not connection_details:
                    self.structured_logger.warning(
                        "Glue connection validation failed - connection not found, will attempt direct connection",
                        connection_name=connection_name
                    )
                    # Return True to allow the job to continue and try direct connection
                    return True
            except Exception as conn_error:
                self.structured_logger.warning(
                    "Glue connection validation failed - will attempt direct connection instead",
                    connection_name=connection_name,
                    error=str(conn_error),
                    error_type=type(conn_error).__name__
                )
                
                # Log specific error types for debugging
                if "EntityNotFoundException" in str(conn_error):
                    self.structured_logger.error(
                        "Glue connection does not exist - please verify the connection name and ensure it's created",
                        connection_name=connection_name
                    )
                elif "AccessDenied" in str(conn_error) or "permission" in str(conn_error).lower():
                    self.structured_logger.error(
                        "Permission denied accessing Glue connection - please verify IAM permissions",
                        connection_name=connection_name
                    )
                elif "timeout" in str(conn_error).lower():
                    self.structured_logger.error(
                        "Timeout accessing Glue connection - may indicate network or service issues",
                        connection_name=connection_name
                    )
                
                # Instead of failing, return True to allow the job to continue
                # The actual database connection will be tested later
                self.structured_logger.info(
                    "Skipping Glue connection validation - will test actual database connectivity instead",
                    connection_name=connection_name
                )
                return True
            
            # Validate connection properties
            connection_properties = connection_details.get('connection_properties', {})
            physical_requirements = connection_details.get('physical_connection_requirements', {})
            
            # Check if connection has network configuration
            # Note: Physical requirements are optional for same-VPC scenarios
            subnet_id = physical_requirements.get('SubnetId')
            security_groups = physical_requirements.get('SecurityGroupIdList', [])
            
            if not physical_requirements:
                # No physical requirements - connection is for same-VPC access
                self.structured_logger.info(
                    "Glue connection has no physical requirements - valid for same-VPC access",
                    connection_name=connection_name
                )
            elif not subnet_id or not security_groups:
                # Partial configuration is invalid
                if not subnet_id:
                    raise GlueConnectionError(
                        f"Glue connection '{connection_name}' has incomplete network config (missing subnet)",
                        connection_name,
                        {'error_type': 'incomplete_network_config', 'physical_requirements': physical_requirements}
                    )
                
                if not security_groups:
                    raise GlueConnectionError(
                        f"Glue connection '{connection_name}' has incomplete network config (missing security groups)",
                        connection_name,
                        {'error_type': 'incomplete_network_config', 'physical_requirements': physical_requirements}
                    )
            else:
                # Full network configuration present - perform detailed validation
                self._validate_subnet_accessibility(subnet_id, connection_name)
                self._validate_security_group_rules(security_groups, connection_name)
            
            # Validate that connection URL matches expected format
            stored_url = connection_properties.get('JDBC_CONNECTION_URL', '')
            if stored_url and stored_url != connection_string:
                self.structured_logger.warning(
                    "Connection string mismatch between parameter and Glue connection",
                    connection_name=connection_name,
                    parameter_url=connection_string[:50] + "...",
                    stored_url=stored_url[:50] + "..."
                )
            
            duration = time.time() - start_time
            self.structured_logger.info(
                "Network connectivity validation completed successfully",
                connection_name=connection_name,
                duration_seconds=round(duration, 2),
                subnet_id=subnet_id,
                security_groups_count=len(security_groups)
            )
            
            return True
            
        except (NetworkConnectivityError, GlueConnectionError, VpcEndpointError) as network_error:
            duration = time.time() - start_time
            self.structured_logger.error(
                "Network connectivity validation failed with network error",
                connection_name=connection_name or "same-vpc",
                error_type=type(network_error).__name__,
                error_message=str(network_error),
                duration_seconds=round(duration, 2)
            )
            raise network_error
            
        except Exception as e:
            duration = time.time() - start_time
            self.structured_logger.error(
                "Network connectivity validation failed with unexpected error",
                connection_name=connection_name or "same-vpc",
                error=str(e),
                duration_seconds=round(duration, 2)
            )
            raise NetworkConnectivityError(
                f"Network connectivity validation failed: {str(e)}",
                error_type='validation_error',
                connection_name=connection_name
            )
    
    def _validate_subnet_accessibility(self, subnet_id: str, connection_name: str):
        """Validate subnet accessibility for Glue connection."""
        try:
            if not hasattr(self, 'ec2_client'):
                self.ec2_client = boto3.client('ec2')
            
            response = self.ec2_client.describe_subnets(SubnetIds=[subnet_id])
            subnet = response['Subnets'][0]
            
            # Check subnet state
            if subnet['State'] != 'available':
                raise NetworkConnectivityError(
                    f"Subnet {subnet_id} for Glue connection '{connection_name}' is in '{subnet['State']}' state, not 'available'",
                    error_type='subnet_unavailable',
                    connection_name=connection_name
                )
            
            # Check available IP addresses
            available_ips = subnet.get('AvailableIpAddressCount', 0)
            if available_ips < 2:
                raise ENICreationError(
                    f"Subnet {subnet_id} has insufficient IP addresses ({available_ips} available) for ENI creation",
                    subnet_id=subnet_id,
                    error_code='insufficient_ips'
                )
            
            self.structured_logger.debug(
                "Subnet accessibility validation passed",
                subnet_id=subnet_id,
                subnet_state=subnet['State'],
                available_ips=available_ips
            )
            
        except (NetworkConnectivityError, ENICreationError):
            raise
        except Exception as e:
            raise NetworkConnectivityError(
                f"Failed to validate subnet {subnet_id} accessibility: {str(e)}",
                error_type='subnet_validation_error',
                connection_name=connection_name
            )
    
    def _validate_security_group_rules(self, security_group_ids: List[str], connection_name: str):
        """Validate security group rules for database connectivity."""
        try:
            if not hasattr(self, 'ec2_client'):
                self.ec2_client = boto3.client('ec2')
            
            response = self.ec2_client.describe_security_groups(GroupIds=security_group_ids)
            
            for sg in response['SecurityGroups']:
                sg_id = sg['GroupId']
                
                # Check outbound rules for database ports
                outbound_rules = sg.get('IpPermissionsEgress', [])
                has_database_outbound = any(
                    self._rule_allows_database_ports(rule) for rule in outbound_rules
                )
                
                if not has_database_outbound:
                    self.structured_logger.warning(
                        f"Security group {sg_id} may lack outbound rules for database ports",
                        connection_name=connection_name,
                        security_group_id=sg_id
                    )
                    # Don't fail validation, just warn
            
            self.structured_logger.debug(
                "Security group validation completed",
                connection_name=connection_name,
                security_groups=security_group_ids
            )
            
        except Exception as e:
            self.structured_logger.warning(
                "Failed to validate security group rules",
                connection_name=connection_name,
                security_groups=security_group_ids,
                error=str(e)
            )
            # Don't fail validation for security group rule issues
    
    def _rule_allows_database_ports(self, rule: Dict[str, Any]) -> bool:
        """Check if security group rule allows common database ports."""
        from_port = rule.get('FromPort')
        to_port = rule.get('ToPort')
        
        if from_port is None or to_port is None:
            return False
        
        # Check for rules that allow all traffic (common in outbound rules)
        if from_port == 0 and to_port == 65535:
            return True
        
        # Check for rules that allow all traffic on all protocols (FromPort = -1)
        if from_port == -1:
            return True
        
        database_ports = [1433, 1521, 5432, 50000]  # SQL Server, Oracle, PostgreSQL, DB2
        
        return any(
            from_port <= port <= to_port for port in database_ports
        )
    
    def create_glue_connection(self, connection_config: ConnectionConfig, 
                             connection_name: str) -> str:
        """Create a new Glue Connection from JDBC parameters with Secrets Manager integration.
        
        Args:
            connection_config: Database connection configuration containing JDBC parameters
            connection_name: Name for the new Glue Connection
            
        Returns:
            str: Name of the created Glue Connection
            
        Raises:
            GlueConnectionError: For Glue Connection creation failures
            SecretCreationError: For Secrets Manager secret creation failures
            ValueError: For invalid parameters
        """
        secret_arn = None
        try:
            self.structured_logger.info(
                "Creating new Glue Connection with Secrets Manager integration",
                connection_name=connection_name,
                engine_type=connection_config.engine_type
            )
            
            # Validate required parameters for Glue Connection creation
            self._validate_glue_connection_creation_params(connection_config, connection_name)
            
            # Create AWS Secrets Manager secret for database credentials
            secret_arn = self._create_secrets_manager_secret(connection_config, connection_name)
            
            # Build Glue Connection input with secret reference
            connection_input = self._build_glue_connection_input_with_secrets(
                connection_config, connection_name, secret_arn
            )
            
            # Create the Glue Connection with retry logic
            response = self.retry_handler.create_connection_with_retry(
                self.glue_client,
                connection_input,
                connection_name
            )
            
            self.structured_logger.info(
                "Successfully created Glue Connection with Secrets Manager integration",
                connection_name=connection_name,
                engine_type=connection_config.engine_type,
                secret_arn=secret_arn
            )
            
            return connection_name
                    
        except (SecretCreationError, SecretsManagerPermissionError, SecretsManagerRetryableError):
            # Re-raise Secrets Manager specific errors without cleanup (secret wasn't created)
            raise
        except GlueConnectionParameterError:
            # Re-raise parameter validation errors without cleanup (secret wasn't created)
            raise
        except GlueConnectionBaseError:
            # Clean up secret if Glue Connection creation failed
            if secret_arn:
                self._cleanup_secret_on_failure(connection_name)
            raise
        except Exception as e:
            # Clean up secret if Glue Connection creation failed
            if secret_arn:
                self._cleanup_secret_on_failure(connection_name)
            
            self.structured_logger.error(
                "Unexpected error during Glue Connection creation with Secrets Manager",
                connection_name=connection_name,
                error=str(e),
                error_type=type(e).__name__,
                secret_arn=secret_arn
            )
            raise GlueConnectionCreationError(
                f"Unexpected error during creation: {str(e)}",
                connection_name=connection_name
            )
    
    def _validate_glue_connection_creation_params(self, connection_config: ConnectionConfig, 
                                                connection_name: str) -> None:
        """Validate parameters required for Glue Connection creation.
        
        Args:
            connection_config: Database connection configuration
            connection_name: Name for the new Glue Connection
            
        Raises:
            ValueError: If required parameters are missing or invalid
            GlueConnectionError: If engine type is not supported for Glue Connections
        """
        # Validate connection name
        if not connection_name or not connection_name.strip():
            raise GlueConnectionParameterError(
                "Connection name cannot be empty",
                parameter_name="connection_name"
            )
        
        # Validate connection name format (AWS Glue naming requirements)
        import re
        if not re.match(r'^[a-zA-Z0-9_-]+$', connection_name):
            raise GlueConnectionParameterError(
                "Connection name must contain only alphanumeric characters, hyphens, and underscores",
                parameter_name="connection_name",
                parameter_value=connection_name
            )
        
        if len(connection_name) > 255:
            raise GlueConnectionParameterError(
                "Connection name cannot exceed 255 characters",
                parameter_name="connection_name",
                parameter_value=connection_name
            )
        
        # Validate that the engine type is supported for JDBC connections
        supported_engines = ['oracle', 'sqlserver', 'postgresql', 'db2']
        if connection_config.engine_type.lower() not in supported_engines:
            # Check if it's an Iceberg engine first for a more specific error
            if connection_config.is_iceberg_engine():
                from ..network.glue_connection_errors import GlueConnectionEngineCompatibilityError
                raise GlueConnectionEngineCompatibilityError(
                    "Glue Connections are only supported for JDBC databases, not Iceberg tables",
                    engine_type=connection_config.engine_type
                )
            else:
                raise GlueConnectionParameterError(
                    f"Engine type '{connection_config.engine_type}' is not supported for Glue Connections. Supported engines: {', '.join(supported_engines)}",
                    parameter_name="engine_type",
                    parameter_value=connection_config.engine_type
                )
        
        # Validate required JDBC parameters for Glue Connection creation
        if not connection_config.connection_string:
            raise GlueConnectionParameterError(
                "Connection string is required for Glue Connection creation",
                parameter_name="connection_string"
            )
        
        if not connection_config.username:
            raise GlueConnectionParameterError(
                "Username is required for Glue Connection creation",
                parameter_name="username"
            )
        
        if not connection_config.password:
            raise GlueConnectionParameterError(
                "Password is required for Glue Connection creation",
                parameter_name="password"
            )
        
        # Validate that username and password are not empty strings
        if not connection_config.username.strip():
            raise GlueConnectionParameterError(
                "Username cannot be empty or whitespace only",
                parameter_name="username",
                parameter_value="<empty>"
            )
        
        if not connection_config.password.strip():
            raise GlueConnectionParameterError(
                "Password cannot be empty or whitespace only",
                parameter_name="password",
                parameter_value="<empty>"
            )
        
        # Validate connection string format for the engine type
        from ..config.database_engines import DatabaseEngineManager
        if not DatabaseEngineManager.validate_connection_string(
            connection_config.engine_type, 
            connection_config.connection_string
        ):
            raise GlueConnectionParameterError(
                f"Invalid connection string format for {connection_config.engine_type} engine",
                parameter_name="connection_string",
                parameter_value=connection_config.connection_string[:50] + "..." if len(connection_config.connection_string) > 50 else connection_config.connection_string
            )
        supported_engines = ['oracle', 'sqlserver', 'postgresql', 'db2']
        if connection_config.engine_type.lower() not in supported_engines:
            raise GlueConnectionParameterError(
                f"Engine type '{connection_config.engine_type}' is not supported for Glue Connections. Supported engines: {', '.join(supported_engines)}",
                parameter_name="engine_type",
                parameter_value=connection_config.engine_type
            )
        
        # Validate database and schema names if provided
        if connection_config.database and not connection_config.database.strip():
            raise GlueConnectionParameterError(
                "Database name cannot be empty or whitespace only if provided",
                parameter_name="database",
                parameter_value="<empty>"
            )
        
        if connection_config.schema and not connection_config.schema.strip():
            raise GlueConnectionParameterError(
                "Schema name cannot be empty or whitespace only if provided",
                parameter_name="schema",
                parameter_value="<empty>"
            )
        
        self.structured_logger.debug(
            "Glue Connection creation parameters validated successfully for Secrets Manager integration",
            connection_name=connection_name,
            engine_type=connection_config.engine_type,
            has_database=bool(connection_config.database),
            has_schema=bool(connection_config.schema)
        )
    
    def _build_glue_connection_input(self, connection_config: ConnectionConfig, 
                                   connection_name: str) -> Dict[str, Any]:
        """Build Glue Connection input dictionary from connection configuration.
        
        Args:
            connection_config: Database connection configuration
            connection_name: Name for the new Glue Connection
            
        Returns:
            Dict[str, Any]: Glue Connection input dictionary
        """
        connection_input = {
            'Name': connection_name,
            'ConnectionType': 'JDBC',
            'ConnectionProperties': {
                'JDBC_CONNECTION_URL': connection_config.connection_string,
                'USERNAME': connection_config.username,
                'PASSWORD': connection_config.password
            },
            'Description': f'Auto-created JDBC connection for {connection_config.engine_type} database'
        }
        
        # Add physical connection requirements if network configuration is available
        if connection_config.network_config and connection_config.network_config.has_network_config():
            physical_requirements = {}
            
            # Add subnet ID (required for cross-VPC connections)
            if connection_config.network_config.subnet_ids:
                # Use the first subnet ID for the connection
                physical_requirements['SubnetId'] = connection_config.network_config.subnet_ids[0]
            
            # Add security group IDs (required for cross-VPC connections)
            if connection_config.network_config.security_group_ids:
                physical_requirements['SecurityGroupIdList'] = connection_config.network_config.security_group_ids
            
            # Add availability zone if specified
            if hasattr(connection_config.network_config, 'availability_zone') and connection_config.network_config.availability_zone:
                physical_requirements['AvailabilityZone'] = connection_config.network_config.availability_zone
            
            if physical_requirements:
                connection_input['PhysicalConnectionRequirements'] = physical_requirements
                
                self.structured_logger.debug(
                    "Added physical connection requirements to Glue Connection",
                    connection_name=connection_name,
                    subnet_id=physical_requirements.get('SubnetId'),
                    security_groups_count=len(physical_requirements.get('SecurityGroupIdList', []))
                )
        
        # Add engine-specific connection properties
        engine_properties = self._get_engine_specific_properties(connection_config.engine_type)
        if engine_properties:
            connection_input['ConnectionProperties'].update(engine_properties)
            
            self.structured_logger.debug(
                "Added engine-specific properties to Glue Connection",
                connection_name=connection_name,
                engine_type=connection_config.engine_type,
                properties_count=len(engine_properties)
            )
        
        return connection_input
    
    def _get_engine_specific_properties(self, engine_type: str) -> Dict[str, str]:
        """Get engine-specific connection properties for Glue Connection.
        
        Args:
            engine_type: Database engine type
            
        Returns:
            Dict[str, str]: Engine-specific connection properties
        """
        engine_type_lower = engine_type.lower()
        
        if engine_type_lower == 'oracle':
            return {
                'JDBC_DRIVER_JAR_URI': 's3://aws-glue-assets-123456789012-us-east-1/drivers/ojdbc8.jar',
                'JDBC_DRIVER_CLASS_NAME': 'oracle.jdbc.driver.OracleDriver'
            }
        elif engine_type_lower == 'sqlserver':
            return {
                'JDBC_DRIVER_JAR_URI': 's3://aws-glue-assets-123456789012-us-east-1/drivers/mssql-jdbc.jar',
                'JDBC_DRIVER_CLASS_NAME': 'com.microsoft.sqlserver.jdbc.SQLServerDriver'
            }
        elif engine_type_lower == 'postgresql':
            return {
                'JDBC_DRIVER_JAR_URI': 's3://aws-glue-assets-123456789012-us-east-1/drivers/postgresql.jar',
                'JDBC_DRIVER_CLASS_NAME': 'org.postgresql.Driver'
            }
        elif engine_type_lower == 'db2':
            return {
                'JDBC_DRIVER_JAR_URI': 's3://aws-glue-assets-123456789012-us-east-1/drivers/db2jcc4.jar',
                'JDBC_DRIVER_CLASS_NAME': 'com.ibm.db2.jcc.DB2Driver'
            }
        else:
            # Return empty dict for unknown engines
            return {}
    
    def _create_secrets_manager_secret(self, connection_config: ConnectionConfig, 
                                     connection_name: str) -> str:
        """Create AWS Secrets Manager secret for database credentials.
        
        Args:
            connection_config: Database connection configuration containing credentials
            connection_name: Name of the Glue Connection (used to generate secret name)
            
        Returns:
            str: Secret ARN for Glue Connection reference
            
        Raises:
            SecretCreationError: If secret creation fails
            SecretsManagerPermissionError: If insufficient permissions
            SecretsManagerRetryableError: For transient failures
        """
        try:
            self.structured_logger.info(
                "Creating Secrets Manager secret for Glue Connection",
                connection_name=connection_name,
                engine_type=connection_config.engine_type
            )
            
            # Validate Secrets Manager permissions before creating secret
            try:
                permissions_valid = self.secrets_manager_handler.validate_secret_permissions()
                if not permissions_valid:
                    raise SecretsManagerPermissionError(
                        "Insufficient permissions for Secrets Manager operations",
                        operation="create_secret",
                        required_permissions=self.secrets_manager_handler.REQUIRED_PERMISSIONS
                    )
            except SecretsManagerPermissionError:
                raise
            except Exception as perm_error:
                self.structured_logger.warning(
                    "Could not validate Secrets Manager permissions, proceeding with secret creation",
                    error=str(perm_error)
                )
            
            # Create the secret with database credentials
            secret_arn = self.secrets_manager_handler.create_secret(
                connection_name=connection_name,
                username=connection_config.username,
                password=connection_config.password,
                description=f"Database credentials for Glue Connection '{connection_name}' ({connection_config.engine_type})",
                tags={
                    'EngineType': connection_config.engine_type,
                    'Database': connection_config.database or 'unknown',
                    'Schema': connection_config.schema or 'unknown'
                }
            )
            
            self.structured_logger.info(
                "Successfully created Secrets Manager secret for Glue Connection",
                connection_name=connection_name,
                secret_arn=secret_arn,
                engine_type=connection_config.engine_type
            )
            
            return secret_arn
            
        except (SecretCreationError, SecretsManagerPermissionError, SecretsManagerRetryableError):
            # Re-raise Secrets Manager specific errors
            raise
        except Exception as e:
            self.structured_logger.error(
                "Unexpected error during Secrets Manager secret creation",
                connection_name=connection_name,
                error=str(e),
                error_type=type(e).__name__
            )
            raise SecretCreationError(
                f"Unexpected error during secret creation: {str(e)}",
                connection_name
            )
    
    def _build_glue_connection_input_with_secrets(self, connection_config: ConnectionConfig, 
                                                connection_name: str, secret_arn: str) -> Dict[str, Any]:
        """Build Glue Connection input dictionary with Secrets Manager secret reference.
        
        Args:
            connection_config: Database connection configuration
            connection_name: Name for the new Glue Connection
            secret_arn: ARN of the Secrets Manager secret containing credentials
            
        Returns:
            Dict[str, Any]: Glue Connection input dictionary with secret reference
        """
        connection_input = {
            'Name': connection_name,
            'ConnectionType': 'JDBC',
            'ConnectionProperties': {
                'JDBC_CONNECTION_URL': connection_config.connection_string,
                'SECRET_ID': secret_arn  # Reference to Secrets Manager secret
            },
            'Description': f'Auto-created JDBC connection for {connection_config.engine_type} database with Secrets Manager integration'
        }
        
        # Add physical connection requirements if network configuration is available
        if connection_config.network_config and connection_config.network_config.has_network_config():
            physical_requirements = {}
            
            # Add subnet ID (required for cross-VPC connections)
            if connection_config.network_config.subnet_ids:
                # Use the first subnet ID for the connection
                physical_requirements['SubnetId'] = connection_config.network_config.subnet_ids[0]
            
            # Add security group IDs (required for cross-VPC connections)
            if connection_config.network_config.security_group_ids:
                physical_requirements['SecurityGroupIdList'] = connection_config.network_config.security_group_ids
            
            # Add availability zone if specified
            if hasattr(connection_config.network_config, 'availability_zone') and connection_config.network_config.availability_zone:
                physical_requirements['AvailabilityZone'] = connection_config.network_config.availability_zone
            
            if physical_requirements:
                connection_input['PhysicalConnectionRequirements'] = physical_requirements
                
                self.structured_logger.debug(
                    "Added physical connection requirements to Glue Connection with Secrets Manager",
                    connection_name=connection_name,
                    subnet_id=physical_requirements.get('SubnetId'),
                    security_groups_count=len(physical_requirements.get('SecurityGroupIdList', []))
                )
        
        # Add engine-specific connection properties
        engine_properties = self._get_engine_specific_properties(connection_config.engine_type)
        if engine_properties:
            connection_input['ConnectionProperties'].update(engine_properties)
            
            self.structured_logger.debug(
                "Added engine-specific properties to Glue Connection with Secrets Manager",
                connection_name=connection_name,
                engine_type=connection_config.engine_type,
                properties_count=len(engine_properties)
            )
        
        return connection_input
    
    def _cleanup_secret_on_failure(self, connection_name: str) -> None:
        """Clean up Secrets Manager secret if Glue Connection creation fails.
        
        Args:
            connection_name: Name of the Glue Connection (used to identify the secret)
        """
        try:
            self.structured_logger.info(
                "Cleaning up Secrets Manager secret after Glue Connection creation failure",
                connection_name=connection_name
            )
            
            cleanup_successful = self.secrets_manager_handler.cleanup_secret_on_failure(connection_name)
            
            if cleanup_successful:
                self.structured_logger.info(
                    "Successfully cleaned up Secrets Manager secret after failure",
                    connection_name=connection_name
                )
            else:
                self.structured_logger.warning(
                    "Failed to clean up Secrets Manager secret after failure",
                    connection_name=connection_name
                )
                
        except Exception as cleanup_error:
            self.structured_logger.error(
                "Unexpected error during secret cleanup after Glue Connection failure",
                connection_name=connection_name,
                error=str(cleanup_error),
                error_type=type(cleanup_error).__name__
            )
            # Don't raise exception during cleanup to avoid masking the original error
    
    def validate_glue_connection_exists(self, connection_name: str) -> bool:
        """Validate that a Glue Connection exists.
        
        Args:
            connection_name: Name of the Glue Connection to validate
            
        Returns:
            bool: True if connection exists and is accessible, False otherwise
            
        Raises:
            GlueConnectionError: For Glue Connection access issues
        """
        try:
            self.structured_logger.info(
                "Validating Glue Connection existence",
                connection_name=connection_name
            )
            
            if not connection_name or not connection_name.strip():
                raise ValueError("Connection name cannot be empty")
            
            # Try to retrieve the connection
            try:
                response = self.glue_client.get_connection(Name=connection_name)
                connection = response.get('Connection', {})
                
                # Validate that it's a JDBC connection
                connection_type = connection.get('ConnectionType', '').upper()
                if connection_type not in ['JDBC', 'NETWORK']:
                    raise GlueConnectionError(
                        f"Glue Connection '{connection_name}' is not a JDBC or NETWORK connection (type: {connection_type})",
                        connection_name,
                        {'error_type': 'invalid_connection_type', 'connection_type': connection_type}
                    )
                
                # Validate connection properties
                connection_properties = connection.get('ConnectionProperties', {})
                if connection_type == 'JDBC' and not connection_properties.get('JDBC_CONNECTION_URL'):
                    raise GlueConnectionError(
                        f"Glue Connection '{connection_name}' lacks required JDBC_CONNECTION_URL property",
                        connection_name,
                        {'error_type': 'missing_jdbc_url'}
                    )
                
                self.structured_logger.info(
                    "Glue Connection validation successful",
                    connection_name=connection_name,
                    connection_type=connection_type
                )
                
                return True
                
            except ClientError as e:
                error_code = e.response.get('Error', {}).get('Code', '')
                error_message = e.response.get('Error', {}).get('Message', str(e))
                
                if error_code == 'EntityNotFoundException':
                    self.structured_logger.warning(
                        "Glue Connection does not exist",
                        connection_name=connection_name
                    )
                    return False
                elif error_code == 'AccessDeniedException':
                    raise GlueConnectionError(
                        f"Access denied to Glue Connection '{connection_name}': {error_message}",
                        connection_name,
                        {'error_type': 'access_denied', 'error_code': error_code}
                    )
                else:
                    raise GlueConnectionError(
                        f"Failed to access Glue Connection '{connection_name}': {error_message}",
                        connection_name,
                        {'error_type': 'access_error', 'error_code': error_code, 'aws_error': error_message}
                    )
                    
        except GlueConnectionError:
            raise
        except ValueError:
            raise
        except Exception as e:
            self.structured_logger.error(
                "Unexpected error during Glue Connection validation",
                connection_name=connection_name,
                error=str(e),
                error_type=type(e).__name__
            )
            raise GlueConnectionError(
                f"Unexpected error validating Glue Connection '{connection_name}': {str(e)}",
                connection_name,
                {'error_type': 'unexpected_error', 'original_error': str(e)}
            )
    
    def setup_jdbc_with_glue_connection_strategy(self, connection_config: ConnectionConfig) -> Dict[str, Any]:
        """Setup JDBC using the appropriate Glue Connection strategy.
        
        Args:
            connection_config: Database connection configuration with Glue Connection config
            
        Returns:
            Dictionary with JDBC connection properties
            
        Raises:
            GlueConnectionError: For Glue Connection specific issues
            NetworkConnectivityError: For network connectivity issues
        """
        try:
            if not connection_config.glue_connection_config:
                # No Glue Connection config, use direct JDBC
                return self.setup_jdbc_with_connection(connection_config, '')
            
            strategy = connection_config.get_glue_connection_strategy()
            
            self.structured_logger.info(
                "Setting up JDBC with Glue Connection strategy",
                engine_type=connection_config.engine_type,
                strategy=strategy
            )
            
            if strategy == "create_glue":
                return self._setup_jdbc_with_create_strategy(connection_config)
            elif strategy == "use_glue":
                return self._setup_jdbc_with_use_strategy(connection_config)
            else:
                # Direct JDBC strategy
                return self.setup_jdbc_with_connection(connection_config, '')
                
        except (GlueConnectionError, NetworkConnectivityError):
            raise
        except Exception as e:
            self.structured_logger.error(
                "Failed to setup JDBC with Glue Connection strategy",
                engine_type=connection_config.engine_type,
                strategy=connection_config.get_glue_connection_strategy(),
                error=str(e)
            )
            raise GlueConnectionError(
                f"Failed to setup JDBC with Glue Connection strategy: {str(e)}",
                connection_config.get_glue_connection_name_for_creation() or "unknown",
                {'error_type': 'strategy_setup_error', 'original_error': str(e)}
            )
    
    def _setup_jdbc_with_create_strategy(self, connection_config: ConnectionConfig) -> Dict[str, Any]:
        """Setup JDBC expecting a Glue Connection to exist.
        
        NOTE: Glue Connections should be created by CloudFormation (IaC), not at runtime.
        This method will fail if the expected connection doesn't exist.
        
        Args:
            connection_config: Database connection configuration
            
        Returns:
            Dictionary with JDBC connection properties
            
        Raises:
            GlueConnectionError: If the expected connection doesn't exist
        """
        # Determine connection name using {job_name}-{role}-jdbc-connection pattern
        # This matches the CloudFormation naming convention
        connection_role = None
        if connection_config.glue_connection_config:
            connection_role = connection_config.glue_connection_config.connection_role
        
        if self.job_name and connection_role:
            # Use CloudFormation naming convention: {job_name}-{source|target}-jdbc-connection
            connection_name = f"{self.job_name}-{connection_role}-jdbc-connection"
        else:
            raise GlueConnectionError(
                "Cannot determine Glue Connection name: job_name or connection_role not set. "
                "Glue Connections should be created by CloudFormation and referenced via UseSourceConnection/UseTargetConnection parameters.",
                "unknown",
                {'error_type': 'missing_connection_config'}
            )
        
        # Check if this connection exists (it should have been created by CloudFormation)
        try:
            existing_conn = self.get_glue_connection(connection_name)
            if existing_conn:
                self.structured_logger.info(
                    "Using Glue Connection created by CloudFormation",
                    connection_name=connection_name,
                    engine_type=connection_config.engine_type
                )
                return self.setup_jdbc_with_connection(connection_config, connection_name)
        except Exception as e:
            self.structured_logger.error(
                "Glue Connection not found. Connections should be created by CloudFormation (IaC), not at runtime.",
                connection_name=connection_name,
                error=str(e)
            )
            raise GlueConnectionError(
                f"Glue Connection '{connection_name}' not found. "
                f"Ensure CloudFormation has created this connection (CreateSourceConnection=true in parameters). "
                f"The Glue job should not create connections at runtime for security/minimal permissions.",
                connection_name,
                {'error_type': 'connection_not_found', 'original_error': str(e)}
            )
    
    def _setup_jdbc_with_use_strategy(self, connection_config: ConnectionConfig) -> Dict[str, Any]:
        """Setup JDBC using an existing Glue Connection.
        
        Args:
            connection_config: Database connection configuration
            
        Returns:
            Dictionary with JDBC connection properties
        """
        connection_name = connection_config.get_glue_connection_name_for_creation()
        
        if not connection_name:
            raise GlueConnectionError(
                "No Glue Connection name specified for use strategy",
                "unknown",
                {'error_type': 'missing_connection_name'}
            )
        
        self.structured_logger.info(
            "Using existing Glue Connection for JDBC setup",
            connection_name=connection_name,
            engine_type=connection_config.engine_type
        )
        
        # Validate that the connection exists
        if not self.validate_glue_connection_exists(connection_name):
            raise GlueConnectionError(
                f"Glue Connection '{connection_name}' does not exist or is not accessible",
                connection_name,
                {'error_type': 'connection_not_found'}
            )
        
        # Use the existing connection
        return self.setup_jdbc_with_connection(connection_config, connection_name)
    
    def setup_jdbc_with_connection(self, connection_config: ConnectionConfig, 
                                 glue_connection_name: str) -> Dict[str, Any]:
        """Configure JDBC connection properties using Glue connection when specified with enhanced error handling.
        
        Args:
            connection_config: Database connection configuration
            glue_connection_name: Name of Glue connection (empty for same-VPC)
            
        Returns:
            Dictionary with JDBC connection properties
            
        Raises:
            GlueConnectionError: For Glue connection specific issues
            NetworkConnectivityError: For network connectivity issues
        """
        try:
            self.structured_logger.info(
                "Setting up JDBC connection with enhanced error handling",
                engine_type=connection_config.engine_type,
                glue_connection_name=glue_connection_name or "same-vpc"
            )
            
            # Base JDBC properties
            jdbc_properties = {
                'url': connection_config.connection_string,
                'user': connection_config.username,
                'password': connection_config.password,
                'driver': DatabaseEngineManager.get_driver_class(connection_config.engine_type)
            }
            
            # If no Glue connection specified, use direct connection
            if not glue_connection_name or glue_connection_name.strip() == '':
                self.structured_logger.info("Using direct JDBC connection (same-VPC)")
                return jdbc_properties
            
            # Retrieve Glue connection details for cross-VPC access with error handling
            try:
                connection_details = self.get_glue_connection(glue_connection_name)
                if not connection_details:
                    raise GlueConnectionError(
                        f"Glue connection '{glue_connection_name}' not found",
                        glue_connection_name,
                        {'error_type': 'not_found'}
                    )
                
                # Check if this is a NETWORK connection (for VPC access only)
                connection_type = connection_details.get('connection_type', '').upper()
                if connection_type == 'NETWORK':
                    self.structured_logger.info(
                        "Using NETWORK connection for VPC access with direct JDBC properties",
                        connection_name=glue_connection_name
                    )
                    # For NETWORK connections, use direct JDBC properties
                    # The network connection just provides VPC access
                    jdbc_properties['_glue_connection_metadata'] = {
                        'connection_name': glue_connection_name,
                        'connection_type': 'NETWORK',
                        'subnet_id': connection_details.get('physical_connection_requirements', {}).get('SubnetId'),
                        'security_groups': connection_details.get('physical_connection_requirements', {}).get('SecurityGroupIdList', [])
                    }
                    return jdbc_properties
                    
            except GlueConnectionError:
                raise
            except Exception as conn_error:
                raise GlueConnectionError(
                    f"Failed to retrieve Glue connection '{glue_connection_name}': {str(conn_error)}",
                    glue_connection_name,
                    {'error_type': 'retrieval_error', 'original_error': str(conn_error)}
                )
            
            # Use connection properties from Glue connection if available
            glue_properties = connection_details.get('connection_properties', {})
            
            # Override with Glue connection properties if they exist
            if glue_properties.get('JDBC_CONNECTION_URL'):
                jdbc_properties['url'] = glue_properties['JDBC_CONNECTION_URL']
                self.structured_logger.debug(
                    "Using connection URL from Glue connection",
                    glue_connection_name=glue_connection_name
                )
            
            if glue_properties.get('USERNAME'):
                jdbc_properties['user'] = glue_properties['USERNAME']
                self.structured_logger.debug(
                    "Using username from Glue connection",
                    glue_connection_name=glue_connection_name
                )
            
            if glue_properties.get('PASSWORD'):
                jdbc_properties['password'] = glue_properties['PASSWORD']
                self.structured_logger.debug(
                    "Using password from Glue connection",
                    glue_connection_name=glue_connection_name
                )
            
            # Add network configuration metadata
            physical_requirements = connection_details.get('physical_connection_requirements', {})
            jdbc_properties['_glue_connection_metadata'] = {
                'connection_name': glue_connection_name,
                'subnet_id': physical_requirements.get('SubnetId'),
                'security_groups': physical_requirements.get('SecurityGroupIdList', []),
                'availability_zone': physical_requirements.get('AvailabilityZone')
            }
            
            # Validate network configuration before returning
            self._validate_network_configuration_for_jdbc(physical_requirements, glue_connection_name)
            
            self.structured_logger.info(
                "Successfully configured JDBC with Glue connection",
                glue_connection_name=glue_connection_name,
                subnet_id=physical_requirements.get('SubnetId'),
                security_groups_count=len(physical_requirements.get('SecurityGroupIdList', []))
            )
            
            return jdbc_properties
            
        except (GlueConnectionError, NetworkConnectivityError):
            raise
        except Exception as e:
            self.structured_logger.error(
                "Failed to setup JDBC with Glue connection",
                glue_connection_name=glue_connection_name,
                error=str(e)
            )
            raise NetworkConnectivityError(
                f"Failed to setup JDBC with Glue connection '{glue_connection_name}': {str(e)}",
                error_type='jdbc_setup_error',
                connection_name=glue_connection_name
            )
    
    def _validate_network_configuration_for_jdbc(self, physical_requirements: Dict[str, Any], connection_name: str):
        """Validate network configuration for JDBC setup.
        
        Note: Physical requirements are optional. Connections without them
        are valid for same-VPC scenarios where cross-VPC access is not needed.
        """
        # If no physical requirements, connection is for same-VPC access - skip validation
        if not physical_requirements:
            self.structured_logger.info(
                "No physical connection requirements - using connection for same-VPC access",
                connection_name=connection_name
            )
            return
        
        subnet_id = physical_requirements.get('SubnetId')
        security_groups = physical_requirements.get('SecurityGroupIdList', [])
        
        # Only validate if physical requirements are partially configured
        # (i.e., some fields present but others missing)
        if subnet_id or security_groups:
            if not subnet_id:
                raise NetworkConnectivityError(
                    f"Glue connection '{connection_name}' has security groups but missing subnet configuration",
                    error_type='incomplete_network_config',
                    connection_name=connection_name
                )
            
            if not security_groups:
                raise NetworkConnectivityError(
                    f"Glue connection '{connection_name}' has subnet but missing security group configuration",
                    error_type='incomplete_network_config',
                    connection_name=connection_name
                )
        
        self.structured_logger.debug(
            "Network configuration validation passed for JDBC setup",
            connection_name=connection_name,
            subnet_id=subnet_id,
            security_groups_count=len(security_groups) if security_groups else 0
        )


class JdbcConnectionManager:
    """Manages JDBC database connections with validation and error handling."""
    
    def __init__(self, spark_session: SparkSession, glue_context: GlueContext, 
                 retry_handler: Optional[ConnectionRetryHandler] = None,
                 job_name: Optional[str] = None):
        self.spark = spark_session
        self.glue_context = glue_context
        self.retry_handler = retry_handler or ConnectionRetryHandler()
        self.job_name = job_name
        self.glue_connection_manager = GlueConnectionManager(glue_context, job_name=job_name)
        self._connection_cache = {}
        self.structured_logger = StructuredLogger("JdbcConnectionManager")
    
    def create_connection_properties(self, connection_config: ConnectionConfig) -> Dict[str, str]:
        """Create JDBC connection properties from connection configuration."""
        properties = {
            'user': str(connection_config.username),
            'password': str(connection_config.password),
            'driver': str(DatabaseEngineManager.get_driver_class(connection_config.engine_type)),
            'fetchsize': '10000',
            'batchsize': '10000'
        }
        
        # Add engine-specific connection properties
        engine_type = connection_config.engine_type.lower()
        
        if engine_type == 'oracle':
            properties.update({
                'oracle.jdbc.timezoneAsRegion': 'false',
                'oracle.net.CONNECT_TIMEOUT': '30000',
                'oracle.jdbc.ReadTimeout': '60000'
            })
        
        elif engine_type == 'sqlserver':
            properties.update({
                'loginTimeout': '30',
                'socketTimeout': '60000',
                'selectMethod': 'cursor'
            })
        
        elif engine_type == 'postgresql':
            properties.update({
                'connectTimeout': '30',
                'socketTimeout': '60',
                'tcpKeepAlive': 'true'
            })
        
        elif engine_type == 'db2':
            properties.update({
                'loginTimeout': '30',
                'blockingReadConnectionTimeout': '60000',
                'resultSetHoldability': '1'
            })
        
        return properties
    
    def create_connection_with_glue_support(self, connection_config: ConnectionConfig, 
                                          glue_connection_name: str = '') -> DataFrame:
        """Create JDBC connection with optional Glue connection support for cross-VPC access.
        
        Args:
            connection_config: Database connection configuration
            glue_connection_name: Name of Glue connection for cross-VPC access (empty for same-VPC)
            
        Returns:
            DataFrame reader configured with appropriate connection properties
        """
        try:
            # Setup JDBC properties using Glue connection if specified
            jdbc_properties = self.glue_connection_manager.setup_jdbc_with_connection(
                connection_config, glue_connection_name
            )
            
            # Create DataFrame reader with JDBC properties
            df_reader = self.spark.read.format('jdbc')
            
            # Configure connection properties
            for key, value in jdbc_properties.items():
                if not key.startswith('_'):  # Skip metadata keys
                    df_reader = df_reader.option(key, value)
            
            self.structured_logger.info(
                "Created JDBC connection with Glue support",
                engine_type=connection_config.engine_type,
                glue_connection_name=glue_connection_name or "same-vpc",
                has_glue_metadata='_glue_connection_metadata' in jdbc_properties
            )
            
            return df_reader
            
        except Exception as e:
            self.structured_logger.error(
                "Failed to create JDBC connection with Glue support",
                engine_type=connection_config.engine_type,
                glue_connection_name=glue_connection_name,
                error=str(e)
            )
            raise RuntimeError(f"Failed to create JDBC connection: {str(e)}")
    
    def validate_connection_with_network_check(self, connection_config: ConnectionConfig, 
                                             glue_connection_name: str = '',
                                             timeout_seconds: int = 30) -> bool:
        """Validate database connection with network connectivity check and enhanced error handling.
        
        Args:
            connection_config: Database connection configuration
            glue_connection_name: Name of Glue connection for cross-VPC access
            timeout_seconds: Connection timeout in seconds
            
        Returns:
            True if connection is valid, False otherwise
            
        Raises:
            NetworkConnectivityError: For network connectivity issues
            GlueConnectionError: For Glue connection specific issues
            ENICreationError: For ENI creation failures
        """
        try:
            # Use network configuration from connection config if not explicitly provided
            if not glue_connection_name and connection_config.requires_cross_vpc_connection():
                glue_connection_name = connection_config.get_glue_connection_name() or ''
            
            self.structured_logger.info(
                "Starting connection validation with network check",
                engine_type=connection_config.engine_type,
                glue_connection_name=glue_connection_name or "same-vpc",
                cross_vpc_required=connection_config.requires_cross_vpc_connection()
            )
            
            start_time = time.time()
            
            # First, validate network connectivity with enhanced error handling
            try:
                network_valid = self.glue_connection_manager.validate_network_connectivity(
                    glue_connection_name, connection_config.connection_string, timeout_seconds
                )
                if not network_valid:
                    raise NetworkConnectivityError(
                        f"Network connectivity validation failed for connection '{glue_connection_name or 'same-vpc'}'",
                        error_type='connectivity_validation_failed',
                        connection_name=glue_connection_name
                    )
            except (NetworkConnectivityError, GlueConnectionError, VpcEndpointError, ENICreationError):
                raise
            except Exception as network_error:
                raise NetworkConnectivityError(
                    f"Network connectivity validation error: {str(network_error)}",
                    error_type='validation_error',
                    connection_name=glue_connection_name
                )
            
            # Then validate actual database connection using Glue connection if specified
            try:
                if glue_connection_name:
                    # For cross-VPC connections, use the Glue connection for validation
                    connection_valid = self._validate_connection_with_glue(connection_config, glue_connection_name)
                else:
                    # For same-VPC connections, use standard validation
                    connection_valid = self.validate_connection(connection_config)
            except Exception as db_error:
                # Classify database connection errors
                if "connection refused" in str(db_error).lower():
                    raise NetworkConnectivityError(
                        f"Database connection refused: {str(db_error)}",
                        error_type='connection_refused',
                        connection_name=glue_connection_name
                    )
                elif "timeout" in str(db_error).lower():
                    raise NetworkConnectivityError(
                        f"Database connection timeout: {str(db_error)}",
                        error_type='connection_timeout',
                        connection_name=glue_connection_name
                    )
                else:
                    raise NetworkConnectivityError(
                        f"Database connection validation failed: {str(db_error)}",
                        error_type='database_connection_error',
                        connection_name=glue_connection_name
                    )
            
            duration = time.time() - start_time
            self.structured_logger.info(
                "Connection validation with network check completed successfully",
                engine_type=connection_config.engine_type,
                glue_connection_name=glue_connection_name or "same-vpc",
                cross_vpc_required=connection_config.requires_cross_vpc_connection(),
                validation_result="passed" if connection_valid else "failed",
                duration_seconds=round(duration, 2)
            )
            
            return connection_valid
            
        except (NetworkConnectivityError, GlueConnectionError, VpcEndpointError, ENICreationError):
            duration = time.time() - start_time
            self.structured_logger.error(
                "Connection validation with network check failed with network error",
                engine_type=connection_config.engine_type,
                glue_connection_name=glue_connection_name or "same-vpc",
                cross_vpc_required=connection_config.requires_cross_vpc_connection(),
                duration_seconds=round(duration, 2)
            )
            raise
        except Exception as e:
            duration = time.time() - start_time
            self.structured_logger.error(
                "Connection validation with network check failed with unexpected error",
                engine_type=connection_config.engine_type,
                glue_connection_name=glue_connection_name or "same-vpc",
                cross_vpc_required=connection_config.requires_cross_vpc_connection(),
                error=str(e),
                duration_seconds=round(duration, 2)
            )
            raise NetworkConnectivityError(
                f"Connection validation failed with unexpected error: {str(e)}",
                error_type='unexpected_error',
                connection_name=glue_connection_name
            )
    
    def _validate_connection_with_glue(self, connection_config: ConnectionConfig, glue_connection_name: str) -> bool:
        """Validate database connection using Glue connection for cross-VPC access with enhanced error handling."""
        try:
            # Setup JDBC properties using Glue connection with error handling
            try:
                jdbc_properties = self.glue_connection_manager.setup_jdbc_with_connection(
                    connection_config, glue_connection_name
                )
            except (GlueConnectionError, NetworkConnectivityError):
                raise
            except Exception as setup_error:
                raise NetworkConnectivityError(
                    f"Failed to setup JDBC properties for Glue connection '{glue_connection_name}': {str(setup_error)}",
                    error_type='jdbc_setup_error',
                    connection_name=glue_connection_name
                )
            
            # Test connection by executing a simple query with timeout and error handling
            try:
                test_df = self.spark.read \
                    .format("jdbc") \
                    .option("url", jdbc_properties['url']) \
                    .option("user", jdbc_properties['user']) \
                    .option("password", jdbc_properties['password']) \
                    .option("driver", jdbc_properties['driver']) \
                    .option("query", "SELECT 1 as test_column") \
                    .option("connectTimeout", "30") \
                    .option("socketTimeout", "60") \
                    .load()
                
                # Execute the query to test connectivity
                test_result = test_df.collect()
                
                if test_result and len(test_result) > 0:
                    self.structured_logger.info(
                        "Database connection validation successful using Glue connection",
                        glue_connection_name=glue_connection_name,
                        test_result_count=len(test_result)
                    )
                    return True
                else:
                    raise NetworkConnectivityError(
                        f"Database connection test returned no results for Glue connection '{glue_connection_name}'",
                        error_type='empty_test_result',
                        connection_name=glue_connection_name
                    )
                    
            except Exception as db_error:
                error_str = str(db_error).lower()
                
                # Classify database connection errors
                if "connection refused" in error_str or "connection reset" in error_str:
                    raise NetworkConnectivityError(
                        f"Database connection refused for Glue connection '{glue_connection_name}': {str(db_error)}",
                        error_type='connection_refused',
                        connection_name=glue_connection_name
                    )
                elif "timeout" in error_str or "timed out" in error_str:
                    raise NetworkConnectivityError(
                        f"Database connection timeout for Glue connection '{glue_connection_name}': {str(db_error)}",
                        error_type='connection_timeout',
                        connection_name=glue_connection_name
                    )
                elif "network" in error_str or "unreachable" in error_str:
                    raise NetworkConnectivityError(
                        f"Network unreachable for Glue connection '{glue_connection_name}': {str(db_error)}",
                        error_type='network_unreachable',
                        connection_name=glue_connection_name
                    )
                elif "eni" in error_str or "elastic network interface" in error_str:
                    raise ENICreationError(
                        f"ENI creation failed for Glue connection '{glue_connection_name}': {str(db_error)}",
                        error_code='eni_creation_failed'
                    )
                else:
                    raise NetworkConnectivityError(
                        f"Database connection validation failed for Glue connection '{glue_connection_name}': {str(db_error)}",
                        error_type='database_connection_error',
                        connection_name=glue_connection_name
                    )
        except (NetworkConnectivityError, GlueConnectionError, ENICreationError):
            raise
        except Exception as e:
            self.structured_logger.error(
                "Database connection validation failed using Glue connection with unexpected error",
                glue_connection_name=glue_connection_name,
                error=str(e)
            )
            raise NetworkConnectivityError(
                f"Unexpected error during database connection validation for Glue connection '{glue_connection_name}': {str(e)}",
                error_type='unexpected_validation_error',
                connection_name=glue_connection_name
            )
    
    def validate_connection(self, connection_config: ConnectionConfig) -> bool:
        """Validate database connection by executing a simple query using Spark DataFrame."""
        def _validate():
            properties = self.create_connection_properties(connection_config)
            
            # Use a simple query appropriate for each database type
            test_queries = {
                'oracle': 'SELECT 1 as test_column FROM DUAL',
                'sqlserver': 'SELECT 1 as test_column',
                'postgresql': 'SELECT 1 as test_column',
                'db2': 'SELECT 1 as test_column FROM SYSIBM.SYSDUMMY1'
            }
            
            test_query = test_queries.get(connection_config.engine_type.lower(), 'SELECT 1 as test_column')
            
            try:
                # Execute test query using Spark DataFrame (not DynamicFrame)
                reader = self.spark.read.format('jdbc')
                reader = reader.option('url', connection_config.connection_string)
                reader = reader.option('query', test_query)
                
                # Add connection properties
                for key, value in properties.items():
                    reader = reader.option(key, str(value))
                
                df = reader.load()
                
                # Trigger execution by collecting one row
                result = df.collect()
                
                if result and len(result) > 0:
                    self.structured_logger.info(f"Connection validation successful for {connection_config.engine_type}")
                    return True
                else:
                    raise RuntimeError("Test query returned no results")
                    
            except Exception as e:
                self.structured_logger.error(f"Connection validation failed for {connection_config.engine_type}: {str(e)}")
                raise
        
        try:
            return self.retry_handler.execute_with_retry(
                _validate,
                f"connection validation for {connection_config.engine_type}"
            )
        except Exception as e:
            self.structured_logger.error(f"Connection validation failed after retries: {str(e)}")
            return False
    
    def get_table_schema(self, connection_config: ConnectionConfig, table_name: str) -> StructType:
        """Get table schema from database using Spark DataFrame."""
        def _get_schema():
            properties = self.create_connection_properties(connection_config)
            
            # Build full table name with schema
            full_table_name = f"{connection_config.schema}.{table_name}"
            
            try:
                # Read table schema by limiting to 0 rows using Spark DataFrame
                reader = self.spark.read.format('jdbc')
                reader = reader.option('url', connection_config.connection_string)
                reader = reader.option('dbtable', full_table_name)
                
                # Add connection properties
                for key, value in properties.items():
                    reader = reader.option(key, str(value))
                
                df = reader.load().limit(0)
                
                schema = df.schema
                self.structured_logger.info(f"Retrieved schema for table {full_table_name}: {len(schema.fields)} columns")
                return schema
                
            except Exception as e:
                self.structured_logger.error(f"Failed to get schema for table {full_table_name}: {str(e)}")
                raise
        
        return self.retry_handler.execute_with_retry(
            _get_schema,
            f"schema retrieval for {connection_config.schema}.{table_name}"
        )
    
    def read_table_data(self, connection_config: ConnectionConfig, table_name: str, 
                       query: Optional[str] = None, **options) -> DataFrame:
        """Read data from database table using Spark DataFrame (not DynamicFrame).
        
        This method respects the Glue connection strategy configured in connection_config:
        - use_glue: Uses an existing Glue Connection for VPC access
        - create_glue: Creates a new Glue Connection (not typically used for reads)
        - direct_jdbc: Uses direct JDBC connection properties
        """
        def _read_data():
            # Check if we should use Glue connection strategy
            if connection_config.glue_connection_config and connection_config.uses_glue_connection():
                # Use Glue connection strategy to get connection properties
                self.structured_logger.info(
                    f"Using Glue Connection strategy for reading {table_name}",
                    table_name=table_name,
                    strategy=connection_config.get_glue_connection_strategy(),
                    connection_name=connection_config.get_glue_connection_name_for_creation()
                )
                properties = self.glue_connection_manager.setup_jdbc_with_glue_connection_strategy(connection_config)
            else:
                # Use direct JDBC properties
                properties = self.create_connection_properties(connection_config)
            
            # Add any additional options
            properties.update(options)
            
            try:
                # Use Spark DataFrame reader directly (not DynamicFrame)
                reader = self.spark.read.format('jdbc')
                
                # Set connection properties - use URL from properties if available (from Glue connection)
                url = properties.pop('url', connection_config.connection_string)
                reader = reader.option('url', url)
                
                for key, value in properties.items():
                    # Skip internal metadata keys
                    if not key.startswith('_'):
                        reader = reader.option(key, str(value))
                
                if query:
                    # Use custom query
                    df = reader.option('query', query).load()
                else:
                    # Use table name
                    full_table_name = f"{connection_config.schema}.{table_name}"
                    df = reader.option('dbtable', full_table_name).load()
                
                self.structured_logger.info(f"Successfully read data from {connection_config.engine_type} table: {table_name}")
                return df
                
            except Exception as e:
                self.structured_logger.error(f"Failed to read data from table {table_name}: {str(e)}")
                raise
        
        return self.retry_handler.execute_with_retry(
            _read_data,
            f"data reading from {connection_config.schema}.{table_name}"
        )
    
    def read_table_data_partitioned(
        self, 
        connection_config: ConnectionConfig, 
        table_name: str,
        partition_column: str,
        lower_bound: int,
        upper_bound: int,
        num_partitions: int,
        fetch_size: int = 10000,
        **options
    ) -> DataFrame:
        """
        Read data from database table using parallel JDBC connections.
        
        This method creates multiple parallel connections to read different
        ranges of the partition column, significantly improving read performance
        for large tables.
        
        This method respects the Glue connection strategy configured in connection_config.
        
        Args:
            connection_config: Database connection configuration
            table_name: Name of the table to read
            partition_column: Numeric column to partition on (e.g., 'id')
            lower_bound: Minimum value of partition column
            upper_bound: Maximum value of partition column
            num_partitions: Number of parallel read partitions
            fetch_size: Rows to fetch per JDBC round-trip
            **options: Additional JDBC options
            
        Returns:
            DataFrame with data distributed across partitions
        """
        def _read_data_partitioned():
            # Check if we should use Glue connection strategy
            if connection_config.glue_connection_config and connection_config.uses_glue_connection():
                self.structured_logger.info(
                    f"Using Glue Connection strategy for partitioned read of {table_name}",
                    table_name=table_name,
                    strategy=connection_config.get_glue_connection_strategy(),
                    connection_name=connection_config.get_glue_connection_name_for_creation()
                )
                properties = self.glue_connection_manager.setup_jdbc_with_glue_connection_strategy(connection_config)
            else:
                properties = self.create_connection_properties(connection_config)
            
            properties.update(options)
            
            full_table_name = f"{connection_config.schema}.{table_name}"
            
            # Get URL from properties if available (from Glue connection), otherwise use connection_config
            url = properties.pop('url', connection_config.connection_string)
            
            try:
                self.structured_logger.info(
                    f"Starting partitioned JDBC read for {table_name}",
                    table_name=table_name,
                    partition_column=partition_column,
                    lower_bound=lower_bound,
                    upper_bound=upper_bound,
                    num_partitions=num_partitions,
                    fetch_size=fetch_size
                )
                
                # Build reader with partitioning options
                reader = self.spark.read.format('jdbc') \
                    .option('url', url) \
                    .option('dbtable', full_table_name) \
                    .option('partitionColumn', partition_column) \
                    .option('lowerBound', str(lower_bound)) \
                    .option('upperBound', str(upper_bound)) \
                    .option('numPartitions', str(num_partitions)) \
                    .option('fetchsize', str(fetch_size))
                
                # Add connection properties (skip internal metadata keys)
                for key, value in properties.items():
                    if not key.startswith('_'):
                        reader = reader.option(key, str(value))
                
                df = reader.load()
                
                actual_partitions = df.rdd.getNumPartitions()
                self.structured_logger.info(
                    f"Partitioned read complete for {table_name}",
                    table_name=table_name,
                    requested_partitions=num_partitions,
                    actual_partitions=actual_partitions
                )
                
                return df
                
            except Exception as e:
                self.structured_logger.error(
                    f"Failed partitioned read for {table_name}: {str(e)}",
                    table_name=table_name,
                    partition_column=partition_column,
                    error_type=type(e).__name__
                )
                raise
        
        return self.retry_handler.execute_with_retry(
            _read_data_partitioned,
            f"partitioned data reading from {connection_config.schema}.{table_name}"
        )
    
    def write_table_data(self, df: DataFrame, connection_config: ConnectionConfig, 
                        table_name: str, mode: str = 'append', **options) -> None:
        """Write data to database table using Spark DataFrame.
        
        This method respects the Glue connection strategy configured in connection_config.
        """
        def _write_data():
            # Check if we should use Glue connection strategy
            if connection_config.glue_connection_config and connection_config.uses_glue_connection():
                self.structured_logger.info(
                    f"Using Glue Connection strategy for writing to {table_name}",
                    table_name=table_name,
                    strategy=connection_config.get_glue_connection_strategy(),
                    connection_name=connection_config.get_glue_connection_name_for_creation()
                )
                properties = self.glue_connection_manager.setup_jdbc_with_glue_connection_strategy(connection_config)
            else:
                properties = self.create_connection_properties(connection_config)
            
            # Add any additional options
            if options:
                for key, value in options.items():
                    properties[key] = str(value)
            
            # Build full table name with schema
            full_table_name = f"{connection_config.schema}.{table_name}"
            
            # Get URL from properties if available (from Glue connection), otherwise use connection_config
            url = properties.pop('url', connection_config.connection_string)
            
            try:
                # Build writer using Spark DataFrame (not DynamicFrame)
                writer = df.write.format('jdbc')
                writer = writer.option('url', url)
                writer = writer.option('dbtable', full_table_name)
                writer = writer.mode(mode)
                
                # Add properties individually (skip internal metadata keys)
                for key, value in properties.items():
                    if not key.startswith('_'):
                        writer = writer.option(key, str(value))
                
                writer.save()
                
                self.structured_logger.info(f"Successfully wrote data to {connection_config.engine_type} table: {table_name}")
                
            except Exception as e:
                self.structured_logger.error(f"Failed to write data to table {table_name}: {str(e)}")
                raise
        
        self.retry_handler.execute_with_retry(
            _write_data,
            f"data writing to {connection_config.schema}.{table_name}"
        )
    
    def test_connection(self, connection_config: ConnectionConfig) -> Dict[str, Any]:
        """Test database connection and return connection information."""
        connection_info = {
            'engine_type': connection_config.engine_type,
            'database': connection_config.database,
            'schema': connection_config.schema,
            'connection_valid': False,
            'error_message': None,
            'test_timestamp': time.time()
        }
        
        try:
            # Validate connection
            is_valid = self.validate_connection(connection_config)
            connection_info['connection_valid'] = is_valid
            
            if is_valid:
                self.structured_logger.info(f"Connection test successful for {connection_config.engine_type}")
            else:
                connection_info['error_message'] = "Connection validation failed"
                
        except Exception as e:
            connection_info['connection_valid'] = False
            connection_info['error_message'] = str(e)
            self.structured_logger.error(f"Connection test failed for {connection_config.engine_type}: {str(e)}")
        
        return connection_info
    
    def get_connection_cache_key(self, connection_config: ConnectionConfig) -> str:
        """Generate cache key for connection configuration."""
        return f"{connection_config.engine_type}_{connection_config.database}_{connection_config.schema}_{connection_config.username}"
    
    def cache_connection_info(self, connection_config: ConnectionConfig, info: Dict[str, Any]) -> None:
        """Cache connection information for reuse."""
        cache_key = self.get_connection_cache_key(connection_config)
        self._connection_cache[cache_key] = info
        self.structured_logger.debug(f"Cached connection info for {cache_key}")
    
    def get_cached_connection_info(self, connection_config: ConnectionConfig) -> Optional[Dict[str, Any]]:
        """Get cached connection information."""
        cache_key = self.get_connection_cache_key(connection_config)
        return self._connection_cache.get(cache_key)


class UnifiedConnectionManager:
    """Unified connection manager that routes between JDBC and Iceberg connections.
    
    This class provides a single interface for managing both traditional JDBC database
    connections and Iceberg table connections, automatically routing requests to the
    appropriate handler based on the engine type.
    """
    
    def __init__(self, spark_session: SparkSession, glue_context: GlueContext,
                 retry_handler: Optional[ConnectionRetryHandler] = None,
                 job_name: Optional[str] = None):
        """Initialize the unified connection manager.
        
        Args:
            spark_session: Active Spark session
            glue_context: AWS Glue context
            retry_handler: Optional retry handler for connection operations
            job_name: Job name used for Glue Connection naming ({job_name}-source/target)
        """
        self.spark = spark_session
        self.glue_context = glue_context
        self.retry_handler = retry_handler or ConnectionRetryHandler()
        self.job_name = job_name
        
        # Initialize JDBC connection manager with job_name for connection naming
        self.jdbc_manager = JdbcConnectionManager(
            spark_session, glue_context, retry_handler, job_name=job_name
        )
        
        # Initialize Iceberg connection handler
        self.iceberg_handler = IcebergConnectionHandler(
            spark_session, glue_context
        )
        
        self.structured_logger = StructuredLogger("UnifiedConnectionManager")
        
        # Connection validation cache
        self._validation_cache = {}
        
        self.structured_logger.info(
            "Initialized UnifiedConnectionManager with JDBC and Iceberg support",
            job_name=job_name
        )
    
    def is_iceberg_engine(self, engine_type: str) -> bool:
        """Check if the engine type is Iceberg.
        
        Args:
            engine_type: Database engine type to check
            
        Returns:
            bool: True if engine is Iceberg, False otherwise
        """
        return DatabaseEngineManager.is_iceberg_engine(engine_type)
    
    def validate_connection_config(self, connection_config: ConnectionConfig) -> bool:
        """Validate connection configuration with engine-specific validation.
        
        Args:
            connection_config: Connection configuration to validate
            
        Returns:
            bool: True if configuration is valid, False otherwise
            
        Raises:
            IcebergValidationError: For Iceberg-specific validation errors
            ValueError: For general configuration errors
        """
        try:
            self.structured_logger.info(
                "Validating connection configuration",
                engine_type=connection_config.engine_type
            )
            
            # Check if engine is supported
            if not DatabaseEngineManager.is_engine_supported(connection_config.engine_type):
                raise ValueError(f"Unsupported database engine: {connection_config.engine_type}")
            
            # Route validation based on engine type
            if self.is_iceberg_engine(connection_config.engine_type):
                return self._validate_iceberg_connection_config(connection_config)
            else:
                return self._validate_jdbc_connection_config(connection_config)
                
        except (IcebergValidationError, ValueError):
            raise
        except Exception as e:
            self.structured_logger.error(
                "Unexpected error during connection configuration validation",
                engine_type=connection_config.engine_type,
                error=str(e)
            )
            raise ValueError(f"Connection configuration validation failed: {str(e)}")
    
    def _validate_iceberg_connection_config(self, connection_config: ConnectionConfig) -> bool:
        """Validate Iceberg-specific connection configuration.
        
        Args:
            connection_config: Connection configuration to validate
            
        Returns:
            bool: True if configuration is valid
            
        Raises:
            IcebergValidationError: If validation fails
        """
        try:
            # For Iceberg, we need to extract configuration from connection_config
            # Since Iceberg doesn't use traditional JDBC parameters, we need to
            # parse the configuration differently
            
            # Create Iceberg config from connection parameters
            # Get Iceberg configuration from the connection config
            connection_iceberg_config = connection_config.get_iceberg_config()
            if not connection_iceberg_config:
                raise ValueError(f"Iceberg configuration missing for connection")
            
            iceberg_config_dict = {
                'database_name': connection_config.database,
                'table_name': getattr(connection_config, 'table_name', ''),
                'warehouse_location': connection_iceberg_config.get('warehouse_location', ''),
                'catalog_id': connection_iceberg_config.get('catalog_id', None),
                'format_version': connection_iceberg_config.get('format_version', '2')
            }
            
            # Use DatabaseEngineManager to validate Iceberg configuration
            is_valid = DatabaseEngineManager.validate_iceberg_config(iceberg_config_dict)
            
            if not is_valid:
                raise IcebergValidationError(
                    "Iceberg configuration validation failed",
                    field="configuration",
                    value=str(iceberg_config_dict)
                )
            
            self.structured_logger.info(
                "Iceberg connection configuration validation passed",
                database=connection_config.database
            )
            
            return True
            
        except IcebergValidationError:
            raise
        except Exception as e:
            raise IcebergValidationError(
                f"Iceberg configuration validation error: {str(e)}",
                field="validation",
                value=str(e)
            )
    
    def _validate_jdbc_connection_config(self, connection_config: ConnectionConfig) -> bool:
        """Validate JDBC connection configuration.
        
        Args:
            connection_config: Connection configuration to validate
            
        Returns:
            bool: True if configuration is valid
            
        Raises:
            ValueError: If validation fails
        """
        try:
            # Validate connection string format
            is_valid = DatabaseEngineManager.validate_connection_string(
                connection_config.engine_type,
                connection_config.connection_string
            )
            
            if not is_valid:
                raise ValueError(
                    f"Invalid connection string format for {connection_config.engine_type}"
                )
            
            # Validate required JDBC parameters
            connection_config.validate()
            
            self.structured_logger.info(
                "JDBC connection configuration validation passed",
                engine_type=connection_config.engine_type,
                database=connection_config.database
            )
            
            return True
            
        except ValueError:
            raise
        except Exception as e:
            raise ValueError(f"JDBC configuration validation error: {str(e)}")
    
    def validate_connection(self, connection_config: ConnectionConfig,
                          timeout_seconds: int = 30) -> bool:
        """Validate database connection with strategy-based routing.
        
        Args:
            connection_config: Connection configuration to validate
            timeout_seconds: Connection timeout in seconds
            
        Returns:
            bool: True if connection is valid, False otherwise
            
        Raises:
            IcebergConnectionError: For Iceberg connection issues
            NetworkConnectivityError: For network connectivity issues
            GlueConnectionError: For Glue Connection specific issues
        """
        try:
            # Check validation cache first
            cache_key = self._get_validation_cache_key(connection_config)
            cached_result = self._validation_cache.get(cache_key)
            if cached_result is not None:
                cache_age = time.time() - cached_result['timestamp']
                if cache_age < 300:  # 5 minutes cache
                    self.structured_logger.debug(
                        "Using cached connection validation result",
                        engine_type=connection_config.engine_type,
                        result=cached_result['valid']
                    )
                    return cached_result['valid']
            
            # Determine connection strategy
            strategy = self._determine_connection_strategy(connection_config)
            
            self.structured_logger.info(
                "Validating database connection with strategy",
                engine_type=connection_config.engine_type,
                strategy=strategy,
                timeout_seconds=timeout_seconds
            )
            
            start_time = time.time()
            
            # Route validation based on strategy
            if strategy == "iceberg":
                result = self._validate_iceberg_connection(connection_config, timeout_seconds)
            elif strategy in ["create_glue", "use_glue"]:
                result = self._validate_jdbc_connection_with_glue_strategy(connection_config, timeout_seconds)
            else:
                result = self._validate_jdbc_connection(connection_config, timeout_seconds)
            
            # Cache the result
            self._validation_cache[cache_key] = {
                'valid': result,
                'timestamp': time.time()
            }
            
            duration = time.time() - start_time
            self.structured_logger.info(
                "Connection validation completed",
                engine_type=connection_config.engine_type,
                strategy=strategy,
                result="passed" if result else "failed",
                duration_seconds=round(duration, 2)
            )
            
            return result
            
        except (IcebergConnectionError, NetworkConnectivityError, GlueConnectionError):
            raise
        except Exception as e:
            self.structured_logger.error(
                "Unexpected error during connection validation",
                engine_type=connection_config.engine_type,
                error=str(e)
            )
            raise NetworkConnectivityError(
                f"Connection validation failed: {str(e)}",
                error_type='validation_error',
                connection_name=connection_config.get_glue_connection_name()
            )
    
    def _validate_iceberg_connection(self, connection_config: ConnectionConfig,
                                   timeout_seconds: int) -> bool:
        """Validate Iceberg connection by checking catalog access.
        
        Args:
            connection_config: Iceberg connection configuration
            timeout_seconds: Connection timeout (not used for Iceberg)
            
        Returns:
            bool: True if connection is valid
            
        Raises:
            IcebergConnectionError: If validation fails
        """
        try:
            self.structured_logger.info(
                "Validating Iceberg connection",
                database=connection_config.database
            )
            
            # Create Iceberg config from connection parameters
            # Get Iceberg configuration from the connection config
            connection_iceberg_config = connection_config.get_iceberg_config()
            if not connection_iceberg_config:
                raise IcebergConnectionError(
                    "Iceberg configuration missing for connection validation",
                    warehouse_location=""
                )
            
            warehouse_location = connection_iceberg_config.get('warehouse_location', '')
            catalog_id = connection_iceberg_config.get('catalog_id', None)
            
            if not warehouse_location:
                raise IcebergConnectionError(
                    "Warehouse location is required for Iceberg connection validation",
                    warehouse_location=warehouse_location
                )
            
            # Configure Iceberg catalog to test connectivity
            self.iceberg_handler.configure_iceberg_catalog(
                warehouse_location=warehouse_location,
                catalog_id=catalog_id
            )
            
            # Test catalog access by checking if database exists
            try:
                # This will test Glue Data Catalog access
                glue_client = boto3.client('glue')
                get_database_kwargs = {'Name': connection_config.database}
                if catalog_id:
                    get_database_kwargs['CatalogId'] = catalog_id
                
                glue_client.get_database(**get_database_kwargs)
                
                self.structured_logger.info(
                    "Iceberg connection validation successful",
                    database=connection_config.database,
                    warehouse_location=warehouse_location
                )
                return True
                
            except ClientError as e:
                error_code = e.response.get('Error', {}).get('Code', '')
                if error_code == 'EntityNotFoundException':
                    # Database doesn't exist, but catalog access works
                    self.structured_logger.warning(
                        "Iceberg database not found, but catalog access is valid",
                        database=connection_config.database
                    )
                    return True
                else:
                    raise IcebergConnectionError(
                        f"Iceberg catalog access failed: {str(e)}",
                        warehouse_location=warehouse_location,
                        aws_error=e
                    )
            
        except IcebergConnectionError:
            raise
        except Exception as e:
            # Get warehouse location for error context
            connection_iceberg_config = connection_config.get_iceberg_config()
            warehouse_location = connection_iceberg_config.get('warehouse_location', '') if connection_iceberg_config else ''
            
            raise IcebergConnectionError(
                f"Iceberg connection validation failed: {str(e)}",
                warehouse_location=warehouse_location,
                spark_error=e
            )
    
    def _validate_jdbc_connection_with_glue_strategy(self, connection_config: ConnectionConfig,
                                                   timeout_seconds: int) -> bool:
        """Validate JDBC connection using Glue Connection strategy.
        
        Args:
            connection_config: JDBC connection configuration with Glue Connection config
            timeout_seconds: Connection timeout in seconds
            
        Returns:
            bool: True if connection is valid
            
        Raises:
            GlueConnectionError: For Glue Connection specific issues
        """
        try:
            strategy = connection_config.get_glue_connection_strategy()
            
            self.structured_logger.info(
                "Validating JDBC connection with Glue Connection strategy",
                engine_type=connection_config.engine_type,
                strategy=strategy
            )
            
            # For create_glue strategy, validate that we can create the connection
            if strategy == "create_glue":
                # Validate that all required parameters are present for creation
                try:
                    self.jdbc_manager.glue_connection_manager._validate_glue_connection_creation_params(
                        connection_config, "validation-test"
                    )
                except Exception as e:
                    self.structured_logger.error(
                        "Glue Connection creation parameters validation failed",
                        error=str(e)
                    )
                    return False
                
                # Test direct JDBC connection since we haven't created the Glue Connection yet
                return self.jdbc_manager.validate_connection(connection_config)
            
            # For use_glue strategy, validate that the connection exists and test it
            elif strategy == "use_glue":
                connection_name = connection_config.get_glue_connection_name_for_creation()
                if not connection_name:
                    self.structured_logger.error("No Glue Connection name specified for use strategy")
                    return False
                
                # Validate that the connection exists
                try:
                    exists = self.jdbc_manager.glue_connection_manager.validate_glue_connection_exists(
                        connection_name
                    )
                    if not exists:
                        self.structured_logger.error(
                            "Glue Connection does not exist",
                            connection_name=connection_name
                        )
                        return False
                except Exception as e:
                    self.structured_logger.error(
                        "Failed to validate Glue Connection existence",
                        connection_name=connection_name,
                        error=str(e)
                    )
                    return False
                
                # Test the connection using the Glue Connection
                return self.jdbc_manager.validate_connection_with_network_check(
                    connection_config=connection_config,
                    glue_connection_name=connection_name,
                    timeout_seconds=timeout_seconds
                )
            
            else:
                # Fallback to direct JDBC validation
                return self._validate_jdbc_connection(connection_config, timeout_seconds)
                
        except GlueConnectionError:
            raise
        except Exception as e:
            self.structured_logger.error(
                "Unexpected error during Glue Connection strategy validation",
                engine_type=connection_config.engine_type,
                strategy=strategy,
                error=str(e)
            )
            raise NetworkConnectivityError(
                f"Glue Connection strategy validation failed: {str(e)}",
                error_type='strategy_validation_error',
                connection_name=connection_config.get_glue_connection_name_for_creation()
            )
    
    def _validate_jdbc_connection(self, connection_config: ConnectionConfig,
                                timeout_seconds: int) -> bool:
        """Validate JDBC connection using direct JDBC (no Glue Connection).
        
        Args:
            connection_config: JDBC connection configuration
            timeout_seconds: Connection timeout in seconds
            
        Returns:
            bool: True if connection is valid
        """
        return self.jdbc_manager.validate_connection_with_network_check(
            connection_config=connection_config,
            glue_connection_name='',  # Empty string for direct JDBC
            timeout_seconds=timeout_seconds
        )
    
    def _determine_connection_strategy(self, connection_config: ConnectionConfig) -> str:
        """Determine connection strategy based on engine type and Glue config.
        
        Args:
            connection_config: Connection configuration to analyze
            
        Returns:
            str: Connection strategy - 'iceberg', 'create_glue', 'use_glue', or 'direct_jdbc'
        """
        # For Iceberg engines, always use Iceberg strategy regardless of Glue config
        if self.is_iceberg_engine(connection_config.engine_type):
            return "iceberg"
        
        # For JDBC engines, check Glue Connection configuration
        if connection_config.uses_glue_connection():
            return connection_config.get_glue_connection_strategy()
        else:
            return "direct_jdbc"
    
    def create_connection(self, connection_config: ConnectionConfig) -> Any:
        """Create appropriate connection based on engine type and connection strategy.
        
        Args:
            connection_config: Connection configuration
            
        Returns:
            Connection object (DataFrame reader for JDBC, IcebergConnectionHandler for Iceberg)
            
        Raises:
            IcebergConnectionError: For Iceberg connection issues
            RuntimeError: For JDBC connection issues
            GlueConnectionError: For Glue Connection specific issues
        """
        try:
            # Determine connection strategy
            strategy = self._determine_connection_strategy(connection_config)
            
            self.structured_logger.info(
                "Creating connection with strategy",
                engine_type=connection_config.engine_type,
                strategy=strategy
            )
            
            # Route connection creation based on strategy
            if strategy == "iceberg":
                return self._create_iceberg_connection(connection_config)
            elif strategy in ["create_glue", "use_glue"]:
                return self._create_jdbc_connection_with_glue(connection_config)
            else:
                return self._create_jdbc_connection(connection_config)
                
        except (IcebergConnectionError, RuntimeError, GlueConnectionError):
            raise
        except Exception as e:
            self.structured_logger.error(
                "Unexpected error during connection creation",
                engine_type=connection_config.engine_type,
                error=str(e)
            )
            raise RuntimeError(f"Connection creation failed: {str(e)}")
    
    def _create_iceberg_connection(self, connection_config: ConnectionConfig) -> IcebergConnectionHandler:
        """Create Iceberg connection handler.
        
        Args:
            connection_config: Iceberg connection configuration
            
        Returns:
            IcebergConnectionHandler: Configured Iceberg handler
            
        Raises:
            IcebergConnectionError: If connection creation fails
        """
        try:
            # Configure Iceberg catalog
            # Get Iceberg configuration from the connection config
            connection_iceberg_config = connection_config.get_iceberg_config()
            if not connection_iceberg_config:
                raise IcebergConnectionError(
                    "Iceberg configuration missing for connection creation",
                    warehouse_location=""
                )
            
            warehouse_location = connection_iceberg_config.get('warehouse_location', '')
            catalog_id = connection_iceberg_config.get('catalog_id', None)
            
            if warehouse_location:
                self.iceberg_handler.configure_iceberg_catalog(
                    warehouse_location=warehouse_location,
                    catalog_id=catalog_id
                )
            
            self.structured_logger.info(
                "Created Iceberg connection",
                database=connection_config.database,
                warehouse_location=warehouse_location
            )
            
            return self.iceberg_handler
            
        except Exception as e:
            # Get warehouse location for error context
            connection_iceberg_config = connection_config.get_iceberg_config()
            warehouse_location = connection_iceberg_config.get('warehouse_location', '') if connection_iceberg_config else ''
            
            raise IcebergConnectionError(
                f"Failed to create Iceberg connection: {str(e)}",
                warehouse_location=warehouse_location,
                spark_error=e
            )
    
    def _create_jdbc_connection_with_glue(self, connection_config: ConnectionConfig) -> DataFrame:
        """Create JDBC connection using Glue Connection strategy.
        
        Args:
            connection_config: JDBC connection configuration with Glue Connection config
            
        Returns:
            DataFrame: Spark DataFrame reader configured for JDBC with Glue Connection
            
        Raises:
            GlueConnectionError: For Glue Connection specific issues
        """
        try:
            # Use the GlueConnectionManager to setup JDBC with the appropriate strategy
            jdbc_properties = self.jdbc_manager.glue_connection_manager.setup_jdbc_with_glue_connection_strategy(
                connection_config
            )
            
            # Create DataFrame reader with JDBC properties
            df_reader = self.spark.read.format('jdbc')
            
            # Configure connection properties
            for key, value in jdbc_properties.items():
                if not key.startswith('_'):  # Skip metadata keys
                    df_reader = df_reader.option(key, value)
            
            strategy = connection_config.get_glue_connection_strategy()
            self.structured_logger.info(
                "Created JDBC connection with Glue Connection strategy",
                engine_type=connection_config.engine_type,
                strategy=strategy,
                has_glue_metadata='_glue_connection_metadata' in jdbc_properties
            )
            
            return df_reader
            
        except GlueConnectionError:
            raise
        except Exception as e:
            self.structured_logger.error(
                "Failed to create JDBC connection with Glue Connection strategy",
                engine_type=connection_config.engine_type,
                strategy=connection_config.get_glue_connection_strategy(),
                error=str(e)
            )
            raise RuntimeError(f"Failed to create JDBC connection with Glue strategy: {str(e)}")
    
    def _create_jdbc_connection(self, connection_config: ConnectionConfig) -> DataFrame:
        """Create JDBC connection using direct JDBC (no Glue Connection).
        
        Args:
            connection_config: JDBC connection configuration
            
        Returns:
            DataFrame: Spark DataFrame reader configured for JDBC
        """
        return self.jdbc_manager.create_connection_with_glue_support(
            connection_config=connection_config,
            glue_connection_name=''  # Empty string for direct JDBC
        )
    
    def read_table(self, connection_config: ConnectionConfig, table_name: str,
                  query: Optional[str] = None, **options) -> DataFrame:
        """Read data from table with engine-specific routing.
        
        Args:
            connection_config: Connection configuration
            table_name: Table name to read from
            query: Optional custom query (JDBC only)
            **options: Additional read options
            
        Returns:
            DataFrame: Spark DataFrame containing table data
            
        Raises:
            IcebergConnectionError: For Iceberg read issues
            RuntimeError: For JDBC read issues
        """
        try:
            self.structured_logger.info(
                "Reading table data",
                engine_type=connection_config.engine_type,
                table_name=table_name
            )
            
            # Route read operation based on engine type
            if self.is_iceberg_engine(connection_config.engine_type):
                return self._read_iceberg_table(connection_config, table_name, **options)
            else:
                return self._read_jdbc_table(connection_config, table_name, query, **options)
                
        except (IcebergConnectionError, RuntimeError):
            raise
        except Exception as e:
            self.structured_logger.error(
                "Unexpected error during table read",
                engine_type=connection_config.engine_type,
                table_name=table_name,
                error=str(e)
            )
            raise RuntimeError(f"Table read failed: {str(e)}")
    
    def _read_iceberg_table(self, connection_config: ConnectionConfig,
                           table_name: str, **options) -> DataFrame:
        """Read data from Iceberg table.
        
        Args:
            connection_config: Iceberg connection configuration
            table_name: Table name to read from
            **options: Additional read options
            
        Returns:
            DataFrame: Spark DataFrame containing table data
        """
        catalog_id = getattr(connection_config, 'catalog_id', None)
        return self.iceberg_handler.read_table(
            database=connection_config.database,
            table=table_name,
            catalog_id=catalog_id,
            additional_options=options
        )
    
    def _read_jdbc_table(self, connection_config: ConnectionConfig,
                        table_name: str, query: Optional[str] = None,
                        **options) -> DataFrame:
        """Read data from JDBC table.
        
        Args:
            connection_config: JDBC connection configuration
            table_name: Table name to read from
            query: Optional custom query
            **options: Additional read options
            
        Returns:
            DataFrame: Spark DataFrame containing table data
        """
        return self.jdbc_manager.read_table_data(
            connection_config=connection_config,
            table_name=table_name,
            query=query,
            **options
        )
    
    def write_table(self, df: DataFrame, connection_config: ConnectionConfig,
                   table_name: str, mode: str = 'append', **options) -> None:
        """Write data to table with engine-specific routing.
        
        Args:
            df: DataFrame to write
            connection_config: Connection configuration
            table_name: Table name to write to
            mode: Write mode ('append', 'overwrite', 'create')
            **options: Additional write options
            
        Raises:
            IcebergConnectionError: For Iceberg write issues
            RuntimeError: For JDBC write issues
        """
        try:
            self.structured_logger.info(
                "Writing table data",
                engine_type=connection_config.engine_type,
                table_name=table_name,
                mode=mode
            )
            
            # Route write operation based on engine type
            if self.is_iceberg_engine(connection_config.engine_type):
                self._write_iceberg_table(df, connection_config, table_name, mode, **options)
            else:
                self._write_jdbc_table(df, connection_config, table_name, mode, **options)
                
        except (IcebergConnectionError, RuntimeError):
            raise
        except Exception as e:
            self.structured_logger.error(
                "Unexpected error during table write",
                engine_type=connection_config.engine_type,
                table_name=table_name,
                error=str(e)
            )
            raise RuntimeError(f"Table write failed: {str(e)}")
    
    def _write_iceberg_table(self, df: DataFrame, connection_config: ConnectionConfig,
                            table_name: str, mode: str, **options) -> None:
        """Write data to Iceberg table.
        
        Args:
            df: DataFrame to write
            connection_config: Iceberg connection configuration
            table_name: Table name to write to
            mode: Write mode
            **options: Additional write options
        """
        print(f"=== _WRITE_ICEBERG_TABLE CALLED FOR {table_name} ===")
        try:
            self.structured_logger.info(
                f"Starting Iceberg table write for {table_name}",
                database=connection_config.database,
                mode=mode
            )
            
            # Create Iceberg config from connection parameters
            # Get Iceberg configuration from the connection config
            connection_iceberg_config = connection_config.get_iceberg_config()
            if not connection_iceberg_config:
                raise ValueError(f"Iceberg configuration missing for connection")
            
            self.structured_logger.info(
                "Configuring Iceberg connection",
                warehouse_location=connection_iceberg_config.get('warehouse_location', ''),
                catalog_id=connection_iceberg_config.get('catalog_id', None)
            )
            
            iceberg_config = IcebergConfig(
                database_name=connection_config.database,
                table_name=table_name,
                warehouse_location=connection_iceberg_config.get('warehouse_location', ''),
                catalog_id=connection_iceberg_config.get('catalog_id', None),
                format_version=connection_iceberg_config.get('format_version', '2')
            )
            
            self.iceberg_handler.write_table(
                dataframe=df,
                database=connection_config.database,
                table=table_name,
                mode=mode,
                iceberg_config=iceberg_config
            )
            
            self.structured_logger.info(f"Successfully completed Iceberg table write for {table_name}")
            
        except Exception as e:
            self.structured_logger.error(
                f"Failed to write Iceberg table {table_name}",
                error=str(e),
                error_type=type(e).__name__
            )
            raise
    
    def _write_jdbc_table(self, df: DataFrame, connection_config: ConnectionConfig,
                         table_name: str, mode: str, **options) -> None:
        """Write data to JDBC table.
        
        Args:
            df: DataFrame to write
            connection_config: JDBC connection configuration
            table_name: Table name to write to
            mode: Write mode
            **options: Additional write options
        """
        self.jdbc_manager.write_table_data(
            df=df,
            connection_config=connection_config,
            table_name=table_name,
            mode=mode,
            **options
        )
    
    def get_table_schema(self, connection_config: ConnectionConfig,
                        table_name: str) -> StructType:
        """Get table schema with engine-specific routing.
        
        Args:
            connection_config: Connection configuration
            table_name: Table name to get schema for
            
        Returns:
            StructType: Spark schema for the table
            
        Raises:
            IcebergConnectionError: For Iceberg schema issues
            RuntimeError: For JDBC schema issues
        """
        try:
            self.structured_logger.info(
                "Getting table schema",
                engine_type=connection_config.engine_type,
                table_name=table_name
            )
            
            # Route schema retrieval based on engine type
            if self.is_iceberg_engine(connection_config.engine_type):
                return self._get_iceberg_table_schema(connection_config, table_name)
            else:
                return self._get_jdbc_table_schema(connection_config, table_name)
                
        except (IcebergConnectionError, RuntimeError):
            raise
        except Exception as e:
            self.structured_logger.error(
                "Unexpected error during schema retrieval",
                engine_type=connection_config.engine_type,
                table_name=table_name,
                error=str(e)
            )
            raise RuntimeError(f"Schema retrieval failed: {str(e)}")
    
    def _get_iceberg_table_schema(self, connection_config: ConnectionConfig,
                                 table_name: str) -> StructType:
        """Get schema for Iceberg table.
        
        Args:
            connection_config: Iceberg connection configuration
            table_name: Table name to get schema for
            
        Returns:
            StructType: Spark schema for the table
        """
        # Read table with limit 0 to get schema
        df = self._read_iceberg_table(connection_config, table_name)
        return df.limit(0).schema
    
    def _get_jdbc_table_schema(self, connection_config: ConnectionConfig,
                              table_name: str) -> StructType:
        """Get schema for JDBC table.
        
        Args:
            connection_config: JDBC connection configuration
            table_name: Table name to get schema for
            
        Returns:
            StructType: Spark schema for the table
        """
        return self.jdbc_manager.get_table_schema(connection_config, table_name)
    
    def _get_validation_cache_key(self, connection_config: ConnectionConfig) -> str:
        """Generate cache key for connection validation.
        
        Args:
            connection_config: Connection configuration
            
        Returns:
            str: Cache key for validation results
        """
        if self.is_iceberg_engine(connection_config.engine_type):
            # Get Iceberg configuration from the connection config
            connection_iceberg_config = connection_config.get_iceberg_config()
            if connection_iceberg_config:
                warehouse_location = connection_iceberg_config.get('warehouse_location', '')
                catalog_id = connection_iceberg_config.get('catalog_id', '')
            else:
                warehouse_location = ''
                catalog_id = ''
            return f"iceberg_{connection_config.database}_{warehouse_location}_{catalog_id}"
        else:
            # Include Glue Connection strategy in cache key for JDBC connections
            strategy = self._determine_connection_strategy(connection_config)
            glue_connection_name = connection_config.get_glue_connection_name_for_creation() or ''
            return f"jdbc_{connection_config.engine_type}_{connection_config.database}_{connection_config.schema}_{connection_config.username}_{strategy}_{glue_connection_name}"
    
    # Compatibility methods for existing migration classes
    def read_table_data(self, connection_config: ConnectionConfig, table_name: str,
                       query: Optional[str] = None, **options) -> DataFrame:
        """Compatibility method for migration classes - delegates to read_table.
        
        Args:
            connection_config: Connection configuration
            table_name: Table name to read from
            query: Optional custom query (JDBC only)
            **options: Additional read options
            
        Returns:
            DataFrame: Spark DataFrame containing table data
        """
        return self.read_table(connection_config, table_name, query, **options)
    
    def write_table_data(self, df: DataFrame, connection_config: ConnectionConfig,
                        table_name: str, mode: str = 'append', **options) -> None:
        """Compatibility method for migration classes - delegates to write_table.
        
        Args:
            df: DataFrame to write
            connection_config: Connection configuration
            table_name: Table name to write to
            mode: Write mode ('append', 'overwrite', 'create')
            **options: Additional write options
        """
        self.write_table(df, connection_config, table_name, mode, **options)
    
    def read_table_data_partitioned(
        self,
        connection_config: ConnectionConfig,
        table_name: str,
        partition_column: str,
        lower_bound: int,
        upper_bound: int,
        num_partitions: int,
        fetch_size: int = 10000,
        **options
    ) -> DataFrame:
        """Compatibility method for migration classes - delegates to JDBC manager for partitioned reads.
        
        This method is only supported for JDBC connections, not Iceberg.
        
        Args:
            connection_config: Database connection configuration
            table_name: Name of the table to read
            partition_column: Numeric column to partition on (e.g., 'id')
            lower_bound: Minimum value of partition column
            upper_bound: Maximum value of partition column
            num_partitions: Number of parallel read partitions
            fetch_size: Rows to fetch per JDBC round-trip
            **options: Additional JDBC options
            
        Returns:
            DataFrame with data distributed across partitions
            
        Raises:
            ValueError: If called for Iceberg engine (not supported)
        """
        if self.is_iceberg_engine(connection_config.engine_type):
            raise ValueError(
                f"Partitioned reads are not supported for Iceberg engine. "
                f"Use read_table_data() instead for {connection_config.engine_type}."
            )
        
        return self.jdbc_manager.read_table_data_partitioned(
            connection_config=connection_config,
            table_name=table_name,
            partition_column=partition_column,
            lower_bound=lower_bound,
            upper_bound=upper_bound,
            num_partitions=num_partitions,
            fetch_size=fetch_size,
            **options
        )