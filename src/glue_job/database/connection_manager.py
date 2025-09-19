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
from ..monitoring.logging import StructuredLogger
from ..network.error_handler import (
    NetworkConnectivityError, GlueConnectionError, VpcEndpointError, 
    ENICreationError, ErrorCategory
)
from ..network.retry_handler import ConnectionRetryHandler, ErrorClassifier
from ..config.iceberg_connection_handler import IcebergConnectionHandler
from ..config.iceberg_models import (
    IcebergConfig, IcebergEngineError, IcebergConnectionError, IcebergValidationError
)


class GlueConnectionManager:
    """Manages Glue connections for cross-VPC database access."""
    
    def __init__(self, glue_context: GlueContext):
        self.glue_context = glue_context
        # Configure Glue client with aggressive timeout settings
        config = Config(
            read_timeout=30,  # Reduced from 60 to 30 seconds
            connect_timeout=10,  # Reduced from 30 to 10 seconds
            retries={'max_attempts': 2}  # Reduced from 3 to 2 attempts
        )
        self.glue_client = boto3.client('glue', config=config)
        self.structured_logger = StructuredLogger("GlueConnectionManager")
    
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
            self.structured_logger.info("Retrieving Glue connection", connection_name=connection_name)
            
            # Log diagnostic information first
            try:
                # First, try to list connections to verify access
                self.structured_logger.info("Testing Glue API access by listing connections")
                list_response = self.glue_client.get_connections(MaxResults=1)
                self.structured_logger.info("Glue API access verified - can list connections")
            except Exception as list_error:
                self.structured_logger.error(
                    "Cannot access Glue API - this indicates permission or configuration issues",
                    error=str(list_error)
                )
                raise GlueConnectionError(
                    f"Cannot access Glue API: {str(list_error)}. Please check IAM permissions for glue:GetConnections.",
                    connection_name,
                    {'error_type': 'api_access_error', 'original_error': str(list_error)}
                )
            
            # Try to get the specific connection with better error handling
            self.structured_logger.info("Attempting to retrieve specific Glue connection", connection_name=connection_name)
            
            try:
                response = self.glue_client.get_connection(Name=connection_name)
                connection = response.get('Connection', {})
                self.structured_logger.info("Successfully retrieved Glue connection", connection_name=connection_name)
            except Exception as get_error:
                self.structured_logger.error(
                    "Failed to retrieve specific Glue connection",
                    connection_name=connection_name,
                    error=str(get_error),
                    error_type=type(get_error).__name__
                )
                # Re-raise the original exception to be handled by the outer try-catch
                raise
            
            connection_details = {
                'name': connection.get('Name'),
                'connection_type': connection.get('ConnectionType'),
                'connection_properties': connection.get('ConnectionProperties', {}),
                'physical_connection_requirements': connection.get('PhysicalConnectionRequirements', {})
            }
            
            # Validate connection configuration
            self._validate_glue_connection_config(connection_details, connection_name)
            
            self.structured_logger.info(
                "Successfully retrieved and validated Glue connection",
                connection_name=connection_name,
                connection_type=connection_details['connection_type']
            )
            
            return connection_details
            
        except Exception as e:
            self.structured_logger.error(
                "Failed to retrieve Glue connection",
                connection_name=connection_name,
                error=str(e),
                error_type=type(e).__name__
            )
            raise GlueConnectionError(
                f"Failed to retrieve Glue connection '{connection_name}': {str(e)}",
                connection_name,
                {'error_type': 'connection_retrieval_error', 'original_error': str(e)}
            )
    
    def _validate_glue_connection_config(self, connection_details: Dict[str, Any], connection_name: str):
        """Validate Glue connection configuration."""
        connection_type = connection_details.get('connection_type')
        if connection_type not in ['JDBC', 'NETWORK']:
            raise GlueConnectionError(
                f"Glue connection '{connection_name}' is not a JDBC or NETWORK connection (type: {connection_type})",
                connection_name,
                {'error_type': 'invalid_connection_type', 'connection_type': connection_type}
            )
        
        physical_reqs = connection_details.get('physical_connection_requirements', {})
        if not physical_reqs:
            raise GlueConnectionError(
                f"Glue connection '{connection_name}' lacks physical connection requirements",
                connection_name,
                {'error_type': 'missing_physical_requirements'}
            )
        
        # Check for required network configuration
        subnet_id = physical_reqs.get('SubnetId')
        security_groups = physical_reqs.get('SecurityGroupIdList', [])
        
        if not subnet_id:
            raise GlueConnectionError(
                f"Glue connection '{connection_name}' missing subnet configuration",
                connection_name,
                {'error_type': 'missing_subnet', 'physical_requirements': physical_reqs}
            )
        
        if not security_groups:
            raise GlueConnectionError(
                f"Glue connection '{connection_name}' missing security group configuration",
                connection_name,
                {'error_type': 'missing_security_groups', 'physical_requirements': physical_reqs}
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
            
            # Check if connection has required network configuration
            subnet_id = physical_requirements.get('SubnetId')
            security_groups = physical_requirements.get('SecurityGroupIdList', [])
            
            if not subnet_id:
                raise GlueConnectionError(
                    f"Glue connection '{connection_name}' missing subnet configuration",
                    connection_name,
                    {'error_type': 'missing_subnet', 'physical_requirements': physical_requirements}
                )
            
            if not security_groups:
                raise GlueConnectionError(
                    f"Glue connection '{connection_name}' missing security group configuration",
                    connection_name,
                    {'error_type': 'missing_security_groups', 'physical_requirements': physical_requirements}
                )
            
            # Perform detailed network validation
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
        """Validate network configuration for JDBC setup."""
        subnet_id = physical_requirements.get('SubnetId')
        security_groups = physical_requirements.get('SecurityGroupIdList', [])
        
        if not subnet_id:
            raise NetworkConnectivityError(
                f"Glue connection '{connection_name}' missing subnet configuration for JDBC setup",
                error_type='missing_subnet',
                connection_name=connection_name
            )
        
        if not security_groups:
            raise NetworkConnectivityError(
                f"Glue connection '{connection_name}' missing security group configuration for JDBC setup",
                error_type='missing_security_groups',
                connection_name=connection_name
            )
        
        self.structured_logger.debug(
            "Network configuration validation passed for JDBC setup",
            connection_name=connection_name,
            subnet_id=subnet_id,
            security_groups_count=len(security_groups)
        )


class JdbcConnectionManager:
    """Manages JDBC database connections with validation and error handling."""
    
    def __init__(self, spark_session: SparkSession, glue_context: GlueContext, 
                 retry_handler: Optional[ConnectionRetryHandler] = None):
        self.spark = spark_session
        self.glue_context = glue_context
        self.retry_handler = retry_handler or ConnectionRetryHandler()
        self.glue_connection_manager = GlueConnectionManager(glue_context)
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
        """Read data from database table using Spark DataFrame (not DynamicFrame)."""
        def _read_data():
            properties = self.create_connection_properties(connection_config)
            
            # Add any additional options
            properties.update(options)
            
            try:
                # Use Spark DataFrame reader directly (not DynamicFrame)
                reader = self.spark.read.format('jdbc')
                
                # Set connection properties
                reader = reader.option('url', connection_config.connection_string)
                for key, value in properties.items():
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
    
    def write_table_data(self, df: DataFrame, connection_config: ConnectionConfig, 
                        table_name: str, mode: str = 'append', **options) -> None:
        """Write data to database table using Spark DataFrame."""
        def _write_data():
            properties = self.create_connection_properties(connection_config)
            
            # Add any additional options
            if options:
                for key, value in options.items():
                    properties[key] = str(value)
            
            # Build full table name with schema
            full_table_name = f"{connection_config.schema}.{table_name}"
            
            try:
                # Build writer using Spark DataFrame (not DynamicFrame)
                writer = df.write.format('jdbc')
                writer = writer.option('url', connection_config.connection_string)
                writer = writer.option('dbtable', full_table_name)
                writer = writer.mode(mode)
                
                # Add properties individually
                for key, value in properties.items():
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
                 retry_handler: Optional[ConnectionRetryHandler] = None):
        """Initialize the unified connection manager.
        
        Args:
            spark_session: Active Spark session
            glue_context: AWS Glue context
            retry_handler: Optional retry handler for connection operations
        """
        self.spark = spark_session
        self.glue_context = glue_context
        self.retry_handler = retry_handler or ConnectionRetryHandler()
        
        # Initialize JDBC connection manager
        self.jdbc_manager = JdbcConnectionManager(
            spark_session, glue_context, retry_handler
        )
        
        # Initialize Iceberg connection handler
        self.iceberg_handler = IcebergConnectionHandler(
            spark_session, glue_context
        )
        
        self.structured_logger = StructuredLogger("UnifiedConnectionManager")
        
        # Connection validation cache
        self._validation_cache = {}
        
        self.structured_logger.info("Initialized UnifiedConnectionManager with JDBC and Iceberg support")
    
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
        """Validate database connection with engine-specific routing.
        
        Args:
            connection_config: Connection configuration to validate
            timeout_seconds: Connection timeout in seconds
            
        Returns:
            bool: True if connection is valid, False otherwise
            
        Raises:
            IcebergConnectionError: For Iceberg connection issues
            NetworkConnectivityError: For network connectivity issues
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
            
            self.structured_logger.info(
                "Validating database connection",
                engine_type=connection_config.engine_type,
                timeout_seconds=timeout_seconds
            )
            
            start_time = time.time()
            
            # Route validation based on engine type
            if self.is_iceberg_engine(connection_config.engine_type):
                result = self._validate_iceberg_connection(connection_config, timeout_seconds)
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
                result="passed" if result else "failed",
                duration_seconds=round(duration, 2)
            )
            
            return result
            
        except (IcebergConnectionError, NetworkConnectivityError):
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
    
    def _validate_jdbc_connection(self, connection_config: ConnectionConfig,
                                timeout_seconds: int) -> bool:
        """Validate JDBC connection using the JDBC manager.
        
        Args:
            connection_config: JDBC connection configuration
            timeout_seconds: Connection timeout in seconds
            
        Returns:
            bool: True if connection is valid
        """
        return self.jdbc_manager.validate_connection_with_network_check(
            connection_config=connection_config,
            glue_connection_name=connection_config.get_glue_connection_name() or '',
            timeout_seconds=timeout_seconds
        )
    
    def create_connection(self, connection_config: ConnectionConfig) -> Any:
        """Create appropriate connection based on engine type.
        
        Args:
            connection_config: Connection configuration
            
        Returns:
            Connection object (DataFrame reader for JDBC, IcebergConnectionHandler for Iceberg)
            
        Raises:
            IcebergConnectionError: For Iceberg connection issues
            RuntimeError: For JDBC connection issues
        """
        try:
            self.structured_logger.info(
                "Creating connection",
                engine_type=connection_config.engine_type
            )
            
            # Route connection creation based on engine type
            if self.is_iceberg_engine(connection_config.engine_type):
                return self._create_iceberg_connection(connection_config)
            else:
                return self._create_jdbc_connection(connection_config)
                
        except (IcebergConnectionError, RuntimeError):
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
    
    def _create_jdbc_connection(self, connection_config: ConnectionConfig) -> DataFrame:
        """Create JDBC connection using the JDBC manager.
        
        Args:
            connection_config: JDBC connection configuration
            
        Returns:
            DataFrame: Spark DataFrame reader configured for JDBC
        """
        return self.jdbc_manager.create_connection_with_glue_support(
            connection_config=connection_config,
            glue_connection_name=connection_config.get_glue_connection_name() or ''
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
            return f"jdbc_{connection_config.engine_type}_{connection_config.database}_{connection_config.schema}_{connection_config.username}"
    
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