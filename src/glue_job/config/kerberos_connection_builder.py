"""
Kerberos connection builder for AWS Glue Data Replication

This module provides utilities for building Kerberos-enabled database connections
and generating appropriate connection properties for Glue Connections.
"""

import logging
from typing import Dict, Optional, Any
from dataclasses import dataclass

from .kerberos_config import KerberosConfig, KerberosEngineCompatibilityError
from .database_engines import DatabaseEngineManager
from .kerberos_environment import KerberosEnvironmentManager
from ..monitoring.logging import StructuredLogger

logger = logging.getLogger(__name__)


@dataclass
class KerberosConnectionProperties:
    """Container for Kerberos connection properties."""
    jdbc_connection_url: str
    authentication_type: str
    connection_properties: Dict[str, str]
    kerberos_properties: Dict[str, str]


class KerberosConnectionBuilder:
    """Builder for Kerberos-enabled database connections."""
    
    def __init__(self):
        """Initialize the Kerberos connection builder."""
        self.structured_logger = StructuredLogger("KerberosConnectionBuilder")
        self.environment_manager = KerberosEnvironmentManager()
        
        # Define engines that support Kerberos authentication
        self.kerberos_supported_engines = ['oracle', 'sqlserver', 'postgresql', 'db2']
    
    def validate_engine_kerberos_support(self, engine_type: str) -> bool:
        """Validate that the engine supports Kerberos authentication.
        
        Args:
            engine_type: Database engine type
            
        Returns:
            bool: True if engine supports Kerberos, False otherwise
            
        Raises:
            KerberosEngineCompatibilityError: If engine doesn't support Kerberos
        """
        engine_type = engine_type.lower()
        
        if engine_type not in self.kerberos_supported_engines:
            raise KerberosEngineCompatibilityError(
                f"Kerberos authentication is not supported for engine '{engine_type}'. "
                f"Supported engines: {', '.join(self.kerberos_supported_engines)}"
            )
        
        self.structured_logger.debug(
            "Engine Kerberos support validated",
            engine_type=engine_type,
            supported=True
        )
        
        return True
    
    def build_kerberos_connection_properties(self, 
                                           engine_type: str,
                                           connection_string: str,
                                           database: str,
                                           kerberos_config: KerberosConfig,
                                           additional_properties: Optional[Dict[str, str]] = None) -> KerberosConnectionProperties:
        """Build connection properties for Kerberos authentication.
        
        Args:
            engine_type: Database engine type
            connection_string: Base JDBC connection string
            database: Database name
            kerberos_config: Kerberos configuration
            additional_properties: Additional connection properties
            
        Returns:
            KerberosConnectionProperties: Complete connection properties for Kerberos
            
        Raises:
            KerberosEngineCompatibilityError: If engine doesn't support Kerberos
            ValueError: If required parameters are missing
        """
        try:
            # Validate engine support
            self.validate_engine_kerberos_support(engine_type)
            
            # Validate Kerberos configuration
            if not kerberos_config or not kerberos_config.is_complete():
                raise ValueError("Complete Kerberos configuration is required")
            
            self.structured_logger.info(
                "Building Kerberos connection properties",
                engine_type=engine_type,
                database=database,
                spn=kerberos_config.spn,
                domain=kerberos_config.domain,
                kdc=kerberos_config.kdc
            )
            
            # Build engine-specific Kerberos connection properties
            kerberos_properties = self._build_engine_specific_kerberos_properties(
                engine_type, kerberos_config
            )
            
            # Build enhanced JDBC connection URL with Kerberos properties
            enhanced_connection_url = self._build_kerberos_jdbc_url(
                engine_type, connection_string, kerberos_config
            )
            
            # Build base connection properties
            connection_properties = {
                'driver': DatabaseEngineManager.get_driver_class(engine_type),
                'fetchsize': '10000',
                'batchsize': '10000'
            }
            
            # Add engine-specific base properties
            connection_properties.update(
                self._get_engine_base_properties(engine_type)
            )
            
            # Add additional properties if provided
            if additional_properties:
                connection_properties.update(additional_properties)
            
            # Create the complete properties object
            result = KerberosConnectionProperties(
                jdbc_connection_url=enhanced_connection_url,
                authentication_type='KERBEROS',
                connection_properties=connection_properties,
                kerberos_properties=kerberos_properties
            )
            
            self.structured_logger.info(
                "Kerberos connection properties built successfully",
                engine_type=engine_type,
                authentication_type=result.authentication_type,
                properties_count=len(result.connection_properties),
                kerberos_properties_count=len(result.kerberos_properties)
            )
            
            return result
            
        except Exception as e:
            self.structured_logger.error(
                "Failed to build Kerberos connection properties",
                engine_type=engine_type,
                error=str(e),
                error_type=type(e).__name__
            )
            raise
    
    def _build_engine_specific_kerberos_properties(self, 
                                                 engine_type: str, 
                                                 kerberos_config: KerberosConfig) -> Dict[str, str]:
        """Build engine-specific Kerberos properties.
        
        Args:
            engine_type: Database engine type
            kerberos_config: Kerberos configuration
            
        Returns:
            Dict[str, str]: Engine-specific Kerberos properties
        """
        engine_type = engine_type.lower()
        
        # For AWS Glue Connections, don't include custom Kerberos properties
        # AWS Glue validation doesn't recognize KERBEROS_SPN, KERBEROS_DOMAIN, etc.
        # Kerberos authentication is handled entirely at runtime through:
        # 1. krb5.conf file generation
        # 2. Java system properties setup
        # 3. JDBC URL parameters (integratedSecurity=true for SQL Server)
        
        if engine_type == 'oracle':
            # Oracle-specific properties that AWS Glue might accept
            return {}
        
        elif engine_type == 'sqlserver':
            # SQL Server - no additional properties needed for Glue Connection
            return {}
        
        elif engine_type == 'postgresql':
            # PostgreSQL - no additional properties needed for Glue Connection
            return {}
        
        elif engine_type == 'db2':
            # DB2 - no additional properties needed for Glue Connection
            return {}
        
        else:
            # Default - no custom Kerberos properties for Glue Connection
            return {}
    
    def _build_kerberos_jdbc_url(self, 
                                engine_type: str, 
                                base_connection_string: str, 
                                kerberos_config: KerberosConfig) -> str:
        """Build JDBC connection URL with Kerberos-specific parameters.
        
        Args:
            engine_type: Database engine type
            base_connection_string: Base JDBC connection string
            kerberos_config: Kerberos configuration
            
        Returns:
            str: Enhanced JDBC connection URL with Kerberos parameters
        """
        engine_type = engine_type.lower()
        
        # Check if connection string already has Kerberos parameters
        existing_params = self._parse_existing_kerberos_params(base_connection_string, engine_type)
        
        if engine_type == 'oracle':
            # Oracle Kerberos URL parameters
            kerberos_params = []
            if 'oracle.net.authentication_services' not in existing_params:
                kerberos_params.append('oracle.net.authentication_services=KERBEROS5')
            if 'oracle.net.kerberos5_mutual_authentication' not in existing_params:
                kerberos_params.append('oracle.net.kerberos5_mutual_authentication=true')
            
        elif engine_type == 'sqlserver':
            # SQL Server requires serverSpn parameter for Kerberos authentication
            kerberos_params = []
            if 'serverSpn' not in existing_params:
                kerberos_params.append(f'serverSpn={kerberos_config.spn}')
            
            self.structured_logger.debug(
                "Adding SQL Server Kerberos parameters",
                spn=kerberos_config.spn,
                params_added=kerberos_params
            )
            
        elif engine_type == 'postgresql':
            # PostgreSQL Kerberos URL parameters
            server_name = kerberos_config.spn.split('/')[0] if '/' in kerberos_config.spn else 'postgres'
            kerberos_params = []
            if 'gsslib' not in existing_params:
                kerberos_params.append('gsslib=gssapi')
            if 'kerberosServerName' not in existing_params:
                kerberos_params.append(f'kerberosServerName={server_name}')
            
        elif engine_type == 'db2':
            # DB2 Kerberos URL parameters
            kerberos_params = []
            if 'securityMechanism' not in existing_params:
                kerberos_params.append('securityMechanism=11')
            if 'kerberosServerPrincipal' not in existing_params:
                kerberos_params.append(f'kerberosServerPrincipal={kerberos_config.spn}')
            
        else:
            # Default - no URL modifications
            kerberos_params = []
        
        # Add Kerberos parameters to the connection string only if not already present
        if kerberos_params:
            separator = '&' if '?' in base_connection_string else '?'
            enhanced_url = base_connection_string + separator + '&'.join(kerberos_params)
        else:
            enhanced_url = base_connection_string
        
        self.structured_logger.debug(
            "Built Kerberos JDBC URL",
            engine_type=engine_type,
            has_kerberos_params=bool(kerberos_params),
            param_count=len(kerberos_params),
            existing_params_count=len(existing_params)
        )
        
        return enhanced_url
    
    def _parse_existing_kerberos_params(self, connection_string: str, engine_type: str) -> Dict[str, str]:
        """Parse existing Kerberos parameters from connection string.
        
        Args:
            connection_string: JDBC connection string
            engine_type: Database engine type
            
        Returns:
            Dict[str, str]: Existing Kerberos parameters
        """
        existing_params = {}
        
        # Extract parameters from connection string
        if '?' in connection_string:
            params_part = connection_string.split('?', 1)[1]
            for param in params_part.split('&'):
                if '=' in param:
                    key, value = param.split('=', 1)
                    existing_params[key.lower()] = value
        
        # Log existing Kerberos-related parameters
        kerberos_related = {k: v for k, v in existing_params.items() 
                          if any(kw in k.lower() for kw in ['kerberos', 'auth', 'integrated', 'gss'])}
        
        if kerberos_related:
            self.structured_logger.debug(
                "Found existing Kerberos parameters in connection string",
                engine_type=engine_type,
                existing_params=kerberos_related
            )
        
        return existing_params
    
    def _get_engine_base_properties(self, engine_type: str) -> Dict[str, str]:
        """Get base connection properties for the engine type.
        
        Args:
            engine_type: Database engine type
            
        Returns:
            Dict[str, str]: Base connection properties
        """
        engine_type = engine_type.lower()
        
        if engine_type == 'oracle':
            return {
                'oracle.jdbc.timezoneAsRegion': 'false',
                'oracle.net.CONNECT_TIMEOUT': '30000',
                'oracle.jdbc.ReadTimeout': '60000'
            }
        
        elif engine_type == 'sqlserver':
            return {
                'loginTimeout': '30',
                'socketTimeout': '60000',
                'selectMethod': 'cursor'
            }
        
        elif engine_type == 'postgresql':
            return {
                'connectTimeout': '30',
                'socketTimeout': '60',
                'tcpKeepAlive': 'true'
            }
        
        elif engine_type == 'db2':
            return {
                'loginTimeout': '30',
                'blockingReadConnectionTimeout': '60000',
                'resultSetHoldability': '1'
            }
        
        else:
            return {}
    
    def build_glue_connection_properties_for_kerberos(self, 
                                                    engine_type: str,
                                                    connection_string: str,
                                                    database: str,
                                                    kerberos_config: KerberosConfig,
                                                    enable_ssl: bool = True,
                                                    additional_properties: Optional[Dict[str, str]] = None) -> Dict[str, str]:
        """Build Glue Connection properties for Kerberos authentication.
        
        This method creates the properties dictionary that can be used directly
        with AWS Glue Connection creation APIs.
        
        Args:
            engine_type: Database engine type
            connection_string: Base JDBC connection string
            database: Database name
            kerberos_config: Kerberos configuration
            enable_ssl: Whether to enable SSL/encryption for the connection
            additional_properties: Additional connection properties
            
        Returns:
            Dict[str, str]: Complete Glue Connection properties
        """
        try:
            # Build the complete Kerberos connection properties
            kerberos_props = self.build_kerberos_connection_properties(
                engine_type, connection_string, database, kerberos_config, additional_properties
            )
            
            # Set up Kerberos environment (krb5.conf and Java properties) but don't include in Glue properties
            # This happens at runtime, not in the Glue Connection definition
            self.environment_manager.setup_kerberos_environment(kerberos_config)
            
            # Create Glue Connection properties format (only standard AWS Glue properties)
            glue_properties = {
                'JDBC_CONNECTION_URL': kerberos_props.jdbc_connection_url,
                # Don't include custom Kerberos properties that AWS Glue doesn't recognize
                # The Kerberos environment setup happens at runtime, not in Glue Connection properties
            }
            
            # Don't add SSL properties to Glue Connection for now to avoid validation issues
            # SSL can be configured at runtime if needed
            # ssl_properties = self._build_ssl_properties_for_kerberos(engine_type, enable_ssl)
            # glue_properties.update(ssl_properties)
            
            # Add minimal connection properties that AWS Glue will accept
            for key, value in kerberos_props.connection_properties.items():
                if key not in ['driver']:  # Skip driver as it's handled separately
                    # Only add basic properties that are known to work with AWS Glue
                    if key.lower() in ['fetchsize', 'batchsize']:
                        glue_properties[f'JDBC_{key.upper()}'] = str(value)
            
            self.structured_logger.info(
                "Built Glue Connection properties for Kerberos",
                engine_type=engine_type,
                authentication_type=kerberos_props.authentication_type,
                ssl_enabled=enable_ssl,
                total_properties=len(glue_properties)
            )
            
            return glue_properties
            
        except Exception as e:
            self.structured_logger.error(
                "Failed to build Glue Connection properties for Kerberos",
                engine_type=engine_type,
                error=str(e)
            )
            raise
    
    def _build_ssl_properties_for_kerberos(self, engine_type: str, enable_ssl: bool) -> Dict[str, str]:
        """Build SSL/encryption properties appropriate for Kerberos connections.
        
        Args:
            engine_type: Database engine type
            enable_ssl: Whether to enable SSL/encryption
            
        Returns:
            Dict[str, str]: SSL properties for the connection
        """
        if not enable_ssl:
            return {}
        
        engine_type = engine_type.lower()
        ssl_properties = {}
        
        if engine_type == 'oracle':
            # Oracle SSL properties for Kerberos
            ssl_properties.update({
                'JDBC_ORACLE_NET_SSL_VERSION': '1.2',
                'JDBC_ORACLE_NET_SSL_CIPHER_SUITES': 'SSL_RSA_WITH_AES_256_CBC_SHA256',
                'JDBC_ORACLE_NET_ENCRYPTION_CLIENT': 'REQUIRED',
                'JDBC_ORACLE_NET_ENCRYPTION_TYPES_CLIENT': 'AES256'
            })
        
        elif engine_type == 'sqlserver':
            # SQL Server SSL properties for Kerberos
            ssl_properties.update({
                'JDBC_ENCRYPT': 'true',
                'JDBC_TRUSTSERVERCERTIFICATE': 'false',
                'JDBC_HOSTNAMEINCERTIFICATE': '*'
            })
        
        elif engine_type == 'postgresql':
            # PostgreSQL SSL properties for Kerberos
            ssl_properties.update({
                'JDBC_SSL': 'true',
                'JDBC_SSLMODE': 'require',
                'JDBC_SSLCERT': '/opt/amazon/glue/lib/client.crt',
                'JDBC_SSLKEY': '/opt/amazon/glue/lib/client.key'
            })
        
        elif engine_type == 'db2':
            # DB2 SSL properties for Kerberos
            ssl_properties.update({
                'JDBC_SSLCONNECTION': 'true',
                'JDBC_SSLTRUSTSTORE': '/opt/amazon/glue/lib/truststore.jks',
                'JDBC_SSLTRUSTSTOREPASSWORD': 'changeit'
            })
        
        self.structured_logger.debug(
            "Built SSL properties for Kerberos connection",
            engine_type=engine_type,
            ssl_enabled=enable_ssl,
            properties_count=len(ssl_properties)
        )
        
        return ssl_properties