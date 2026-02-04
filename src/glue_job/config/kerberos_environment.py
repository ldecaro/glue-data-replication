"""
Kerberos environment manager for AWS Glue Data Replication

This module provides utilities for setting up the Kerberos environment
in AWS Glue jobs, including krb5.conf generation and Java system properties.
"""

import os
import sys
import tempfile
import logging
from typing import Optional, Dict
from pathlib import Path

try:
    from .kerberos_config import KerberosConfig, KerberosConfigurationError
    from ..monitoring.logging import StructuredLogger
except ImportError:
    # Fallback for direct execution
    from kerberos_config import KerberosConfig, KerberosConfigurationError
    import logging
    
    # Mock StructuredLogger for testing
    class StructuredLogger:
        def __init__(self, name):
            self.logger = logging.getLogger(name)
        
        def info(self, message, **kwargs):
            self.logger.info(f"{message} {kwargs}")
        
        def debug(self, message, **kwargs):
            self.logger.debug(f"{message} {kwargs}")
        
        def error(self, message, **kwargs):
            self.logger.error(f"{message} {kwargs}")
        
        def warning(self, message, **kwargs):
            self.logger.warning(f"{message} {kwargs}")


class KerberosEnvironmentManager:
    """Manager for Kerberos environment setup in AWS Glue jobs."""
    
    def __init__(self):
        """Initialize the Kerberos environment manager."""
        self.structured_logger = StructuredLogger("KerberosEnvironmentManager")
        self.krb5_conf_path: Optional[str] = None
        self._original_java_properties: Dict[str, Optional[str]] = {}
    
    def setup_kerberos_environment(self, kerberos_config: KerberosConfig, 
                                 username: str = None, password: str = None,
                                 keytab_s3_path: str = None) -> str:
        """Set up the complete Kerberos environment for JDBC connections.
        
        Args:
            kerberos_config: Kerberos configuration parameters
            username: Kerberos principal username (for username/password auth)
            password: User password (for username/password auth)
            keytab_s3_path: S3 path to keytab file (for keytab auth, e.g., s3://bucket/path/user.keytab)
            
        Returns:
            str: Path to the generated krb5.conf file
            
        Raises:
            KerberosConfigurationError: If environment setup fails
        """
        try:
            import os
            pid = os.getpid()
            
            self.structured_logger.info(
                "Setting up Kerberos environment - JVM Process Info",
                domain=kerberos_config.domain,
                kdc=kerberos_config.kdc,
                jvm_pid=pid,
                has_credentials=bool(username and password),
                note="Tracking which JVM process sets up Kerberos environment"
            )
            
            # Generate krb5.conf file
            krb5_conf_path = self._generate_krb5_conf(kerberos_config)
            
            # Set Java system properties ON THIS JVM (driver or executor)
            self._set_java_kerberos_properties(krb5_conf_path, kerberos_config)
            
            # Store Kerberos configuration in environment variables for executor access
            # This allows executors to set up their own Kerberos environment
            self._store_kerberos_config_for_executors(kerberos_config, username, password, keytab_s3_path)
            

            
            # Store the path for cleanup
            self.krb5_conf_path = krb5_conf_path
            
            self.structured_logger.info(
                "Kerberos environment setup completed",
                krb5_conf_path=krb5_conf_path,
                java_properties_set=True
            )
            
            # Set up authentication method - keytab takes precedence over username/password
            if keytab_s3_path:
                self.structured_logger.info(
                    "Setting up keytab-only authentication for JDBC driver",
                    keytab_s3_path=keytab_s3_path,
                    domain=kerberos_config.domain,
                    note="Pure keytab authentication - may fail with some JDBC drivers"
                )
                
                keytab_setup = self.setup_keytab_authentication(
                    keytab_s3_path, username, kerberos_config.domain
                )
                
                if keytab_setup:
                    self.structured_logger.info(
                        "Successfully configured keytab-only authentication",
                        keytab_s3_path=keytab_s3_path,
                        domain=kerberos_config.domain
                    )
                else:
                    self.structured_logger.warning(
                        "Failed to set up keytab authentication - authentication may fail",
                        keytab_s3_path=keytab_s3_path,
                        domain=kerberos_config.domain
                    )
            elif username and password:
                self.structured_logger.info(
                    "Setting up username/password credentials for JDBC driver automatic ticket acquisition",
                    username=username,
                    domain=kerberos_config.domain
                )
                
                credentials_setup = self.setup_credentials_for_executors(
                    username, password, kerberos_config.domain
                )
                
                if credentials_setup:
                    self.structured_logger.info(
                        "Successfully configured credentials for executors - JDBC driver will handle ticket acquisition",
                        username=username,
                        domain=kerberos_config.domain
                    )
                else:
                    self.structured_logger.warning(
                        "Failed to set up credentials for executors - authentication may fail",
                        username=username,
                        domain=kerberos_config.domain
                    )
            else:
                self.structured_logger.info(
                    "No credentials or keytab provided - Kerberos environment configured but no authentication method",
                    note="Executors will likely fail without valid credentials or keytab"
                )
            
            # Log completion of environment setup - JDBC driver will handle ticket acquisition automatically
            # Determine authentication method based on what's configured
            if keytab_s3_path and username and password:
                auth_method = "hybrid_keytab_and_password"
            elif keytab_s3_path:
                auth_method = "keytab_only"
            elif username and password:
                auth_method = "username_password"
            else:
                auth_method = "none"
                
            self.structured_logger.info(
                "Kerberos environment setup and authentication configuration completed",
                authentication_method=auth_method,
                credentials_provided=bool(username and password),
                keytab_provided=bool(keytab_s3_path),
                note="JDBC driver will obtain tickets automatically when needed"
            )
            
            # Verify that environment is properly set up for JDBC driver automatic ticket acquisition
            self._verify_jdbc_driver_environment(kerberos_config, username, password, keytab_s3_path)
            
            return krb5_conf_path
            
        except Exception as e:
            self.structured_logger.error(
                "Failed to setup Kerberos environment",
                error=str(e),
                error_type=type(e).__name__
            )
            raise KerberosConfigurationError(f"Kerberos environment setup failed: {str(e)}")
    
    def _generate_krb5_conf(self, kerberos_config: KerberosConfig) -> str:
        """Generate a krb5.conf file from Kerberos configuration.
        
        Args:
            kerberos_config: Kerberos configuration parameters
            
        Returns:
            str: Path to the generated krb5.conf file
        """
        try:
            # Create temporary file for krb5.conf
            temp_dir = tempfile.gettempdir()
            krb5_conf_path = os.path.join(temp_dir, "krb5.conf")
            
            # Generate krb5.conf content
            krb5_content = self._build_krb5_conf_content(kerberos_config)
            
            # Write the configuration file
            with open(krb5_conf_path, 'w') as f:
                f.write(krb5_content)
            
            # Set appropriate permissions
            os.chmod(krb5_conf_path, 0o644)
            
            self.structured_logger.debug(
                "Generated krb5.conf file",
                path=krb5_conf_path,
                size=len(krb5_content)
            )
            
            # Upload krb5.conf to S3 for executor access
            s3_path = self._upload_krb5_conf_to_s3(krb5_conf_path, kerberos_config)
            if s3_path:
                self.structured_logger.info(
                    "Uploaded krb5.conf to S3 for executor distribution",
                    s3_path=s3_path,
                    local_path=krb5_conf_path
                )
            
            return krb5_conf_path
            
        except Exception as e:
            self.structured_logger.error(
                "Failed to generate krb5.conf",
                error=str(e)
            )
            raise
    
    def _upload_krb5_conf_to_s3(self, krb5_conf_path: str, kerberos_config: KerberosConfig) -> str:
        """Upload krb5.conf file to S3 for executor distribution.
        
        Args:
            krb5_conf_path: Local path to krb5.conf file
            kerberos_config: Kerberos configuration
            
        Returns:
            str: S3 path to uploaded file, or empty string if upload fails
        """
        try:
            import boto3
            import hashlib
            
            # Generate a unique S3 key based on domain and timestamp
            domain_hash = hashlib.md5(kerberos_config.domain.encode()).hexdigest()[:8]
            timestamp = int(time.time())
            s3_key = f"kerberos/krb5-{domain_hash}-{timestamp}.conf"
            
            # Try to determine S3 bucket from environment or use a default pattern
            # In AWS Glue, we can use the aws-glue-assets bucket
            import os
            account_id = boto3.client('sts').get_caller_identity()['Account']
            region = os.environ.get('AWS_REGION', 'us-east-1')
            bucket = f"aws-glue-assets-{account_id}-{region}"
            
            # Upload to S3
            s3_client = boto3.client('s3')
            s3_client.upload_file(krb5_conf_path, bucket, s3_key)
            
            s3_path = f"s3://{bucket}/{s3_key}"
            
            self.structured_logger.info(
                "Successfully uploaded krb5.conf to S3",
                s3_path=s3_path,
                bucket=bucket,
                key=s3_key
            )
            
            # Store S3 path for later use
            os.environ['KERBEROS_KRB5_CONF_S3_PATH'] = s3_path
            
            return s3_path
            
        except Exception as e:
            self.structured_logger.warning(
                "Failed to upload krb5.conf to S3 - executors may not have access",
                error=str(e),
                error_type=type(e).__name__
            )
            return ""
    
    def _build_krb5_conf_content(self, kerberos_config: KerberosConfig) -> str:
        """Build the content for krb5.conf file.
        
        Args:
            kerberos_config: Kerberos configuration parameters
            
        Returns:
            str: Complete krb5.conf file content
        """
        # Extract KDC host and port
        kdc_host = kerberos_config.kdc
        kdc_port = "88"  # Default Kerberos port
        
        if ":" in kdc_host:
            kdc_host, kdc_port = kdc_host.split(":", 1)
            

        
        # Log what KDC we're actually going to use
        self.structured_logger.info(
            "KDC configuration for krb5.conf",
            configured_kdc=kerberos_config.kdc,
            resolved_kdc_host=kdc_host,
            kdc_port=kdc_port,
            note="Ensure this resolves to your INTERNAL domain controller, not a public one"
        )
        
        # Build krb5.conf content with enhanced time tolerance for AWS Glue
        krb5_content = f"""[libdefaults]
    default_realm = {kerberos_config.domain.upper()}
    dns_lookup_realm = false
    dns_lookup_kdc = false
    # Explicitly disable DNS to prevent finding wrong KDCs
    dns_canonicalize_hostname = false
    dns_fallback = false
    ticket_lifetime = 24h
    renew_lifetime = 7d
    forwardable = true
    rdns = false
    default_ccache_name = FILE:/tmp/krb5cc_glue
    # Enhanced time tolerance for cloud environments (24 hours for severe time sync issues)
    clockskew = 86400
    # Linux/Java Kerberos compatibility settings for non-domain-joined machines
    allow_weak_crypto = true
    # Use encryption types that work well with Linux MIT Kerberos and Java
    default_tkt_enctypes = aes256-cts-hmac-sha1-96 aes128-cts-hmac-sha1-96 rc4-hmac des3-cbc-sha1
    default_tgs_enctypes = aes256-cts-hmac-sha1-96 aes128-cts-hmac-sha1-96 rc4-hmac des3-cbc-sha1
    permitted_enctypes = aes256-cts-hmac-sha1-96 aes128-cts-hmac-sha1-96 rc4-hmac des3-cbc-sha1
    # Additional Java compatibility settings
    ignore_acceptor_hostname = true
    noaddresses = true
    # Additional settings for non-domain-joined machines
    dns_canonicalize_hostname = false
    dns_fallback = false
    # Force TCP for all Kerberos communication - UDP fails with large pre-auth packets
    udp_preference_limit = 1
    # Set maximum UDP packet size very small to force TCP fallback
    kdc_default_options = 0x00000010
    # Additional settings to prefer TCP
    tcp_only = true

[realms]
    {kerberos_config.domain.upper()} = {{
        kdc = {kdc_host}:{kdc_port}
        admin_server = {kdc_host}:{kdc_port}
        default_domain = {kerberos_config.domain.lower()}
    }}

[domain_realm]
    .{kerberos_config.domain.lower()} = {kerberos_config.domain.upper()}
    {kerberos_config.domain.lower()} = {kerberos_config.domain.upper()}

[logging]
    default = FILE:/tmp/krb5libs.log
    kdc = FILE:/tmp/krb5kdc.log
    admin_server = FILE:/tmp/kadmind.log
"""
        
        return krb5_content
    
    def _set_java_kerberos_properties(self, krb5_conf_path: str, kerberos_config: KerberosConfig):
        """Set Java system properties for Kerberos authentication in AWS Glue/PySpark environment.
        
        Args:
            krb5_conf_path: Path to the krb5.conf file
            kerberos_config: Kerberos configuration parameters
        """
        # Store original values for potential cleanup
        java_properties = {
            'java.security.krb5.conf': krb5_conf_path,
            'java.security.krb5.realm': kerberos_config.domain.upper(),
            'java.security.krb5.kdc': kerberos_config.kdc,
            'java.security.krb5.debug': 'true',  # Enable Kerberos debugging
            # Force TCP for Java Kerberos
            'sun.security.krb5.udp.preference.limit': '1',
            'java.security.krb5.tcp.only': 'true',
            # Java Kerberos compatibility settings for ASN.1 parsing issues
            'sun.security.krb5.msinterop.kstring': 'true',
            'sun.security.krb5.disableReferrals': 'true',
            'javax.security.auth.useSubjectCredsOnly': 'false',
            # Buffer size settings for large responses
            'sun.security.krb5.maxReferrals': '10',
            'sun.security.krb5.tcp.max_retries': '3',
            # JVM 8 specific compatibility settings
            'sun.security.krb5.principal': f'ldecaro@{kerberos_config.domain.upper()}',
            'java.security.krb5.checkaddrs': 'false',
            # ASN.1 parsing compatibility for older JVMs
            'sun.security.krb5.acceptor.subkey': 'true'
        }
        
        # Try to set properties in the actual JVM through PySpark
        properties_set_successfully = self._set_jvm_properties_via_spark(java_properties)
        
        if not properties_set_successfully:
            # Fallback: try direct System.setProperty (will use mock in most cases)
            self.structured_logger.warning(
                "Could not access Spark JVM - falling back to direct System.setProperty (may not work in distributed environment)"
            )
            self._set_jvm_properties_direct(java_properties)
        
        # Also set environment variables as fallback
        os.environ['KRB5_CONFIG'] = krb5_conf_path
        os.environ['KRB5CCNAME'] = 'FILE:/tmp/krb5cc_glue'
        
        # Verify that properties were actually set in the JVM
        verification_props = self._verify_jvm_properties(java_properties.keys())
        
        self.structured_logger.info(
            "Java Kerberos properties configured",
            krb5_conf_path=krb5_conf_path,
            properties_count=len(java_properties),
            jvm_access_successful=properties_set_successfully
        )
        
        self.structured_logger.info(
            "Verification of Java system properties after setting",
            verification_props=verification_props
        )
        
        # Also log the krb5.conf file contents for debugging
        try:
            with open(krb5_conf_path, 'r') as f:
                krb5_contents = f.read()
            self.structured_logger.info(
                "Generated krb5.conf file contents",
                krb5_conf_path=krb5_conf_path,
                file_size=len(krb5_contents),
                contents=krb5_contents[:500] + "..." if len(krb5_contents) > 500 else krb5_contents
            )
        except Exception as e:
            self.structured_logger.error(
                "Failed to read krb5.conf file for verification",
                krb5_conf_path=krb5_conf_path,
                error=str(e)
            )
    
    def _set_jvm_properties_via_spark(self, java_properties: Dict[str, Optional[str]]) -> bool:
        """Set JVM properties through PySpark's JVM access.
        
        Args:
            java_properties: Dictionary of property names and values to set
            
        Returns:
            bool: True if properties were set successfully, False otherwise
        """
        try:
            # Try to get the Spark context and access the JVM
            from pyspark import SparkContext
            
            # Get the current Spark context
            sc = SparkContext.getOrCreate()
            
            if sc is None or sc._jvm is None:
                self.structured_logger.warning("Could not access Spark context or JVM")
                return False
            
            # Access the Java System class through PySpark's JVM
            java_system = sc._jvm.java.lang.System
            
            for prop_name, prop_value in java_properties.items():
                # Store original value for cleanup
                try:
                    original_value = java_system.getProperty(prop_name)
                    self._original_java_properties[prop_name] = original_value
                except:
                    self._original_java_properties[prop_name] = None
                
                # Set new value in the actual JVM
                if prop_value is not None:
                    java_system.setProperty(prop_name, prop_value)
                    self.structured_logger.debug(
                        "Set JVM system property via Spark",
                        property=prop_name,
                        value=prop_value if 'debug' in prop_name else '[REDACTED]'
                    )
            
            self.structured_logger.info(
                "Successfully set JVM properties through PySpark",
                properties_count=len([v for v in java_properties.values() if v is not None])
            )
            return True
            
        except ImportError as e:
            self.structured_logger.warning(
                "PySpark not available - cannot set JVM properties directly",
                error=str(e)
            )
            return False
        except Exception as e:
            self.structured_logger.warning(
                "Failed to set JVM properties through PySpark",
                error=str(e),
                error_type=type(e).__name__
            )
            return False
    
    def _set_jvm_properties_direct(self, java_properties: Dict[str, Optional[str]]):
        """Fallback method to set properties using direct System.setProperty.
        
        Args:
            java_properties: Dictionary of property names and values to set
        """
        for prop_name, prop_value in java_properties.items():
            # Store original value
            try:
                self._original_java_properties[prop_name] = System.getProperty(prop_name)
            except:
                self._original_java_properties[prop_name] = None
            
            # Set new value
            if prop_value is not None:
                System.setProperty(prop_name, prop_value)
                self.structured_logger.debug(
                    "Set system property (direct/mock)",
                    property=prop_name,
                    value=prop_value if 'debug' in prop_name else '[REDACTED]'
                )
    
    def _verify_jvm_properties(self, property_names) -> Dict[str, str]:
        """Verify that JVM properties were set correctly.
        
        Args:
            property_names: List of property names to verify
            
        Returns:
            Dict[str, str]: Dictionary of property names and their current values
        """
        verification_props = {}
        
        # Try to verify through Spark JVM first
        try:
            from pyspark import SparkContext
            sc = SparkContext.getOrCreate()
            
            if sc is not None and sc._jvm is not None:
                java_system = sc._jvm.java.lang.System
                
                for prop_name in property_names:
                    try:
                        current_value = java_system.getProperty(prop_name)
                        verification_props[prop_name] = current_value if current_value else "NULL"
                    except:
                        verification_props[prop_name] = "ERROR_GETTING_JVM_PROPERTY"
                
                self.structured_logger.debug("Verified properties through Spark JVM")
                return verification_props
                
        except Exception as e:
            self.structured_logger.debug(
                "Could not verify through Spark JVM, falling back to direct System",
                error=str(e)
            )
        
        # Fallback to direct System (likely mock)
        for prop_name in property_names:
            try:
                current_value = System.getProperty(prop_name)
                verification_props[prop_name] = current_value if current_value else "NULL_MOCK"
            except:
                verification_props[prop_name] = "ERROR_GETTING_MOCK_PROPERTY"
        
        return verification_props
    
    def get_glue_kerberos_properties(self, kerberos_config: KerberosConfig) -> Dict[str, str]:
        """Get Kerberos properties for Glue Connection configuration.
        
        Args:
            kerberos_config: Kerberos configuration parameters
            
        Returns:
            Dict[str, str]: Glue Connection properties for Kerberos (minimal set for AWS validation)
        """
        # Set up environment first to get krb5.conf path
        krb5_conf_path = self.setup_kerberos_environment(kerberos_config)
        
        # Return minimal properties that AWS Glue will accept
        # The Java system properties and krb5.conf are set up at runtime, not in Glue Connection
        return {
            # Store Kerberos config for runtime use, but don't include Java properties
            # that AWS Glue validation might reject
        }
    
    def cleanup_kerberos_environment(self):
        """Clean up Kerberos environment resources."""
        try:
            # Remove krb5.conf file if it exists
            if self.krb5_conf_path and os.path.exists(self.krb5_conf_path):
                os.remove(self.krb5_conf_path)
                self.structured_logger.debug(
                    "Removed krb5.conf file",
                    path=self.krb5_conf_path
                )
            
            # Remove keytab file if it exists
            if hasattr(self, '_keytab_path') and self._keytab_path and os.path.exists(self._keytab_path):
                os.remove(self._keytab_path)
                self.structured_logger.debug(
                    "Removed keytab file",
                    path=self._keytab_path
                )
            
            # Remove JAAS config file if it exists
            if hasattr(self, '_jaas_config_path') and self._jaas_config_path and os.path.exists(self._jaas_config_path):
                os.remove(self._jaas_config_path)
                self.structured_logger.debug(
                    "Removed JAAS config file",
                    path=self._jaas_config_path
                )
            
            # Clean up broadcast credentials if they exist
            if hasattr(self, '_broadcast_credentials') and self._broadcast_credentials:
                try:
                    self._broadcast_credentials.unpersist()
                    self.structured_logger.debug("Cleaned up broadcast credentials")
                except Exception as broadcast_error:
                    self.structured_logger.debug(
                        "Error cleaning up broadcast credentials",
                        error=str(broadcast_error)
                    )
            
            # Clean up keytab environment variables
            keytab_env_vars = [
                'KERBEROS_KEYTAB_S3_PATH', 'KERBEROS_USERNAME', 
                'KERBEROS_DOMAIN', 'KERBEROS_PRINCIPAL'
            ]
            
            for env_var in keytab_env_vars:
                if env_var in os.environ:
                    del os.environ[env_var]
            
            # Restore original Java properties - try Spark JVM first
            self._restore_jvm_properties()
            
            # Clean up environment variables
            env_vars_to_clean = [
                'KRB5_CONFIG', 'KRB5CCNAME', 
                'KERBEROS_USERNAME', 'KERBEROS_PASSWORD', 'KERBEROS_DOMAIN'
            ]
            
            for env_var in env_vars_to_clean:
                if env_var in os.environ:
                    del os.environ[env_var]
            
            self.structured_logger.info("Kerberos environment cleanup completed")
            
        except Exception as e:
            self.structured_logger.warning(
                "Error during Kerberos environment cleanup",
                error=str(e)
            )
    
    def _restore_jvm_properties(self):
        """Restore original JVM properties."""
        try:
            # Try to restore through Spark JVM first
            from pyspark import SparkContext
            sc = SparkContext.getOrCreate()
            
            if sc is not None and sc._jvm is not None:
                java_system = sc._jvm.java.lang.System
                
                for prop_name, original_value in self._original_java_properties.items():
                    try:
                        if original_value is not None:
                            java_system.setProperty(prop_name, original_value)
                        else:
                            java_system.clearProperty(prop_name)
                    except Exception as e:
                        self.structured_logger.debug(
                            "Could not restore JVM property",
                            property=prop_name,
                            error=str(e)
                        )
                
                self.structured_logger.debug("Restored JVM properties through Spark")
                return
                
        except Exception as e:
            self.structured_logger.debug(
                "Could not restore through Spark JVM, using direct System",
                error=str(e)
            )
        
        # Fallback to direct System (likely mock)
        for prop_name, original_value in self._original_java_properties.items():
            try:
                if original_value is not None:
                    System.setProperty(prop_name, original_value)
                else:
                    System.clearProperty(prop_name)
            except Exception as e:
                self.structured_logger.debug(
                    "Could not restore mock property",
                    property=prop_name,
                    error=str(e)
                )
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def setup_keytab_authentication(self, keytab_s3_path: str, username: str, domain: str) -> bool:
        """Set up keytab-based authentication for Kerberos.
        
        Downloads keytab from S3 and configures Java system properties for keytab authentication.
        This approach works in AWS Glue environment without requiring kinit.
        
        Args:
            keytab_s3_path: S3 path to keytab file (e.g., s3://bucket/path/user.keytab)
            username: Kerberos principal username
            domain: Kerberos realm/domain
            
        Returns:
            bool: True if keytab setup succeeded
        """
        try:
            self.structured_logger.info(
                "Setting up keytab-based Kerberos authentication",
                keytab_s3_path=keytab_s3_path,
                username=username,
                domain=domain
            )
            
            # Download keytab from S3
            local_keytab_path = self._download_keytab_from_s3(keytab_s3_path)
            
            if not local_keytab_path:
                self.structured_logger.error(
                    "Failed to download keytab from S3",
                    keytab_s3_path=keytab_s3_path
                )
                return False
            
            # Configure Java system properties for keytab authentication
            principal = f"{username}@{domain.upper()}"
            keytab_properties = {
                'java.security.auth.login.config': self._create_jaas_config(principal, local_keytab_path),
                'javax.security.auth.useSubjectCredsOnly': 'false',
                'java.security.krb5.principal': principal,
                'java.security.krb5.keytab': local_keytab_path
            }
            
            # Set keytab properties in JVM
            properties_set = self._set_jvm_properties_via_spark(keytab_properties)
            
            if not properties_set:
                self.structured_logger.warning(
                    "Could not set keytab properties via Spark JVM - falling back to direct method"
                )
                self._set_jvm_properties_direct(keytab_properties)
            
            # Store keytab and JAAS config paths for cleanup
            self._keytab_path = local_keytab_path
            self._jaas_config_path = keytab_properties.get('java.security.auth.login.config')
            
            self.structured_logger.info(
                "Keytab authentication setup completed",
                principal=principal,
                keytab_path=local_keytab_path,
                properties_set_via_spark=properties_set
            )
            
            # Store keytab configuration for executor access
            self._store_keytab_config_for_executors(keytab_s3_path, username, domain)
            
            return True
            
        except Exception as e:
            self.structured_logger.error(
                "Failed to set up keytab authentication",
                keytab_s3_path=keytab_s3_path,
                username=username,
                domain=domain,
                error=str(e),
                error_type=type(e).__name__
            )
            return False

    def setup_username_password_authentication(self, username: str, password: str, domain: str) -> bool:
        """Set up username/password authentication for Kerberos pre-authentication.
        
        This method validates that credentials are available for JDBC driver use
        but does NOT store them in environment variables for security reasons.
        The credentials should be retrieved from AWS Secrets Manager when needed.
        
        Args:
            username: Kerberos principal username
            password: User password (will not be stored)
            domain: Kerberos realm/domain
            
        Returns:
            bool: True if username/password are valid for setup
        """
        try:
            self.structured_logger.info(
                "Validating username/password availability for Kerberos pre-auth",
                username=username,
                domain=domain,
                note="Credentials will be retrieved from AWS Secrets Manager when needed"
            )
            
            # Validate that we have the required information
            if not username or not password or not domain:
                self.structured_logger.error(
                    "Missing required authentication information",
                    has_username=bool(username),
                    has_password=bool(password),
                    has_domain=bool(domain)
                )
                return False
            
            principal = f"{username}@{domain.upper()}"
            
            # Only store non-sensitive information
            os.environ['KERBEROS_USERNAME'] = username
            os.environ['KERBEROS_PRINCIPAL'] = principal
            os.environ['KERBEROS_DOMAIN'] = domain.upper()
            # NOTE: Password is NOT stored in environment for security
            
            self.structured_logger.info(
                "Username/password authentication validation completed",
                username=username,
                principal=principal,
                domain=domain,
                note="Password will be retrieved from AWS Secrets Manager during JDBC operations"
            )
            
            return True
            
        except Exception as e:
            self.structured_logger.error(
                "Failed to validate username/password authentication",
                username=username,
                domain=domain,
                error=str(e),
                error_type=type(e).__name__
            )
            return False
    
    def _store_kerberos_config_for_executors(self, kerberos_config: KerberosConfig, 
                                            username: str = None, password: str = None,
                                            keytab_s3_path: str = None):
        """Store Kerberos configuration for executor access.
        
        This method stores the Kerberos configuration in environment variables
        so executors can set up their own Kerberos environment when needed.
        
        Args:
            kerberos_config: Kerberos configuration parameters
            username: Kerberos principal username (optional)
            password: User password (optional, NOT stored for security)
            keytab_s3_path: S3 path to keytab file (optional)
        """
        try:
            import os
            
            # Store Kerberos configuration in environment variables
            os.environ['KERBEROS_DOMAIN'] = kerberos_config.domain
            os.environ['KERBEROS_KDC'] = kerberos_config.kdc
            os.environ['KERBEROS_SPN'] = kerberos_config.spn if kerberos_config.spn else ''
            
            if username:
                os.environ['KERBEROS_USERNAME'] = username
                os.environ['KERBEROS_PRINCIPAL'] = f"{username}@{kerberos_config.domain.upper()}"
            
            if keytab_s3_path:
                os.environ['KERBEROS_KEYTAB_S3_PATH'] = keytab_s3_path
            
            # NOTE: Password is NOT stored in environment for security reasons
            # Executors will need to retrieve it from Glue Connection/Secrets Manager
            
            self.structured_logger.info(
                "Stored Kerberos configuration in environment variables for executor access",
                domain=kerberos_config.domain,
                kdc=kerberos_config.kdc,
                username=username,
                has_keytab=bool(keytab_s3_path),
                note="Executors can now set up their own Kerberos environment"
            )
            
        except Exception as e:
            self.structured_logger.warning(
                "Failed to store Kerberos configuration for executors",
                error=str(e)
            )
    
    def _store_keytab_config_for_executors(self, keytab_s3_path: str, username: str, domain: str):
        """Store keytab configuration for executor access.
        
        This method stores the keytab configuration in environment variables
        so executors can access it when needed. Uses single keytab for both
        source and target connections.
        
        Args:
            keytab_s3_path: S3 path to keytab file
            username: Kerberos principal username  
            domain: Kerberos realm/domain
        """
        try:
            # Store keytab configuration in environment variables for executor access
            # This is more reliable than broadcasting in AWS Glue
            os.environ['KERBEROS_KEYTAB_S3_PATH'] = keytab_s3_path
            os.environ['KERBEROS_USERNAME'] = username
            os.environ['KERBEROS_DOMAIN'] = domain
            os.environ['KERBEROS_PRINCIPAL'] = f"{username}@{domain.upper()}"
            
            self.structured_logger.info(
                "Stored keytab configuration in environment variables for executor access",
                keytab_s3_path=keytab_s3_path,
                username=username,
                domain=domain,
                note="Single keytab will be used for both source and target connections"
            )
            
        except Exception as e:
            self.structured_logger.warning(
                "Failed to store keytab configuration for executors",
                keytab_s3_path=keytab_s3_path,
                error=str(e)
            )
    
    def ensure_executor_kerberos_environment(self) -> bool:
        """Ensure current executor has Kerberos environment set up.
        
        This method should be called on executors before JDBC operations
        to ensure they have proper Kerberos environment configuration.
        
        Returns:
            bool: True if Kerberos environment is available, False otherwise
        """
        try:
            import os
            
            # Check if krb5.conf was distributed via Spark --files
            # Spark distributes files to the current working directory
            distributed_krb5_conf = "./krb5.conf"
            if os.path.exists(distributed_krb5_conf):
                self.structured_logger.info(
                    "Found krb5.conf distributed by Spark --files",
                    path=distributed_krb5_conf,
                    note="Using Spark-distributed krb5.conf for executor"
                )
                
                # Set Java system properties to use the distributed file
                self._set_java_kerberos_properties_for_distributed_file(distributed_krb5_conf)
                
                return True
            
            # Check if Kerberos environment is already set up on this executor
            if hasattr(self, 'krb5_conf_path') and self.krb5_conf_path and os.path.exists(self.krb5_conf_path):
                self.structured_logger.debug(
                    "Kerberos environment already set up on this executor",
                    krb5_conf_path=self.krb5_conf_path
                )
                return True
            
            # Get Kerberos configuration from environment variables
            domain = os.environ.get('KERBEROS_DOMAIN')
            kdc = os.environ.get('KERBEROS_KDC')
            spn = os.environ.get('KERBEROS_SPN', '')
            username = os.environ.get('KERBEROS_USERNAME')
            keytab_s3_path = os.environ.get('KERBEROS_KEYTAB_S3_PATH')
            
            if not (domain and kdc):
                self.structured_logger.debug(
                    "No Kerberos configuration found in environment variables",
                    has_domain=bool(domain),
                    has_kdc=bool(kdc)
                )
                return False
            
            # Create Kerberos config from environment variables
            from .kerberos_config import KerberosConfig
            kerberos_config = KerberosConfig(
                spn=spn,
                domain=domain,
                kdc=kdc
            )
            
            # Set up Kerberos environment on this executor
            self.structured_logger.info(
                "Setting up Kerberos environment on executor from environment config",
                domain=domain,
                kdc=kdc,
                username=username,
                has_keytab=bool(keytab_s3_path),
                note="Executor initializing its own Kerberos environment"
            )
            
            # Set up the environment (without password since it's not stored in env)
            krb5_conf_path = self.setup_kerberos_environment(
                kerberos_config, username, None, keytab_s3_path
            )
            
            self.structured_logger.info(
                "Successfully set up Kerberos environment on executor",
                krb5_conf_path=krb5_conf_path,
                domain=domain
            )
            
            return True
            
        except Exception as e:
            self.structured_logger.error(
                "Failed to ensure executor Kerberos environment",
                error=str(e),
                error_type=type(e).__name__
            )
            return False
    
    def _set_java_kerberos_properties_for_distributed_file(self, krb5_conf_path: str):
        """Set Java Kerberos properties for a Spark-distributed krb5.conf file.
        
        Args:
            krb5_conf_path: Path to the distributed krb5.conf file
        """
        try:
            # Get domain and KDC from environment variables
            domain = os.environ.get('KERBEROS_DOMAIN', '')
            kdc = os.environ.get('KERBEROS_KDC', '')
            
            # Set Java system properties
            java_properties = {
                'java.security.krb5.conf': krb5_conf_path,
                'java.security.krb5.realm': domain,
                'java.security.krb5.kdc': kdc,
                'sun.security.krb5.debug': 'true'
            }
            
            # Try to set via Spark JVM
            properties_set = self._set_jvm_properties_via_spark(java_properties)
            
            if not properties_set:
                # Fallback to direct System.setProperty
                self._set_jvm_properties_direct(java_properties)
            
            self.structured_logger.info(
                "Set Java Kerberos properties for distributed krb5.conf",
                krb5_conf_path=krb5_conf_path,
                domain=domain,
                kdc=kdc
            )
            
        except Exception as e:
            self.structured_logger.error(
                "Failed to set Java properties for distributed krb5.conf",
                error=str(e)
            )
    
    def ensure_executor_keytab_access(self) -> bool:
        """Ensure current executor has keytab access for authentication.
        
        This method should be called on executors before JDBC operations
        to ensure they have proper keytab authentication set up.
        
        Returns:
            bool: True if keytab access is available, False otherwise
        """
        try:
            # Check if keytab is already set up on this executor
            if hasattr(self, '_keytab_path') and self._keytab_path and os.path.exists(self._keytab_path):
                self.structured_logger.debug(
                    "Keytab already available on executor",
                    keytab_path=self._keytab_path
                )
                return True
            
            # Get keytab configuration from environment variables
            keytab_s3_path = os.environ.get('KERBEROS_KEYTAB_S3_PATH')
            username = os.environ.get('KERBEROS_USERNAME')
            domain = os.environ.get('KERBEROS_DOMAIN')
            
            if not (keytab_s3_path and username and domain):
                self.structured_logger.debug(
                    "No keytab configuration found in environment variables",
                    has_keytab_path=bool(keytab_s3_path),
                    has_username=bool(username),
                    has_domain=bool(domain)
                )
                return False
            
            # Set up keytab on this executor
            self.structured_logger.info(
                "Setting up keytab authentication on executor from environment config",
                keytab_s3_path=keytab_s3_path,
                username=username,
                domain=domain,
                note="Single keytab will be used for both source and target connections"
            )
            
            return self.setup_keytab_authentication(keytab_s3_path, username, domain)
            
        except Exception as e:
            self.structured_logger.error(
                "Failed to ensure executor keytab access",
                error=str(e),
                error_type=type(e).__name__
            )
            return False

    
    def _download_keytab_from_s3(self, keytab_s3_path: str) -> Optional[str]:
        """Download keytab file from S3 to local filesystem.
        
        Args:
            keytab_s3_path: S3 path to keytab file
            
        Returns:
            str: Local path to downloaded keytab file, or None if failed
        """
        try:
            import boto3
            import tempfile
            from urllib.parse import urlparse
            
            # Parse S3 path
            parsed = urlparse(keytab_s3_path)
            if parsed.scheme != 's3':
                raise ValueError(f"Invalid S3 path: {keytab_s3_path}")
            
            bucket = parsed.netloc
            key = parsed.path.lstrip('/')
            
            # Create local temporary file for keytab
            temp_dir = tempfile.gettempdir()
            local_keytab_path = os.path.join(temp_dir, f"kerberos_{os.getpid()}.keytab")
            
            # Download keytab from S3
            s3_client = boto3.client('s3')
            s3_client.download_file(bucket, key, local_keytab_path)
            
            # Set appropriate permissions (readable by owner only)
            os.chmod(local_keytab_path, 0o600)
            
            self.structured_logger.info(
                "Successfully downloaded keytab from S3",
                s3_path=keytab_s3_path,
                local_path=local_keytab_path,
                bucket=bucket,
                key=key
            )
            
            return local_keytab_path
            
        except Exception as e:
            self.structured_logger.error(
                "Failed to download keytab from S3",
                s3_path=keytab_s3_path,
                error=str(e),
                error_type=type(e).__name__
            )
            return None
    
    def _create_jaas_config(self, principal: str, keytab_path: str) -> str:
        """Create JAAS configuration file for keytab authentication.
        
        Args:
            principal: Kerberos principal (user@REALM)
            keytab_path: Path to keytab file
            
        Returns:
            str: Path to JAAS configuration file
        """
        try:
            import tempfile
            
            # Create JAAS configuration content with enhanced compatibility
            jaas_content = f"""
KerberosClient {{
    com.sun.security.auth.module.Krb5LoginModule required
    useKeyTab=true
    keyTab="{keytab_path}"
    principal="{principal}"
    useTicketCache=false
    renewTGT=true
    doNotPrompt=true
    storeKey=true
    debug=false
    isInitiator=true;
}};

Client {{
    com.sun.security.auth.module.Krb5LoginModule required
    useKeyTab=true
    keyTab="{keytab_path}"
    principal="{principal}"
    useTicketCache=false
    renewTGT=true
    doNotPrompt=true
    storeKey=true
    debug=false
    isInitiator=true;
}};

com.sun.security.jgss.krb5.initiate {{
    com.sun.security.auth.module.Krb5LoginModule required
    useKeyTab=true
    keyTab="{keytab_path}"
    principal="{principal}"
    useTicketCache=false
    renewTGT=true
    doNotPrompt=true
    storeKey=true
    debug=false
    isInitiator=true;
}};
"""
            
            # Write JAAS config to temporary file
            temp_dir = tempfile.gettempdir()
            jaas_config_path = os.path.join(temp_dir, f"jaas_{os.getpid()}.conf")
            
            with open(jaas_config_path, 'w') as f:
                f.write(jaas_content)
            
            # Set appropriate permissions
            os.chmod(jaas_config_path, 0o644)
            
            self.structured_logger.debug(
                "Created JAAS configuration file",
                jaas_config_path=jaas_config_path,
                principal=principal,
                keytab_path=keytab_path
            )
            
            return jaas_config_path
            
        except Exception as e:
            self.structured_logger.error(
                "Failed to create JAAS configuration",
                principal=principal,
                keytab_path=keytab_path,
                error=str(e)
            )
            raise
    
    def setup_credentials_for_executors(self, username: str, password: str, domain: str) -> bool:
        """Set up credentials for executor access to Kerberos authentication.
        
        The JDBC driver handles ticket acquisition automatically, but executors need
        access to credentials and proper environment setup.
        
        Args:
            username: Kerberos principal username
            password: User password
            domain: Kerberos realm/domain
            
        Returns:
            bool: True if credentials were set up successfully
        """
        try:
            self.structured_logger.info(
                "Setting up credentials for executor Kerberos access",
                username=username,
                domain=domain
            )
            
            # Log JVM process information for debugging
            import os
            pid = os.getpid()
            
            self.structured_logger.info(
                "Setting up credentials for executor Kerberos access - JVM Process Info",
                username=username,
                domain=domain,
                jvm_pid=pid,
                note="Tracking which JVM process handles credential setup"
            )
            
            # Store credentials in environment variables for debugging purposes only
            # The JDBC driver should handle ticket acquisition automatically
            try:
                import os
                os.environ['KERBEROS_USERNAME'] = username
                os.environ['KERBEROS_DOMAIN'] = domain
                os.environ['KERBEROS_PRINCIPAL'] = f"{username}@{domain}"
                # Note: Not storing password in environment for security
                
                self.structured_logger.info(
                    "Set environment variables for Kerberos debugging (no password stored)",
                    username=username,
                    domain=domain,
                    jvm_pid=pid,
                    note="JDBC driver should acquire tickets automatically using Glue Connection credentials"
                )
                
                return True
                
            except Exception as e:
                self.structured_logger.error(
                    "Failed to set up environment variables for debugging",
                    username=username,
                    domain=domain,
                    jvm_pid=pid,
                    error=str(e)
                )
                return False
                
        except Exception as e:
            self.structured_logger.error(
                "Failed to set up credentials for executors",
                username=username,
                domain=domain,
                error=str(e),
                error_type=type(e).__name__
            )
            return False
    

    

    def _verify_jdbc_driver_environment(self, kerberos_config: KerberosConfig, 
                                       username: str = None, password: str = None,
                                       keytab_s3_path: str = None):
        """Verify that the environment is properly set up for JDBC driver automatic ticket acquisition.
        
        Args:
            kerberos_config: Kerberos configuration parameters
            username: Optional username for credential verification
            password: Optional password for credential verification
            keytab_s3_path: Optional keytab S3 path for keytab verification
        """
        try:
            import os
            pid = os.getpid()
            
            # Determine authentication method based on what's configured
            if keytab_s3_path and username and password:
                auth_method = "hybrid_keytab_and_password"
            elif keytab_s3_path:
                auth_method = "keytab_only"
            elif username and password:
                auth_method = "username_password"
            else:
                auth_method = "none"
                
            self.structured_logger.info(
                "Verifying JDBC driver environment for automatic ticket acquisition",
                domain=kerberos_config.domain,
                kdc=kerberos_config.kdc,
                authentication_method=auth_method,
                has_credentials=bool(username and password),
                has_keytab=bool(keytab_s3_path),
                jvm_pid=pid
            )
            
            # Verify krb5.conf file exists and is readable
            if self.krb5_conf_path and os.path.exists(self.krb5_conf_path):
                self.structured_logger.info(
                    "✓ krb5.conf file is available for JDBC driver",
                    krb5_conf_path=self.krb5_conf_path
                )
            else:
                self.structured_logger.warning(
                    "✗ krb5.conf file not found - JDBC driver may fail",
                    expected_path=self.krb5_conf_path
                )
            
            # Verify Java system properties are set
            verification_props = self._verify_jvm_properties([
                'java.security.krb5.conf',
                'java.security.krb5.realm', 
                'java.security.krb5.kdc'
            ])
            
            properties_ok = all(prop != "NULL" and prop != "ERROR_GETTING_JVM_PROPERTY" 
                              for prop in verification_props.values())
            
            if properties_ok:
                self.structured_logger.info(
                    "✓ Java system properties are set for JDBC driver Kerberos authentication",
                    properties=verification_props
                )
            else:
                self.structured_logger.warning(
                    "✗ Some Java system properties are missing - JDBC driver may fail",
                    properties=verification_props
                )
            
            # Verify authentication method setup
            if keytab_s3_path:
                # Verify keytab authentication setup
                keytab_available = hasattr(self, '_keytab_path') and self._keytab_path and os.path.exists(self._keytab_path)
                jaas_available = hasattr(self, '_jaas_config_path') and self._jaas_config_path and os.path.exists(self._jaas_config_path)
                
                if keytab_available and jaas_available:
                    self.structured_logger.info(
                        "✓ Keytab authentication configured for JDBC driver",
                        keytab_s3_path=keytab_s3_path,
                        local_keytab_path=self._keytab_path,
                        jaas_config_path=self._jaas_config_path,
                        username=username,
                        domain=kerberos_config.domain,
                        jvm_pid=pid,
                        note="JDBC driver will use keytab for automatic ticket acquisition"
                    )
                else:
                    self.structured_logger.warning(
                        "✗ Keytab authentication setup incomplete",
                        keytab_available=keytab_available,
                        jaas_available=jaas_available,
                        jvm_pid=pid
                    )
            elif username and password:
                # Check environment variables for debugging info only
                env_username = os.environ.get('KERBEROS_USERNAME')
                env_domain = os.environ.get('KERBEROS_DOMAIN')
                
                self.structured_logger.info(
                    "✓ Username/password credentials available for JDBC driver automatic ticket acquisition",
                    username=username,
                    domain=kerberos_config.domain,
                    jvm_pid=pid,
                    env_username_set=bool(env_username),
                    env_domain_set=bool(env_domain),
                    note="JDBC driver will use Glue Connection credentials for ticket acquisition"
                )
            else:
                self.structured_logger.warning(
                    "✗ No authentication method configured - JDBC driver will not be able to acquire tickets automatically",
                    jvm_pid=pid,
                    note="This JVM process has no credentials or keytab for Kerberos authentication"
                )
            
            self.structured_logger.info(
                "JDBC driver environment verification completed",
                summary="JDBC driver should be able to acquire Kerberos tickets automatically if all checks passed"
            )
            
        except Exception as e:
            self.structured_logger.error(
                "Error during JDBC driver environment verification",
                error=str(e),
                error_type=type(e).__name__
            )



    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit with cleanup."""
        self.cleanup_kerberos_environment()


# Try to import Java System class for property management
try:
    from java.lang import System
except ImportError:
    # Mock System class for local development/testing
    class System:
        _properties = {}
        
        @classmethod
        def setProperty(cls, key, value):
            cls._properties[key] = value
        
        @classmethod
        def getProperty(cls, key):
            return cls._properties.get(key)
        
        @classmethod
        def clearProperty(cls, key):
            cls._properties.pop(key, None)