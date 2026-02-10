"""
Kerberos environment manager for AWS Glue Data Replication

This module provides utilities for setting up the Kerberos environment
in AWS Glue jobs, including krb5.conf generation, JAAS configuration,
and keytab-based authentication for both driver and executor nodes.

Key features:
- Generates krb5.conf for Kerberos realm configuration
- Creates JAAS config with principal for keytab authentication
- Downloads keytab from S3 for authentication
- Distributes Kerberos files to Spark executors
- Works with full load, incremental load, serial and parallel reads
"""

import os
import sys
import tempfile
import time
from typing import Optional, Dict
from urllib.parse import urlparse

try:
    from .kerberos_config import KerberosConfig, KerberosConfigurationError
    from ..monitoring.logging import StructuredLogger
except ImportError:
    from kerberos_config import KerberosConfig, KerberosConfigurationError
    
    class StructuredLogger:
        def __init__(self, name):
            import logging
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
    """Manager for Kerberos environment setup in AWS Glue jobs.
    
    This class handles:
    1. krb5.conf generation and upload to S3
    2. JAAS configuration with principal for keytab auth
    3. Keytab download from S3
    4. Java system property configuration
    5. Executor distribution of Kerberos files
    """
    
    def __init__(self):
        """Initialize the Kerberos environment manager."""
        self.structured_logger = StructuredLogger("KerberosEnvironmentManager")
        self.krb5_conf_path: Optional[str] = None
        self._keytab_path: Optional[str] = None
        self._jaas_conf_path: Optional[str] = None
        self._original_java_properties: Dict[str, Optional[str]] = {}
    
    def setup_kerberos_environment(self, kerberos_config: KerberosConfig, 
                                   username: str = None, password: str = None,
                                   keytab_s3_path: str = None) -> str:
        """Set up the complete Kerberos environment for JDBC connections.
        
        This method first tries to download existing config files from S3.
        If they don't exist, it generates them and uploads to S3 for reuse.
        
        Args:
            kerberos_config: Kerberos configuration (domain, KDC, SPN)
            username: Kerberos principal username
            password: User password (not needed for keytab auth)
            keytab_s3_path: S3 path to keytab file (e.g., s3://bucket/kerberos/job/user.keytab)
            
        Returns:
            str: Path to the krb5.conf file (local)
        """
        try:
            self.structured_logger.info(
                "Setting up Kerberos environment",
                domain=kerberos_config.domain,
                kdc=kerberos_config.kdc,
                has_credentials=bool(username and password),
                has_keytab=bool(keytab_s3_path)
            )
            
            # Step 1: Download keytab from S3 (always download - it's the credential)
            if keytab_s3_path:
                self._keytab_path = self._download_keytab_from_s3(keytab_s3_path)
                if self._keytab_path:
                    os.environ['KERBEROS_KEYTAB_S3_PATH'] = keytab_s3_path
                    self.structured_logger.info("Downloaded keytab from S3", 
                                                local_path=self._keytab_path)
                else:
                    self.structured_logger.error("Failed to download keytab - auth will fail")
            
            # Step 2: Try to download existing krb5.conf and jaas.conf from S3
            # If they exist, reuse them. If not, generate and upload.
            configs_loaded = self._load_or_create_configs(kerberos_config, username)
            
            # Step 3: Set environment variables for Kerberos
            self._set_java_kerberos_properties(kerberos_config, username)
            
            # Step 4: Store config for executor distribution
            self._store_config_for_executors(kerberos_config, username, password, keytab_s3_path)
            
            self.structured_logger.info(
                "Kerberos environment setup completed",
                krb5_conf_path=self.krb5_conf_path,
                jaas_conf_path=self._jaas_conf_path,
                keytab_path=self._keytab_path,
                configs_from_s3=configs_loaded
            )
            
            return self.krb5_conf_path
            
        except Exception as e:
            self.structured_logger.error("Failed to setup Kerberos environment", error=str(e))
            raise KerberosConfigurationError(f"Kerberos setup failed: {str(e)}")

    def _load_or_create_configs(self, kerberos_config: KerberosConfig, username: str = None) -> bool:
        """Try to load krb5.conf and jaas.conf from S3, or create them if not found.
        
        Returns:
            bool: True if configs were loaded from S3, False if newly created
        """
        import boto3
        
        job_name = self._get_glue_arg('JOB_NAME', 'glue-job')
        bucket = self._get_s3_bucket()
        s3_client = boto3.client('s3')
        
        krb5_s3_key = f"kerberos/{job_name}/krb5.conf"
        jaas_s3_key = f"kerberos/{job_name}/jaas.conf"
        
        krb5_loaded = False
        jaas_loaded = False
        
        # Try to download krb5.conf from S3
        try:
            s3_client.head_object(Bucket=bucket, Key=krb5_s3_key)
            # File exists, download it
            self.krb5_conf_path = '/tmp/krb5.conf'
            s3_client.download_file(bucket, krb5_s3_key, self.krb5_conf_path)
            os.chmod(self.krb5_conf_path, 0o644)
            self.structured_logger.info("Loaded krb5.conf from S3", 
                                        s3_path=f"s3://{bucket}/{krb5_s3_key}")
            krb5_loaded = True
        except s3_client.exceptions.ClientError as e:
            if e.response['Error']['Code'] == '404':
                # File doesn't exist, create it
                self.structured_logger.info("krb5.conf not found in S3, creating new one")
                self.krb5_conf_path = self._generate_krb5_conf(kerberos_config)
                # Upload for future reuse
                s3_client.upload_file(self.krb5_conf_path, bucket, krb5_s3_key)
                self.structured_logger.info("Uploaded new krb5.conf to S3",
                                            s3_path=f"s3://{bucket}/{krb5_s3_key}")
            else:
                raise
        except Exception as e:
            # Any other error, create locally
            self.structured_logger.warning(f"Could not check S3 for krb5.conf: {e}, creating new one")
            self.krb5_conf_path = self._generate_krb5_conf(kerberos_config)
        
        os.environ['KERBEROS_KRB5_CONF_S3_PATH'] = f"s3://{bucket}/{krb5_s3_key}"
        
        # Try to download jaas.conf from S3 (only if username provided)
        if username:
            try:
                s3_client.head_object(Bucket=bucket, Key=jaas_s3_key)
                # File exists, download it
                self._jaas_conf_path = '/tmp/jaas.conf'
                s3_client.download_file(bucket, jaas_s3_key, self._jaas_conf_path)
                os.chmod(self._jaas_conf_path, 0o644)
                self.structured_logger.info("Loaded jaas.conf from S3",
                                            s3_path=f"s3://{bucket}/{jaas_s3_key}")
                jaas_loaded = True
            except s3_client.exceptions.ClientError as e:
                if e.response['Error']['Code'] == '404':
                    # File doesn't exist, create it
                    self.structured_logger.info("jaas.conf not found in S3, creating new one")
                    self._jaas_conf_path = self._generate_jaas_conf(kerberos_config, username)
                    # Upload for future reuse
                    s3_client.upload_file(self._jaas_conf_path, bucket, jaas_s3_key)
                    self.structured_logger.info("Uploaded new jaas.conf to S3",
                                                s3_path=f"s3://{bucket}/{jaas_s3_key}")
                else:
                    raise
            except Exception as e:
                # Any other error, create locally
                self.structured_logger.warning(f"Could not check S3 for jaas.conf: {e}, creating new one")
                self._jaas_conf_path = self._generate_jaas_conf(kerberos_config, username)
            
            os.environ['KERBEROS_JAAS_CONF_S3_PATH'] = f"s3://{bucket}/{jaas_s3_key}"
        
        return krb5_loaded and jaas_loaded

    
    def _generate_krb5_conf(self, kerberos_config: KerberosConfig) -> str:
        """Generate krb5.conf file for Kerberos realm configuration."""
        temp_dir = tempfile.gettempdir()
        krb5_conf_path = os.path.join(temp_dir, "krb5.conf")
        
        kdc_host = kerberos_config.kdc
        kdc_port = "88"
        if ":" in kdc_host:
            kdc_host, kdc_port = kdc_host.split(":", 1)
        
        krb5_content = f"""[libdefaults]
    default_realm = {kerberos_config.domain.upper()}
    dns_lookup_realm = false
    dns_lookup_kdc = false
    dns_canonicalize_hostname = false
    ticket_lifetime = 24h
    renew_lifetime = 7d
    forwardable = true
    rdns = false
    default_ccache_name = FILE:/tmp/krb5cc_glue
    clockskew = 86400
    allow_weak_crypto = true
    default_tkt_enctypes = aes256-cts-hmac-sha1-96 aes128-cts-hmac-sha1-96 rc4-hmac
    default_tgs_enctypes = aes256-cts-hmac-sha1-96 aes128-cts-hmac-sha1-96 rc4-hmac
    permitted_enctypes = aes256-cts-hmac-sha1-96 aes128-cts-hmac-sha1-96 rc4-hmac
    udp_preference_limit = 1

[realms]
    {kerberos_config.domain.upper()} = {{
        kdc = {kdc_host}:{kdc_port}
        admin_server = {kdc_host}:{kdc_port}
        default_domain = {kerberos_config.domain.lower()}
    }}

[domain_realm]
    .{kerberos_config.domain.lower()} = {kerberos_config.domain.upper()}
    {kerberos_config.domain.lower()} = {kerberos_config.domain.upper()}
"""
        
        with open(krb5_conf_path, 'w') as f:
            f.write(krb5_content)
        os.chmod(krb5_conf_path, 0o644)
        
        self.structured_logger.debug("Generated krb5.conf", path=krb5_conf_path)
        return krb5_conf_path
    
    def _generate_jaas_conf(self, kerberos_config: KerberosConfig, username: str) -> str:
        """Generate JAAS configuration file for keytab authentication.
        
        CRITICAL: Includes principal to avoid 'Unable to obtain Principal Name' errors.
        Uses doNotPrompt=true to prevent System.in prompts on executors.
        """
        temp_dir = tempfile.gettempdir()
        jaas_conf_path = os.path.join(temp_dir, "jaas.conf")
        
        principal = f"{username}@{kerberos_config.domain.upper()}"
        keytab_path = "/tmp/krb5.keytab"
        
        jaas_content = f"""// JAAS Configuration for AWS Glue Kerberos Authentication
// doNotPrompt=true - NEVER prompt on System.in
// principal - REQUIRED to avoid "Unable to obtain Principal Name" errors

SQLJDBCDriver {{
    com.sun.security.auth.module.Krb5LoginModule required
    refreshKrb5Config=true
    useKeyTab=true
    keyTab="{keytab_path}"
    principal="{principal}"
    storeKey=true
    doNotPrompt=true;
}};

Client {{
    com.sun.security.auth.module.Krb5LoginModule required
    refreshKrb5Config=true
    useKeyTab=true
    keyTab="{keytab_path}"
    principal="{principal}"
    storeKey=true
    doNotPrompt=true;
}};

com.sun.security.jgss.krb5.initiate {{
    com.sun.security.auth.module.Krb5LoginModule required
    refreshKrb5Config=true
    useKeyTab=true
    keyTab="{keytab_path}"
    principal="{principal}"
    storeKey=true
    doNotPrompt=true;
}};
"""
        
        with open(jaas_conf_path, 'w') as f:
            f.write(jaas_content)
        os.chmod(jaas_conf_path, 0o644)
        
        self.structured_logger.debug("Generated JAAS config", path=jaas_conf_path, principal=principal)
        return jaas_conf_path

    
    def _download_keytab_from_s3(self, keytab_s3_path: str) -> Optional[str]:
        """Download keytab file from S3 to /tmp/krb5.keytab."""
        try:
            import boto3
            
            parsed = urlparse(keytab_s3_path)
            if parsed.scheme != 's3':
                raise ValueError(f"Invalid S3 path: {keytab_s3_path}")
            
            bucket = parsed.netloc
            key = parsed.path.lstrip('/')
            
            # Download to fixed path that JAAS config expects
            local_keytab_path = "/tmp/krb5.keytab"
            
            s3_client = boto3.client('s3')
            s3_client.download_file(bucket, key, local_keytab_path)
            os.chmod(local_keytab_path, 0o600)
            
            self.structured_logger.info("Downloaded keytab from S3",
                                        s3_path=keytab_s3_path, local_path=local_keytab_path)
            return local_keytab_path
            
        except Exception as e:
            self.structured_logger.error("Failed to download keytab", 
                                         s3_path=keytab_s3_path, error=str(e))
            return None
    
    def _set_java_kerberos_properties(self, kerberos_config: KerberosConfig, username: str = None):
        """Set Java system properties for Kerberos authentication."""
        os.environ['KRB5_CONFIG'] = self.krb5_conf_path or '/tmp/krb5.conf'
        os.environ['KRB5CCNAME'] = 'FILE:/tmp/krb5cc_glue'
        
        # Store properties to set after SparkContext is available
        self._pending_java_properties = {
            'java.security.krb5.conf': self.krb5_conf_path,
            'java.security.krb5.realm': kerberos_config.domain.upper(),
            'java.security.krb5.kdc': kerberos_config.kdc,
            'javax.security.auth.useSubjectCredsOnly': 'false',
            'sun.security.krb5.udp.preference.limit': '1',
        }
        
        if self._jaas_conf_path:
            self._pending_java_properties['java.security.auth.login.config'] = self._jaas_conf_path
        
        if username:
            principal = f"{username}@{kerberos_config.domain.upper()}"
            self._pending_java_properties['java.security.krb5.principal'] = principal
        
        self.structured_logger.info("Kerberos environment variables set",
                                    KRB5_CONFIG=os.environ.get('KRB5_CONFIG'))
    
    def _store_config_for_executors(self, kerberos_config: KerberosConfig,
                                    username: str = None, password: str = None,
                                    keytab_s3_path: str = None):
        """Store Kerberos configuration in environment variables for executor access."""
        os.environ['KERBEROS_DOMAIN'] = kerberos_config.domain
        os.environ['KERBEROS_REALM'] = kerberos_config.domain.upper()
        os.environ['KERBEROS_KDC'] = kerberos_config.kdc
        os.environ['KERBEROS_SPN'] = kerberos_config.spn or ''
        
        if username:
            os.environ['KERBEROS_USERNAME'] = username
            os.environ['KERBEROS_PRINCIPAL'] = f"{username}@{kerberos_config.domain.upper()}"
        
        if password:
            os.environ['KERBEROS_PASSWORD'] = password
        
        if keytab_s3_path:
            os.environ['KERBEROS_KEYTAB_S3_PATH'] = keytab_s3_path
        
        self.structured_logger.info("Stored Kerberos config for executors",
                                    domain=kerberos_config.domain, username=username,
                                    has_keytab=bool(keytab_s3_path))

    
    def _get_glue_arg(self, arg_name: str, default: str = '') -> str:
        """Extract argument value from sys.argv (Glue passes args as --NAME value)."""
        try:
            for i, arg in enumerate(sys.argv):
                if arg == f'--{arg_name}' and i + 1 < len(sys.argv):
                    return sys.argv[i + 1]
        except:
            pass
        return default
    
    def _get_s3_bucket(self) -> str:
        """Get S3 bucket for storing Kerberos config files."""
        import boto3
        
        bucket = self._get_glue_arg('S3_ASSETS_BUCKET', '')
        
        if not bucket:
            source_jdbc_path = self._get_glue_arg('SOURCE_JDBC_DRIVER_S3_PATH', '')
            if source_jdbc_path.startswith('s3://'):
                bucket = source_jdbc_path.replace('s3://', '').split('/')[0]
        
        if not bucket:
            account_id = boto3.client('sts').get_caller_identity()['Account']
            region = os.environ.get('AWS_REGION', 'us-east-1')
            bucket = f"aws-glue-assets-{account_id}-{region}"
        
        return bucket
    
    def _upload_configs_to_s3(self, kerberos_config: KerberosConfig):
        """Upload krb5.conf and jaas.conf to S3 for executor distribution.
        
        Note: This method is kept for backward compatibility but the main upload
        logic is now in _load_or_create_configs which handles both download and upload.
        """
        # Configs are now uploaded in _load_or_create_configs
        pass

    
    def set_jvm_properties_post_spark_init(self, spark_context) -> bool:
        """Set JVM properties after SparkContext has been initialized.
        
        Call this after SparkContext is created to set Java system properties.
        """
        if not hasattr(self, '_pending_java_properties') or not self._pending_java_properties:
            return True
            
        try:
            if spark_context is None or spark_context._jvm is None:
                self.structured_logger.warning("SparkContext or JVM not available")
                return False
            
            java_system = spark_context._jvm.java.lang.System
            
            for prop_name, prop_value in self._pending_java_properties.items():
                if prop_value:
                    java_system.setProperty(prop_name, prop_value)
            
            # Refresh Kerberos config cache
            try:
                spark_context._jvm.sun.security.krb5.Config.refresh()
            except:
                pass
            
            self.structured_logger.info("Set JVM Kerberos properties",
                                        properties_count=len(self._pending_java_properties))
            return True
            
        except Exception as e:
            self.structured_logger.warning("Failed to set JVM properties", error=str(e))
            return False
    
    def get_executor_spark_config(self) -> Dict[str, str]:
        """Get Spark configuration for distributing Kerberos to executors.
        
        Returns config to set BEFORE SparkContext is created.
        """
        spark_conf = {}
        
        if not hasattr(self, '_pending_java_properties'):
            return spark_conf
        
        realm = self._pending_java_properties.get('java.security.krb5.realm', '')
        kdc = self._pending_java_properties.get('java.security.krb5.kdc', '')
        
        # Environment variables for executors
        spark_conf['spark.executorEnv.KRB5_CONFIG'] = '/tmp/krb5.conf'
        spark_conf['spark.executorEnv.KRB5CCNAME'] = 'FILE:/tmp/krb5cc_glue'
        spark_conf['spark.executorEnv.KERBEROS_REALM'] = realm
        spark_conf['spark.executorEnv.KERBEROS_KDC'] = kdc
        
        # Pass S3 paths for executor download
        for env_var in ['KERBEROS_KRB5_CONF_S3_PATH', 'KERBEROS_JAAS_CONF_S3_PATH', 
                        'KERBEROS_KEYTAB_S3_PATH', 'KERBEROS_USERNAME', 
                        'KERBEROS_PASSWORD', 'KERBEROS_PRINCIPAL']:
            value = os.environ.get(env_var, '')
            if value:
                spark_conf[f'spark.executorEnv.{env_var}'] = value
        
        self.structured_logger.info("Generated Spark executor config for Kerberos",
                                    realm=realm, kdc=kdc)
        return spark_conf

    
    def distribute_kerberos_to_executors(self, spark_context, spark_session) -> bool:
        """Distribute Kerberos configuration to Spark executors.
        
        Downloads krb5.conf, jaas.conf, and keytab from S3 to each executor.
        Uses broadcast variables to pass S3 paths since env vars don't propagate.
        """
        try:
            import os
            
            num_partitions = spark_context.defaultParallelism
            
            # Get S3 paths from driver's environment
            krb5_s3_path = os.environ.get('KERBEROS_KRB5_CONF_S3_PATH', '')
            jaas_s3_path = os.environ.get('KERBEROS_JAAS_CONF_S3_PATH', '')
            keytab_s3_path = os.environ.get('KERBEROS_KEYTAB_S3_PATH', '')
            kerberos_realm = os.environ.get('KERBEROS_REALM', '')
            kerberos_kdc = os.environ.get('KERBEROS_KDC', '')
            
            self.structured_logger.info("Broadcasting Kerberos S3 paths to executors",
                                        krb5_s3=krb5_s3_path,
                                        jaas_s3=jaas_s3_path,
                                        keytab_s3=keytab_s3_path)
            
            # Broadcast the S3 paths to all executors
            kerberos_config_broadcast = spark_context.broadcast({
                'krb5_s3_path': krb5_s3_path,
                'jaas_s3_path': jaas_s3_path,
                'keytab_s3_path': keytab_s3_path,
                'realm': kerberos_realm,
                'kdc': kerberos_kdc
            })
            
            def setup_kerberos_on_executor(partition_index):
                """Set up Kerberos files on each executor."""
                import os
                import boto3
                
                # Get config from broadcast variable
                config = kerberos_config_broadcast.value
                
                result = {
                    'partition': partition_index,
                    'krb5_ok': False,
                    'jaas_ok': False,
                    'keytab_ok': False,
                    'jvm_ok': False,
                    'error': None
                }
                
                try:
                    s3_client = boto3.client('s3')
                    
                    def download_from_s3(s3_path, local_path, permissions=0o644):
                        if not s3_path:
                            return False
                        try:
                            parts = s3_path.replace('s3://', '').split('/', 1)
                            bucket, key = parts[0], parts[1] if len(parts) > 1 else ''
                            s3_client.download_file(bucket, key, local_path)
                            os.chmod(local_path, permissions)
                            return True
                        except Exception as e:
                            result['error'] = str(e)
                            return False
                    
                    # Download krb5.conf
                    result['krb5_ok'] = download_from_s3(config['krb5_s3_path'], '/tmp/krb5.conf')
                    
                    # Download jaas.conf
                    result['jaas_ok'] = download_from_s3(config['jaas_s3_path'], '/tmp/jaas.conf')
                    
                    # Download keytab (secure permissions)
                    result['keytab_ok'] = download_from_s3(config['keytab_s3_path'], '/tmp/krb5.keytab', 0o600)
                    
                    # Set environment variables on executor
                    if result['krb5_ok']:
                        os.environ['KRB5_CONFIG'] = '/tmp/krb5.conf'
                        os.environ['KRB5CCNAME'] = 'FILE:/tmp/krb5cc_glue'
                    
                    # Set JVM properties on executor
                    try:
                        from pyspark import SparkContext
                        sc = SparkContext._active_spark_context
                        if sc and sc._jvm:
                            java_system = sc._jvm.java.lang.System
                            java_system.setProperty('java.security.krb5.conf', '/tmp/krb5.conf')
                            if config['realm']:
                                java_system.setProperty('java.security.krb5.realm', config['realm'])
                            if config['kdc']:
                                java_system.setProperty('java.security.krb5.kdc', config['kdc'])
                            if result['jaas_ok']:
                                java_system.setProperty('java.security.auth.login.config', '/tmp/jaas.conf')
                            java_system.setProperty('javax.security.auth.useSubjectCredsOnly', 'false')
                            
                            # Refresh Kerberos config cache
                            try:
                                sc._jvm.sun.security.krb5.Config.refresh()
                            except:
                                pass
                            result['jvm_ok'] = True
                    except Exception as e:
                        result['error'] = f"JVM setup failed: {e}"
                        result['jvm_ok'] = False
                    
                    result['success'] = result['krb5_ok'] and result['jaas_ok'] and result['keytab_ok']
                    
                except Exception as e:
                    result['error'] = str(e)
                    result['success'] = False
                
                yield result
            
            # Run setup on all executors
            rdd = spark_context.parallelize(range(num_partitions), num_partitions)
            results = rdd.mapPartitionsWithIndex(
                lambda idx, _: setup_kerberos_on_executor(idx)
            ).collect()
            
            success_count = sum(1 for r in results if r.get('success', False))
            
            # Log detailed results for debugging
            for r in results:
                if not r.get('success', False):
                    self.structured_logger.warning(
                        f"Executor {r.get('partition')} Kerberos setup failed",
                        krb5_ok=r.get('krb5_ok'),
                        jaas_ok=r.get('jaas_ok'),
                        keytab_ok=r.get('keytab_ok'),
                        jvm_ok=r.get('jvm_ok'),
                        error=r.get('error')
                    )
            
            self.structured_logger.info("Executor Kerberos setup completed",
                                        total=len(results), successful=success_count)
            
            return success_count > 0
            
        except Exception as e:
            self.structured_logger.error("Failed to distribute Kerberos to executors", error=str(e))
            return False

    
    def cleanup_kerberos_environment(self):
        """Clean up Kerberos environment resources."""
        try:
            # Remove local files
            for path in [self.krb5_conf_path, self._keytab_path, self._jaas_conf_path]:
                if path and os.path.exists(path):
                    os.remove(path)
            
            # Clean up environment variables
            env_vars = ['KRB5_CONFIG', 'KRB5CCNAME', 'KERBEROS_USERNAME', 'KERBEROS_PASSWORD',
                        'KERBEROS_DOMAIN', 'KERBEROS_REALM', 'KERBEROS_KDC', 'KERBEROS_SPN',
                        'KERBEROS_PRINCIPAL', 'KERBEROS_KEYTAB_S3_PATH',
                        'KERBEROS_KRB5_CONF_S3_PATH', 'KERBEROS_JAAS_CONF_S3_PATH']
            
            for env_var in env_vars:
                if env_var in os.environ:
                    del os.environ[env_var]
            
            self.structured_logger.info("Kerberos environment cleanup completed")
            
        except Exception as e:
            self.structured_logger.warning("Error during cleanup", error=str(e))
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit with cleanup."""
        self.cleanup_kerberos_environment()


# Mock System class for local development/testing
try:
    from java.lang import System
except ImportError:
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
