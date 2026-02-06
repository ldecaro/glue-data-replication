"""
Kerberos executor setup utilities for AWS Glue.

This module provides functions to set up Kerberos configuration on Spark executors
at runtime, working around AWS Glue's limitation of not honoring spark.executor.extraJavaOptions.

The key insight is that we need to:
1. Set Java system properties on each executor
2. Refresh the Kerberos configuration cache (sun.security.krb5.Config.refresh())
3. Do this BEFORE any JDBC operations

This is accomplished by running a setup function on each partition before JDBC reads.
"""

import os
import logging

logger = logging.getLogger(__name__)


def setup_kerberos_on_executor():
    """Set up Kerberos configuration on the current executor.
    
    This function should be called on each executor before JDBC operations.
    It reads the krb5.conf and jaas.conf files that were distributed via SparkFiles
    and sets the appropriate Java system properties.
    
    CRITICAL: The JAAS config must have doNotPrompt=true to prevent System.in prompts.
    
    Returns:
        bool: True if setup was successful
    """
    try:
        # Get paths from environment variables (set by distribute_kerberos_to_executors)
        krb5_conf_path = os.environ.get('KRB5_CONFIG', '/tmp/krb5.conf')
        jaas_conf_path = '/tmp/jaas.conf'
        realm = os.environ.get('KERBEROS_REALM', '')
        kdc = os.environ.get('KERBEROS_KDC', '')
        
        # Check if krb5.conf exists
        if not os.path.exists(krb5_conf_path):
            logger.warning(f"krb5.conf not found at {krb5_conf_path}")
            return False
        
        # Try to access the JVM through PySpark
        try:
            from pyspark import SparkContext
            sc = SparkContext._active_spark_context
            
            if sc is None or sc._jvm is None:
                logger.warning("SparkContext or JVM not available on executor")
                return False
            
            java_system = sc._jvm.java.lang.System
            
            # Set Java system properties for Kerberos
            java_system.setProperty('java.security.krb5.conf', krb5_conf_path)
            if realm:
                java_system.setProperty('java.security.krb5.realm', realm)
            if kdc:
                java_system.setProperty('java.security.krb5.kdc', kdc)
            java_system.setProperty('javax.security.auth.useSubjectCredsOnly', 'false')
            java_system.setProperty('sun.security.krb5.debug', 'true')
            
            # Set JAAS config if available (CRITICAL for Kerberos auth)
            if os.path.exists(jaas_conf_path):
                java_system.setProperty('java.security.auth.login.config', jaas_conf_path)
                logger.info(f"Set JAAS config: {jaas_conf_path}")
            
            # CRITICAL: Refresh the Kerberos configuration cache
            # This forces the JVM to re-read krb5.conf
            try:
                krb5_config_class = sc._jvm.sun.security.krb5.Config
                krb5_config_class.refresh()
                logger.info("Successfully refreshed Kerberos configuration on executor")
            except Exception as refresh_error:
                logger.warning(f"Could not refresh Kerberos config: {refresh_error}")
            
            return True
            
        except ImportError:
            logger.warning("PySpark not available")
            return False
            
    except Exception as e:
        logger.error(f"Failed to set up Kerberos on executor: {e}")
        return False


def ensure_kerberos_on_partition(iterator):
    """Generator that ensures Kerberos is set up before yielding partition data.
    
    Use this with mapPartitions to ensure Kerberos is configured on each executor
    before processing data.
    
    Args:
        iterator: Partition iterator
        
    Yields:
        Items from the iterator after Kerberos setup
    """
    # Set up Kerberos on this executor
    setup_kerberos_on_executor()
    
    # Yield all items from the iterator
    for item in iterator:
        yield item


def create_kerberos_aware_jdbc_options(connection_config, spark_context) -> dict:
    """Create JDBC options with Kerberos-aware settings.
    
    This function creates JDBC connection options that work with Kerberos authentication
    on AWS Glue executors.
    
    Args:
        connection_config: ConnectionConfig object with database settings
        spark_context: SparkContext for JVM access
        
    Returns:
        dict: JDBC options dictionary
    """
    from ..config.database_engines import DatabaseEngineManager
    
    options = {
        'url': connection_config.get_jdbc_url_with_credentials(),
        'driver': DatabaseEngineManager.get_driver_class(connection_config.engine_type),
    }
    
    # For Kerberos connections, we need to ensure the JAAS config is used
    if connection_config.uses_kerberos_authentication():
        # The JAAS config with refreshKrb5Config=true should handle the rest
        # But we can also set connection properties
        if connection_config.engine_type.lower() == 'sqlserver':
            # SQL Server specific: ensure authenticationScheme is set
            if 'authenticationScheme=JavaKerberos' not in options['url']:
                separator = ';' if '?' not in options['url'] else '&'
                options['url'] = f"{options['url']}{separator}authenticationScheme=JavaKerberos"
    
    return options


def refresh_kerberos_config_via_jvm(spark_context) -> bool:
    """Refresh Kerberos configuration via JVM.
    
    This function calls sun.security.krb5.Config.refresh() to force the JVM
    to re-read the krb5.conf file.
    
    Args:
        spark_context: SparkContext with JVM access
        
    Returns:
        bool: True if refresh was successful
    """
    try:
        if spark_context is None or spark_context._jvm is None:
            return False
        
        krb5_config_class = spark_context._jvm.sun.security.krb5.Config
        krb5_config_class.refresh()
        return True
        
    except Exception as e:
        logger.warning(f"Failed to refresh Kerberos config: {e}")
        return False
