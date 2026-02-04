"""
Configuration parsers and connection string builders

This module provides utilities for parsing job configuration from CloudFormation
parameters and building JDBC connection strings for different database engines.
"""

import sys
import logging
from typing import Dict, List, Optional, Any

# Conditional import for AWS Glue utilities (only available in Glue runtime)
try:
    from awsglue.utils import getResolvedOptions
except ImportError:
    # Mock getResolvedOptions for local development/testing
    def getResolvedOptions(argv: List[str], options: List[str]) -> Dict[str, str]:
        """Mock implementation for local testing"""
        return {opt: f"mock_{opt.lower()}" for opt in options}

from .job_config import JobConfig, NetworkConfig, ConnectionConfig, GlueConnectionConfig
from .database_engines import DatabaseEngineManager
from .glue_connection_validator import validate_glue_connection_parameters_comprehensive
from ..network.glue_connection_errors import (
    GlueConnectionParameterError,
    GlueConnectionEngineCompatibilityError
)
from ..monitoring.logging import StructuredLogger

logger = logging.getLogger(__name__)


class JobConfigurationParser:
    """Parses job configuration from CloudFormation parameters."""
    
    # Base required CloudFormation parameters (always required)
    BASE_REQUIRED_PARAMS = [
        'JOB_NAME',
        'SOURCE_ENGINE_TYPE',
        'TARGET_ENGINE_TYPE',
        'SOURCE_DATABASE',
        'TARGET_DATABASE',
        'SOURCE_SCHEMA',
        'TARGET_SCHEMA',
        'TABLE_NAMES'
    ]
    
    # Traditional JDBC-specific parameters
    JDBC_REQUIRED_PARAMS = [
        'SOURCE_DB_USER',
        'SOURCE_DB_PASSWORD',
        'TARGET_DB_USER',
        'TARGET_DB_PASSWORD',
        'SOURCE_JDBC_DRIVER_S3_PATH',
        'TARGET_JDBC_DRIVER_S3_PATH',
        'SOURCE_CONNECTION_STRING',
        'TARGET_CONNECTION_STRING'
    ]
    
    # Legacy combined list for backward compatibility
    REQUIRED_PARAMS = BASE_REQUIRED_PARAMS + JDBC_REQUIRED_PARAMS
    
    # Iceberg-specific parameters
    ICEBERG_PARAMS = [
        'SOURCE_WAREHOUSE_LOCATION',
        'TARGET_WAREHOUSE_LOCATION',
        'SOURCE_CATALOG_ID',
        'TARGET_CATALOG_ID',
        'SOURCE_FORMAT_VERSION',
        'TARGET_FORMAT_VERSION'
    ]
    
    # Glue Connection parameters (CloudFormation format)
    GLUE_CONNECTION_PARAMS = [
        'CREATE_SOURCE_CONNECTION',
        'CREATE_TARGET_CONNECTION',
        'USE_SOURCE_CONNECTION',
        'USE_TARGET_CONNECTION',
        'SOURCE_JDBC_CONNECTION_NAME',
        'TARGET_JDBC_CONNECTION_NAME',
        'SOURCE_DATABASE_SECRET_ARN',
        'TARGET_DATABASE_SECRET_ARN'
    ]
    
    # Mapping from CloudFormation parameter names to internal names
    GLUE_CONNECTION_PARAM_MAPPING = {
        'CREATE_SOURCE_CONNECTION': 'createSourceConnection',
        'CREATE_TARGET_CONNECTION': 'createTargetConnection',
        'USE_SOURCE_CONNECTION': 'useSourceConnection',
        'USE_TARGET_CONNECTION': 'useTargetConnection'
    }
    
    # Optional network configuration parameters
    OPTIONAL_NETWORK_PARAMS = [
        'SOURCE_VPC_ID',
        'SOURCE_SUBNET_IDS',
        'SOURCE_SECURITY_GROUP_IDS',
        'CREATE_SOURCE_S3_VPC_ENDPOINT',
        'TARGET_VPC_ID',
        'TARGET_SUBNET_IDS',
        'TARGET_SECURITY_GROUP_IDS',
        'CREATE_TARGET_S3_VPC_ENDPOINT',
        'SOURCE_GLUE_CONNECTION_NAME',
        'TARGET_GLUE_CONNECTION_NAME',
        'VALIDATE_CONNECTIONS',
        'CONNECTION_TIMEOUT_SECONDS'
    ]
    
    # Optional bookmark configuration parameters
    OPTIONAL_BOOKMARK_PARAMS = [
        'MANUAL_BOOKMARK_CONFIG'
    ]
    
    # Optional migration performance configuration parameters
    OPTIONAL_PERFORMANCE_PARAMS = [
        'COUNTING_STRATEGY',
        'SIZE_THRESHOLD_ROWS',
        'FORCE_IMMEDIATE_COUNTING',
        'FORCE_DEFERRED_COUNTING',
        'PROGRESS_UPDATE_INTERVAL_SECONDS',
        'PROGRESS_BATCH_SIZE_ROWS',
        'ENABLE_PROGRESS_TRACKING',
        'ENABLE_PROGRESS_LOGGING',
        'ENABLE_DETAILED_METRICS',
        'METRICS_NAMESPACE'
    ]
    
    # Optional partitioned read parameters (for large dataset optimization)
    OPTIONAL_PARTITIONED_READ_PARAMS = [
        'ENABLE_PARTITIONED_READS',
        'PARTITIONED_READ_CONFIG',
        'DEFAULT_NUM_PARTITIONS',
        'DEFAULT_FETCH_SIZE'
    ]
    
    # Kerberos authentication parameters (optional)
    KERBEROS_PARAMS = [
        'SOURCE_KERBEROS_SPN',
        'SOURCE_KERBEROS_DOMAIN', 
        'SOURCE_KERBEROS_KDC',
        'SOURCE_KERBEROS_KEYTAB_S3_PATH',
        'TARGET_KERBEROS_SPN',
        'TARGET_KERBEROS_DOMAIN',
        'TARGET_KERBEROS_KDC',
        'TARGET_KERBEROS_KEYTAB_S3_PATH'
    ]
    
    # All optional parameters (network + Iceberg + bookmark + Glue Connection + performance + partitioned reads + Kerberos)
    ALL_OPTIONAL_PARAMS = OPTIONAL_NETWORK_PARAMS + ICEBERG_PARAMS + OPTIONAL_BOOKMARK_PARAMS + GLUE_CONNECTION_PARAMS + OPTIONAL_PERFORMANCE_PARAMS + OPTIONAL_PARTITIONED_READ_PARAMS + KERBEROS_PARAMS
    
    @classmethod
    def get_required_params_for_engines(cls, source_engine: str, target_engine: str) -> List[str]:
        """Get required parameters based on source and target engine types.
        
        Args:
            source_engine: Source database engine type
            target_engine: Target database engine type
            
        Returns:
            List[str]: List of required parameter names
        """
        required_params = []
        
        # Get excluded parameters for each engine
        source_excluded = DatabaseEngineManager.get_excluded_parameters(source_engine, 'source')
        target_excluded = DatabaseEngineManager.get_excluded_parameters(target_engine, 'target')
        
        # Always required base parameters (excluding schema which is optional for some engines)
        base_required = ['JOB_NAME', 'SOURCE_ENGINE_TYPE', 'TARGET_ENGINE_TYPE', 
                        'SOURCE_DATABASE', 'TARGET_DATABASE', 'TABLE_NAMES']
        
        # Add base parameters only if not excluded by the respective engine
        for param in base_required:
            should_exclude = False
            
            if param.startswith('SOURCE_') and param in source_excluded:
                should_exclude = True
            elif param.startswith('TARGET_') and param in target_excluded:
                should_exclude = True
            
            if not should_exclude:
                required_params.append(param)
        
        # Add schema parameters only if not excluded (optional for Iceberg)
        schema_params = ['SOURCE_SCHEMA', 'TARGET_SCHEMA']
        for param in schema_params:
            should_exclude = False
            
            if param.startswith('SOURCE_') and param in source_excluded:
                should_exclude = True
            elif param.startswith('TARGET_') and param in target_excluded:
                should_exclude = True
            
            if not should_exclude:
                required_params.append(param)
        
        # Add JDBC parameters only if not excluded by the respective engine
        for param in cls.JDBC_REQUIRED_PARAMS:
            should_exclude = False
            
            if param.startswith('SOURCE_') and param in source_excluded:
                should_exclude = True
            elif param.startswith('TARGET_') and param in target_excluded:
                should_exclude = True
            
            if not should_exclude:
                required_params.append(param)
        
        # Add Iceberg-specific required parameters
        if DatabaseEngineManager.is_iceberg_engine(source_engine):
            required_params.append('SOURCE_WAREHOUSE_LOCATION')
        
        if DatabaseEngineManager.is_iceberg_engine(target_engine):
            required_params.append('TARGET_WAREHOUSE_LOCATION')
        
        return required_params
    
    @classmethod
    def parse_job_arguments(cls) -> Dict[str, str]:
        """Parse job arguments from CloudFormation parameters with robust error handling."""
        try:
            logger.info(f"Starting argument parsing. Command line length: {len(sys.argv)}")
            
            # First, parse with all possible parameters to handle different engine types
            all_possible_params = cls.BASE_REQUIRED_PARAMS + cls.JDBC_REQUIRED_PARAMS + cls.ALL_OPTIONAL_PARAMS
            
            try:
                # Parse all parameters at once with getResolvedOptions
                args = getResolvedOptions(sys.argv, all_possible_params)
                logger.info(f"Successfully parsed all parameters using getResolvedOptions")
                
                # Set defaults for optional parameters that might be empty
                for param in cls.ALL_OPTIONAL_PARAMS:
                    if param not in args or args[param] is None:
                        args[param] = ''
                
            except Exception as e:
                logger.warning(f"getResolvedOptions failed: {str(e)}, trying manual parsing")
                
                # Fallback to manual parsing if getResolvedOptions fails
                args = cls._manual_parse_arguments(sys.argv)
            
            # Map CloudFormation parameter names to internal names for Glue Connection params
            cls._map_glue_connection_params(args)
            
            # Validate that all required parameters are present based on engine types
            cls.validate_required_parameters(args)
            
            # Set defaults for specific parameters
            args.setdefault('VALIDATE_CONNECTIONS', 'true')
            args.setdefault('CONNECTION_TIMEOUT_SECONDS', '30')
            
            # Log final parsed arguments (excluding sensitive data)
            safe_args = {k: v if 'PASSWORD' not in k else '***' for k, v in args.items()}
            logger.info(f"Successfully parsed {len(args)} arguments")
            logger.debug(f"Parsed arguments: {safe_args}")
            
            return args
            
        except Exception as e:
            logger.error(f"CRITICAL: Failed to parse job arguments: {str(e)}")
            logger.error(f"Command line arguments: {sys.argv}")
            raise RuntimeError(f"Job argument parsing failed: {str(e)}")
    
    @classmethod
    def _map_glue_connection_params(cls, args: Dict[str, str]) -> None:
        """Map CloudFormation parameter names to internal camelCase names.
        
        This ensures compatibility between CloudFormation parameters (uppercase with underscores)
        and internal code that expects camelCase parameter names.
        
        Args:
            args: Parsed job arguments (modified in place)
        """
        for cf_name, internal_name in cls.GLUE_CONNECTION_PARAM_MAPPING.items():
            if cf_name in args and args[cf_name]:
                args[internal_name] = args[cf_name]
                logger.debug(f"Mapped Glue Connection parameter: {cf_name} -> {internal_name} = {args[cf_name]}")
        
        # When CloudFormation creates a connection and passes its name via SOURCE_JDBC_CONNECTION_NAME,
        # the job should USE that connection, not create a new one.
        # CloudFormation handles creation; the job only uses existing connections.
        if args.get('SOURCE_JDBC_CONNECTION_NAME'):
            if not args.get('useSourceConnection'):
                args['useSourceConnection'] = args['SOURCE_JDBC_CONNECTION_NAME']
                logger.info(f"Auto-mapped SOURCE_JDBC_CONNECTION_NAME to useSourceConnection: {args['SOURCE_JDBC_CONNECTION_NAME']}")
            # Disable runtime creation since CloudFormation already created the connection
            # Use string 'false' to maintain type consistency (validator expects strings)
            if args.get('createSourceConnection'):
                args['createSourceConnection'] = 'false'
                logger.info("Disabled createSourceConnection - connection already created by CloudFormation")
        
        if args.get('TARGET_JDBC_CONNECTION_NAME'):
            if not args.get('useTargetConnection'):
                args['useTargetConnection'] = args['TARGET_JDBC_CONNECTION_NAME']
                logger.info(f"Auto-mapped TARGET_JDBC_CONNECTION_NAME to useTargetConnection: {args['TARGET_JDBC_CONNECTION_NAME']}")
            # Disable runtime creation since CloudFormation already created the connection
            # Use string 'false' to maintain type consistency (validator expects strings)
            if args.get('createTargetConnection'):
                args['createTargetConnection'] = 'false'
                logger.info("Disabled createTargetConnection - connection already created by CloudFormation")
    
    @classmethod
    def _manual_parse_arguments(cls, argv: List[str]) -> Dict[str, str]:
        """Manually parse command line arguments as fallback."""
        args = {}
        i = 0
        
        while i < len(argv):
            arg = argv[i]
            
            # Look for our custom parameters (start with --)
            if arg.startswith('--') and len(arg) > 2:
                param_name = arg[2:]  # Remove --
                
                # Check if this is one of our expected parameters
                all_possible_params = cls.BASE_REQUIRED_PARAMS + cls.JDBC_REQUIRED_PARAMS + cls.ALL_OPTIONAL_PARAMS
                if param_name in all_possible_params:
                    # Get the next argument as the value
                    if i + 1 < len(argv) and not argv[i + 1].startswith('--'):
                        args[param_name] = argv[i + 1]
                        i += 2  # Skip both parameter and value
                    else:
                        # Parameter without value, set as empty string
                        args[param_name] = ''
                        i += 1
                else:
                    # Skip unknown parameters
                    i += 1
            else:
                i += 1
        
        logger.info(f"Manual parsing found {len(args)} parameters")
        return args
    
    @classmethod
    def validate_required_parameters(cls, args: Dict[str, str]) -> None:
        """Validate that all required parameters are present based on engine types.
        
        Args:
            args: Parsed job arguments
            
        Raises:
            RuntimeError: If required parameters are missing
        """
        # Always required base parameters (excluding schema which is optional for some engines)
        base_required = ['JOB_NAME', 'SOURCE_ENGINE_TYPE', 'TARGET_ENGINE_TYPE', 
                        'SOURCE_DATABASE', 'TARGET_DATABASE', 'TABLE_NAMES']
        
        missing_base = [param for param in base_required if param not in args or not args[param]]
        
        if missing_base:
            raise RuntimeError(f"Missing required base parameters: {missing_base}")
        
        # Get engine types for validation
        source_engine = args.get('SOURCE_ENGINE_TYPE', '').lower()
        target_engine = args.get('TARGET_ENGINE_TYPE', '').lower()
        
        # Validate engine-specific parameters using the new approach
        cls._validate_engine_specific_parameters(args, 'SOURCE', source_engine)
        cls._validate_engine_specific_parameters(args, 'TARGET', target_engine)
        
        logger.info("Required parameter validation completed successfully")
    
    @classmethod
    def _validate_engine_specific_parameters(cls, args: Dict[str, str], 
                                           connection_type: str, engine_type: str) -> None:
        """Validate engine-specific required parameters.
        
        Args:
            args: Parsed job arguments
            connection_type: 'SOURCE' or 'TARGET'
            engine_type: Database engine type
            
        Raises:
            RuntimeError: If required parameters are missing
        """
        # Get required parameters for this engine type and role
        required_params = DatabaseEngineManager.get_required_parameters(engine_type, connection_type.lower())
        
        if required_params:
            # Use engine-specific required parameters
            missing_params = [param for param in required_params 
                            if param not in args or not args[param].strip()]
            
            if missing_params:
                raise RuntimeError(
                    f"Missing required parameters for {connection_type} {engine_type} engine: {missing_params}"
                )
            
            logger.info(f"Engine-specific parameter validation passed for {connection_type} {engine_type} engine")
        else:
            # Fallback validation based on engine type
            if DatabaseEngineManager.is_iceberg_engine(engine_type):
                # Iceberg required parameters
                required_iceberg = [
                    f'{connection_type}_DATABASE',  # database_name
                    f'{connection_type}_WAREHOUSE_LOCATION'
                ]
                
                missing_iceberg = [param for param in required_iceberg 
                                 if param not in args or not args[param].strip()]
                
                if missing_iceberg:
                    raise RuntimeError(
                        f"Missing required Iceberg parameters for {connection_type} engine: {missing_iceberg}"
                    )
                
                logger.info(f"Iceberg parameter validation passed for {connection_type} engine")
            else:
                # JDBC required parameters
                required_jdbc = [
                    f'{connection_type}_DATABASE',
                    f'{connection_type}_SCHEMA',
                    f'{connection_type}_DB_USER',
                    f'{connection_type}_DB_PASSWORD',
                    f'{connection_type}_JDBC_DRIVER_S3_PATH',
                    f'{connection_type}_CONNECTION_STRING'
                ]
                
                missing_jdbc = [param for param in required_jdbc 
                              if param not in args or not args[param].strip()]
                
                if missing_jdbc:
                    raise RuntimeError(
                        f"Missing required JDBC parameters for {connection_type} engine: {missing_jdbc}"
                    )
                
                logger.info(f"JDBC parameter validation passed for {connection_type} {engine_type} engine")
    
    @classmethod
    def parse_glue_connection_params(cls, args: Dict[str, str], connection_type: str) -> 'GlueConnectionConfig':
        """Parse Glue Connection parameters for source or target with enhanced logging.
        
        Args:
            args: Parsed job arguments
            connection_type: 'SOURCE' or 'TARGET' to identify which connection to parse
            
        Returns:
            GlueConnectionConfig: Parsed Glue Connection configuration
            
        Raises:
            GlueConnectionParameterError: If Glue Connection parameters are invalid
        """
        from .job_config import GlueConnectionConfig
        
        structured_logger = StructuredLogger("JobConfigurationParser")
        
        # Get connection type prefix for parameter names
        prefix = connection_type.upper()
        
        # Parse create connection parameter
        create_param_name = f'create{connection_type.title()}Connection'
        create_connection_str = args.get(create_param_name, '').strip().lower()
        create_connection = create_connection_str in ['true', '1', 'yes']
        
        # Parse use existing connection parameter
        use_param_name = f'use{connection_type.title()}Connection'
        use_existing_connection = args.get(use_param_name, '').strip()
        use_existing_connection = use_existing_connection if use_existing_connection else None
        
        # Determine connection strategy
        if create_connection:
            strategy = "create_glue"
            structured_logger.info(
                f"Glue Connection strategy determined: CREATE new connection",
                connection_type=connection_type.lower(),
                strategy=strategy,
                parameter=create_param_name
            )
        elif use_existing_connection:
            strategy = "use_glue"
            structured_logger.info(
                f"Glue Connection strategy determined: USE existing connection",
                connection_type=connection_type.lower(),
                strategy=strategy,
                connection_name=use_existing_connection,
                parameter=use_param_name
            )
        else:
            strategy = "direct_jdbc"
            structured_logger.info(
                f"Glue Connection strategy determined: DIRECT JDBC connection",
                connection_type=connection_type.lower(),
                strategy=strategy
            )
        
        # Create and validate configuration
        try:
            glue_config = GlueConnectionConfig(
                create_connection=create_connection,
                use_existing_connection=use_existing_connection
            )
            
            # Validate the configuration
            glue_config.validate()
            
            structured_logger.debug(
                f"Glue Connection configuration parsed successfully",
                connection_type=connection_type.lower(),
                strategy=glue_config.connection_strategy,
                create_connection=create_connection,
                use_existing_connection=use_existing_connection
            )
            
        except Exception as e:
            structured_logger.error(
                f"Failed to parse Glue Connection configuration",
                connection_type=connection_type.lower(),
                error=str(e)
            )
            raise GlueConnectionParameterError(
                f"Invalid Glue Connection configuration for {connection_type.lower()}: {str(e)}"
            )
        
        logger.info(f"Parsed Glue Connection configuration for {connection_type}: strategy={glue_config.connection_strategy}")
        
        return glue_config
    
    @classmethod
    def validate_glue_connection_params(cls, args: Dict[str, str]) -> Dict[str, Any]:
        """Validate Glue Connection parameter combinations with comprehensive validation.
        
        Args:
            args: Parsed job arguments
            
        Returns:
            Dict containing validation results and configurations
            
        Raises:
            GlueConnectionParameterError: If Glue Connection parameter combinations are invalid
            GlueConnectionEngineCompatibilityError: If engine compatibility issues exist
        """
        structured_logger = StructuredLogger("JobConfigurationParser")
        
        try:
            structured_logger.info("Starting comprehensive Glue Connection parameter validation")
            
            # Use comprehensive validator
            validation_results = validate_glue_connection_parameters_comprehensive(args)
            
            # Log validation summary
            structured_logger.info(
                "Glue Connection parameter validation completed successfully",
                source_strategy=validation_results.get('source_config', {}).get('strategy') if validation_results.get('source_config') else None,
                target_strategy=validation_results.get('target_config', {}).get('strategy') if validation_results.get('target_config') else None,
                warnings_count=len(validation_results.get('validation_warnings', [])),
                errors_count=len(validation_results.get('validation_errors', []))
            )
            
            return validation_results
            
        except (GlueConnectionParameterError, GlueConnectionEngineCompatibilityError) as e:
            structured_logger.error(
                "Glue Connection parameter validation failed",
                error_type=type(e).__name__,
                error_message=str(e)
            )
            raise e
        except Exception as e:
            structured_logger.error(
                "Unexpected error during Glue Connection parameter validation",
                error=str(e)
            )
            raise GlueConnectionParameterError(
                f"Validation failed due to unexpected error: {str(e)}"
            )
    
    @classmethod
    def validate_engine_specific_glue_connection_params(cls, args: Dict[str, str]) -> None:
        """Validate Glue Connection parameters against engine types and log warnings for Iceberg.
        
        Args:
            args: Parsed job arguments
        """
        source_engine = args.get('SOURCE_ENGINE_TYPE', '').lower()
        target_engine = args.get('TARGET_ENGINE_TYPE', '').lower()
        
        # Check for Glue Connection parameters
        glue_connection_params = [
            ('createSourceConnection', 'source', source_engine),
            ('createTargetConnection', 'target', target_engine),
            ('useSourceConnection', 'source', source_engine),
            ('useTargetConnection', 'target', target_engine)
        ]
        
        for param_name, connection_type, engine_type in glue_connection_params:
            param_value = args.get(param_name, '').strip()
            
            # If parameter is provided and engine is Iceberg, log warning
            if param_value and DatabaseEngineManager.is_iceberg_engine(engine_type):
                logger.warning(
                    f"Glue Connection parameter '{param_name}' provided for {connection_type} "
                    f"Iceberg engine '{engine_type}' will be ignored. "
                    "Iceberg connections use existing connection mechanism."
                )
        
        logger.info("Engine-specific Glue Connection parameter validation completed")
    
    @classmethod
    def parse_kerberos_config(cls, args: Dict[str, str], connection_type: str) -> Optional[Any]:
        """Parse Kerberos configuration for source or target connection.
        
        Args:
            args: Parsed job arguments
            connection_type: 'source' or 'target' to identify which connection to parse
            
        Returns:
            KerberosConfig if all three Kerberos parameters are provided, None otherwise
        """
        from .kerberos_config import KerberosConfig
        
        # Extract Kerberos parameters (using CloudFormation parameter format with underscores)
        prefix_upper = connection_type.upper()
        spn = args.get(f'{prefix_upper}_KERBEROS_SPN', '').strip()
        domain = args.get(f'{prefix_upper}_KERBEROS_DOMAIN', '').strip()
        kdc = args.get(f'{prefix_upper}_KERBEROS_KDC', '').strip()
        
        # Debug logging for Kerberos parameter detection
        logger.info(f"Parsing Kerberos parameters for {connection_type} connection:")
        logger.info(f"  Found SPN: '{spn}' (length: {len(spn)})")
        logger.info(f"  Found Domain: '{domain}' (length: {len(domain)})")
        logger.info(f"  Found KDC: '{kdc}' (length: {len(kdc)})")
        
        # Only create config if all three parameters are provided
        if spn and domain and kdc:
            logger.info(f"✓ COMPLETE KERBEROS CONFIGURATION DETECTED for {connection_type} connection")
            try:
                kerberos_config = KerberosConfig(spn=spn, domain=domain, kdc=kdc)
                logger.info(f"✓ KerberosConfig created successfully for {connection_type} connection")
                return kerberos_config
            except Exception as e:
                logger.error(f"✗ Failed to create KerberosConfig for {connection_type}: {str(e)}")
                raise
        
        # Log warning if partial configuration is detected
        if spn or domain or kdc:
            provided_params = []
            missing_params = []
            
            if spn:
                provided_params.append(f'{prefix_upper}_KERBEROS_SPN')
            else:
                missing_params.append(f'{prefix_upper}_KERBEROS_SPN')
            
            if domain:
                provided_params.append(f'{prefix_upper}_KERBEROS_DOMAIN')
            else:
                missing_params.append(f'{prefix_upper}_KERBEROS_DOMAIN')
            
            if kdc:
                provided_params.append(f'{prefix_upper}_KERBEROS_KDC')
            else:
                missing_params.append(f'{prefix_upper}_KERBEROS_KDC')
            
            logger.warning(
                f"Partial Kerberos configuration detected for {connection_type} connection. "
                f"Provided: {provided_params}, Missing: {missing_params}. "
                "All three parameters (SPN, Domain, KDC) are required for Kerberos authentication."
            )
        
        return None

    @classmethod
    def create_connection_config_with_glue_support(cls, args: Dict[str, str], 
                                                  connection_type: str,
                                                  network_config: Optional['NetworkConfig'],
                                                  iceberg_config: Dict[str, Any]) -> 'ConnectionConfig':
        """Create ConnectionConfig with Glue Connection support.
        
        Args:
            args: Parsed job arguments
            connection_type: 'SOURCE' or 'TARGET'
            network_config: Network configuration (if any)
            iceberg_config: Iceberg configuration (if Iceberg engine)
            
        Returns:
            ConnectionConfig: Configured connection with Glue Connection support
        """
        from .job_config import ConnectionConfig
        
        engine_type = args[f'{connection_type}_ENGINE_TYPE'].lower()
        
        # Parse Kerberos configuration
        kerberos_config = cls.parse_kerberos_config(args, connection_type.lower())
        
        # Parse Glue Connection configuration
        glue_connection_config = None
        if not DatabaseEngineManager.is_iceberg_engine(engine_type):
            # Only parse Glue Connection config for JDBC engines
            glue_connection_config = cls.parse_glue_connection_params(args, connection_type)
            
            # If Kerberos is configured and CreateConnection=true, override to use pre-created Kerberos connection
            if kerberos_config and glue_connection_config and glue_connection_config.create_connection:
                from .job_config import GlueConnectionConfig
                # Get the pre-created Kerberos connection name from CloudFormation
                kerberos_connection_name = args.get(f'{connection_type}_JDBC_CONNECTION_NAME', '').strip()
                if kerberos_connection_name:
                    logger.info(f"Overriding Glue Connection config to use pre-created Kerberos connection: {kerberos_connection_name}")
                    glue_connection_config = GlueConnectionConfig(
                        create_connection=False,
                        use_existing_connection=kerberos_connection_name
                    )
        
        # Log Kerberos configuration detection
        if kerberos_config:
            logger.info(f"✓ KERBEROS AUTHENTICATION CONFIGURED for {connection_type.lower()} connection")
            logger.info(f"  Kerberos Config: SPN='{kerberos_config.spn}', Domain='{kerberos_config.domain}', KDC='{kerberos_config.kdc}'")
        else:
            logger.info(f"Standard username/password authentication will be used for {connection_type.lower()} connection")
        
        # Get keytab S3 path if available
        keytab_s3_path = args.get(f'{connection_type}_KERBEROS_KEYTAB_S3_PATH', '').strip()
        keytab_s3_path = keytab_s3_path if keytab_s3_path else None
        
        if DatabaseEngineManager.is_iceberg_engine(engine_type):
            # For Iceberg engines, use Iceberg-specific parameters
            return ConnectionConfig(
                engine_type=engine_type,
                connection_string='',  # Not used for Iceberg
                database=iceberg_config['database_name'],
                schema=iceberg_config['table_name'],  # Schema field reused for table name in Iceberg
                username='',  # Not used for Iceberg
                password='',  # Not used for Iceberg
                jdbc_driver_path='',  # Not used for Iceberg
                network_config=network_config,
                iceberg_config=iceberg_config,
                glue_connection_config=glue_connection_config,  # Will be None for Iceberg
                kerberos_config=kerberos_config,
                kerberos_keytab_s3_path=keytab_s3_path
            )
        else:
            # For JDBC engines, use traditional parameters
            # When using Glue connections, some parameters may be omitted
            connection_string = args.get(f'{connection_type}_CONNECTION_STRING', '')
            username = args.get(f'{connection_type}_DB_USER', '')
            password = args.get(f'{connection_type}_DB_PASSWORD', '')
            jdbc_driver_path = args.get(f'{connection_type}_JDBC_DRIVER_S3_PATH', '')
            
            return ConnectionConfig(
                engine_type=engine_type,
                connection_string=connection_string,
                database=args[f'{connection_type}_DATABASE'],
                schema=args[f'{connection_type}_SCHEMA'],
                username=username,
                password=password,
                jdbc_driver_path=jdbc_driver_path,
                network_config=network_config,
                glue_connection_config=glue_connection_config,
                kerberos_config=kerberos_config,
                kerberos_keytab_s3_path=keytab_s3_path
            )
    
    @classmethod
    def validate_iceberg_parameters(cls, args: Dict[str, str], connection_type: str) -> Dict[str, Any]:
        """Validate and extract Iceberg-specific parameters.
        
        Args:
            args: Parsed job arguments
            connection_type: 'SOURCE' or 'TARGET' to identify which connection to validate
            
        Returns:
            Dict containing validated Iceberg parameters
            
        Raises:
            ValueError: If required Iceberg parameters are missing or invalid
        """
        engine_type = args.get(f'{connection_type}_ENGINE_TYPE', '').lower()
        
        if not DatabaseEngineManager.is_iceberg_engine(engine_type):
            return {}  # Not an Iceberg engine, return empty dict
        
        logger.info(f"Validating Iceberg parameters for {connection_type} engine")
        
        # Extract Iceberg-specific parameters
        database_name = args.get(f'{connection_type}_DATABASE', '').strip()
        # NOTE: For Iceberg engines, the SCHEMA parameter is reused to pass the table name
        # since AWS Glue Data Catalog for Iceberg has only Database->Table hierarchy (no schemas)
        # However, for multi-table replication, we use the table names from TABLE_NAMES parameter
        table_name = args.get(f'{connection_type}_SCHEMA', '').strip()  # Schema field used as table name for Iceberg
        warehouse_location = args.get(f'{connection_type}_WAREHOUSE_LOCATION', '').strip()
        catalog_id = args.get(f'{connection_type}_CATALOG_ID', '').strip()
        format_version = args.get(f'{connection_type}_FORMAT_VERSION', '2').strip() or '2'
        
        # Validate required parameters
        if not database_name:
            raise ValueError(f"Missing required Iceberg parameter: {connection_type}_DATABASE (database_name)")
        
        # For Iceberg, table_name can be empty if we're using TABLE_NAMES for multi-table replication
        # We'll use a placeholder if table_name is empty
        if not table_name:
            table_name = 'multi_table_replication'  # Placeholder for multi-table scenarios
            logger.info(f"Using placeholder table name for {connection_type} Iceberg engine: {table_name}")
        
        if not warehouse_location:
            raise ValueError(f"Missing required Iceberg parameter: {connection_type}_WAREHOUSE_LOCATION")
        
        # Create Iceberg configuration for validation
        iceberg_config = {
            'database_name': database_name,
            'table_name': table_name,
            'warehouse_location': warehouse_location,
            'catalog_id': catalog_id if catalog_id else None,
            'format_version': format_version
        }
        
        # Validate using DatabaseEngineManager
        if not DatabaseEngineManager.validate_iceberg_config(iceberg_config):
            raise ValueError(f"Invalid Iceberg configuration for {connection_type} engine")
        
        logger.info(f"Iceberg parameters validated successfully for {connection_type} engine")
        return iceberg_config
    
    @classmethod
    def get_required_parameters_for_engine(cls, engine_type: str) -> List[str]:
        """Get required parameters based on engine type.
        
        Args:
            engine_type: Database engine type
            
        Returns:
            List of required parameter names
        """
        base_params = ['JOB_NAME', 'SOURCE_ENGINE_TYPE', 'TARGET_ENGINE_TYPE', 'TABLE_NAMES']
        
        if DatabaseEngineManager.is_iceberg_engine(engine_type):
            # For Iceberg engines, we need different parameters
            return base_params + [
                'SOURCE_DATABASE', 'TARGET_DATABASE',  # database_name
                'SOURCE_SCHEMA', 'TARGET_SCHEMA',      # table_name (reusing schema field)
                'SOURCE_WAREHOUSE_LOCATION', 'TARGET_WAREHOUSE_LOCATION'
            ]
        else:
            # For JDBC engines, use traditional parameters
            return cls.REQUIRED_PARAMS
    
    @classmethod
    def create_connection_config(cls, args: Dict[str, str], connection_type: str, 
                               network_config: Optional[NetworkConfig],
                               iceberg_config: Dict[str, Any]) -> ConnectionConfig:
        """Create ConnectionConfig for the specified connection type.
        
        Args:
            args: Parsed job arguments
            connection_type: 'SOURCE' or 'TARGET'
            network_config: Network configuration (if any)
            iceberg_config: Iceberg configuration (if Iceberg engine)
            
        Returns:
            ConnectionConfig: Configured connection
        """
        # Use the enhanced method with Glue Connection support
        return cls.create_connection_config_with_glue_support(
            args, connection_type, network_config, iceberg_config
        )
    
    @classmethod
    def parse_migration_performance_config(cls, args: Dict[str, str]) -> 'MigrationPerformanceConfig':
        """Parse migration performance configuration from CloudFormation parameters.
        
        Args:
            args: Parsed job arguments
            
        Returns:
            MigrationPerformanceConfig with parsed values or defaults
        """
        from .job_config import MigrationPerformanceConfig
        
        # Parse counting strategy parameters
        counting_strategy = args.get('COUNTING_STRATEGY', 'auto').strip().lower()
        if counting_strategy not in ['immediate', 'deferred', 'auto']:
            logger.warning(f"Invalid counting_strategy '{counting_strategy}', defaulting to 'auto'")
            counting_strategy = 'auto'
        
        size_threshold_rows = int(args.get('SIZE_THRESHOLD_ROWS', '1000000'))
        
        force_immediate_str = args.get('FORCE_IMMEDIATE_COUNTING', 'false').strip().lower()
        force_immediate_counting = force_immediate_str in ['true', '1', 'yes']
        
        force_deferred_str = args.get('FORCE_DEFERRED_COUNTING', 'false').strip().lower()
        force_deferred_counting = force_deferred_str in ['true', '1', 'yes']
        
        # Parse progress tracking parameters
        progress_update_interval_seconds = int(args.get('PROGRESS_UPDATE_INTERVAL_SECONDS', '60'))
        progress_batch_size_rows = int(args.get('PROGRESS_BATCH_SIZE_ROWS', '100000'))
        
        enable_progress_tracking_str = args.get('ENABLE_PROGRESS_TRACKING', 'true').strip().lower()
        enable_progress_tracking = enable_progress_tracking_str in ['true', '1', 'yes']
        
        enable_progress_logging_str = args.get('ENABLE_PROGRESS_LOGGING', 'true').strip().lower()
        enable_progress_logging = enable_progress_logging_str in ['true', '1', 'yes']
        
        # Parse metrics parameters
        enable_detailed_metrics_str = args.get('ENABLE_DETAILED_METRICS', 'true').strip().lower()
        enable_detailed_metrics = enable_detailed_metrics_str in ['true', '1', 'yes']
        
        metrics_namespace = args.get('METRICS_NAMESPACE', 'AWS/Glue/DataReplication').strip()
        
        # Create configuration
        config = MigrationPerformanceConfig(
            counting_strategy=counting_strategy,
            size_threshold_rows=size_threshold_rows,
            force_immediate_counting=force_immediate_counting,
            force_deferred_counting=force_deferred_counting,
            progress_update_interval_seconds=progress_update_interval_seconds,
            progress_batch_size_rows=progress_batch_size_rows,
            enable_progress_tracking=enable_progress_tracking,
            enable_progress_logging=enable_progress_logging,
            enable_detailed_metrics=enable_detailed_metrics,
            metrics_namespace=metrics_namespace
        )
        
        # Validate configuration
        try:
            config.validate()
            logger.info(f"Migration performance configuration parsed successfully: strategy={counting_strategy}, "
                       f"progress_tracking={enable_progress_tracking}, detailed_metrics={enable_detailed_metrics}")
        except ValueError as e:
            logger.error(f"Invalid migration performance configuration: {str(e)}")
            raise
        
        return config
    
    @classmethod
    def parse_partitioned_read_config(cls, args: Dict[str, str]) -> Optional['PartitionedReadConfig']:
        """Parse partitioned read configuration from CloudFormation parameters.
        
        Args:
            args: Parsed job arguments
            
        Returns:
            PartitionedReadConfig if enabled, None otherwise
        """
        from .partitioned_read_config import PartitionedReadConfig
        
        enable_partitioned_reads = args.get('ENABLE_PARTITIONED_READS', 'auto').strip().lower()
        
        # If disabled, return None
        if enable_partitioned_reads == 'disabled':
            logger.info("Partitioned JDBC reads disabled by configuration")
            return None
        
        # Parse configuration JSON
        partitioned_read_config_json = args.get('PARTITIONED_READ_CONFIG', '').strip()
        
        # Parse default values
        default_num_partitions = int(args.get('DEFAULT_NUM_PARTITIONS', '0'))
        default_fetch_size = int(args.get('DEFAULT_FETCH_SIZE', '10000'))
        
        # Create configuration
        config = PartitionedReadConfig.from_args(
            enable_partitioned_reads=enable_partitioned_reads,
            partitioned_read_config_json=partitioned_read_config_json,
            default_num_partitions=default_num_partitions,
            default_fetch_size=default_fetch_size
        )
        
        logger.info(f"Partitioned read configuration parsed: enabled={config.enabled}, "
                   f"default_partitions={config.default_num_partitions}, "
                   f"default_fetch_size={config.default_fetch_size}, "
                   f"table_configs={len(config.table_configs)}")
        
        return config
    
    @classmethod
    def parse_network_config(cls, args: Dict[str, str], prefix: str) -> Optional[NetworkConfig]:
        """Parse network configuration from CloudFormation parameters.
        
        Args:
            args: Parsed job arguments
            prefix: 'SOURCE' or 'TARGET' to identify which network config to parse
            
        Returns:
            NetworkConfig if network parameters are provided, None otherwise
        """
        vpc_id = args.get(f'{prefix}_VPC_ID', '').strip()
        subnet_ids_str = args.get(f'{prefix}_SUBNET_IDS', '').strip()
        security_group_ids_str = args.get(f'{prefix}_SECURITY_GROUP_IDS', '').strip()
        glue_connection_name = args.get(f'{prefix}_GLUE_CONNECTION_NAME', '').strip()
        create_s3_vpc_endpoint = args.get(f'CREATE_{prefix}_S3_VPC_ENDPOINT', 'NO').upper() == 'YES'
        
        # If no network configuration provided, return None
        if not vpc_id and not subnet_ids_str and not security_group_ids_str and not glue_connection_name:
            return None
        
        # Parse comma-separated lists
        subnet_ids = [s.strip() for s in subnet_ids_str.split(',') if s.strip()] if subnet_ids_str else None
        security_group_ids = [s.strip() for s in security_group_ids_str.split(',') if s.strip()] if security_group_ids_str else None
        
        return NetworkConfig(
            vpc_id=vpc_id if vpc_id else None,
            subnet_ids=subnet_ids,
            security_group_ids=security_group_ids,
            glue_connection_name=glue_connection_name if glue_connection_name else None,
            create_s3_vpc_endpoint=create_s3_vpc_endpoint
        )
    
    @classmethod
    def create_job_config(cls, args: Dict[str, str]) -> JobConfig:
        """Create JobConfig from parsed arguments."""
        try:
            # Validate Glue Connection parameters
            cls.validate_glue_connection_params(args)
            cls.validate_engine_specific_glue_connection_params(args)
            
            # Parse table names (comma-separated)
            table_names = [table.strip() for table in args['TABLE_NAMES'].split(',') if table.strip()]
            
            # Parse network configurations
            source_network_config = cls.parse_network_config(args, 'SOURCE')
            target_network_config = cls.parse_network_config(args, 'TARGET')
            
            # Validate and extract Iceberg parameters
            source_iceberg_config = cls.validate_iceberg_parameters(args, 'SOURCE')
            target_iceberg_config = cls.validate_iceberg_parameters(args, 'TARGET')
            
            # Create source connection config
            source_connection = cls.create_connection_config(
                args, 'SOURCE', source_network_config, source_iceberg_config
            )
            
            # Create target connection config
            target_connection = cls.create_connection_config(
                args, 'TARGET', target_network_config, target_iceberg_config
            )
            
            # Parse connection validation settings
            validate_connections = args.get('VALIDATE_CONNECTIONS', 'true').lower() == 'true'
            connection_timeout_seconds = int(args.get('CONNECTION_TIMEOUT_SECONDS', '30'))
            
            # Parse manual bookmark configuration
            manual_bookmark_config = args.get('MANUAL_BOOKMARK_CONFIG', '').strip()
            manual_bookmark_config = manual_bookmark_config if manual_bookmark_config else None
            
            # Parse migration performance configuration
            migration_performance_config = cls.parse_migration_performance_config(args)
            
            # Parse partitioned read configuration
            partitioned_read_config = cls.parse_partitioned_read_config(args)
            
            # Create job config
            job_config = JobConfig(
                job_name=args['JOB_NAME'],
                source_connection=source_connection,
                target_connection=target_connection,
                tables=table_names,
                validate_connections=validate_connections,
                connection_timeout_seconds=connection_timeout_seconds,
                manual_bookmark_config=manual_bookmark_config,
                migration_performance_config=migration_performance_config,
                partitioned_read_config=partitioned_read_config
            )
            
            logger.info(f"Created job configuration for: {job_config.job_name}")
            
            # Log network configuration summary
            network_summary = job_config.get_network_summary()
            if job_config.has_cross_vpc_connections():
                logger.info(f"Cross-VPC network configuration detected: {network_summary}")
            else:
                logger.info("Using same-VPC connectivity (no cross-VPC configuration)")
            
            # Log performance configuration summary
            performance_summary = job_config.get_performance_summary()
            logger.info(f"Migration performance configuration: {performance_summary}")
            
            return job_config
            
        except Exception as e:
            logger.error(f"Failed to create job configuration: {str(e)}")
            raise RuntimeError(f"Invalid job configuration: {str(e)}")
    
    @classmethod
    def validate_configuration(cls, job_config: JobConfig) -> None:
        """Validate the complete job configuration."""
        # Validate engine types
        if not DatabaseEngineManager.is_engine_supported(job_config.source_connection.engine_type):
            raise ValueError(f"Unsupported source engine: {job_config.source_connection.engine_type}")
        
        if not DatabaseEngineManager.is_engine_supported(job_config.target_connection.engine_type):
            raise ValueError(f"Unsupported target engine: {job_config.target_connection.engine_type}")
        
        # Validate connection strings (bypass for Iceberg engines)
        if not DatabaseEngineManager.is_iceberg_engine(job_config.source_connection.engine_type):
            if not DatabaseEngineManager.validate_connection_string(
                job_config.source_connection.engine_type,
                job_config.source_connection.connection_string
            ):
                raise ValueError("Invalid source connection string format")
        
        if not DatabaseEngineManager.is_iceberg_engine(job_config.target_connection.engine_type):
            if not DatabaseEngineManager.validate_connection_string(
                job_config.target_connection.engine_type,
                job_config.target_connection.connection_string
            ):
                raise ValueError("Invalid target connection string format")
        
        # Validate Iceberg configurations
        cls.validate_iceberg_configurations(job_config)
        
        # Validate network configurations
        cls.validate_network_configuration(job_config)
        
        logger.info("Job configuration validation completed successfully")
    
    @classmethod
    def validate_iceberg_configurations(cls, job_config: JobConfig) -> None:
        """Validate Iceberg-specific configurations."""
        # Validate source Iceberg configuration
        if job_config.source_connection.is_iceberg_engine():
            iceberg_config = job_config.source_connection.get_iceberg_config()
            if not iceberg_config:
                raise ValueError("Missing Iceberg configuration for source engine")
            
            if not DatabaseEngineManager.validate_iceberg_config(iceberg_config):
                raise ValueError("Invalid source Iceberg configuration")
            
            logger.info("Source Iceberg configuration validated successfully")
        
        # Validate target Iceberg configuration
        if job_config.target_connection.is_iceberg_engine():
            iceberg_config = job_config.target_connection.get_iceberg_config()
            if not iceberg_config:
                raise ValueError("Missing Iceberg configuration for target engine")
            
            if not DatabaseEngineManager.validate_iceberg_config(iceberg_config):
                raise ValueError("Invalid target Iceberg configuration")
            
            logger.info("Target Iceberg configuration validated successfully")
        
        # Validate engine combination compatibility
        cls.validate_engine_compatibility(job_config)
    
    @classmethod
    def validate_engine_compatibility(cls, job_config: JobConfig) -> None:
        """Validate compatibility between source and target engines."""
        source_engine = job_config.source_connection.engine_type
        target_engine = job_config.target_connection.engine_type
        
        source_is_iceberg = DatabaseEngineManager.is_iceberg_engine(source_engine)
        target_is_iceberg = DatabaseEngineManager.is_iceberg_engine(target_engine)
        
        # Log engine combination
        if source_is_iceberg and target_is_iceberg:
            logger.info("Iceberg-to-Iceberg replication detected")
        elif source_is_iceberg:
            logger.info(f"Iceberg-to-{target_engine} replication detected")
        elif target_is_iceberg:
            logger.info(f"{source_engine}-to-Iceberg replication detected")
        else:
            logger.info(f"{source_engine}-to-{target_engine} replication detected")
        
        # All combinations are currently supported
        # Future: Add specific validation rules if certain combinations are not supported
        logger.info("Engine combination compatibility validated successfully")
    
    @classmethod
    def validate_network_configuration(cls, job_config: JobConfig) -> None:
        """Validate network configuration parameters."""
        # Validate source network configuration
        if job_config.source_connection.network_config:
            cls._validate_single_network_config(
                job_config.source_connection.network_config, "source"
            )
        
        # Validate target network configuration
        if job_config.target_connection.network_config:
            cls._validate_single_network_config(
                job_config.target_connection.network_config, "target"
            )
        
        logger.info("Network configuration validation completed")
    
    @classmethod
    def _validate_single_network_config(cls, network_config: NetworkConfig, connection_type: str) -> None:
        """Validate a single network configuration."""
        # If Glue connection name is provided, it should be sufficient
        if network_config.glue_connection_name:
            logger.info(f"Using Glue connection for {connection_type}: {network_config.glue_connection_name}")
            return
        
        # If network details are provided, validate completeness
        if network_config.has_network_config():
            if not network_config.vpc_id:
                raise ValueError(f"VPC ID is required for {connection_type} network configuration")
            if not network_config.subnet_ids:
                raise ValueError(f"Subnet IDs are required for {connection_type} network configuration")
            if not network_config.security_group_ids:
                raise ValueError(f"Security Group IDs are required for {connection_type} network configuration")
            
            logger.info(f"Network configuration validated for {connection_type}: VPC {network_config.vpc_id}")
        
        # Warn if partial network configuration is provided
        if (network_config.vpc_id or network_config.subnet_ids or network_config.security_group_ids) and not network_config.has_network_config():
            logger.warning(f"Partial network configuration detected for {connection_type} - some parameters may be missing")


class ConnectionStringBuilder:
    """Builds JDBC connection strings for different database engines."""
    
    @classmethod
    def build_connection_string(cls, engine_type: str, host: str, port: int, 
                              database: str, **kwargs) -> str:
        """Build JDBC connection string for the specified engine."""
        engine_type = engine_type.lower()
        
        if not DatabaseEngineManager.is_engine_supported(engine_type):
            raise ValueError(f"Unsupported database engine: {engine_type}")
        
        config = DatabaseEngineManager.ENGINE_CONFIGS[engine_type]
        
        # Use default port if not specified
        if port is None or port <= 0:
            port = config['default_port']
        
        # Build base connection string
        connection_string = config['url_template'].format(
            host=host,
            port=port,
            database=database
        )
        
        # Add engine-specific parameters
        if engine_type == 'sqlserver':
            # Add common SQL Server parameters
            params = []
            if kwargs.get('encrypt', True):
                params.append('encrypt=true')
            if kwargs.get('trustServerCertificate', False):
                params.append('trustServerCertificate=true')
            if kwargs.get('loginTimeout'):
                params.append(f"loginTimeout={kwargs['loginTimeout']}")
            
            if params:
                connection_string += ';' + ';'.join(params)
        
        elif engine_type == 'oracle':
            # Add Oracle-specific parameters if needed
            if kwargs.get('connectionTimeout'):
                connection_string += f"?oracle.net.CONNECT_TIMEOUT={kwargs['connectionTimeout']}"
        
        elif engine_type == 'postgresql':
            # Add PostgreSQL-specific parameters
            params = []
            if kwargs.get('ssl', False):
                params.append('ssl=true')
            if kwargs.get('connectTimeout'):
                params.append(f"connectTimeout={kwargs['connectTimeout']}")
            if kwargs.get('socketTimeout'):
                params.append(f"socketTimeout={kwargs['socketTimeout']}")
            
            if params:
                connection_string += '?' + '&'.join(params)
        
        elif engine_type == 'db2':
            # Add DB2-specific parameters
            params = []
            if kwargs.get('loginTimeout'):
                params.append(f"loginTimeout={kwargs['loginTimeout']}")
            if kwargs.get('blockingReadConnectionTimeout'):
                params.append(f"blockingReadConnectionTimeout={kwargs['blockingReadConnectionTimeout']}")
            
            if params:
                connection_string += ':' + ';'.join(params) + ';'
        
        return connection_string
    
    @classmethod
    def parse_connection_string(cls, connection_string: str) -> Dict[str, Any]:
        """Parse connection string to extract components."""
        try:
            # Determine engine type from connection string prefix
            engine_type = None
            for engine, config in DatabaseEngineManager.ENGINE_CONFIGS.items():
                url_prefix = config['url_template'].split('{')[0]
                if connection_string.startswith(url_prefix.replace('{host}', '').replace('{port}', '').replace('{database}', '')):
                    engine_type = engine
                    break
            
            if not engine_type:
                raise ValueError("Unable to determine engine type from connection string")
            
            # Basic parsing - this is a simplified implementation
            # In production, you might want more robust parsing
            parsed = {
                'engine_type': engine_type,
                'connection_string': connection_string
            }
            
            return parsed
            
        except Exception as e:
            raise ValueError(f"Failed to parse connection string: {str(e)}")