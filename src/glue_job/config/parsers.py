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

from .job_config import JobConfig, NetworkConfig, ConnectionConfig
from .database_engines import DatabaseEngineManager

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
    
    # All optional parameters (network + Iceberg)
    ALL_OPTIONAL_PARAMS = OPTIONAL_NETWORK_PARAMS + ICEBERG_PARAMS
    
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
        engine_type = args[f'{connection_type}_ENGINE_TYPE'].lower()
        
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
                iceberg_config=iceberg_config
            )
        else:
            # For JDBC engines, use traditional parameters
            return ConnectionConfig(
                engine_type=engine_type,
                connection_string=args[f'{connection_type}_CONNECTION_STRING'],
                database=args[f'{connection_type}_DATABASE'],
                schema=args[f'{connection_type}_SCHEMA'],
                username=args[f'{connection_type}_DB_USER'],
                password=args[f'{connection_type}_DB_PASSWORD'],
                jdbc_driver_path=args[f'{connection_type}_JDBC_DRIVER_S3_PATH'],
                network_config=network_config
            )
    
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
            
            # Create job config
            job_config = JobConfig(
                job_name=args['JOB_NAME'],
                source_connection=source_connection,
                target_connection=target_connection,
                tables=table_names,
                validate_connections=validate_connections,
                connection_timeout_seconds=connection_timeout_seconds
            )
            
            logger.info(f"Created job configuration for: {job_config.job_name}")
            
            # Log network configuration summary
            network_summary = job_config.get_network_summary()
            if job_config.has_cross_vpc_connections():
                logger.info(f"Cross-VPC network configuration detected: {network_summary}")
            else:
                logger.info("Using same-VPC connectivity (no cross-VPC configuration)")
            
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