"""
Enhanced parameter validation for Glue Connection operations.

This module provides comprehensive validation for Glue Connection parameters
with clear error messages and detailed validation rules.
"""

import re
import logging
from typing import Dict, Any, List, Optional, Tuple
from ..network.glue_connection_errors import (
    GlueConnectionParameterError,
    GlueConnectionEngineCompatibilityError
)
from .database_engines import DatabaseEngineManager
from ..monitoring.logging import StructuredLogger

logger = logging.getLogger(__name__)


class GlueConnectionParameterValidator:
    """Comprehensive validator for Glue Connection parameters."""
    
    # Valid connection name pattern (AWS Glue requirements)
    CONNECTION_NAME_PATTERN = re.compile(r'^[a-zA-Z0-9_-]+$')
    MAX_CONNECTION_NAME_LENGTH = 255
    
    # Glue Connection parameter names
    GLUE_CONNECTION_PARAMS = {
        'source': {
            'create': 'createSourceConnection',
            'use': 'useSourceConnection'
        },
        'target': {
            'create': 'createTargetConnection', 
            'use': 'useTargetConnection'
        }
    }
    
    # Required JDBC parameters for Glue Connection creation
    REQUIRED_JDBC_PARAMS = {
        'source': [
            'SOURCE_CONNECTION_STRING',
            'SOURCE_DB_USER',
            'SOURCE_DB_PASSWORD'
        ],
        'target': [
            'TARGET_CONNECTION_STRING',
            'TARGET_DB_USER', 
            'TARGET_DB_PASSWORD'
        ]
    }
    
    def __init__(self):
        self.structured_logger = StructuredLogger("GlueConnectionParameterValidator")
    
    def validate_all_glue_connection_parameters(self, args: Dict[str, str]) -> Dict[str, Any]:
        """Perform comprehensive validation of all Glue Connection parameters.
        
        Args:
            args: Parsed job arguments
            
        Returns:
            Dict containing validation results and parsed configurations
            
        Raises:
            GlueConnectionParameterError: For parameter validation failures
            GlueConnectionEngineCompatibilityError: For engine compatibility issues
        """
        validation_results = {
            'source_config': None,
            'target_config': None,
            'validation_warnings': [],
            'validation_errors': []
        }
        
        try:
            self.structured_logger.info("Starting comprehensive Glue Connection parameter validation")
            
            # Step 1: Validate parameter combinations (mutual exclusivity)
            self._validate_parameter_combinations(args)
            
            # Step 2: Validate engine compatibility
            engine_compatibility = self._validate_engine_compatibility(args)
            validation_results['validation_warnings'].extend(engine_compatibility['warnings'])
            
            # Step 3: Validate individual connection configurations
            for connection_type in ['source', 'target']:
                try:
                    config = self._validate_connection_configuration(args, connection_type)
                    validation_results[f'{connection_type}_config'] = config
                    
                    if config and config.get('warnings'):
                        validation_results['validation_warnings'].extend(config['warnings'])
                        
                except GlueConnectionParameterError as e:
                    validation_results['validation_errors'].append(str(e))
                    raise e
            
            # Step 4: Validate cross-connection consistency
            self._validate_cross_connection_consistency(
                validation_results['source_config'],
                validation_results['target_config']
            )
            
            # Step 5: Log validation summary
            self._log_validation_summary(validation_results)
            
            self.structured_logger.info("Glue Connection parameter validation completed successfully")
            return validation_results
            
        except (GlueConnectionParameterError, GlueConnectionEngineCompatibilityError):
            raise
        except Exception as e:
            self.structured_logger.error(
                "Unexpected error during Glue Connection parameter validation",
                error=str(e)
            )
            raise GlueConnectionParameterError(
                f"Validation failed due to unexpected error: {str(e)}"
            )
    
    def _validate_parameter_combinations(self, args: Dict[str, str]) -> None:
        """Validate that Glue Connection parameter combinations are valid.
        
        Args:
            args: Parsed job arguments
            
        Raises:
            GlueConnectionParameterError: If parameter combinations are invalid
        """
        validation_errors = []
        
        for connection_type in ['source', 'target']:
            create_param = self.GLUE_CONNECTION_PARAMS[connection_type]['create']
            use_param = self.GLUE_CONNECTION_PARAMS[connection_type]['use']
            
            create_value = self._parse_boolean_parameter(args.get(create_param, ''))
            use_value = args.get(use_param, '').strip()
            
            # Check for mutual exclusivity
            if create_value and use_value:
                validation_errors.append({
                    'type': 'mutual_exclusivity',
                    'connection_type': connection_type,
                    'create_param': create_param,
                    'use_param': use_param,
                    'message': (
                        f"Parameters {create_param} and {use_param} are mutually exclusive. "
                        f"Cannot both create and use existing {connection_type} Glue Connection. "
                        f"Choose either '{create_param}=true' OR provide '{use_param}=<connection-name>', but not both."
                    )
                })
            
            # Validate connection name format if using existing connection
            if use_value:
                try:
                    self._validate_connection_name_format(use_value, use_param)
                except GlueConnectionParameterError as e:
                    validation_errors.append({
                        'type': 'invalid_format',
                        'connection_type': connection_type,
                        'parameter': use_param,
                        'value': use_value,
                        'message': str(e)
                    })
        
        # If there are validation errors, raise with detailed information
        if validation_errors:
            error_messages = []
            for error in validation_errors:
                error_messages.append(f"• {error['message']}")
            
            combined_message = (
                f"Glue Connection parameter validation failed:\n" +
                "\n".join(error_messages) +
                f"\n\nValid parameter combinations:\n"
                f"• For creating new connections: createSourceConnection=true, createTargetConnection=true\n"
                f"• For using existing connections: useSourceConnection=<name>, useTargetConnection=<name>\n"
                f"• For mixed usage: createSourceConnection=true + useTargetConnection=<name>\n"
                f"• Connection names must contain only alphanumeric characters, hyphens, and underscores"
            )
            
            # Use the first error for the exception details
            first_error = validation_errors[0]
            raise GlueConnectionParameterError(
                combined_message,
                parameter_name=first_error.get('create_param') or first_error.get('parameter'),
                parameter_value=first_error.get('value', 'true'),
                conflicting_parameter=first_error.get('use_param')
            )
    
    def _validate_engine_compatibility(self, args: Dict[str, str]) -> Dict[str, Any]:
        """Validate Glue Connection parameters against engine types.
        
        Args:
            args: Parsed job arguments
            
        Returns:
            Dict containing compatibility validation results
            
        Raises:
            GlueConnectionEngineCompatibilityError: For serious compatibility issues
        """
        compatibility_results = {
            'warnings': [],
            'iceberg_engines': []
        }
        
        for connection_type in ['source', 'target']:
            engine_type = args.get(f'{connection_type.upper()}_ENGINE_TYPE', '').lower()
            
            if DatabaseEngineManager.is_iceberg_engine(engine_type):
                compatibility_results['iceberg_engines'].append(connection_type)
                
                # Check if Glue Connection parameters are provided for Iceberg
                glue_params_provided = []
                for param_type in ['create', 'use']:
                    param_name = self.GLUE_CONNECTION_PARAMS[connection_type][param_type]
                    param_value = args.get(param_name, '').strip()
                    
                    if param_value:
                        if param_type == 'create':
                            param_value = self._parse_boolean_parameter(param_value)
                        
                        if param_value:
                            glue_params_provided.append(param_name)
                
                if glue_params_provided:
                    warning_msg = (
                        f"Glue Connection parameters {glue_params_provided} provided for "
                        f"{connection_type} Iceberg engine '{engine_type}' will be ignored. "
                        "Iceberg connections use existing connection mechanism."
                    )
                    compatibility_results['warnings'].append(warning_msg)
                    
                    self.structured_logger.warning(
                        "Glue Connection parameters ignored for Iceberg engine",
                        connection_type=connection_type,
                        engine_type=engine_type,
                        ignored_parameters=glue_params_provided
                    )
        
        return compatibility_results
    
    def _validate_connection_configuration(self, args: Dict[str, str], 
                                         connection_type: str) -> Optional[Dict[str, Any]]:
        """Validate configuration for a specific connection (source or target).
        
        Args:
            args: Parsed job arguments
            connection_type: 'source' or 'target'
            
        Returns:
            Dict containing connection configuration or None if no Glue Connection config
            
        Raises:
            GlueConnectionParameterError: For configuration validation failures
        """
        engine_type = args.get(f'{connection_type.upper()}_ENGINE_TYPE', '').lower()
        
        # Skip validation for Iceberg engines
        if DatabaseEngineManager.is_iceberg_engine(engine_type):
            return None
        
        create_param = self.GLUE_CONNECTION_PARAMS[connection_type]['create']
        use_param = self.GLUE_CONNECTION_PARAMS[connection_type]['use']
        
        create_value = self._parse_boolean_parameter(args.get(create_param, ''))
        use_value = args.get(use_param, '').strip()
        
        # If no Glue Connection parameters, return None (will use direct JDBC)
        if not create_value and not use_value:
            return None
        
        config = {
            'connection_type': connection_type,
            'engine_type': engine_type,
            'strategy': None,
            'connection_name': None,
            'warnings': []
        }
        
        if create_value:
            config['strategy'] = 'create_glue'
            config['connection_name'] = self._generate_connection_name(args, connection_type)
            
            # Validate required parameters for connection creation
            self._validate_creation_parameters(args, connection_type)
            
        elif use_value:
            config['strategy'] = 'use_glue'
            config['connection_name'] = use_value
            
            # Validate connection name format
            self._validate_connection_name_format(use_value, use_param)
            
            # Warn about unused JDBC parameters
            unused_params = self._check_unused_jdbc_parameters(args, connection_type)
            if unused_params:
                warning_msg = (
                    f"JDBC parameters {unused_params} provided for {connection_type} "
                    f"will be ignored when using existing Glue Connection '{use_value}'"
                )
                config['warnings'].append(warning_msg)
        
        return config
    
    def _validate_creation_parameters(self, args: Dict[str, str], connection_type: str) -> None:
        """Validate parameters required for Glue Connection creation.
        
        Args:
            args: Parsed job arguments
            connection_type: 'source' or 'target'
            
        Raises:
            GlueConnectionParameterError: If required parameters are missing or invalid
        """
        required_params = self.REQUIRED_JDBC_PARAMS[connection_type]
        missing_params = []
        invalid_params = []
        
        for param_name in required_params:
            param_value = args.get(param_name, '').strip()
            
            if not param_value:
                missing_params.append(param_name)
            else:
                # Validate specific parameter formats
                if 'CONNECTION_STRING' in param_name:
                    engine_type = args.get(f'{connection_type.upper()}_ENGINE_TYPE', '').lower()
                    if not self._validate_connection_string_format(param_value, engine_type):
                        invalid_params.append(f"{param_name} (invalid format for {engine_type})")
        
        if missing_params:
            raise GlueConnectionParameterError(
                f"Missing required parameters for Glue Connection creation: {missing_params}",
                parameter_name=missing_params[0] if missing_params else None
            )
        
        if invalid_params:
            raise GlueConnectionParameterError(
                f"Invalid parameter formats: {invalid_params}",
                parameter_name=invalid_params[0].split(' ')[0] if invalid_params else None
            )
    
    def _validate_connection_name_format(self, connection_name: str, parameter_name: str) -> None:
        """Validate Glue Connection name format.
        
        Args:
            connection_name: Name to validate
            parameter_name: Parameter name for error context
            
        Raises:
            GlueConnectionParameterError: If connection name format is invalid
        """
        if not connection_name:
            raise GlueConnectionParameterError(
                f"Connection name cannot be empty for parameter '{parameter_name}'. "
                f"Please provide a valid Glue Connection name.",
                parameter_name=parameter_name,
                parameter_value=connection_name
            )
        
        if len(connection_name) > self.MAX_CONNECTION_NAME_LENGTH:
            raise GlueConnectionParameterError(
                f"Connection name '{connection_name}' exceeds maximum length of {self.MAX_CONNECTION_NAME_LENGTH} characters "
                f"(current length: {len(connection_name)}). Please use a shorter connection name.",
                parameter_name=parameter_name,
                parameter_value=connection_name
            )
        
        if not self.CONNECTION_NAME_PATTERN.match(connection_name):
            # Find specific invalid characters for better error message
            invalid_chars = []
            for char in connection_name:
                if not char.isalnum() and char not in ['-', '_']:
                    if char not in invalid_chars:
                        invalid_chars.append(char)
            
            error_message = (
                f"Connection name '{connection_name}' contains invalid characters. "
                f"Connection names must contain only alphanumeric characters, hyphens (-), and underscores (_)."
            )
            
            if invalid_chars:
                error_message += f" Invalid characters found: {', '.join(repr(c) for c in invalid_chars)}"
            
            error_message += f" Example valid names: 'my-connection', 'db_connection_1', 'prod-oracle-db'"
            
            raise GlueConnectionParameterError(
                error_message,
                parameter_name=parameter_name,
                parameter_value=connection_name
            )
    
    def _validate_connection_string_format(self, connection_string: str, engine_type: str) -> bool:
        """Validate connection string format for the engine type.
        
        Args:
            connection_string: JDBC connection string
            engine_type: Database engine type
            
        Returns:
            True if format is valid, False otherwise
        """
        try:
            return DatabaseEngineManager.validate_connection_string(engine_type, connection_string)
        except Exception:
            return False
    
    def _validate_cross_connection_consistency(self, source_config: Optional[Dict[str, Any]],
                                             target_config: Optional[Dict[str, Any]]) -> None:
        """Validate consistency between source and target connection configurations.
        
        Args:
            source_config: Source connection configuration
            target_config: Target connection configuration
            
        Raises:
            GlueConnectionParameterError: For consistency validation failures
        """
        # Check for connection name conflicts if both are creating connections
        if (source_config and source_config.get('strategy') == 'create_glue' and
            target_config and target_config.get('strategy') == 'create_glue'):
            
            source_name = source_config.get('connection_name')
            target_name = target_config.get('connection_name')
            
            if source_name == target_name:
                raise GlueConnectionParameterError(
                    f"Source and target cannot use the same connection name '{source_name}' "
                    "when both are creating new connections",
                    parameter_name="createSourceConnection/createTargetConnection"
                )
    
    def _generate_connection_name(self, args: Dict[str, str], connection_type: str) -> str:
        """Generate a connection name for Glue Connection creation.
        
        Args:
            args: Parsed job arguments
            connection_type: 'source' or 'target'
            
        Returns:
            Generated connection name
        """
        # Use job name as base if available
        job_name = args.get('JOB_NAME', 'glue-data-replication')
        
        # Clean job name for use in connection name
        clean_job_name = re.sub(r'[^a-zA-Z0-9_-]', '-', job_name)
        
        # Generate connection name
        connection_name = f"{clean_job_name}-{connection_type}-connection"
        
        # Ensure it meets length requirements
        if len(connection_name) > self.MAX_CONNECTION_NAME_LENGTH:
            # Truncate job name part if needed
            max_job_name_length = self.MAX_CONNECTION_NAME_LENGTH - len(f"-{connection_type}-connection")
            clean_job_name = clean_job_name[:max_job_name_length]
            connection_name = f"{clean_job_name}-{connection_type}-connection"
        
        return connection_name
    
    def _check_unused_jdbc_parameters(self, args: Dict[str, str], connection_type: str) -> List[str]:
        """Check for JDBC parameters that will be unused when using existing Glue Connection.
        
        Args:
            args: Parsed job arguments
            connection_type: 'source' or 'target'
            
        Returns:
            List of unused parameter names
        """
        jdbc_params = [
            f'{connection_type.upper()}_CONNECTION_STRING',
            f'{connection_type.upper()}_DB_USER',
            f'{connection_type.upper()}_DB_PASSWORD'
        ]
        
        unused_params = []
        for param_name in jdbc_params:
            if args.get(param_name, '').strip():
                unused_params.append(param_name)
        
        return unused_params
    
    def _parse_boolean_parameter(self, value: str) -> bool:
        """Parse boolean parameter value.
        
        Args:
            value: Parameter value to parse
            
        Returns:
            Boolean value
        """
        return value.strip().lower() in ['true', '1', 'yes', 'on']
    
    def _log_validation_summary(self, validation_results: Dict[str, Any]) -> None:
        """Log summary of validation results.
        
        Args:
            validation_results: Results from validation process
        """
        summary = {
            'source_strategy': None,
            'target_strategy': None,
            'warnings_count': len(validation_results['validation_warnings']),
            'errors_count': len(validation_results['validation_errors'])
        }
        
        if validation_results['source_config']:
            summary['source_strategy'] = validation_results['source_config']['strategy']
        
        if validation_results['target_config']:
            summary['target_strategy'] = validation_results['target_config']['strategy']
        
        self.structured_logger.info(
            "Glue Connection parameter validation summary",
            **summary
        )
        
        # Log warnings
        for warning in validation_results['validation_warnings']:
            self.structured_logger.warning(f"Validation warning: {warning}")
        
        # Log errors (if any)
        for error in validation_results['validation_errors']:
            self.structured_logger.error(f"Validation error: {error}")


def validate_glue_connection_parameters_comprehensive(args: Dict[str, str]) -> Dict[str, Any]:
    """Comprehensive validation function for Glue Connection parameters.
    
    This is the main entry point for Glue Connection parameter validation.
    
    Args:
        args: Parsed job arguments
        
    Returns:
        Dict containing validation results and configurations
        
    Raises:
        GlueConnectionParameterError: For parameter validation failures
        GlueConnectionEngineCompatibilityError: For engine compatibility issues
    """
    validator = GlueConnectionParameterValidator()
    return validator.validate_all_glue_connection_parameters(args)