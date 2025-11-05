"""
Connection strategy decision logging for AWS Glue Data Replication.

This module provides specialized logging for connection strategy decisions,
helping users understand how the system determines which connection method
to use for each database.
"""

import logging
from typing import Dict, Any, Optional
from .logging import StructuredLogger


class ConnectionStrategyLogger:
    """Logger for connection strategy decisions and operations."""
    
    def __init__(self, job_name: str = "glue-data-replication"):
        self.job_name = job_name
        self.structured_logger = StructuredLogger(f"ConnectionStrategy-{job_name}")
        self.logger = logging.getLogger(__name__)
    
    def log_strategy_decision(self, connection_type: str, engine_type: str, 
                            strategy: str, config: Dict[str, Any],
                            reasoning: str) -> None:
        """Log connection strategy decision with detailed reasoning.
        
        Args:
            connection_type: 'source' or 'target'
            engine_type: Database engine type (e.g., 'oracle', 'iceberg')
            strategy: Chosen strategy ('create_glue', 'use_glue', 'direct_jdbc', 'iceberg')
            config: Connection configuration details
            reasoning: Human-readable explanation of why this strategy was chosen
        """
        self.structured_logger.info(
            f"Connection strategy determined for {connection_type}",
            connection_type=connection_type,
            engine_type=engine_type,
            strategy=strategy,
            reasoning=reasoning,
            config_summary=self._create_config_summary(config, strategy)
        )
        
        # Also log to standard logger for visibility
        self.logger.info(
            f"[{connection_type.upper()}] Using {strategy} strategy for {engine_type} engine: {reasoning}"
        )
    
    def log_glue_connection_creation_start(self, connection_name: str, 
                                         connection_type: str, engine_type: str,
                                         use_secrets_manager: bool = True) -> None:
        """Log the start of Glue Connection creation process.
        
        Args:
            connection_name: Name of the Glue Connection being created
            connection_type: 'source' or 'target'
            engine_type: Database engine type
            use_secrets_manager: Whether Secrets Manager will be used for credentials
        """
        self.structured_logger.info(
            f"Starting Glue Connection creation for {connection_type}",
            connection_name=connection_name,
            connection_type=connection_type,
            engine_type=engine_type,
            use_secrets_manager=use_secrets_manager,
            operation="glue_connection_creation_start"
        )
        
        secrets_info = "with AWS Secrets Manager integration" if use_secrets_manager else "with direct credentials"
        self.logger.info(
            f"[{connection_type.upper()}] Creating Glue Connection '{connection_name}' "
            f"for {engine_type} engine {secrets_info}"
        )
    
    def log_glue_connection_creation_success(self, connection_name: str,
                                           connection_type: str, secret_arn: Optional[str] = None) -> None:
        """Log successful Glue Connection creation.
        
        Args:
            connection_name: Name of the created Glue Connection
            connection_type: 'source' or 'target'
            secret_arn: ARN of the created secret (if using Secrets Manager)
        """
        self.structured_logger.info(
            f"Glue Connection created successfully for {connection_type}",
            connection_name=connection_name,
            connection_type=connection_type,
            secret_arn=secret_arn,
            operation="glue_connection_creation_success"
        )
        
        secrets_info = f" (credentials stored in Secrets Manager: {secret_arn})" if secret_arn else ""
        self.logger.info(
            f"[{connection_type.upper()}] Successfully created Glue Connection '{connection_name}'{secrets_info}"
        )
    
    def log_existing_glue_connection_usage(self, connection_name: str,
                                         connection_type: str, validation_result: Dict[str, Any]) -> None:
        """Log usage of existing Glue Connection.
        
        Args:
            connection_name: Name of the existing Glue Connection
            connection_type: 'source' or 'target'
            validation_result: Results of connection validation
        """
        self.structured_logger.info(
            f"Using existing Glue Connection for {connection_type}",
            connection_name=connection_name,
            connection_type=connection_type,
            validation_result=validation_result,
            operation="existing_glue_connection_usage"
        )
        
        self.logger.info(
            f"[{connection_type.upper()}] Using existing Glue Connection '{connection_name}' "
            f"(validation: {'passed' if validation_result.get('valid', False) else 'failed'})"
        )
    
    def log_direct_jdbc_usage(self, connection_type: str, engine_type: str,
                            connection_string: str, reason: str) -> None:
        """Log usage of direct JDBC connection.
        
        Args:
            connection_type: 'source' or 'target'
            engine_type: Database engine type
            connection_string: JDBC connection string (sanitized)
            reason: Reason for using direct JDBC
        """
        # Sanitize connection string for logging
        sanitized_connection_string = self._sanitize_connection_string(connection_string)
        
        self.structured_logger.info(
            f"Using direct JDBC connection for {connection_type}",
            connection_type=connection_type,
            engine_type=engine_type,
            connection_string=sanitized_connection_string,
            reason=reason,
            operation="direct_jdbc_usage"
        )
        
        self.logger.info(
            f"[{connection_type.upper()}] Using direct JDBC connection for {engine_type} engine: {reason}"
        )
    
    def log_iceberg_connection_usage(self, connection_type: str, warehouse_location: str,
                                   catalog_config: Dict[str, Any]) -> None:
        """Log usage of Iceberg connection.
        
        Args:
            connection_type: 'source' or 'target'
            warehouse_location: S3 warehouse location
            catalog_config: Iceberg catalog configuration
        """
        self.structured_logger.info(
            f"Using Iceberg connection for {connection_type}",
            connection_type=connection_type,
            warehouse_location=warehouse_location,
            catalog_config=catalog_config,
            operation="iceberg_connection_usage"
        )
        
        self.logger.info(
            f"[{connection_type.upper()}] Using Iceberg connection with warehouse: {warehouse_location}"
        )
    
    def log_parameter_compatibility_warning(self, engine_type: str, 
                                          ignored_parameters: list,
                                          connection_type: str) -> None:
        """Log warning about ignored Glue Connection parameters for incompatible engines.
        
        Args:
            engine_type: Engine type that doesn't support Glue Connections
            ignored_parameters: List of parameters that will be ignored
            connection_type: 'source' or 'target'
        """
        self.structured_logger.warning(
            f"Glue Connection parameters ignored for {engine_type} engine",
            engine_type=engine_type,
            connection_type=connection_type,
            ignored_parameters=ignored_parameters,
            operation="parameter_compatibility_warning"
        )
        
        self.logger.warning(
            f"[{connection_type.upper()}] Glue Connection parameters {ignored_parameters} "
            f"are not supported for {engine_type} engine and will be ignored. "
            f"Iceberg connections use their own connection mechanism."
        )
    
    def log_connection_strategy_summary(self, source_strategy: str, target_strategy: str,
                                      source_engine: str, target_engine: str) -> None:
        """Log overall connection strategy summary for the job.
        
        Args:
            source_strategy: Strategy chosen for source connection
            target_strategy: Strategy chosen for target connection
            source_engine: Source engine type
            target_engine: Target engine type
        """
        self.structured_logger.info(
            "Connection strategy summary for job",
            source_strategy=source_strategy,
            target_strategy=target_strategy,
            source_engine=source_engine,
            target_engine=target_engine,
            operation="connection_strategy_summary"
        )
        
        self.logger.info(
            f"Connection Strategy Summary: "
            f"SOURCE ({source_engine}) -> {source_strategy}, "
            f"TARGET ({target_engine}) -> {target_strategy}"
        )
    
    def log_secrets_manager_operation(self, operation: str, connection_name: str,
                                    success: bool, details: Dict[str, Any] = None) -> None:
        """Log Secrets Manager operations.
        
        Args:
            operation: Type of operation ('create_secret', 'validate_permissions', etc.)
            connection_name: Name of the Glue Connection
            success: Whether the operation was successful
            details: Additional operation details
        """
        log_level = "info" if success else "error"
        status = "successful" if success else "failed"
        
        log_data = {
            'operation': f"secrets_manager_{operation}",
            'connection_name': connection_name,
            'success': success,
            'details': details or {}
        }
        
        if success:
            self.structured_logger.info(f"Secrets Manager {operation} {status}", **log_data)
        else:
            self.structured_logger.error(f"Secrets Manager {operation} {status}", **log_data)
        
        self.logger.log(
            logging.INFO if success else logging.ERROR,
            f"Secrets Manager {operation} {status} for connection '{connection_name}'"
        )
    
    def log_retry_attempt(self, operation: str, attempt: int, max_attempts: int,
                         error: str, retry_delay: float) -> None:
        """Log retry attempts for connection operations.
        
        Args:
            operation: Operation being retried
            attempt: Current attempt number
            max_attempts: Maximum number of attempts
            error: Error that caused the retry
            retry_delay: Delay before next retry
        """
        self.structured_logger.warning(
            f"Retrying {operation} after failure",
            operation=operation,
            attempt=attempt,
            max_attempts=max_attempts,
            error=error,
            retry_delay=retry_delay,
            operation_type="retry_attempt"
        )
        
        self.logger.warning(
            f"Retrying {operation} (attempt {attempt}/{max_attempts}) after error: {error}. "
            f"Next retry in {retry_delay:.1f} seconds."
        )
    
    def _create_config_summary(self, config: Dict[str, Any], strategy: str) -> Dict[str, Any]:
        """Create a summary of connection configuration for logging.
        
        Args:
            config: Full connection configuration
            strategy: Connection strategy
            
        Returns:
            Sanitized configuration summary
        """
        summary = {
            'strategy': strategy,
            'engine_type': config.get('engine_type'),
            'database': config.get('database'),
            'schema': config.get('schema')
        }
        
        if strategy in ['create_glue', 'use_glue']:
            glue_config = config.get('glue_connection_config', {})
            summary.update({
                'create_connection': glue_config.get('create_connection', False),
                'use_existing_connection': glue_config.get('use_existing_connection'),
                'connection_strategy': glue_config.get('connection_strategy')
            })
        
        if strategy == 'direct_jdbc':
            # Include sanitized connection string
            connection_string = config.get('connection_string', '')
            summary['connection_string'] = self._sanitize_connection_string(connection_string)
        
        if strategy == 'iceberg':
            iceberg_config = config.get('iceberg_config', {})
            summary.update({
                'warehouse_location': iceberg_config.get('warehouse_location'),
                'catalog_name': iceberg_config.get('catalog_name'),
                'database_name': iceberg_config.get('database_name')
            })
        
        return summary
    
    def _sanitize_connection_string(self, connection_string: str) -> str:
        """Remove sensitive information from connection string for logging.
        
        Args:
            connection_string: Original connection string
            
        Returns:
            Sanitized connection string
        """
        import re
        
        if not connection_string:
            return ""
        
        # Remove password from connection string
        sanitized = re.sub(r'password=[^;]+', 'password=***', connection_string, flags=re.IGNORECASE)
        sanitized = re.sub(r'pwd=[^;]+', 'pwd=***', sanitized, flags=re.IGNORECASE)
        
        # Remove user credentials from URLs
        sanitized = re.sub(r'://[^:]+:[^@]+@', '://***:***@', sanitized)
        
        return sanitized