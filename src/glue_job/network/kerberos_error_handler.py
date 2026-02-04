"""
Kerberos-specific error handling and troubleshooting for AWS Glue Data Replication.

This module provides specialized error handling for Kerberos authentication issues,
including detailed diagnostics, troubleshooting guidance, and audit logging.
"""

import logging
from typing import Dict, List, Optional, Any
from datetime import datetime, timezone

from .error_handler import ErrorCategory, NetworkErrorHandler
from ..config.kerberos_config import (
    KerberosAuthenticationError, 
    KerberosConfigurationError, 
    KerberosConnectionError, 
    KerberosEngineCompatibilityError
)
from ..monitoring.logging import StructuredLogger
from ..monitoring.metrics import CloudWatchMetricsPublisher

logger = logging.getLogger(__name__)


class KerberosErrorCategory:
    """Defines Kerberos-specific error categories."""
    CONFIG_VALIDATION = "kerberos_config_validation"
    AUTHENTICATION_FAILURE = "kerberos_authentication_failure"
    CONNECTION_CREATION = "kerberos_connection_creation"
    ENGINE_COMPATIBILITY = "kerberos_engine_compatibility"
    TICKET_REFRESH = "kerberos_ticket_refresh"
    SECURITY_CONTEXT = "kerberos_security_context"
    MIXED_AUTH_CONFIG = "kerberos_mixed_auth_config"


class KerberosTroubleshootingGuide:
    """Provides troubleshooting guidance for common Kerberos issues."""
    
    TROUBLESHOOTING_GUIDES = {
        KerberosErrorCategory.CONFIG_VALIDATION: {
            "description": "Kerberos configuration validation failed",
            "common_causes": [
                "Missing or empty Kerberos parameters (SPN, Domain, KDC)",
                "Invalid SPN format (should be service/hostname or service/hostname@REALM)",
                "Invalid domain/realm format",
                "Invalid KDC hostname or IP address format"
            ],
            "troubleshooting_steps": [
                "Verify all three Kerberos parameters are provided: SPN, Domain, KDC",
                "Check SPN format follows 'service/hostname' or 'service/hostname@REALM' pattern",
                "Ensure domain/realm uses uppercase letters and valid characters",
                "Verify KDC hostname or IP address is accessible from Glue environment",
                "Test Kerberos configuration with kinit command if possible"
            ],
            "documentation_links": [
                "docs/KERBEROS_AUTHENTICATION_GUIDE.md#configuration-validation",
                "docs/ERROR_HANDLING_GUIDE.md#kerberos-configuration-errors"
            ]
        },
        
        KerberosErrorCategory.AUTHENTICATION_FAILURE: {
            "description": "Kerberos authentication to database failed",
            "common_causes": [
                "Invalid or expired Kerberos credentials",
                "KDC server unreachable from Glue environment",
                "Clock skew between Glue and KDC server",
                "Incorrect service principal name (SPN)",
                "Database server not configured for Kerberos authentication"
            ],
            "troubleshooting_steps": [
                "Verify KDC server is accessible from Glue VPC/subnet",
                "Check network connectivity to KDC on port 88 (default Kerberos port)",
                "Ensure clock synchronization between Glue environment and KDC",
                "Verify SPN matches the database server's Kerberos configuration",
                "Check database server Kerberos configuration and service account",
                "Review database server logs for Kerberos authentication errors"
            ],
            "documentation_links": [
                "docs/KERBEROS_AUTHENTICATION_GUIDE.md#authentication-troubleshooting",
                "docs/NETWORK_CONFIGURATION_GUIDE.md#kerberos-network-requirements"
            ]
        },
        
        KerberosErrorCategory.CONNECTION_CREATION: {
            "description": "Failed to create Glue Connection with Kerberos properties",
            "common_causes": [
                "Insufficient IAM permissions for Glue Connection creation",
                "Invalid Kerberos connection properties",
                "Network configuration issues (VPC, subnets, security groups)",
                "Glue service limits exceeded"
            ],
            "troubleshooting_steps": [
                "Verify IAM role has glue:CreateConnection permission",
                "Check Glue Connection properties for Kerberos configuration",
                "Ensure VPC and subnet configuration allows Glue access",
                "Verify security group rules allow outbound access to KDC and database",
                "Check Glue service limits and quotas",
                "Review CloudTrail logs for Glue API call failures"
            ],
            "documentation_links": [
                "docs/KERBEROS_AUTHENTICATION_GUIDE.md#glue-connection-setup",
                "infrastructure/iam/README.md#kerberos-permissions"
            ]
        },
        
        KerberosErrorCategory.ENGINE_COMPATIBILITY: {
            "description": "Database engine does not support Kerberos authentication",
            "common_causes": [
                "Using Kerberos with unsupported database engine",
                "Iceberg tables configured with Kerberos parameters",
                "Engine-specific Kerberos configuration missing"
            ],
            "troubleshooting_steps": [
                "Verify database engine supports Kerberos (Oracle, SQL Server, PostgreSQL, DB2)",
                "Remove Kerberos parameters for Iceberg connections",
                "Check engine-specific Kerberos configuration requirements",
                "Use standard authentication for unsupported engines"
            ],
            "documentation_links": [
                "docs/KERBEROS_AUTHENTICATION_GUIDE.md#supported-engines",
                "docs/DATABASE_CONFIGURATION_GUIDE.md#engine-compatibility"
            ]
        },
        
        KerberosErrorCategory.MIXED_AUTH_CONFIG: {
            "description": "Issues with mixed authentication mode configuration",
            "common_causes": [
                "Inconsistent authentication configuration between source and target",
                "Partial Kerberos configuration (missing parameters)",
                "Authentication method mismatch in connection properties"
            ],
            "troubleshooting_steps": [
                "Verify authentication configuration for both source and target",
                "Ensure complete Kerberos configuration (all three parameters) or none",
                "Check connection properties match intended authentication method",
                "Review parameter file for authentication consistency"
            ],
            "documentation_links": [
                "docs/KERBEROS_AUTHENTICATION_GUIDE.md#mixed-authentication",
                "examples/README.md#kerberos-examples"
            ]
        }
    }
    
    @classmethod
    def get_troubleshooting_guide(cls, error_category: str) -> Dict[str, Any]:
        """Get troubleshooting guide for specific error category."""
        return cls.TROUBLESHOOTING_GUIDES.get(error_category, {
            "description": "Unknown Kerberos error",
            "common_causes": ["Unclassified Kerberos authentication issue"],
            "troubleshooting_steps": [
                "Review Kerberos configuration parameters",
                "Check network connectivity to KDC and database",
                "Verify database engine Kerberos support",
                "Consult Kerberos authentication guide"
            ],
            "documentation_links": ["docs/KERBEROS_AUTHENTICATION_GUIDE.md"]
        })


class KerberosErrorHandler:
    """Specialized error handler for Kerberos authentication issues."""
    
    def __init__(self, job_name: str):
        self.job_name = job_name
        self.structured_logger = StructuredLogger(job_name)
        self.metrics_publisher = CloudWatchMetricsPublisher(job_name)
        self.network_error_handler = NetworkErrorHandler()
        self.troubleshooting_guide = KerberosTroubleshootingGuide()
        
        # Track Kerberos-specific error history for audit logging
        self.kerberos_error_history = []
        self.authentication_attempts = []
    
    def handle_kerberos_configuration_error(self, error: KerberosConfigurationError, 
                                          connection_type: str, 
                                          kerberos_params: Dict[str, str]) -> Dict[str, Any]:
        """Handle Kerberos configuration validation errors with detailed guidance."""
        error_info = {
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'error_type': 'KerberosConfigurationError',
            'error_category': KerberosErrorCategory.CONFIG_VALIDATION,
            'error_message': str(error),
            'connection_type': connection_type,
            'configuration_status': self._analyze_kerberos_config_completeness(kerberos_params),
            'troubleshooting_guide': self.troubleshooting_guide.get_troubleshooting_guide(
                KerberosErrorCategory.CONFIG_VALIDATION
            )
        }
        
        # Log detailed error with troubleshooting guidance
        self.structured_logger.log_kerberos_config_validation_failure(
            connection_type=connection_type,
            error=str(error),
            validation_duration_ms=0.0,  # Immediate validation failure
            fallback_action="standard_authentication"
        )
        
        # Log troubleshooting guidance
        self._log_troubleshooting_guidance(error_info)
        
        # Publish metrics
        self.metrics_publisher.publish_kerberos_configuration_validation_metrics(
            connection_type=connection_type,
            success=False,
            duration_ms=0.0,
            validation_errors=1
        )
        
        # Store in error history for audit
        self.kerberos_error_history.append(error_info)
        
        return error_info
    
    def handle_kerberos_authentication_error(self, error: KerberosAuthenticationError,
                                           connection_type: str, engine_type: str,
                                           domain: str, attempt_number: int = 1,
                                           auth_duration_ms: float = 0.0) -> Dict[str, Any]:
        """Handle Kerberos authentication failures with detailed diagnostics."""
        error_info = {
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'error_type': 'KerberosAuthenticationError',
            'error_category': KerberosErrorCategory.AUTHENTICATION_FAILURE,
            'error_message': str(error),
            'connection_type': connection_type,
            'engine_type': engine_type,
            'kerberos_domain': domain,
            'attempt_number': attempt_number,
            'auth_duration_ms': auth_duration_ms,
            'troubleshooting_guide': self.troubleshooting_guide.get_troubleshooting_guide(
                KerberosErrorCategory.AUTHENTICATION_FAILURE
            )
        }
        
        # Log authentication failure with context
        self.structured_logger.log_kerberos_authentication_failure(
            connection_type=connection_type,
            engine_type=engine_type,
            domain=domain,
            error=str(error),
            auth_duration_ms=auth_duration_ms,
            attempt_number=attempt_number,
            retry_action="retry_with_backoff" if attempt_number < 3 else "fallback_to_standard_auth"
        )
        
        # Log troubleshooting guidance
        self._log_troubleshooting_guidance(error_info)
        
        # Publish metrics
        self.metrics_publisher.publish_kerberos_authentication_metrics(
            connection_type=connection_type,
            engine_type=engine_type,
            success=False,
            duration_ms=auth_duration_ms,
            attempt_number=attempt_number,
            error_category=KerberosErrorCategory.AUTHENTICATION_FAILURE
        )
        
        # Store authentication attempt for audit
        self.authentication_attempts.append({
            'timestamp': error_info['timestamp'],
            'connection_type': connection_type,
            'engine_type': engine_type,
            'domain': domain,
            'success': False,
            'attempt_number': attempt_number,
            'error_message': str(error)
        })
        
        # Store in error history
        self.kerberos_error_history.append(error_info)
        
        return error_info
    
    def handle_kerberos_connection_error(self, error: KerberosConnectionError,
                                       connection_type: str, engine_type: str,
                                       connection_name: str,
                                       creation_duration_ms: float = 0.0) -> Dict[str, Any]:
        """Handle Kerberos connection creation errors."""
        error_info = {
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'error_type': 'KerberosConnectionError',
            'error_category': KerberosErrorCategory.CONNECTION_CREATION,
            'error_message': str(error),
            'connection_type': connection_type,
            'engine_type': engine_type,
            'connection_name': connection_name,
            'creation_duration_ms': creation_duration_ms,
            'troubleshooting_guide': self.troubleshooting_guide.get_troubleshooting_guide(
                KerberosErrorCategory.CONNECTION_CREATION
            )
        }
        
        # Log connection creation failure
        self.structured_logger.log_kerberos_connection_creation_failure(
            connection_type=connection_type,
            engine_type=engine_type,
            connection_name=connection_name,
            error=str(error),
            creation_duration_ms=creation_duration_ms,
            fallback_action="standard_authentication"
        )
        
        # Log troubleshooting guidance
        self._log_troubleshooting_guidance(error_info)
        
        # Publish metrics
        self.metrics_publisher.publish_kerberos_connection_creation_metrics(
            connection_type=connection_type,
            engine_type=engine_type,
            success=False,
            duration_ms=creation_duration_ms,
            error_category=KerberosErrorCategory.CONNECTION_CREATION
        )
        
        # Store in error history
        self.kerberos_error_history.append(error_info)
        
        return error_info
    
    def handle_kerberos_engine_compatibility_error(self, error: KerberosEngineCompatibilityError,
                                                 engine_type: str,
                                                 supported_engines: List[str]) -> Dict[str, Any]:
        """Handle Kerberos engine compatibility errors."""
        error_info = {
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'error_type': 'KerberosEngineCompatibilityError',
            'error_category': KerberosErrorCategory.ENGINE_COMPATIBILITY,
            'error_message': str(error),
            'engine_type': engine_type,
            'supported_engines': supported_engines,
            'troubleshooting_guide': self.troubleshooting_guide.get_troubleshooting_guide(
                KerberosErrorCategory.ENGINE_COMPATIBILITY
            )
        }
        
        # Log engine compatibility issue
        self.structured_logger.log_kerberos_engine_compatibility_check(
            engine_type=engine_type,
            is_supported=False,
            supported_engines=supported_engines
        )
        
        # Log troubleshooting guidance
        self._log_troubleshooting_guidance(error_info)
        
        # Publish metrics
        self.metrics_publisher.publish_kerberos_engine_compatibility_metrics(
            engine_type=engine_type,
            is_supported=False
        )
        
        # Store in error history
        self.kerberos_error_history.append(error_info)
        
        return error_info
    
    def handle_mixed_authentication_configuration_issue(self, source_auth: str, target_auth: str,
                                                      source_engine: str, target_engine: str,
                                                      issue_description: str) -> Dict[str, Any]:
        """Handle mixed authentication configuration issues."""
        error_info = {
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'error_type': 'MixedAuthenticationConfigurationIssue',
            'error_category': KerberosErrorCategory.MIXED_AUTH_CONFIG,
            'error_message': issue_description,
            'source_authentication': source_auth,
            'target_authentication': target_auth,
            'source_engine': source_engine,
            'target_engine': target_engine,
            'troubleshooting_guide': self.troubleshooting_guide.get_troubleshooting_guide(
                KerberosErrorCategory.MIXED_AUTH_CONFIG
            )
        }
        
        # Log mixed authentication issue
        self.structured_logger.log_mixed_authentication_mode_detected(
            source_auth=source_auth,
            target_auth=target_auth
        )
        
        # Log troubleshooting guidance
        self._log_troubleshooting_guidance(error_info)
        
        # Publish metrics
        self.metrics_publisher.publish_mixed_authentication_metrics(
            source_auth_method=source_auth,
            target_auth_method=target_auth,
            source_engine=source_engine,
            target_engine=target_engine
        )
        
        # Store in error history
        self.kerberos_error_history.append(error_info)
        
        return error_info
    
    def log_successful_kerberos_authentication(self, connection_type: str, engine_type: str,
                                             domain: str, auth_duration_ms: float,
                                             attempt_number: int = 1) -> None:
        """Log successful Kerberos authentication for audit purposes."""
        # Log success
        self.structured_logger.log_kerberos_authentication_success(
            connection_type=connection_type,
            engine_type=engine_type,
            domain=domain,
            auth_duration_ms=auth_duration_ms,
            attempt_number=attempt_number
        )
        
        # Publish success metrics
        self.metrics_publisher.publish_kerberos_authentication_metrics(
            connection_type=connection_type,
            engine_type=engine_type,
            success=True,
            duration_ms=auth_duration_ms,
            attempt_number=attempt_number
        )
        
        # Store successful authentication attempt for audit
        self.authentication_attempts.append({
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'connection_type': connection_type,
            'engine_type': engine_type,
            'domain': domain,
            'success': True,
            'attempt_number': attempt_number,
            'auth_duration_ms': auth_duration_ms
        })
    
    def _analyze_kerberos_config_completeness(self, kerberos_params: Dict[str, str]) -> Dict[str, Any]:
        """Analyze Kerberos configuration completeness."""
        spn = kerberos_params.get('spn', '').strip()
        domain = kerberos_params.get('domain', '').strip()
        kdc = kerberos_params.get('kdc', '').strip()
        
        return {
            'has_spn': bool(spn),
            'has_domain': bool(domain),
            'has_kdc': bool(kdc),
            'is_complete': bool(spn and domain and kdc),
            'missing_parameters': [
                param for param, value in [('spn', spn), ('domain', domain), ('kdc', kdc)]
                if not value
            ]
        }
    
    def _log_troubleshooting_guidance(self, error_info: Dict[str, Any]) -> None:
        """Log detailed troubleshooting guidance for Kerberos errors."""
        guide = error_info.get('troubleshooting_guide', {})
        
        if not guide:
            return
        
        self.structured_logger.error(
            f"Kerberos Error Troubleshooting Guide: {guide.get('description', 'Unknown error')}"
        )
        
        # Log common causes
        common_causes = guide.get('common_causes', [])
        if common_causes:
            self.structured_logger.info("Common causes:")
            for i, cause in enumerate(common_causes, 1):
                self.structured_logger.info(f"  {i}. {cause}")
        
        # Log troubleshooting steps
        steps = guide.get('troubleshooting_steps', [])
        if steps:
            self.structured_logger.info("Troubleshooting steps:")
            for i, step in enumerate(steps, 1):
                self.structured_logger.info(f"  {i}. {step}")
        
        # Log documentation links
        docs = guide.get('documentation_links', [])
        if docs:
            self.structured_logger.info("Documentation references:")
            for doc in docs:
                self.structured_logger.info(f"  - {doc}")
    
    def get_kerberos_error_summary(self) -> Dict[str, Any]:
        """Get summary of all Kerberos-related errors for audit reporting."""
        if not self.kerberos_error_history:
            return {
                'total_kerberos_errors': 0,
                'error_categories': {},
                'authentication_attempts': len(self.authentication_attempts),
                'successful_authentications': sum(1 for attempt in self.authentication_attempts if attempt['success']),
                'failed_authentications': sum(1 for attempt in self.authentication_attempts if not attempt['success'])
            }
        
        # Count errors by category
        error_categories = {}
        for error in self.kerberos_error_history:
            category = error['error_category']
            error_categories[category] = error_categories.get(category, 0) + 1
        
        return {
            'total_kerberos_errors': len(self.kerberos_error_history),
            'error_categories': error_categories,
            'authentication_attempts': len(self.authentication_attempts),
            'successful_authentications': sum(1 for attempt in self.authentication_attempts if attempt['success']),
            'failed_authentications': sum(1 for attempt in self.authentication_attempts if not attempt['success']),
            'latest_errors': self.kerberos_error_history[-5:] if len(self.kerberos_error_history) > 5 else self.kerberos_error_history,
            'authentication_success_rate': (
                sum(1 for attempt in self.authentication_attempts if attempt['success']) / 
                len(self.authentication_attempts) * 100
            ) if self.authentication_attempts else 0.0
        }
    
    def log_kerberos_audit_summary(self) -> None:
        """Log comprehensive Kerberos audit summary at job completion."""
        summary = self.get_kerberos_error_summary()
        
        self.structured_logger.info(
            "Kerberos Authentication Audit Summary",
            total_kerberos_errors=summary['total_kerberos_errors'],
            authentication_attempts=summary['authentication_attempts'],
            successful_authentications=summary['successful_authentications'],
            failed_authentications=summary['failed_authentications'],
            success_rate=round(summary['authentication_success_rate'], 2)
        )
        
        # Log error breakdown by category
        if summary['error_categories']:
            self.structured_logger.info("Kerberos error breakdown by category:")
            for category, count in summary['error_categories'].items():
                self.structured_logger.info(f"  {category}: {count} errors")
        
        # Log authentication attempts by connection type and engine
        if self.authentication_attempts:
            auth_by_type = {}
            for attempt in self.authentication_attempts:
                key = f"{attempt['connection_type']}_{attempt['engine_type']}"
                if key not in auth_by_type:
                    auth_by_type[key] = {'total': 0, 'successful': 0}
                auth_by_type[key]['total'] += 1
                if attempt['success']:
                    auth_by_type[key]['successful'] += 1
            
            self.structured_logger.info("Authentication attempts by connection type and engine:")
            for key, stats in auth_by_type.items():
                success_rate = (stats['successful'] / stats['total'] * 100) if stats['total'] > 0 else 0
                self.structured_logger.info(
                    f"  {key}: {stats['successful']}/{stats['total']} successful ({success_rate:.1f}%)"
                )