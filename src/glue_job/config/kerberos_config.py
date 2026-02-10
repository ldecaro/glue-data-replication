"""
Kerberos configuration dataclasses for AWS Glue Data Replication

This module contains the Kerberos authentication configuration dataclass
and related exception classes for secure database authentication.
"""

import re
import logging
from dataclasses import dataclass
from typing import Dict, Optional


# Kerberos-specific exception classes
class KerberosAuthenticationError(Exception):
    """Base exception for Kerberos authentication errors."""
    pass


class KerberosConfigurationError(KerberosAuthenticationError):
    """Exception for Kerberos configuration validation errors."""
    pass


class KerberosConnectionError(KerberosAuthenticationError):
    """Exception for Kerberos connection establishment errors."""
    pass


class KerberosEngineCompatibilityError(KerberosAuthenticationError):
    """Exception for unsupported engine types with Kerberos."""
    pass


@dataclass
class KerberosConfig:
    """Configuration for Kerberos authentication.
    
    This class encapsulates the three required parameters for Kerberos authentication:
    - SPN (Service Principal Name): Unique identifier for the service instance
    - Domain: Kerberos realm or administrative domain
    - KDC: Key Distribution Center for authentication services
    """
    spn: str
    domain: str
    kdc: str
    
    def __post_init__(self):
        """Validate Kerberos configuration after initialization."""
        self.validate()
    
    def is_complete(self) -> bool:
        """Check if all required Kerberos parameters are provided.
        
        Returns:
            bool: True if all three parameters (spn, domain, kdc) are non-empty, False otherwise
        """
        return bool(self.spn and self.spn.strip() and 
                   self.domain and self.domain.strip() and 
                   self.kdc and self.kdc.strip())
    
    def validate(self) -> None:
        """Validate Kerberos configuration parameters.
        
        Raises:
            KerberosConfigurationError: If any parameter is invalid or missing
        """
        logger = logging.getLogger(__name__)
        
        # Check if all parameters are provided
        if not self.is_complete():
            missing_params = []
            if not (self.spn and self.spn.strip()):
                missing_params.append("SPN")
            if not (self.domain and self.domain.strip()):
                missing_params.append("Domain")
            if not (self.kdc and self.kdc.strip()):
                missing_params.append("KDC")
            
            raise KerberosConfigurationError(
                f"Incomplete Kerberos configuration. Missing or empty parameters: {', '.join(missing_params)}. "
                "All three parameters (SPN, Domain, KDC) are required for Kerberos authentication."
            )
        
        # Validate SPN format (service/hostname@REALM or service/hostname or service/hostname:port)
        spn_pattern = r'^[a-zA-Z0-9_-]+\/[a-zA-Z0-9._:-]+(@[A-Z0-9._-]+)?$'
        if not re.match(spn_pattern, self.spn.strip()):
            raise KerberosConfigurationError(
                f"Invalid SPN format: '{self.spn}'. "
                "SPN must follow the format 'service/hostname', 'service/hostname:port', or 'service/hostname@REALM'"
            )
        
        # Validate domain format (Kerberos realm name)
        domain_pattern = r'^[A-Z0-9._-]+$'
        if not re.match(domain_pattern, self.domain.strip().upper()):
            raise KerberosConfigurationError(
                f"Invalid Kerberos domain format: '{self.domain}'. "
                "Domain must contain only alphanumeric characters, dots, hyphens, and underscores"
            )
        
        # Validate KDC format (hostname or IP address with optional port)
        kdc_pattern = r'^[a-zA-Z0-9._-]+(:[0-9]+)?$'
        if not re.match(kdc_pattern, self.kdc.strip()):
            raise KerberosConfigurationError(
                f"Invalid KDC format: '{self.kdc}'. "
                "KDC must be a valid hostname or IP address with optional port (e.g., 'kdc.example.com' or 'kdc.example.com:88')"
            )
        
        logger.debug(f"Kerberos configuration validated successfully: SPN={self.spn}, Domain={self.domain}, KDC={self.kdc}")
    
    def to_glue_properties(self) -> Dict[str, str]:
        """Convert Kerberos configuration to Glue Connection properties format.
        
        Returns:
            Dict[str, str]: Dictionary containing Kerberos properties for Glue Connection
        """
        if not self.is_complete():
            raise KerberosConfigurationError(
                "Cannot convert incomplete Kerberos configuration to Glue properties. "
                "All three parameters (SPN, Domain, KDC) must be provided."
            )
        
        return {
            'KERBEROS_SPN': self.spn.strip(),
            'KERBEROS_DOMAIN': self.domain.strip(),
            'KERBEROS_KDC': self.kdc.strip()
        }
    
    def __str__(self) -> str:
        """String representation of KerberosConfig (without sensitive details)."""
        return f"KerberosConfig(domain={self.domain}, kdc={self.kdc}, spn_configured={bool(self.spn)})"
    
    def __repr__(self) -> str:
        """Detailed representation of KerberosConfig."""
        return f"KerberosConfig(spn='{self.spn}', domain='{self.domain}', kdc='{self.kdc}')"