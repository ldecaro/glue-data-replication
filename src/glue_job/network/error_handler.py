"""
Network error handling and diagnostics for AWS Glue Data Replication.

This module provides specialized error handling for network-specific issues,
including Glue connection diagnostics, VPC endpoint validation, and
infrastructure troubleshooting.
"""

import boto3
import logging
from typing import Dict, List, Optional, Any
from ..monitoring.logging import StructuredLogger

logger = logging.getLogger(__name__)


class ErrorCategory:
    """Defines error categories for different types of failures."""
    CONNECTION = "connection"
    AUTHENTICATION = "authentication"
    NETWORK = "network"
    DATA_PROCESSING = "data_processing"
    SCHEMA_MISMATCH = "schema_mismatch"
    PERMISSION = "permission"
    RESOURCE = "resource"
    TIMEOUT = "timeout"
    UNKNOWN = "unknown"


class NetworkConnectivityError(Exception):
    """Exception raised for network connectivity issues."""
    def __init__(self, message: str, error_type: str = 'unknown', connection_name: str = None):
        super().__init__(message)
        self.error_type = error_type
        self.connection_name = connection_name


class GlueConnectionError(Exception):
    """Exception raised for Glue connection specific issues."""
    def __init__(self, message: str, connection_name: str, error_details: Dict[str, Any] = None):
        super().__init__(message)
        self.connection_name = connection_name
        self.error_details = error_details or {}


class VpcEndpointError(Exception):
    """Exception raised for VPC endpoint connectivity issues."""
    def __init__(self, message: str, vpc_id: str = None, endpoint_type: str = None):
        super().__init__(message)
        self.vpc_id = vpc_id
        self.endpoint_type = endpoint_type


class ENICreationError(Exception):
    """Exception raised for Elastic Network Interface creation failures."""
    def __init__(self, message: str, subnet_id: str = None, error_code: str = None):
        super().__init__(message)
        self.subnet_id = subnet_id
        self.error_code = error_code


class NetworkErrorHandler:
    """Specialized error handler for network-specific issues."""
    
    def __init__(self):
        self.structured_logger = StructuredLogger("NetworkErrorHandler")
        try:
            self.ec2_client = boto3.client('ec2')
            self.glue_client = boto3.client('glue')
        except Exception as e:
            self.structured_logger.warning(f"Failed to initialize AWS clients: {e}")
            self.ec2_client = None
            self.glue_client = None
    
    def diagnose_glue_connection_failure(self, connection_name: str, error: Exception) -> Dict[str, Any]:
        """Diagnose Glue connection failures with detailed diagnostics."""
        diagnostics = {
            'connection_name': connection_name,
            'error_type': type(error).__name__,
            'error_message': str(error),
            'diagnostics': [],
            'recommendations': []
        }
        
        if not self.glue_client:
            diagnostics['diagnostics'].append("Unable to perform diagnostics - Glue client not available")
            return diagnostics
        
        try:
            self.structured_logger.info("Starting Glue connection diagnostics", connection_name=connection_name)
            
            # Check if connection exists
            try:
                response = self.glue_client.get_connection(Name=connection_name)
                connection = response.get('Connection', {})
                diagnostics['connection_exists'] = True
                
                # Analyze connection configuration
                physical_reqs = connection.get('PhysicalConnectionRequirements', {})
                subnet_id = physical_reqs.get('SubnetId')
                security_groups = physical_reqs.get('SecurityGroupIdList', [])
                availability_zone = physical_reqs.get('AvailabilityZone')
                
                diagnostics['subnet_id'] = subnet_id
                diagnostics['security_groups'] = security_groups
                diagnostics['availability_zone'] = availability_zone
                
                # Validate subnet accessibility
                if subnet_id:
                    subnet_diagnostics = self._diagnose_subnet_accessibility(subnet_id)
                    diagnostics['subnet_diagnostics'] = subnet_diagnostics
                    diagnostics['diagnostics'].extend(subnet_diagnostics.get('issues', []))
                
                # Validate security group rules
                if security_groups:
                    sg_diagnostics = self._diagnose_security_group_rules(security_groups)
                    diagnostics['security_group_diagnostics'] = sg_diagnostics
                    diagnostics['diagnostics'].extend(sg_diagnostics.get('issues', []))
                
                # Check for ENI creation issues
                eni_diagnostics = self._diagnose_eni_creation_issues(subnet_id)
                diagnostics['eni_diagnostics'] = eni_diagnostics
                diagnostics['diagnostics'].extend(eni_diagnostics.get('issues', []))
                
            except self.glue_client.exceptions.EntityNotFoundException:
                diagnostics['connection_exists'] = False
                diagnostics['diagnostics'].append(f"Glue connection '{connection_name}' does not exist")
                diagnostics['recommendations'].append(f"Create Glue connection '{connection_name}' with proper VPC configuration")
            
        except Exception as diag_error:
            self.structured_logger.error("Failed to diagnose Glue connection", 
                                       connection_name=connection_name, 
                                       error=str(diag_error))
            diagnostics['diagnostics'].append(f"Diagnostic failure: {str(diag_error)}")
        
        return diagnostics
    
    def _diagnose_subnet_accessibility(self, subnet_id: str) -> Dict[str, Any]:
        """Diagnose subnet accessibility issues."""
        diagnostics = {'subnet_id': subnet_id, 'issues': [], 'recommendations': []}
        
        if not self.ec2_client:
            diagnostics['issues'].append("Unable to diagnose subnet - EC2 client not available")
            return diagnostics
        
        try:
            response = self.ec2_client.describe_subnets(SubnetIds=[subnet_id])
            subnet = response['Subnets'][0]
            
            # Check subnet state
            if subnet['State'] != 'available':
                diagnostics['issues'].append(f"Subnet {subnet_id} is in '{subnet['State']}' state, not 'available'")
                diagnostics['recommendations'].append(f"Ensure subnet {subnet_id} is in 'available' state")
            
            # Check available IP addresses
            available_ips = subnet.get('AvailableIpAddressCount', 0)
            if available_ips < 2:
                diagnostics['issues'].append(f"Subnet {subnet_id} has only {available_ips} available IP addresses")
                diagnostics['recommendations'].append(f"Ensure subnet {subnet_id} has sufficient available IP addresses for ENI creation")
            
            # Check route table for internet/NAT gateway access
            vpc_id = subnet['VpcId']
            route_diagnostics = self._diagnose_route_table_connectivity(subnet_id, vpc_id)
            diagnostics.update(route_diagnostics)
            
        except Exception as e:
            diagnostics['issues'].append(f"Failed to describe subnet {subnet_id}: {str(e)}")
        
        return diagnostics
    
    def _diagnose_route_table_connectivity(self, subnet_id: str, vpc_id: str) -> Dict[str, Any]:
        """Diagnose route table connectivity for subnet."""
        diagnostics = {'route_issues': [], 'route_recommendations': []}
        
        if not self.ec2_client:
            diagnostics['route_issues'].append("Unable to diagnose routes - EC2 client not available")
            return diagnostics
        
        try:
            # Get route tables associated with the subnet
            response = self.ec2_client.describe_route_tables(
                Filters=[
                    {'Name': 'association.subnet-id', 'Values': [subnet_id]}
                ]
            )
            
            route_tables = response.get('RouteTables', [])
            if not route_tables:
                # Check VPC main route table
                response = self.ec2_client.describe_route_tables(
                    Filters=[
                        {'Name': 'vpc-id', 'Values': [vpc_id]},
                        {'Name': 'association.main', 'Values': ['true']}
                    ]
                )
                route_tables = response.get('RouteTables', [])
            
            if route_tables:
                route_table = route_tables[0]
                routes = route_table.get('Routes', [])
                
                # Check for internet gateway or NAT gateway routes
                has_internet_route = any(
                    route.get('GatewayId', '').startswith('igw-') or 
                    route.get('NatGatewayId', '').startswith('nat-')
                    for route in routes
                )
                
                if not has_internet_route:
                    diagnostics['route_issues'].append(
                        f"Subnet {subnet_id} route table lacks internet gateway or NAT gateway route"
                    )
                    diagnostics['route_recommendations'].append(
                        "Add route to internet gateway (for public subnet) or NAT gateway (for private subnet)"
                    )
            
        except Exception as e:
            diagnostics['route_issues'].append(f"Failed to analyze route tables: {str(e)}")
        
        return diagnostics
    
    def _diagnose_security_group_rules(self, security_group_ids: List[str]) -> Dict[str, Any]:
        """Diagnose security group rule issues."""
        diagnostics = {'security_groups': security_group_ids, 'issues': [], 'recommendations': []}
        
        if not self.ec2_client:
            diagnostics['issues'].append("Unable to diagnose security groups - EC2 client not available")
            return diagnostics
        
        try:
            response = self.ec2_client.describe_security_groups(GroupIds=security_group_ids)
            
            for sg in response['SecurityGroups']:
                sg_id = sg['GroupId']
                
                # Check outbound rules for database ports
                outbound_rules = sg.get('IpPermissionsEgress', [])
                has_database_outbound = any(
                    self._rule_allows_database_ports(rule) for rule in outbound_rules
                )
                
                if not has_database_outbound:
                    diagnostics['issues'].append(
                        f"Security group {sg_id} lacks outbound rules for common database ports"
                    )
                    diagnostics['recommendations'].append(
                        f"Add outbound rules to security group {sg_id} for database ports (1433, 1521, 5432, 50000)"
                    )
                
                # Check for overly restrictive rules
                inbound_rules = sg.get('IpPermissions', [])
                if not inbound_rules:
                    diagnostics['issues'].append(
                        f"Security group {sg_id} has no inbound rules - may be too restrictive"
                    )
        
        except Exception as e:
            diagnostics['issues'].append(f"Failed to analyze security groups: {str(e)}")
        
        return diagnostics
    
    def _rule_allows_database_ports(self, rule: Dict[str, Any]) -> bool:
        """Check if security group rule allows common database ports."""
        from_port = rule.get('FromPort')
        to_port = rule.get('ToPort')
        
        if from_port is None or to_port is None:
            return False
        
        # Check for rules that allow all traffic (common in outbound rules)
        if from_port == 0 and to_port == 65535:
            return True
        
        # Check for rules that allow all traffic on all protocols (FromPort = -1)
        if from_port == -1:
            return True
        
        database_ports = [1433, 1521, 5432, 50000]  # SQL Server, Oracle, PostgreSQL, DB2
        
        return any(
            from_port <= port <= to_port for port in database_ports
        )
    
    def _diagnose_eni_creation_issues(self, subnet_id: str) -> Dict[str, Any]:
        """Diagnose ENI creation issues and service limits."""
        diagnostics = {'issues': [], 'recommendations': []}
        
        if not self.ec2_client:
            diagnostics['issues'].append("Unable to diagnose ENI issues - EC2 client not available")
            return diagnostics
        
        try:
            # Check ENI limits
            response = self.ec2_client.describe_account_attributes(
                AttributeNames=['max-elastic-network-interfaces']
            )
            
            max_enis = 0
            for attr in response.get('AccountAttributes', []):
                if attr['AttributeName'] == 'max-elastic-network-interfaces':
                    max_enis = int(attr['AttributeValues'][0]['AttributeValue'])
                    break
            
            # Count current ENIs
            eni_response = self.ec2_client.describe_network_interfaces()
            current_enis = len(eni_response.get('NetworkInterfaces', []))
            
            if current_enis >= max_enis * 0.9:  # 90% threshold
                diagnostics['issues'].append(
                    f"ENI usage is high: {current_enis}/{max_enis} (90%+ threshold reached)"
                )
                diagnostics['recommendations'].append(
                    "Consider requesting ENI limit increase or cleaning up unused ENIs"
                )
            
        except Exception as e:
            diagnostics['issues'].append(f"Failed to check ENI limits: {str(e)}")
        
        return diagnostics
    
    def diagnose_vpc_endpoint_issues(self, vpc_id: str, service_name: str = 's3') -> Dict[str, Any]:
        """Diagnose VPC endpoint connectivity issues."""
        diagnostics = {
            'vpc_id': vpc_id,
            'service_name': service_name,
            'issues': [],
            'recommendations': []
        }
        
        if not self.ec2_client:
            diagnostics['issues'].append("Unable to diagnose VPC endpoints - EC2 client not available")
            return diagnostics
        
        try:
            # Check if VPC endpoint exists
            region = boto3.Session().region_name or 'us-east-1'
            response = self.ec2_client.describe_vpc_endpoints(
                Filters=[
                    {'Name': 'vpc-id', 'Values': [vpc_id]},
                    {'Name': 'service-name', 'Values': [f'com.amazonaws.{region}.{service_name}']}
                ]
            )
            
            endpoints = response.get('VpcEndpoints', [])
            if not endpoints:
                diagnostics['issues'].append(f"No {service_name} VPC endpoint found in VPC {vpc_id}")
                diagnostics['recommendations'].append(f"Create {service_name} VPC endpoint in VPC {vpc_id}")
            else:
                endpoint = endpoints[0]
                if endpoint['State'] != 'Available':
                    diagnostics['issues'].append(
                        f"VPC endpoint {endpoint['VpcEndpointId']} is in '{endpoint['State']}' state"
                    )
                    diagnostics['recommendations'].append(
                        f"Wait for VPC endpoint {endpoint['VpcEndpointId']} to become 'Available'"
                    )
        
        except Exception as e:
            diagnostics['issues'].append(f"Failed to check VPC endpoints: {str(e)}")
        
        return diagnostics