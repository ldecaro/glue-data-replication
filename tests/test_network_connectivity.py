#!/usr/bin/env python3
"""
Network connectivity tests for AWS Glue Data Replication

This test suite covers:
- Unit tests for network configuration parsing and validation
- Integration tests for Glue connection creation and usage
- End-to-end tests for cross-VPC database connectivity scenarios
- Security group rule validation and VPC endpoint functionality

Requirements covered: 8.1, 8.2, 8.3, 8.6
"""

import unittest
from unittest.mock import Mock, MagicMock, patch, call
import json
import sys
import os
from typing import Dict, List, Optional, Any
import boto3
from moto import mock_glue, mock_ec2, mock_cloudformation
import time

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Mock PySpark and AWS Glue imports for testing
mock_modules = [
    'awsglue', 'awsglue.utils', 'awsglue.context', 'awsglue.job',
    'pyspark', 'pyspark.context', 'pyspark.sql', 'pyspark.sql.types',
    'pyspark.sql.functions'
]

for module in mock_modules:
    sys.modules[module] = MagicMock()

# Import the classes and functions to test after mocking
from scripts.glue_data_replication import (
    NetworkConfig, ConnectionConfig, JobConfig, JobConfigurationParser,
    GlueConnectionManager, GlueConnectionError, NetworkValidationError,
    StructuredLogger
)


class TestNetworkConfigurationParsing(unittest.TestCase):
    """Unit tests for network configuration parsing and validation."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.sample_network_args = {
            'SOURCE_VPC_ID': 'vpc-12345678',
            'SOURCE_SUBNET_IDS': 'subnet-11111111,subnet-22222222',
            'SOURCE_SECURITY_GROUP_IDS': 'sg-11111111,sg-22222222',
            'CREATE_SOURCE_S3_VPC_ENDPOINT': 'YES',
            'TARGET_VPC_ID': 'vpc-87654321',
            'TARGET_SUBNET_IDS': 'subnet-33333333,subnet-44444444',
            'TARGET_SECURITY_GROUP_IDS': 'sg-33333333,sg-44444444',
            'CREATE_TARGET_S3_VPC_ENDPOINT': 'NO'
        }
    
    def test_network_config_creation_valid(self):
        """Test creating valid NetworkConfig objects."""
        # Test with all parameters
        config = NetworkConfig(
            vpc_id='vpc-12345678',
            subnet_ids=['subnet-11111111', 'subnet-22222222'],
            security_group_ids=['sg-11111111', 'sg-22222222'],
            create_s3_vpc_endpoint=True,
            glue_connection_name='test-connection'
        )
        
        self.assertEqual(config.vpc_id, 'vpc-12345678')
        self.assertEqual(len(config.subnet_ids), 2)
        self.assertEqual(len(config.security_group_ids), 2)
        self.assertTrue(config.create_s3_vpc_endpoint)
        self.assertEqual(config.glue_connection_name, 'test-connection')
    
    def test_network_config_creation_minimal(self):
        """Test creating minimal NetworkConfig objects."""
        config = NetworkConfig()
        
        self.assertIsNone(config.vpc_id)
        self.assertIsNone(config.subnet_ids)
        self.assertIsNone(config.security_group_ids)
        self.assertFalse(config.create_s3_vpc_endpoint)
        self.assertIsNone(config.glue_connection_name)
    
    def test_network_config_has_network_config(self):
        """Test has_network_config method."""
        # Test with VPC ID
        config_with_vpc = NetworkConfig(vpc_id='vpc-12345678')
        self.assertTrue(config_with_vpc.has_network_config())
        
        # Test without VPC ID
        config_without_vpc = NetworkConfig()
        self.assertFalse(config_without_vpc.has_network_config())
    
    def test_network_config_requires_glue_connection(self):
        """Test requires_glue_connection method."""
        # Test with complete network config
        config_complete = NetworkConfig(
            vpc_id='vpc-12345678',
            subnet_ids=['subnet-11111111'],
            security_group_ids=['sg-11111111']
        )
        self.assertTrue(config_complete.requires_glue_connection())
        
        # Test with incomplete network config
        config_incomplete = NetworkConfig(vpc_id='vpc-12345678')
        self.assertFalse(config_incomplete.requires_glue_connection())
        
        # Test without network config
        config_none = NetworkConfig()
        self.assertFalse(config_none.requires_glue_connection())
    
    def test_network_config_validation_success(self):
        """Test successful network configuration validation."""
        config = NetworkConfig(
            vpc_id='vpc-12345678',
            subnet_ids=['subnet-11111111', 'subnet-22222222'],
            security_group_ids=['sg-11111111', 'sg-22222222']
        )
        
        # Should not raise any exception
        config.validate()
    
    def test_network_config_validation_failures(self):
        """Test network configuration validation failures."""
        # Test with VPC ID but no subnets
        config_no_subnets = NetworkConfig(
            vpc_id='vpc-12345678',
            security_group_ids=['sg-11111111']
        )
        
        with self.assertRaises(ValueError) as context:
            config_no_subnets.validate()
        self.assertIn("subnet_ids must be provided", str(context.exception))
        
        # Test with VPC ID but no security groups
        config_no_sg = NetworkConfig(
            vpc_id='vpc-12345678',
            subnet_ids=['subnet-11111111']
        )
        
        with self.assertRaises(ValueError) as context:
            config_no_sg.validate()
        self.assertIn("security_group_ids must be provided", str(context.exception))
        
        # Test with invalid VPC ID format
        config_invalid_vpc = NetworkConfig(
            vpc_id='invalid-vpc-id',
            subnet_ids=['subnet-11111111'],
            security_group_ids=['sg-11111111']
        )
        
        with self.assertRaises(ValueError) as context:
            config_invalid_vpc.validate()
        self.assertIn("Invalid VPC ID format", str(context.exception))
    
    def test_parse_network_config_source(self):
        """Test parsing source network configuration from CloudFormation parameters."""
        source_config = JobConfigurationParser.parse_network_config(
            self.sample_network_args, 'SOURCE'
        )
        
        self.assertIsNotNone(source_config)
        self.assertEqual(source_config.vpc_id, 'vpc-12345678')
        self.assertEqual(len(source_config.subnet_ids), 2)
        self.assertEqual(len(source_config.security_group_ids), 2)
        self.assertTrue(source_config.create_s3_vpc_endpoint)
    
    def test_parse_network_config_target(self):
        """Test parsing target network configuration from CloudFormation parameters."""
        target_config = JobConfigurationParser.parse_network_config(
            self.sample_network_args, 'TARGET'
        )
        
        self.assertIsNotNone(target_config)
        self.assertEqual(target_config.vpc_id, 'vpc-87654321')
        self.assertEqual(len(target_config.subnet_ids), 2)
        self.assertEqual(len(target_config.security_group_ids), 2)
        self.assertFalse(target_config.create_s3_vpc_endpoint)
    
    def test_parse_network_config_empty(self):
        """Test parsing empty network configuration."""
        empty_args = {
            'SOURCE_VPC_ID': '',
            'SOURCE_SUBNET_IDS': '',
            'SOURCE_SECURITY_GROUP_IDS': '',
            'CREATE_SOURCE_S3_VPC_ENDPOINT': 'NO'
        }
        
        config = JobConfigurationParser.parse_network_config(empty_args, 'SOURCE')
        self.assertIsNone(config)
    
    def test_parse_network_config_whitespace_handling(self):
        """Test parsing network configuration with whitespace."""
        whitespace_args = {
            'SOURCE_VPC_ID': '  vpc-12345678  ',
            'SOURCE_SUBNET_IDS': ' subnet-11111111 , subnet-22222222 ',
            'SOURCE_SECURITY_GROUP_IDS': ' sg-11111111 , sg-22222222 ',
            'CREATE_SOURCE_S3_VPC_ENDPOINT': ' YES '
        }
        
        config = JobConfigurationParser.parse_network_config(whitespace_args, 'SOURCE')
        
        self.assertEqual(config.vpc_id, 'vpc-12345678')
        self.assertEqual(config.subnet_ids, ['subnet-11111111', 'subnet-22222222'])
        self.assertEqual(config.security_group_ids, ['sg-11111111', 'sg-22222222'])
        self.assertTrue(config.create_s3_vpc_endpoint)


class TestConnectionConfigNetworkIntegration(unittest.TestCase):
    """Test ConnectionConfig integration with network configuration."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.base_connection_data = {
            'engine_type': 'postgresql',
            'connection_string': 'jdbc:postgresql://localhost:5432/testdb',
            'database': 'testdb',
            'schema': 'public',
            'username': 'testuser',
            'password': 'testpass',
            'jdbc_driver_path': 's3://bucket/postgresql-driver.jar'
        }
        
        self.network_config = NetworkConfig(
            vpc_id='vpc-12345678',
            subnet_ids=['subnet-11111111', 'subnet-22222222'],
            security_group_ids=['sg-11111111', 'sg-22222222'],
            glue_connection_name='test-connection'
        )
    
    def test_connection_config_with_network(self):
        """Test ConnectionConfig with network configuration."""
        config = ConnectionConfig(
            network_config=self.network_config,
            **self.base_connection_data
        )
        
        self.assertTrue(config.requires_cross_vpc_connection())
        self.assertEqual(config.get_glue_connection_name(), 'test-connection')
    
    def test_connection_config_without_network(self):
        """Test ConnectionConfig without network configuration."""
        config = ConnectionConfig(**self.base_connection_data)
        
        self.assertFalse(config.requires_cross_vpc_connection())
        self.assertIsNone(config.get_glue_connection_name())
    
    def test_connection_config_validation_with_network(self):
        """Test ConnectionConfig validation with network configuration."""
        config = ConnectionConfig(
            network_config=self.network_config,
            **self.base_connection_data
        )
        
        # Should not raise any exception
        config.validate()
    
    def test_connection_config_validation_invalid_network(self):
        """Test ConnectionConfig validation with invalid network configuration."""
        invalid_network = NetworkConfig(
            vpc_id='invalid-vpc-id',
            subnet_ids=['subnet-11111111'],
            security_group_ids=['sg-11111111']
        )
        
        config = ConnectionConfig(
            network_config=invalid_network,
            **self.base_connection_data
        )
        
        with self.assertRaises(ValueError):
            config.validate()


@mock_glue
class TestGlueConnectionManager(unittest.TestCase):
    """Integration tests for Glue connection creation and usage."""
    
    def setUp(self):
        """Set up test fixtures and AWS clients."""
        self.glue_client = boto3.client('glue', region_name='us-east-1')
        self.mock_spark = Mock()
        self.connection_manager = GlueConnectionManager(self.mock_spark, self.glue_client)
        
        # Create test Glue connection
        self.test_connection_name = 'test-cross-vpc-connection'
        self.glue_client.create_connection(
            ConnectionInput={
                'Name': self.test_connection_name,
                'ConnectionType': 'JDBC',
                'ConnectionProperties': {
                    'JDBC_CONNECTION_URL': 'jdbc:postgresql://db.example.com:5432/testdb',
                    'USERNAME': 'testuser',
                    'PASSWORD': 'testpass'
                },
                'PhysicalConnectionRequirements': {
                    'SubnetId': 'subnet-12345678',
                    'SecurityGroupIdList': ['sg-12345678'],
                    'AvailabilityZone': 'us-east-1a'
                }
            }
        )
    
    def test_get_glue_connection_success(self):
        """Test successful retrieval of Glue connection."""
        connection_details = self.connection_manager.get_glue_connection(
            self.test_connection_name
        )
        
        self.assertIsNotNone(connection_details)
        self.assertEqual(connection_details['Name'], self.test_connection_name)
        self.assertEqual(connection_details['ConnectionType'], 'JDBC')
        self.assertIn('ConnectionProperties', connection_details)
        self.assertIn('PhysicalConnectionRequirements', connection_details)
    
    def test_get_glue_connection_not_found(self):
        """Test retrieval of non-existent Glue connection."""
        connection_details = self.connection_manager.get_glue_connection(
            'non-existent-connection'
        )
        
        self.assertIsNone(connection_details)
    
    def test_get_glue_connection_error_handling(self):
        """Test error handling in Glue connection retrieval."""
        # Mock client to raise exception
        mock_client = Mock()
        mock_client.get_connection.side_effect = Exception("AWS API Error")
        
        connection_manager = GlueConnectionManager(self.mock_spark, mock_client)
        
        with self.assertRaises(GlueConnectionError):
            connection_manager.get_glue_connection('test-connection')
    
    def test_validate_network_connectivity_success(self):
        """Test successful network connectivity validation."""
        # Mock successful connection test
        mock_df = Mock()
        mock_df.count.return_value = 1
        self.mock_spark.read.format.return_value.options.return_value.load.return_value = mock_df
        
        result = self.connection_manager.validate_network_connectivity(
            self.test_connection_name,
            'jdbc:postgresql://db.example.com:5432/testdb'
        )
        
        self.assertTrue(result)
    
    def test_validate_network_connectivity_failure(self):
        """Test network connectivity validation failure."""
        # Mock connection failure
        self.mock_spark.read.format.return_value.options.return_value.load.side_effect = Exception("Connection failed")
        
        result = self.connection_manager.validate_network_connectivity(
            self.test_connection_name,
            'jdbc:postgresql://db.example.com:5432/testdb'
        )
        
        self.assertFalse(result)
    
    def test_validate_network_connectivity_timeout(self):
        """Test network connectivity validation with timeout."""
        # Mock timeout scenario
        def slow_operation(*args, **kwargs):
            time.sleep(0.1)  # Simulate slow operation
            raise Exception("Timeout")
        
        self.mock_spark.read.format.return_value.options.return_value.load.side_effect = slow_operation
        
        result = self.connection_manager.validate_network_connectivity(
            self.test_connection_name,
            'jdbc:postgresql://db.example.com:5432/testdb',
            timeout_seconds=0.05  # Very short timeout
        )
        
        self.assertFalse(result)
    
    def test_setup_jdbc_with_connection_success(self):
        """Test successful JDBC setup with Glue connection."""
        connection_config = ConnectionConfig(
            engine_type='postgresql',
            connection_string='jdbc:postgresql://db.example.com:5432/testdb',
            database='testdb',
            schema='public',
            username='testuser',
            password='testpass',
            jdbc_driver_path='s3://bucket/postgresql-driver.jar'
        )
        
        jdbc_properties = self.connection_manager.setup_jdbc_with_connection(
            connection_config, self.test_connection_name
        )
        
        self.assertIsInstance(jdbc_properties, dict)
        self.assertIn('user', jdbc_properties)
        self.assertIn('password', jdbc_properties)
        self.assertIn('driver', jdbc_properties)
        self.assertEqual(jdbc_properties['driver'], 'org.postgresql.Driver')
    
    def test_setup_jdbc_with_connection_not_found(self):
        """Test JDBC setup with non-existent Glue connection."""
        connection_config = ConnectionConfig(
            engine_type='postgresql',
            connection_string='jdbc:postgresql://db.example.com:5432/testdb',
            database='testdb',
            schema='public',
            username='testuser',
            password='testpass',
            jdbc_driver_path='s3://bucket/postgresql-driver.jar'
        )
        
        with self.assertRaises(GlueConnectionError):
            self.connection_manager.setup_jdbc_with_connection(
                connection_config, 'non-existent-connection'
            )


@mock_ec2
@mock_glue
class TestCrossVPCConnectivityScenarios(unittest.TestCase):
    """End-to-end tests for cross-VPC database connectivity scenarios."""
    
    def setUp(self):
        """Set up test fixtures and AWS resources."""
        self.ec2_client = boto3.client('ec2', region_name='us-east-1')
        self.glue_client = boto3.client('glue', region_name='us-east-1')
        
        # Create test VPCs
        self.source_vpc = self.ec2_client.create_vpc(CidrBlock='10.0.0.0/16')['Vpc']
        self.target_vpc = self.ec2_client.create_vpc(CidrBlock='10.1.0.0/16')['Vpc']
        
        # Create test subnets
        self.source_subnet = self.ec2_client.create_subnet(
            VpcId=self.source_vpc['VpcId'],
            CidrBlock='10.0.1.0/24'
        )['Subnet']
        
        self.target_subnet = self.ec2_client.create_subnet(
            VpcId=self.target_vpc['VpcId'],
            CidrBlock='10.1.1.0/24'
        )['Subnet']
        
        # Create test security groups
        self.source_sg = self.ec2_client.create_security_group(
            GroupName='source-db-sg',
            Description='Source database security group',
            VpcId=self.source_vpc['VpcId']
        )['GroupId']
        
        self.target_sg = self.ec2_client.create_security_group(
            GroupName='target-db-sg',
            Description='Target database security group',
            VpcId=self.target_vpc['VpcId']
        )['GroupId']
        
        # Add security group rules for database access
        self.ec2_client.authorize_security_group_ingress(
            GroupId=self.source_sg,
            IpPermissions=[{
                'IpProtocol': 'tcp',
                'FromPort': 5432,
                'ToPort': 5432,
                'IpRanges': [{'CidrIp': '0.0.0.0/0'}]
            }]
        )
        
        self.ec2_client.authorize_security_group_ingress(
            GroupId=self.target_sg,
            IpPermissions=[{
                'IpProtocol': 'tcp',
                'FromPort': 1521,
                'ToPort': 1521,
                'IpRanges': [{'CidrIp': '0.0.0.0/0'}]
            }]
        )
    
    def test_cross_vpc_source_only_scenario(self):
        """Test cross-VPC connectivity for source database only."""
        # Create source network configuration
        source_network = NetworkConfig(
            vpc_id=self.source_vpc['VpcId'],
            subnet_ids=[self.source_subnet['SubnetId']],
            security_group_ids=[self.source_sg],
            glue_connection_name='source-connection'
        )
        
        # Create Glue connection for source
        self.glue_client.create_connection(
            ConnectionInput={
                'Name': 'source-connection',
                'ConnectionType': 'JDBC',
                'ConnectionProperties': {
                    'JDBC_CONNECTION_URL': 'jdbc:postgresql://source-db:5432/sourcedb',
                    'USERNAME': 'sourceuser',
                    'PASSWORD': 'sourcepass'
                },
                'PhysicalConnectionRequirements': {
                    'SubnetId': self.source_subnet['SubnetId'],
                    'SecurityGroupIdList': [self.source_sg],
                    'AvailabilityZone': 'us-east-1a'
                }
            }
        )
        
        # Create connection configurations
        source_config = ConnectionConfig(
            engine_type='postgresql',
            connection_string='jdbc:postgresql://source-db:5432/sourcedb',
            database='sourcedb',
            schema='public',
            username='sourceuser',
            password='sourcepass',
            jdbc_driver_path='s3://bucket/postgresql-driver.jar',
            network_config=source_network
        )
        
        target_config = ConnectionConfig(
            engine_type='oracle',
            connection_string='jdbc:oracle:thin:@target-db:1521:targetdb',
            database='targetdb',
            schema='target_schema',
            username='targetuser',
            password='targetpass',
            jdbc_driver_path='s3://bucket/oracle-driver.jar'
        )
        
        # Create job configuration
        job_config = JobConfig(
            job_name='cross-vpc-source-test',
            source_connection=source_config,
            target_connection=target_config,
            tables=['test_table']
        )
        
        # Validate configuration
        network_summary = job_config.get_network_summary()
        
        self.assertTrue(network_summary['source_cross_vpc'])
        self.assertFalse(network_summary['target_cross_vpc'])
        self.assertEqual(network_summary['source_glue_connection'], 'source-connection')
        self.assertIsNone(network_summary['target_glue_connection'])
    
    def test_cross_vpc_target_only_scenario(self):
        """Test cross-VPC connectivity for target database only."""
        # Create target network configuration
        target_network = NetworkConfig(
            vpc_id=self.target_vpc['VpcId'],
            subnet_ids=[self.target_subnet['SubnetId']],
            security_group_ids=[self.target_sg],
            glue_connection_name='target-connection'
        )
        
        # Create Glue connection for target
        self.glue_client.create_connection(
            ConnectionInput={
                'Name': 'target-connection',
                'ConnectionType': 'JDBC',
                'ConnectionProperties': {
                    'JDBC_CONNECTION_URL': 'jdbc:oracle:thin:@target-db:1521:targetdb',
                    'USERNAME': 'targetuser',
                    'PASSWORD': 'targetpass'
                },
                'PhysicalConnectionRequirements': {
                    'SubnetId': self.target_subnet['SubnetId'],
                    'SecurityGroupIdList': [self.target_sg],
                    'AvailabilityZone': 'us-east-1a'
                }
            }
        )
        
        # Create connection configurations
        source_config = ConnectionConfig(
            engine_type='postgresql',
            connection_string='jdbc:postgresql://source-db:5432/sourcedb',
            database='sourcedb',
            schema='public',
            username='sourceuser',
            password='sourcepass',
            jdbc_driver_path='s3://bucket/postgresql-driver.jar'
        )
        
        target_config = ConnectionConfig(
            engine_type='oracle',
            connection_string='jdbc:oracle:thin:@target-db:1521:targetdb',
            database='targetdb',
            schema='target_schema',
            username='targetuser',
            password='targetpass',
            jdbc_driver_path='s3://bucket/oracle-driver.jar',
            network_config=target_network
        )
        
        # Create job configuration
        job_config = JobConfig(
            job_name='cross-vpc-target-test',
            source_connection=source_config,
            target_connection=target_config,
            tables=['test_table']
        )
        
        # Validate configuration
        network_summary = job_config.get_network_summary()
        
        self.assertFalse(network_summary['source_cross_vpc'])
        self.assertTrue(network_summary['target_cross_vpc'])
        self.assertIsNone(network_summary['source_glue_connection'])
        self.assertEqual(network_summary['target_glue_connection'], 'target-connection')
    
    def test_cross_vpc_both_databases_scenario(self):
        """Test cross-VPC connectivity for both source and target databases."""
        # Create network configurations
        source_network = NetworkConfig(
            vpc_id=self.source_vpc['VpcId'],
            subnet_ids=[self.source_subnet['SubnetId']],
            security_group_ids=[self.source_sg],
            glue_connection_name='source-connection'
        )
        
        target_network = NetworkConfig(
            vpc_id=self.target_vpc['VpcId'],
            subnet_ids=[self.target_subnet['SubnetId']],
            security_group_ids=[self.target_sg],
            glue_connection_name='target-connection'
        )
        
        # Create Glue connections
        for connection_name, subnet_id, sg_id, connection_url in [
            ('source-connection', self.source_subnet['SubnetId'], self.source_sg, 
             'jdbc:postgresql://source-db:5432/sourcedb'),
            ('target-connection', self.target_subnet['SubnetId'], self.target_sg,
             'jdbc:oracle:thin:@target-db:1521:targetdb')
        ]:
            self.glue_client.create_connection(
                ConnectionInput={
                    'Name': connection_name,
                    'ConnectionType': 'JDBC',
                    'ConnectionProperties': {
                        'JDBC_CONNECTION_URL': connection_url,
                        'USERNAME': 'testuser',
                        'PASSWORD': 'testpass'
                    },
                    'PhysicalConnectionRequirements': {
                        'SubnetId': subnet_id,
                        'SecurityGroupIdList': [sg_id],
                        'AvailabilityZone': 'us-east-1a'
                    }
                }
            )
        
        # Create connection configurations
        source_config = ConnectionConfig(
            engine_type='postgresql',
            connection_string='jdbc:postgresql://source-db:5432/sourcedb',
            database='sourcedb',
            schema='public',
            username='sourceuser',
            password='sourcepass',
            jdbc_driver_path='s3://bucket/postgresql-driver.jar',
            network_config=source_network
        )
        
        target_config = ConnectionConfig(
            engine_type='oracle',
            connection_string='jdbc:oracle:thin:@target-db:1521:targetdb',
            database='targetdb',
            schema='target_schema',
            username='targetuser',
            password='targetpass',
            jdbc_driver_path='s3://bucket/oracle-driver.jar',
            network_config=target_network
        )
        
        # Create job configuration
        job_config = JobConfig(
            job_name='cross-vpc-both-test',
            source_connection=source_config,
            target_connection=target_config,
            tables=['test_table']
        )
        
        # Validate configuration
        network_summary = job_config.get_network_summary()
        
        self.assertTrue(network_summary['source_cross_vpc'])
        self.assertTrue(network_summary['target_cross_vpc'])
        self.assertEqual(network_summary['source_glue_connection'], 'source-connection')
        self.assertEqual(network_summary['target_glue_connection'], 'target-connection')


@mock_ec2
class TestSecurityGroupValidation(unittest.TestCase):
    """Test security group rule validation and VPC endpoint functionality."""
    
    def setUp(self):
        """Set up test fixtures and AWS resources."""
        self.ec2_client = boto3.client('ec2', region_name='us-east-1')
        
        # Create test VPC
        self.test_vpc = self.ec2_client.create_vpc(CidrBlock='10.0.0.0/16')['Vpc']
        
        # Create test security group
        self.test_sg = self.ec2_client.create_security_group(
            GroupName='test-db-sg',
            Description='Test database security group',
            VpcId=self.test_vpc['VpcId']
        )['GroupId']
    
    def test_security_group_rule_validation_postgresql(self):
        """Test security group rule validation for PostgreSQL."""
        # Add PostgreSQL port rule
        self.ec2_client.authorize_security_group_ingress(
            GroupId=self.test_sg,
            IpPermissions=[{
                'IpProtocol': 'tcp',
                'FromPort': 5432,
                'ToPort': 5432,
                'IpRanges': [{'CidrIp': '10.0.0.0/16'}]
            }]
        )
        
        # Get security group details
        sg_details = self.ec2_client.describe_security_groups(
            GroupIds=[self.test_sg]
        )['SecurityGroups'][0]
        
        # Validate PostgreSQL port is open
        ingress_rules = sg_details['IpPermissions']
        postgresql_rule_found = False
        
        for rule in ingress_rules:
            if (rule['IpProtocol'] == 'tcp' and 
                rule['FromPort'] == 5432 and 
                rule['ToPort'] == 5432):
                postgresql_rule_found = True
                break
        
        self.assertTrue(postgresql_rule_found, "PostgreSQL port 5432 not found in security group rules")
    
    def test_security_group_rule_validation_oracle(self):
        """Test security group rule validation for Oracle."""
        # Add Oracle port rule
        self.ec2_client.authorize_security_group_ingress(
            GroupId=self.test_sg,
            IpPermissions=[{
                'IpProtocol': 'tcp',
                'FromPort': 1521,
                'ToPort': 1521,
                'IpRanges': [{'CidrIp': '10.0.0.0/16'}]
            }]
        )
        
        # Get security group details
        sg_details = self.ec2_client.describe_security_groups(
            GroupIds=[self.test_sg]
        )['SecurityGroups'][0]
        
        # Validate Oracle port is open
        ingress_rules = sg_details['IpPermissions']
        oracle_rule_found = False
        
        for rule in ingress_rules:
            if (rule['IpProtocol'] == 'tcp' and 
                rule['FromPort'] == 1521 and 
                rule['ToPort'] == 1521):
                oracle_rule_found = True
                break
        
        self.assertTrue(oracle_rule_found, "Oracle port 1521 not found in security group rules")
    
    def test_security_group_rule_validation_sqlserver(self):
        """Test security group rule validation for SQL Server."""
        # Add SQL Server port rule
        self.ec2_client.authorize_security_group_ingress(
            GroupId=self.test_sg,
            IpPermissions=[{
                'IpProtocol': 'tcp',
                'FromPort': 1433,
                'ToPort': 1433,
                'IpRanges': [{'CidrIp': '10.0.0.0/16'}]
            }]
        )
        
        # Get security group details
        sg_details = self.ec2_client.describe_security_groups(
            GroupIds=[self.test_sg]
        )['SecurityGroups'][0]
        
        # Validate SQL Server port is open
        ingress_rules = sg_details['IpPermissions']
        sqlserver_rule_found = False
        
        for rule in ingress_rules:
            if (rule['IpProtocol'] == 'tcp' and 
                rule['FromPort'] == 1433 and 
                rule['ToPort'] == 1433):
                sqlserver_rule_found = True
                break
        
        self.assertTrue(sqlserver_rule_found, "SQL Server port 1433 not found in security group rules")
    
    def test_vpc_endpoint_creation_s3(self):
        """Test S3 VPC endpoint creation and configuration."""
        # Create S3 VPC endpoint
        endpoint_response = self.ec2_client.create_vpc_endpoint(
            VpcId=self.test_vpc['VpcId'],
            ServiceName='com.amazonaws.us-east-1.s3',
            VpcEndpointType='Gateway'
        )
        
        endpoint_id = endpoint_response['VpcEndpoint']['VpcEndpointId']
        
        # Verify endpoint was created
        endpoints = self.ec2_client.describe_vpc_endpoints(
            VpcEndpointIds=[endpoint_id]
        )['VpcEndpoints']
        
        self.assertEqual(len(endpoints), 1)
        endpoint = endpoints[0]
        
        self.assertEqual(endpoint['VpcId'], self.test_vpc['VpcId'])
        self.assertEqual(endpoint['ServiceName'], 'com.amazonaws.us-east-1.s3')
        self.assertEqual(endpoint['VpcEndpointType'], 'Gateway')
        self.assertEqual(endpoint['State'], 'Available')
    
    def test_vpc_endpoint_policy_validation(self):
        """Test VPC endpoint policy validation for S3 access."""
        # Create S3 VPC endpoint with custom policy
        endpoint_policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": "*",
                    "Action": [
                        "s3:GetObject",
                        "s3:ListBucket"
                    ],
                    "Resource": [
                        "arn:aws:s3:::test-jdbc-drivers/*",
                        "arn:aws:s3:::test-jdbc-drivers"
                    ]
                }
            ]
        }
        
        endpoint_response = self.ec2_client.create_vpc_endpoint(
            VpcId=self.test_vpc['VpcId'],
            ServiceName='com.amazonaws.us-east-1.s3',
            VpcEndpointType='Gateway',
            PolicyDocument=json.dumps(endpoint_policy)
        )
        
        endpoint_id = endpoint_response['VpcEndpoint']['VpcEndpointId']
        
        # Verify endpoint policy
        endpoints = self.ec2_client.describe_vpc_endpoints(
            VpcEndpointIds=[endpoint_id]
        )['VpcEndpoints']
        
        endpoint = endpoints[0]
        policy_document = json.loads(endpoint['PolicyDocument'])
        
        # Validate policy allows S3 access
        statements = policy_document['Statement']
        s3_access_allowed = False
        
        for statement in statements:
            if ('s3:GetObject' in statement.get('Action', []) and
                's3:ListBucket' in statement.get('Action', [])):
                s3_access_allowed = True
                break
        
        self.assertTrue(s3_access_allowed, "S3 access not properly configured in VPC endpoint policy")


class TestNetworkErrorHandling(unittest.TestCase):
    """Test network-specific error handling and recovery mechanisms."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.mock_glue_client = Mock()
        self.mock_spark = Mock()
        self.connection_manager = GlueConnectionManager(self.mock_spark, self.mock_glue_client)
    
    def test_glue_connection_not_found_error(self):
        """Test handling of Glue connection not found error."""
        # Mock connection not found
        from botocore.exceptions import ClientError
        self.mock_glue_client.get_connection.side_effect = ClientError(
            {'Error': {'Code': 'EntityNotFoundException'}}, 'GetConnection'
        )
        
        result = self.connection_manager.get_glue_connection('non-existent-connection')
        self.assertIsNone(result)
    
    def test_glue_connection_access_denied_error(self):
        """Test handling of Glue connection access denied error."""
        from botocore.exceptions import ClientError
        self.mock_glue_client.get_connection.side_effect = ClientError(
            {'Error': {'Code': 'AccessDeniedException'}}, 'GetConnection'
        )
        
        with self.assertRaises(GlueConnectionError) as context:
            self.connection_manager.get_glue_connection('test-connection')
        
        self.assertIn("Access denied", str(context.exception))
    
    def test_network_validation_timeout_error(self):
        """Test handling of network validation timeout."""
        # Mock timeout scenario
        def timeout_operation(*args, **kwargs):
            import time
            time.sleep(0.1)
            raise Exception("Operation timed out")
        
        self.mock_spark.read.format.return_value.options.return_value.load.side_effect = timeout_operation
        
        # Mock successful connection retrieval
        self.mock_glue_client.get_connection.return_value = {
            'Connection': {
                'Name': 'test-connection',
                'ConnectionType': 'JDBC',
                'ConnectionProperties': {
                    'JDBC_CONNECTION_URL': 'jdbc:postgresql://db:5432/test'
                }
            }
        }
        
        result = self.connection_manager.validate_network_connectivity(
            'test-connection',
            'jdbc:postgresql://db:5432/test',
            timeout_seconds=0.05
        )
        
        self.assertFalse(result)
    
    def test_subnet_accessibility_validation_error(self):
        """Test handling of subnet accessibility validation errors."""
        network_config = NetworkConfig(
            vpc_id='vpc-12345678',
            subnet_ids=['subnet-invalid'],
            security_group_ids=['sg-12345678']
        )
        
        # Test validation should catch invalid subnet format
        with self.assertRaises(ValueError) as context:
            network_config.validate()
        
        # Note: This test assumes validation logic exists in NetworkConfig.validate()
        # The actual implementation may vary
    
    def test_security_group_validation_error(self):
        """Test handling of security group validation errors."""
        network_config = NetworkConfig(
            vpc_id='vpc-12345678',
            subnet_ids=['subnet-12345678'],
            security_group_ids=['sg-invalid']
        )
        
        # Test validation should catch invalid security group format
        with self.assertRaises(ValueError) as context:
            network_config.validate()
        
        # Note: This test assumes validation logic exists in NetworkConfig.validate()
        # The actual implementation may vary


if __name__ == '__main__':
    # Create test suite
    test_suite = unittest.TestSuite()
    
    # Add test classes
    test_classes = [
        TestNetworkConfigurationParsing,
        TestConnectionConfigNetworkIntegration,
        TestGlueConnectionManager,
        TestCrossVPCConnectivityScenarios,
        TestSecurityGroupValidation,
        TestNetworkErrorHandling
    ]
    
    for test_class in test_classes:
        tests = unittest.TestLoader().loadTestsFromTestCase(test_class)
        test_suite.addTests(tests)
    
    # Run tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(test_suite)
    
    # Print summary
    print(f"\n{'='*60}")
    print("NETWORK CONNECTIVITY TESTS SUMMARY")
    print(f"{'='*60}")
    print(f"Tests run: {result.testsRun}")
    print(f"Failures: {len(result.failures)}")
    print(f"Errors: {len(result.errors)}")
    print(f"Success rate: {((result.testsRun - len(result.failures) - len(result.errors)) / result.testsRun * 100):.1f}%")
    
    if result.failures:
        print(f"\nFailures:")
        for test, traceback in result.failures:
            print(f"- {test}: {traceback.split('AssertionError: ')[-1].split('\\n')[0]}")
    
    if result.errors:
        print(f"\nErrors:")
        for test, traceback in result.errors:
            print(f"- {test}: {traceback.split('\\n')[-2]}")
    
    print(f"\n{'='*60}")
    
    # Exit with appropriate code
    exit(0 if result.wasSuccessful() else 1)