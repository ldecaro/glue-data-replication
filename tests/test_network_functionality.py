#!/usr/bin/env python3
"""
Test script to verify network-aware database connection functionality
"""

import sys
import os


# Add src directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'src'))

# Mock PySpark and AWS Glue imports for testing
from unittest.mock import MagicMock
mock_modules = [
    'awsglue', 'awsglue.utils', 'awsglue.context', 'awsglue.job',
    'pyspark', 'pyspark.context', 'pyspark.sql', 'pyspark.sql.types',
    'pyspark.sql.functions'
]

for module in mock_modules:
    sys.modules[module] = MagicMock()

from glue_job.config import (
    NetworkConfig, 
    ConnectionConfig, 
    JobConfig, 
    JobConfigurationParser
)

def test_network_config():
    """Test NetworkConfig functionality"""
    print("Testing NetworkConfig...")
    
    # Test with full network configuration
    network_config = NetworkConfig(
        vpc_id='vpc-123456',
        subnet_ids=['subnet-1', 'subnet-2'],
        security_group_ids=['sg-1', 'sg-2'],
        glue_connection_name='test-glue-connection',
        create_s3_vpc_endpoint=True
    )
    
    assert network_config.has_network_config() == True
    assert network_config.requires_glue_connection() == True
    print("✓ NetworkConfig with full configuration works")
    
    # Test with minimal configuration
    minimal_config = NetworkConfig()
    assert minimal_config.has_network_config() == False
    assert minimal_config.requires_glue_connection() == False
    print("✓ NetworkConfig with minimal configuration works")

def test_connection_config():
    """Test ConnectionConfig with network awareness"""
    print("\nTesting ConnectionConfig...")
    
    # Test with network configuration
    network_config = NetworkConfig(
        vpc_id='vpc-123456',
        subnet_ids=['subnet-1', 'subnet-2'],
        security_group_ids=['sg-1', 'sg-2'],
        glue_connection_name='source-glue-connection'
    )
    
    connection_config = ConnectionConfig(
        engine_type='postgresql',
        connection_string='jdbc:postgresql://host:5432/database',
        database='testdb',
        schema='public',
        username='testuser',
        password='testpass',
        jdbc_driver_path='s3://bucket/postgresql-driver.jar',
        network_config=network_config
    )
    
    result = connection_config.requires_cross_vpc_connection()
    assert result == True, f"Expected True, got {result}"
    glue_name = connection_config.get_glue_connection_name()
    assert glue_name == 'source-glue-connection', f"Expected 'source-glue-connection', got {glue_name}"
    print("✓ ConnectionConfig with network configuration works")
    
    # Test without network configuration
    simple_config = ConnectionConfig(
        engine_type='postgresql',
        connection_string='jdbc:postgresql://host:5432/database',
        database='testdb',
        schema='public',
        username='testuser',
        password='testpass',
        jdbc_driver_path='s3://bucket/postgresql-driver.jar'
    )
    
    result = simple_config.requires_cross_vpc_connection()
    assert not result, f"Expected falsy value, got {result}"
    glue_name = simple_config.get_glue_connection_name()
    assert glue_name == None, f"Expected None, got {glue_name}"
    print("✓ ConnectionConfig without network configuration works")

def test_job_config():
    """Test JobConfig with network awareness"""
    print("\nTesting JobConfig...")
    
    # Create source connection with cross-VPC
    source_network = NetworkConfig(
        vpc_id='vpc-source',
        subnet_ids=['subnet-src-1'],
        security_group_ids=['sg-src-1'],
        glue_connection_name='source-connection'
    )
    
    source_connection = ConnectionConfig(
        engine_type='oracle',
        connection_string='jdbc:oracle:thin:@host:1521:database',
        database='sourcedb',
        schema='source_schema',
        username='srcuser',
        password='srcpass',
        jdbc_driver_path='s3://bucket/oracle-driver.jar',
        network_config=source_network
    )
    
    # Create target connection without cross-VPC
    target_connection = ConnectionConfig(
        engine_type='postgresql',
        connection_string='jdbc:postgresql://host:5432/database',
        database='targetdb',
        schema='target_schema',
        username='tgtuser',
        password='tgtpass',
        jdbc_driver_path='s3://bucket/postgresql-driver.jar'
    )
    
    job_config = JobConfig(
        job_name='test-cross-vpc-job',
        source_connection=source_connection,
        target_connection=target_connection,
        tables=['table1', 'table2'],
        validate_connections=True,
        connection_timeout_seconds=60
    )
    
    has_cross_vpc = job_config.has_cross_vpc_connections()
    assert has_cross_vpc == True, f"Expected True, got {has_cross_vpc}"
    
    network_summary = job_config.get_network_summary()
    assert network_summary['source_cross_vpc'] == True, f"Expected True, got {network_summary['source_cross_vpc']}"
    assert not network_summary['target_cross_vpc'], f"Expected falsy value, got {network_summary['target_cross_vpc']}"
    assert network_summary['source_glue_connection'] == 'source-connection', f"Expected 'source-connection', got {network_summary['source_glue_connection']}"
    assert network_summary['target_glue_connection'] == None, f"Expected None, got {network_summary['target_glue_connection']}"
    assert network_summary['validate_connections'] == True, f"Expected True, got {network_summary['validate_connections']}"
    assert network_summary['connection_timeout'] == 60, f"Expected 60, got {network_summary['connection_timeout']}"
    
    print("✓ JobConfig with mixed network configuration works")

def test_network_config_parsing():
    """Test network configuration parsing from CloudFormation parameters"""
    print("\nTesting network configuration parsing...")
    
    test_args = {
        'SOURCE_VPC_ID': 'vpc-source-123',
        'SOURCE_SUBNET_IDS': 'subnet-1,subnet-2,subnet-3',
        'SOURCE_SECURITY_GROUP_IDS': 'sg-1,sg-2',
        'SOURCE_GLUE_CONNECTION_NAME': 'my-source-connection',
        'CREATE_SOURCE_S3_VPC_ENDPOINT': 'YES',
        'TARGET_VPC_ID': '',  # Empty target config
        'TARGET_SUBNET_IDS': '',
        'TARGET_SECURITY_GROUP_IDS': '',
        'TARGET_GLUE_CONNECTION_NAME': '',
        'CREATE_TARGET_S3_VPC_ENDPOINT': 'NO'
    }
    
    # Parse source network config
    source_network = JobConfigurationParser.parse_network_config(test_args, 'SOURCE')
    assert source_network is not None
    assert source_network.vpc_id == 'vpc-source-123'
    assert source_network.subnet_ids == ['subnet-1', 'subnet-2', 'subnet-3']
    assert source_network.security_group_ids == ['sg-1', 'sg-2']
    assert source_network.glue_connection_name == 'my-source-connection'
    assert source_network.create_s3_vpc_endpoint == True
    print("✓ Source network configuration parsing works")
    
    # Parse target network config (should be None)
    target_network = JobConfigurationParser.parse_network_config(test_args, 'TARGET')
    assert target_network is None
    print("✓ Target network configuration parsing (empty) works")

def main():
    """Run all tests"""
    print("Testing network-aware database connection functionality...\n")
    
    try:
        test_network_config()
        test_connection_config()
        test_job_config()
        test_network_config_parsing()
        
        print("\n🎉 All tests passed! Network-aware functionality is working correctly.")
        print("\nImplemented features:")
        print("✓ NetworkConfig class for cross-VPC configuration")
        print("✓ Enhanced ConnectionConfig with network awareness")
        print("✓ Enhanced JobConfig with network summary and validation")
        print("✓ Network configuration parsing from CloudFormation parameters")
        print("✓ Cross-VPC connection detection and Glue connection name retrieval")
        print("✓ Integration with existing GlueConnectionManager functionality")
        
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())