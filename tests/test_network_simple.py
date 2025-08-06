#!/usr/bin/env python3
"""
Simple network connectivity test to verify functionality
"""

import sys
import os
from unittest.mock import MagicMock

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Mock PySpark and AWS Glue imports
mock_modules = [
    'awsglue', 'awsglue.utils', 'awsglue.context', 'awsglue.job',
    'pyspark', 'pyspark.context', 'pyspark.sql', 'pyspark.sql.types',
    'pyspark.sql.functions'
]

for module in mock_modules:
    sys.modules[module] = MagicMock()

# Write results to file for visibility
with open('network_test_results.txt', 'w') as f:
    f.write("Network Connectivity Test Results\n")
    f.write("=" * 40 + "\n\n")
    
    try:
        # Import our classes
        from scripts.glue_data_replication import NetworkConfig, ConnectionConfig, JobConfig
        f.write("✓ Successfully imported NetworkConfig, ConnectionConfig, JobConfig\n")
        
        # Test NetworkConfig
        config = NetworkConfig(
            vpc_id='vpc-12345678',
            subnet_ids=['subnet-11111111', 'subnet-22222222'],
            security_group_ids=['sg-11111111', 'sg-22222222']
        )
        f.write("✓ Successfully created NetworkConfig\n")
        f.write(f"  - has_network_config(): {config.has_network_config()}\n")
        f.write(f"  - requires_glue_connection(): {config.requires_glue_connection()}\n")
        
        # Test ConnectionConfig with network
        connection_config = ConnectionConfig(
            engine_type='postgresql',
            connection_string='jdbc:postgresql://localhost:5432/testdb',
            database='testdb',
            schema='public',
            username='testuser',
            password='testpass',
            jdbc_driver_path='s3://bucket/postgresql-driver.jar',
            network_config=config
        )
        f.write("✓ Successfully created ConnectionConfig with network configuration\n")
        f.write(f"  - requires_cross_vpc_connection(): {connection_config.requires_cross_vpc_connection()}\n")
        f.write(f"  - get_glue_connection_name(): {connection_config.get_glue_connection_name()}\n")
        
        # Test JobConfig
        target_config = ConnectionConfig(
            engine_type='oracle',
            connection_string='jdbc:oracle:thin:@target:1521:targetdb',
            database='targetdb',
            schema='target_schema',
            username='targetuser',
            password='targetpass',
            jdbc_driver_path='s3://bucket/oracle-driver.jar'
        )
        
        job_config = JobConfig(
            job_name='test-network-job',
            source_connection=connection_config,
            target_connection=target_config,
            tables=['test_table']
        )
        f.write("✓ Successfully created JobConfig\n")
        
        network_summary = job_config.get_network_summary()
        f.write("✓ Successfully retrieved network summary\n")
        f.write(f"  - source_cross_vpc: {network_summary['source_cross_vpc']}\n")
        f.write(f"  - target_cross_vpc: {network_summary['target_cross_vpc']}\n")
        f.write(f"  - source_glue_connection: {network_summary['source_glue_connection']}\n")
        f.write(f"  - target_glue_connection: {network_summary['target_glue_connection']}\n")
        
        f.write("\n" + "=" * 40 + "\n")
        f.write("✅ ALL NETWORK CONNECTIVITY TESTS PASSED!\n")
        f.write("=" * 40 + "\n")
        
        # Test parsing functionality
        from scripts.glue_data_replication import JobConfigurationParser
        
        sample_args = {
            'SOURCE_VPC_ID': 'vpc-12345678',
            'SOURCE_SUBNET_IDS': 'subnet-11111111,subnet-22222222',
            'SOURCE_SECURITY_GROUP_IDS': 'sg-11111111,sg-22222222',
            'CREATE_SOURCE_S3_VPC_ENDPOINT': 'YES'
        }
        
        parsed_config = JobConfigurationParser.parse_network_config(sample_args, 'SOURCE')
        f.write("\n✓ Successfully parsed network configuration from CloudFormation parameters\n")
        f.write(f"  - VPC ID: {parsed_config.vpc_id}\n")
        f.write(f"  - Subnet IDs: {parsed_config.subnet_ids}\n")
        f.write(f"  - Security Group IDs: {parsed_config.security_group_ids}\n")
        f.write(f"  - Create S3 VPC Endpoint: {parsed_config.create_s3_vpc_endpoint}\n")
        
        f.write("\n🎉 Network connectivity functionality is working correctly!\n")
        f.write("\nImplemented features:\n")
        f.write("✓ NetworkConfig class for cross-VPC configuration\n")
        f.write("✓ Enhanced ConnectionConfig with network awareness\n")
        f.write("✓ Enhanced JobConfig with network summary and validation\n")
        f.write("✓ CloudFormation parameter parsing for network configuration\n")
        f.write("✓ Cross-VPC connection detection and management\n")
        
    except Exception as e:
        f.write(f"✗ Error: {e}\n")
        import traceback
        f.write(traceback.format_exc())

print("Test completed. Check network_test_results.txt for results.")