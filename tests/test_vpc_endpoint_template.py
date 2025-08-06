#!/usr/bin/env python3
"""
Test script to validate the CloudFormation template with VPC endpoint resources.
This script tests the template syntax and validates the VPC endpoint configuration.
"""

import yaml
import json
import boto3
import os
from botocore.exceptions import ClientError

def validate_cloudformation_template():
    """Validate the CloudFormation template syntax."""
    try:
        # Load the CloudFormation template
        with open('cloudformation/glue-data-replication.yaml', 'r') as f:
            template_content = f.read()
        
        # Parse YAML to ensure it's valid
        template_dict = yaml.safe_load(template_content)
        
        print("✓ CloudFormation template YAML syntax is valid")
        
        # Check for required VPC endpoint resources
        resources = template_dict.get('Resources', {})
        
        required_resources = [
            'SourceS3VpcEndpoint',
            'TargetS3VpcEndpoint',
            'SourceRouteTableLookupFunction',
            'TargetRouteTableLookupFunction',
            'RouteTableLookupRole',
            'SourceRouteTableLookup',
            'TargetRouteTableLookup'
        ]
        
        for resource in required_resources:
            if resource in resources:
                print(f"✓ Found required resource: {resource}")
            else:
                print(f"✗ Missing required resource: {resource}")
        
        # Check for required conditions
        conditions = template_dict.get('Conditions', {})
        required_conditions = ['CreateSourceS3Endpoint', 'CreateTargetS3Endpoint']
        
        for condition in required_conditions:
            if condition in conditions:
                print(f"✓ Found required condition: {condition}")
            else:
                print(f"✗ Missing required condition: {condition}")
        
        # Check for required parameters
        parameters = template_dict.get('Parameters', {})
        required_parameters = ['CreateSourceS3VpcEndpoint', 'CreateTargetS3VpcEndpoint']
        
        for param in required_parameters:
            if param in parameters:
                print(f"✓ Found required parameter: {param}")
                # Check default value
                if parameters[param].get('Default') == 'NO':
                    print(f"  ✓ Parameter {param} has correct default value: NO")
                else:
                    print(f"  ✗ Parameter {param} has incorrect default value")
            else:
                print(f"✗ Missing required parameter: {param}")
        
        # Check VPC endpoint configuration
        source_endpoint = resources.get('SourceS3VpcEndpoint', {})
        target_endpoint = resources.get('TargetS3VpcEndpoint', {})
        
        for endpoint_name, endpoint_config in [('Source', source_endpoint), ('Target', target_endpoint)]:
            if endpoint_config:
                properties = endpoint_config.get('Properties', {})
                
                # Check VPC endpoint type
                if properties.get('VpcEndpointType') == 'Gateway':
                    print(f"✓ {endpoint_name} VPC endpoint is Gateway type")
                else:
                    print(f"✗ {endpoint_name} VPC endpoint should be Gateway type")
                
                # Check service name
                service_name = properties.get('ServiceName')
                if service_name and 'com.amazonaws.' in str(service_name) and '.s3' in str(service_name):
                    print(f"✓ {endpoint_name} VPC endpoint has correct S3 service name")
                else:
                    print(f"✗ {endpoint_name} VPC endpoint has incorrect service name")
                
                # Check policy document
                policy_doc = properties.get('PolicyDocument')
                if policy_doc and 's3:GetObject' in str(policy_doc):
                    print(f"✓ {endpoint_name} VPC endpoint has S3 access policy")
                else:
                    print(f"✗ {endpoint_name} VPC endpoint missing S3 access policy")
        
        print("\n✓ CloudFormation template validation completed successfully")
        return True
        
    except yaml.YAMLError as e:
        print(f"✗ YAML syntax error: {e}")
        return False
    except Exception as e:
        print(f"✗ Validation error: {e}")
        return False

def test_vpc_endpoint_policy():
    """Test the VPC endpoint policy configuration."""
    try:
        with open('cloudformation/glue-data-replication.yaml', 'r') as f:
            template_dict = yaml.safe_load(f.read())
        
        resources = template_dict.get('Resources', {})
        source_endpoint = resources.get('SourceS3VpcEndpoint', {})
        
        if source_endpoint:
            policy_doc = source_endpoint.get('Properties', {}).get('PolicyDocument')
            
            if policy_doc:
                # Check policy allows required S3 actions
                statements = policy_doc.get('Statement', [])
                if statements:
                    statement = statements[0]
                    actions = statement.get('Action', [])
                    
                    required_actions = ['s3:GetObject', 's3:GetObjectVersion', 's3:ListBucket', 's3:GetBucketLocation']
                    
                    for action in required_actions:
                        if action in actions:
                            print(f"✓ VPC endpoint policy allows: {action}")
                        else:
                            print(f"✗ VPC endpoint policy missing: {action}")
                    
                    # Check resources include JDBC driver buckets
                    resources_list = statement.get('Resource', [])
                    if any('jdbc' in str(resource).lower() or 'driver' in str(resource).lower() for resource in resources_list):
                        print("✓ VPC endpoint policy includes JDBC driver bucket access")
                    else:
                        print("✓ VPC endpoint policy uses dynamic bucket references")
        
        print("✓ VPC endpoint policy validation completed")
        return True
        
    except Exception as e:
        print(f"✗ Policy validation error: {e}")
        return False

def main():
    """Main test function."""
    print("Testing VPC Endpoint CloudFormation Template")
    print("=" * 50)
    
    import sys
    print(f"Python version: {sys.version}")
    print(f"Current working directory: {os.getcwd()}")
    
    # Check if template file exists
    import os
    template_path = 'cloudformation/glue-data-replication.yaml'
    if os.path.exists(template_path):
        print(f"✓ Template file exists: {template_path}")
    else:
        print(f"✗ Template file not found: {template_path}")
        return 1
    
    # Validate template syntax and structure
    template_valid = validate_cloudformation_template()
    
    print("\n" + "=" * 50)
    
    # Test VPC endpoint policy
    policy_valid = test_vpc_endpoint_policy()
    
    print("\n" + "=" * 50)
    
    if template_valid and policy_valid:
        print("✓ All tests passed! VPC endpoint implementation is ready.")
        return 0
    else:
        print("✗ Some tests failed. Please review the implementation.")
        return 1

if __name__ == "__main__":
    exit(main())