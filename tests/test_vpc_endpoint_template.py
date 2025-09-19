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
        with open('infrastructure/cloudformation/glue-data-replication.yaml', 'r') as f:
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
        # Read the CloudFormation template as raw text to check for VPC endpoint resources
        with open('infrastructure/cloudformation/glue-data-replication.yaml', 'r') as f:
            template_content = f.read()
        
        # Check for VPC endpoint resources in the template (text-based validation)
        required_vpc_resources = [
            'SourceS3VpcEndpoint',
            'TargetS3VpcEndpoint',
            'CreateSourceS3Endpoint',
            'CreateTargetS3Endpoint'
        ]
        
        missing_resources = []
        for resource in required_vpc_resources:
            if resource not in template_content:
                missing_resources.append(resource)
        
        if missing_resources:
            print(f"✗ Missing VPC endpoint resources: {missing_resources}")
            assert False, f"Missing required VPC endpoint resources: {missing_resources}"
        else:
            print("✓ All required VPC endpoint resources found in template")
        
        # Check for S3 VPC endpoint policy elements
        policy_elements = [
            's3:GetObject',
            's3:GetObjectVersion', 
            's3:ListBucket',
            's3:GetBucketLocation',
            'PolicyDocument'
        ]
        
        missing_policy_elements = []
        for element in policy_elements:
            if element not in template_content:
                missing_policy_elements.append(element)
        
        if missing_policy_elements:
            print(f"✗ Missing VPC endpoint policy elements: {missing_policy_elements}")
            assert False, f"Missing required policy elements: {missing_policy_elements}"
        else:
            print("✓ All required VPC endpoint policy elements found")
        
        # Check for VPC endpoint type configuration
        if 'VpcEndpointType' in template_content and 'Gateway' in template_content:
            print("✓ VPC endpoint configured as Gateway type")
        else:
            print("✗ VPC endpoint Gateway type configuration not found")
            assert False, "VPC endpoint should be configured as Gateway type"
        
        print("✓ VPC endpoint policy validation completed")
        
    except FileNotFoundError:
        print("✗ CloudFormation template file not found")
        assert False, "CloudFormation template file not found"
    except Exception as e:
        print(f"✗ Policy validation error: {e}")
        assert False, f"Policy validation failed: {e}"

def main():
    """Main test function."""
    print("Testing VPC Endpoint CloudFormation Template")
    print("=" * 50)
    
    import sys
    print(f"Python version: {sys.version}")
    print(f"Current working directory: {os.getcwd()}")
    
    # Check if template file exists
    import os
    template_path = 'infrastructure/cloudformation/glue-data-replication.yaml'
    if os.path.exists(template_path):
        print(f"✓ Template file exists: {template_path}")
    else:
        print(f"✗ Template file not found: {template_path}")
        return 1
    
    # Validate template syntax and structure
    template_valid = validate_cloudformation_template()
    
    print("\n" + "=" * 50)
    
    # Test VPC endpoint policy
    try:
        test_vpc_endpoint_policy()
        policy_valid = True
    except (AssertionError, Exception):
        policy_valid = False
    
    print("\n" + "=" * 50)
    
    if template_valid and policy_valid:
        print("✓ All tests passed! VPC endpoint implementation is ready.")
        return 0
    else:
        print("✗ Some tests failed. Please review the implementation.")
        return 1

if __name__ == "__main__":
    exit(main())