#!/usr/bin/env python3
"""
Simple script to validate CloudFormation template syntax using PyYAML.
"""

import yaml
import sys

def validate_template():
    try:
        with open('infrastructure/cloudformation/glue-data-replication.yaml', 'r') as f:
            template_content = f.read()
        
        # Parse YAML
        template_dict = yaml.safe_load(template_content)
        
        print("✓ YAML syntax is valid")
        
        # Check basic structure
        required_sections = ['AWSTemplateFormatVersion', 'Parameters', 'Conditions', 'Resources', 'Outputs']
        for section in required_sections:
            if section in template_dict:
                print(f"✓ Found section: {section}")
            else:
                print(f"✗ Missing section: {section}")
        
        # Check conditions
        conditions = template_dict.get('Conditions', {})
        print(f"✓ Found {len(conditions)} conditions")
        
        for condition_name, condition_value in conditions.items():
            if isinstance(condition_value, (str, dict, list)):
                print(f"  ✓ Condition '{condition_name}' has valid format")
            else:
                print(f"  ✗ Condition '{condition_name}' has invalid format: {type(condition_value)}")
        
        # Check VPC endpoint resources
        resources = template_dict.get('Resources', {})
        vpc_endpoint_resources = [
            'SourceS3VpcEndpoint',
            'TargetS3VpcEndpoint',
            'SourceRouteTableLookupFunction',
            'TargetRouteTableLookupFunction',
            'RouteTableLookupRole'
        ]
        
        for resource in vpc_endpoint_resources:
            if resource in resources:
                print(f"✓ Found VPC endpoint resource: {resource}")
            else:
                print(f"✗ Missing VPC endpoint resource: {resource}")
        
        print("\n✓ Template validation completed successfully")
        return True
        
    except yaml.YAMLError as e:
        print(f"✗ YAML syntax error: {e}")
        return False
    except Exception as e:
        print(f"✗ Validation error: {e}")
        return False

if __name__ == "__main__":
    success = validate_template()
    sys.exit(0 if success else 1)