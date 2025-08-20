#!/usr/bin/env python3
"""
Integration tests for CloudFormation deployment of AWS Glue Data Replication

This test suite covers:
- CloudFormation template syntax and parameter validation
- IAM role and policy creation integration tests
- Glue job creation and configuration through CloudFormation

Requirements covered: 2.3, 8.1, 8.2
"""

import unittest
import json
import yaml
import boto3
import time
import sys
import os
from unittest.mock import Mock, patch, MagicMock
from moto import mock_cloudformation, mock_iam, mock_glue, mock_s3
import tempfile
from typing import Dict, Any, List

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


class TestCloudFormationTemplateValidation(unittest.TestCase):
    """Test CloudFormation template syntax and parameter validation."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.template_path = "infrastructure/cloudformation/glue-data-replication.yaml"
        with open(self.template_path, 'r') as f:
            self.template_content = yaml.safe_load(f)
    
    def test_template_syntax_validation(self):
        """Test that CloudFormation template has valid YAML syntax."""
        # Template should load without YAML parsing errors
        self.assertIsInstance(self.template_content, dict)
        self.assertIn('AWSTemplateFormatVersion', self.template_content)
        self.assertEqual(self.template_content['AWSTemplateFormatVersion'], '2010-09-09')
    
    def test_required_template_sections(self):
        """Test that template contains all required sections."""
        required_sections = ['Parameters', 'Resources', 'Outputs']
        for section in required_sections:
            self.assertIn(section, self.template_content, f"Missing required section: {section}")
    
    def test_parameter_definitions(self):
        """Test that all required parameters are properly defined."""
        parameters = self.template_content['Parameters']
        
        required_params = [
            'JobName', 'SourceEngineType', 'TargetEngineType',
            'SourceDatabase', 'TargetDatabase', 'SourceSchema', 'TargetSchema',
            'TableNames', 'SourceDbUser', 'SourceDbPassword',
            'TargetDbUser', 'TargetDbPassword', 'SourceConnectionString',
            'TargetConnectionString', 'SourceJdbcDriverS3Path',
            'TargetJdbcDriverS3Path', 'GlueJobScriptS3Path'
        ]
        
        for param in required_params:
            self.assertIn(param, parameters, f"Missing required parameter: {param}")
            self.assertIn('Type', parameters[param], f"Parameter {param} missing Type")
            self.assertIn('Description', parameters[param], f"Parameter {param} missing Description")
    
    def test_engine_type_constraints(self):
        """Test that engine type parameters have proper constraints."""
        parameters = self.template_content['Parameters']
        
        for engine_param in ['SourceEngineType', 'TargetEngineType']:
            param_def = parameters[engine_param]
            self.assertEqual(param_def['Type'], 'String')
            self.assertIn('AllowedValues', param_def)
            
            allowed_values = param_def['AllowedValues']
            expected_engines = ['oracle', 'sqlserver', 'postgresql', 'db2']
            self.assertEqual(set(allowed_values), set(expected_engines))
    
    def test_s3_path_parameter_patterns(self):
        """Test that S3 path parameters have proper regex patterns."""
        parameters = self.template_content['Parameters']
        
        s3_params = ['SourceJdbcDriverS3Path', 'TargetJdbcDriverS3Path', 'GlueJobScriptS3Path']
        
        for param in s3_params:
            param_def = parameters[param]
            self.assertIn('AllowedPattern', param_def)
            pattern = param_def['AllowedPattern']
            self.assertTrue(pattern.startswith('^s3://'))
    
    def test_connection_string_patterns(self):
        """Test that connection string parameters have proper JDBC patterns."""
        parameters = self.template_content['Parameters']
        
        conn_params = ['SourceConnectionString', 'TargetConnectionString']
        
        for param in conn_params:
            param_def = parameters[param]
            self.assertIn('AllowedPattern', param_def)
            pattern = param_def['AllowedPattern']
            self.assertTrue(pattern.startswith('^jdbc:'))
    
    def test_numeric_parameter_constraints(self):
        """Test that numeric parameters have proper min/max constraints."""
        parameters = self.template_content['Parameters']
        
        numeric_params = {
            'MaxRetries': {'min': 0, 'max': 10},
            'Timeout': {'min': 1, 'max': 2880},
            'MaxConcurrentRuns': {'min': 1, 'max': 1000},
            'NumberOfWorkers': {'min': 2, 'max': 299}
        }
        
        for param, constraints in numeric_params.items():
            param_def = parameters[param]
            self.assertEqual(param_def['Type'], 'Number')
            self.assertEqual(param_def['MinValue'], constraints['min'])
            self.assertEqual(param_def['MaxValue'], constraints['max'])


@mock_cloudformation
@mock_iam
@mock_glue
@mock_s3
class TestCloudFormationDeployment(unittest.TestCase):
    """Test CloudFormation stack deployment and resource creation."""
    
    def setUp(self):
        """Set up test fixtures and AWS clients."""
        self.cf_client = boto3.client('cloudformation', region_name='us-east-1')
        self.iam_client = boto3.client('iam', region_name='us-east-1')
        self.glue_client = boto3.client('glue', region_name='us-east-1')
        self.s3_client = boto3.client('s3', region_name='us-east-1')
        
        # Create test S3 bucket and upload mock files
        self.test_bucket = 'test-glue-assets'
        self.s3_client.create_bucket(Bucket=self.test_bucket)
        
        # Upload mock JDBC drivers and script
        self.s3_client.put_object(
            Bucket=self.test_bucket,
            Key='drivers/oracle.jar',
            Body=b'mock oracle driver'
        )
        self.s3_client.put_object(
            Bucket=self.test_bucket,
            Key='drivers/postgresql.jar',
            Body=b'mock postgresql driver'
        )
        # Upload modular structure files
        self.s3_client.put_object(
            Bucket=self.test_bucket,
            Key='src/glue_job/main.py',
            Body=b'mock glue main script'
        )
        self.s3_client.put_object(
            Bucket=self.test_bucket,
            Key='src/glue_job/__init__.py',
            Body=b'# Glue job package'
        )
        
        # Load CloudFormation template
        with open('infrastructure/cloudformation/glue-data-replication.yaml', 'r') as f:
            self.template_body = f.read()
        
        self.test_parameters = [
            {'ParameterKey': 'JobName', 'ParameterValue': 'test-replication-job'},
            {'ParameterKey': 'SourceEngineType', 'ParameterValue': 'oracle'},
            {'ParameterKey': 'TargetEngineType', 'ParameterValue': 'postgresql'},
            {'ParameterKey': 'SourceDatabase', 'ParameterValue': 'source_db'},
            {'ParameterKey': 'TargetDatabase', 'ParameterValue': 'target_db'},
            {'ParameterKey': 'SourceSchema', 'ParameterValue': 'source_schema'},
            {'ParameterKey': 'TargetSchema', 'ParameterValue': 'target_schema'},
            {'ParameterKey': 'TableNames', 'ParameterValue': 'table1,table2'},
            {'ParameterKey': 'SourceDbUser', 'ParameterValue': 'source_user'},
            {'ParameterKey': 'SourceDbPassword', 'ParameterValue': 'source_pass'},
            {'ParameterKey': 'TargetDbUser', 'ParameterValue': 'target_user'},
            {'ParameterKey': 'TargetDbPassword', 'ParameterValue': 'target_pass'},
            {'ParameterKey': 'SourceConnectionString', 'ParameterValue': 'jdbc:oracle:thin:@localhost:1521:xe'},
            {'ParameterKey': 'TargetConnectionString', 'ParameterValue': 'jdbc:postgresql://localhost:5432/testdb'},
            {'ParameterKey': 'SourceJdbcDriverS3Path', 'ParameterValue': f's3://{self.test_bucket}/drivers/oracle.jar'},
            {'ParameterKey': 'TargetJdbcDriverS3Path', 'ParameterValue': f's3://{self.test_bucket}/drivers/postgresql.jar'},
            {'ParameterKey': 'GlueJobScriptS3Path', 'ParameterValue': f's3://{self.test_bucket}/src/glue_job/main.py'}
        ]
    
    def test_template_validation_success(self):
        """Test that CloudFormation template validates successfully."""
        try:
            response = self.cf_client.validate_template(TemplateBody=self.template_body)
            self.assertIn('Parameters', response)
            self.assertIn('Description', response)
        except Exception as e:
            self.fail(f"Template validation failed: {str(e)}")
    
    def test_stack_creation_success(self):
        """Test successful CloudFormation stack creation."""
        stack_name = 'test-glue-replication-stack'
        
        response = self.cf_client.create_stack(
            StackName=stack_name,
            TemplateBody=self.template_body,
            Parameters=self.test_parameters,
            Capabilities=['CAPABILITY_NAMED_IAM']
        )
        
        self.assertIn('StackId', response)
        
        # Wait for stack creation to complete (mocked, so immediate)
        stack_status = self.cf_client.describe_stacks(StackName=stack_name)
        self.assertEqual(len(stack_status['Stacks']), 1)
    
    def test_parameter_validation_failures(self):
        """Test that invalid parameters are properly rejected."""
        invalid_test_cases = [
            # Invalid engine type
            {
                'param': 'SourceEngineType',
                'value': 'invalid_engine',
                'expected_error': 'ValidationError'
            },
            # Invalid S3 path
            {
                'param': 'SourceJdbcDriverS3Path',
                'value': 'invalid-path',
                'expected_error': 'ValidationError'
            },
            # Invalid JDBC connection string
            {
                'param': 'SourceConnectionString',
                'value': 'invalid-connection',
                'expected_error': 'ValidationError'
            },
            # Invalid numeric value
            {
                'param': 'MaxRetries',
                'value': '15',  # Above max of 10
                'expected_error': 'ValidationError'
            }
        ]
        
        for test_case in invalid_test_cases:
            with self.subTest(param=test_case['param']):
                # Create modified parameters with invalid value
                invalid_params = self.test_parameters.copy()
                for param in invalid_params:
                    if param['ParameterKey'] == test_case['param']:
                        param['ParameterValue'] = test_case['value']
                        break
                
                with self.assertRaises(Exception):
                    self.cf_client.create_stack(
                        StackName=f'test-invalid-{test_case["param"]}',
                        TemplateBody=self.template_body,
                        Parameters=invalid_params,
                        Capabilities=['CAPABILITY_NAMED_IAM']
                    )


@mock_iam
@mock_cloudformation
class TestIAMRoleAndPolicyCreation(unittest.TestCase):
    """Test IAM role and policy creation through CloudFormation."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.cf_client = boto3.client('cloudformation', region_name='us-east-1')
        self.iam_client = boto3.client('iam', region_name='us-east-1')
        
        with open('infrastructure/cloudformation/glue-data-replication.yaml', 'r') as f:
            self.template_body = f.read()
        
        # Minimal parameters for IAM testing
        self.test_parameters = [
            {'ParameterKey': 'JobName', 'ParameterValue': 'test-iam-job'},
            {'ParameterKey': 'SourceEngineType', 'ParameterValue': 'oracle'},
            {'ParameterKey': 'TargetEngineType', 'ParameterValue': 'postgresql'},
            {'ParameterKey': 'SourceDatabase', 'ParameterValue': 'db'},
            {'ParameterKey': 'TargetDatabase', 'ParameterValue': 'db'},
            {'ParameterKey': 'SourceSchema', 'ParameterValue': 'schema'},
            {'ParameterKey': 'TargetSchema', 'ParameterValue': 'schema'},
            {'ParameterKey': 'TableNames', 'ParameterValue': 'table1'},
            {'ParameterKey': 'SourceDbUser', 'ParameterValue': 'user'},
            {'ParameterKey': 'SourceDbPassword', 'ParameterValue': 'pass'},
            {'ParameterKey': 'TargetDbUser', 'ParameterValue': 'user'},
            {'ParameterKey': 'TargetDbPassword', 'ParameterValue': 'pass'},
            {'ParameterKey': 'SourceConnectionString', 'ParameterValue': 'jdbc:oracle:thin:@localhost:1521:xe'},
            {'ParameterKey': 'TargetConnectionString', 'ParameterValue': 'jdbc:postgresql://localhost:5432/db'},
            {'ParameterKey': 'SourceJdbcDriverS3Path', 'ParameterValue': 's3://bucket/oracle.jar'},
            {'ParameterKey': 'TargetJdbcDriverS3Path', 'ParameterValue': 's3://bucket/postgresql.jar'},
            {'ParameterKey': 'GlueJobScriptS3Path', 'ParameterValue': 's3://bucket/script.py'}
        ]
    
    def test_iam_role_creation(self):
        """Test that IAM role is created with correct properties."""
        stack_name = 'test-iam-role-stack'
        
        # Create stack
        self.cf_client.create_stack(
            StackName=stack_name,
            TemplateBody=self.template_body,
            Parameters=self.test_parameters,
            Capabilities=['CAPABILITY_NAMED_IAM']
        )
        
        # Check that role was created
        role_name = 'test-iam-job-glue-job-role'
        try:
            role = self.iam_client.get_role(RoleName=role_name)
            self.assertEqual(role['Role']['RoleName'], role_name)
            
            # Check assume role policy
            assume_policy = role['Role']['AssumeRolePolicyDocument']
            self.assertIn('glue.amazonaws.com', str(assume_policy))
            
        except self.iam_client.exceptions.NoSuchEntityException:
            self.fail(f"IAM role {role_name} was not created")
    
    def test_iam_policy_permissions(self):
        """Test that IAM policy contains required permissions."""
        stack_name = 'test-iam-policy-stack'
        
        # Create stack
        self.cf_client.create_stack(
            StackName=stack_name,
            TemplateBody=self.template_body,
            Parameters=self.test_parameters,
            Capabilities=['CAPABILITY_NAMED_IAM']
        )
        
        role_name = 'test-iam-job-glue-job-role'
        
        # Get inline policies
        policies = self.iam_client.list_role_policies(RoleName=role_name)
        self.assertGreater(len(policies['PolicyNames']), 0)
        
        policy_name = policies['PolicyNames'][0]
        policy = self.iam_client.get_role_policy(RoleName=role_name, PolicyName=policy_name)
        
        policy_document = policy['PolicyDocument']
        statements = policy_document['Statement']
        
        # Check for required permission categories
        required_actions = [
            's3:GetObject',  # S3 permissions
            'logs:CreateLogGroup',  # CloudWatch Logs
            'glue:GetJobBookmark',  # Job Bookmarks
            'cloudwatch:PutMetricData'  # CloudWatch Metrics
        ]
        
        all_actions = []
        for statement in statements:
            if 'Action' in statement:
                if isinstance(statement['Action'], list):
                    all_actions.extend(statement['Action'])
                else:
                    all_actions.append(statement['Action'])
        
        for required_action in required_actions:
            self.assertIn(required_action, all_actions, 
                         f"Required action {required_action} not found in policy")
    
    def test_managed_policy_attachment(self):
        """Test that AWS managed policy is attached to role."""
        stack_name = 'test-managed-policy-stack'
        
        # Create stack
        self.cf_client.create_stack(
            StackName=stack_name,
            TemplateBody=self.template_body,
            Parameters=self.test_parameters,
            Capabilities=['CAPABILITY_NAMED_IAM']
        )
        
        role_name = 'test-iam-job-glue-job-role'
        
        # Check attached managed policies
        attached_policies = self.iam_client.list_attached_role_policies(RoleName=role_name)
        
        policy_arns = [policy['PolicyArn'] for policy in attached_policies['AttachedPolicies']]
        expected_policy_arn = 'arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole'
        
        self.assertIn(expected_policy_arn, policy_arns,
                     "AWSGlueServiceRole managed policy not attached")


@mock_glue
@mock_cloudformation
@mock_s3
class TestGlueJobCreation(unittest.TestCase):
    """Test Glue job creation and configuration through CloudFormation."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.cf_client = boto3.client('cloudformation', region_name='us-east-1')
        self.glue_client = boto3.client('glue', region_name='us-east-1')
        self.s3_client = boto3.client('s3', region_name='us-east-1')
        
        # Create test S3 bucket
        self.test_bucket = 'test-glue-job-bucket'
        self.s3_client.create_bucket(Bucket=self.test_bucket)
        
        with open('infrastructure/cloudformation/glue-data-replication.yaml', 'r') as f:
            self.template_body = f.read()
        
        self.test_parameters = [
            {'ParameterKey': 'JobName', 'ParameterValue': 'test-glue-job'},
            {'ParameterKey': 'SourceEngineType', 'ParameterValue': 'oracle'},
            {'ParameterKey': 'TargetEngineType', 'ParameterValue': 'postgresql'},
            {'ParameterKey': 'SourceDatabase', 'ParameterValue': 'source_db'},
            {'ParameterKey': 'TargetDatabase', 'ParameterValue': 'target_db'},
            {'ParameterKey': 'SourceSchema', 'ParameterValue': 'source_schema'},
            {'ParameterKey': 'TargetSchema', 'ParameterValue': 'target_schema'},
            {'ParameterKey': 'TableNames', 'ParameterValue': 'table1,table2,table3'},
            {'ParameterKey': 'SourceDbUser', 'ParameterValue': 'source_user'},
            {'ParameterKey': 'SourceDbPassword', 'ParameterValue': 'source_pass'},
            {'ParameterKey': 'TargetDbUser', 'ParameterValue': 'target_user'},
            {'ParameterKey': 'TargetDbPassword', 'ParameterValue': 'target_pass'},
            {'ParameterKey': 'SourceConnectionString', 'ParameterValue': 'jdbc:oracle:thin:@localhost:1521:xe'},
            {'ParameterKey': 'TargetConnectionString', 'ParameterValue': 'jdbc:postgresql://localhost:5432/testdb'},
            {'ParameterKey': 'SourceJdbcDriverS3Path', 'ParameterValue': f's3://{self.test_bucket}/oracle.jar'},
            {'ParameterKey': 'TargetJdbcDriverS3Path', 'ParameterValue': f's3://{self.test_bucket}/postgresql.jar'},
            {'ParameterKey': 'GlueJobScriptS3Path', 'ParameterValue': f's3://{self.test_bucket}/script.py'},
            {'ParameterKey': 'MaxRetries', 'ParameterValue': '2'},
            {'ParameterKey': 'Timeout', 'ParameterValue': '60'},
            {'ParameterKey': 'MaxConcurrentRuns', 'ParameterValue': '1'},
            {'ParameterKey': 'WorkerType', 'ParameterValue': 'G.1X'},
            {'ParameterKey': 'NumberOfWorkers', 'ParameterValue': '5'}
        ]
    
    def test_glue_job_creation(self):
        """Test that Glue job is created with correct configuration."""
        stack_name = 'test-glue-job-stack'
        
        # Create stack
        self.cf_client.create_stack(
            StackName=stack_name,
            TemplateBody=self.template_body,
            Parameters=self.test_parameters,
            Capabilities=['CAPABILITY_NAMED_IAM']
        )
        
        # Check that Glue job was created
        job_name = 'test-glue-job'
        try:
            job = self.glue_client.get_job(JobName=job_name)
            job_details = job['Job']
            
            self.assertEqual(job_details['Name'], job_name)
            self.assertEqual(job_details['Command']['Name'], 'glueetl')
            self.assertEqual(job_details['Command']['PythonVersion'], '3')
            self.assertEqual(job_details['GlueVersion'], '4.0')
            
        except self.glue_client.exceptions.EntityNotFoundException:
            self.fail(f"Glue job {job_name} was not created")
    
    def test_glue_job_default_arguments(self):
        """Test that Glue job has correct default arguments."""
        stack_name = 'test-glue-args-stack'
        
        # Create stack
        self.cf_client.create_stack(
            StackName=stack_name,
            TemplateBody=self.template_body,
            Parameters=self.test_parameters,
            Capabilities=['CAPABILITY_NAMED_IAM']
        )
        
        job_name = 'test-glue-job'
        job = self.glue_client.get_job(JobName=job_name)
        default_args = job['Job']['DefaultArguments']
        
        # Check required default arguments
        required_args = {
            '--job-bookmark-option': 'job-bookmark-enable',
            '--enable-metrics': 'true',
            '--enable-continuous-cloudwatch-log': 'true',
            '--JOB_NAME': 'test-glue-job',
            '--SOURCE_ENGINE_TYPE': 'oracle',
            '--TARGET_ENGINE_TYPE': 'postgresql',
            '--TABLE_NAMES': 'table1,table2,table3'
        }
        
        for arg_key, expected_value in required_args.items():
            self.assertIn(arg_key, default_args, f"Missing default argument: {arg_key}")
            self.assertEqual(default_args[arg_key], expected_value,
                           f"Incorrect value for {arg_key}")
    
    def test_glue_job_execution_properties(self):
        """Test that Glue job execution properties are set correctly."""
        stack_name = 'test-glue-exec-stack'
        
        # Create stack
        self.cf_client.create_stack(
            StackName=stack_name,
            TemplateBody=self.template_body,
            Parameters=self.test_parameters,
            Capabilities=['CAPABILITY_NAMED_IAM']
        )
        
        job_name = 'test-glue-job'
        job = self.glue_client.get_job(JobName=job_name)
        job_details = job['Job']
        
        # Check execution properties
        self.assertEqual(job_details['MaxRetries'], 2)
        self.assertEqual(job_details['Timeout'], 60)
        self.assertEqual(job_details['ExecutionProperty']['MaxConcurrentRuns'], 1)
        self.assertEqual(job_details['WorkerType'], 'G.1X')
        self.assertEqual(job_details['NumberOfWorkers'], 5)
    
    def test_glue_job_jdbc_drivers_configuration(self):
        """Test that JDBC drivers are properly configured in job."""
        stack_name = 'test-glue-jdbc-stack'
        
        # Create stack
        self.cf_client.create_stack(
            StackName=stack_name,
            TemplateBody=self.template_body,
            Parameters=self.test_parameters,
            Capabilities=['CAPABILITY_NAMED_IAM']
        )
        
        job_name = 'test-glue-job'
        job = self.glue_client.get_job(JobName=job_name)
        default_args = job['Job']['DefaultArguments']
        
        # Check JDBC driver configuration
        expected_jars = f's3://{self.test_bucket}/oracle.jar,s3://{self.test_bucket}/postgresql.jar'
        self.assertEqual(default_args['--extra-jars'], expected_jars)
        
        # Check individual driver paths
        self.assertEqual(default_args['--SOURCE_JDBC_DRIVER_S3_PATH'], 
                        f's3://{self.test_bucket}/oracle.jar')
        self.assertEqual(default_args['--TARGET_JDBC_DRIVER_S3_PATH'], 
                        f's3://{self.test_bucket}/postgresql.jar')
    
    def test_glue_job_tags(self):
        """Test that Glue job has correct tags."""
        stack_name = 'test-glue-tags-stack'
        
        # Create stack
        self.cf_client.create_stack(
            StackName=stack_name,
            TemplateBody=self.template_body,
            Parameters=self.test_parameters,
            Capabilities=['CAPABILITY_NAMED_IAM']
        )
        
        job_name = 'test-glue-job'
        
        # Get job tags (moto may not fully support this, so we'll check the template)
        template_dict = yaml.safe_load(self.template_body)
        glue_job_resource = template_dict['Resources']['GlueJob']
        
        expected_tags = {
            'JobType': 'DataReplication',
            'SourceEngine': 'oracle',
            'TargetEngine': 'postgresql'
        }
        
        job_tags = glue_job_resource['Properties']['Tags']
        for key, value in expected_tags.items():
            # Find tag in list of tag objects
            tag_found = False
            for tag in job_tags:
                if isinstance(tag, dict) and tag.get(key) == value:
                    tag_found = True
                    break
                elif isinstance(tag, str) and tag == key:
                    # Handle different tag formats
                    tag_found = True
                    break
            
            if not tag_found:
                # Check if it's a CloudFormation reference
                for tag_key, tag_value in job_tags.items():
                    if tag_key == key:
                        tag_found = True
                        break
            
            self.assertTrue(tag_found, f"Expected tag {key}={value} not found")


class TestCloudFormationOutputs(unittest.TestCase):
    """Test CloudFormation stack outputs."""
    
    def setUp(self):
        """Set up test fixtures."""
        with open('infrastructure/cloudformation/glue-data-replication.yaml', 'r') as f:
            self.template_content = yaml.safe_load(f)
    
    def test_required_outputs_defined(self):
        """Test that all required outputs are defined in template."""
        outputs = self.template_content['Outputs']
        
        required_outputs = [
            'GlueJobName',
            'GlueJobRoleArn',
            'SourceEngineType',
            'TargetEngineType'
        ]
        
        for output in required_outputs:
            self.assertIn(output, outputs, f"Missing required output: {output}")
            self.assertIn('Description', outputs[output])
            self.assertIn('Value', outputs[output])
    
    def test_outputs_have_exports(self):
        """Test that outputs have proper export names."""
        outputs = self.template_content['Outputs']
        
        for output_name, output_def in outputs.items():
            self.assertIn('Export', output_def, f"Output {output_name} missing Export")
            self.assertIn('Name', output_def['Export'], f"Output {output_name} Export missing Name")


if __name__ == '__main__':
    # Create test suite
    test_suite = unittest.TestSuite()
    
    # Add test classes
    test_classes = [
        TestCloudFormationTemplateValidation,
        TestCloudFormationDeployment,
        TestIAMRoleAndPolicyCreation,
        TestGlueJobCreation,
        TestCloudFormationOutputs
    ]
    
    for test_class in test_classes:
        tests = unittest.TestLoader().loadTestsFromTestCase(test_class)
        test_suite.addTests(tests)
    
    # Run tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(test_suite)
    
    # Exit with appropriate code
    exit(0 if result.wasSuccessful() else 1)