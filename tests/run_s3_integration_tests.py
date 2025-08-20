#!/usr/bin/env python3
"""
Test runner for S3 Bookmark Integration Tests - Task 15

This script runs the comprehensive integration tests for S3 bookmark persistence.
It includes setup validation, test execution, and cleanup procedures.

Usage:
    python run_s3_integration_tests.py [--bucket BUCKET_NAME] [--region REGION] [--verbose]

Requirements:
- AWS credentials configured (via AWS CLI, IAM role, or environment variables)
- S3 bucket access permissions (read, write, list, delete)
- pytest installed
"""

import os
import sys
import argparse
import boto3
import subprocess
from botocore.exceptions import ClientError, NoCredentialsError


def check_aws_credentials():
    """Check if AWS credentials are properly configured."""
    try:
        # Try to get caller identity
        sts = boto3.client('sts')
        identity = sts.get_caller_identity()
        print(f"✓ AWS credentials configured for account: {identity['Account']}")
        print(f"  User/Role ARN: {identity['Arn']}")
        return True
    except NoCredentialsError:
        print("✗ AWS credentials not found")
        print("  Please configure AWS credentials using one of:")
        print("  - AWS CLI: aws configure")
        print("  - Environment variables: AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY")
        print("  - IAM role (if running on EC2/Lambda/Glue)")
        return False
    except Exception as e:
        print(f"✗ Error checking AWS credentials: {e}")
        return False


def check_s3_permissions(bucket_name, region=None):
    """Check if S3 permissions are sufficient for testing."""
    try:
        if region:
            s3_client = boto3.client('s3', region_name=region)
        else:
            s3_client = boto3.client('s3')
        
        # Check if bucket exists or can be created
        try:
            s3_client.head_bucket(Bucket=bucket_name)
            print(f"✓ S3 bucket '{bucket_name}' exists and is accessible")
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == '404':
                print(f"! S3 bucket '{bucket_name}' does not exist - tests will attempt to create it")
            elif error_code == '403':
                print(f"✗ Access denied to S3 bucket '{bucket_name}'")
                return False
            else:
                print(f"✗ Error accessing S3 bucket '{bucket_name}': {e}")
                return False
        
        # Test basic S3 operations with a test key
        test_key = "integration-test-permissions-check.json"
        test_content = '{"test": "permissions"}'
        
        try:
            # Test write
            s3_client.put_object(
                Bucket=bucket_name,
                Key=test_key,
                Body=test_content,
                ContentType='application/json'
            )
            
            # Test read
            s3_client.get_object(Bucket=bucket_name, Key=test_key)
            
            # Test list
            s3_client.list_objects_v2(Bucket=bucket_name, Prefix="integration-test", MaxKeys=1)
            
            # Test delete
            s3_client.delete_object(Bucket=bucket_name, Key=test_key)
            
            print("✓ S3 permissions verified (read, write, list, delete)")
            return True
            
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == 'AccessDenied':
                print(f"✗ Insufficient S3 permissions for bucket '{bucket_name}'")
                print("  Required permissions: s3:GetObject, s3:PutObject, s3:DeleteObject, s3:ListBucket")
            else:
                print(f"✗ S3 operation failed: {e}")
            return False
            
    except Exception as e:
        print(f"✗ Error checking S3 permissions: {e}")
        return False


def check_dependencies():
    """Check if required Python dependencies are installed."""
    required_packages = ['pytest', 'boto3', 'botocore']
    missing_packages = []
    
    for package in required_packages:
        try:
            __import__(package)
        except ImportError:
            missing_packages.append(package)
    
    if missing_packages:
        print(f"✗ Missing required packages: {', '.join(missing_packages)}")
        print("  Install with: pip install " + " ".join(missing_packages))
        return False
    else:
        print("✓ All required Python packages are installed")
        return True


def run_integration_tests(bucket_name, region=None, verbose=False):
    """Run the S3 bookmark integration tests."""
    # Set environment variables for tests
    env = os.environ.copy()
    env['TEST_S3_BUCKET'] = bucket_name
    if region:
        env['AWS_DEFAULT_REGION'] = region
    
    # Prepare pytest command
    pytest_cmd = [
        sys.executable, '-m', 'pytest',
        'test_s3_bookmark_integration_complete.py',
        '-v' if verbose else '-q',
        '--tb=short',
        '--durations=10'
    ]
    
    if verbose:
        pytest_cmd.extend(['-s', '--capture=no'])
    
    print(f"Running integration tests with bucket: {bucket_name}")
    if region:
        print(f"Using AWS region: {region}")
    
    try:
        # Run pytest
        result = subprocess.run(
            pytest_cmd,
            env=env,
            cwd=os.path.dirname(os.path.abspath(__file__)),
            timeout=1800  # 30 minute timeout
        )
        
        if result.returncode == 0:
            print("\n✓ All integration tests passed successfully!")
            return True
        else:
            print(f"\n✗ Integration tests failed with exit code: {result.returncode}")
            return False
            
    except subprocess.TimeoutExpired:
        print("\n✗ Integration tests timed out after 30 minutes")
        return False
    except Exception as e:
        print(f"\n✗ Error running integration tests: {e}")
        return False


def cleanup_test_resources(bucket_name, region=None):
    """Clean up any remaining test resources."""
    try:
        if region:
            s3_client = boto3.client('s3', region_name=region)
        else:
            s3_client = boto3.client('s3')
        
        # List and delete test objects
        prefixes_to_clean = [
            'integration-tests/',
            'persistence-tests/',
            'corruption-tests/',
            'performance-tests/'
        ]
        
        total_deleted = 0
        for prefix in prefixes_to_clean:
            try:
                response = s3_client.list_objects_v2(
                    Bucket=bucket_name,
                    Prefix=prefix
                )
                
                if 'Contents' in response:
                    objects_to_delete = [{'Key': obj['Key']} for obj in response['Contents']]
                    if objects_to_delete:
                        s3_client.delete_objects(
                            Bucket=bucket_name,
                            Delete={'Objects': objects_to_delete}
                        )
                        total_deleted += len(objects_to_delete)
            except ClientError:
                # Ignore errors during cleanup
                pass
        
        if total_deleted > 0:
            print(f"✓ Cleaned up {total_deleted} test objects from S3")
        else:
            print("✓ No test objects found to clean up")
            
    except Exception as e:
        print(f"! Warning: Failed to clean up test resources: {e}")


def main():
    """Main function to run integration tests."""
    parser = argparse.ArgumentParser(
        description='Run S3 Bookmark Integration Tests',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python run_s3_integration_tests.py
  python run_s3_integration_tests.py --bucket my-test-bucket
  python run_s3_integration_tests.py --bucket my-test-bucket --region us-west-2 --verbose
        """
    )
    
    parser.add_argument(
        '--bucket',
        default=None,
        help='S3 bucket name for testing (default: auto-generated)'
    )
    
    parser.add_argument(
        '--region',
        default=None,
        help='AWS region (default: from AWS configuration)'
    )
    
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Enable verbose output'
    )
    
    parser.add_argument(
        '--cleanup-only',
        action='store_true',
        help='Only perform cleanup of test resources'
    )
    
    args = parser.parse_args()
    
    # Generate bucket name if not provided
    if not args.bucket:
        import uuid
        test_id = str(uuid.uuid4())[:8]
        args.bucket = f'glue-s3-bookmark-integration-test-{test_id}'
    
    print("S3 Bookmark Integration Test Runner")
    print("=" * 50)
    
    if args.cleanup_only:
        print("Performing cleanup only...")
        cleanup_test_resources(args.bucket, args.region)
        return
    
    # Pre-flight checks
    print("Performing pre-flight checks...")
    
    if not check_dependencies():
        sys.exit(1)
    
    if not check_aws_credentials():
        sys.exit(1)
    
    if not check_s3_permissions(args.bucket, args.region):
        sys.exit(1)
    
    print("\n" + "=" * 50)
    print("Starting integration tests...")
    
    try:
        # Run tests
        success = run_integration_tests(args.bucket, args.region, args.verbose)
        
        # Cleanup
        print("\nCleaning up test resources...")
        cleanup_test_resources(args.bucket, args.region)
        
        if success:
            print("\n🎉 Integration tests completed successfully!")
            sys.exit(0)
        else:
            print("\n❌ Integration tests failed!")
            sys.exit(1)
            
    except KeyboardInterrupt:
        print("\n\nTest execution interrupted by user")
        print("Cleaning up test resources...")
        cleanup_test_resources(args.bucket, args.region)
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        print("Cleaning up test resources...")
        cleanup_test_resources(args.bucket, args.region)
        sys.exit(1)


if __name__ == "__main__":
    main()