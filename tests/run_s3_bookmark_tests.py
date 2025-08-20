#!/usr/bin/env python3
"""
Test runner for S3BookmarkStorage comprehensive unit tests.

This script runs the comprehensive unit test suite for S3BookmarkStorage class
covering all error scenarios, retry logic, and data type handling.
"""

import sys
import os
import subprocess
import pytest

def main():
    """Run the S3BookmarkStorage unit tests."""
    print("=" * 80)
    print("Running S3BookmarkStorage Comprehensive Unit Tests")
    print("=" * 80)
    
    # Add the current directory to Python path for imports
    current_dir = os.path.dirname(os.path.abspath(__file__))
    if current_dir not in sys.path:
        sys.path.insert(0, current_dir)
    
    # Check if pytest is available
    try:
        import pytest
    except ImportError:
        print("ERROR: pytest is not installed. Installing...")
        subprocess.check_call([sys.executable, "-m", "pip", "install", "pytest", "pytest-asyncio"])
        import pytest
    
    # Check if required dependencies are available
    try:
        import boto3
        import botocore
    except ImportError:
        print("ERROR: boto3/botocore not installed. Installing...")
        subprocess.check_call([sys.executable, "-m", "pip", "install", "boto3", "botocore"])
    
    # Run the tests with verbose output
    test_file = os.path.join(current_dir, "test_s3_bookmark_storage.py")
    
    if not os.path.exists(test_file):
        print(f"ERROR: Test file not found: {test_file}")
        return 1
    
    print(f"Running tests from: {test_file}")
    print()
    
    # Run pytest with specific options
    pytest_args = [
        test_file,
        "-v",  # Verbose output
        "-s",  # Don't capture output
        "--tb=short",  # Short traceback format
        "--asyncio-mode=auto",  # Auto-detect async tests
        "--color=yes"  # Colored output
    ]
    
    try:
        exit_code = pytest.main(pytest_args)
        
        print()
        print("=" * 80)
        if exit_code == 0:
            print("✅ All S3BookmarkStorage tests PASSED!")
        else:
            print("❌ Some S3BookmarkStorage tests FAILED!")
        print("=" * 80)
        
        return exit_code
        
    except Exception as e:
        print(f"ERROR running tests: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main())