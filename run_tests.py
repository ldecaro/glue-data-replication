#!/usr/bin/env python3
"""
Main test runner for AWS Glue Data Replication project.

This script provides a convenient way to run different types of tests
from the project root directory.
"""

import sys
import os
import subprocess
import argparse

def run_unit_tests():
    """Run unit tests."""
    print("Running unit tests...")
    result = subprocess.run([sys.executable, "tests/run_tests.py"], cwd=os.getcwd())
    return result.returncode

def run_cloudformation_tests():
    """Run CloudFormation integration tests."""
    print("Running CloudFormation integration tests...")
    result = subprocess.run([sys.executable, "tests/run_cloudformation_tests.py"], cwd=os.getcwd())
    return result.returncode

def run_end_to_end_tests(engines=None, scenarios=None):
    """Run end-to-end tests."""
    print("Running end-to-end tests...")
    cmd = [sys.executable, "tests/run_end_to_end_tests.py"]
    
    if engines:
        cmd.extend(["--engines", engines])
    if scenarios:
        cmd.extend(["--scenarios", scenarios])
    
    result = subprocess.run(cmd, cwd=os.getcwd())
    return result.returncode

def run_simple_test():
    """Run simple test to verify setup."""
    print("Running simple test...")
    result = subprocess.run([sys.executable, "tests/simple_test.py"], cwd=os.getcwd())
    return result.returncode

def run_logging_monitoring_test():
    """Run logging and monitoring test."""
    print("Running logging and monitoring test...")
    result = subprocess.run([sys.executable, "tests/test_logging_monitoring.py"], cwd=os.getcwd())
    return result.returncode

def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description='Run tests for AWS Glue Data Replication')
    parser.add_argument('test_type', nargs='?', default='unit',
                       choices=['unit', 'cloudformation', 'e2e', 'simple', 'logging', 'all'],
                       help='Type of tests to run (default: unit)')
    parser.add_argument('--engines', type=str, help='Comma-separated list of engines for e2e tests')
    parser.add_argument('--scenarios', type=str, help='Comma-separated list of scenarios for e2e tests')
    
    args = parser.parse_args()
    
    exit_code = 0
    
    if args.test_type == 'unit':
        exit_code = run_unit_tests()
    elif args.test_type == 'cloudformation':
        exit_code = run_cloudformation_tests()
    elif args.test_type == 'e2e':
        exit_code = run_end_to_end_tests(args.engines, args.scenarios)
    elif args.test_type == 'simple':
        exit_code = run_simple_test()
    elif args.test_type == 'logging':
        exit_code = run_logging_monitoring_test()
    elif args.test_type == 'all':
        print("Running all test suites...\n")
        
        # Run simple test first
        print("1. Simple Test")
        print("-" * 40)
        exit_code = run_simple_test()
        if exit_code != 0:
            print("❌ Simple test failed, stopping execution")
            sys.exit(exit_code)
        print("✅ Simple test passed\n")
        
        # Run unit tests
        print("2. Unit Tests")
        print("-" * 40)
        exit_code = run_unit_tests()
        if exit_code != 0:
            print("❌ Unit tests failed")
        else:
            print("✅ Unit tests passed")
        print()
        
        # Run logging test
        print("3. Logging and Monitoring Test")
        print("-" * 40)
        logging_exit_code = run_logging_monitoring_test()
        if logging_exit_code != 0:
            print("❌ Logging test failed")
            exit_code = max(exit_code, logging_exit_code)
        else:
            print("✅ Logging test passed")
        print()
        
        # Run CloudFormation tests
        print("4. CloudFormation Integration Tests")
        print("-" * 40)
        cf_exit_code = run_cloudformation_tests()
        if cf_exit_code != 0:
            print("❌ CloudFormation tests failed")
            exit_code = max(exit_code, cf_exit_code)
        else:
            print("✅ CloudFormation tests passed")
        print()
        
        # Run end-to-end tests
        print("5. End-to-End Tests")
        print("-" * 40)
        e2e_exit_code = run_end_to_end_tests()
        if e2e_exit_code != 0:
            print("❌ End-to-end tests failed")
            exit_code = max(exit_code, e2e_exit_code)
        else:
            print("✅ End-to-end tests passed")
        print()
        
        if exit_code == 0:
            print("🎉 All test suites passed!")
        else:
            print("❌ Some test suites failed")
    
    sys.exit(exit_code)

if __name__ == '__main__':
    main()