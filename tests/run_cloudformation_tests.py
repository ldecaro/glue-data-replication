#!/usr/bin/env python3
"""
Test runner for CloudFormation integration tests.

This script runs the CloudFormation deployment integration tests
and provides detailed output about test results.
"""

import sys
import unittest
import os

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def run_cloudformation_tests():
    """Run CloudFormation integration tests."""
    print("=" * 70)
    print("AWS Glue Data Replication - CloudFormation Integration Tests")
    print("=" * 70)
    print()
    
    # Import test module
    try:
        import test_cloudformation_integration
    except ImportError as e:
        print(f"Error importing test module: {e}")
        print("Make sure all dependencies are installed:")
        print("pip install -r requirements-test.txt")
        return False
    
    # Create test suite
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    
    # Add all test classes
    test_classes = [
        test_cloudformation_integration.TestCloudFormationTemplateValidation,
        test_cloudformation_integration.TestCloudFormationDeployment,
        test_cloudformation_integration.TestIAMRoleAndPolicyCreation,
        test_cloudformation_integration.TestGlueJobCreation,
        test_cloudformation_integration.TestCloudFormationOutputs
    ]
    
    for test_class in test_classes:
        tests = loader.loadTestsFromTestCase(test_class)
        suite.addTests(tests)
    
    # Run tests with detailed output
    runner = unittest.TextTestRunner(
        verbosity=2,
        stream=sys.stdout,
        descriptions=True,
        failfast=False
    )
    
    print("Running CloudFormation integration tests...")
    print()
    
    result = runner.run(suite)
    
    # Print summary
    print()
    print("=" * 70)
    print("Test Summary")
    print("=" * 70)
    print(f"Tests run: {result.testsRun}")
    print(f"Failures: {len(result.failures)}")
    print(f"Errors: {len(result.errors)}")
    print(f"Skipped: {len(result.skipped) if hasattr(result, 'skipped') else 0}")
    
    if result.failures:
        print("\nFailures:")
        for test, traceback in result.failures:
            print(f"  - {test}: {traceback.split('AssertionError:')[-1].strip()}")
    
    if result.errors:
        print("\nErrors:")
        for test, traceback in result.errors:
            print(f"  - {test}: {traceback.split('Exception:')[-1].strip()}")
    
    success = result.wasSuccessful()
    print(f"\nResult: {'PASSED' if success else 'FAILED'}")
    print("=" * 70)
    
    return success

if __name__ == '__main__':
    success = run_cloudformation_tests()
    sys.exit(0 if success else 1)