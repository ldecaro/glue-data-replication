#!/usr/bin/env python3
"""
Simple test runner for network connectivity tests
"""

import sys
import os
import unittest
from unittest.mock import MagicMock

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Mock PySpark and AWS Glue imports before importing our modules
mock_modules = [
    'awsglue', 'awsglue.utils', 'awsglue.context', 'awsglue.job',
    'pyspark', 'pyspark.context', 'pyspark.sql', 'pyspark.sql.types',
    'pyspark.sql.functions'
]

for module in mock_modules:
    sys.modules[module] = MagicMock()

print("Setting up test environment...")

try:
    # Import test classes
    from test_network_connectivity import (
        TestNetworkConfigurationParsing,
        TestConnectionConfigNetworkIntegration,
        TestGlueConnectionManager,
        TestCrossVPCConnectivityScenarios,
        TestSecurityGroupValidation,
        TestNetworkErrorHandling
    )
    
    print("✓ Test classes imported successfully")
    
    # Create test suite
    test_suite = unittest.TestSuite()
    
    # Add test classes
    test_classes = [
        TestNetworkConfigurationParsing,
        TestConnectionConfigNetworkIntegration,
        TestGlueConnectionManager,
        TestCrossVPCConnectivityScenarios,
        TestSecurityGroupValidation,
        TestNetworkErrorHandling
    ]
    
    for test_class in test_classes:
        tests = unittest.TestLoader().loadTestsFromTestCase(test_class)
        test_suite.addTests(tests)
        print(f"✓ Added tests from {test_class.__name__}")
    
    print(f"\nRunning {test_suite.countTestCases()} network connectivity tests...")
    print("=" * 60)
    
    # Run tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(test_suite)
    
    # Print summary
    print(f"\n{'='*60}")
    print("NETWORK CONNECTIVITY TESTS SUMMARY")
    print(f"{'='*60}")
    print(f"Tests run: {result.testsRun}")
    print(f"Failures: {len(result.failures)}")
    print(f"Errors: {len(result.errors)}")
    
    if result.testsRun > 0:
        success_rate = ((result.testsRun - len(result.failures) - len(result.errors)) / result.testsRun * 100)
        print(f"Success rate: {success_rate:.1f}%")
    
    if result.failures:
        print(f"\nFailures:")
        for test, traceback in result.failures:
            print(f"- {test}")
    
    if result.errors:
        print(f"\nErrors:")
        for test, traceback in result.errors:
            print(f"- {test}")
    
    print(f"\n{'='*60}")
    
    # Exit with appropriate code
    sys.exit(0 if result.wasSuccessful() else 1)
    
except Exception as e:
    print(f"✗ Error setting up tests: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)