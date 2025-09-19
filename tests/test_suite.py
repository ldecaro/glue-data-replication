#!/usr/bin/env python3
"""
Comprehensive test suite for AWS Glue Data Replication modular architecture.

This test runner consolidates all unit tests for the refactored modular structure
and provides detailed reporting on test results.
"""

import sys
import unittest
import os
from io import StringIO

# Add src directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'src'))

def run_comprehensive_tests():
    """Run comprehensive test suite for all modules."""
    
    print("="*80)
    print("AWS Glue Data Replication - Comprehensive Test Suite")
    print("Modular Architecture Validation")
    print("="*80)
    print()
    
    # Test modules to run
    test_modules = [
        'test_config',
        'test_storage', 
        'test_database',
        'test_monitoring',
        'test_network',
        'test_utils'
    ]
    
    # Discover and load tests
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    
    # Add tests from each module
    for module_name in test_modules:
        try:
            module_suite = loader.loadTestsFromName(module_name)
            suite.addTest(module_suite)
            print(f"✓ Loaded tests from {module_name}")
        except Exception as e:
            print(f"✗ Failed to load tests from {module_name}: {e}")
    
    print()
    print("Running tests...")
    print("-" * 80)
    
    # Run tests with detailed output
    runner = unittest.TextTestRunner(
        verbosity=2,
        stream=sys.stdout,
        buffer=True
    )
    
    result = runner.run(suite)
    
    # Print summary
    print()
    print("="*80)
    print("TEST SUMMARY")
    print("="*80)
    print(f"Tests run: {result.testsRun}")
    print(f"Failures: {len(result.failures)}")
    print(f"Errors: {len(result.errors)}")
    print(f"Skipped: {len(result.skipped) if hasattr(result, 'skipped') else 0}")
    
    if result.failures:
        print("\nFAILURES:")
        for test, traceback in result.failures:
            print(f"- {test}: {traceback.split('AssertionError:')[-1].strip() if 'AssertionError:' in traceback else 'See details above'}")
    
    if result.errors:
        print("\nERRORS:")
        for test, traceback in result.errors:
            print(f"- {test}: {traceback.split('Exception:')[-1].strip() if 'Exception:' in traceback else 'See details above'}")
    
    success_rate = ((result.testsRun - len(result.failures) - len(result.errors)) / result.testsRun * 100) if result.testsRun > 0 else 0
    print(f"\nSuccess Rate: {success_rate:.1f}%")
    
    if result.wasSuccessful():
        print("\n🎉 All tests passed! Modular architecture is working correctly.")
        return True
    else:
        print(f"\n❌ {len(result.failures) + len(result.errors)} test(s) failed. Please review and fix issues.")
        return False


def run_integration_tests():
    """Run integration tests for cross-module functionality."""
    
    print("\n" + "="*80)
    print("INTEGRATION TESTS")
    print("="*80)
    
    # Integration test modules
    integration_modules = [
        'test_s3_bookmark_integration_complete',
        'test_job_bookmark_manager'
    ]
    
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    
    for module_name in integration_modules:
        try:
            module_suite = loader.loadTestsFromName(module_name)
            suite.addTest(module_suite)
            print(f"✓ Loaded integration tests from {module_name}")
        except Exception as e:
            print(f"✗ Failed to load integration tests from {module_name}: {e}")
    
    if suite.countTestCases() > 0:
        print("\nRunning integration tests...")
        print("-" * 80)
        
        runner = unittest.TextTestRunner(
            verbosity=2,
            stream=sys.stdout,
            buffer=True
        )
        
        result = runner.run(suite)
        
        print(f"\nIntegration Tests - Run: {result.testsRun}, "
              f"Failures: {len(result.failures)}, "
              f"Errors: {len(result.errors)}")
        
        return result.wasSuccessful()
    else:
        print("No integration tests found.")
        return True


def validate_module_imports():
    """Validate that all modules can be imported correctly."""
    
    print("\n" + "="*80)
    print("MODULE IMPORT VALIDATION")
    print("="*80)
    
    modules_to_test = [
        ('glue_job.config', ['JobConfig', 'ConnectionConfig', 'DatabaseEngineManager']),
        ('glue_job.storage', ['S3BookmarkStorage', 'JobBookmarkManager']),
        ('glue_job.database', ['JdbcConnectionManager', 'SchemaCompatibilityValidator']),
        ('glue_job.monitoring', ['StructuredLogger', 'CloudWatchMetricsPublisher']),
        ('glue_job.network', ['NetworkErrorHandler', 'ConnectionRetryHandler']),
        ('glue_job.utils', ['S3PathUtilities', 'EnhancedS3ParallelOperations'])
    ]
    
    all_imports_successful = True
    
    for module_name, classes in modules_to_test:
        try:
            module = __import__(module_name, fromlist=classes)
            for class_name in classes:
                if hasattr(module, class_name):
                    print(f"✓ {module_name}.{class_name}")
                else:
                    print(f"✗ {module_name}.{class_name} - Class not found")
                    all_imports_successful = False
        except ImportError as e:
            print(f"✗ {module_name} - Import failed: {e}")
            all_imports_successful = False
        except Exception as e:
            print(f"✗ {module_name} - Unexpected error: {e}")
            all_imports_successful = False
    
    if all_imports_successful:
        print("\n🎉 All module imports successful!")
    else:
        print("\n❌ Some module imports failed. Please check the modular structure.")
    
    return all_imports_successful


if __name__ == '__main__':
    print("Starting comprehensive test validation...")
    
    # Step 1: Validate module imports
    imports_ok = validate_module_imports()
    
    # Step 2: Run unit tests
    if imports_ok:
        unit_tests_ok = run_comprehensive_tests()
        
        # Step 3: Run integration tests
        if unit_tests_ok:
            integration_tests_ok = run_integration_tests()
            
            if unit_tests_ok and integration_tests_ok:
                print("\n" + "="*80)
                print("🎉 ALL TESTS PASSED!")
                print("The modular architecture is working correctly.")
                print("Task 11 implementation is complete and successful.")
                print("="*80)
                sys.exit(0)
            else:
                print("\n" + "="*80)
                print("❌ SOME TESTS FAILED")
                print("Please review and fix the failing tests.")
                print("="*80)
                sys.exit(1)
        else:
            print("\n❌ Unit tests failed. Skipping integration tests.")
            sys.exit(1)
    else:
        print("\n❌ Module imports failed. Cannot run tests.")
        sys.exit(1)