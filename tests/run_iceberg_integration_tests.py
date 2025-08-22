#!/usr/bin/env python3
"""
Test Runner for Iceberg Integration Tests (Task 11)

This script runs comprehensive integration tests for Iceberg functionality,
including end-to-end replication flows, bookmark management, and error scenarios.

Usage:
    python tests/run_iceberg_integration_tests.py [--verbose] [--test-pattern PATTERN]
"""

import sys
import os
import unittest
import argparse
import logging
from typing import List, Optional

# Add the src directory to the path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def discover_iceberg_integration_tests(test_pattern: Optional[str] = None) -> unittest.TestSuite:
    """Discover and load Iceberg integration tests.
    
    Args:
        test_pattern: Optional pattern to filter test methods
        
    Returns:
        unittest.TestSuite: Test suite containing discovered tests
    """
    # Import test modules
    try:
        from test_iceberg_integration_e2e import TestIcebergIntegrationE2E
        
        # Create test suite
        suite = unittest.TestSuite()
        
        # Add all test methods from the integration test class
        test_methods = [
            'test_iceberg_table_creation_with_real_catalog',
            'test_complete_replication_traditional_to_iceberg',
            'test_replication_iceberg_to_traditional',
            'test_bookmark_management_with_identifier_field_ids',
            'test_error_scenarios_and_recovery',
            'test_cross_account_catalog_access',
            'test_incremental_replication_with_bookmarks'
        ]
        
        for method_name in test_methods:
            if test_pattern is None or test_pattern.lower() in method_name.lower():
                suite.addTest(TestIcebergIntegrationE2E(method_name))
        
        return suite
        
    except ImportError as e:
        logger.error(f"Failed to import Iceberg integration tests: {e}")
        return unittest.TestSuite()


def run_integration_tests(verbose: bool = False, test_pattern: Optional[str] = None) -> bool:
    """Run Iceberg integration tests.
    
    Args:
        verbose: Enable verbose output
        test_pattern: Optional pattern to filter tests
        
    Returns:
        bool: True if all tests passed, False otherwise
    """
    logger.info("Starting Iceberg Integration Tests (Task 11)")
    logger.info("=" * 60)
    
    # Discover tests
    suite = discover_iceberg_integration_tests(test_pattern)
    
    if suite.countTestCases() == 0:
        logger.warning("No integration tests found to run")
        return False
    
    logger.info(f"Discovered {suite.countTestCases()} integration tests")
    
    # Configure test runner
    verbosity = 2 if verbose else 1
    runner = unittest.TextTestRunner(
        verbosity=verbosity,
        stream=sys.stdout,
        buffer=True
    )
    
    # Run tests
    result = runner.run(suite)
    
    # Print summary
    logger.info("=" * 60)
    logger.info("Integration Test Summary:")
    logger.info(f"Tests run: {result.testsRun}")
    logger.info(f"Failures: {len(result.failures)}")
    logger.info(f"Errors: {len(result.errors)}")
    logger.info(f"Skipped: {len(result.skipped) if hasattr(result, 'skipped') else 0}")
    
    # Print detailed failure information
    if result.failures:
        logger.error("FAILURES:")
        for test, traceback in result.failures:
            logger.error(f"  {test}: {traceback}")
    
    if result.errors:
        logger.error("ERRORS:")
        for test, traceback in result.errors:
            logger.error(f"  {test}: {traceback}")
    
    # Return success status
    success = len(result.failures) == 0 and len(result.errors) == 0
    
    if success:
        logger.info("All integration tests PASSED!")
    else:
        logger.error("Some integration tests FAILED!")
    
    return success


def validate_test_environment() -> bool:
    """Validate that the test environment is properly set up.
    
    Returns:
        bool: True if environment is valid, False otherwise
    """
    logger.info("Validating test environment...")
    
    # Check for required modules
    required_modules = [
        'src.glue_job.config.iceberg_connection_handler',
        'src.glue_job.config.iceberg_schema_manager',
        'src.glue_job.config.iceberg_models',
        'src.glue_job.storage.bookmark_manager',
        'src.glue_job.database.connection_manager'
    ]
    
    missing_modules = []
    for module_name in required_modules:
        try:
            __import__(module_name)
        except ImportError as e:
            missing_modules.append(f"{module_name}: {e}")
    
    if missing_modules:
        logger.error("Missing required modules:")
        for module in missing_modules:
            logger.error(f"  - {module}")
        return False
    
    # Check for test files
    test_files = [
        'tests/test_iceberg_integration_e2e.py'
    ]
    
    missing_files = []
    for test_file in test_files:
        if not os.path.exists(test_file):
            missing_files.append(test_file)
    
    if missing_files:
        logger.error("Missing test files:")
        for file in missing_files:
            logger.error(f"  - {file}")
        return False
    
    logger.info("Test environment validation passed!")
    return True


def main():
    """Main entry point for the test runner."""
    parser = argparse.ArgumentParser(
        description="Run Iceberg Integration Tests (Task 11)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Run all integration tests
    python tests/run_iceberg_integration_tests.py
    
    # Run with verbose output
    python tests/run_iceberg_integration_tests.py --verbose
    
    # Run specific test pattern
    python tests/run_iceberg_integration_tests.py --test-pattern bookmark
    
    # Run with both verbose and pattern filter
    python tests/run_iceberg_integration_tests.py --verbose --test-pattern replication
        """
    )
    
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Enable verbose test output'
    )
    
    parser.add_argument(
        '--test-pattern', '-p',
        type=str,
        help='Pattern to filter test methods (case-insensitive)'
    )
    
    parser.add_argument(
        '--validate-only',
        action='store_true',
        help='Only validate test environment, do not run tests'
    )
    
    args = parser.parse_args()
    
    # Validate environment first
    if not validate_test_environment():
        logger.error("Test environment validation failed!")
        sys.exit(1)
    
    if args.validate_only:
        logger.info("Environment validation completed successfully!")
        sys.exit(0)
    
    # Run integration tests
    success = run_integration_tests(
        verbose=args.verbose,
        test_pattern=args.test_pattern
    )
    
    # Exit with appropriate code
    sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()