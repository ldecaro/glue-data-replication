
#!/usr/bin/env python3
"""
Test runner for AWS Glue Data Replication unit tests.

This script runs all unit tests for the modular PySpark job components and provides
detailed reporting on test results.
"""

import sys
import unittest
import os
from io import StringIO

# Add src directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'src'))

def run_tests():
    """Run all unit tests and provide detailed reporting."""
    
    print("="*80)
    print("AWS Glue Data Replication - Unit Test Suite (Modular)")
    print("="*80)
    print()
    
    # Import and run the comprehensive test suite
    try:
        from test_suite import run_comprehensive_tests, validate_module_imports
        
        # First validate imports
        if validate_module_imports():
            # Then run comprehensive tests
            return run_comprehensive_tests()
        else:
            print("❌ Module import validation failed")
            return False
            
    except ImportError as e:
        print(f"❌ Failed to import test suite: {e}")
        print("Falling back to basic test discovery...")
        
        # Fallback to basic test discovery
        loader = unittest.TestLoader()
        start_dir = os.path.dirname(os.path.abspath(__file__))
        suite = loader.discover(start_dir, pattern='test_*.py')
        
        runner = unittest.TextTestRunner(verbosity=2)
        result = runner.run(suite)
        
        return result.wasSuccessful()
    

if __name__ == '__main__':
    success = run_tests()
    if success:
        print("\n✅ All tests completed successfully!")
        sys.exit(0)
    else:
        print("\n❌ Some tests failed. Please review and fix.")
        sys.exit(1)