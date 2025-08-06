#!/usr/bin/env python3
"""
Minimal test to verify test framework is working.
"""

import unittest
import sys
import os

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

class TestMinimal(unittest.TestCase):
    """Minimal test case."""
    
    def test_basic_assertion(self):
        """Test basic assertion."""
        self.assertEqual(1 + 1, 2)
        print("Basic test passed!")
    
    def test_string_operations(self):
        """Test string operations."""
        test_string = "hello world"
        self.assertIn("world", test_string)
        self.assertEqual(test_string.upper(), "HELLO WORLD")

if __name__ == '__main__':
    print("Running minimal tests...")
    unittest.main(verbosity=2)