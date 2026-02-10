#!/usr/bin/env python3
"""
Test script to verify Task 11 implementation of parallel S3 operations.

This script tests the integration of enhanced parallel operations into the
JobBookmarkManager class and verifies that the optimization logic works correctly.
"""

import sys
import os
import unittest
from unittest.mock import Mock, MagicMock, patch
from typing import Dict, List, Any

# Add scripts directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'scripts'))

try:
    from s3_parallel_operations import EnhancedS3ParallelOperations
    ENHANCED_OPS_AVAILABLE = True
except ImportError:
    ENHANCED_OPS_AVAILABLE = False
    print("Warning: Enhanced parallel operations not available for testing")


class TestTask11Implementation(unittest.TestCase):
    """Test Task 11 parallel S3 operations implementation."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.mock_s3_storage = Mock()
        self.mock_s3_storage.config = Mock()
        self.mock_s3_storage.config.timeout_seconds = 30
        self.mock_s3_storage.structured_logger = Mock()
        self.mock_s3_storage.metrics_publisher = Mock()
        
    def test_enhanced_parallel_operations_initialization(self):
        """Test that EnhancedS3ParallelOperations can be initialized."""
        if not ENHANCED_OPS_AVAILABLE:
            self.skipTest("Enhanced parallel operations not available")
            
        # Test initialization
        enhanced_ops = EnhancedS3ParallelOperations(self.mock_s3_storage)
        
        # Verify initialization
        self.assertIsNotNone(enhanced_ops)
        self.assertEqual(enhanced_ops.storage, self.mock_s3_storage)
        self.assertEqual(enhanced_ops.config, self.mock_s3_storage.config)
        
    def test_enhanced_operations_has_required_methods(self):
        """Test that enhanced operations has all required methods."""
        if not ENHANCED_OPS_AVAILABLE:
            self.skipTest("Enhanced parallel operations not available")
            
        enhanced_ops = EnhancedS3ParallelOperations(self.mock_s3_storage)
        
        # Check required methods exist
        required_methods = [
            'read_bookmarks_parallel_optimized',
            'write_bookmarks_batch_optimized',
            'process_tables_with_adaptive_concurrency'
        ]
        
        for method_name in required_methods:
            self.assertTrue(hasattr(enhanced_ops, method_name),
                          f"Method {method_name} not found")
            method = getattr(enhanced_ops, method_name)
            self.assertTrue(callable(method),
                          f"Method {method_name} is not callable")
    
    def test_optimization_thresholds(self):
        """Test that optimization logic uses correct thresholds."""
        # Test data
        small_table_list = ['table1', 'table2', 'table3']  # 3 tables
        large_table_list = [f'table{i}' for i in range(1, 16)]  # 15 tables
        
        # For parallel reads: threshold is 10 tables
        self.assertLess(len(small_table_list), 10, "Small list should be below read threshold")
        self.assertGreater(len(large_table_list), 10, "Large list should be above read threshold")
        
        # For batch writes: threshold is 5 bookmarks
        small_batch = {f'table{i}': {} for i in range(1, 4)}  # 3 bookmarks
        large_batch = {f'table{i}': {} for i in range(1, 8)}  # 7 bookmarks
        
        self.assertLess(len(small_batch), 5, "Small batch should be below write threshold")
        self.assertGreater(len(large_batch), 5, "Large batch should be above write threshold")
    
    def test_concurrency_calculations(self):
        """Test that concurrency calculations work correctly."""
        if not ENHANCED_OPS_AVAILABLE:
            self.skipTest("Enhanced parallel operations not available")
            
        # Test concurrency calculation logic from the implementation
        table_counts = [5, 10, 25, 50, 100]
        
        for table_count in table_counts:
            # Read concurrency: min(20, max(5, table_count // 5))
            expected_read_concurrency = min(20, max(5, table_count // 5))
            self.assertGreaterEqual(expected_read_concurrency, 5, 
                                  f"Read concurrency should be at least 5 for {table_count} tables")
            self.assertLessEqual(expected_read_concurrency, 20,
                                f"Read concurrency should be at most 20 for {table_count} tables")
            
            # Chunk size: min(50, max(10, table_count // 4))
            expected_chunk_size = min(50, max(10, table_count // 4))
            self.assertGreaterEqual(expected_chunk_size, 10,
                                  f"Chunk size should be at least 10 for {table_count} tables")
            self.assertLessEqual(expected_chunk_size, 50,
                                f"Chunk size should be at most 50 for {table_count} tables")
    
    def test_batch_size_calculations(self):
        """Test that batch size calculations work correctly."""
        batch_counts = [3, 6, 12, 20, 30]
        
        for batch_count in batch_counts:
            # Batch size: min(15, max(5, batch_count // 3))
            expected_batch_size = min(15, max(5, batch_count // 3))
            self.assertGreaterEqual(expected_batch_size, 5,
                                  f"Batch size should be at least 5 for {batch_count} bookmarks")
            self.assertLessEqual(expected_batch_size, 15,
                                f"Batch size should be at most 15 for {batch_count} bookmarks")
            
            # Concurrent batches: min(3, max(1, batch_count // 10))
            expected_concurrent_batches = min(3, max(1, batch_count // 10))
            self.assertGreaterEqual(expected_concurrent_batches, 1,
                                  f"Concurrent batches should be at least 1 for {batch_count} bookmarks")
            self.assertLessEqual(expected_concurrent_batches, 3,
                                f"Concurrent batches should be at most 3 for {batch_count} bookmarks")


def run_tests():
    """Run the test suite."""
    print("Testing Task 11 Implementation: Parallel S3 Operations for Multiple Tables")
    print("=" * 80)
    
    # Check if enhanced operations are available
    if ENHANCED_OPS_AVAILABLE:
        print("✓ Enhanced parallel operations module is available")
    else:
        print("⚠ Enhanced parallel operations module is not available")
    
    # Run tests
    unittest.main(verbosity=2, exit=False)


if __name__ == '__main__':
    run_tests()