#!/usr/bin/env python3
"""
Test runner for JobBookmarkManager unit tests.

This script runs the comprehensive unit tests for the enhanced JobBookmarkManager class
covering S3 bucket extraction, state transitions, fallback mechanisms, and S3 integration.
"""

import sys
import os
import unittest
from unittest.mock import Mock, patch

# Add current directory to path for imports
current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path:
    sys.path.insert(0, current_dir)

# Add scripts directory to path
scripts_path = os.path.join(current_dir, 'scripts')
if scripts_path not in sys.path:
    sys.path.insert(0, scripts_path)

def run_basic_import_test():
    """Test basic imports to ensure modules are available."""
    print("Testing basic imports...")
    
    try:
        # Test importing the test module
        import test_job_bookmark_manager
        print("✓ Successfully imported test_job_bookmark_manager")
        
        # Test importing required classes from the main module
        # Add src directory to path for imports
        sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'src'))
        
        # Mock PySpark and AWS Glue imports for testing
        from unittest.mock import MagicMock
        mock_modules = [
            'awsglue', 'awsglue.utils', 'awsglue.context', 'awsglue.job',
            'pyspark', 'pyspark.context', 'pyspark.sql', 'pyspark.sql.types',
            'pyspark.sql.functions', 'boto3'
        ]
        
        for module in mock_modules:
            sys.modules[module] = MagicMock()
        
        from glue_job.storage import (
            JobBookmarkManager, 
            JobBookmarkState, 
            S3BookmarkStorage, 
            S3BookmarkConfig
        )
        print("✓ Successfully imported required classes from glue_data_replication")
        
        return True
        
    except ImportError as e:
        print(f"✗ Import error: {e}")
        return False
    except Exception as e:
        print(f"✗ Unexpected error during import: {e}")
        return False

def run_basic_functionality_test():
    """Test basic functionality of JobBookmarkManager."""
    print("\nTesting basic JobBookmarkManager functionality...")
    
    try:
        from glue_job.storage import JobBookmarkManager, JobBookmarkState
        
        # Create mock Glue context
        mock_glue_context = Mock()
        
        # Test creating JobBookmarkManager without S3 paths (in-memory mode)
        manager = JobBookmarkManager(
            glue_context=mock_glue_context,
            job_name="test-job"
        )
        
        print("✓ Successfully created JobBookmarkManager in memory mode")
        
        # Test initializing bookmark state
        state = manager.initialize_bookmark_state(
            table_name="test_table",
            incremental_strategy="timestamp",
            incremental_column="updated_at"
        )
        
        print("✓ Successfully initialized bookmark state")
        
        # Verify state properties
        assert state.table_name == "test_table"
        assert state.incremental_strategy == "timestamp"
        assert state.incremental_column == "updated_at"
        assert state.is_first_run is True
        assert state.job_name == "test-job"
        
        print("✓ Bookmark state properties are correct")
        
        # Test updating bookmark state
        manager.update_bookmark_state(
            table_name="test_table",
            new_max_value="2024-01-15T10:30:00Z",
            processed_rows=100
        )
        
        # Verify state was updated
        updated_state = manager.bookmark_states["test_table"]
        assert updated_state.is_first_run is False
        assert updated_state.last_processed_value == "2024-01-15T10:30:00Z"
        
        print("✓ Successfully updated bookmark state")
        print("✓ State transition from first run to incremental works correctly")
        
        return True
        
    except Exception as e:
        print(f"✗ Error during basic functionality test: {e}")
        import traceback
        traceback.print_exc()
        return False

def run_s3_bucket_extraction_test():
    """Test S3 bucket extraction functionality."""
    print("\nTesting S3 bucket extraction...")
    
    try:
        from glue_job.storage import JobBookmarkManager
        
        # Mock the S3PathUtilities
        with patch('glue_data_replication.S3PathUtilities') as mock_s3_utils:
            mock_s3_utils.detect_s3_bucket_from_jdbc_paths.return_value = "test-bucket"
            mock_s3_utils.validate_s3_bucket_accessibility.return_value = True
            
            # Mock S3BookmarkStorage to avoid actual S3 operations
            with patch('glue_data_replication.S3BookmarkStorage') as mock_storage_class:
                mock_storage = Mock()
                mock_storage_class.return_value = mock_storage
                
                # Create mock Glue context
                mock_glue_context = Mock()
                
                # Test creating JobBookmarkManager with S3 paths
                manager = JobBookmarkManager(
                    glue_context=mock_glue_context,
                    job_name="test-job",
                    source_jdbc_path="s3://test-bucket/drivers/oracle-driver.jar",
                    target_jdbc_path="s3://test-bucket/drivers/postgres-driver.jar"
                )
                
                print("✓ Successfully created JobBookmarkManager with S3 paths")
                
                # Verify S3 bucket detection was called
                mock_s3_utils.detect_s3_bucket_from_jdbc_paths.assert_called_once()
                
                print("✓ S3 bucket detection was called correctly")
                
                # Verify S3 storage was initialized
                assert manager.s3_enabled is True
                assert manager.s3_bookmark_storage is not None
                
                print("✓ S3 bookmark storage was initialized correctly")
                
        return True
        
    except Exception as e:
        print(f"✗ Error during S3 bucket extraction test: {e}")
        import traceback
        traceback.print_exc()
        return False

def run_fallback_mechanism_test():
    """Test fallback to in-memory bookmarks."""
    print("\nTesting fallback mechanisms...")
    
    try:
        from glue_job.storage import JobBookmarkManager
        
        # Test fallback when S3 bucket extraction fails
        with patch('glue_data_replication.S3PathUtilities') as mock_s3_utils:
            mock_s3_utils.detect_s3_bucket_from_jdbc_paths.side_effect = ValueError("Invalid S3 path")
            
            # Create mock Glue context
            mock_glue_context = Mock()
            
            # Test creating JobBookmarkManager with invalid S3 path
            manager = JobBookmarkManager(
                glue_context=mock_glue_context,
                job_name="test-job",
                source_jdbc_path="invalid-path"
            )
            
            print("✓ Successfully handled S3 bucket extraction failure")
            
            # Verify fallback to in-memory bookmarks
            assert manager.s3_enabled is False
            assert manager.s3_bookmark_storage is None
            
            print("✓ Correctly fell back to in-memory bookmarks")
            
            # Test that bookmark operations still work
            state = manager.initialize_bookmark_state(
                table_name="test_table",
                incremental_strategy="timestamp",
                incremental_column="updated_at"
            )
            
            assert state.is_first_run is True
            print("✓ Bookmark operations work correctly in fallback mode")
        
        return True
        
    except Exception as e:
        print(f"✗ Error during fallback mechanism test: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """Run all tests."""
    print("=" * 60)
    print("JobBookmarkManager Unit Tests")
    print("=" * 60)
    
    tests_passed = 0
    total_tests = 4
    
    # Run basic import test
    if run_basic_import_test():
        tests_passed += 1
    
    # Run basic functionality test
    if run_basic_functionality_test():
        tests_passed += 1
    
    # Run S3 bucket extraction test
    if run_s3_bucket_extraction_test():
        tests_passed += 1
    
    # Run fallback mechanism test
    if run_fallback_mechanism_test():
        tests_passed += 1
    
    print("\n" + "=" * 60)
    print(f"Test Results: {tests_passed}/{total_tests} tests passed")
    print("=" * 60)
    
    if tests_passed == total_tests:
        print("✓ All tests passed successfully!")
        print("\nTask 14 Implementation Summary:")
        print("- ✓ S3 bucket extraction from JDBC driver paths")
        print("- ✓ Bookmark state transitions from first run to incremental loading")
        print("- ✓ Fallback mechanisms to in-memory bookmarks")
        print("- ✓ Integration between JobBookmarkManager and S3BookmarkStorage")
        return 0
    else:
        print(f"✗ {total_tests - tests_passed} tests failed")
        return 1

if __name__ == "__main__":
    sys.exit(main())