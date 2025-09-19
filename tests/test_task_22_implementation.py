"""
Test implementation for Task 22: Update JobBookmarkState to track manual configuration usage.

This test verifies that the JobBookmarkState class correctly handles the new manual configuration
tracking fields (is_manually_configured and manual_column_data_type) in both serialization
and deserialization operations while maintaining backward compatibility.
"""

import unittest
import pytest
from datetime import datetime, timezone
from src.glue_job.storage.bookmark_manager import JobBookmarkState


class TestJobBookmarkStateManualConfigTracking(unittest.TestCase):
    """Test JobBookmarkState manual configuration tracking functionality."""
    
    def test_new_fields_initialization(self):
        """Test that new manual configuration fields are properly initialized."""
        # Test default initialization
        state = JobBookmarkState(
            table_name="test_table",
            incremental_strategy="timestamp"
        )
        
        assert state.is_manually_configured is False
        assert state.manual_column_data_type is None
    
    def test_new_fields_explicit_initialization(self):
        """Test explicit initialization of manual configuration fields."""
        state = JobBookmarkState(
            table_name="test_table",
            incremental_strategy="timestamp",
            incremental_column="updated_at",
            is_manually_configured=True,
            manual_column_data_type="TIMESTAMP"
        )
        
        assert state.is_manually_configured is True
        assert state.manual_column_data_type == "TIMESTAMP"
    
    def test_to_s3_dict_includes_new_fields(self):
        """Test that to_s3_dict() includes the new manual configuration fields."""
        state = JobBookmarkState(
            table_name="customers",
            incremental_strategy="timestamp",
            incremental_column="last_modified",
            job_name="test_job",
            is_manually_configured=True,
            manual_column_data_type="TIMESTAMP_WITH_TIMEZONE"
        )
        
        s3_dict = state.to_s3_dict()
        
        # Verify new fields are included
        assert 'is_manually_configured' in s3_dict
        assert 'manual_column_data_type' in s3_dict
        assert s3_dict['is_manually_configured'] is True
        assert s3_dict['manual_column_data_type'] == "TIMESTAMP_WITH_TIMEZONE"
        
        # Verify existing fields are still present
        assert s3_dict['table_name'] == "customers"
        assert s3_dict['incremental_strategy'] == "timestamp"
        assert s3_dict['incremental_column'] == "last_modified"
        assert s3_dict['job_name'] == "test_job"
    
    def test_to_s3_dict_with_default_values(self):
        """Test to_s3_dict() with default values for new fields."""
        state = JobBookmarkState(
            table_name="orders",
            incremental_strategy="primary_key",
            job_name="test_job"
        )
        
        s3_dict = state.to_s3_dict()
        
        # Verify default values are serialized correctly
        assert s3_dict['is_manually_configured'] is False
        assert s3_dict['manual_column_data_type'] is None
    
    def test_from_s3_dict_with_new_fields(self):
        """Test from_s3_dict() correctly deserializes new fields."""
        s3_data = {
            'table_name': 'products',
            'incremental_strategy': 'timestamp',
            'incremental_column': 'created_date',
            'job_name': 'product_sync',
            'version': '1.0',
            'is_first_run': False,
            'is_manually_configured': True,
            'manual_column_data_type': 'DATE'
        }
        
        state = JobBookmarkState.from_s3_dict(s3_data)
        
        assert state.table_name == 'products'
        assert state.incremental_strategy == 'timestamp'
        assert state.incremental_column == 'created_date'
        assert state.is_manually_configured is True
        assert state.manual_column_data_type == 'DATE'
    
    def test_from_s3_dict_backward_compatibility(self):
        """Test backward compatibility with existing bookmark files without new fields."""
        # Simulate old bookmark file without manual configuration fields
        old_s3_data = {
            'table_name': 'legacy_table',
            'incremental_strategy': 'hash',
            'job_name': 'legacy_job',
            'version': '1.0',
            'is_first_run': True
        }
        
        state = JobBookmarkState.from_s3_dict(old_s3_data)
        
        # Verify existing fields work
        assert state.table_name == 'legacy_table'
        assert state.incremental_strategy == 'hash'
        assert state.job_name == 'legacy_job'
        
        # Verify new fields have default values
        assert state.is_manually_configured is False
        assert state.manual_column_data_type is None
    
    def test_from_s3_dict_with_null_manual_fields(self):
        """Test from_s3_dict() with explicit null values for manual fields."""
        s3_data = {
            'table_name': 'test_table',
            'incremental_strategy': 'primary_key',
            'incremental_column': 'id',  # Required for primary_key strategy
            'job_name': 'test_job',
            'version': '1.0',
            'is_manually_configured': False,
            'manual_column_data_type': None
        }
        
        state = JobBookmarkState.from_s3_dict(s3_data)
        
        assert state.is_manually_configured is False
        assert state.manual_column_data_type is None
    
    def test_roundtrip_serialization(self):
        """Test that serialization and deserialization preserve manual configuration data."""
        original_state = JobBookmarkState(
            table_name="roundtrip_test",
            incremental_strategy="timestamp",
            incremental_column="updated_timestamp",
            job_name="roundtrip_job",
            last_processed_value="2024-01-15T10:30:00Z",
            is_first_run=False,
            is_manually_configured=True,
            manual_column_data_type="TIMESTAMP_WITH_TIMEZONE"
        )
        
        # Serialize to S3 dict
        s3_dict = original_state.to_s3_dict()
        
        # Deserialize back to object
        restored_state = JobBookmarkState.from_s3_dict(s3_dict)
        
        # Verify all fields match
        assert restored_state.table_name == original_state.table_name
        assert restored_state.incremental_strategy == original_state.incremental_strategy
        assert restored_state.incremental_column == original_state.incremental_column
        assert restored_state.job_name == original_state.job_name
        assert restored_state.is_first_run == original_state.is_first_run
        assert restored_state.is_manually_configured == original_state.is_manually_configured
        assert restored_state.manual_column_data_type == original_state.manual_column_data_type
    
    def test_validate_s3_data_with_manual_fields(self):
        """Test _validate_s3_data() correctly validates manual configuration fields."""
        # Valid data with manual configuration
        valid_data = {
            'table_name': 'test_table',
            'incremental_strategy': 'timestamp',
            'incremental_column': 'updated_at',  # Required for timestamp strategy
            'is_manually_configured': True,
            'manual_column_data_type': 'TIMESTAMP'
        }
        
        assert JobBookmarkState._validate_s3_data(valid_data) is True
    
    def test_validate_s3_data_invalid_manual_configured_type(self):
        """Test validation fails for invalid is_manually_configured type."""
        invalid_data = {
            'table_name': 'test_table',
            'incremental_strategy': 'timestamp',
            'is_manually_configured': 'true'  # Should be boolean, not string
        }
        
        assert JobBookmarkState._validate_s3_data(invalid_data) is False
    
    def test_validate_s3_data_invalid_manual_column_data_type(self):
        """Test validation fails for invalid manual_column_data_type."""
        # Test with non-string type
        invalid_data1 = {
            'table_name': 'test_table',
            'incremental_strategy': 'timestamp',
            'manual_column_data_type': 123  # Should be string
        }
        
        assert JobBookmarkState._validate_s3_data(invalid_data1) is False
        
        # Test with empty string
        invalid_data2 = {
            'table_name': 'test_table',
            'incremental_strategy': 'timestamp',
            'manual_column_data_type': ''  # Should not be empty
        }
        
        assert JobBookmarkState._validate_s3_data(invalid_data2) is False
        
        # Test with whitespace-only string
        invalid_data3 = {
            'table_name': 'test_table',
            'incremental_strategy': 'timestamp',
            'manual_column_data_type': '   '  # Should not be whitespace-only
        }
        
        assert JobBookmarkState._validate_s3_data(invalid_data3) is False
    
    def test_validate_s3_data_backward_compatibility(self):
        """Test validation works with old data without manual configuration fields."""
        old_data = {
            'table_name': 'legacy_table',
            'incremental_strategy': 'hash'
        }
        
        assert JobBookmarkState._validate_s3_data(old_data) is True


if __name__ == "__main__":
    pytest.main([__file__, "-v"])