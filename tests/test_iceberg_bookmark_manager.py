"""
Test suite for Iceberg identifier-field-ids support in BookmarkManager

This test suite validates the enhanced BookmarkManager functionality
for Iceberg tables with identifier-field-ids support.
"""

import unittest
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime, timezone
import sys
import os

# Add the src directory to the path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

# Import the classes we're testing
from glue_job.storage.bookmark_manager import JobBookmarkManager, JobBookmarkState


class TestIcebergBookmarkManager(unittest.TestCase):
    """Test cases for Iceberg identifier-field-ids support in BookmarkManager."""
    
    def setUp(self):
        """Set up test fixtures."""
        # Mock Glue context and job
        self.mock_glue_context = Mock()
        self.mock_job = Mock()
        self.job_name = "test-iceberg-job"
        
        # Create BookmarkManager instance
        self.bookmark_manager = JobBookmarkManager(
            glue_context=self.mock_glue_context,
            job_name=self.job_name,
            job=self.mock_job
        )
        
        # Test data
        self.test_database = "test_database"
        self.test_table = "test_iceberg_table"
        self.test_catalog_id = "123456789012"
        
        # Mock Iceberg table metadata
        self.mock_iceberg_table_metadata = {
            'Table': {
                'Name': self.test_table,
                'DatabaseName': self.test_database,
                'Parameters': {
                    'table_type': 'ICEBERG',
                    'identifier-field-ids': '1,2',
                    'format-version': '2'
                },
                'StorageDescriptor': {
                    'Location': 's3://test-bucket/warehouse/test_database/test_iceberg_table/',
                    'Columns': [
                        {'Name': 'id', 'Type': 'bigint', 'Comment': 'Primary key'},
                        {'Name': 'updated_at', 'Type': 'timestamp', 'Comment': 'Last update timestamp'},
                        {'Name': 'name', 'Type': 'string', 'Comment': 'Record name'},
                        {'Name': 'value', 'Type': 'double', 'Comment': 'Record value'}
                    ]
                },
                'CreateTime': datetime.now(timezone.utc),
                'UpdateTime': datetime.now(timezone.utc)
            }
        }
        
        # Mock non-Iceberg table metadata
        self.mock_regular_table_metadata = {
            'Table': {
                'Name': self.test_table,
                'DatabaseName': self.test_database,
                'Parameters': {
                    'table_type': 'EXTERNAL_TABLE'
                },
                'StorageDescriptor': {
                    'Location': 's3://test-bucket/data/test_table/',
                    'Columns': [
                        {'Name': 'id', 'Type': 'bigint'},
                        {'Name': 'name', 'Type': 'string'}
                    ]
                }
            }
        }
        
        # Mock DataFrame for fallback testing
        self.mock_dataframe = Mock()
        self.mock_schema = Mock()
        
        # Create proper mock fields with string representations
        mock_id_field = Mock()
        mock_id_field.name = 'id'
        mock_id_field.dataType = Mock()
        mock_id_field.dataType.__str__ = Mock(return_value='LongType()')
        
        mock_timestamp_field = Mock()
        mock_timestamp_field.name = 'updated_at'
        mock_timestamp_field.dataType = Mock()
        mock_timestamp_field.dataType.__str__ = Mock(return_value='TimestampType()')
        
        mock_name_field = Mock()
        mock_name_field.name = 'name'
        mock_name_field.dataType = Mock()
        mock_name_field.dataType.__str__ = Mock(return_value='StringType()')
        
        mock_value_field = Mock()
        mock_value_field.name = 'value'
        mock_value_field.dataType = Mock()
        mock_value_field.dataType.__str__ = Mock(return_value='DoubleType()')
        
        self.mock_schema.fields = [
            mock_id_field,
            mock_timestamp_field,
            mock_name_field,
            mock_value_field
        ]
        self.mock_dataframe.schema = self.mock_schema
    
    @patch('boto3.client')
    def test_extract_identifier_field_ids_success(self, mock_boto_client):
        """Test successful extraction of identifier-field-ids from Iceberg table."""
        # Setup mock Glue client
        mock_glue_client = Mock()
        mock_boto_client.return_value = mock_glue_client
        mock_glue_client.get_table.return_value = self.mock_iceberg_table_metadata
        
        # Test extraction
        result = self.bookmark_manager.extract_identifier_field_ids(
            database=self.test_database,
            table=self.test_table,
            catalog_id=self.test_catalog_id
        )
        
        # Verify results
        self.assertIsNotNone(result)
        self.assertEqual(result, [1, 2])
        
        # Verify Glue client was called correctly
        mock_glue_client.get_table.assert_called_once_with(
            DatabaseName=self.test_database,
            Name=self.test_table,
            CatalogId=self.test_catalog_id
        )
    
    @patch('boto3.client')
    def test_extract_identifier_field_ids_no_property(self, mock_boto_client):
        """Test extraction when identifier-field-ids property is missing."""
        # Setup mock without identifier-field-ids
        mock_glue_client = Mock()
        mock_boto_client.return_value = mock_glue_client
        
        table_metadata = self.mock_iceberg_table_metadata.copy()
        table_metadata['Table']['Parameters'].pop('identifier-field-ids')
        mock_glue_client.get_table.return_value = table_metadata
        
        # Test extraction
        result = self.bookmark_manager.extract_identifier_field_ids(
            database=self.test_database,
            table=self.test_table
        )
        
        # Verify no field IDs returned
        self.assertIsNone(result)
    
    @patch('boto3.client')
    def test_extract_identifier_field_ids_non_iceberg_table(self, mock_boto_client):
        """Test extraction from non-Iceberg table."""
        # Setup mock for regular table
        mock_glue_client = Mock()
        mock_boto_client.return_value = mock_glue_client
        mock_glue_client.get_table.return_value = self.mock_regular_table_metadata
        
        # Test extraction
        result = self.bookmark_manager.extract_identifier_field_ids(
            database=self.test_database,
            table=self.test_table
        )
        
        # Verify no field IDs returned for non-Iceberg table
        self.assertIsNone(result)
    
    @patch('boto3.client')
    def test_extract_identifier_field_ids_table_not_found(self, mock_boto_client):
        """Test extraction when table doesn't exist."""
        # Setup mock to raise EntityNotFoundException
        mock_glue_client = Mock()
        mock_boto_client.return_value = mock_glue_client
        
        from botocore.exceptions import ClientError
        mock_glue_client.get_table.side_effect = ClientError(
            error_response={'Error': {'Code': 'EntityNotFoundException'}},
            operation_name='GetTable'
        )
        
        # Test extraction
        result = self.bookmark_manager.extract_identifier_field_ids(
            database=self.test_database,
            table=self.test_table
        )
        
        # Verify no field IDs returned for missing table
        self.assertIsNone(result)
    
    @patch('boto3.client')
    def test_get_iceberg_bookmark_column_success(self, mock_boto_client):
        """Test successful bookmark column extraction from identifier-field-ids."""
        # Setup mock Glue client
        mock_glue_client = Mock()
        mock_boto_client.return_value = mock_glue_client
        mock_glue_client.get_table.return_value = self.mock_iceberg_table_metadata
        
        # Test bookmark column extraction
        result = self.bookmark_manager.get_iceberg_bookmark_column(
            database=self.test_database,
            table=self.test_table,
            catalog_id=self.test_catalog_id
        )
        
        # Verify bookmark column is extracted (first field with ID 1)
        self.assertIsNotNone(result)
        self.assertEqual(result, 'id')  # First column corresponds to field ID 1
    
    @patch('boto3.client')
    def test_get_iceberg_bookmark_column_no_identifier_field_ids(self, mock_boto_client):
        """Test bookmark column extraction when no identifier-field-ids exist."""
        # Setup mock without identifier-field-ids
        mock_glue_client = Mock()
        mock_boto_client.return_value = mock_glue_client
        
        table_metadata = self.mock_iceberg_table_metadata.copy()
        table_metadata['Table']['Parameters'].pop('identifier-field-ids')
        mock_glue_client.get_table.return_value = table_metadata
        
        # Test bookmark column extraction
        result = self.bookmark_manager.get_iceberg_bookmark_column(
            database=self.test_database,
            table=self.test_table
        )
        
        # Verify no bookmark column returned
        self.assertIsNone(result)
    
    def test_fallback_to_traditional_bookmark_timestamp_column(self):
        """Test fallback bookmark detection with timestamp column."""
        # Test with DataFrame containing timestamp column
        result = self.bookmark_manager.fallback_to_traditional_bookmark(
            dataframe=self.mock_dataframe,
            table_name=self.test_table
        )
        
        # Verify timestamp column is detected as bookmark
        self.assertIsNotNone(result)
        self.assertEqual(result, 'updated_at')
    
    def test_fallback_to_traditional_bookmark_id_column(self):
        """Test fallback bookmark detection with ID column when no timestamp."""
        # Mock schema with only ID and string columns
        mock_schema_no_timestamp = Mock()
        
        mock_id_field = Mock()
        mock_id_field.name = 'id'
        mock_id_field.dataType = Mock()
        mock_id_field.dataType.__str__ = Mock(return_value='LongType()')
        
        mock_name_field = Mock()
        mock_name_field.name = 'name'
        mock_name_field.dataType = Mock()
        mock_name_field.dataType.__str__ = Mock(return_value='StringType()')
        
        mock_schema_no_timestamp.fields = [
            mock_id_field,
            mock_name_field
        ]
        
        mock_df_no_timestamp = Mock()
        mock_df_no_timestamp.schema = mock_schema_no_timestamp
        
        # Test fallback detection
        result = self.bookmark_manager.fallback_to_traditional_bookmark(
            dataframe=mock_df_no_timestamp,
            table_name=self.test_table
        )
        
        # Verify ID column is detected as bookmark
        self.assertIsNotNone(result)
        self.assertEqual(result, 'id')
    
    def test_fallback_to_traditional_bookmark_no_suitable_column(self):
        """Test fallback bookmark detection when no suitable columns exist."""
        # Mock schema with only string columns
        mock_schema_no_bookmark = Mock()
        
        mock_name_field = Mock()
        mock_name_field.name = 'name'
        mock_name_field.dataType = Mock()
        mock_name_field.dataType.__str__ = Mock(return_value='StringType()')
        
        mock_desc_field = Mock()
        mock_desc_field.name = 'description'
        mock_desc_field.dataType = Mock()
        mock_desc_field.dataType.__str__ = Mock(return_value='StringType()')
        
        mock_schema_no_bookmark.fields = [
            mock_name_field,
            mock_desc_field
        ]
        
        mock_df_no_bookmark = Mock()
        mock_df_no_bookmark.schema = mock_schema_no_bookmark
        
        # Test fallback detection
        result = self.bookmark_manager.fallback_to_traditional_bookmark(
            dataframe=mock_df_no_bookmark,
            table_name=self.test_table
        )
        
        # Verify no bookmark column found
        self.assertIsNone(result)
    
    def test_fallback_to_traditional_bookmark_no_dataframe(self):
        """Test fallback bookmark detection with no DataFrame."""
        # Test with None DataFrame
        result = self.bookmark_manager.fallback_to_traditional_bookmark(
            dataframe=None,
            table_name=self.test_table
        )
        
        # Verify no bookmark column returned
        self.assertIsNone(result)
    
    @patch('boto3.client')
    def test_initialize_iceberg_bookmark_state_with_identifier_field_ids(self, mock_boto_client):
        """Test Iceberg bookmark state initialization with identifier-field-ids."""
        # Setup mock Glue client
        mock_glue_client = Mock()
        mock_boto_client.return_value = mock_glue_client
        mock_glue_client.get_table.return_value = self.mock_iceberg_table_metadata
        
        # Test initialization
        result = self.bookmark_manager.initialize_iceberg_bookmark_state(
            database=self.test_database,
            table=self.test_table,
            incremental_strategy='timestamp',
            catalog_id=self.test_catalog_id
        )
        
        # Verify bookmark state is created
        self.assertIsInstance(result, JobBookmarkState)
        self.assertEqual(result.table_name, f"{self.test_database}.{self.test_table}")
        self.assertEqual(result.incremental_strategy, 'timestamp')
        self.assertEqual(result.incremental_column, 'id')  # From identifier-field-ids
        self.assertTrue(result.is_first_run)
    
    @patch('boto3.client')
    def test_initialize_iceberg_bookmark_state_fallback_to_traditional(self, mock_boto_client):
        """Test Iceberg bookmark state initialization with fallback to traditional detection."""
        # Setup mock without identifier-field-ids
        mock_glue_client = Mock()
        mock_boto_client.return_value = mock_glue_client
        
        table_metadata = self.mock_iceberg_table_metadata.copy()
        table_metadata['Table']['Parameters'].pop('identifier-field-ids')
        mock_glue_client.get_table.return_value = table_metadata
        
        # Test initialization with DataFrame for fallback
        result = self.bookmark_manager.initialize_iceberg_bookmark_state(
            database=self.test_database,
            table=self.test_table,
            incremental_strategy='timestamp',
            dataframe=self.mock_dataframe
        )
        
        # Verify bookmark state uses fallback detection
        self.assertIsInstance(result, JobBookmarkState)
        self.assertEqual(result.incremental_column, 'updated_at')  # From fallback detection
    
    @patch('boto3.client')
    def test_is_iceberg_table_detection(self, mock_boto_client):
        """Test Iceberg table detection logic."""
        # Setup mock Glue client
        mock_glue_client = Mock()
        mock_boto_client.return_value = mock_glue_client
        mock_glue_client.get_table.return_value = self.mock_iceberg_table_metadata
        
        # Test with engine type hint
        result = self.bookmark_manager._is_iceberg_table(
            table_name=self.test_table,
            engine_type='iceberg'
        )
        self.assertTrue(result)
        
        # Test with database lookup
        result = self.bookmark_manager._is_iceberg_table(
            table_name=self.test_table,
            database=self.test_database
        )
        self.assertTrue(result)
        
        # Test with non-Iceberg table
        mock_glue_client.get_table.return_value = self.mock_regular_table_metadata
        result = self.bookmark_manager._is_iceberg_table(
            table_name=self.test_table,
            database=self.test_database
        )
        self.assertFalse(result)
    
    @patch('boto3.client')
    def test_initialize_bookmark_state_iceberg_routing(self, mock_boto_client):
        """Test that initialize_bookmark_state routes Iceberg tables correctly."""
        # Setup mock Glue client
        mock_glue_client = Mock()
        mock_boto_client.return_value = mock_glue_client
        mock_glue_client.get_table.return_value = self.mock_iceberg_table_metadata
        
        # Test initialization with Iceberg engine type
        result = self.bookmark_manager.initialize_bookmark_state(
            table_name=self.test_table,
            incremental_strategy='timestamp',
            database=self.test_database,
            engine_type='iceberg',
            dataframe=self.mock_dataframe
        )
        
        # Verify Iceberg-specific initialization was used
        self.assertIsInstance(result, JobBookmarkState)
        self.assertEqual(result.table_name, f"{self.test_database}.{self.test_table}")
        self.assertEqual(result.incremental_column, 'id')  # From identifier-field-ids
    
    @patch('boto3.client')
    def test_update_bookmark_state_iceberg_detection(self, mock_boto_client):
        """Test that update_bookmark_state detects and logs Iceberg tables."""
        # Setup mock Glue client
        mock_glue_client = Mock()
        mock_boto_client.return_value = mock_glue_client
        mock_glue_client.get_table.return_value = self.mock_iceberg_table_metadata
        
        # First initialize a bookmark state
        table_name = f"{self.test_database}.{self.test_table}"
        self.bookmark_manager.bookmark_states[table_name] = JobBookmarkState(
            table_name=table_name,
            incremental_strategy='timestamp',
            incremental_column='id',
            job_name=self.job_name
        )
        
        # Test update with Iceberg detection
        self.bookmark_manager.update_bookmark_state(
            table_name=table_name,
            new_max_value=12345,
            processed_rows=100,
            database=self.test_database,
            engine_type='iceberg'
        )
        
        # Verify state was updated
        updated_state = self.bookmark_manager.bookmark_states[table_name]
        self.assertEqual(updated_state.last_processed_value, 12345)
        self.assertFalse(updated_state.is_first_run)
    
    def test_error_handling_in_identifier_field_ids_extraction(self):
        """Test error handling during identifier-field-ids extraction."""
        # Test with invalid field IDs format
        with patch('boto3.client') as mock_boto_client:
            mock_glue_client = Mock()
            mock_boto_client.return_value = mock_glue_client
            
            # Setup table with invalid identifier-field-ids format
            invalid_metadata = self.mock_iceberg_table_metadata.copy()
            invalid_metadata['Table']['Parameters']['identifier-field-ids'] = 'invalid,format,abc'
            mock_glue_client.get_table.return_value = invalid_metadata
            
            result = self.bookmark_manager.extract_identifier_field_ids(
                database=self.test_database,
                table=self.test_table
            )
            
            # Should return None due to parsing error
            self.assertIsNone(result)
    
    def test_error_handling_in_fallback_bookmark_detection(self):
        """Test error handling during fallback bookmark detection."""
        # Test with DataFrame that raises exception
        mock_error_dataframe = Mock()
        mock_error_dataframe.schema.side_effect = Exception("Schema access error")
        
        result = self.bookmark_manager.fallback_to_traditional_bookmark(
            dataframe=mock_error_dataframe,
            table_name=self.test_table
        )
        
        # Should return None due to error
        self.assertIsNone(result)


class TestIcebergBookmarkManagerIntegration(unittest.TestCase):
    """Integration tests for Iceberg bookmark manager functionality."""
    
    def test_end_to_end_iceberg_bookmark_workflow(self):
        """Test complete workflow from initialization to update for Iceberg table."""
        # This test would require more complex setup with actual AWS services
        # For now, we'll test the workflow with mocked components
        
        # Setup
        mock_glue_context = Mock()
        job_name = "integration-test-job"
        
        bookmark_manager = JobBookmarkManager(
            glue_context=mock_glue_context,
            job_name=job_name
        )
        
        # Mock successful workflow
        with patch('boto3.client') as mock_boto_client:
            mock_glue_client = Mock()
            mock_boto_client.return_value = mock_glue_client
            
            # Setup Iceberg table metadata
            iceberg_metadata = {
                'Table': {
                    'Name': 'test_table',
                    'DatabaseName': 'test_db',
                    'Parameters': {
                        'table_type': 'ICEBERG',
                        'identifier-field-ids': '1'
                    },
                    'StorageDescriptor': {
                        'Columns': [
                            {'Name': 'id', 'Type': 'bigint'},
                            {'Name': 'data', 'Type': 'string'}
                        ]
                    }
                }
            }
            mock_glue_client.get_table.return_value = iceberg_metadata
            
            # Test initialization
            state = bookmark_manager.initialize_bookmark_state(
                table_name='test_table',
                incremental_strategy='primary_key',
                database='test_db',
                engine_type='iceberg'
            )
            
            # Verify initialization
            self.assertIsInstance(state, JobBookmarkState)
            self.assertEqual(state.incremental_column, 'id')
            
            # Test update
            bookmark_manager.update_bookmark_state(
                table_name='test_db.test_table',
                new_max_value=100,
                processed_rows=50,
                database='test_db',
                engine_type='iceberg'
            )
            
            # Verify update
            updated_state = bookmark_manager.get_bookmark_state('test_db.test_table')
            self.assertEqual(updated_state.last_processed_value, 100)
            self.assertFalse(updated_state.is_first_run)


if __name__ == '__main__':
    # Run the tests
    unittest.main()