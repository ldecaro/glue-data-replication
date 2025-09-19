"""
Unit tests for Iceberg parameter validation

This module tests that traditional database parameters are properly excluded
when Iceberg engines are used, and that Iceberg-specific parameters are
correctly validated.
"""

import unittest
import pytest
import sys
from unittest.mock import patch, MagicMock

# Add src to path for imports
sys.path.insert(0, 'src')

from glue_job.config.database_engines import DatabaseEngineManager
from glue_job.config.parsers import JobConfigurationParser


class TestIcebergParameterExclusion(unittest.TestCase):
    """Test that traditional database parameters are excluded for Iceberg engines."""
    
    def test_get_excluded_parameters_iceberg_source(self):
        """Test that Iceberg source engine excludes traditional database parameters."""
        excluded_params = DatabaseEngineManager.get_excluded_parameters('iceberg', 'source')
        
        expected_excluded = [
            'SOURCE_DB_PASSWORD',
            'SOURCE_DB_USER',
            'SOURCE_SCHEMA',
            'SOURCE_JDBC_DRIVER_S3_PATH',
            'SOURCE_CONNECTION_STRING'
        ]
        
        assert excluded_params == expected_excluded
        assert len(excluded_params) == 5
    
    def test_get_excluded_parameters_iceberg_target(self):
        """Test that Iceberg target engine excludes traditional database parameters."""
        excluded_params = DatabaseEngineManager.get_excluded_parameters('iceberg', 'target')
        
        expected_excluded = [
            'TARGET_DB_PASSWORD',
            'TARGET_DB_USER',
            'TARGET_SCHEMA',
            'TARGET_JDBC_DRIVER_S3_PATH', 
            'TARGET_CONNECTION_STRING'
        ]
        
        assert excluded_params == expected_excluded
        assert len(excluded_params) == 5
    
    def test_get_excluded_parameters_jdbc_engines(self):
        """Test that JDBC engines don't exclude any parameters."""
        jdbc_engines = ['oracle', 'postgresql', 'sqlserver', 'db2']
        
        for engine in jdbc_engines:
            source_excluded = DatabaseEngineManager.get_excluded_parameters(engine, 'source')
            target_excluded = DatabaseEngineManager.get_excluded_parameters(engine, 'target')
            
            assert source_excluded == []
            assert target_excluded == []
    
    def test_get_excluded_parameters_invalid_engine(self):
        """Test that invalid engines return empty exclusion lists."""
        excluded_params = DatabaseEngineManager.get_excluded_parameters('invalid_engine', 'source')
        assert excluded_params == []
        
        excluded_params = DatabaseEngineManager.get_excluded_parameters('invalid_engine', 'target')
        assert excluded_params == []
    
    def test_get_excluded_parameters_invalid_role(self):
        """Test that invalid roles return empty exclusion lists."""
        excluded_params = DatabaseEngineManager.get_excluded_parameters('iceberg', 'invalid_role')
        assert excluded_params == []


class TestIcebergRequiredParameters(unittest.TestCase):
    """Test that Iceberg engines have correct required parameters."""
    
    def test_get_required_parameters_iceberg_source(self):
        """Test that Iceberg source engine has correct required parameters."""
        required_params = DatabaseEngineManager.get_required_parameters('iceberg', 'source')
        
        expected_required = [
            'SOURCE_DATABASE',
            'SOURCE_WAREHOUSE_LOCATION'
        ]
        
        assert required_params == expected_required
        assert len(required_params) == 2
    
    def test_get_required_parameters_iceberg_target(self):
        """Test that Iceberg target engine has correct required parameters."""
        required_params = DatabaseEngineManager.get_required_parameters('iceberg', 'target')
        
        expected_required = [
            'TARGET_DATABASE',
            'TARGET_WAREHOUSE_LOCATION'
        ]
        
        assert required_params == expected_required
        assert len(required_params) == 2
    
    def test_get_required_parameters_jdbc_engines(self):
        """Test that JDBC engines return empty required parameters (use legacy logic)."""
        jdbc_engines = ['oracle', 'postgresql', 'sqlserver', 'db2']
        
        for engine in jdbc_engines:
            source_required = DatabaseEngineManager.get_required_parameters(engine, 'source')
            target_required = DatabaseEngineManager.get_required_parameters(engine, 'target')
            
            assert source_required == []
            assert target_required == []
    
    def test_get_required_parameters_invalid_engine(self):
        """Test that invalid engines return empty required parameter lists."""
        required_params = DatabaseEngineManager.get_required_parameters('invalid_engine', 'source')
        assert required_params == []
        
        required_params = DatabaseEngineManager.get_required_parameters('invalid_engine', 'target')
        assert required_params == []


class TestJobConfigurationParserParameterExclusion(unittest.TestCase):
    """Test parameter exclusion in JobConfigurationParser."""
    
    def test_get_required_params_for_engines_iceberg_to_iceberg(self):
        """Test parameter requirements for Iceberg-to-Iceberg replication."""
        required_params = JobConfigurationParser.get_required_params_for_engines('iceberg', 'iceberg')
        
        # Should include base params EXCEPT schema params + Iceberg warehouse locations
        expected_base_without_schema = [p for p in JobConfigurationParser.BASE_REQUIRED_PARAMS 
                                       if p not in ['SOURCE_SCHEMA', 'TARGET_SCHEMA']]
        expected_iceberg = ['SOURCE_WAREHOUSE_LOCATION', 'TARGET_WAREHOUSE_LOCATION']
        
        # Should NOT include any JDBC parameters or schema parameters
        jdbc_params = JobConfigurationParser.JDBC_REQUIRED_PARAMS
        
        for param in expected_base_without_schema + expected_iceberg:
            assert param in required_params
        
        for param in jdbc_params:
            assert param not in required_params
        
        # Schema parameters should be excluded for Iceberg engines
        assert 'SOURCE_SCHEMA' not in required_params
        assert 'TARGET_SCHEMA' not in required_params
    
    def test_get_required_params_for_engines_jdbc_to_iceberg(self):
        """Test parameter requirements for JDBC-to-Iceberg replication."""
        required_params = JobConfigurationParser.get_required_params_for_engines('oracle', 'iceberg')
        
        # Should include base params EXCEPT target schema (excluded for Iceberg target)
        expected_base_without_target_schema = [p for p in JobConfigurationParser.BASE_REQUIRED_PARAMS 
                                              if p != 'TARGET_SCHEMA']
        for param in expected_base_without_target_schema:
            assert param in required_params
        
        # Should include source JDBC params but exclude target JDBC params
        source_jdbc_params = [p for p in JobConfigurationParser.JDBC_REQUIRED_PARAMS if p.startswith('SOURCE_')]
        target_jdbc_params = [p for p in JobConfigurationParser.JDBC_REQUIRED_PARAMS if p.startswith('TARGET_')]
        
        for param in source_jdbc_params:
            assert param in required_params
        
        for param in target_jdbc_params:
            assert param not in required_params
        
        # Should include target Iceberg warehouse location
        assert 'TARGET_WAREHOUSE_LOCATION' in required_params
        assert 'SOURCE_WAREHOUSE_LOCATION' not in required_params
        
        # Target schema should be excluded for Iceberg target
        assert 'TARGET_SCHEMA' not in required_params
        # Source schema should be included for Oracle source
        assert 'SOURCE_SCHEMA' in required_params
    
    def test_get_required_params_for_engines_iceberg_to_jdbc(self):
        """Test parameter requirements for Iceberg-to-JDBC replication."""
        required_params = JobConfigurationParser.get_required_params_for_engines('iceberg', 'postgresql')
        
        # Should include base params EXCEPT source schema (excluded for Iceberg source)
        expected_base_without_source_schema = [p for p in JobConfigurationParser.BASE_REQUIRED_PARAMS 
                                              if p != 'SOURCE_SCHEMA']
        for param in expected_base_without_source_schema:
            assert param in required_params
        
        # Should include target JDBC params but exclude source JDBC params
        source_jdbc_params = [p for p in JobConfigurationParser.JDBC_REQUIRED_PARAMS if p.startswith('SOURCE_')]
        target_jdbc_params = [p for p in JobConfigurationParser.JDBC_REQUIRED_PARAMS if p.startswith('TARGET_')]
        
        for param in source_jdbc_params:
            assert param not in required_params
        
        for param in target_jdbc_params:
            assert param in required_params
        
        # Should include source Iceberg warehouse location
        assert 'SOURCE_WAREHOUSE_LOCATION' in required_params
        assert 'TARGET_WAREHOUSE_LOCATION' not in required_params
        
        # Source schema should be excluded for Iceberg source
        assert 'SOURCE_SCHEMA' not in required_params
        # Target schema should be included for PostgreSQL target
        assert 'TARGET_SCHEMA' in required_params
    
    def test_get_required_params_for_engines_jdbc_to_jdbc(self):
        """Test parameter requirements for JDBC-to-JDBC replication."""
        required_params = JobConfigurationParser.get_required_params_for_engines('oracle', 'postgresql')
        
        # Should include all base params and all JDBC params
        expected_base = JobConfigurationParser.BASE_REQUIRED_PARAMS
        expected_jdbc = JobConfigurationParser.JDBC_REQUIRED_PARAMS
        
        for param in expected_base + expected_jdbc:
            assert param in required_params
        
        # Should NOT include Iceberg warehouse locations
        assert 'SOURCE_WAREHOUSE_LOCATION' not in required_params
        assert 'TARGET_WAREHOUSE_LOCATION' not in required_params


class TestParameterValidationIntegration:
    """Integration tests for parameter validation with different engine combinations."""
    
    def create_base_args(self):
        """Create base arguments that are always required."""
        return {
            'JOB_NAME': 'test-job',
            'SOURCE_ENGINE_TYPE': 'oracle',
            'TARGET_ENGINE_TYPE': 'postgresql',
            'SOURCE_DATABASE': 'source_db',
            'TARGET_DATABASE': 'target_db',
            'SOURCE_SCHEMA': 'source_schema',
            'TARGET_SCHEMA': 'target_schema',
            'TABLE_NAMES': 'table1,table2'
        }
    
    def create_jdbc_args(self):
        """Create JDBC-specific arguments."""
        return {
            'SOURCE_DB_USER': 'source_user',
            'SOURCE_DB_PASSWORD': 'source_pass',
            'TARGET_DB_USER': 'target_user',
            'TARGET_DB_PASSWORD': 'target_pass',
            'SOURCE_JDBC_DRIVER_S3_PATH': 's3://bucket/source-driver.jar',
            'TARGET_JDBC_DRIVER_S3_PATH': 's3://bucket/target-driver.jar',
            'SOURCE_CONNECTION_STRING': 'jdbc:oracle:thin:@host:1521:db',
            'TARGET_CONNECTION_STRING': 'jdbc:postgresql://host:5432/db'
        }
    
    def create_iceberg_args(self):
        """Create Iceberg-specific arguments."""
        return {
            'SOURCE_WAREHOUSE_LOCATION': 's3://bucket/source-warehouse/',
            'TARGET_WAREHOUSE_LOCATION': 's3://bucket/target-warehouse/',
            'SOURCE_CATALOG_ID': '123456789012',
            'TARGET_CATALOG_ID': '123456789012'
        }
    
    @patch('glue_job.config.parsers.getResolvedOptions')
    def test_validate_iceberg_to_iceberg_parameters(self, mock_get_resolved_options):
        """Test validation passes for Iceberg-to-Iceberg with correct parameters."""
        # Setup arguments for Iceberg-to-Iceberg
        args = self.create_base_args()
        args.update(self.create_iceberg_args())
        args['SOURCE_ENGINE_TYPE'] = 'iceberg'
        args['TARGET_ENGINE_TYPE'] = 'iceberg'
        
        # Mock getResolvedOptions to return our test args
        mock_get_resolved_options.return_value = args
        
        # Should not raise any exceptions
        try:
            parsed_args = JobConfigurationParser.parse_job_arguments()
            JobConfigurationParser.validate_required_parameters(parsed_args)
        except Exception as e:
            pytest.fail(f"Validation should pass for Iceberg-to-Iceberg: {str(e)}")
    
    @patch('glue_job.config.parsers.getResolvedOptions')
    def test_validate_iceberg_to_iceberg_missing_jdbc_params_allowed(self, mock_get_resolved_options):
        """Test that missing JDBC parameters are allowed for Iceberg-to-Iceberg."""
        # Setup arguments for Iceberg-to-Iceberg WITHOUT JDBC parameters
        args = self.create_base_args()
        args.update(self.create_iceberg_args())
        args['SOURCE_ENGINE_TYPE'] = 'iceberg'
        args['TARGET_ENGINE_TYPE'] = 'iceberg'
        # Explicitly NOT adding JDBC parameters
        
        # Mock getResolvedOptions to return our test args
        mock_get_resolved_options.return_value = args
        
        # Should not raise any exceptions even without JDBC parameters
        try:
            parsed_args = JobConfigurationParser.parse_job_arguments()
            JobConfigurationParser.validate_required_parameters(parsed_args)
        except Exception as e:
            pytest.fail(f"Validation should pass for Iceberg-to-Iceberg without JDBC params: {str(e)}")
    
    @patch('glue_job.config.parsers.getResolvedOptions')
    def test_validate_jdbc_to_iceberg_parameters(self, mock_get_resolved_options):
        """Test validation for JDBC-to-Iceberg with correct parameters."""
        # Setup arguments for JDBC-to-Iceberg
        args = self.create_base_args()
        args.update(self.create_iceberg_args())
        args['SOURCE_ENGINE_TYPE'] = 'oracle'
        args['TARGET_ENGINE_TYPE'] = 'iceberg'
        
        # Add only source JDBC parameters (target JDBC should be excluded)
        args.update({
            'SOURCE_DB_USER': 'source_user',
            'SOURCE_DB_PASSWORD': 'source_pass',
            'SOURCE_JDBC_DRIVER_S3_PATH': 's3://bucket/source-driver.jar',
            'SOURCE_CONNECTION_STRING': 'jdbc:oracle:thin:@host:1521:db'
        })
        
        # Mock getResolvedOptions to return our test args
        mock_get_resolved_options.return_value = args
        
        # Should not raise any exceptions
        try:
            parsed_args = JobConfigurationParser.parse_job_arguments()
            JobConfigurationParser.validate_required_parameters(parsed_args)
        except Exception as e:
            pytest.fail(f"Validation should pass for JDBC-to-Iceberg: {str(e)}")
    
    @patch('glue_job.config.parsers.getResolvedOptions')
    def test_validate_iceberg_missing_warehouse_location_fails(self, mock_get_resolved_options):
        """Test that missing warehouse location fails validation for Iceberg engines."""
        # Setup arguments for Iceberg-to-Iceberg WITHOUT warehouse locations
        args = self.create_base_args()
        args['SOURCE_ENGINE_TYPE'] = 'iceberg'
        args['TARGET_ENGINE_TYPE'] = 'iceberg'
        # NOT adding warehouse locations
        
        # Mock getResolvedOptions to return our test args
        mock_get_resolved_options.return_value = args
        
        # Should raise an exception due to missing warehouse locations
        with pytest.raises(RuntimeError) as exc_info:
            parsed_args = JobConfigurationParser.parse_job_arguments()
            JobConfigurationParser.validate_required_parameters(parsed_args)
        
        error_message = str(exc_info.value)
        assert 'WAREHOUSE_LOCATION' in error_message
    
    @patch('glue_job.config.parsers.getResolvedOptions')
    def test_validate_jdbc_to_jdbc_requires_all_jdbc_params(self, mock_get_resolved_options):
        """Test that JDBC-to-JDBC requires all traditional JDBC parameters."""
        # Setup arguments for JDBC-to-JDBC with missing JDBC parameters
        args = self.create_base_args()
        args['SOURCE_ENGINE_TYPE'] = 'oracle'
        args['TARGET_ENGINE_TYPE'] = 'postgresql'
        # NOT adding JDBC parameters
        
        # Mock getResolvedOptions to return our test args
        mock_get_resolved_options.return_value = args
        
        # Should raise an exception due to missing JDBC parameters
        with pytest.raises(RuntimeError) as exc_info:
            parsed_args = JobConfigurationParser.parse_job_arguments()
            JobConfigurationParser.validate_required_parameters(parsed_args)
        
        error_message = str(exc_info.value)
        # Should mention missing JDBC parameters
        assert any(param in error_message for param in ['DB_USER', 'DB_PASSWORD', 'CONNECTION_STRING', 'JDBC_DRIVER'])


if __name__ == '__main__':
    pytest.main([__file__, '-v'])