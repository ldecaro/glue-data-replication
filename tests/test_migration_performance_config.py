#!/usr/bin/env python3
"""
Unit tests for migration performance configuration parsing.

This test suite covers:
- MigrationPerformanceConfig dataclass
- JobConfigurationParser.parse_migration_performance_config
"""

import unittest
from unittest.mock import MagicMock
import sys
import os

# Add src directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'src'))

# Mock PySpark and AWS Glue imports for testing
mock_modules = [
    'awsglue', 'awsglue.utils', 'awsglue.context', 'awsglue.job',
    'pyspark', 'pyspark.context', 'pyspark.sql', 'pyspark.sql.types',
    'pyspark.sql.functions'
]

for module in mock_modules:
    sys.modules[module] = MagicMock()

# Import the classes to test
from glue_job.config.job_config import MigrationPerformanceConfig
from glue_job.config.parsers import JobConfigurationParser


class TestMigrationPerformanceConfig(unittest.TestCase):
    """Test MigrationPerformanceConfig dataclass."""
    
    def test_default_config(self):
        """Test default MigrationPerformanceConfig creation."""
        config = MigrationPerformanceConfig()
        
        self.assertEqual(config.counting_strategy, "auto")
        self.assertEqual(config.size_threshold_rows, 1_000_000)
        self.assertFalse(config.force_immediate_counting)
        self.assertFalse(config.force_deferred_counting)
        self.assertEqual(config.progress_update_interval_seconds, 60)
        self.assertEqual(config.progress_batch_size_rows, 100_000)
        self.assertTrue(config.enable_progress_tracking)
        self.assertTrue(config.enable_progress_logging)
        self.assertTrue(config.enable_detailed_metrics)
        self.assertEqual(config.metrics_namespace, "AWS/Glue/DataReplication")
    
    def test_custom_config(self):
        """Test custom MigrationPerformanceConfig creation."""
        config = MigrationPerformanceConfig(
            counting_strategy="deferred",
            size_threshold_rows=500_000,
            force_deferred_counting=True,
            progress_update_interval_seconds=30,
            enable_detailed_metrics=False
        )
        
        self.assertEqual(config.counting_strategy, "deferred")
        self.assertEqual(config.size_threshold_rows, 500_000)
        self.assertTrue(config.force_deferred_counting)
        self.assertEqual(config.progress_update_interval_seconds, 30)
        self.assertFalse(config.enable_detailed_metrics)
    
    def test_validation_success(self):
        """Test successful validation of MigrationPerformanceConfig."""
        config = MigrationPerformanceConfig()
        config.validate()  # Should not raise
    
    def test_validation_invalid_strategy(self):
        """Test validation fails for invalid counting strategy."""
        config = MigrationPerformanceConfig(counting_strategy="invalid")
        
        with self.assertRaises(ValueError) as context:
            config.validate()
        
        self.assertIn("Invalid counting_strategy", str(context.exception))
    
    def test_validation_mutually_exclusive_force_flags(self):
        """Test validation fails for mutually exclusive force flags."""
        config = MigrationPerformanceConfig(
            force_immediate_counting=True,
            force_deferred_counting=True
        )
        
        with self.assertRaises(ValueError) as context:
            config.validate()
        
        self.assertIn("Cannot force both", str(context.exception))
    
    def test_validation_invalid_threshold(self):
        """Test validation fails for invalid size threshold."""
        config = MigrationPerformanceConfig(size_threshold_rows=0)
        
        with self.assertRaises(ValueError) as context:
            config.validate()
        
        self.assertIn("size_threshold_rows must be positive", str(context.exception))
    
    def test_validation_invalid_interval(self):
        """Test validation fails for invalid progress update interval."""
        config = MigrationPerformanceConfig(progress_update_interval_seconds=-1)
        
        with self.assertRaises(ValueError) as context:
            config.validate()
        
        self.assertIn("progress_update_interval_seconds must be positive", str(context.exception))
    
    def test_get_counting_strategy_config(self):
        """Test getting CountingStrategyConfig from MigrationPerformanceConfig."""
        config = MigrationPerformanceConfig(
            counting_strategy="deferred",
            size_threshold_rows=500_000,
            force_deferred_counting=True
        )
        
        strategy_config = config.get_counting_strategy_config()
        
        from glue_job.database.counting_strategy import CountingStrategyType
        self.assertEqual(strategy_config.strategy_type, CountingStrategyType.DEFERRED)
        self.assertEqual(strategy_config.size_threshold_rows, 500_000)
        self.assertTrue(strategy_config.force_deferred)
    
    def test_get_streaming_progress_config(self):
        """Test getting StreamingProgressConfig from MigrationPerformanceConfig."""
        config = MigrationPerformanceConfig(
            progress_update_interval_seconds=30,
            progress_batch_size_rows=50_000,
            enable_detailed_metrics=False
        )
        
        progress_config = config.get_streaming_progress_config()
        
        self.assertEqual(progress_config.update_interval_seconds, 30)
        self.assertEqual(progress_config.batch_size_rows, 50_000)
        self.assertFalse(progress_config.enable_metrics)


class TestJobConfigurationParserPerformanceConfig(unittest.TestCase):
    """Test JobConfigurationParser migration performance config parsing."""
    
    def test_parse_default_config(self):
        """Test parsing with default values."""
        args = {}
        
        config = JobConfigurationParser.parse_migration_performance_config(args)
        
        self.assertEqual(config.counting_strategy, "auto")
        self.assertEqual(config.size_threshold_rows, 1_000_000)
        self.assertFalse(config.force_immediate_counting)
        self.assertFalse(config.force_deferred_counting)
        self.assertEqual(config.progress_update_interval_seconds, 60)
        self.assertTrue(config.enable_progress_tracking)
        self.assertTrue(config.enable_detailed_metrics)
    
    def test_parse_custom_config(self):
        """Test parsing with custom values."""
        args = {
            'COUNTING_STRATEGY': 'deferred',
            'SIZE_THRESHOLD_ROWS': '500000',
            'FORCE_DEFERRED_COUNTING': 'true',
            'PROGRESS_UPDATE_INTERVAL_SECONDS': '30',
            'ENABLE_PROGRESS_TRACKING': 'false',
            'ENABLE_DETAILED_METRICS': 'false',
            'METRICS_NAMESPACE': 'Custom/Metrics'
        }
        
        config = JobConfigurationParser.parse_migration_performance_config(args)
        
        self.assertEqual(config.counting_strategy, "deferred")
        self.assertEqual(config.size_threshold_rows, 500_000)
        self.assertTrue(config.force_deferred_counting)
        self.assertEqual(config.progress_update_interval_seconds, 30)
        self.assertFalse(config.enable_progress_tracking)
        self.assertFalse(config.enable_detailed_metrics)
        self.assertEqual(config.metrics_namespace, "Custom/Metrics")
    
    def test_parse_boolean_variations(self):
        """Test parsing boolean values with different formats."""
        # Test 'true' variations
        for value in ['true', 'True', 'TRUE', '1', 'yes', 'Yes']:
            args = {'ENABLE_PROGRESS_TRACKING': value}
            config = JobConfigurationParser.parse_migration_performance_config(args)
            self.assertTrue(config.enable_progress_tracking, f"Failed for value: {value}")
        
        # Test 'false' variations
        for value in ['false', 'False', 'FALSE', '0', 'no', 'No']:
            args = {'ENABLE_PROGRESS_TRACKING': value}
            config = JobConfigurationParser.parse_migration_performance_config(args)
            self.assertFalse(config.enable_progress_tracking, f"Failed for value: {value}")
    
    def test_parse_invalid_strategy_defaults_to_auto(self):
        """Test that invalid counting strategy defaults to 'auto' with warning."""
        args = {'COUNTING_STRATEGY': 'invalid_strategy'}
        
        config = JobConfigurationParser.parse_migration_performance_config(args)
        
        # Should default to 'auto' when invalid
        self.assertEqual(config.counting_strategy, "auto")
    
    def test_parse_validates_config(self):
        """Test that parsing validates the configuration."""
        args = {
            'FORCE_IMMEDIATE_COUNTING': 'true',
            'FORCE_DEFERRED_COUNTING': 'true'
        }
        
        # Should raise ValueError due to mutually exclusive flags
        with self.assertRaises(ValueError):
            JobConfigurationParser.parse_migration_performance_config(args)


if __name__ == '__main__':
    unittest.main()
