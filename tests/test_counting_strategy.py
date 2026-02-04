"""
Tests for counting strategy component.

This module tests the CountingStrategy class and its configuration.
"""

import pytest
from unittest.mock import Mock, MagicMock, patch
from src.glue_job.database.counting_strategy import (
    CountingStrategy,
    CountingStrategyType,
    CountingStrategyConfig
)
from src.glue_job.config.job_config import ConnectionConfig


class TestCountingStrategyConfig:
    """Tests for CountingStrategyConfig dataclass."""
    
    def test_default_configuration(self):
        """Test default configuration values."""
        config = CountingStrategyConfig()
        assert config.strategy_type == CountingStrategyType.AUTO
        assert config.size_threshold_rows == 1_000_000
        assert config.force_immediate is False
        assert config.force_deferred is False
    
    def test_custom_configuration(self):
        """Test custom configuration values."""
        config = CountingStrategyConfig(
            strategy_type=CountingStrategyType.DEFERRED,
            size_threshold_rows=500_000,
            force_deferred=True
        )
        assert config.strategy_type == CountingStrategyType.DEFERRED
        assert config.size_threshold_rows == 500_000
        assert config.force_deferred is True
    
    def test_validation_both_forced(self):
        """Test validation fails when both immediate and deferred are forced."""
        config = CountingStrategyConfig(
            force_immediate=True,
            force_deferred=True
        )
        with pytest.raises(ValueError, match="Cannot force both immediate and deferred strategies"):
            config.validate()
    
    def test_validation_invalid_threshold(self):
        """Test validation fails with invalid threshold."""
        config = CountingStrategyConfig(size_threshold_rows=0)
        with pytest.raises(ValueError, match="Size threshold must be positive"):
            config.validate()
        
        config = CountingStrategyConfig(size_threshold_rows=-100)
        with pytest.raises(ValueError, match="Size threshold must be positive"):
            config.validate()
    
    def test_validation_success(self):
        """Test validation succeeds with valid configuration."""
        config = CountingStrategyConfig(
            strategy_type=CountingStrategyType.IMMEDIATE,
            size_threshold_rows=1_000_000
        )
        config.validate()  # Should not raise


class TestCountingStrategy:
    """Tests for CountingStrategy class."""
    
    def test_initialization(self):
        """Test CountingStrategy initialization."""
        config = CountingStrategyConfig()
        strategy = CountingStrategy(config)
        assert strategy.config == config
        assert strategy.structured_logger is not None
    
    def test_select_strategy_forced_immediate(self):
        """Test strategy selection with forced immediate."""
        config = CountingStrategyConfig(force_immediate=True)
        strategy = CountingStrategy(config)
        
        result = strategy.select_strategy(None, "test_table", "postgresql")
        assert result == CountingStrategyType.IMMEDIATE
    
    def test_select_strategy_forced_deferred(self):
        """Test strategy selection with forced deferred."""
        config = CountingStrategyConfig(force_deferred=True)
        strategy = CountingStrategy(config)
        
        result = strategy.select_strategy(None, "test_table", "postgresql")
        assert result == CountingStrategyType.DEFERRED
    
    def test_select_strategy_explicit_immediate(self):
        """Test strategy selection with explicit immediate configuration."""
        config = CountingStrategyConfig(strategy_type=CountingStrategyType.IMMEDIATE)
        strategy = CountingStrategy(config)
        
        result = strategy.select_strategy(None, "test_table", "postgresql")
        assert result == CountingStrategyType.IMMEDIATE
    
    def test_select_strategy_explicit_deferred(self):
        """Test strategy selection with explicit deferred configuration."""
        config = CountingStrategyConfig(strategy_type=CountingStrategyType.DEFERRED)
        strategy = CountingStrategy(config)
        
        result = strategy.select_strategy(None, "test_table", "postgresql")
        assert result == CountingStrategyType.DEFERRED
    
    @patch('src.glue_job.config.database_engines.DatabaseEngineManager')
    def test_auto_select_iceberg_engine(self, mock_engine_manager):
        """Test auto selection for Iceberg engine - now uses IMMEDIATE with SQL COUNT."""
        mock_engine_manager.is_iceberg_engine.return_value = True
        
        config = CountingStrategyConfig(strategy_type=CountingStrategyType.AUTO)
        strategy = CountingStrategy(config)
        
        result = strategy.select_strategy(None, "test_table", "iceberg")
        # AUTO now always selects IMMEDIATE for SQL COUNT optimization
        assert result == CountingStrategyType.IMMEDIATE
    
    @patch('src.glue_job.config.database_engines.DatabaseEngineManager')
    def test_auto_select_jdbc_engine(self, mock_engine_manager):
        """Test auto selection for JDBC engine - now uses IMMEDIATE with SQL COUNT."""
        mock_engine_manager.is_iceberg_engine.return_value = False
        
        config = CountingStrategyConfig(strategy_type=CountingStrategyType.AUTO)
        strategy = CountingStrategy(config)
        
        result = strategy.select_strategy(None, "test_table", "postgresql")
        # AUTO now always selects IMMEDIATE for SQL COUNT optimization
        assert result == CountingStrategyType.IMMEDIATE
    
    @patch('src.glue_job.config.database_engines.DatabaseEngineManager')
    def test_auto_select_small_dataset(self, mock_engine_manager):
        """Test auto selection for small dataset - now uses IMMEDIATE regardless of size."""
        mock_engine_manager.is_iceberg_engine.return_value = False
        
        config = CountingStrategyConfig(
            strategy_type=CountingStrategyType.AUTO,
            size_threshold_rows=1_000_000
        )
        strategy = CountingStrategy(config)
        
        # estimated_size is now ignored - always uses IMMEDIATE
        result = strategy.select_strategy(None, "test_table", "postgresql", estimated_size=500_000)
        assert result == CountingStrategyType.IMMEDIATE
    
    @patch('src.glue_job.config.database_engines.DatabaseEngineManager')
    def test_auto_select_large_dataset(self, mock_engine_manager):
        """Test auto selection for large dataset - now uses IMMEDIATE regardless of size."""
        mock_engine_manager.is_iceberg_engine.return_value = False
        
        config = CountingStrategyConfig(
            strategy_type=CountingStrategyType.AUTO,
            size_threshold_rows=1_000_000
        )
        strategy = CountingStrategy(config)
        
        # estimated_size is now ignored - always uses IMMEDIATE
        result = strategy.select_strategy(None, "test_table", "postgresql", estimated_size=5_000_000)
        assert result == CountingStrategyType.IMMEDIATE
    
    @patch('src.glue_job.config.database_engines.DatabaseEngineManager')
    def test_auto_select_unknown_size(self, mock_engine_manager):
        """Test auto selection with unknown size - now uses IMMEDIATE."""
        mock_engine_manager.is_iceberg_engine.return_value = False
        
        config = CountingStrategyConfig(strategy_type=CountingStrategyType.AUTO)
        strategy = CountingStrategy(config)
        
        # Unknown size now uses IMMEDIATE (SQL COUNT is fast for all sizes)
        result = strategy.select_strategy(None, "test_table", "postgresql", estimated_size=None)
        assert result == CountingStrategyType.IMMEDIATE
    
    def test_execute_immediate_count(self):
        """Test immediate count execution."""
        config = CountingStrategyConfig()
        strategy = CountingStrategy(config)
        
        # Mock DataFrame
        mock_df = Mock()
        mock_df.count.return_value = 1000
        
        result = strategy.execute_immediate_count(mock_df, "test_table")
        assert result == 1000
        mock_df.count.assert_called_once()
    
    def test_execute_immediate_count_error(self):
        """Test immediate count execution with error."""
        config = CountingStrategyConfig()
        strategy = CountingStrategy(config)
        
        # Mock DataFrame that raises error
        mock_df = Mock()
        mock_df.count.side_effect = Exception("Count failed")
        
        with pytest.raises(Exception, match="Count failed"):
            strategy.execute_immediate_count(mock_df, "test_table")
    
    @patch('src.glue_job.config.database_engines.DatabaseEngineManager')
    def test_execute_deferred_count_jdbc(self, mock_engine_manager):
        """Test deferred count execution for JDBC table."""
        mock_engine_manager.is_iceberg_engine.return_value = False
        
        config = CountingStrategyConfig()
        strategy = CountingStrategy(config)
        
        # Mock connection manager and target config
        mock_connection_manager = Mock()
        mock_target_config = Mock()
        mock_target_config.engine_type = "postgresql"
        mock_target_config.schema = "public"
        
        # Mock count result
        mock_count_df = Mock()
        mock_count_result = {'row_count': 2000}
        mock_count_df.collect.return_value = [mock_count_result]
        mock_connection_manager.read_table_data.return_value = mock_count_df
        
        result = strategy.execute_deferred_count(
            mock_target_config, 
            "test_table", 
            mock_connection_manager
        )
        
        assert result == 2000
        mock_connection_manager.read_table_data.assert_called_once()
    
    @patch('src.glue_job.config.database_engines.DatabaseEngineManager')
    def test_execute_deferred_count_iceberg(self, mock_engine_manager):
        """Test deferred count execution for Iceberg table with metadata optimization."""
        mock_engine_manager.is_iceberg_engine.return_value = True
        
        config = CountingStrategyConfig()
        strategy = CountingStrategy(config)
        
        # Mock connection manager and target config
        mock_connection_manager = Mock()
        mock_spark = Mock()
        mock_connection_manager.spark = mock_spark
        
        mock_target_config = Mock()
        mock_target_config.engine_type = "iceberg"
        mock_target_config.database = "test_db"
        
        # Mock Iceberg config
        mock_iceberg_config = {
            'database_name': 'test_db',
            'catalog_name': 'glue_catalog',
            'warehouse_location': 's3://test-bucket/warehouse'
        }
        mock_target_config.get_iceberg_config.return_value = mock_iceberg_config
        
        # Mock Spark SQL query result for metadata-based counting
        mock_count_df = Mock()
        mock_count_result = {'row_count': 3000}
        mock_count_df.collect.return_value = [mock_count_result]
        mock_spark.sql.return_value = mock_count_df
        
        result = strategy.execute_deferred_count(
            mock_target_config, 
            "test_table", 
            mock_connection_manager
        )
        
        assert result == 3000
        # Verify that Spark SQL was used for metadata-based counting
        mock_spark.sql.assert_called_once()
        call_args = mock_spark.sql.call_args[0][0]
        assert "SELECT COUNT(*)" in call_args
        assert "glue_catalog.test_db.test_table" in call_args
    
    @patch('src.glue_job.config.database_engines.DatabaseEngineManager')
    def test_execute_deferred_count_iceberg_fallback(self, mock_engine_manager):
        """Test deferred count execution for Iceberg with fallback to DataFrame count."""
        mock_engine_manager.is_iceberg_engine.return_value = True
        
        config = CountingStrategyConfig()
        strategy = CountingStrategy(config)
        
        # Mock connection manager and target config
        mock_connection_manager = Mock()
        mock_spark = Mock()
        mock_connection_manager.spark = mock_spark
        
        mock_target_config = Mock()
        mock_target_config.engine_type = "iceberg"
        mock_target_config.database = "test_db"
        
        # Mock Iceberg config
        mock_iceberg_config = {
            'database_name': 'test_db',
            'catalog_name': 'glue_catalog',
            'warehouse_location': 's3://test-bucket/warehouse'
        }
        mock_target_config.get_iceberg_config.return_value = mock_iceberg_config
        
        # Mock Spark SQL to fail (triggering fallback)
        mock_spark.sql.side_effect = Exception("Metadata query failed")
        
        # Mock DataFrame fallback
        mock_df = Mock()
        mock_df.count.return_value = 2500
        mock_connection_manager.read_table.return_value = mock_df
        
        result = strategy.execute_deferred_count(
            mock_target_config, 
            "test_table", 
            mock_connection_manager
        )
        
        assert result == 2500
        # Verify fallback to DataFrame count was used
        mock_connection_manager.read_table.assert_called_once()
        mock_df.count.assert_called_once()
    
    @patch('src.glue_job.config.database_engines.DatabaseEngineManager')
    def test_execute_deferred_count_error(self, mock_engine_manager):
        """Test deferred count execution with error."""
        mock_engine_manager.is_iceberg_engine.return_value = False
        
        config = CountingStrategyConfig()
        strategy = CountingStrategy(config)
        
        # Mock connection manager that raises error
        mock_connection_manager = Mock()
        mock_connection_manager.read_table_data.side_effect = Exception("Query failed")
        
        mock_target_config = Mock()
        mock_target_config.engine_type = "postgresql"
        mock_target_config.schema = "public"
        
        with pytest.raises(Exception, match="Query failed"):
            strategy.execute_deferred_count(
                mock_target_config, 
                "test_table", 
                mock_connection_manager
            )


class TestSQLCountOptimization:
    """Tests for SQL COUNT(*) optimization (Task 23)."""
    
    @patch('src.glue_job.config.database_engines.DatabaseEngineManager')
    def test_execute_immediate_count_sql_jdbc(self, mock_engine_manager):
        """Test SQL COUNT(*) for JDBC table."""
        mock_engine_manager.is_iceberg_engine.return_value = False
        
        config = CountingStrategyConfig()
        strategy = CountingStrategy(config)
        
        # Mock connection manager and source config
        mock_connection_manager = Mock()
        mock_source_config = Mock()
        mock_source_config.engine_type = "postgresql"
        mock_source_config.schema = "public"
        
        # Mock count result
        mock_count_df = Mock()
        mock_count_result = {'row_count': 5000}
        mock_count_df.collect.return_value = [mock_count_result]
        mock_connection_manager.read_table_data.return_value = mock_count_df
        
        result = strategy.execute_immediate_count_sql(
            mock_source_config, 
            "test_table", 
            mock_connection_manager
        )
        
        assert result == 5000
        mock_connection_manager.read_table_data.assert_called_once()
        # Verify the query contains COUNT(*)
        call_args = mock_connection_manager.read_table_data.call_args
        assert "COUNT(*)" in call_args[1]['query']
    
    @patch('src.glue_job.config.database_engines.DatabaseEngineManager')
    def test_execute_immediate_count_sql_iceberg(self, mock_engine_manager):
        """Test SQL COUNT(*) for Iceberg table using Spark SQL."""
        mock_engine_manager.is_iceberg_engine.return_value = True
        
        config = CountingStrategyConfig()
        strategy = CountingStrategy(config)
        
        # Mock connection manager and source config
        mock_connection_manager = Mock()
        mock_spark = Mock()
        mock_connection_manager.spark = mock_spark
        
        mock_source_config = Mock()
        mock_source_config.engine_type = "iceberg"
        mock_source_config.database = "test_db"
        
        # Mock Iceberg config
        mock_iceberg_config = {
            'database_name': 'test_db',
            'catalog_name': 'glue_catalog',
            'warehouse_location': 's3://test-bucket/warehouse'
        }
        mock_source_config.get_iceberg_config.return_value = mock_iceberg_config
        
        # Mock Spark SQL query result
        mock_count_df = Mock()
        mock_count_result = {'row_count': 10000}
        mock_count_df.collect.return_value = [mock_count_result]
        mock_spark.sql.return_value = mock_count_df
        
        result = strategy.execute_immediate_count_sql(
            mock_source_config, 
            "test_table", 
            mock_connection_manager
        )
        
        assert result == 10000
        # Verify Spark SQL was used
        mock_spark.sql.assert_called_once()
        call_args = mock_spark.sql.call_args[0][0]
        assert "SELECT COUNT(*)" in call_args
        assert "glue_catalog.test_db.test_table" in call_args
    
    @patch('src.glue_job.config.database_engines.DatabaseEngineManager')
    def test_execute_immediate_count_sql_error(self, mock_engine_manager):
        """Test SQL COUNT(*) error handling."""
        mock_engine_manager.is_iceberg_engine.return_value = False
        
        config = CountingStrategyConfig()
        strategy = CountingStrategy(config)
        
        # Mock connection manager that raises error
        mock_connection_manager = Mock()
        mock_connection_manager.read_table_data.side_effect = Exception("Connection failed")
        
        mock_source_config = Mock()
        mock_source_config.engine_type = "postgresql"
        mock_source_config.schema = "public"
        
        with pytest.raises(Exception, match="Connection failed"):
            strategy.execute_immediate_count_sql(
                mock_source_config, 
                "test_table", 
                mock_connection_manager
            )
    
    @patch('src.glue_job.config.database_engines.DatabaseEngineManager')
    def test_execute_immediate_count_sql_with_fallback_success(self, mock_engine_manager):
        """Test SQL COUNT(*) with fallback - success case."""
        mock_engine_manager.is_iceberg_engine.return_value = False
        
        config = CountingStrategyConfig()
        strategy = CountingStrategy(config)
        
        # Mock connection manager and source config
        mock_connection_manager = Mock()
        mock_source_config = Mock()
        mock_source_config.engine_type = "postgresql"
        mock_source_config.schema = "public"
        
        # Mock count result
        mock_count_df = Mock()
        mock_count_result = {'row_count': 7500}
        mock_count_df.collect.return_value = [mock_count_result]
        mock_connection_manager.read_table_data.return_value = mock_count_df
        
        row_count, method = strategy.execute_immediate_count_sql_with_fallback(
            mock_source_config, 
            "test_table", 
            mock_connection_manager
        )
        
        assert row_count == 7500
        assert method == 'immediate_sql'
    
    @patch('src.glue_job.config.database_engines.DatabaseEngineManager')
    def test_execute_immediate_count_sql_with_fallback_failure(self, mock_engine_manager):
        """Test SQL COUNT(*) with fallback - fallback case."""
        mock_engine_manager.is_iceberg_engine.return_value = False
        
        config = CountingStrategyConfig()
        strategy = CountingStrategy(config)
        
        # Mock connection manager that raises error
        mock_connection_manager = Mock()
        mock_connection_manager.read_table_data.side_effect = Exception("Connection failed")
        
        mock_source_config = Mock()
        mock_source_config.engine_type = "postgresql"
        mock_source_config.schema = "public"
        
        row_count, method = strategy.execute_immediate_count_sql_with_fallback(
            mock_source_config, 
            "test_table", 
            mock_connection_manager
        )
        
        assert row_count is None
        assert method == 'deferred_fallback'
    
    @patch('src.glue_job.config.database_engines.DatabaseEngineManager')
    def test_execute_immediate_count_sql_with_metrics(self, mock_engine_manager):
        """Test SQL COUNT(*) publishes metrics."""
        mock_engine_manager.is_iceberg_engine.return_value = False
        
        mock_metrics_publisher = Mock()
        config = CountingStrategyConfig()
        strategy = CountingStrategy(config, metrics_publisher=mock_metrics_publisher)
        
        # Mock connection manager and source config
        mock_connection_manager = Mock()
        mock_source_config = Mock()
        mock_source_config.engine_type = "postgresql"
        mock_source_config.schema = "public"
        
        # Mock count result
        mock_count_df = Mock()
        mock_count_result = {'row_count': 3000}
        mock_count_df.collect.return_value = [mock_count_result]
        mock_connection_manager.read_table_data.return_value = mock_count_df
        
        result = strategy.execute_immediate_count_sql(
            mock_source_config, 
            "test_table", 
            mock_connection_manager
        )
        
        assert result == 3000
        # Verify metrics were published
        mock_metrics_publisher.publish_counting_strategy_metrics.assert_called_once()
        call_kwargs = mock_metrics_publisher.publish_counting_strategy_metrics.call_args[1]
        assert call_kwargs['table_name'] == 'test_table'
        assert call_kwargs['strategy_type'] == 'immediate_sql'
        assert call_kwargs['row_count'] == 3000
    
    @patch('src.glue_job.config.database_engines.DatabaseEngineManager')
    def test_execute_immediate_count_sql_iceberg_no_config(self, mock_engine_manager):
        """Test SQL COUNT(*) for Iceberg table without config raises error."""
        mock_engine_manager.is_iceberg_engine.return_value = True
        
        config = CountingStrategyConfig()
        strategy = CountingStrategy(config)
        
        # Mock connection manager and source config
        mock_connection_manager = Mock()
        mock_source_config = Mock()
        mock_source_config.engine_type = "iceberg"
        mock_source_config.database = "test_db"
        mock_source_config.get_iceberg_config.return_value = None
        
        with pytest.raises(ValueError, match="No Iceberg config found"):
            strategy.execute_immediate_count_sql(
                mock_source_config, 
                "test_table", 
                mock_connection_manager
            )



if __name__ == "__main__":
    pytest.main([__file__, "-v"])
