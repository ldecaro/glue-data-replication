"""
Test bookmark value formatting for SQL queries.

This test verifies that bookmark values are properly formatted for different
database engines to prevent SQL conversion errors.
"""

import pytest
from unittest.mock import Mock, MagicMock, patch
from datetime import datetime

# Mock AWS Glue dependencies
import sys
sys.modules['awsglue'] = MagicMock()
sys.modules['awsglue.context'] = MagicMock()
sys.modules['awsglue.job'] = MagicMock()
sys.modules['awsglue.utils'] = MagicMock()
sys.modules['pyspark'] = MagicMock()
sys.modules['pyspark.sql'] = MagicMock()
sys.modules['pyspark.sql.functions'] = MagicMock()

from src.glue_job.database.migration import IncrementalDataMigrator
from src.glue_job.config.job_config import ConnectionConfig


class TestBookmarkValueFormatting:
    """Test bookmark value formatting for different database engines."""
    
    @pytest.fixture
    def migrator(self):
        """Create an IncrementalDataMigrator instance for testing."""
        mock_spark = Mock()
        mock_connection_manager = Mock()
        mock_bookmark_manager = Mock()
        mock_metrics_publisher = Mock()
        
        migrator = IncrementalDataMigrator(
            spark_session=mock_spark,
            connection_manager=mock_connection_manager,
            bookmark_manager=mock_bookmark_manager,
            metrics_publisher=mock_metrics_publisher
        )
        
        return migrator
    
    def test_format_sqlserver_datetime(self, migrator):
        """Test SQL Server datetime formatting."""
        value = "2025-08-17 16:27:06.407000"
        result = migrator._format_bookmark_value_for_sql(value, "sqlserver", "last_modified")
        
        assert result == "CONVERT(DATETIME2, '2025-08-17 16:27:06.407000', 121)"
    
    def test_format_sqlserver_integer(self, migrator):
        """Test SQL Server integer formatting."""
        value = "12345"
        result = migrator._format_bookmark_value_for_sql(value, "sqlserver", "id")
        
        assert result == "'12345'"
    
    def test_format_oracle_datetime(self, migrator):
        """Test Oracle datetime formatting."""
        value = "2025-08-17 16:27:06.407000"
        result = migrator._format_bookmark_value_for_sql(value, "oracle", "last_modified")
        
        assert result == "TO_TIMESTAMP('2025-08-17 16:27:06.407000', 'YYYY-MM-DD HH24:MI:SS.FF')"
    
    def test_format_oracle_datetime_no_fractional(self, migrator):
        """Test Oracle datetime formatting without fractional seconds."""
        value = "2025-08-17 16:27:06"
        result = migrator._format_bookmark_value_for_sql(value, "oracle", "last_modified")
        
        assert result == "TO_TIMESTAMP('2025-08-17 16:27:06', 'YYYY-MM-DD HH24:MI:SS')"
    
    def test_format_postgresql_datetime(self, migrator):
        """Test PostgreSQL datetime formatting."""
        value = "2025-08-17 16:27:06.407000"
        result = migrator._format_bookmark_value_for_sql(value, "postgresql", "last_modified")
        
        assert result == "'2025-08-17 16:27:06.407000'::timestamp"
    
    def test_format_corrupted_datetime(self, migrator):
        """Test handling of corrupted datetime values (concatenated dates)."""
        # Simulate the bug where two datetimes get concatenated
        corrupted_value = "2025-08-17 16:27:06.4070002025-11-03 02:38:18.048949"
        result = migrator._format_bookmark_value_for_sql(corrupted_value, "sqlserver", "last_modified")
        
        # Should extract just the first datetime portion
        assert "2025-08-17 16:27:06.407000" in result
        assert "CONVERT(DATETIME2" in result
        # Should not contain the second date
        assert "2025-11-03" not in result
    
    def test_format_null_value(self, migrator):
        """Test NULL value formatting."""
        result = migrator._format_bookmark_value_for_sql(None, "sqlserver", "last_modified")
        
        assert result == "NULL"
    
    def test_format_generic_database(self, migrator):
        """Test formatting for generic database engine."""
        value = "2025-08-17 16:27:06.407000"
        result = migrator._format_bookmark_value_for_sql(value, "db2", "last_modified")
        
        # Should use simple quoting for unknown engines
        assert result == "'2025-08-17 16:27:06.407000'"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
