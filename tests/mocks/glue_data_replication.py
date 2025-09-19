"""
Mock glue_data_replication module for testing.

This module provides mock implementations of AWS Glue utilities
that are used in tests but not available in the test environment.
"""

import sys
from typing import Dict, Any, List
from unittest.mock import Mock

# Mock AWS Glue utilities for testing
def getResolvedOptions(args: List[str]) -> Dict[str, Any]:
    """Mock implementation of AWS Glue getResolvedOptions."""
    # Return a mock set of resolved options for testing
    return {
        'JOB_NAME': 'test-replication-job',
        'SOURCE_ENGINE_TYPE': 'postgresql',
        'TARGET_ENGINE_TYPE': 'oracle',
        'SOURCE_DATABASE': 'sourcedb',
        'TARGET_DATABASE': 'targetdb',
        'SOURCE_SCHEMA': 'public',
        'TARGET_SCHEMA': 'target_schema',
        'TABLE_NAMES': 'table1,table2,table3',
        'SOURCE_DB_USER': 'sourceuser',
        'SOURCE_DB_PASSWORD': 'sourcepass',
        'TARGET_DB_USER': 'targetuser',
        'TARGET_DB_PASSWORD': 'targetpass',
        'SOURCE_JDBC_DRIVER_S3_PATH': 's3://bucket/postgresql-driver.jar',
        'TARGET_JDBC_DRIVER_S3_PATH': 's3://bucket/oracle-driver.jar',
        'SOURCE_CONNECTION_STRING': 'jdbc:postgresql://source:5432/sourcedb',
        'TARGET_CONNECTION_STRING': 'jdbc:oracle:thin:@target:1521:targetdb'
    }

# Mock classes for testing
class MockGlueContext:
    """Mock GlueContext for testing."""
    
    def __init__(self):
        self.spark_session = Mock()
    
    def get_bookmark_state(self, table_name: str):
        """Mock bookmark state retrieval."""
        return None

class MockJob:
    """Mock Job for testing."""
    
    @staticmethod
    def init(args, glue_context):
        """Mock job initialization."""
        pass
    
    @staticmethod
    def commit():
        """Mock job commit."""
        pass

# Export mock objects
Job = MockJob
GlueContext = MockGlueContext

# Mock logger
logger = Mock()