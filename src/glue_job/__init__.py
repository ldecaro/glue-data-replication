"""
AWS Glue Data Replication Job Package

This package contains modular components for AWS Glue data replication jobs,
supporting full-load and incremental data migration across multiple database types.
"""

__version__ = "1.0.0"
__author__ = "AWS Glue Data Replication Team"

# Package-level imports for convenience (only core classes without PySpark dependencies)
from .config.job_config import JobConfig, NetworkConfig, ConnectionConfig
from .config.database_engines import DatabaseEngineManager, JdbcDriverLoader
from .config.parsers import JobConfigurationParser, ConnectionStringBuilder
from .storage.s3_bookmark import S3BookmarkConfig, S3BookmarkStorage
from .monitoring.logging import StructuredLogger
from .utils.s3_utils import S3PathUtilities

# Note: Some modules with PySpark dependencies are not imported at package level
# to avoid import errors in environments without PySpark. Import them directly:
# from glue_job.storage.bookmark_manager import JobBookmarkManager, JobBookmarkState
# from glue_job.database import *
# from glue_job.monitoring.metrics import *

__all__ = [
    'JobConfig',
    'NetworkConfig', 
    'ConnectionConfig',
    'DatabaseEngineManager',
    'JdbcDriverLoader',
    'JobConfigurationParser',
    'ConnectionStringBuilder',
    'S3BookmarkConfig',
    'S3BookmarkStorage',
    'StructuredLogger',
    'S3PathUtilities'
]