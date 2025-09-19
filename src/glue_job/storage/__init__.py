"""
Storage module for AWS Glue Data Replication.

This module provides storage and bookmark management functionality including:
- S3 bookmark storage and configuration
- Job bookmark management with persistent state
- Progress tracking for full-load and incremental operations
- Manual bookmark configuration and strategy resolution
"""

from .s3_bookmark import S3BookmarkConfig, S3BookmarkStorage
from .bookmark_manager import (
    JobBookmarkManager, 
    JobBookmarkState, 
    FullLoadProgress, 
    IncrementalLoadProgress
)
from .manual_bookmark_config import ManualBookmarkConfig, BookmarkStrategyResolver

__all__ = [
    'S3BookmarkConfig',
    'S3BookmarkStorage', 
    'JobBookmarkManager',
    'JobBookmarkState',
    'FullLoadProgress',
    'IncrementalLoadProgress',
    'ManualBookmarkConfig',
    'BookmarkStrategyResolver'
]