"""
Progress tracking classes for AWS Glue Data Replication.

This module provides data classes for tracking processing metrics,
full-load progress, and incremental-load progress during job execution.
"""

import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, Optional


@dataclass
class ProcessingMetrics:
    """Data class to track processing metrics for a table."""
    table_name: str
    start_time: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    end_time: Optional[datetime] = None
    rows_processed: int = 0
    rows_inserted: int = 0
    rows_updated: int = 0
    rows_failed: int = 0
    bytes_processed: int = 0
    processing_duration_seconds: float = 0.0
    status: str = "in_progress"  # in_progress, completed, failed
    error_message: Optional[str] = None
    
    def mark_completed(self, rows_processed: int = 0, bytes_processed: int = 0):
        """Mark processing as completed and calculate duration."""
        self.end_time = datetime.now(timezone.utc)
        self.processing_duration_seconds = (self.end_time - self.start_time).total_seconds()
        self.rows_processed = rows_processed
        self.bytes_processed = bytes_processed
        self.status = "completed"
    
    def mark_failed(self, error_message: str):
        """Mark processing as failed."""
        self.end_time = datetime.now(timezone.utc)
        self.processing_duration_seconds = (self.end_time - self.start_time).total_seconds()
        self.status = "failed"
        self.error_message = error_message
    
    def get_throughput_rows_per_second(self) -> float:
        """Calculate rows processed per second."""
        if self.processing_duration_seconds > 0:
            return self.rows_processed / self.processing_duration_seconds
        return 0.0
    
    def get_throughput_mb_per_second(self) -> float:
        """Calculate MB processed per second."""
        if self.processing_duration_seconds > 0:
            return (self.bytes_processed / 1024 / 1024) / self.processing_duration_seconds
        return 0.0


@dataclass
class FullLoadProgress:
    """Tracks progress of full-load operations with streaming support."""
    table_name: str
    total_rows: int = 0
    processed_rows: int = 0
    start_time: float = 0.0
    end_time: Optional[float] = None
    status: str = 'pending'  # pending, in_progress, completed, failed
    error_message: Optional[str] = None
    
    # Streaming progress fields
    counting_strategy: str = 'auto'  # immediate, deferred, auto
    rows_counted_at: str = 'unknown'  # before_write, after_write, unknown
    last_progress_update: float = 0.0
    progress_updates_count: int = 0
    
    # Performance metrics - phase-specific duration fields
    read_duration_seconds: float = 0.0
    write_duration_seconds: float = 0.0
    count_duration_seconds: float = 0.0
    
    @property
    def progress_percentage(self) -> float:
        """Calculate progress percentage."""
        if self.total_rows == 0:
            return 0.0
        return (self.processed_rows / self.total_rows) * 100.0
    
    @property
    def duration_seconds(self) -> float:
        """Calculate operation duration in seconds."""
        if self.start_time == 0.0:
            return 0.0
        end_time = self.end_time or time.time()
        return end_time - self.start_time
    
    @property
    def total_duration_seconds(self) -> float:
        """Total operation duration including all phases."""
        return self.read_duration_seconds + self.write_duration_seconds + self.count_duration_seconds
    
    @property
    def rows_per_second(self) -> float:
        """Calculate processing rate in rows per second."""
        duration = self.duration_seconds
        if duration == 0.0:
            return 0.0
        return self.processed_rows / duration
    
    @property
    def effective_rows_per_second(self) -> float:
        """Effective throughput excluding counting overhead."""
        if self.write_duration_seconds == 0.0:
            return 0.0
        return self.processed_rows / self.write_duration_seconds


@dataclass
class IncrementalLoadProgress:
    """Tracks progress of incremental-load operations."""
    table_name: str
    incremental_strategy: str  # timestamp, primary_key, hash
    incremental_column: Optional[str] = None
    last_processed_value: Optional[Any] = None
    current_max_value: Optional[Any] = None
    delta_rows: int = 0
    processed_rows: int = 0
    start_time: float = 0.0
    end_time: Optional[float] = None
    status: str = 'pending'  # pending, in_progress, completed, failed
    error_message: Optional[str] = None
    bookmark_state: Optional[Dict[str, Any]] = None
    
    @property
    def progress_percentage(self) -> float:
        """Calculate progress percentage."""
        if self.delta_rows == 0:
            return 100.0 if self.status == 'completed' else 0.0
        return (self.processed_rows / self.delta_rows) * 100.0
    
    @property
    def duration_seconds(self) -> float:
        """Calculate operation duration in seconds."""
        if self.start_time == 0.0:
            return 0.0
        end_time = self.end_time or time.time()
        return end_time - self.start_time
    
    @property
    def rows_per_second(self) -> float:
        """Calculate processing rate in rows per second."""
        duration = self.duration_seconds
        if duration == 0.0:
            return 0.0
        return self.processed_rows / duration