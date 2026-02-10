"""
Streaming progress tracker for real-time migration monitoring.

This module provides the StreamingProgressTracker class that tracks and reports
real-time progress during data transfer operations for both full load and 
incremental load migrations.
"""

import time
from dataclasses import dataclass
from typing import Optional

from .logging import StructuredLogger
from .metrics import CloudWatchMetricsPublisher


@dataclass
class StreamingProgressConfig:
    """Configuration for streaming progress tracking."""
    update_interval_seconds: int = 60
    batch_size_rows: int = 100_000
    enable_metrics: bool = True
    enable_logging: bool = True
    
    def validate(self):
        """Validate configuration parameters."""
        if self.update_interval_seconds <= 0:
            raise ValueError("update_interval_seconds must be positive")
        if self.batch_size_rows <= 0:
            raise ValueError("batch_size_rows must be positive")


class StreamingProgressTracker:
    """Tracks real-time progress during data transfer operations.
    
    Supports both JDBC and Iceberg engines with engine-specific optimizations
    and progress tracking capabilities.
    """
    
    def __init__(self, table_name: str, load_type: str, config: StreamingProgressConfig,
                 metrics_publisher: CloudWatchMetricsPublisher, engine_type: Optional[str] = None):
        """
        Initialize streaming progress tracker.
        
        Args:
            table_name: Name of the table being processed
            load_type: Type of load operation ('full' or 'incremental')
            config: Configuration for progress tracking
            metrics_publisher: CloudWatch metrics publisher instance
            engine_type: Optional database engine type for engine-specific tracking
        """
        self.table_name = table_name
        self.load_type = load_type  # 'full' or 'incremental'
        self.config = config
        self.metrics_publisher = metrics_publisher
        self.engine_type = engine_type or 'unknown'
        self.structured_logger = StructuredLogger(f"StreamingProgressTracker-{table_name}")
        
        # Progress state
        self.rows_processed = 0
        self.start_time = time.time()
        self.last_update_time = time.time()
        self.total_rows: Optional[int] = None
        self.is_active = False
        self.progress_updates_emitted = 0
        
        # Determine if this is an Iceberg engine
        self.is_iceberg = self._is_iceberg_engine(engine_type)
    
    def _is_iceberg_engine(self, engine_type: Optional[str]) -> bool:
        """Check if the engine type is Iceberg.
        
        Args:
            engine_type: Database engine type
            
        Returns:
            True if engine is Iceberg, False otherwise
        """
        if not engine_type:
            return False
        return engine_type.lower() == 'iceberg'
    
    def start_tracking(self, total_rows: Optional[int] = None):
        """
        Start progress tracking.
        
        Args:
            total_rows: Total number of rows (if known in advance)
        """
        try:
            self.start_time = time.time()
            self.last_update_time = self.start_time
            self.total_rows = total_rows
            self.rows_processed = 0
            self.is_active = True
            self.progress_updates_emitted = 0
            
            if self.config.enable_logging:
                self.structured_logger.info(
                    f"Started streaming progress tracking for {self.load_type} load",
                    table_name=self.table_name,
                    load_type=self.load_type,
                    engine_type=self.engine_type,
                    is_iceberg=self.is_iceberg,
                    total_rows=total_rows,
                    update_interval_seconds=self.config.update_interval_seconds,
                    tracking_mode="metadata_aware" if self.is_iceberg else "standard"
                )
        except Exception as e:
            # Requirement 2.1: Continue migration even if progress tracking fails
            self.structured_logger.warning(
                f"Failed to start progress tracking for {self.table_name}, tracking disabled",
                table_name=self.table_name,
                load_type=self.load_type,
                error=str(e),
                error_type=type(e).__name__
            )
            self.is_active = False
            
            # Publish error metrics
            try:
                self.metrics_publisher.publish_error_metrics(
                    error_category="PROGRESS_TRACKING_START_FAILED",
                    error_operation="start_tracking"
                )
            except Exception:
                # Silently fail if metrics publishing also fails
                pass
    
    def update_progress(self, rows_processed: int):
        """
        Update progress with new row count.
        
        Args:
            rows_processed: Number of rows processed so far
        """
        if not self.is_active:
            return
        
        try:
            self.rows_processed = rows_processed
            
            # Check if we should emit an update
            if self._should_emit_update():
                self._emit_progress_update()
        except Exception as e:
            # Requirement 2.1: Graceful degradation if tracking fails
            self.structured_logger.warning(
                f"Failed to update progress for {self.table_name}, disabling tracking",
                table_name=self.table_name,
                load_type=self.load_type,
                rows_processed=rows_processed,
                error=str(e),
                error_type=type(e).__name__
            )
            
            # Disable tracking to prevent further errors
            self.is_active = False
            
            # Publish error metrics
            try:
                self.metrics_publisher.publish_error_metrics(
                    error_category="PROGRESS_TRACKING_UPDATE_FAILED",
                    error_operation="update_progress"
                )
            except Exception:
                # Silently fail if metrics publishing also fails
                pass
    
    def complete_tracking(self, final_row_count: int):
        """
        Complete tracking and emit final metrics.
        
        Args:
            final_row_count: Final number of rows processed
        """
        if not self.is_active:
            return
        
        try:
            self.rows_processed = final_row_count
            self.is_active = False
            
            # Emit final progress update
            self._emit_progress_update(is_final=True)
            
            # Calculate final metrics
            total_duration = time.time() - self.start_time
            rows_per_second = self.get_rows_per_second()
            
            if self.config.enable_logging:
                self.structured_logger.info(
                    f"Completed streaming progress tracking for {self.load_type} load",
                    table_name=self.table_name,
                    load_type=self.load_type,
                    engine_type=self.engine_type,
                    is_iceberg=self.is_iceberg,
                    final_row_count=final_row_count,
                    total_duration_seconds=round(total_duration, 2),
                    rows_per_second=round(rows_per_second, 2),
                    progress_updates_emitted=self.progress_updates_emitted,
                    tracking_mode="metadata_aware" if self.is_iceberg else "standard"
                )
        except Exception as e:
            # Requirement 2.1: Continue migration even if progress tracking fails
            self.structured_logger.warning(
                f"Failed to complete progress tracking for {self.table_name}",
                table_name=self.table_name,
                load_type=self.load_type,
                final_row_count=final_row_count,
                error=str(e),
                error_type=type(e).__name__
            )
            
            # Mark as inactive to prevent further operations
            self.is_active = False
            
            # Publish error metrics
            try:
                self.metrics_publisher.publish_error_metrics(
                    error_category="PROGRESS_TRACKING_COMPLETE_FAILED",
                    error_operation="complete_tracking"
                )
            except Exception:
                # Silently fail if metrics publishing also fails
                pass
    
    def _should_emit_update(self) -> bool:
        """
        Check if progress update should be emitted.
        
        Returns:
            True if update should be emitted, False otherwise
        """
        current_time = time.time()
        time_since_last_update = current_time - self.last_update_time
        
        # Emit update if interval has elapsed
        if time_since_last_update >= self.config.update_interval_seconds:
            return True
        
        # Also emit update every batch_size_rows
        if self.rows_processed > 0 and self.rows_processed % self.config.batch_size_rows == 0:
            return True
        
        return False
    
    def _emit_progress_update(self, is_final: bool = False):
        """
        Emit progress update to CloudWatch and logs.
        
        Args:
            is_final: Whether this is the final progress update
        """
        try:
            current_time = time.time()
            self.last_update_time = current_time
            self.progress_updates_emitted += 1
            
            # Calculate metrics
            rows_per_second = self.get_rows_per_second()
            progress_percentage = self.get_progress_percentage()
            eta_seconds = self._calculate_eta()
            
            # Publish CloudWatch metrics
            if self.config.enable_metrics:
                try:
                    self.metrics_publisher.publish_migration_progress_metrics(
                        table_name=self.table_name,
                        load_type=self.load_type,
                        rows_processed=self.rows_processed,
                        total_rows=self.total_rows,
                        rows_per_second=rows_per_second,
                        progress_percentage=progress_percentage
                    )
                except Exception as metrics_error:
                    # Log but don't fail if metrics publishing fails
                    self.structured_logger.warning(
                        f"Failed to publish progress metrics for {self.table_name}",
                        table_name=self.table_name,
                        error=str(metrics_error),
                        error_type=type(metrics_error).__name__
                    )
            
            # Requirement 5.2: Log progress updates every 100,000 rows
            if self.config.enable_logging:
                elapsed_seconds = self.get_elapsed_time()
                
                # Use the new structured migration progress logging method
                self.structured_logger.log_migration_progress(
                    table_name=self.table_name,
                    load_type=self.load_type,
                    rows_processed=self.rows_processed,
                    total_rows=self.total_rows,
                    elapsed_seconds=elapsed_seconds,
                    rows_per_second=rows_per_second,
                    engine_type=self.engine_type,
                    is_iceberg=self.is_iceberg,
                    is_final=is_final,
                    eta_seconds=eta_seconds,
                    tracking_mode="metadata_aware" if self.is_iceberg else "standard"
                )
        except Exception as e:
            # Requirement 2.1: Continue migration even if progress update emission fails
            self.structured_logger.warning(
                f"Failed to emit progress update for {self.table_name}",
                table_name=self.table_name,
                load_type=self.load_type,
                is_final=is_final,
                error=str(e),
                error_type=type(e).__name__
            )
            
            # Publish error metrics
            try:
                self.metrics_publisher.publish_error_metrics(
                    error_category="PROGRESS_TRACKING_EMIT_FAILED",
                    error_operation="emit_progress_update"
                )
            except Exception:
                # Silently fail if metrics publishing also fails
                pass
    
    def _calculate_eta(self) -> Optional[float]:
        """
        Calculate estimated time to completion.
        
        Returns:
            Estimated seconds to completion, or None if cannot be calculated
        """
        if self.total_rows is None or self.total_rows == 0:
            return None
        
        if self.rows_processed == 0:
            return None
        
        rows_per_second = self.get_rows_per_second()
        if rows_per_second == 0:
            return None
        
        rows_remaining = self.total_rows - self.rows_processed
        if rows_remaining <= 0:
            return 0.0
        
        return rows_remaining / rows_per_second
    
    def get_progress_percentage(self) -> float:
        """
        Get current progress percentage.
        
        Returns:
            Progress percentage (0-100)
        """
        if self.total_rows is None or self.total_rows == 0:
            return 0.0
        
        return (self.rows_processed / self.total_rows) * 100.0
    
    def get_rows_per_second(self) -> float:
        """
        Get current processing rate.
        
        Returns:
            Rows processed per second
        """
        elapsed_time = time.time() - self.start_time
        if elapsed_time == 0:
            return 0.0
        
        return self.rows_processed / elapsed_time
    
    def get_elapsed_time(self) -> float:
        """
        Get elapsed time since tracking started.
        
        Returns:
            Elapsed time in seconds
        """
        return time.time() - self.start_time
