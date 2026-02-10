"""
Spark progress listener for real-time job tracking.

This module provides a SparkListener implementation that tracks Spark job, stage,
and task progress for real-time visibility into data processing operations,
particularly useful for Iceberg writes where foreachPartition is not applicable.
"""

import time
import threading
from typing import Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from .streaming_progress_tracker import StreamingProgressTracker

# Conditional imports for PySpark (only available in Glue runtime)
try:
    from pyspark import SparkContext
    from pyspark.java_gateway import java_import
except ImportError:
    # Mock classes for local development/testing
    class SparkContext:
        pass
    def java_import(gateway, package):
        pass

from .logging import StructuredLogger


class SparkProgressListener:
    """
    Spark listener that tracks job/stage/task progress for real-time updates.
    
    This listener integrates with StreamingProgressTracker to provide progress
    updates during Spark operations, particularly useful for Iceberg writes
    where partition-level tracking is not feasible.
    """
    
    def __init__(self, table_name: str, streaming_tracker: Optional['StreamingProgressTracker'] = None):
        """
        Initialize Spark progress listener.
        
        Args:
            table_name: Name of the table being processed
            streaming_tracker: Optional streaming progress tracker for updates
        """
        self.table_name = table_name
        self.streaming_tracker = streaming_tracker
        self.structured_logger = StructuredLogger(f"SparkProgressListener-{table_name}")
        
        # Progress tracking state
        self.lock = threading.Lock()
        self.total_tasks = 0
        self.completed_tasks = 0
        self.failed_tasks = 0
        self.active_stages = set()
        self.completed_stages = set()
        self.start_time = time.time()
        self.last_update_time = time.time()
        self.update_interval_seconds = 30  # Update every 30 seconds
        
        # Estimated rows per task (will be refined as we process)
        self.estimated_rows_per_task = 0
        self.total_rows_estimate = 0
        
        self.is_active = False
        self.listener_registered = False
    
    def register_listener(self, spark_context: SparkContext) -> bool:
        """
        Register this listener with the Spark context.
        
        Args:
            spark_context: Active Spark context
            
        Returns:
            True if registration successful, False otherwise
        """
        try:
            # Create Java listener wrapper
            gateway = spark_context._gateway
            java_import(gateway.jvm, "org.apache.spark.scheduler.*")
            
            # Create Python listener wrapper
            listener_wrapper = self._create_java_listener_wrapper(gateway)
            
            # Add listener to Spark context
            spark_context._jsc.sc().addSparkListener(listener_wrapper)
            
            self.listener_registered = True
            self.is_active = True
            
            self.structured_logger.info(
                f"Registered Spark progress listener for {self.table_name}",
                table_name=self.table_name
            )
            
            return True
            
        except Exception as e:
            self.structured_logger.warning(
                f"Failed to register Spark progress listener for {self.table_name}",
                table_name=self.table_name,
                error=str(e),
                error_type=type(e).__name__
            )
            return False
    
    def _create_java_listener_wrapper(self, gateway):
        """
        Create a Java listener wrapper that calls back to Python methods.
        
        This is a simplified approach - in production, you might want to use
        a more robust Java-Python bridge.
        
        Args:
            gateway: Py4J gateway
            
        Returns:
            Java listener wrapper object
        """
        # Note: This is a simplified implementation
        # In production, you would create a proper Java class that implements SparkListener
        # and calls back to Python methods via Py4J
        
        # For now, we'll use a simpler approach with periodic polling
        return None
    
    def start_tracking(self, total_rows_estimate: Optional[int] = None):
        """
        Start progress tracking.
        
        Args:
            total_rows_estimate: Optional estimate of total rows to process
        """
        with self.lock:
            self.is_active = True
            self.start_time = time.time()
            self.last_update_time = self.start_time
            self.total_rows_estimate = total_rows_estimate or 0
            
            if total_rows_estimate and total_rows_estimate > 0:
                # Estimate rows per task (assuming ~200 tasks for large jobs)
                estimated_tasks = min(200, max(10, total_rows_estimate // 100000))
                self.estimated_rows_per_task = total_rows_estimate // estimated_tasks
    
    def on_stage_submitted(self, stage_info):
        """
        Called when a stage is submitted.
        
        Args:
            stage_info: Spark stage information
        """
        with self.lock:
            stage_id = stage_info.stageId()
            num_tasks = stage_info.numTasks()
            
            self.active_stages.add(stage_id)
            self.total_tasks += num_tasks
            
            self.structured_logger.debug(
                f"Stage {stage_id} submitted with {num_tasks} tasks",
                table_name=self.table_name,
                stage_id=stage_id,
                num_tasks=num_tasks,
                total_tasks=self.total_tasks
            )
    
    def on_stage_completed(self, stage_info):
        """
        Called when a stage completes.
        
        Args:
            stage_info: Spark stage information
        """
        with self.lock:
            stage_id = stage_info.stageId()
            
            if stage_id in self.active_stages:
                self.active_stages.remove(stage_id)
            self.completed_stages.add(stage_id)
            
            self._maybe_update_progress()
    
    def on_task_end(self, task_info):
        """
        Called when a task ends.
        
        Args:
            task_info: Spark task information
        """
        with self.lock:
            if task_info.successful():
                self.completed_tasks += 1
            else:
                self.failed_tasks += 1
            
            self._maybe_update_progress()
    
    def _maybe_update_progress(self):
        """
        Update progress if enough time has elapsed.
        
        This method should be called while holding self.lock.
        """
        current_time = time.time()
        time_since_update = current_time - self.last_update_time
        
        if time_since_update >= self.update_interval_seconds:
            self._emit_progress_update()
            self.last_update_time = current_time
    
    def _emit_progress_update(self):
        """
        Emit progress update to streaming tracker.
        
        This method should be called while holding self.lock.
        """
        if not self.is_active or self.streaming_tracker is None:
            return
        
        try:
            # Calculate progress
            if self.total_tasks > 0:
                progress_percentage = (self.completed_tasks / self.total_tasks) * 100
            else:
                progress_percentage = 0
            
            # Estimate rows processed
            if self.estimated_rows_per_task > 0:
                estimated_rows_processed = self.completed_tasks * self.estimated_rows_per_task
            else:
                estimated_rows_processed = 0
            
            # Update streaming tracker
            if estimated_rows_processed > 0:
                self.streaming_tracker.update_progress(estimated_rows_processed)
            
            # Log progress
            elapsed_seconds = time.time() - self.start_time
            self.structured_logger.info(
                f"Spark job progress for {self.table_name}",
                table_name=self.table_name,
                completed_tasks=self.completed_tasks,
                total_tasks=self.total_tasks,
                failed_tasks=self.failed_tasks,
                progress_percentage=round(progress_percentage, 2),
                estimated_rows_processed=estimated_rows_processed,
                elapsed_seconds=round(elapsed_seconds, 2),
                active_stages=len(self.active_stages),
                completed_stages=len(self.completed_stages)
            )
            
        except Exception as e:
            self.structured_logger.warning(
                f"Failed to emit progress update for {self.table_name}",
                table_name=self.table_name,
                error=str(e),
                error_type=type(e).__name__
            )
    
    def complete_tracking(self):
        """Complete progress tracking and emit final update."""
        with self.lock:
            if not self.is_active:
                return
            
            self.is_active = False
            
            # Emit final progress update
            elapsed_seconds = time.time() - self.start_time
            
            self.structured_logger.info(
                f"Completed Spark job tracking for {self.table_name}",
                table_name=self.table_name,
                total_tasks=self.total_tasks,
                completed_tasks=self.completed_tasks,
                failed_tasks=self.failed_tasks,
                elapsed_seconds=round(elapsed_seconds, 2),
                completed_stages=len(self.completed_stages)
            )


class SimpleSparkProgressTracker:
    """
    Simplified Spark progress tracker using periodic polling.
    
    This is a simpler alternative to SparkProgressListener that doesn't require
    Java listener registration. It periodically checks Spark UI metrics to
    estimate progress.
    """
    
    def __init__(self, spark_session, table_name: str, 
                 streaming_tracker: Optional['StreamingProgressTracker'] = None):
        """
        Initialize simple Spark progress tracker.
        
        Args:
            spark_session: Active Spark session
            table_name: Name of the table being processed
            streaming_tracker: Optional streaming progress tracker for updates
        """
        self.spark = spark_session
        self.table_name = table_name
        self.streaming_tracker = streaming_tracker
        self.structured_logger = StructuredLogger(f"SimpleSparkProgressTracker-{table_name}")
        
        self.is_active = False
        self.polling_thread = None
        self.stop_event = threading.Event()
        self.poll_interval_seconds = 30
    
    def start_tracking(self, total_rows_estimate: Optional[int] = None):
        """
        Start progress tracking with periodic polling.
        
        Args:
            total_rows_estimate: Optional estimate of total rows to process
        """
        self.is_active = True
        self.stop_event.clear()
        
        # Start polling thread
        self.polling_thread = threading.Thread(
            target=self._poll_progress,
            args=(total_rows_estimate,),
            daemon=True
        )
        self.polling_thread.start()
        
        self.structured_logger.info(
            f"Started simple Spark progress tracking for {self.table_name}",
            table_name=self.table_name,
            poll_interval_seconds=self.poll_interval_seconds
        )
    
    def _poll_progress(self, total_rows_estimate: Optional[int]):
        """
        Poll Spark progress by accessing Java StatusTracker directly.
        
        This method bypasses the PySpark wrapper and accesses the Java StatusTracker
        object directly through _jvm to get accurate task/stage completion metrics.
        
        Args:
            total_rows_estimate: Optional estimate of total rows to process
        """
        start_time = time.time()
        last_update_time = start_time
        
        self.structured_logger.info(
            f"Started Spark progress tracking for {self.table_name}",
            table_name=self.table_name,
            total_rows_estimate=total_rows_estimate,
            note="Tracking via Java StatusTracker API"
        )
        
        while not self.stop_event.is_set():
            try:
                current_time = time.time()
                elapsed_seconds = current_time - start_time
                
                # Get SparkContext
                sc = self.spark.sparkContext
                
                # Access Java StatusTracker directly through _jsc (Java Spark Context)
                try:
                    # Get the Java StatusTracker object
                    java_status_tracker = sc._jsc.sc().statusTracker()
                    
                    # Get active job IDs (returns Java array)
                    active_job_ids = java_status_tracker.getActiveJobIds()
                    
                    # Get active stage IDs (returns Java array)
                    active_stage_ids = java_status_tracker.getActiveStageIds()
                    
                    # Convert Java arrays to Python lists
                    active_jobs_count = len(active_job_ids) if active_job_ids else 0
                    active_stages_count = len(active_stage_ids) if active_stage_ids else 0
                    
                    # Count completed tasks across all active stages
                    total_tasks = 0
                    completed_tasks = 0
                    
                    if active_stage_ids:
                        for stage_id in active_stage_ids:
                            try:
                                # Get stage info from Java object
                                stage_info_option = java_status_tracker.getStageInfo(int(stage_id))
                                
                                # Check if stage info is present (Option type in Scala)
                                if stage_info_option.isDefined():
                                    stage_info = stage_info_option.get()
                                    
                                    # Access Java properties directly
                                    total_tasks += stage_info.numTasks()
                                    completed_tasks += stage_info.numCompletedTasks()
                            except Exception as stage_error:
                                # Stage might have completed between getting IDs and info
                                self.structured_logger.debug(
                                    f"Could not get info for stage {stage_id}",
                                    stage_id=stage_id,
                                    error=str(stage_error)
                                )
                                continue
                    
                    # Calculate progress if we have task information
                    if total_tasks > 0:
                        progress_percentage = (completed_tasks / total_tasks) * 100
                        
                        # Estimate rows processed based on task completion
                        if total_rows_estimate and total_rows_estimate > 0:
                            estimated_rows = int((completed_tasks / total_tasks) * total_rows_estimate)
                        else:
                            estimated_rows = 0
                        
                        # Update streaming tracker
                        if self.streaming_tracker and estimated_rows > 0:
                            self.streaming_tracker.update_progress(estimated_rows)
                        
                        # Log progress every poll interval
                        if current_time - last_update_time >= self.poll_interval_seconds:
                            # Calculate throughput and ETA
                            current_throughput = estimated_rows / elapsed_seconds if elapsed_seconds > 0 else 0
                            
                            if progress_percentage > 0 and progress_percentage < 100:
                                remaining_percentage = 100 - progress_percentage
                                eta_seconds = (elapsed_seconds / progress_percentage) * remaining_percentage
                            else:
                                eta_seconds = 0
                            
                            self.structured_logger.info(
                                f"Spark progress for {self.table_name}",
                                table_name=self.table_name,
                                active_jobs=active_jobs_count,
                                active_stages=active_stages_count,
                                completed_tasks=completed_tasks,
                                total_tasks=total_tasks,
                                progress_percentage=round(progress_percentage, 2),
                                estimated_rows=estimated_rows,
                                total_rows=total_rows_estimate,
                                elapsed_seconds=round(elapsed_seconds, 2),
                                current_throughput=round(current_throughput, 0),
                                eta_seconds=round(eta_seconds, 2) if eta_seconds > 0 else None
                            )
                            last_update_time = current_time
                    else:
                        # No active tasks, just log heartbeat
                        if current_time - last_update_time >= self.poll_interval_seconds:
                            self.structured_logger.info(
                                f"Spark job running for {self.table_name}",
                                table_name=self.table_name,
                                active_jobs=active_jobs_count,
                                active_stages=active_stages_count,
                                elapsed_seconds=round(elapsed_seconds, 2),
                                note="No active tasks detected - job may be starting or completing"
                            )
                            last_update_time = current_time
                
                except AttributeError as attr_error:
                    # Java API not accessible, fall back to heartbeat
                    if current_time - last_update_time >= self.poll_interval_seconds:
                        self.structured_logger.info(
                            f"Job running for {self.table_name} (Java API unavailable)",
                            table_name=self.table_name,
                            elapsed_seconds=round(elapsed_seconds, 2),
                            error=str(attr_error),
                            note="Falling back to heartbeat logging"
                        )
                        last_update_time = current_time
                
            except Exception as e:
                self.structured_logger.warning(
                    f"Error polling Spark progress for {self.table_name}",
                    table_name=self.table_name,
                    error=str(e),
                    error_type=type(e).__name__
                )
            
            # Wait for next poll interval
            self.stop_event.wait(self.poll_interval_seconds)
    
    def complete_tracking(self):
        """Stop progress tracking."""
        self.is_active = False
        self.stop_event.set()
        
        if self.polling_thread and self.polling_thread.is_alive():
            self.polling_thread.join(timeout=5)
        
        self.structured_logger.info(
            f"Completed simple Spark progress tracking for {self.table_name}",
            table_name=self.table_name
        )
