#!/usr/bin/env python3
"""
AWS Glue Data Replication PySpark Job

This script implements a data replication system that supports full-load and incremental
data migration across multiple database types using AWS Glue job bookmarks.

Supported database engines: Oracle, SQL Server, PostgreSQL, DB2
"""

import sys
import json
import logging
import time
import boto3
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any
from urllib.parse import urlparse
from datetime import datetime, timezone

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType, LongType, DateType
from pyspark.sql.functions import col, max as spark_max, min as spark_min, hash, concat_ws, lit

# Configure structured logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - [%(funcName)s:%(lineno)d] - %(message)s'
)
logger = logging.getLogger(__name__)

# Initialize CloudWatch client for metrics
try:
    cloudwatch = boto3.client('cloudwatch')
except Exception as e:
    logger.warning(f"Failed to initialize CloudWatch client: {e}")
    cloudwatch = None


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


class StructuredLogger:
    """Enhanced logger with structured logging and contextual information."""
    
    def __init__(self, job_name: str, logger_instance: logging.Logger = None):
        self.job_name = job_name
        self.logger = logger_instance or logger
        self.context = {"job_name": job_name}
    
    def _format_message(self, message: str, **kwargs) -> str:
        """Format message with context and additional fields."""
        context_data = {**self.context, **kwargs}
        if context_data:
            context_str = " | ".join([f"{k}={v}" for k, v in context_data.items()])
            return f"{message} | {context_str}"
        return message
    
    def set_context(self, **kwargs):
        """Set additional context for all subsequent log messages."""
        self.context.update(kwargs)
    
    def clear_context(self, *keys):
        """Clear specific context keys."""
        for key in keys:
            self.context.pop(key, None)
    
    def info(self, message: str, **kwargs):
        """Log info message with context."""
        self.logger.info(self._format_message(message, **kwargs))
    
    def warning(self, message: str, **kwargs):
        """Log warning message with context."""
        self.logger.warning(self._format_message(message, **kwargs))
    
    def error(self, message: str, **kwargs):
        """Log error message with context."""
        self.logger.error(self._format_message(message, **kwargs))
    
    def debug(self, message: str, **kwargs):
        """Log debug message with context."""
        self.logger.debug(self._format_message(message, **kwargs))
    
    def critical(self, message: str, **kwargs):
        """Log critical message with context."""
        self.logger.critical(self._format_message(message, **kwargs))
    
    def log_table_processing_start(self, table_name: str, operation: str):
        """Log start of table processing."""
        self.info(
            f"Starting {operation} for table",
            table_name=table_name,
            operation=operation,
            timestamp=datetime.now(timezone.utc).isoformat()
        )
    
    def log_table_processing_complete(self, table_name: str, operation: str, metrics: ProcessingMetrics):
        """Log completion of table processing with metrics."""
        self.info(
            f"Completed {operation} for table",
            table_name=table_name,
            operation=operation,
            rows_processed=metrics.rows_processed,
            duration_seconds=round(metrics.processing_duration_seconds, 2),
            throughput_rows_per_sec=round(metrics.get_throughput_rows_per_second(), 2),
            throughput_mb_per_sec=round(metrics.get_throughput_mb_per_second(), 2),
            bytes_processed=metrics.bytes_processed,
            status=metrics.status
        )
    
    def log_table_processing_failed(self, table_name: str, operation: str, error: str, metrics: ProcessingMetrics):
        """Log failure of table processing."""
        self.error(
            f"Failed {operation} for table",
            table_name=table_name,
            operation=operation,
            error=error,
            duration_seconds=round(metrics.processing_duration_seconds, 2),
            status=metrics.status
        )
    
    def log_job_summary(self, total_tables: int, successful_tables: int, failed_tables: int, 
                       total_rows: int, total_duration: float):
        """Log job execution summary."""
        self.info(
            "Job execution summary",
            total_tables=total_tables,
            successful_tables=successful_tables,
            failed_tables=failed_tables,
            success_rate=round((successful_tables / total_tables * 100), 2) if total_tables > 0 else 0,
            total_rows_processed=total_rows,
            total_duration_seconds=round(total_duration, 2),
            average_throughput_rows_per_sec=round(total_rows / total_duration, 2) if total_duration > 0 else 0
        )


class CloudWatchMetricsPublisher:
    """Publishes metrics to CloudWatch for monitoring and alerting."""
    
    def __init__(self, job_name: str, namespace: str = "AWS/Glue/DataReplication"):
        self.job_name = job_name
        self.namespace = namespace
        self.cloudwatch = cloudwatch
        self.metrics_buffer = []
        self.structured_logger = StructuredLogger(job_name)
    
    def _create_metric_data(self, metric_name: str, value: float, unit: str = 'Count', 
                           dimensions: Dict[str, str] = None) -> Dict:
        """Create CloudWatch metric data structure."""
        base_dimensions = [
            {'Name': 'JobName', 'Value': self.job_name}
        ]
        
        if dimensions:
            for key, val in dimensions.items():
                base_dimensions.append({'Name': key, 'Value': str(val)})
        
        return {
            'MetricName': metric_name,
            'Value': value,
            'Unit': unit,
            'Dimensions': base_dimensions,
            'Timestamp': datetime.now(timezone.utc)
        }
    
    def put_metric(self, metric_name: str, value: float, unit: str = 'Count', 
                  dimensions: Dict[str, str] = None, buffer: bool = True):
        """Put a single metric to CloudWatch."""
        if not self.cloudwatch:
            self.structured_logger.warning("CloudWatch client not available, skipping metric", 
                                         metric_name=metric_name, value=value)
            return
        
        metric_data = self._create_metric_data(metric_name, value, unit, dimensions)
        
        if buffer:
            self.metrics_buffer.append(metric_data)
            self.structured_logger.debug("Buffered metric", metric_name=metric_name, value=value)
        else:
            try:
                self.cloudwatch.put_metric_data(
                    Namespace=self.namespace,
                    MetricData=[metric_data]
                )
                self.structured_logger.debug("Published metric to CloudWatch", 
                                           metric_name=metric_name, value=value)
            except Exception as e:
                self.structured_logger.error("Failed to publish metric to CloudWatch", 
                                           metric_name=metric_name, error=str(e))
    
    def flush_metrics(self):
        """Flush all buffered metrics to CloudWatch."""
        if not self.cloudwatch or not self.metrics_buffer:
            return
        
        # CloudWatch allows max 20 metrics per put_metric_data call
        batch_size = 20
        
        for i in range(0, len(self.metrics_buffer), batch_size):
            batch = self.metrics_buffer[i:i + batch_size]
            
            try:
                self.cloudwatch.put_metric_data(
                    Namespace=self.namespace,
                    MetricData=batch
                )
                self.structured_logger.debug(f"Published {len(batch)} metrics to CloudWatch")
            except Exception as e:
                self.structured_logger.error(f"Failed to publish metrics batch to CloudWatch: {e}")
        
        # Clear buffer after flushing
        self.metrics_buffer.clear()
        self.structured_logger.info(f"Flushed all metrics to CloudWatch")
    
    def publish_job_start_metrics(self):
        """Publish job start metrics."""
        self.put_metric('JobStarted', 1, 'Count')
        self.structured_logger.info("Published job start metrics")
    
    def publish_job_completion_metrics(self, success: bool, duration_seconds: float, 
                                     total_tables: int, successful_tables: int, 
                                     total_rows: int):
        """Publish job completion metrics."""
        # Job completion status
        self.put_metric('JobCompleted', 1 if success else 0, 'Count')
        self.put_metric('JobFailed', 0 if success else 1, 'Count')
        
        # Job duration
        self.put_metric('JobDurationSeconds', duration_seconds, 'Seconds')
        
        # Table processing metrics
        self.put_metric('TotalTables', total_tables, 'Count')
        self.put_metric('SuccessfulTables', successful_tables, 'Count')
        self.put_metric('FailedTables', total_tables - successful_tables, 'Count')
        
        # Success rate
        success_rate = (successful_tables / total_tables * 100) if total_tables > 0 else 0
        self.put_metric('SuccessRate', success_rate, 'Percent')
        
        # Data processing metrics
        self.put_metric('TotalRowsProcessed', total_rows, 'Count')
        
        # Throughput metrics
        if duration_seconds > 0:
            self.put_metric('RowsPerSecond', total_rows / duration_seconds, 'Count/Second')
        
        self.structured_logger.info("Published job completion metrics", 
                                   success=success, duration=duration_seconds, 
                                   total_rows=total_rows)
    
    def publish_table_metrics(self, table_name: str, metrics: ProcessingMetrics):
        """Publish table-specific processing metrics."""
        dimensions = {'TableName': table_name}
        
        # Processing status
        self.put_metric('TableProcessed', 1 if metrics.status == 'completed' else 0, 
                       'Count', dimensions)
        self.put_metric('TableFailed', 1 if metrics.status == 'failed' else 0, 
                       'Count', dimensions)
        
        # Processing duration
        self.put_metric('TableProcessingDurationSeconds', 
                       metrics.processing_duration_seconds, 'Seconds', dimensions)
        
        # Data volume metrics
        if metrics.status == 'completed':
            self.put_metric('TableRowsProcessed', metrics.rows_processed, 'Count', dimensions)
            self.put_metric('TableBytesProcessed', metrics.bytes_processed, 'Bytes', dimensions)
            
            # Throughput metrics
            if metrics.processing_duration_seconds > 0:
                self.put_metric('TableRowsPerSecond', 
                               metrics.get_throughput_rows_per_second(), 
                               'Count/Second', dimensions)
                self.put_metric('TableMBPerSecond', 
                               metrics.get_throughput_mb_per_second(), 
                               'Bytes/Second', dimensions)
        
        self.structured_logger.debug("Published table metrics", table_name=table_name, 
                                   status=metrics.status)
    
    def publish_connection_metrics(self, connection_type: str, engine_type: str, 
                                 success: bool, duration_seconds: float):
        """Publish database connection metrics."""
        dimensions = {
            'ConnectionType': connection_type,  # source or target
            'EngineType': engine_type
        }
        
        self.put_metric('ConnectionAttempt', 1, 'Count', dimensions)
        self.put_metric('ConnectionSuccess', 1 if success else 0, 'Count', dimensions)
        self.put_metric('ConnectionDurationSeconds', duration_seconds, 'Seconds', dimensions)
        
        self.structured_logger.debug("Published connection metrics", 
                                   connection_type=connection_type, 
                                   engine_type=engine_type, success=success)
    
    def publish_error_metrics(self, error_category: str, error_operation: str):
        """Publish error metrics for monitoring and alerting."""
        dimensions = {
            'ErrorCategory': error_category,
            'Operation': error_operation
        }
        
        self.put_metric('ErrorOccurred', 1, 'Count', dimensions)
        
        self.structured_logger.debug("Published error metrics", 
                                   error_category=error_category, 
                                   operation=error_operation)


def estimate_dataframe_size(df: DataFrame, sample_size: int = 1000) -> int:
    """Estimate DataFrame size in bytes using sampling."""
    try:
        row_count = df.count()
        if row_count == 0:
            return 0
        
        # Sample rows to estimate average size
        sample_count = min(sample_size, row_count)
        sample_df = df.limit(sample_count)
        sample_rows = sample_df.collect()
        
        if not sample_rows:
            return 0
        
        # Calculate average row size by converting to string representation
        total_sample_size = 0
        for row in sample_rows:
            # Convert row to dictionary and estimate size
            row_dict = row.asDict()
            row_str = json.dumps(row_dict, default=str)
            total_sample_size += len(row_str.encode('utf-8'))
        
        avg_row_size = total_sample_size / len(sample_rows)
        estimated_total_size = int(avg_row_size * row_count)
        
        return estimated_total_size
        
    except Exception:
        # Return 0 if estimation fails
        return 0


class PerformanceMonitor:
    """Monitors and tracks performance metrics during job execution."""
    
    def __init__(self, job_name: str):
        self.job_name = job_name
        self.structured_logger = StructuredLogger(job_name)
        self.metrics_publisher = CloudWatchMetricsPublisher(job_name)
        self.job_start_time = datetime.now(timezone.utc)
        self.table_metrics: Dict[str, ProcessingMetrics] = {}
        self.connection_metrics = []
    
    def start_job_monitoring(self):
        """Start job-level monitoring."""
        self.job_start_time = datetime.now(timezone.utc)
        self.metrics_publisher.publish_job_start_metrics()
        self.structured_logger.info("Started job performance monitoring", 
                                  start_time=self.job_start_time.isoformat())
    
    def start_table_processing(self, table_name: str) -> ProcessingMetrics:
        """Start monitoring for a specific table."""
        metrics = ProcessingMetrics(table_name=table_name)
        self.table_metrics[table_name] = metrics
        
        self.structured_logger.log_table_processing_start(table_name, "data_replication")
        return metrics
    
    def complete_table_processing(self, table_name: str, rows_processed: int = 0, 
                                bytes_processed: int = 0):
        """Complete monitoring for a specific table."""
        if table_name not in self.table_metrics:
            self.structured_logger.warning("Table metrics not found", table_name=table_name)
            return
        
        metrics = self.table_metrics[table_name]
        metrics.mark_completed(rows_processed, bytes_processed)
        
        # Log completion
        self.structured_logger.log_table_processing_complete(table_name, "data_replication", metrics)
        
        # Publish metrics
        self.metrics_publisher.publish_table_metrics(table_name, metrics)
    
    def fail_table_processing(self, table_name: str, error_message: str):
        """Mark table processing as failed."""
        if table_name not in self.table_metrics:
            metrics = ProcessingMetrics(table_name=table_name)
            self.table_metrics[table_name] = metrics
        
        metrics = self.table_metrics[table_name]
        metrics.mark_failed(error_message)
        
        # Log failure
        self.structured_logger.log_table_processing_failed(table_name, "data_replication", 
                                                          error_message, metrics)
        
        # Publish metrics
        self.metrics_publisher.publish_table_metrics(table_name, metrics)
    
    def record_connection_attempt(self, connection_type: str, engine_type: str, 
                                success: bool, duration_seconds: float):
        """Record database connection attempt."""
        self.connection_metrics.append({
            'connection_type': connection_type,
            'engine_type': engine_type,
            'success': success,
            'duration_seconds': duration_seconds,
            'timestamp': datetime.now(timezone.utc)
        })
        
        # Publish connection metrics
        self.metrics_publisher.publish_connection_metrics(connection_type, engine_type, 
                                                         success, duration_seconds)
        
        self.structured_logger.info("Recorded connection attempt", 
                                  connection_type=connection_type, 
                                  engine_type=engine_type, 
                                  success=success, 
                                  duration_seconds=round(duration_seconds, 2))
    
    def record_error(self, error_category: str, operation: str, error_message: str):
        """Record error occurrence for monitoring."""
        self.metrics_publisher.publish_error_metrics(error_category, operation)
        
        self.structured_logger.error("Error recorded for monitoring", 
                                   error_category=error_category, 
                                   operation=operation, 
                                   error_message=error_message)
    
    def complete_job_monitoring(self, success: bool = True):
        """Complete job-level monitoring and publish final metrics."""
        job_end_time = datetime.now(timezone.utc)
        job_duration = (job_end_time - self.job_start_time).total_seconds()
        
        # Calculate summary statistics
        total_tables = len(self.table_metrics)
        successful_tables = sum(1 for m in self.table_metrics.values() if m.status == 'completed')
        failed_tables = total_tables - successful_tables
        total_rows = sum(m.rows_processed for m in self.table_metrics.values() if m.status == 'completed')
        
        # Log job summary
        self.structured_logger.log_job_summary(total_tables, successful_tables, failed_tables, 
                                             total_rows, job_duration)
        
        # Publish job completion metrics
        self.metrics_publisher.publish_job_completion_metrics(success, job_duration, 
                                                             total_tables, successful_tables, 
                                                             total_rows)
        
        # Flush all buffered metrics
        self.metrics_publisher.flush_metrics()
        
        self.structured_logger.info("Completed job performance monitoring", 
                                  duration_seconds=round(job_duration, 2), 
                                  success=success)
    
    def get_processing_summary(self) -> Dict[str, Any]:
        """Get summary of processing metrics."""
        total_tables = len(self.table_metrics)
        successful_tables = sum(1 for m in self.table_metrics.values() if m.status == 'completed')
        total_rows = sum(m.rows_processed for m in self.table_metrics.values() if m.status == 'completed')
        total_bytes = sum(m.bytes_processed for m in self.table_metrics.values() if m.status == 'completed')
        
        job_duration = (datetime.now(timezone.utc) - self.job_start_time).total_seconds()
        
        return {
            'job_name': self.job_name,
            'job_duration_seconds': job_duration,
            'total_tables': total_tables,
            'successful_tables': successful_tables,
            'failed_tables': total_tables - successful_tables,
            'success_rate': (successful_tables / total_tables * 100) if total_tables > 0 else 0,
            'total_rows_processed': total_rows,
            'total_bytes_processed': total_bytes,
            'average_throughput_rows_per_sec': total_rows / job_duration if job_duration > 0 else 0,
            'table_metrics': {name: {
                'status': metrics.status,
                'rows_processed': metrics.rows_processed,
                'duration_seconds': metrics.processing_duration_seconds,
                'throughput_rows_per_sec': metrics.get_throughput_rows_per_second()
            } for name, metrics in self.table_metrics.items()}
        }


@dataclass
class NetworkConfig:
    """Network configuration for cross-VPC database connections."""
    vpc_id: Optional[str] = None
    subnet_ids: Optional[List[str]] = None
    security_group_ids: Optional[List[str]] = None
    glue_connection_name: Optional[str] = None
    create_s3_vpc_endpoint: bool = False
    
    def has_network_config(self) -> bool:
        """Check if network configuration is provided."""
        return bool(self.vpc_id and self.subnet_ids and self.security_group_ids)
    
    def requires_glue_connection(self) -> bool:
        """Check if Glue connection is required for cross-VPC access."""
        return self.has_network_config() and bool(self.glue_connection_name)


@dataclass
class ConnectionConfig:
    """Configuration for database connection."""
    engine_type: str
    connection_string: str
    database: str
    schema: str
    username: str
    password: str
    jdbc_driver_path: str
    network_config: Optional[NetworkConfig] = None
    
    def __post_init__(self):
        """Validate connection configuration after initialization."""
        self.validate()
    
    def validate(self) -> None:
        """Validate connection configuration parameters."""
        if not self.engine_type:
            raise ValueError("Engine type cannot be empty")
        if not self.connection_string:
            raise ValueError("Connection string cannot be empty")
        if not self.database:
            raise ValueError("Database name cannot be empty")
        if not self.schema:
            raise ValueError("Schema name cannot be empty")
        if not self.username:
            raise ValueError("Username cannot be empty")
        if not self.password:
            raise ValueError("Password cannot be empty")
        if not self.jdbc_driver_path:
            raise ValueError("JDBC driver path cannot be empty")
    
    def requires_cross_vpc_connection(self) -> bool:
        """Check if this connection requires cross-VPC connectivity."""
        return self.network_config and self.network_config.requires_glue_connection()
    
    def get_glue_connection_name(self) -> Optional[str]:
        """Get the Glue connection name if configured."""
        return self.network_config.glue_connection_name if self.network_config else None


@dataclass
class JobConfig:
    """Main job configuration containing all parameters."""
    job_name: str
    source_connection: ConnectionConfig
    target_connection: ConnectionConfig
    tables: List[str]
    validate_connections: bool = True
    connection_timeout_seconds: int = 30
    
    def __post_init__(self):
        """Validate job configuration after initialization."""
        self.validate()
    
    def validate(self) -> None:
        """Validate job configuration parameters."""
        if not self.job_name:
            raise ValueError("Job name cannot be empty")
        if not self.tables:
            raise ValueError("Table list cannot be empty")
        
        # Validate connection configurations
        self.source_connection.validate()
        self.target_connection.validate()
    
    def has_cross_vpc_connections(self) -> bool:
        """Check if any connections require cross-VPC connectivity."""
        return (self.source_connection.requires_cross_vpc_connection() or 
                self.target_connection.requires_cross_vpc_connection())
    
    def get_network_summary(self) -> Dict[str, Any]:
        """Get summary of network configuration."""
        return {
            'source_cross_vpc': self.source_connection.requires_cross_vpc_connection(),
            'target_cross_vpc': self.target_connection.requires_cross_vpc_connection(),
            'source_glue_connection': self.source_connection.get_glue_connection_name(),
            'target_glue_connection': self.target_connection.get_glue_connection_name(),
            'validate_connections': self.validate_connections,
            'connection_timeout': self.connection_timeout_seconds
        }


class DatabaseEngineManager:
    """Manages database engine configurations and JDBC driver loading."""
    
    # Database engine configurations
    ENGINE_CONFIGS = {
        'oracle': {
            'driver_class': 'oracle.jdbc.OracleDriver',
            'url_template': 'jdbc:oracle:thin:@{host}:{port}:{database}',
            'default_port': 1521
        },
        'sqlserver': {
            'driver_class': 'com.microsoft.sqlserver.jdbc.SQLServerDriver',
            'url_template': 'jdbc:sqlserver://{host}:{port};databaseName={database}',
            'default_port': 1433
        },
        'postgresql': {
            'driver_class': 'org.postgresql.Driver',
            'url_template': 'jdbc:postgresql://{host}:{port}/{database}',
            'default_port': 5432
        },
        'db2': {
            'driver_class': 'com.ibm.db2.jcc.DB2Driver',
            'url_template': 'jdbc:db2://{host}:{port}/{database}',
            'default_port': 50000
        }
    }
    
    @classmethod
    def get_supported_engines(cls) -> List[str]:
        """Get list of supported database engines."""
        return list(cls.ENGINE_CONFIGS.keys())
    
    @classmethod
    def is_engine_supported(cls, engine_type: str) -> bool:
        """Check if database engine is supported."""
        return engine_type.lower() in cls.ENGINE_CONFIGS
    
    @classmethod
    def get_driver_class(cls, engine_type: str) -> str:
        """Get JDBC driver class for the specified engine."""
        engine_type = engine_type.lower()
        if not cls.is_engine_supported(engine_type):
            raise ValueError(f"Unsupported database engine: {engine_type}")
        return cls.ENGINE_CONFIGS[engine_type]['driver_class']
    
    @classmethod
    def validate_connection_string(cls, engine_type: str, connection_string: str) -> bool:
        """Validate connection string format for the specified engine."""
        engine_type = engine_type.lower()
        if not cls.is_engine_supported(engine_type):
            return False
        
        # Basic validation - check if connection string starts with expected JDBC URL prefix
        expected_prefixes = {
            'oracle': 'jdbc:oracle:thin:@',
            'sqlserver': 'jdbc:sqlserver://',
            'postgresql': 'jdbc:postgresql://',
            'db2': 'jdbc:db2://'
        }
        
        return connection_string.startswith(expected_prefixes[engine_type])


class JdbcDriverLoader:
    """Handles JDBC driver loading and validation."""
    
    def __init__(self, spark_context: SparkContext):
        self.spark_context = spark_context
        self.loaded_drivers = set()
    
    def load_driver(self, engine_type: str, driver_s3_path: str) -> None:
        """Load JDBC driver from S3 path."""
        if not driver_s3_path.startswith('s3://'):
            raise ValueError(f"JDBC driver path must be an S3 URL: {driver_s3_path}")
        
        if driver_s3_path in self.loaded_drivers:
            logger.info(f"JDBC driver already loaded: {driver_s3_path}")
            return
        
        try:
            # Add the JAR file to Spark context
            self.spark_context.addPyFile(driver_s3_path)
            self.loaded_drivers.add(driver_s3_path)
            logger.info(f"Successfully loaded JDBC driver for {engine_type}: {driver_s3_path}")
        except Exception as e:
            raise RuntimeError(f"Failed to load JDBC driver from {driver_s3_path}: {str(e)}")
    
    def validate_driver_path(self, driver_s3_path: str) -> bool:
        """Validate S3 driver path format."""
        if not driver_s3_path.startswith('s3://'):
            return False
        
        # Parse S3 URL to validate format
        try:
            parsed = urlparse(driver_s3_path)
            return bool(parsed.netloc and parsed.path and parsed.path.endswith('.jar'))
        except Exception:
            return False


class JobConfigurationParser:
    """Parses job configuration from CloudFormation parameters."""
    
    # Required CloudFormation parameters
    REQUIRED_PARAMS = [
        'JOB_NAME',
        'SOURCE_ENGINE_TYPE',
        'TARGET_ENGINE_TYPE',
        'SOURCE_DATABASE',
        'TARGET_DATABASE',
        'SOURCE_SCHEMA',
        'TARGET_SCHEMA',
        'TABLE_NAMES',
        'SOURCE_DB_USER',
        'SOURCE_DB_PASSWORD',
        'TARGET_DB_USER',
        'TARGET_DB_PASSWORD',
        'SOURCE_JDBC_DRIVER_S3_PATH',
        'TARGET_JDBC_DRIVER_S3_PATH',
        'SOURCE_CONNECTION_STRING',
        'TARGET_CONNECTION_STRING'
    ]
    
    # Optional network configuration parameters
    OPTIONAL_NETWORK_PARAMS = [
        'SOURCE_VPC_ID',
        'SOURCE_SUBNET_IDS',
        'SOURCE_SECURITY_GROUP_IDS',
        'CREATE_SOURCE_S3_VPC_ENDPOINT',
        'TARGET_VPC_ID',
        'TARGET_SUBNET_IDS',
        'TARGET_SECURITY_GROUP_IDS',
        'CREATE_TARGET_S3_VPC_ENDPOINT',
        'SOURCE_GLUE_CONNECTION_NAME',
        'TARGET_GLUE_CONNECTION_NAME',
        'VALIDATE_CONNECTIONS',
        'CONNECTION_TIMEOUT_SECONDS'
    ]
    
    @classmethod
    def parse_job_arguments(cls) -> Dict[str, str]:
        """Parse job arguments from CloudFormation parameters."""
        try:
            # Parse required parameters
            args = getResolvedOptions(sys.argv, cls.REQUIRED_PARAMS)
            
            # Parse optional network parameters with defaults
            try:
                optional_args = getResolvedOptions(sys.argv, cls.OPTIONAL_NETWORK_PARAMS)
                args.update(optional_args)
            except Exception:
                # Set default values for optional parameters if not provided
                for param in cls.OPTIONAL_NETWORK_PARAMS:
                    if param not in args:
                        args[param] = ''
            
            # Set defaults for specific parameters
            args.setdefault('VALIDATE_CONNECTIONS', 'true')
            args.setdefault('CONNECTION_TIMEOUT_SECONDS', '30')
            
            logger.info("Successfully parsed job arguments")
            return args
        except Exception as e:
            logger.error(f"Failed to parse job arguments: {str(e)}")
            raise RuntimeError(f"Missing required job parameters: {str(e)}")
    
    @classmethod
    def parse_network_config(cls, args: Dict[str, str], prefix: str) -> Optional[NetworkConfig]:
        """Parse network configuration from CloudFormation parameters.
        
        Args:
            args: Parsed job arguments
            prefix: 'SOURCE' or 'TARGET' to identify which network config to parse
            
        Returns:
            NetworkConfig if network parameters are provided, None otherwise
        """
        vpc_id = args.get(f'{prefix}_VPC_ID', '').strip()
        subnet_ids_str = args.get(f'{prefix}_SUBNET_IDS', '').strip()
        security_group_ids_str = args.get(f'{prefix}_SECURITY_GROUP_IDS', '').strip()
        glue_connection_name = args.get(f'{prefix}_GLUE_CONNECTION_NAME', '').strip()
        create_s3_vpc_endpoint = args.get(f'CREATE_{prefix}_S3_VPC_ENDPOINT', 'NO').upper() == 'YES'
        
        # If no network configuration provided, return None
        if not vpc_id and not subnet_ids_str and not security_group_ids_str and not glue_connection_name:
            return None
        
        # Parse comma-separated lists
        subnet_ids = [s.strip() for s in subnet_ids_str.split(',') if s.strip()] if subnet_ids_str else None
        security_group_ids = [s.strip() for s in security_group_ids_str.split(',') if s.strip()] if security_group_ids_str else None
        
        return NetworkConfig(
            vpc_id=vpc_id if vpc_id else None,
            subnet_ids=subnet_ids,
            security_group_ids=security_group_ids,
            glue_connection_name=glue_connection_name if glue_connection_name else None,
            create_s3_vpc_endpoint=create_s3_vpc_endpoint
        )
    
    @classmethod
    def create_job_config(cls, args: Dict[str, str]) -> JobConfig:
        """Create JobConfig from parsed arguments."""
        try:
            # Parse table names (comma-separated)
            table_names = [table.strip() for table in args['TABLE_NAMES'].split(',') if table.strip()]
            
            # Parse network configurations
            source_network_config = cls.parse_network_config(args, 'SOURCE')
            target_network_config = cls.parse_network_config(args, 'TARGET')
            
            # Create source connection config
            source_connection = ConnectionConfig(
                engine_type=args['SOURCE_ENGINE_TYPE'].lower(),
                connection_string=args['SOURCE_CONNECTION_STRING'],
                database=args['SOURCE_DATABASE'],
                schema=args['SOURCE_SCHEMA'],
                username=args['SOURCE_DB_USER'],
                password=args['SOURCE_DB_PASSWORD'],
                jdbc_driver_path=args['SOURCE_JDBC_DRIVER_S3_PATH'],
                network_config=source_network_config
            )
            
            # Create target connection config
            target_connection = ConnectionConfig(
                engine_type=args['TARGET_ENGINE_TYPE'].lower(),
                connection_string=args['TARGET_CONNECTION_STRING'],
                database=args['TARGET_DATABASE'],
                schema=args['TARGET_SCHEMA'],
                username=args['TARGET_DB_USER'],
                password=args['TARGET_DB_PASSWORD'],
                jdbc_driver_path=args['TARGET_JDBC_DRIVER_S3_PATH'],
                network_config=target_network_config
            )
            
            # Parse connection validation settings
            validate_connections = args.get('VALIDATE_CONNECTIONS', 'true').lower() == 'true'
            connection_timeout_seconds = int(args.get('CONNECTION_TIMEOUT_SECONDS', '30'))
            
            # Create job config
            job_config = JobConfig(
                job_name=args['JOB_NAME'],
                source_connection=source_connection,
                target_connection=target_connection,
                tables=table_names,
                validate_connections=validate_connections,
                connection_timeout_seconds=connection_timeout_seconds
            )
            
            logger.info(f"Created job configuration for: {job_config.job_name}")
            
            # Log network configuration summary
            network_summary = job_config.get_network_summary()
            if job_config.has_cross_vpc_connections():
                logger.info(f"Cross-VPC network configuration detected: {network_summary}")
            else:
                logger.info("Using same-VPC connectivity (no cross-VPC configuration)")
            
            return job_config
            
        except Exception as e:
            logger.error(f"Failed to create job configuration: {str(e)}")
            raise RuntimeError(f"Invalid job configuration: {str(e)}")
    
    @classmethod
    def validate_configuration(cls, job_config: JobConfig) -> None:
        """Validate the complete job configuration."""
        # Validate engine types
        if not DatabaseEngineManager.is_engine_supported(job_config.source_connection.engine_type):
            raise ValueError(f"Unsupported source engine: {job_config.source_connection.engine_type}")
        
        if not DatabaseEngineManager.is_engine_supported(job_config.target_connection.engine_type):
            raise ValueError(f"Unsupported target engine: {job_config.target_connection.engine_type}")
        
        # Validate connection strings
        if not DatabaseEngineManager.validate_connection_string(
            job_config.source_connection.engine_type,
            job_config.source_connection.connection_string
        ):
            raise ValueError("Invalid source connection string format")
        
        if not DatabaseEngineManager.validate_connection_string(
            job_config.target_connection.engine_type,
            job_config.target_connection.connection_string
        ):
            raise ValueError("Invalid target connection string format")
        
        # Validate network configurations
        cls.validate_network_configuration(job_config)
        
        logger.info("Job configuration validation completed successfully")
    
    @classmethod
    def validate_network_configuration(cls, job_config: JobConfig) -> None:
        """Validate network configuration parameters."""
        # Validate source network configuration
        if job_config.source_connection.network_config:
            cls._validate_single_network_config(
                job_config.source_connection.network_config, "source"
            )
        
        # Validate target network configuration
        if job_config.target_connection.network_config:
            cls._validate_single_network_config(
                job_config.target_connection.network_config, "target"
            )
        
        logger.info("Network configuration validation completed")
    
    @classmethod
    def _validate_single_network_config(cls, network_config: NetworkConfig, connection_type: str) -> None:
        """Validate a single network configuration."""
        # If Glue connection name is provided, it should be sufficient
        if network_config.glue_connection_name:
            logger.info(f"Using Glue connection for {connection_type}: {network_config.glue_connection_name}")
            return
        
        # If network details are provided, validate completeness
        if network_config.has_network_config():
            if not network_config.vpc_id:
                raise ValueError(f"VPC ID is required for {connection_type} network configuration")
            if not network_config.subnet_ids:
                raise ValueError(f"Subnet IDs are required for {connection_type} network configuration")
            if not network_config.security_group_ids:
                raise ValueError(f"Security Group IDs are required for {connection_type} network configuration")
            
            logger.info(f"Network configuration validated for {connection_type}: VPC {network_config.vpc_id}")
        
        # Warn if partial network configuration is provided
        if (network_config.vpc_id or network_config.subnet_ids or network_config.security_group_ids) and not network_config.has_network_config():
            logger.warning(f"Partial network configuration detected for {connection_type} - some parameters may be missing")


def initialize_spark_session(job_config: JobConfig) -> tuple[SparkSession, GlueContext, Job]:
    """Initialize Spark session, Glue context, and job."""
    try:
        # Initialize Spark context
        sc = SparkContext()
        
        # Initialize Glue context
        glue_context = GlueContext(sc)
        
        # Get Spark session
        spark = glue_context.spark_session
        
        # Initialize Glue job
        job = Job(glue_context)
        job.init(job_config.job_name, {})
        
        logger.info(f"Initialized Spark session for job: {job_config.job_name}")
        return spark, glue_context, job
        
    except Exception as e:
        logger.error(f"Failed to initialize Spark session: {str(e)}")
        raise RuntimeError(f"Spark initialization failed: {str(e)}")


def load_jdbc_drivers(spark_context: SparkContext, job_config: JobConfig) -> None:
    """Load JDBC drivers for source and target databases."""
    driver_loader = JdbcDriverLoader(spark_context)
    
    try:
        # Load source JDBC driver
        driver_loader.load_driver(
            job_config.source_connection.engine_type,
            job_config.source_connection.jdbc_driver_path
        )
        
        # Load target JDBC driver (if different from source)
        if (job_config.target_connection.jdbc_driver_path != 
            job_config.source_connection.jdbc_driver_path):
            driver_loader.load_driver(
                job_config.target_connection.engine_type,
                job_config.target_connection.jdbc_driver_path
            )
        
        logger.info("Successfully loaded all JDBC drivers")
        
    except Exception as e:
        logger.error(f"Failed to load JDBC drivers: {str(e)}")
        raise RuntimeError(f"JDBC driver loading failed: {str(e)}")


def main():
    """Main entry point for the Glue job with comprehensive error handling."""
    error_recovery_manager = None
    job = None
    performance_monitor = None
    structured_logger = None
    
    try:
        logger.info("Starting AWS Glue Data Replication Job")
        
        # Initialize performance monitoring early
        temp_job_name = "glue-data-replication"  # Will be updated with actual job name
        performance_monitor = PerformanceMonitor(temp_job_name)
        structured_logger = StructuredLogger(temp_job_name)
        
        structured_logger.info("Initializing AWS Glue Data Replication Job")
        
        # Parse job arguments from CloudFormation parameters
        try:
            args = JobConfigurationParser.parse_job_arguments()
        except Exception as e:
            logger.critical(f"Failed to parse job arguments: {str(e)}")
            raise RuntimeError(f"Job configuration parsing failed: {str(e)}")
        
        # Create job configuration
        try:
            job_config = JobConfigurationParser.create_job_config(args)
            
            # Update monitoring with actual job name
            performance_monitor = PerformanceMonitor(job_config.job_name)
            structured_logger = StructuredLogger(job_config.job_name)
            performance_monitor.start_job_monitoring()
            
        except Exception as e:
            if structured_logger:
                structured_logger.critical("Failed to create job configuration", error=str(e))
            else:
                logger.critical(f"Failed to create job configuration: {str(e)}")
            raise RuntimeError(f"Job configuration creation failed: {str(e)}")
        
        # Initialize error recovery manager with monitoring
        error_recovery_manager = ErrorRecoveryManager(job_config.job_name)
        
        # Set context for structured logging
        structured_logger.set_context(
            source_engine=job_config.source_connection.engine_type,
            target_engine=job_config.target_connection.engine_type,
            table_count=len(job_config.tables)
        )
        
        # Validate configuration
        try:
            JobConfigurationParser.validate_configuration(job_config)
        except Exception as e:
            error_info = error_recovery_manager.handle_infrastructure_error(
                e, "Configuration", "job_configuration_validation"
            )
            logger.critical(f"Job configuration validation failed: {str(e)}")
            raise RuntimeError(f"Configuration validation failed: {str(e)}")
        
        # Initialize Spark session and Glue context
        try:
            spark, glue_context, job = initialize_spark_session(job_config)
        except Exception as e:
            error_info = error_recovery_manager.handle_infrastructure_error(
                e, "Spark", "spark_session_initialization"
            )
            logger.critical(f"Spark session initialization failed: {str(e)}")
            raise RuntimeError(f"Spark initialization failed: {str(e)}")
        
        # Load JDBC drivers with error handling
        try:
            load_jdbc_drivers(spark.sparkContext, job_config)
        except Exception as e:
            error_info = error_recovery_manager.handle_infrastructure_error(
                e, "S3/JDBC", "jdbc_driver_loading"
            )
            
            # Attempt recovery for JDBC driver loading
            if error_recovery_manager.attempt_graceful_recovery(error_info):
                logger.info("Retrying JDBC driver loading after recovery attempt")
                try:
                    load_jdbc_drivers(spark.sparkContext, job_config)
                except Exception as retry_error:
                    logger.critical(f"JDBC driver loading failed after recovery: {str(retry_error)}")
                    raise RuntimeError(f"JDBC driver loading failed: {str(retry_error)}")
            else:
                logger.critical(f"JDBC driver loading failed: {str(e)}")
                raise RuntimeError(f"JDBC driver loading failed: {str(e)}")
        
        logger.info(f"Job configuration completed successfully for: {job_config.job_name}")
        logger.info(f"Source: {job_config.source_connection.engine_type} -> Target: {job_config.target_connection.engine_type}")
        logger.info(f"Tables to replicate: {', '.join(job_config.tables)}")
        
        # Test database connections with enhanced error handling and monitoring
        try:
            test_database_connections_with_recovery(spark, job_config, error_recovery_manager, performance_monitor)
        except Exception as e:
            structured_logger.critical("Database connection testing failed", error=str(e))
            if performance_monitor:
                performance_monitor.record_error("connection", "database_connection_test", str(e))
            raise RuntimeError(f"Database connectivity validation failed: {str(e)}")
        
        # Determine migration mode based on job bookmarks
        try:
            migration_mode = determine_migration_mode(glue_context, job_config.job_name, job_config.tables)
        except Exception as e:
            error_info = error_recovery_manager.handle_infrastructure_error(
                e, "Glue", "migration_mode_determination"
            )
            logger.warning(f"Failed to determine migration mode, defaulting to full-load: {str(e)}")
            migration_mode = 'full_load'
        
        # Perform data migration based on mode with comprehensive error handling and monitoring
        migration_results = {}
        try:
            if migration_mode == 'incremental':
                structured_logger.info("Performing incremental data migration with job bookmarks", 
                                     migration_mode=migration_mode)
                migration_results = perform_incremental_data_migration_with_recovery(
                    spark, glue_context, job_config, error_recovery_manager, performance_monitor
                )
            else:
                structured_logger.info("Performing full-load data migration", 
                                     migration_mode=migration_mode)
                migration_results = perform_full_load_data_migration_with_recovery(
                    spark, job_config, error_recovery_manager, performance_monitor
                )
        except Exception as e:
            structured_logger.error("Data migration failed", error=str(e))
            if performance_monitor:
                performance_monitor.record_error("data_processing", "data_migration", str(e))
            # Don't immediately fail - log partial results if any
            if migration_results:
                structured_logger.info("Partial migration results available despite failure")
        
        # Log final results with error details and complete monitoring
        successful_count = sum(1 for p in migration_results.values() if p.status == 'completed')
        failed_count = sum(1 for p in migration_results.values() if p.status == 'failed')
        total_count = len(migration_results)
        total_rows = sum(p.processed_rows for p in migration_results.values() if p.status == 'completed')
        
        structured_logger.info(
            "Data migration completed",
            successful_tables=successful_count,
            failed_tables=failed_count,
            total_tables=total_count,
            total_rows_migrated=total_rows,
            success_rate=round((successful_count / total_count * 100), 2) if total_count > 0 else 0
        )
        
        # Log detailed error report
        if error_recovery_manager:
            error_recovery_manager.log_final_error_report()
        
        # Determine job success/failure
        job_success = failed_count == 0
        if failed_count > 0:
            structured_logger.warning("Job completed with table failures", 
                                    failed_count=failed_count, 
                                    successful_count=successful_count)
            if successful_count == 0:
                job_success = False
                if performance_monitor:
                    performance_monitor.complete_job_monitoring(success=False)
                raise RuntimeError(f"All {total_count} tables failed to migrate")
        
        # Complete monitoring before job commit
        if performance_monitor:
            performance_monitor.complete_job_monitoring(success=job_success)
        
        # Commit the job
        if job:
            job.commit()
        structured_logger.info("Job completed successfully", 
                             final_status="success" if job_success else "partial_success")
        
    except Exception as e:
        if structured_logger:
            structured_logger.error("Job failed with error", error=str(e))
        else:
            logger.error(f"Job failed with error: {str(e)}")
        
        # Complete monitoring with failure status
        if performance_monitor:
            performance_monitor.complete_job_monitoring(success=False)
        
        # Log final error report even on failure
        if error_recovery_manager:
            error_recovery_manager.log_final_error_report()
        
        # Ensure job is properly handled on failure
        if job:
            try:
                job.commit()
            except Exception as commit_error:
                if structured_logger:
                    structured_logger.error("Failed to commit job on error", commit_error=str(commit_error))
                else:
                    logger.error(f"Failed to commit job on error: {str(commit_error)}")
        
        raise


if __name__ == "__main__":
    main()


class ConnectionStringBuilder:
    """Builds JDBC connection strings for different database engines."""
    
    @classmethod
    def build_connection_string(cls, engine_type: str, host: str, port: int, 
                              database: str, **kwargs) -> str:
        """Build JDBC connection string for the specified engine."""
        engine_type = engine_type.lower()
        
        if not DatabaseEngineManager.is_engine_supported(engine_type):
            raise ValueError(f"Unsupported database engine: {engine_type}")
        
        config = DatabaseEngineManager.ENGINE_CONFIGS[engine_type]
        
        # Use default port if not specified
        if port is None or port <= 0:
            port = config['default_port']
        
        # Build base connection string
        connection_string = config['url_template'].format(
            host=host,
            port=port,
            database=database
        )
        
        # Add engine-specific parameters
        if engine_type == 'sqlserver':
            # Add common SQL Server parameters
            params = []
            if kwargs.get('encrypt', True):
                params.append('encrypt=true')
            if kwargs.get('trustServerCertificate', False):
                params.append('trustServerCertificate=true')
            if kwargs.get('loginTimeout'):
                params.append(f"loginTimeout={kwargs['loginTimeout']}")
            
            if params:
                connection_string += ';' + ';'.join(params)
        
        elif engine_type == 'oracle':
            # Add Oracle-specific parameters if needed
            if kwargs.get('connectionTimeout'):
                connection_string += f"?oracle.net.CONNECT_TIMEOUT={kwargs['connectionTimeout']}"
        
        elif engine_type == 'postgresql':
            # Add PostgreSQL-specific parameters
            params = []
            if kwargs.get('ssl', False):
                params.append('ssl=true')
            if kwargs.get('connectTimeout'):
                params.append(f"connectTimeout={kwargs['connectTimeout']}")
            if kwargs.get('socketTimeout'):
                params.append(f"socketTimeout={kwargs['socketTimeout']}")
            
            if params:
                connection_string += '?' + '&'.join(params)
        
        elif engine_type == 'db2':
            # Add DB2-specific parameters
            params = []
            if kwargs.get('loginTimeout'):
                params.append(f"loginTimeout={kwargs['loginTimeout']}")
            if kwargs.get('blockingReadConnectionTimeout'):
                params.append(f"blockingReadConnectionTimeout={kwargs['blockingReadConnectionTimeout']}")
            
            if params:
                connection_string += ':' + ';'.join(params) + ';'
        
        return connection_string
    
    @classmethod
    def parse_connection_string(cls, connection_string: str) -> Dict[str, Any]:
        """Parse connection string to extract components."""
        try:
            # Determine engine type from connection string prefix
            engine_type = None
            for engine, config in DatabaseEngineManager.ENGINE_CONFIGS.items():
                url_prefix = config['url_template'].split('{')[0]
                if connection_string.startswith(url_prefix.replace('{host}', '').replace('{port}', '').replace('{database}', '')):
                    engine_type = engine
                    break
            
            if not engine_type:
                raise ValueError("Unable to determine engine type from connection string")
            
            # Basic parsing - this is a simplified implementation
            # In production, you might want more robust parsing
            parsed = {
                'engine_type': engine_type,
                'connection_string': connection_string
            }
            
            return parsed
            
        except Exception as e:
            raise ValueError(f"Failed to parse connection string: {str(e)}")


class ErrorCategory:
    """Defines error categories for different types of failures."""
    CONNECTION = "connection"
    AUTHENTICATION = "authentication"
    NETWORK = "network"
    DATA_PROCESSING = "data_processing"
    SCHEMA_MISMATCH = "schema_mismatch"
    PERMISSION = "permission"
    RESOURCE = "resource"
    TIMEOUT = "timeout"
    UNKNOWN = "unknown"


class ErrorClassifier:
    """Classifies errors into categories for appropriate handling."""
    
    # Error patterns for classification
    ERROR_PATTERNS = {
        ErrorCategory.CONNECTION: [
            'connection refused', 'connection timed out', 'connection reset',
            'no route to host', 'network unreachable', 'connection failed',
            'could not connect', 'unable to connect', 'connection error'
        ],
        ErrorCategory.AUTHENTICATION: [
            'authentication failed', 'login failed', 'invalid credentials',
            'access denied', 'unauthorized', 'invalid username or password',
            'authentication error', 'login incorrect'
        ],
        ErrorCategory.NETWORK: [
            'network error', 'socket timeout', 'read timeout', 'write timeout',
            'network is unreachable', 'host unreachable', 'dns resolution failed',
            'connection timeout', 'socket error'
        ],
        ErrorCategory.DATA_PROCESSING: [
            'data type mismatch', 'conversion error', 'parsing error',
            'invalid data format', 'constraint violation', 'data truncation',
            'null value', 'duplicate key'
        ],
        ErrorCategory.SCHEMA_MISMATCH: [
            'column not found', 'table not found', 'schema mismatch',
            'invalid column name', 'missing column', 'unknown column',
            'table does not exist', 'column does not exist'
        ],
        ErrorCategory.PERMISSION: [
            'permission denied', 'access forbidden', 'insufficient privileges',
            'not authorized', 'privilege error', 'access violation',
            'security error', 'forbidden'
        ],
        ErrorCategory.RESOURCE: [
            'out of memory', 'disk full', 'resource exhausted',
            'too many connections', 'connection pool exhausted',
            'memory error', 'resource unavailable'
        ],
        ErrorCategory.TIMEOUT: [
            'timeout', 'timed out', 'operation timeout', 'query timeout',
            'connection timeout', 'read timeout', 'write timeout'
        ]
    }
    
    @classmethod
    def classify_error(cls, error: Exception) -> str:
        """Classify error into appropriate category."""
        error_message = str(error).lower()
        
        for category, patterns in cls.ERROR_PATTERNS.items():
            for pattern in patterns:
                if pattern in error_message:
                    return category
        
        return ErrorCategory.UNKNOWN
    
    @classmethod
    def is_retryable_error(cls, error: Exception) -> bool:
        """Determine if error is retryable based on its category."""
        category = cls.classify_error(error)
        
        # Retryable error categories
        retryable_categories = {
            ErrorCategory.CONNECTION,
            ErrorCategory.NETWORK,
            ErrorCategory.TIMEOUT,
            ErrorCategory.RESOURCE
        }
        
        return category in retryable_categories
    
    @classmethod
    def get_recovery_strategy(cls, error: Exception) -> str:
        """Get recommended recovery strategy for error."""
        category = cls.classify_error(error)
        
        strategies = {
            ErrorCategory.CONNECTION: "retry_with_backoff",
            ErrorCategory.AUTHENTICATION: "fail_immediately",
            ErrorCategory.NETWORK: "retry_with_backoff",
            ErrorCategory.DATA_PROCESSING: "log_and_continue",
            ErrorCategory.SCHEMA_MISMATCH: "fail_immediately",
            ErrorCategory.PERMISSION: "fail_immediately",
            ErrorCategory.RESOURCE: "retry_with_longer_delay",
            ErrorCategory.TIMEOUT: "retry_with_backoff",
            ErrorCategory.UNKNOWN: "retry_with_backoff"
        }
        
        return strategies.get(category, "retry_with_backoff")


class NetworkConnectivityError(Exception):
    """Exception raised for network connectivity issues."""
    def __init__(self, message: str, error_type: str = 'unknown', connection_name: str = None):
        super().__init__(message)
        self.error_type = error_type
        self.connection_name = connection_name


class GlueConnectionError(Exception):
    """Exception raised for Glue connection specific issues."""
    def __init__(self, message: str, connection_name: str, error_details: Dict[str, Any] = None):
        super().__init__(message)
        self.connection_name = connection_name
        self.error_details = error_details or {}


class VpcEndpointError(Exception):
    """Exception raised for VPC endpoint connectivity issues."""
    def __init__(self, message: str, vpc_id: str = None, endpoint_type: str = None):
        super().__init__(message)
        self.vpc_id = vpc_id
        self.endpoint_type = endpoint_type


class ENICreationError(Exception):
    """Exception raised for Elastic Network Interface creation failures."""
    def __init__(self, message: str, subnet_id: str = None, error_code: str = None):
        super().__init__(message)
        self.subnet_id = subnet_id
        self.error_code = error_code


class NetworkErrorHandler:
    """Specialized error handler for network-specific issues."""
    
    def __init__(self):
        self.structured_logger = StructuredLogger("NetworkErrorHandler")
        try:
            self.ec2_client = boto3.client('ec2')
            self.glue_client = boto3.client('glue')
        except Exception as e:
            self.structured_logger.warning(f"Failed to initialize AWS clients: {e}")
            self.ec2_client = None
            self.glue_client = None
    
    def diagnose_glue_connection_failure(self, connection_name: str, error: Exception) -> Dict[str, Any]:
        """Diagnose Glue connection failures with detailed diagnostics."""
        diagnostics = {
            'connection_name': connection_name,
            'error_type': type(error).__name__,
            'error_message': str(error),
            'diagnostics': [],
            'recommendations': []
        }
        
        if not self.glue_client:
            diagnostics['diagnostics'].append("Unable to perform diagnostics - Glue client not available")
            return diagnostics
        
        try:
            self.structured_logger.info("Starting Glue connection diagnostics", connection_name=connection_name)
            
            # Check if connection exists
            try:
                response = self.glue_client.get_connection(Name=connection_name)
                connection = response.get('Connection', {})
                diagnostics['connection_exists'] = True
                
                # Analyze connection configuration
                physical_reqs = connection.get('PhysicalConnectionRequirements', {})
                subnet_id = physical_reqs.get('SubnetId')
                security_groups = physical_reqs.get('SecurityGroupIdList', [])
                availability_zone = physical_reqs.get('AvailabilityZone')
                
                diagnostics['subnet_id'] = subnet_id
                diagnostics['security_groups'] = security_groups
                diagnostics['availability_zone'] = availability_zone
                
                # Validate subnet accessibility
                if subnet_id:
                    subnet_diagnostics = self._diagnose_subnet_accessibility(subnet_id)
                    diagnostics['subnet_diagnostics'] = subnet_diagnostics
                    diagnostics['diagnostics'].extend(subnet_diagnostics.get('issues', []))
                
                # Validate security group rules
                if security_groups:
                    sg_diagnostics = self._diagnose_security_group_rules(security_groups)
                    diagnostics['security_group_diagnostics'] = sg_diagnostics
                    diagnostics['diagnostics'].extend(sg_diagnostics.get('issues', []))
                
                # Check for ENI creation issues
                eni_diagnostics = self._diagnose_eni_creation_issues(subnet_id)
                diagnostics['eni_diagnostics'] = eni_diagnostics
                diagnostics['diagnostics'].extend(eni_diagnostics.get('issues', []))
                
            except self.glue_client.exceptions.EntityNotFoundException:
                diagnostics['connection_exists'] = False
                diagnostics['diagnostics'].append(f"Glue connection '{connection_name}' does not exist")
                diagnostics['recommendations'].append(f"Create Glue connection '{connection_name}' with proper VPC configuration")
            
        except Exception as diag_error:
            self.structured_logger.error("Failed to diagnose Glue connection", 
                                       connection_name=connection_name, 
                                       error=str(diag_error))
            diagnostics['diagnostics'].append(f"Diagnostic failure: {str(diag_error)}")
        
        return diagnostics
    
    def _diagnose_subnet_accessibility(self, subnet_id: str) -> Dict[str, Any]:
        """Diagnose subnet accessibility issues."""
        diagnostics = {'subnet_id': subnet_id, 'issues': [], 'recommendations': []}
        
        if not self.ec2_client:
            diagnostics['issues'].append("Unable to diagnose subnet - EC2 client not available")
            return diagnostics
        
        try:
            response = self.ec2_client.describe_subnets(SubnetIds=[subnet_id])
            subnet = response['Subnets'][0]
            
            # Check subnet state
            if subnet['State'] != 'available':
                diagnostics['issues'].append(f"Subnet {subnet_id} is in '{subnet['State']}' state, not 'available'")
                diagnostics['recommendations'].append(f"Ensure subnet {subnet_id} is in 'available' state")
            
            # Check available IP addresses
            available_ips = subnet.get('AvailableIpAddressCount', 0)
            if available_ips < 2:
                diagnostics['issues'].append(f"Subnet {subnet_id} has only {available_ips} available IP addresses")
                diagnostics['recommendations'].append(f"Ensure subnet {subnet_id} has sufficient available IP addresses for ENI creation")
            
            # Check route table for internet/NAT gateway access
            vpc_id = subnet['VpcId']
            route_diagnostics = self._diagnose_route_table_connectivity(subnet_id, vpc_id)
            diagnostics.update(route_diagnostics)
            
        except Exception as e:
            diagnostics['issues'].append(f"Failed to describe subnet {subnet_id}: {str(e)}")
        
        return diagnostics
    
    def _diagnose_route_table_connectivity(self, subnet_id: str, vpc_id: str) -> Dict[str, Any]:
        """Diagnose route table connectivity for subnet."""
        diagnostics = {'route_issues': [], 'route_recommendations': []}
        
        if not self.ec2_client:
            diagnostics['route_issues'].append("Unable to diagnose routes - EC2 client not available")
            return diagnostics
        
        try:
            # Get route tables associated with the subnet
            response = self.ec2_client.describe_route_tables(
                Filters=[
                    {'Name': 'association.subnet-id', 'Values': [subnet_id]}
                ]
            )
            
            route_tables = response.get('RouteTables', [])
            if not route_tables:
                # Check VPC main route table
                response = self.ec2_client.describe_route_tables(
                    Filters=[
                        {'Name': 'vpc-id', 'Values': [vpc_id]},
                        {'Name': 'association.main', 'Values': ['true']}
                    ]
                )
                route_tables = response.get('RouteTables', [])
            
            if route_tables:
                route_table = route_tables[0]
                routes = route_table.get('Routes', [])
                
                # Check for internet gateway or NAT gateway routes
                has_internet_route = any(
                    route.get('GatewayId', '').startswith('igw-') or 
                    route.get('NatGatewayId', '').startswith('nat-')
                    for route in routes
                )
                
                if not has_internet_route:
                    diagnostics['route_issues'].append(
                        f"Subnet {subnet_id} route table lacks internet gateway or NAT gateway route"
                    )
                    diagnostics['route_recommendations'].append(
                        "Add route to internet gateway (for public subnet) or NAT gateway (for private subnet)"
                    )
            
        except Exception as e:
            diagnostics['route_issues'].append(f"Failed to analyze route tables: {str(e)}")
        
        return diagnostics
    
    def _diagnose_security_group_rules(self, security_group_ids: List[str]) -> Dict[str, Any]:
        """Diagnose security group rule issues."""
        diagnostics = {'security_groups': security_group_ids, 'issues': [], 'recommendations': []}
        
        if not self.ec2_client:
            diagnostics['issues'].append("Unable to diagnose security groups - EC2 client not available")
            return diagnostics
        
        try:
            response = self.ec2_client.describe_security_groups(GroupIds=security_group_ids)
            
            for sg in response['SecurityGroups']:
                sg_id = sg['GroupId']
                
                # Check outbound rules for database ports
                outbound_rules = sg.get('IpPermissionsEgress', [])
                has_database_outbound = any(
                    self._rule_allows_database_ports(rule) for rule in outbound_rules
                )
                
                if not has_database_outbound:
                    diagnostics['issues'].append(
                        f"Security group {sg_id} lacks outbound rules for common database ports"
                    )
                    diagnostics['recommendations'].append(
                        f"Add outbound rules to security group {sg_id} for database ports (1433, 1521, 5432, 50000)"
                    )
                
                # Check for overly restrictive rules
                inbound_rules = sg.get('IpPermissions', [])
                if not inbound_rules:
                    diagnostics['issues'].append(
                        f"Security group {sg_id} has no inbound rules - may be too restrictive"
                    )
        
        except Exception as e:
            diagnostics['issues'].append(f"Failed to analyze security groups: {str(e)}")
        
        return diagnostics
    
    def _rule_allows_database_ports(self, rule: Dict[str, Any]) -> bool:
        """Check if security group rule allows common database ports."""
        from_port = rule.get('FromPort')
        to_port = rule.get('ToPort')
        
        if from_port is None or to_port is None:
            return False
        
        database_ports = [1433, 1521, 5432, 50000]  # SQL Server, Oracle, PostgreSQL, DB2
        
        return any(
            from_port <= port <= to_port for port in database_ports
        )
    
    def _diagnose_eni_creation_issues(self, subnet_id: str) -> Dict[str, Any]:
        """Diagnose ENI creation issues and service limits."""
        diagnostics = {'issues': [], 'recommendations': []}
        
        if not self.ec2_client:
            diagnostics['issues'].append("Unable to diagnose ENI issues - EC2 client not available")
            return diagnostics
        
        try:
            # Check ENI limits
            response = self.ec2_client.describe_account_attributes(
                AttributeNames=['max-elastic-network-interfaces']
            )
            
            max_enis = 0
            for attr in response.get('AccountAttributes', []):
                if attr['AttributeName'] == 'max-elastic-network-interfaces':
                    max_enis = int(attr['AttributeValues'][0]['AttributeValue'])
                    break
            
            # Count current ENIs
            eni_response = self.ec2_client.describe_network_interfaces()
            current_enis = len(eni_response.get('NetworkInterfaces', []))
            
            if current_enis >= max_enis * 0.9:  # 90% threshold
                diagnostics['issues'].append(
                    f"ENI usage is high: {current_enis}/{max_enis} (90%+ threshold reached)"
                )
                diagnostics['recommendations'].append(
                    "Consider requesting ENI limit increase or cleaning up unused ENIs"
                )
            
        except Exception as e:
            diagnostics['issues'].append(f"Failed to check ENI limits: {str(e)}")
        
        return diagnostics
    
    def diagnose_vpc_endpoint_issues(self, vpc_id: str, service_name: str = 's3') -> Dict[str, Any]:
        """Diagnose VPC endpoint connectivity issues."""
        diagnostics = {
            'vpc_id': vpc_id,
            'service_name': service_name,
            'issues': [],
            'recommendations': []
        }
        
        if not self.ec2_client:
            diagnostics['issues'].append("Unable to diagnose VPC endpoints - EC2 client not available")
            return diagnostics
        
        try:
            # Check if VPC endpoint exists
            region = boto3.Session().region_name or 'us-east-1'
            response = self.ec2_client.describe_vpc_endpoints(
                Filters=[
                    {'Name': 'vpc-id', 'Values': [vpc_id]},
                    {'Name': 'service-name', 'Values': [f'com.amazonaws.{region}.{service_name}']}
                ]
            )
            
            endpoints = response.get('VpcEndpoints', [])
            if not endpoints:
                diagnostics['issues'].append(f"No {service_name} VPC endpoint found in VPC {vpc_id}")
                diagnostics['recommendations'].append(f"Create {service_name} VPC endpoint in VPC {vpc_id}")
            else:
                endpoint = endpoints[0]
                if endpoint['State'] != 'Available':
                    diagnostics['issues'].append(
                        f"VPC endpoint {endpoint['VpcEndpointId']} is in '{endpoint['State']}' state"
                    )
                    diagnostics['recommendations'].append(
                        f"Wait for VPC endpoint {endpoint['VpcEndpointId']} to become 'Available'"
                    )
        
        except Exception as e:
            diagnostics['issues'].append(f"Failed to check VPC endpoints: {str(e)}")
        
        return diagnostics


class ConnectionRetryHandler:
    """Enhanced connection retry handler with exponential backoff and error classification."""
    
    def __init__(self, max_retries: int = 3, base_delay: float = 1.0, 
                 max_delay: float = 60.0, backoff_factor: float = 2.0,
                 jitter: bool = True):
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.backoff_factor = backoff_factor
        self.jitter = jitter
        self.error_classifier = ErrorClassifier()
        self.network_error_handler = NetworkErrorHandler()
        self.structured_logger = StructuredLogger("ConnectionRetryHandler")
    
    def execute_with_retry(self, operation, operation_name: str, *args, **kwargs):
        """Execute operation with intelligent retry logic based on error classification."""
        last_exception = None
        retry_count = 0
        
        for attempt in range(self.max_retries + 1):
            try:
                self.structured_logger.info(f"Attempting {operation_name} (attempt {attempt + 1}/{self.max_retries + 1})")
                result = operation(*args, **kwargs)
                
                if attempt > 0:
                    self.structured_logger.info(f"{operation_name} succeeded after {attempt + 1} attempts")
                
                return result
                
            except Exception as e:
                last_exception = e
                error_category = self.error_classifier.classify_error(e)
                recovery_strategy = self.error_classifier.get_recovery_strategy(e)
                
                self.structured_logger.warning(
                    f"{operation_name} failed (attempt {attempt + 1}): {str(e)} "
                    f"[Category: {error_category}, Strategy: {recovery_strategy}]"
                )
                
                # Perform network-specific diagnostics for network errors
                self._perform_network_diagnostics(e, error_category, operation_name, *args, **kwargs)
                
                # Check if error is retryable
                if not self.error_classifier.is_retryable_error(e):
                    self.structured_logger.error(f"{operation_name} failed with non-retryable error: {str(e)}")
                    raise RuntimeError(f"{operation_name} failed: {str(e)} [Non-retryable: {error_category}]")
                
                # Don't retry on last attempt
                if attempt < self.max_retries:
                    delay = self._calculate_delay(attempt, recovery_strategy)
                    self.structured_logger.info(f"Retrying {operation_name} in {delay:.1f} seconds...")
                    time.sleep(delay)
                    retry_count += 1
                else:
                    self.structured_logger.error(f"{operation_name} failed after {self.max_retries + 1} attempts: {str(e)}")
        
        # Create detailed error message with retry information
        error_details = {
            'operation': operation_name,
            'total_attempts': self.max_retries + 1,
            'retry_count': retry_count,
            'last_error': str(last_exception),
            'error_category': self.error_classifier.classify_error(last_exception),
            'recovery_strategy': self.error_classifier.get_recovery_strategy(last_exception)
        }
        
        raise RuntimeError(
            f"{operation_name} failed after {self.max_retries + 1} attempts. "
            f"Error details: {error_details}"
        )
    
    def _calculate_delay(self, attempt: int, recovery_strategy: str) -> float:
        """Calculate delay based on attempt number and recovery strategy."""
        if recovery_strategy == "retry_with_longer_delay":
            # Use longer delays for resource exhaustion
            base_delay = self.base_delay * 3
        else:
            base_delay = self.base_delay
        
        # Calculate exponential backoff
        delay = min(base_delay * (self.backoff_factor ** attempt), self.max_delay)
        
        # Add jitter to prevent thundering herd
        if self.jitter:
            import random
            jitter_factor = random.uniform(0.5, 1.5)
            delay *= jitter_factor
        
        return delay
    
    def retry_with_network_recovery(self, operation, operation_name: str, 
                                  connection_config=None, *args, **kwargs):
        """Execute operation with network-aware retry logic and recovery."""
        last_exception = None
        
        for attempt in range(self.max_retries + 1):
            try:
                self.structured_logger.info(
                    f"Attempting {operation_name} with network recovery (attempt {attempt + 1}/{self.max_retries + 1})"
                )
                
                # Pre-validate network connectivity if connection config is provided
                if connection_config and attempt > 0:  # Skip on first attempt
                    self._validate_network_connectivity_before_retry(connection_config)
                
                result = operation(*args, **kwargs)
                
                if attempt > 0:
                    self.structured_logger.info(f"{operation_name} succeeded after {attempt + 1} attempts with network recovery")
                
                return result
                
            except (NetworkConnectivityError, GlueConnectionError, VpcEndpointError, ENICreationError) as network_error:
                last_exception = network_error
                
                self.structured_logger.error(
                    f"Network-specific error in {operation_name} (attempt {attempt + 1})",
                    error_type=type(network_error).__name__,
                    error_message=str(network_error)
                )
                
                # Perform detailed diagnostics for network errors
                if hasattr(network_error, 'connection_name') and network_error.connection_name:
                    diagnostics = self.network_error_handler.diagnose_glue_connection_failure(
                        network_error.connection_name, network_error
                    )
                    self.structured_logger.error(
                        "Network error diagnostics",
                        connection_name=network_error.connection_name,
                        diagnostics=diagnostics
                    )
                
                # Don't retry network configuration errors
                if isinstance(network_error, (GlueConnectionError, VpcEndpointError)):
                    self.structured_logger.error(f"Non-retryable network configuration error: {str(network_error)}")
                    raise network_error
                
                # Retry ENI and connectivity errors with longer delays
                if attempt < self.max_retries:
                    delay = self._calculate_network_recovery_delay(attempt, type(network_error))
                    self.structured_logger.info(f"Retrying {operation_name} after network error in {delay:.1f} seconds...")
                    time.sleep(delay)
                else:
                    self.structured_logger.error(f"{operation_name} failed after {self.max_retries + 1} attempts with network errors")
                    raise network_error
                    
            except Exception as e:
                # Handle non-network errors with standard retry logic
                return self.execute_with_retry(operation, operation_name, *args, **kwargs)
        
        raise last_exception
    
    def _validate_network_connectivity_before_retry(self, connection_config):
        """Validate network connectivity before retry attempt."""
        try:
            if hasattr(connection_config, 'requires_cross_vpc_connection') and connection_config.requires_cross_vpc_connection():
                connection_name = connection_config.get_glue_connection_name()
                if connection_name:
                    # Quick validation of Glue connection existence
                    diagnostics = self.network_error_handler.diagnose_glue_connection_failure(
                        connection_name, Exception("Pre-retry validation")
                    )
                    
                    if not diagnostics.get('connection_exists', False):
                        raise GlueConnectionError(
                            f"Glue connection '{connection_name}' does not exist",
                            connection_name
                        )
                    
                    self.structured_logger.info(
                        "Network connectivity pre-validation passed",
                        connection_name=connection_name
                    )
        
        except Exception as validation_error:
            self.structured_logger.warning(
                "Network connectivity pre-validation failed",
                error=str(validation_error)
            )
            # Don't fail the retry attempt due to validation issues
    
    def _calculate_network_recovery_delay(self, attempt: int, error_type: type) -> float:
        """Calculate delay for network recovery based on error type."""
        # Use longer delays for network-related errors
        base_multiplier = 1.0
        
        if error_type == ENICreationError:
            # ENI creation failures may need longer recovery time
            base_multiplier = 3.0
        elif error_type == NetworkConnectivityError:
            # Network connectivity issues may resolve quickly
            base_multiplier = 1.5
        elif error_type == VpcEndpointError:
            # VPC endpoint issues typically need longer recovery
            base_multiplier = 2.0
        
        delay = min(
            self.base_delay * base_multiplier * (self.backoff_factor ** attempt), 
            self.max_delay
        )
        
        # Add jitter for network recovery
        if self.jitter:
            import random
            jitter_factor = random.uniform(0.8, 1.2)
            delay *= jitter_factor
        
        return delay
    
    def execute_with_circuit_breaker(self, operation, operation_name: str, 
                                   failure_threshold: int = 5, 
                                   recovery_timeout: float = 300.0, *args, **kwargs):
        """Execute operation with circuit breaker pattern for repeated failures."""
        # Simple circuit breaker implementation
        circuit_key = f"circuit_{operation_name}"
        
        # This would typically be stored in a shared cache/database
        # For simplicity, using class-level storage
        if not hasattr(self, '_circuit_states'):
            self._circuit_states = {}
        
        circuit_state = self._circuit_states.get(circuit_key, {
            'failure_count': 0,
            'last_failure_time': 0,
            'state': 'closed'  # closed, open, half_open
        })
        
        current_time = time.time()
        
        # Check circuit state
        if circuit_state['state'] == 'open':
            if current_time - circuit_state['last_failure_time'] > recovery_timeout:
                circuit_state['state'] = 'half_open'
                logger.info(f"Circuit breaker for {operation_name} moving to half-open state")
            else:
                raise RuntimeError(
                    f"Circuit breaker is open for {operation_name}. "
                    f"Will retry after {recovery_timeout - (current_time - circuit_state['last_failure_time']):.1f} seconds"
                )
        
        try:
            result = self.execute_with_retry(operation, operation_name, *args, **kwargs)
            
            # Success - reset circuit breaker
            if circuit_state['state'] == 'half_open':
                circuit_state['state'] = 'closed'
                circuit_state['failure_count'] = 0
                logger.info(f"Circuit breaker for {operation_name} reset to closed state")
            
            self._circuit_states[circuit_key] = circuit_state
            return result
            
        except Exception as e:
            # Failure - update circuit breaker
            circuit_state['failure_count'] += 1
            circuit_state['last_failure_time'] = current_time
            
            if circuit_state['failure_count'] >= failure_threshold:
                circuit_state['state'] = 'open'
                logger.error(
                    f"Circuit breaker opened for {operation_name} after {failure_threshold} failures. "
                    f"Will remain open for {recovery_timeout} seconds"
                )
            
            self._circuit_states[circuit_key] = circuit_state
            raise
    
    def _perform_network_diagnostics(self, error: Exception, error_category: str, operation_name: str, *args, **kwargs):
        """Perform detailed network diagnostics for network-related errors."""
        try:
            # Check if this is a network-related error that warrants diagnostics
            network_error_categories = {
                'network_connectivity', 'cross_vpc_routing', 'glue_connection', 
                'eni_creation', 'vpc_endpoint'
            }
            
            if error_category not in network_error_categories:
                return
            
            self.structured_logger.info(
                "Performing network diagnostics for error",
                operation_name=operation_name,
                error_category=error_category,
                error_message=str(error)
            )
            
            # Extract connection information from arguments
            connection_name = self._extract_connection_name_from_args(*args, **kwargs)
            vpc_id = self._extract_vpc_id_from_args(*args, **kwargs)
            
            # Perform Glue connection diagnostics if connection name is available
            if connection_name and error_category in ['glue_connection', 'cross_vpc_routing']:
                try:
                    diagnostics = self.network_error_handler.diagnose_glue_connection_failure(
                        connection_name, error
                    )
                    self.structured_logger.error(
                        "Glue connection diagnostics completed",
                        operation_name=operation_name,
                        connection_name=connection_name,
                        diagnostics_summary={
                            'connection_exists': diagnostics.get('connection_exists'),
                            'issues_count': len(diagnostics.get('diagnostics', [])),
                            'recommendations_count': len(diagnostics.get('recommendations', []))
                        }
                    )
                    
                    # Log specific issues and recommendations
                    for issue in diagnostics.get('diagnostics', []):
                        self.structured_logger.warning(f"Network issue detected: {issue}")
                    
                    for recommendation in diagnostics.get('recommendations', []):
                        self.structured_logger.info(f"Network recommendation: {recommendation}")
                        
                except Exception as diag_error:
                    self.structured_logger.warning(
                        "Failed to perform Glue connection diagnostics",
                        connection_name=connection_name,
                        error=str(diag_error)
                    )
            
            # Perform VPC endpoint diagnostics if VPC ID is available
            if vpc_id and error_category == 'vpc_endpoint':
                try:
                    vpc_diagnostics = self.network_error_handler.diagnose_vpc_endpoint_issues(vpc_id)
                    self.structured_logger.error(
                        "VPC endpoint diagnostics completed",
                        operation_name=operation_name,
                        vpc_id=vpc_id,
                        diagnostics_summary={
                            'issues_count': len(vpc_diagnostics.get('issues', [])),
                            'recommendations_count': len(vpc_diagnostics.get('recommendations', []))
                        }
                    )
                    
                    # Log specific issues and recommendations
                    for issue in vpc_diagnostics.get('issues', []):
                        self.structured_logger.warning(f"VPC endpoint issue: {issue}")
                    
                    for recommendation in vpc_diagnostics.get('recommendations', []):
                        self.structured_logger.info(f"VPC endpoint recommendation: {recommendation}")
                        
                except Exception as diag_error:
                    self.structured_logger.warning(
                        "Failed to perform VPC endpoint diagnostics",
                        vpc_id=vpc_id,
                        error=str(diag_error)
                    )
            
        except Exception as diag_error:
            self.structured_logger.warning(
                "Failed to perform network diagnostics",
                operation_name=operation_name,
                error=str(diag_error)
            )
    
    def _extract_connection_name_from_args(self, *args, **kwargs) -> Optional[str]:
        """Extract Glue connection name from operation arguments."""
        try:
            # Check kwargs first
            if 'glue_connection_name' in kwargs:
                return kwargs['glue_connection_name']
            
            if 'connection_name' in kwargs:
                return kwargs['connection_name']
            
            # Check args for connection config objects
            for arg in args:
                if hasattr(arg, 'get_glue_connection_name'):
                    connection_name = arg.get_glue_connection_name()
                    if connection_name:
                        return connection_name
                
                if hasattr(arg, 'network_config') and arg.network_config:
                    if hasattr(arg.network_config, 'glue_connection_name'):
                        return arg.network_config.glue_connection_name
            
            return None
            
        except Exception:
            return None
    
    def _extract_vpc_id_from_args(self, *args, **kwargs) -> Optional[str]:
        """Extract VPC ID from operation arguments."""
        try:
            # Check kwargs first
            if 'vpc_id' in kwargs:
                return kwargs['vpc_id']
            
            # Check args for connection config objects
            for arg in args:
                if hasattr(arg, 'network_config') and arg.network_config:
                    if hasattr(arg.network_config, 'vpc_id'):
                        return arg.network_config.vpc_id
            
            return None
            
        except Exception:
            return None


class GlueConnectionManager:
    """Manages Glue connections for cross-VPC database access."""
    
    def __init__(self, glue_context: GlueContext):
        self.glue_context = glue_context
        self.glue_client = boto3.client('glue')
        self.structured_logger = StructuredLogger("GlueConnectionManager")
    
    def get_glue_connection(self, connection_name: str) -> Optional[Dict[str, Any]]:
        """Retrieve Glue connection details for cross-VPC database access with enhanced error handling.
        
        Args:
            connection_name: Name of the Glue connection
            
        Returns:
            Dictionary containing connection details or None if not found
            
        Raises:
            GlueConnectionError: For Glue connection specific issues
        """
        if not connection_name or connection_name.strip() == '':
            self.structured_logger.debug("No Glue connection name provided")
            return None
        
        try:
            self.structured_logger.info("Retrieving Glue connection", connection_name=connection_name)
            
            response = self.glue_client.get_connection(Name=connection_name)
            connection = response.get('Connection', {})
            
            connection_details = {
                'name': connection.get('Name'),
                'connection_type': connection.get('ConnectionType'),
                'connection_properties': connection.get('ConnectionProperties', {}),
                'physical_connection_requirements': connection.get('PhysicalConnectionRequirements', {})
            }
            
            # Validate connection configuration
            self._validate_glue_connection_config(connection_details, connection_name)
            
            self.structured_logger.info(
                "Successfully retrieved and validated Glue connection",
                connection_name=connection_name,
                connection_type=connection_details['connection_type']
            )
            
            return connection_details
            
        except self.glue_client.exceptions.EntityNotFoundException:
            self.structured_logger.warning(
                "Glue connection not found",
                connection_name=connection_name
            )
            return None
        except GlueConnectionError:
            raise
        except Exception as e:
            self.structured_logger.error(
                "Failed to retrieve Glue connection",
                connection_name=connection_name,
                error=str(e)
            )
            raise GlueConnectionError(
                f"Failed to retrieve Glue connection '{connection_name}': {str(e)}",
                connection_name,
                {'error_type': 'retrieval_error', 'original_error': str(e)}
            )
    
    def _validate_glue_connection_config(self, connection_details: Dict[str, Any], connection_name: str):
        """Validate Glue connection configuration."""
        connection_type = connection_details.get('connection_type')
        if connection_type != 'JDBC':
            raise GlueConnectionError(
                f"Glue connection '{connection_name}' is not a JDBC connection (type: {connection_type})",
                connection_name,
                {'error_type': 'invalid_connection_type', 'connection_type': connection_type}
            )
        
        physical_reqs = connection_details.get('physical_connection_requirements', {})
        if not physical_reqs:
            raise GlueConnectionError(
                f"Glue connection '{connection_name}' lacks physical connection requirements",
                connection_name,
                {'error_type': 'missing_physical_requirements'}
            )
        
        # Check for required network configuration
        subnet_id = physical_reqs.get('SubnetId')
        security_groups = physical_reqs.get('SecurityGroupIdList', [])
        
        if not subnet_id:
            raise GlueConnectionError(
                f"Glue connection '{connection_name}' missing subnet configuration",
                connection_name,
                {'error_type': 'missing_subnet', 'physical_requirements': physical_reqs}
            )
        
        if not security_groups:
            raise GlueConnectionError(
                f"Glue connection '{connection_name}' missing security group configuration",
                connection_name,
                {'error_type': 'missing_security_groups', 'physical_requirements': physical_reqs}
            )
    
    def validate_network_connectivity(self, connection_name: str, 
                                    connection_string: str, 
                                    timeout_seconds: int = 30) -> bool:
        """Test database connectivity before processing with enhanced error handling.
        
        Args:
            connection_name: Name of the Glue connection (empty for same-VPC)
            connection_string: JDBC connection string
            timeout_seconds: Connection timeout in seconds
            
        Returns:
            True if connectivity test passes, False otherwise
            
        Raises:
            NetworkConnectivityError: For network connectivity issues
            GlueConnectionError: For Glue connection specific issues
            VpcEndpointError: For VPC endpoint issues
        """
        try:
            self.structured_logger.info(
                "Starting network connectivity validation",
                connection_name=connection_name or "same-vpc",
                timeout_seconds=timeout_seconds
            )
            
            start_time = time.time()
            
            # If no Glue connection name, assume same-VPC connectivity
            if not connection_name or connection_name.strip() == '':
                self.structured_logger.info("Using same-VPC connectivity (no Glue connection)")
                # For same-VPC, we'll validate during actual connection attempt
                return True
            
            # Retrieve Glue connection details with error handling
            try:
                connection_details = self.get_glue_connection(connection_name)
                if not connection_details:
                    raise GlueConnectionError(
                        f"Glue connection '{connection_name}' not found or inaccessible",
                        connection_name
                    )
            except Exception as conn_error:
                if "EntityNotFoundException" in str(conn_error):
                    raise GlueConnectionError(
                        f"Glue connection '{connection_name}' does not exist",
                        connection_name,
                        {'error_type': 'not_found'}
                    )
                else:
                    raise GlueConnectionError(
                        f"Failed to retrieve Glue connection '{connection_name}': {str(conn_error)}",
                        connection_name,
                        {'error_type': 'access_error', 'original_error': str(conn_error)}
                    )
            
            # Validate connection properties
            connection_properties = connection_details.get('connection_properties', {})
            physical_requirements = connection_details.get('physical_connection_requirements', {})
            
            # Check if connection has required network configuration
            subnet_id = physical_requirements.get('SubnetId')
            security_groups = physical_requirements.get('SecurityGroupIdList', [])
            
            if not subnet_id:
                raise GlueConnectionError(
                    f"Glue connection '{connection_name}' missing subnet configuration",
                    connection_name,
                    {'error_type': 'missing_subnet', 'physical_requirements': physical_requirements}
                )
            
            if not security_groups:
                raise GlueConnectionError(
                    f"Glue connection '{connection_name}' missing security group configuration",
                    connection_name,
                    {'error_type': 'missing_security_groups', 'physical_requirements': physical_requirements}
                )
            
            # Perform detailed network validation
            self._validate_subnet_accessibility(subnet_id, connection_name)
            self._validate_security_group_rules(security_groups, connection_name)
            
            # Validate that connection URL matches expected format
            stored_url = connection_properties.get('JDBC_CONNECTION_URL', '')
            if stored_url and stored_url != connection_string:
                self.structured_logger.warning(
                    "Connection string mismatch between parameter and Glue connection",
                    connection_name=connection_name,
                    parameter_url=connection_string[:50] + "...",
                    stored_url=stored_url[:50] + "..."
                )
            
            duration = time.time() - start_time
            self.structured_logger.info(
                "Network connectivity validation completed successfully",
                connection_name=connection_name,
                duration_seconds=round(duration, 2),
                subnet_id=subnet_id,
                security_groups_count=len(security_groups)
            )
            
            return True
            
        except (NetworkConnectivityError, GlueConnectionError, VpcEndpointError) as network_error:
            duration = time.time() - start_time
            self.structured_logger.error(
                "Network connectivity validation failed with network error",
                connection_name=connection_name or "same-vpc",
                error_type=type(network_error).__name__,
                error_message=str(network_error),
                duration_seconds=round(duration, 2)
            )
            raise network_error
            
        except Exception as e:
            duration = time.time() - start_time
            self.structured_logger.error(
                "Network connectivity validation failed with unexpected error",
                connection_name=connection_name or "same-vpc",
                error=str(e),
                duration_seconds=round(duration, 2)
            )
            raise NetworkConnectivityError(
                f"Network connectivity validation failed: {str(e)}",
                error_type='validation_error',
                connection_name=connection_name
            )
    
    def _validate_subnet_accessibility(self, subnet_id: str, connection_name: str):
        """Validate subnet accessibility for Glue connection."""
        try:
            if not hasattr(self, 'ec2_client'):
                self.ec2_client = boto3.client('ec2')
            
            response = self.ec2_client.describe_subnets(SubnetIds=[subnet_id])
            subnet = response['Subnets'][0]
            
            # Check subnet state
            if subnet['State'] != 'available':
                raise NetworkConnectivityError(
                    f"Subnet {subnet_id} for Glue connection '{connection_name}' is in '{subnet['State']}' state, not 'available'",
                    error_type='subnet_unavailable',
                    connection_name=connection_name
                )
            
            # Check available IP addresses
            available_ips = subnet.get('AvailableIpAddressCount', 0)
            if available_ips < 2:
                raise ENICreationError(
                    f"Subnet {subnet_id} has insufficient IP addresses ({available_ips} available) for ENI creation",
                    subnet_id=subnet_id,
                    error_code='insufficient_ips'
                )
            
            self.structured_logger.debug(
                "Subnet accessibility validation passed",
                subnet_id=subnet_id,
                subnet_state=subnet['State'],
                available_ips=available_ips
            )
            
        except (NetworkConnectivityError, ENICreationError):
            raise
        except Exception as e:
            raise NetworkConnectivityError(
                f"Failed to validate subnet {subnet_id} accessibility: {str(e)}",
                error_type='subnet_validation_error',
                connection_name=connection_name
            )
    
    def _validate_security_group_rules(self, security_group_ids: List[str], connection_name: str):
        """Validate security group rules for database connectivity."""
        try:
            if not hasattr(self, 'ec2_client'):
                self.ec2_client = boto3.client('ec2')
            
            response = self.ec2_client.describe_security_groups(GroupIds=security_group_ids)
            
            for sg in response['SecurityGroups']:
                sg_id = sg['GroupId']
                
                # Check outbound rules for database ports
                outbound_rules = sg.get('IpPermissionsEgress', [])
                has_database_outbound = any(
                    self._rule_allows_database_ports(rule) for rule in outbound_rules
                )
                
                if not has_database_outbound:
                    self.structured_logger.warning(
                        f"Security group {sg_id} may lack outbound rules for database ports",
                        connection_name=connection_name,
                        security_group_id=sg_id
                    )
                    # Don't fail validation, just warn
            
            self.structured_logger.debug(
                "Security group validation completed",
                connection_name=connection_name,
                security_groups=security_group_ids
            )
            
        except Exception as e:
            self.structured_logger.warning(
                "Failed to validate security group rules",
                connection_name=connection_name,
                security_groups=security_group_ids,
                error=str(e)
            )
            # Don't fail validation for security group rule issues
    
    def _rule_allows_database_ports(self, rule: Dict[str, Any]) -> bool:
        """Check if security group rule allows common database ports."""
        from_port = rule.get('FromPort')
        to_port = rule.get('ToPort')
        
        if from_port is None or to_port is None:
            return False
        
        database_ports = [1433, 1521, 5432, 50000]  # SQL Server, Oracle, PostgreSQL, DB2
        
        return any(
            from_port <= port <= to_port for port in database_ports
        )
    
    def setup_jdbc_with_connection(self, connection_config: 'ConnectionConfig', 
                                 glue_connection_name: str) -> Dict[str, Any]:
        """Configure JDBC connection properties using Glue connection when specified with enhanced error handling.
        
        Args:
            connection_config: Database connection configuration
            glue_connection_name: Name of Glue connection (empty for same-VPC)
            
        Returns:
            Dictionary with JDBC connection properties
            
        Raises:
            GlueConnectionError: For Glue connection specific issues
            NetworkConnectivityError: For network connectivity issues
        """
        try:
            self.structured_logger.info(
                "Setting up JDBC connection with enhanced error handling",
                engine_type=connection_config.engine_type,
                glue_connection_name=glue_connection_name or "same-vpc"
            )
            
            # Base JDBC properties
            jdbc_properties = {
                'url': connection_config.connection_string,
                'user': connection_config.username,
                'password': connection_config.password,
                'driver': DatabaseEngineManager.get_driver_class(connection_config.engine_type)
            }
            
            # If no Glue connection specified, use direct connection
            if not glue_connection_name or glue_connection_name.strip() == '':
                self.structured_logger.info("Using direct JDBC connection (same-VPC)")
                return jdbc_properties
            
            # Retrieve Glue connection details for cross-VPC access with error handling
            try:
                connection_details = self.get_glue_connection(glue_connection_name)
                if not connection_details:
                    raise GlueConnectionError(
                        f"Glue connection '{glue_connection_name}' not found",
                        glue_connection_name,
                        {'error_type': 'not_found'}
                    )
            except GlueConnectionError:
                raise
            except Exception as conn_error:
                raise GlueConnectionError(
                    f"Failed to retrieve Glue connection '{glue_connection_name}': {str(conn_error)}",
                    glue_connection_name,
                    {'error_type': 'retrieval_error', 'original_error': str(conn_error)}
                )
            
            # Use connection properties from Glue connection if available
            glue_properties = connection_details.get('connection_properties', {})
            
            # Override with Glue connection properties if they exist
            if glue_properties.get('JDBC_CONNECTION_URL'):
                jdbc_properties['url'] = glue_properties['JDBC_CONNECTION_URL']
                self.structured_logger.debug(
                    "Using connection URL from Glue connection",
                    glue_connection_name=glue_connection_name
                )
            
            if glue_properties.get('USERNAME'):
                jdbc_properties['user'] = glue_properties['USERNAME']
                self.structured_logger.debug(
                    "Using username from Glue connection",
                    glue_connection_name=glue_connection_name
                )
            
            if glue_properties.get('PASSWORD'):
                jdbc_properties['password'] = glue_properties['PASSWORD']
                self.structured_logger.debug(
                    "Using password from Glue connection",
                    glue_connection_name=glue_connection_name
                )
            
            # Add network configuration metadata
            physical_requirements = connection_details.get('physical_connection_requirements', {})
            jdbc_properties['_glue_connection_metadata'] = {
                'connection_name': glue_connection_name,
                'subnet_id': physical_requirements.get('SubnetId'),
                'security_groups': physical_requirements.get('SecurityGroupIdList', []),
                'availability_zone': physical_requirements.get('AvailabilityZone')
            }
            
            # Validate network configuration before returning
            self._validate_network_configuration_for_jdbc(physical_requirements, glue_connection_name)
            
            self.structured_logger.info(
                "Successfully configured JDBC with Glue connection",
                glue_connection_name=glue_connection_name,
                subnet_id=physical_requirements.get('SubnetId'),
                security_groups_count=len(physical_requirements.get('SecurityGroupIdList', []))
            )
            
            return jdbc_properties
            
        except (GlueConnectionError, NetworkConnectivityError):
            raise
        except Exception as e:
            self.structured_logger.error(
                "Failed to setup JDBC with Glue connection",
                glue_connection_name=glue_connection_name,
                error=str(e)
            )
            raise NetworkConnectivityError(
                f"Failed to setup JDBC with Glue connection '{glue_connection_name}': {str(e)}",
                error_type='jdbc_setup_error',
                connection_name=glue_connection_name
            )
    
    def _validate_network_configuration_for_jdbc(self, physical_requirements: Dict[str, Any], connection_name: str):
        """Validate network configuration for JDBC setup."""
        subnet_id = physical_requirements.get('SubnetId')
        security_groups = physical_requirements.get('SecurityGroupIdList', [])
        
        if not subnet_id:
            raise NetworkConnectivityError(
                f"Glue connection '{connection_name}' missing subnet configuration for JDBC setup",
                error_type='missing_subnet',
                connection_name=connection_name
            )
        
        if not security_groups:
            raise NetworkConnectivityError(
                f"Glue connection '{connection_name}' missing security group configuration for JDBC setup",
                error_type='missing_security_groups',
                connection_name=connection_name
            )
        
        self.structured_logger.debug(
            "Network configuration validation passed for JDBC setup",
            connection_name=connection_name,
            subnet_id=subnet_id,
            security_groups_count=len(security_groups)
        )


class JdbcConnectionManager:
    """Manages JDBC database connections with validation and error handling."""
    
    def __init__(self, spark_session: SparkSession, glue_context: GlueContext, 
                 retry_handler: Optional[ConnectionRetryHandler] = None):
        self.spark = spark_session
        self.glue_context = glue_context
        self.retry_handler = retry_handler or ConnectionRetryHandler()
        self.glue_connection_manager = GlueConnectionManager(glue_context)
        self._connection_cache = {}
        self.structured_logger = StructuredLogger("JdbcConnectionManager")
    
    def create_connection_properties(self, connection_config: ConnectionConfig) -> Dict[str, str]:
        """Create JDBC connection properties from connection configuration."""
        properties = {
            'user': connection_config.username,
            'password': connection_config.password,
            'driver': DatabaseEngineManager.get_driver_class(connection_config.engine_type)
        }
        
        # Add engine-specific connection properties
        engine_type = connection_config.engine_type.lower()
        
        if engine_type == 'oracle':
            properties.update({
                'oracle.jdbc.timezoneAsRegion': 'false',
                'oracle.net.CONNECT_TIMEOUT': '30000',
                'oracle.jdbc.ReadTimeout': '60000'
            })
        
        elif engine_type == 'sqlserver':
            properties.update({
                'loginTimeout': '30',
                'socketTimeout': '60000',
                'selectMethod': 'cursor'
            })
        
        elif engine_type == 'postgresql':
            properties.update({
                'connectTimeout': '30',
                'socketTimeout': '60',
                'tcpKeepAlive': 'true'
            })
        
        elif engine_type == 'db2':
            properties.update({
                'loginTimeout': '30',
                'blockingReadConnectionTimeout': '60000',
                'resultSetHoldability': '1'
            })
        
        return properties
    
    def create_connection_with_glue_support(self, connection_config: ConnectionConfig, 
                                          glue_connection_name: str = '') -> DataFrame:
        """Create JDBC connection with optional Glue connection support for cross-VPC access.
        
        Args:
            connection_config: Database connection configuration
            glue_connection_name: Name of Glue connection for cross-VPC access (empty for same-VPC)
            
        Returns:
            DataFrame reader configured with appropriate connection properties
        """
        try:
            # Setup JDBC properties using Glue connection if specified
            jdbc_properties = self.glue_connection_manager.setup_jdbc_with_connection(
                connection_config, glue_connection_name
            )
            
            # Create DataFrame reader with JDBC properties
            df_reader = self.spark.read.format('jdbc')
            
            # Configure connection properties
            for key, value in jdbc_properties.items():
                if not key.startswith('_'):  # Skip metadata keys
                    df_reader = df_reader.option(key, value)
            
            self.structured_logger.info(
                "Created JDBC connection with Glue support",
                engine_type=connection_config.engine_type,
                glue_connection_name=glue_connection_name or "same-vpc",
                has_glue_metadata='_glue_connection_metadata' in jdbc_properties
            )
            
            return df_reader
            
        except Exception as e:
            self.structured_logger.error(
                "Failed to create JDBC connection with Glue support",
                engine_type=connection_config.engine_type,
                glue_connection_name=glue_connection_name,
                error=str(e)
            )
            raise RuntimeError(f"Failed to create JDBC connection: {str(e)}")
    
    def validate_connection_with_network_check(self, connection_config: ConnectionConfig, 
                                             glue_connection_name: str = '',
                                             timeout_seconds: int = 30) -> bool:
        """Validate database connection with network connectivity check and enhanced error handling.
        
        Args:
            connection_config: Database connection configuration
            glue_connection_name: Name of Glue connection for cross-VPC access
            timeout_seconds: Connection timeout in seconds
            
        Returns:
            True if connection is valid, False otherwise
            
        Raises:
            NetworkConnectivityError: For network connectivity issues
            GlueConnectionError: For Glue connection specific issues
            ENICreationError: For ENI creation failures
        """
        try:
            # Use network configuration from connection config if not explicitly provided
            if not glue_connection_name and connection_config.requires_cross_vpc_connection():
                glue_connection_name = connection_config.get_glue_connection_name() or ''
            
            self.structured_logger.info(
                "Starting connection validation with network check",
                engine_type=connection_config.engine_type,
                glue_connection_name=glue_connection_name or "same-vpc",
                cross_vpc_required=connection_config.requires_cross_vpc_connection()
            )
            
            start_time = time.time()
            
            # First, validate network connectivity with enhanced error handling
            try:
                network_valid = self.glue_connection_manager.validate_network_connectivity(
                    glue_connection_name, connection_config.connection_string, timeout_seconds
                )
                if not network_valid:
                    raise NetworkConnectivityError(
                        f"Network connectivity validation failed for connection '{glue_connection_name or 'same-vpc'}'",
                        error_type='connectivity_validation_failed',
                        connection_name=glue_connection_name
                    )
            except (NetworkConnectivityError, GlueConnectionError, VpcEndpointError, ENICreationError):
                raise
            except Exception as network_error:
                raise NetworkConnectivityError(
                    f"Network connectivity validation error: {str(network_error)}",
                    error_type='validation_error',
                    connection_name=glue_connection_name
                )
            
            # Then validate actual database connection using Glue connection if specified
            try:
                if glue_connection_name:
                    # For cross-VPC connections, use the Glue connection for validation
                    connection_valid = self._validate_connection_with_glue(connection_config, glue_connection_name)
                else:
                    # For same-VPC connections, use standard validation
                    connection_valid = self.validate_connection(connection_config)
            except Exception as db_error:
                # Classify database connection errors
                if "connection refused" in str(db_error).lower():
                    raise NetworkConnectivityError(
                        f"Database connection refused: {str(db_error)}",
                        error_type='connection_refused',
                        connection_name=glue_connection_name
                    )
                elif "timeout" in str(db_error).lower():
                    raise NetworkConnectivityError(
                        f"Database connection timeout: {str(db_error)}",
                        error_type='connection_timeout',
                        connection_name=glue_connection_name
                    )
                else:
                    raise NetworkConnectivityError(
                        f"Database connection validation failed: {str(db_error)}",
                        error_type='database_connection_error',
                        connection_name=glue_connection_name
                    )
            
            duration = time.time() - start_time
            self.structured_logger.info(
                "Connection validation with network check completed successfully",
                engine_type=connection_config.engine_type,
                glue_connection_name=glue_connection_name or "same-vpc",
                cross_vpc_required=connection_config.requires_cross_vpc_connection(),
                validation_result="passed" if connection_valid else "failed",
                duration_seconds=round(duration, 2)
            )
            
            return connection_valid
            
        except (NetworkConnectivityError, GlueConnectionError, VpcEndpointError, ENICreationError):
            duration = time.time() - start_time
            self.structured_logger.error(
                "Connection validation with network check failed with network error",
                engine_type=connection_config.engine_type,
                glue_connection_name=glue_connection_name or "same-vpc",
                cross_vpc_required=connection_config.requires_cross_vpc_connection(),
                duration_seconds=round(duration, 2)
            )
            raise
        except Exception as e:
            duration = time.time() - start_time
            self.structured_logger.error(
                "Connection validation with network check failed with unexpected error",
                engine_type=connection_config.engine_type,
                glue_connection_name=glue_connection_name or "same-vpc",
                cross_vpc_required=connection_config.requires_cross_vpc_connection(),
                error=str(e),
                duration_seconds=round(duration, 2)
            )
            raise NetworkConnectivityError(
                f"Connection validation failed with unexpected error: {str(e)}",
                error_type='unexpected_error',
                connection_name=glue_connection_name
            )
    
    def _validate_connection_with_glue(self, connection_config: ConnectionConfig, glue_connection_name: str) -> bool:
        """Validate database connection using Glue connection for cross-VPC access with enhanced error handling."""
        try:
            # Setup JDBC properties using Glue connection with error handling
            try:
                jdbc_properties = self.glue_connection_manager.setup_jdbc_with_connection(
                    connection_config, glue_connection_name
                )
            except (GlueConnectionError, NetworkConnectivityError):
                raise
            except Exception as setup_error:
                raise NetworkConnectivityError(
                    f"Failed to setup JDBC properties for Glue connection '{glue_connection_name}': {str(setup_error)}",
                    error_type='jdbc_setup_error',
                    connection_name=glue_connection_name
                )
            
            # Test connection by executing a simple query with timeout and error handling
            try:
                test_df = self.spark.read \
                    .format("jdbc") \
                    .option("url", jdbc_properties['url']) \
                    .option("user", jdbc_properties['user']) \
                    .option("password", jdbc_properties['password']) \
                    .option("driver", jdbc_properties['driver']) \
                    .option("query", "SELECT 1 as test_column") \
                    .option("connectTimeout", "30") \
                    .option("socketTimeout", "60") \
                    .load()
                
                # Execute the query to test connectivity
                test_result = test_df.collect()
                
                if test_result and len(test_result) > 0:
                    self.structured_logger.info(
                        "Database connection validation successful using Glue connection",
                        glue_connection_name=glue_connection_name,
                        test_result_count=len(test_result)
                    )
                    return True
                else:
                    raise NetworkConnectivityError(
                        f"Database connection test returned no results for Glue connection '{glue_connection_name}'",
                        error_type='empty_test_result',
                        connection_name=glue_connection_name
                    )
                    
            except Exception as db_error:
                error_str = str(db_error).lower()
                
                # Classify database connection errors
                if "connection refused" in error_str or "connection reset" in error_str:
                    raise NetworkConnectivityError(
                        f"Database connection refused for Glue connection '{glue_connection_name}': {str(db_error)}",
                        error_type='connection_refused',
                        connection_name=glue_connection_name
                    )
                elif "timeout" in error_str or "timed out" in error_str:
                    raise NetworkConnectivityError(
                        f"Database connection timeout for Glue connection '{glue_connection_name}': {str(db_error)}",
                        error_type='connection_timeout',
                        connection_name=glue_connection_name
                    )
                elif "network" in error_str or "unreachable" in error_str:
                    raise NetworkConnectivityError(
                        f"Network unreachable for Glue connection '{glue_connection_name}': {str(db_error)}",
                        error_type='network_unreachable',
                        connection_name=glue_connection_name
                    )
                elif "eni" in error_str or "elastic network interface" in error_str:
                    raise ENICreationError(
                        f"ENI creation failed for Glue connection '{glue_connection_name}': {str(db_error)}",
                        error_code='eni_creation_failed'
                    )
                else:
                    raise NetworkConnectivityError(
                        f"Database connection validation failed for Glue connection '{glue_connection_name}': {str(db_error)}",
                        error_type='database_connection_error',
                        connection_name=glue_connection_name
                    )
        except (NetworkConnectivityError, GlueConnectionError, ENICreationError):
            raise
        except Exception as e:
            self.structured_logger.error(
                "Database connection validation failed using Glue connection with unexpected error",
                glue_connection_name=glue_connection_name,
                error=str(e)
            )
            raise NetworkConnectivityError(
                f"Unexpected error during database connection validation for Glue connection '{glue_connection_name}': {str(e)}",
                error_type='unexpected_validation_error',
                connection_name=glue_connection_name
            )
    
    def validate_connection(self, connection_config: ConnectionConfig) -> bool:
        """Validate database connection by executing a simple query."""
        def _validate():
            properties = self.create_connection_properties(connection_config)
            
            # Use a simple query appropriate for each database type
            test_queries = {
                'oracle': 'SELECT 1 FROM DUAL',
                'sqlserver': 'SELECT 1',
                'postgresql': 'SELECT 1',
                'db2': 'SELECT 1 FROM SYSIBM.SYSDUMMY1'
            }
            
            test_query = test_queries.get(connection_config.engine_type.lower(), 'SELECT 1')
            
            try:
                # Execute test query
                df = self.spark.read \
                    .format('jdbc') \
                    .option('url', connection_config.connection_string) \
                    .option('query', test_query) \
                    .options(**properties) \
                    .load()
                
                # Trigger execution by collecting one row
                result = df.collect()
                
                if result and len(result) > 0:
                    logger.info(f"Connection validation successful for {connection_config.engine_type}")
                    return True
                else:
                    raise RuntimeError("Test query returned no results")
                    
            except Exception as e:
                logger.error(f"Connection validation failed for {connection_config.engine_type}: {str(e)}")
                raise
        
        try:
            return self.retry_handler.execute_with_retry(
                _validate,
                f"connection validation for {connection_config.engine_type}"
            )
        except Exception as e:
            logger.error(f"Connection validation failed after retries: {str(e)}")
            return False
    
    def get_table_schema(self, connection_config: ConnectionConfig, table_name: str) -> StructType:
        """Get table schema from database."""
        def _get_schema():
            properties = self.create_connection_properties(connection_config)
            
            # Build full table name with schema
            full_table_name = f"{connection_config.schema}.{table_name}"
            
            try:
                # Read table schema by limiting to 0 rows
                df = self.spark.read \
                    .format('jdbc') \
                    .option('url', connection_config.connection_string) \
                    .option('dbtable', full_table_name) \
                    .options(**properties) \
                    .load() \
                    .limit(0)
                
                schema = df.schema
                logger.info(f"Retrieved schema for table {full_table_name}: {len(schema.fields)} columns")
                return schema
                
            except Exception as e:
                logger.error(f"Failed to get schema for table {full_table_name}: {str(e)}")
                raise
        
        return self.retry_handler.execute_with_retry(
            _get_schema,
            f"schema retrieval for {connection_config.schema}.{table_name}"
        )
    
    def read_table_data(self, connection_config: ConnectionConfig, table_name: str, 
                       query: Optional[str] = None, **options) -> DataFrame:
        """Read data from database table."""
        def _read_data():
            properties = self.create_connection_properties(connection_config)
            
            # Add any additional options
            properties.update(options)
            
            try:
                reader = self.spark.read \
                    .format('jdbc') \
                    .option('url', connection_config.connection_string) \
                    .options(**properties)
                
                if query:
                    # Use custom query
                    df = reader.option('query', query).load()
                else:
                    # Use table name
                    full_table_name = f"{connection_config.schema}.{table_name}"
                    df = reader.option('dbtable', full_table_name).load()
                
                logger.info(f"Successfully read data from {connection_config.engine_type} table: {table_name}")
                return df
                
            except Exception as e:
                logger.error(f"Failed to read data from table {table_name}: {str(e)}")
                raise
        
        return self.retry_handler.execute_with_retry(
            _read_data,
            f"data reading from {connection_config.schema}.{table_name}"
        )
    
    def write_table_data(self, df: DataFrame, connection_config: ConnectionConfig, 
                        table_name: str, mode: str = 'append', **options) -> None:
        """Write data to database table."""
        def _write_data():
            properties = self.create_connection_properties(connection_config)
            
            # Add any additional options
            properties.update(options)
            
            # Build full table name with schema
            full_table_name = f"{connection_config.schema}.{table_name}"
            
            try:
                df.write \
                    .format('jdbc') \
                    .option('url', connection_config.connection_string) \
                    .option('dbtable', full_table_name) \
                    .options(**properties) \
                    .mode(mode) \
                    .save()
                
                logger.info(f"Successfully wrote data to {connection_config.engine_type} table: {table_name}")
                
            except Exception as e:
                logger.error(f"Failed to write data to table {table_name}: {str(e)}")
                raise
        
        self.retry_handler.execute_with_retry(
            _write_data,
            f"data writing to {connection_config.schema}.{table_name}"
        )
    
    def test_connection(self, connection_config: ConnectionConfig) -> Dict[str, Any]:
        """Test database connection and return connection information."""
        connection_info = {
            'engine_type': connection_config.engine_type,
            'database': connection_config.database,
            'schema': connection_config.schema,
            'connection_valid': False,
            'error_message': None,
            'test_timestamp': time.time()
        }
        
        try:
            # Validate connection
            is_valid = self.validate_connection(connection_config)
            connection_info['connection_valid'] = is_valid
            
            if is_valid:
                logger.info(f"Connection test successful for {connection_config.engine_type}")
            else:
                connection_info['error_message'] = "Connection validation failed"
                
        except Exception as e:
            connection_info['connection_valid'] = False
            connection_info['error_message'] = str(e)
            logger.error(f"Connection test failed for {connection_config.engine_type}: {str(e)}")
        
        return connection_info
    
    def get_connection_cache_key(self, connection_config: ConnectionConfig) -> str:
        """Generate cache key for connection configuration."""
        return f"{connection_config.engine_type}_{connection_config.database}_{connection_config.schema}_{connection_config.username}"
    
    def cache_connection_info(self, connection_config: ConnectionConfig, info: Dict[str, Any]) -> None:
        """Cache connection information for reuse."""
        cache_key = self.get_connection_cache_key(connection_config)
        self._connection_cache[cache_key] = info
        logger.debug(f"Cached connection info for {cache_key}")
    
    def get_cached_connection_info(self, connection_config: ConnectionConfig) -> Optional[Dict[str, Any]]:
        """Get cached connection information."""
        cache_key = self.get_connection_cache_key(connection_config)
        return self._connection_cache.get(cache_key)


class ErrorRecoveryManager:
    """Manages error recovery strategies and graceful failure handling."""
    
    def __init__(self, job_name: str, enable_detailed_logging: bool = True):
        self.job_name = job_name
        self.enable_detailed_logging = enable_detailed_logging
        self.error_history = []
        self.recovery_attempts = {}
        self.critical_errors = []
        
    def handle_database_connection_error(self, error: Exception, connection_config: ConnectionConfig, 
                                       operation_context: str) -> Dict[str, Any]:
        """Handle database connection errors with appropriate recovery strategies."""
        error_info = {
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'error_type': type(error).__name__,
            'error_message': str(error),
            'error_category': ErrorClassifier.classify_error(error),
            'operation_context': operation_context,
            'connection_details': {
                'engine_type': connection_config.engine_type,
                'database': connection_config.database,
                'schema': connection_config.schema,
                'connection_string': self._sanitize_connection_string(connection_config.connection_string)
            },
            'recovery_strategy': ErrorClassifier.get_recovery_strategy(error),
            'is_retryable': ErrorClassifier.is_retryable_error(error)
        }
        
        # Log detailed error information
        if self.enable_detailed_logging:
            logger.error(
                f"Database connection error in {operation_context}: {error_info['error_message']} "
                f"[Category: {error_info['error_category']}, Engine: {connection_config.engine_type}]"
            )
            
            # Log connection details (sanitized)
            logger.debug(f"Connection details: {error_info['connection_details']}")
        
        # Store error in history
        self.error_history.append(error_info)
        
        # Determine if this is a critical error that should stop the job
        if self._is_critical_error(error_info):
            self.critical_errors.append(error_info)
            logger.critical(
                f"Critical database error detected: {error_info['error_message']}. "
                f"Job may need to be terminated."
            )
        
        return error_info
    
    def handle_data_processing_error(self, error: Exception, table_name: str, 
                                   operation_type: str, context_data: Dict[str, Any] = None) -> Dict[str, Any]:
        """Handle data processing errors with recovery options."""
        error_info = {
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'error_type': type(error).__name__,
            'error_message': str(error),
            'error_category': ErrorClassifier.classify_error(error),
            'table_name': table_name,
            'operation_type': operation_type,
            'context_data': context_data or {},
            'recovery_strategy': ErrorClassifier.get_recovery_strategy(error),
            'is_retryable': ErrorClassifier.is_retryable_error(error)
        }
        
        # Log error with context
        logger.error(
            f"Data processing error for table {table_name} during {operation_type}: "
            f"{error_info['error_message']} [Category: {error_info['error_category']}]"
        )
        
        if context_data:
            logger.debug(f"Error context: {context_data}")
        
        # Store error in history
        self.error_history.append(error_info)
        
        # Handle specific data processing error types
        if error_info['error_category'] == ErrorCategory.SCHEMA_MISMATCH:
            logger.error(
                f"Schema mismatch detected for table {table_name}. "
                f"Please verify that target schema matches source schema requirements."
            )
        elif error_info['error_category'] == ErrorCategory.DATA_PROCESSING:
            logger.warning(
                f"Data processing issue for table {table_name}. "
                f"Attempting to continue with remaining data."
            )
        
        return error_info
    
    def handle_infrastructure_error(self, error: Exception, component: str, 
                                  operation_context: str) -> Dict[str, Any]:
        """Handle infrastructure-related errors (S3, IAM, Glue, etc.)."""
        error_info = {
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'error_type': type(error).__name__,
            'error_message': str(error),
            'error_category': ErrorClassifier.classify_error(error),
            'component': component,
            'operation_context': operation_context,
            'recovery_strategy': ErrorClassifier.get_recovery_strategy(error),
            'is_retryable': ErrorClassifier.is_retryable_error(error)
        }
        
        # Log infrastructure error
        logger.error(
            f"Infrastructure error in {component} during {operation_context}: "
            f"{error_info['error_message']} [Category: {error_info['error_category']}]"
        )
        
        # Store error in history
        self.error_history.append(error_info)
        
        # Handle specific infrastructure errors
        if 'S3' in component.upper() or 's3://' in str(error).lower():
            logger.error(
                f"S3 access error detected. Please verify:"
                f"\n- S3 bucket permissions and IAM role access"
                f"\n- JDBC driver file paths and availability"
                f"\n- Network connectivity to S3"
            )
        elif 'IAM' in component.upper() or 'permission' in str(error).lower():
            logger.error(
                f"IAM permission error detected. Please verify:"
                f"\n- Glue job execution role permissions"
                f"\n- Database access permissions"
                f"\n- S3 bucket access permissions"
            )
        
        return error_info
    
    def attempt_graceful_recovery(self, error_info: Dict[str, Any], 
                                recovery_context: Dict[str, Any] = None) -> bool:
        """Attempt graceful recovery based on error type and context."""
        recovery_key = f"{error_info['error_category']}_{error_info.get('table_name', 'global')}"
        
        # Track recovery attempts
        if recovery_key not in self.recovery_attempts:
            self.recovery_attempts[recovery_key] = 0
        
        self.recovery_attempts[recovery_key] += 1
        max_recovery_attempts = 3
        
        if self.recovery_attempts[recovery_key] > max_recovery_attempts:
            logger.error(
                f"Maximum recovery attempts ({max_recovery_attempts}) exceeded for {recovery_key}. "
                f"Giving up on recovery."
            )
            return False
        
        logger.info(
            f"Attempting graceful recovery for {recovery_key} "
            f"(attempt {self.recovery_attempts[recovery_key]}/{max_recovery_attempts})"
        )
        
        try:
            # Implement recovery strategies based on error category
            if error_info['error_category'] == ErrorCategory.CONNECTION:
                return self._recover_connection_error(error_info, recovery_context)
            elif error_info['error_category'] == ErrorCategory.DATA_PROCESSING:
                return self._recover_data_processing_error(error_info, recovery_context)
            elif error_info['error_category'] == ErrorCategory.RESOURCE:
                return self._recover_resource_error(error_info, recovery_context)
            elif error_info['error_category'] == ErrorCategory.TIMEOUT:
                return self._recover_timeout_error(error_info, recovery_context)
            else:
                logger.warning(f"No specific recovery strategy for category: {error_info['error_category']}")
                return False
                
        except Exception as recovery_error:
            logger.error(f"Recovery attempt failed: {str(recovery_error)}")
            return False
    
    def _recover_connection_error(self, error_info: Dict[str, Any], 
                                recovery_context: Dict[str, Any] = None) -> bool:
        """Attempt to recover from connection errors."""
        logger.info("Attempting connection error recovery...")
        
        # Wait before retry to allow transient issues to resolve
        time.sleep(5)
        
        # Could implement connection pool reset, alternative connection strings, etc.
        logger.info("Connection error recovery completed")
        return True
    
    def _recover_data_processing_error(self, error_info: Dict[str, Any], 
                                     recovery_context: Dict[str, Any] = None) -> bool:
        """Attempt to recover from data processing errors."""
        logger.info("Attempting data processing error recovery...")
        
        # Could implement data validation, schema refresh, etc.
        logger.info("Data processing error recovery completed")
        return True
    
    def _recover_resource_error(self, error_info: Dict[str, Any], 
                              recovery_context: Dict[str, Any] = None) -> bool:
        """Attempt to recover from resource exhaustion errors."""
        logger.info("Attempting resource error recovery...")
        
        # Wait longer for resource exhaustion
        time.sleep(30)
        
        # Could implement memory cleanup, connection pool management, etc.
        logger.info("Resource error recovery completed")
        return True
    
    def _recover_timeout_error(self, error_info: Dict[str, Any], 
                             recovery_context: Dict[str, Any] = None) -> bool:
        """Attempt to recover from timeout errors."""
        logger.info("Attempting timeout error recovery...")
        
        # Wait before retry
        time.sleep(10)
        
        # Could implement query optimization, batch size reduction, etc.
        logger.info("Timeout error recovery completed")
        return True
    
    def _is_critical_error(self, error_info: Dict[str, Any]) -> bool:
        """Determine if an error is critical and should stop the job."""
        critical_categories = {
            ErrorCategory.AUTHENTICATION,
            ErrorCategory.PERMISSION,
            ErrorCategory.SCHEMA_MISMATCH
        }
        
        return error_info['error_category'] in critical_categories
    
    def _sanitize_connection_string(self, connection_string: str) -> str:
        """Remove sensitive information from connection string for logging."""
        import re
        
        # Remove password from connection string
        sanitized = re.sub(r'password=[^;]+', 'password=***', connection_string, flags=re.IGNORECASE)
        sanitized = re.sub(r'pwd=[^;]+', 'pwd=***', sanitized, flags=re.IGNORECASE)
        
        return sanitized
    
    def get_error_summary(self) -> Dict[str, Any]:
        """Get summary of all errors encountered during job execution."""
        if not self.error_history:
            return {'total_errors': 0, 'error_categories': {}, 'critical_errors': 0}
        
        # Count errors by category
        error_categories = {}
        for error in self.error_history:
            category = error['error_category']
            error_categories[category] = error_categories.get(category, 0) + 1
        
        return {
            'total_errors': len(self.error_history),
            'error_categories': error_categories,
            'critical_errors': len(self.critical_errors),
            'recovery_attempts': dict(self.recovery_attempts),
            'latest_errors': self.error_history[-5:] if len(self.error_history) > 5 else self.error_history
        }
    
    def log_final_error_report(self) -> None:
        """Log final error report at job completion."""
        summary = self.get_error_summary()
        
        if summary['total_errors'] == 0:
            logger.info(f"Job {self.job_name} completed without errors")
            return
        
        logger.info(
            f"Job {self.job_name} error summary: "
            f"{summary['total_errors']} total errors, "
            f"{summary['critical_errors']} critical errors"
        )
        
        # Log error breakdown by category
        for category, count in summary['error_categories'].items():
            logger.info(f"  {category}: {count} errors")
        
        # Log recovery attempts
        if summary['recovery_attempts']:
            logger.info("Recovery attempts:")
            for recovery_key, attempts in summary['recovery_attempts'].items():
                logger.info(f"  {recovery_key}: {attempts} attempts")
        
        # Log critical errors if any
        if summary['critical_errors'] > 0:
            logger.error(f"Critical errors detected ({summary['critical_errors']}):")
            for error in self.critical_errors:
                logger.error(
                    f"  {error['timestamp']}: {error['error_message']} "
                    f"[{error['error_category']}]"
                )


@dataclass
class FullLoadProgress:
    """Tracks progress of full-load operations."""
    table_name: str
    total_rows: int = 0
    processed_rows: int = 0
    start_time: float = 0.0
    end_time: Optional[float] = None
    status: str = 'pending'  # pending, in_progress, completed, failed
    error_message: Optional[str] = None
    
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
    def rows_per_second(self) -> float:
        """Calculate processing rate in rows per second."""
        duration = self.duration_seconds
        if duration == 0.0:
            return 0.0
        return self.processed_rows / duration


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


@dataclass
class JobBookmarkState:
    """Represents job bookmark state for a table."""
    table_name: str
    incremental_strategy: str
    incremental_column: Optional[str] = None
    last_processed_value: Optional[Any] = None
    last_update_timestamp: Optional[datetime] = None
    row_hash_checkpoint: Optional[str] = None
    is_first_run: bool = True
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert bookmark state to dictionary for storage."""
        return {
            'table_name': self.table_name,
            'incremental_strategy': self.incremental_strategy,
            'incremental_column': self.incremental_column,
            'last_processed_value': str(self.last_processed_value) if self.last_processed_value is not None else None,
            'last_update_timestamp': self.last_update_timestamp.isoformat() if self.last_update_timestamp else None,
            'row_hash_checkpoint': self.row_hash_checkpoint,
            'is_first_run': self.is_first_run
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'JobBookmarkState':
        """Create bookmark state from dictionary."""
        last_update_timestamp = None
        if data.get('last_update_timestamp'):
            last_update_timestamp = datetime.fromisoformat(data['last_update_timestamp'])
        
        return cls(
            table_name=data['table_name'],
            incremental_strategy=data['incremental_strategy'],
            incremental_column=data.get('incremental_column'),
            last_processed_value=data.get('last_processed_value'),
            last_update_timestamp=last_update_timestamp,
            row_hash_checkpoint=data.get('row_hash_checkpoint'),
            is_first_run=data.get('is_first_run', True)
        )


class DataTypeMapper:
    """Handles data type mapping between different database engines."""
    
    # Comprehensive data type mappings for cross-database compatibility
    TYPE_MAPPINGS = {
        'oracle_to_postgresql': {
            'NUMBER': 'NUMERIC',
            'VARCHAR2': 'VARCHAR',
            'NVARCHAR2': 'VARCHAR',
            'CHAR': 'CHAR',
            'NCHAR': 'CHAR',
            'DATE': 'TIMESTAMP',
            'TIMESTAMP': 'TIMESTAMP',
            'CLOB': 'TEXT',
            'NCLOB': 'TEXT',
            'BLOB': 'BYTEA',
            'RAW': 'BYTEA',
            'LONG': 'TEXT',
            'LONG RAW': 'BYTEA',
            'BINARY_FLOAT': 'REAL',
            'BINARY_DOUBLE': 'DOUBLE PRECISION'
        },
        'oracle_to_sqlserver': {
            'NUMBER': 'NUMERIC',
            'VARCHAR2': 'NVARCHAR',
            'NVARCHAR2': 'NVARCHAR',
            'CHAR': 'CHAR',
            'NCHAR': 'NCHAR',
            'DATE': 'DATETIME2',
            'TIMESTAMP': 'DATETIME2',
            'CLOB': 'NTEXT',
            'NCLOB': 'NTEXT',
            'BLOB': 'VARBINARY',
            'RAW': 'VARBINARY',
            'LONG': 'NTEXT',
            'LONG RAW': 'VARBINARY',
            'BINARY_FLOAT': 'REAL',
            'BINARY_DOUBLE': 'FLOAT'
        },
        'oracle_to_db2': {
            'NUMBER': 'DECIMAL',
            'VARCHAR2': 'VARCHAR',
            'NVARCHAR2': 'VARCHAR',
            'CHAR': 'CHAR',
            'NCHAR': 'CHAR',
            'DATE': 'TIMESTAMP',
            'TIMESTAMP': 'TIMESTAMP',
            'CLOB': 'CLOB',
            'NCLOB': 'CLOB',
            'BLOB': 'BLOB',
            'RAW': 'VARBINARY',
            'LONG': 'CLOB',
            'LONG RAW': 'BLOB',
            'BINARY_FLOAT': 'REAL',
            'BINARY_DOUBLE': 'DOUBLE'
        },
        'sqlserver_to_postgresql': {
            'INT': 'INTEGER',
            'BIGINT': 'BIGINT',
            'SMALLINT': 'SMALLINT',
            'TINYINT': 'SMALLINT',
            'DECIMAL': 'NUMERIC',
            'NUMERIC': 'NUMERIC',
            'FLOAT': 'DOUBLE PRECISION',
            'REAL': 'REAL',
            'VARCHAR': 'VARCHAR',
            'NVARCHAR': 'VARCHAR',
            'CHAR': 'CHAR',
            'NCHAR': 'CHAR',
            'TEXT': 'TEXT',
            'NTEXT': 'TEXT',
            'DATETIME': 'TIMESTAMP',
            'DATETIME2': 'TIMESTAMP',
            'DATE': 'DATE',
            'TIME': 'TIME',
            'BIT': 'BOOLEAN',
            'VARBINARY': 'BYTEA',
            'IMAGE': 'BYTEA',
            'MONEY': 'NUMERIC',
            'SMALLMONEY': 'NUMERIC',
            'UNIQUEIDENTIFIER': 'UUID'
        },
        'sqlserver_to_oracle': {
            'INT': 'NUMBER',
            'BIGINT': 'NUMBER',
            'SMALLINT': 'NUMBER',
            'TINYINT': 'NUMBER',
            'DECIMAL': 'NUMBER',
            'NUMERIC': 'NUMBER',
            'FLOAT': 'BINARY_DOUBLE',
            'REAL': 'BINARY_FLOAT',
            'VARCHAR': 'VARCHAR2',
            'NVARCHAR': 'NVARCHAR2',
            'CHAR': 'CHAR',
            'NCHAR': 'NCHAR',
            'TEXT': 'CLOB',
            'NTEXT': 'NCLOB',
            'DATETIME': 'TIMESTAMP',
            'DATETIME2': 'TIMESTAMP',
            'DATE': 'DATE',
            'TIME': 'TIMESTAMP',
            'BIT': 'NUMBER',
            'VARBINARY': 'RAW',
            'IMAGE': 'BLOB',
            'MONEY': 'NUMBER',
            'SMALLMONEY': 'NUMBER',
            'UNIQUEIDENTIFIER': 'RAW'
        },
        'sqlserver_to_db2': {
            'INT': 'INTEGER',
            'BIGINT': 'BIGINT',
            'SMALLINT': 'SMALLINT',
            'TINYINT': 'SMALLINT',
            'DECIMAL': 'DECIMAL',
            'NUMERIC': 'NUMERIC',
            'FLOAT': 'DOUBLE',
            'REAL': 'REAL',
            'VARCHAR': 'VARCHAR',
            'NVARCHAR': 'VARCHAR',
            'CHAR': 'CHAR',
            'NCHAR': 'CHAR',
            'TEXT': 'CLOB',
            'NTEXT': 'CLOB',
            'DATETIME': 'TIMESTAMP',
            'DATETIME2': 'TIMESTAMP',
            'DATE': 'DATE',
            'TIME': 'TIME',
            'BIT': 'SMALLINT',
            'VARBINARY': 'VARBINARY',
            'IMAGE': 'BLOB',
            'MONEY': 'DECIMAL',
            'SMALLMONEY': 'DECIMAL',
            'UNIQUEIDENTIFIER': 'CHAR'
        },
        'postgresql_to_oracle': {
            'INTEGER': 'NUMBER',
            'BIGINT': 'NUMBER',
            'SMALLINT': 'NUMBER',
            'NUMERIC': 'NUMBER',
            'DECIMAL': 'NUMBER',
            'REAL': 'BINARY_FLOAT',
            'DOUBLE PRECISION': 'BINARY_DOUBLE',
            'VARCHAR': 'VARCHAR2',
            'CHAR': 'CHAR',
            'TEXT': 'CLOB',
            'DATE': 'DATE',
            'TIME': 'TIMESTAMP',
            'TIMESTAMP': 'TIMESTAMP',
            'BOOLEAN': 'NUMBER',
            'BYTEA': 'BLOB',
            'UUID': 'RAW',
            'JSON': 'CLOB',
            'JSONB': 'CLOB'
        },
        'postgresql_to_sqlserver': {
            'INTEGER': 'INT',
            'BIGINT': 'BIGINT',
            'SMALLINT': 'SMALLINT',
            'NUMERIC': 'NUMERIC',
            'DECIMAL': 'DECIMAL',
            'REAL': 'REAL',
            'DOUBLE PRECISION': 'FLOAT',
            'VARCHAR': 'NVARCHAR',
            'CHAR': 'CHAR',
            'TEXT': 'NTEXT',
            'DATE': 'DATE',
            'TIME': 'TIME',
            'TIMESTAMP': 'DATETIME2',
            'BOOLEAN': 'BIT',
            'BYTEA': 'VARBINARY',
            'UUID': 'UNIQUEIDENTIFIER',
            'JSON': 'NTEXT',
            'JSONB': 'NTEXT'
        },
        'postgresql_to_db2': {
            'INTEGER': 'INTEGER',
            'BIGINT': 'BIGINT',
            'SMALLINT': 'SMALLINT',
            'NUMERIC': 'DECIMAL',
            'DECIMAL': 'DECIMAL',
            'REAL': 'REAL',
            'DOUBLE PRECISION': 'DOUBLE',
            'VARCHAR': 'VARCHAR',
            'CHAR': 'CHAR',
            'TEXT': 'CLOB',
            'DATE': 'DATE',
            'TIME': 'TIME',
            'TIMESTAMP': 'TIMESTAMP',
            'BOOLEAN': 'SMALLINT',
            'BYTEA': 'BLOB',
            'UUID': 'CHAR',
            'JSON': 'CLOB',
            'JSONB': 'CLOB'
        },
        'db2_to_postgresql': {
            'INTEGER': 'INTEGER',
            'BIGINT': 'BIGINT',
            'SMALLINT': 'SMALLINT',
            'DECIMAL': 'NUMERIC',
            'NUMERIC': 'NUMERIC',
            'REAL': 'REAL',
            'DOUBLE': 'DOUBLE PRECISION',
            'VARCHAR': 'VARCHAR',
            'CHAR': 'CHAR',
            'CLOB': 'TEXT',
            'DATE': 'DATE',
            'TIME': 'TIME',
            'TIMESTAMP': 'TIMESTAMP',
            'BLOB': 'BYTEA',
            'VARBINARY': 'BYTEA',
            'GRAPHIC': 'VARCHAR',
            'VARGRAPHIC': 'VARCHAR'
        },
        'db2_to_oracle': {
            'INTEGER': 'NUMBER',
            'BIGINT': 'NUMBER',
            'SMALLINT': 'NUMBER',
            'DECIMAL': 'NUMBER',
            'NUMERIC': 'NUMBER',
            'REAL': 'BINARY_FLOAT',
            'DOUBLE': 'BINARY_DOUBLE',
            'VARCHAR': 'VARCHAR2',
            'CHAR': 'CHAR',
            'CLOB': 'CLOB',
            'DATE': 'DATE',
            'TIME': 'TIMESTAMP',
            'TIMESTAMP': 'TIMESTAMP',
            'BLOB': 'BLOB',
            'VARBINARY': 'RAW',
            'GRAPHIC': 'NVARCHAR2',
            'VARGRAPHIC': 'NVARCHAR2'
        },
        'db2_to_sqlserver': {
            'INTEGER': 'INT',
            'BIGINT': 'BIGINT',
            'SMALLINT': 'SMALLINT',
            'DECIMAL': 'DECIMAL',
            'NUMERIC': 'NUMERIC',
            'REAL': 'REAL',
            'DOUBLE': 'FLOAT',
            'VARCHAR': 'NVARCHAR',
            'CHAR': 'CHAR',
            'CLOB': 'NTEXT',
            'DATE': 'DATE',
            'TIME': 'TIME',
            'TIMESTAMP': 'DATETIME2',
            'BLOB': 'VARBINARY',
            'VARBINARY': 'VARBINARY',
            'GRAPHIC': 'NVARCHAR',
            'VARGRAPHIC': 'NVARCHAR'
        }
    }
    
    # Data transformation functions for specific type conversions
    TRANSFORMATION_FUNCTIONS = {
        'oracle_to_postgresql': {
            'DATE': lambda col: col.cast('timestamp'),
            'NUMBER': lambda col: col.cast('decimal(38,10)'),
            'CLOB': lambda col: col.cast('string')
        },
        'sqlserver_to_postgresql': {
            'BIT': lambda col: col.cast('boolean'),
            'DATETIME': lambda col: col.cast('timestamp'),
            'UNIQUEIDENTIFIER': lambda col: col.cast('string')
        },
        'postgresql_to_oracle': {
            'BOOLEAN': lambda col: col.cast('int'),
            'UUID': lambda col: col.cast('string'),
            'JSON': lambda col: col.cast('string'),
            'JSONB': lambda col: col.cast('string')
        },
        'postgresql_to_sqlserver': {
            'UUID': lambda col: col.cast('string'),
            'JSON': lambda col: col.cast('string'),
            'JSONB': lambda col: col.cast('string')
        }
    }
    
    @classmethod
    def get_mapping_key(cls, source_engine: str, target_engine: str) -> str:
        """Generate mapping key for source-target engine combination."""
        return f"{source_engine.lower()}_to_{target_engine.lower()}"
    
    @classmethod
    def map_data_type(cls, source_engine: str, target_engine: str, source_type: str) -> str:
        """Map data type from source engine to target engine."""
        # If same engine, no mapping needed
        if source_engine.lower() == target_engine.lower():
            return source_type
        
        mapping_key = cls.get_mapping_key(source_engine, target_engine)
        type_mapping = cls.TYPE_MAPPINGS.get(mapping_key, {})
        
        # Return mapped type or original if no mapping exists
        mapped_type = type_mapping.get(source_type.upper(), source_type)
        
        if mapped_type == source_type and mapping_key in cls.TYPE_MAPPINGS:
            logger.warning(
                f"No type mapping found for {source_type} in {source_engine} -> {target_engine} conversion. "
                f"Using original type: {source_type}"
            )
        
        return mapped_type
    
    @classmethod
    def is_cross_database_replication(cls, source_engine: str, target_engine: str) -> bool:
        """Check if this is a cross-database replication scenario."""
        return source_engine.lower() != target_engine.lower()
    
    @classmethod
    def get_supported_mappings(cls) -> List[str]:
        """Get list of all supported database engine mappings."""
        return list(cls.TYPE_MAPPINGS.keys())
    
    @classmethod
    def is_mapping_supported(cls, source_engine: str, target_engine: str) -> bool:
        """Check if mapping between source and target engines is supported."""
        mapping_key = cls.get_mapping_key(source_engine, target_engine)
        return mapping_key in cls.TYPE_MAPPINGS
    
    @classmethod
    def transform_dataframe_types(cls, df: DataFrame, source_engine: str, target_engine: str) -> DataFrame:
        """Transform DataFrame column types for cross-database compatibility."""
        if not cls.is_cross_database_replication(source_engine, target_engine):
            return df
        
        mapping_key = cls.get_mapping_key(source_engine, target_engine)
        transformation_funcs = cls.TRANSFORMATION_FUNCTIONS.get(mapping_key, {})
        
        if not transformation_funcs:
            logger.info(f"No specific transformations needed for {mapping_key}")
            return df
        
        transformed_df = df
        
        for field in df.schema.fields:
            field_type = str(field.dataType).upper()
            
            # Check if this field type needs transformation
            for source_type, transform_func in transformation_funcs.items():
                if source_type in field_type:
                    logger.info(f"Transforming column {field.name} from {field_type} using {source_type} transformation")
                    transformed_df = transformed_df.withColumn(field.name, transform_func(col(field.name)))
                    break
        
        return transformed_df


class SchemaCompatibilityValidator:
    """Validates schema compatibility between source and target databases for cross-database replication."""
    
    def __init__(self, data_type_mapper: DataTypeMapper):
        self.data_type_mapper = data_type_mapper
    
    def validate_schema_compatibility(self, source_schema: StructType, target_schema: StructType,
                                    source_engine: str, target_engine: str, table_name: str) -> Dict[str, Any]:
        """Validate that source and target schemas are compatible for replication."""
        validation_result = {
            'is_compatible': True,
            'warnings': [],
            'errors': [],
            'column_mappings': {},
            'missing_columns': [],
            'extra_columns': [],
            'type_mismatches': []
        }
        
        try:
            logger.info(f"Validating schema compatibility for table {table_name}: {source_engine} -> {target_engine}")
            
            # Create column dictionaries for easier comparison
            source_columns = {field.name.lower(): field for field in source_schema.fields}
            target_columns = {field.name.lower(): field for field in target_schema.fields}
            
            # Check for missing columns in target
            for col_name, source_field in source_columns.items():
                if col_name not in target_columns:
                    validation_result['missing_columns'].append(col_name)
                    validation_result['errors'].append(
                        f"Column '{col_name}' exists in source but missing in target schema"
                    )
                    validation_result['is_compatible'] = False
            
            # Check for extra columns in target (warnings only)
            for col_name in target_columns:
                if col_name not in source_columns:
                    validation_result['extra_columns'].append(col_name)
                    validation_result['warnings'].append(
                        f"Column '{col_name}' exists in target but not in source schema"
                    )
            
            # Validate data type compatibility for matching columns
            for col_name, source_field in source_columns.items():
                if col_name in target_columns:
                    target_field = target_columns[col_name]
                    
                    # Get string representations of data types
                    source_type_str = self._get_type_string(source_field.dataType)
                    target_type_str = self._get_type_string(target_field.dataType)
                    
                    # Map source type to expected target type
                    expected_target_type = self.data_type_mapper.map_data_type(
                        source_engine, target_engine, source_type_str
                    )
                    
                    # Check if target type is compatible
                    if not self._are_types_compatible(target_type_str, expected_target_type):
                        mismatch_info = {
                            'column': col_name,
                            'source_type': source_type_str,
                            'target_type': target_type_str,
                            'expected_type': expected_target_type
                        }
                        validation_result['type_mismatches'].append(mismatch_info)
                        validation_result['errors'].append(
                            f"Type mismatch for column '{col_name}': source={source_type_str}, "
                            f"target={target_type_str}, expected={expected_target_type}"
                        )
                        validation_result['is_compatible'] = False
                    
                    # Store column mapping
                    validation_result['column_mappings'][col_name] = {
                        'source_type': source_type_str,
                        'target_type': target_type_str,
                        'mapped_type': expected_target_type
                    }
            
            # Log validation results
            if validation_result['is_compatible']:
                logger.info(f"Schema validation passed for table {table_name}")
                if validation_result['warnings']:
                    logger.warning(f"Schema validation warnings for table {table_name}: {len(validation_result['warnings'])} warnings")
            else:
                logger.error(f"Schema validation failed for table {table_name}: {len(validation_result['errors'])} errors")
            
            return validation_result
            
        except Exception as e:
            logger.error(f"Schema validation failed with exception for table {table_name}: {str(e)}")
            validation_result['is_compatible'] = False
            validation_result['errors'].append(f"Validation exception: {str(e)}")
            return validation_result
    
    def _get_type_string(self, data_type) -> str:
        """Convert Spark DataType to string representation."""
        type_str = str(data_type)
        
        # Normalize common type representations
        type_mappings = {
            'StringType': 'VARCHAR',
            'IntegerType': 'INTEGER',
            'LongType': 'BIGINT',
            'DoubleType': 'DOUBLE',
            'FloatType': 'FLOAT',
            'BooleanType': 'BOOLEAN',
            'TimestampType': 'TIMESTAMP',
            'DateType': 'DATE',
            'BinaryType': 'BINARY',
            'DecimalType': 'DECIMAL'
        }
        
        for spark_type, db_type in type_mappings.items():
            if spark_type in type_str:
                return db_type
        
        return type_str.upper()
    
    def _are_types_compatible(self, actual_type: str, expected_type: str) -> bool:
        """Check if actual and expected types are compatible."""
        # Normalize types for comparison
        actual = actual_type.upper().strip()
        expected = expected_type.upper().strip()
        
        # Exact match
        if actual == expected:
            return True
        
        # Define compatible type groups
        compatible_groups = [
            {'VARCHAR', 'NVARCHAR', 'TEXT', 'STRING', 'CHAR', 'NCHAR'},
            {'INTEGER', 'INT', 'SMALLINT', 'BIGINT', 'NUMBER'},
            {'DECIMAL', 'NUMERIC', 'NUMBER'},
            {'FLOAT', 'REAL', 'DOUBLE', 'DOUBLE PRECISION'},
            {'TIMESTAMP', 'DATETIME', 'DATETIME2'},
            {'BYTEA', 'VARBINARY', 'BINARY', 'BLOB', 'RAW'},
            {'BOOLEAN', 'BIT'},
            {'DATE'},
            {'TIME'},
            {'UUID', 'UNIQUEIDENTIFIER'}
        ]
        
        # Check if both types are in the same compatibility group
        for group in compatible_groups:
            if actual in group and expected in group:
                return True
        
        # Special cases for partial matches
        if 'VARCHAR' in actual and 'VARCHAR' in expected:
            return True
        if 'DECIMAL' in actual and 'DECIMAL' in expected:
            return True
        if 'NUMERIC' in actual and 'NUMERIC' in expected:
            return True
        
        return False
    
    def generate_compatibility_report(self, validation_result: Dict[str, Any], table_name: str) -> str:
        """Generate a human-readable compatibility report."""
        report_lines = [
            f"Schema Compatibility Report for Table: {table_name}",
            "=" * 60,
            f"Overall Status: {'COMPATIBLE' if validation_result['is_compatible'] else 'INCOMPATIBLE'}",
            ""
        ]
        
        if validation_result['errors']:
            report_lines.extend([
                "ERRORS:",
                "-" * 20
            ])
            for error in validation_result['errors']:
                report_lines.append(f"  • {error}")
            report_lines.append("")
        
        if validation_result['warnings']:
            report_lines.extend([
                "WARNINGS:",
                "-" * 20
            ])
            for warning in validation_result['warnings']:
                report_lines.append(f"  • {warning}")
            report_lines.append("")
        
        if validation_result['column_mappings']:
            report_lines.extend([
                "COLUMN MAPPINGS:",
                "-" * 20
            ])
            for col_name, mapping in validation_result['column_mappings'].items():
                report_lines.append(
                    f"  {col_name}: {mapping['source_type']} -> {mapping['target_type']} "
                    f"(mapped: {mapping['mapped_type']})"
                )
            report_lines.append("")
        
        if validation_result['type_mismatches']:
            report_lines.extend([
                "TYPE MISMATCHES:",
                "-" * 20
            ])
            for mismatch in validation_result['type_mismatches']:
                report_lines.append(
                    f"  {mismatch['column']}: Expected {mismatch['expected_type']}, "
                    f"Found {mismatch['target_type']}"
                )
            report_lines.append("")
        
        return "\n".join(report_lines)
    
    def validate_cross_database_assumptions(self, source_config: ConnectionConfig, 
                                          target_config: ConnectionConfig,
                                          table_names: List[str]) -> Dict[str, Any]:
        """Validate assumptions for cross-database replication scenarios."""
        validation_summary = {
            'overall_compatible': True,
            'table_results': {},
            'unsupported_mappings': [],
            'recommendations': []
        }
        
        # Check if the engine mapping is supported
        if not self.data_type_mapper.is_mapping_supported(source_config.engine_type, target_config.engine_type):
            mapping_key = self.data_type_mapper.get_mapping_key(source_config.engine_type, target_config.engine_type)
            validation_summary['unsupported_mappings'].append(mapping_key)
            validation_summary['overall_compatible'] = False
            validation_summary['recommendations'].append(
                f"Mapping {source_config.engine_type} -> {target_config.engine_type} is not fully supported. "
                "Manual schema verification is recommended."
            )
        
        # Add general recommendations for cross-database replication
        validation_summary['recommendations'].extend([
            "Ensure target database schema and tables are created before running replication",
            "Verify that target table indexes and constraints are properly configured",
            "Test with a small subset of data before full migration",
            "Monitor data type conversions for potential data loss"
        ])
        
        if self.data_type_mapper.is_cross_database_replication(source_config.engine_type, target_config.engine_type):
            validation_summary['recommendations'].append(
                "Cross-database replication detected. Schema migration should be completed before data replication."
            )
        
        logger.info(
            f"Cross-database validation summary: {source_config.engine_type} -> {target_config.engine_type}, "
            f"Compatible: {validation_summary['overall_compatible']}"
        )
        
        return validation_summary


class IncrementalColumnDetector:
    """Detects suitable columns for incremental loading strategies."""
    
    # Common timestamp column names
    TIMESTAMP_COLUMN_NAMES = [
        'updated_at', 'modified_at', 'last_modified', 'last_updated',
        'created_at', 'insert_date', 'update_date', 'modified_date',
        'timestamp', 'last_change', 'change_date', 'mod_time'
    ]
    
    # Common primary key column names
    PRIMARY_KEY_COLUMN_NAMES = [
        'id', 'pk', 'primary_key', 'key', 'seq', 'sequence',
        'row_id', 'record_id', 'unique_id'
    ]
    
    @classmethod
    def detect_incremental_strategy(cls, schema: StructType, table_name: str) -> Dict[str, Any]:
        """Detect the best incremental loading strategy for a table."""
        column_names = [field.name.lower() for field in schema.fields]
        column_types = {field.name.lower(): field.dataType for field in schema.fields}
        
        strategy_info = {
            'strategy': 'hash',  # Default fallback
            'column': None,
            'confidence': 0.0,
            'reason': 'No suitable incremental column found, using hash-based strategy'
        }
        
        # Strategy 1: Look for timestamp columns
        timestamp_candidates = []
        for col_name in column_names:
            col_type = column_types[col_name]
            
            # Check if it's a timestamp/date type
            if isinstance(col_type, (TimestampType, DateType)):
                confidence = 0.5  # Base confidence for timestamp types
                
                # Boost confidence for common timestamp column names
                for pattern in cls.TIMESTAMP_COLUMN_NAMES:
                    if pattern in col_name:
                        confidence += 0.3
                        break
                
                # Prefer 'updated_at' or 'modified_at' patterns
                if any(pattern in col_name for pattern in ['updated', 'modified', 'last_modified']):
                    confidence += 0.2
                
                timestamp_candidates.append((col_name, confidence))
        
        # Select best timestamp candidate
        if timestamp_candidates:
            best_timestamp = max(timestamp_candidates, key=lambda x: x[1])
            if best_timestamp[1] > strategy_info['confidence']:
                strategy_info = {
                    'strategy': 'timestamp',
                    'column': best_timestamp[0],
                    'confidence': best_timestamp[1],
                    'reason': f'Found timestamp column: {best_timestamp[0]}'
                }
        
        # Strategy 2: Look for auto-incrementing primary key columns
        pk_candidates = []
        for col_name in column_names:
            col_type = column_types[col_name]
            
            # Check if it's an integer type (potential auto-increment)
            if isinstance(col_type, (IntegerType, LongType)):
                confidence = 0.3  # Base confidence for integer types
                
                # Boost confidence for common primary key column names
                for pattern in cls.PRIMARY_KEY_COLUMN_NAMES:
                    if pattern == col_name or col_name.endswith('_' + pattern):
                        confidence += 0.4
                        break
                
                # Special boost for 'id' columns
                if col_name == 'id' or col_name.endswith('_id'):
                    confidence += 0.2
                
                pk_candidates.append((col_name, confidence))
        
        # Select best primary key candidate
        if pk_candidates:
            best_pk = max(pk_candidates, key=lambda x: x[1])
            if best_pk[1] > strategy_info['confidence']:
                strategy_info = {
                    'strategy': 'primary_key',
                    'column': best_pk[0],
                    'confidence': best_pk[1],
                    'reason': f'Found auto-increment primary key column: {best_pk[0]}'
                }
        
        logger.info(
            f"Incremental strategy for table {table_name}: {strategy_info['strategy']} "
            f"(column: {strategy_info['column']}, confidence: {strategy_info['confidence']:.2f}) - "
            f"{strategy_info['reason']}"
        )
        
        return strategy_info
    
    @classmethod
    def validate_incremental_column(cls, connection_manager: JdbcConnectionManager,
                                  connection_config: ConnectionConfig, table_name: str,
                                  column_name: str, strategy: str) -> bool:
        """Validate that the incremental column is suitable for the strategy."""
        try:
            # Build validation query based on strategy
            if strategy == 'timestamp':
                # Check if column has reasonable timestamp values
                validation_query = f"""
                SELECT 
                    COUNT(*) as total_rows,
                    COUNT({column_name}) as non_null_rows,
                    MIN({column_name}) as min_value,
                    MAX({column_name}) as max_value
                FROM {connection_config.schema}.{table_name}
                """
            elif strategy == 'primary_key':
                # Check if column has sequential values
                validation_query = f"""
                SELECT 
                    COUNT(*) as total_rows,
                    COUNT(DISTINCT {column_name}) as unique_rows,
                    MIN({column_name}) as min_value,
                    MAX({column_name}) as max_value
                FROM {connection_config.schema}.{table_name}
                """
            else:
                return True  # Hash strategy doesn't need column validation
            
            df = connection_manager.read_table_data(
                connection_config=connection_config,
                table_name=table_name,
                query=validation_query
            )
            
            result = df.collect()[0]
            total_rows = result['total_rows']
            
            if strategy == 'timestamp':
                non_null_rows = result['non_null_rows']
                if non_null_rows < total_rows * 0.95:  # At least 95% non-null
                    logger.warning(
                        f"Timestamp column {column_name} has too many null values: "
                        f"{non_null_rows}/{total_rows} ({non_null_rows/total_rows*100:.1f}%)"
                    )
                    return False
            
            elif strategy == 'primary_key':
                unique_rows = result['unique_rows']
                if unique_rows != total_rows:  # Must be unique
                    logger.warning(
                        f"Primary key column {column_name} is not unique: "
                        f"{unique_rows}/{total_rows} unique values"
                    )
                    return False
            
            logger.info(f"Incremental column {column_name} validation passed for strategy {strategy}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to validate incremental column {column_name}: {str(e)}")
            return False


class JobBookmarkManager:
    """Manages AWS Glue job bookmarks for incremental loading."""
    
    def __init__(self, glue_context: GlueContext, job_name: str):
        self.glue_context = glue_context
        self.job_name = job_name
        self.bookmark_states = {}
    
    def initialize_bookmark_state(self, table_name: str, incremental_strategy: str,
                                incremental_column: Optional[str] = None) -> JobBookmarkState:
        """Initialize job bookmark state for a table."""
        bookmark_key = f"{self.job_name}_{table_name}"
        
        try:
            # Try to read existing bookmark state
            existing_state = self.glue_context.get_bookmark_state(bookmark_key)
            
            if existing_state and existing_state.get('bookmark'):
                # Parse existing bookmark state
                bookmark_data = existing_state['bookmark']
                
                if isinstance(bookmark_data, dict):
                    state = JobBookmarkState.from_dict(bookmark_data)
                    state.is_first_run = False
                    logger.info(f"Loaded existing bookmark state for table {table_name}")
                else:
                    # Legacy or corrupted bookmark, create new state
                    state = JobBookmarkState(
                        table_name=table_name,
                        incremental_strategy=incremental_strategy,
                        incremental_column=incremental_column,
                        is_first_run=True
                    )
                    logger.info(f"Created new bookmark state for table {table_name} (legacy bookmark found)")
            else:
                # No existing bookmark, create new state
                state = JobBookmarkState(
                    table_name=table_name,
                    incremental_strategy=incremental_strategy,
                    incremental_column=incremental_column,
                    is_first_run=True
                )
                logger.info(f"Created new bookmark state for table {table_name} (first run)")
        
        except Exception as e:
            logger.warning(f"Failed to read bookmark state for {table_name}: {str(e)}. Creating new state.")
            state = JobBookmarkState(
                table_name=table_name,
                incremental_strategy=incremental_strategy,
                incremental_column=incremental_column,
                is_first_run=True
            )
        
        self.bookmark_states[table_name] = state
        return state
    
    def update_bookmark_state(self, table_name: str, new_max_value: Any,
                            processed_rows: int = 0) -> None:
        """Update job bookmark state after successful processing."""
        if table_name not in self.bookmark_states:
            raise ValueError(f"No bookmark state found for table {table_name}")
        
        state = self.bookmark_states[table_name]
        state.last_processed_value = new_max_value
        state.last_update_timestamp = datetime.now(timezone.utc)
        state.is_first_run = False
        
        # Save bookmark state to Glue
        bookmark_key = f"{self.job_name}_{table_name}"
        bookmark_data = state.to_dict()
        
        try:
            self.glue_context.set_bookmark_state(bookmark_key, {'bookmark': bookmark_data})
            logger.info(
                f"Updated bookmark state for table {table_name}: "
                f"last_value={new_max_value}, processed_rows={processed_rows}"
            )
        except Exception as e:
            logger.error(f"Failed to save bookmark state for {table_name}: {str(e)}")
            raise RuntimeError(f"Bookmark state update failed: {str(e)}")
    
    def get_bookmark_state(self, table_name: str) -> Optional[JobBookmarkState]:
        """Get current bookmark state for a table."""
        return self.bookmark_states.get(table_name)
    
    def reset_bookmark_state(self, table_name: str) -> None:
        """Reset bookmark state for a table (force full reload)."""
        bookmark_key = f"{self.job_name}_{table_name}"
        
        try:
            self.glue_context.reset_bookmark_state(bookmark_key)
            
            # Update local state
            if table_name in self.bookmark_states:
                state = self.bookmark_states[table_name]
                state.last_processed_value = None
                state.last_update_timestamp = None
                state.is_first_run = True
            
            logger.info(f"Reset bookmark state for table {table_name}")
        except Exception as e:
            logger.error(f"Failed to reset bookmark state for {table_name}: {str(e)}")
            raise RuntimeError(f"Bookmark state reset failed: {str(e)}")
    
    def get_all_bookmark_states(self) -> Dict[str, JobBookmarkState]:
        """Get all bookmark states."""
        return self.bookmark_states.copy()


class FullLoadDataMigrator:
    """Handles full-load data migration operations."""
    
    def __init__(self, spark_session: SparkSession, connection_manager: JdbcConnectionManager):
        self.spark = spark_session
        self.connection_manager = connection_manager
        self.data_type_mapper = DataTypeMapper()
        self.schema_validator = SchemaCompatibilityValidator(self.data_type_mapper)
        self.progress_tracker = {}
    
    def get_table_row_count(self, connection_config: ConnectionConfig, table_name: str) -> int:
        """Get total row count for a table."""
        try:
            count_query = f"SELECT COUNT(*) as row_count FROM {connection_config.schema}.{table_name}"
            
            df = self.connection_manager.read_table_data(
                connection_config=connection_config,
                table_name=table_name,
                query=count_query
            )
            
            row_count = df.collect()[0]['row_count']
            logger.info(f"Table {connection_config.schema}.{table_name} has {row_count:,} rows")
            return row_count
            
        except Exception as e:
            logger.error(f"Failed to get row count for table {table_name}: {str(e)}")
            raise RuntimeError(f"Row count query failed: {str(e)}")
    
    def read_source_table_data(self, source_config: ConnectionConfig, table_name: str, 
                              batch_size: int = 10000, 
                              structured_logger: StructuredLogger = None) -> tuple[DataFrame, Dict[str, Any]]:
        """Read complete table data from source database with volume tracking."""
        if not structured_logger:
            structured_logger = StructuredLogger("data_migration")
            
        read_start_time = time.time()
        volume_stats = {
            'rows_read': 0,
            'bytes_read': 0,
            'read_duration_seconds': 0,
            'read_throughput_rows_per_sec': 0,
            'read_throughput_mb_per_sec': 0
        }
        
        try:
            structured_logger.info("Reading complete data from source table", 
                                 table_name=f"{source_config.schema}.{table_name}",
                                 batch_size=batch_size)
            
            # Configure JDBC options for better performance
            jdbc_options = {
                'fetchsize': str(batch_size),
                'batchsize': str(batch_size)
            }
            
            # Add partitioning for large tables if possible
            if source_config.engine_type.lower() in ['oracle', 'sqlserver', 'postgresql']:
                jdbc_options['numPartitions'] = '4'
            
            df = self.connection_manager.read_table_data(
                connection_config=source_config,
                table_name=table_name,
                **jdbc_options
            )
            
            # Cache the DataFrame for better performance during multiple operations
            df.cache()
            
            # Calculate volume statistics
            read_end_time = time.time()
            volume_stats['read_duration_seconds'] = read_end_time - read_start_time
            
            # Get row count and estimate data size
            try:
                row_count = df.count()
                volume_stats['rows_read'] = row_count
                
                # Estimate data size using improved estimation
                if row_count > 0:
                    volume_stats['bytes_read'] = estimate_dataframe_size(df)
                
                # Calculate throughput
                if volume_stats['read_duration_seconds'] > 0:
                    volume_stats['read_throughput_rows_per_sec'] = row_count / volume_stats['read_duration_seconds']
                    volume_stats['read_throughput_mb_per_sec'] = (volume_stats['bytes_read'] / 1024 / 1024) / volume_stats['read_duration_seconds']
                
            except Exception as stats_error:
                structured_logger.warning("Failed to calculate volume statistics", 
                                        error=str(stats_error))
            
            structured_logger.info("Successfully read data from source table", 
                                 table_name=table_name,
                                 rows_read=volume_stats['rows_read'],
                                 duration_seconds=round(volume_stats['read_duration_seconds'], 2),
                                 throughput_rows_per_sec=round(volume_stats['read_throughput_rows_per_sec'], 2),
                                 estimated_mb=round(volume_stats['bytes_read'] / 1024 / 1024, 2))
            
            return df, volume_stats
            
        except Exception as e:
            volume_stats['read_duration_seconds'] = time.time() - read_start_time
            structured_logger.error("Failed to read source table data", 
                                  table_name=table_name, 
                                  error=str(e),
                                  duration_seconds=round(volume_stats['read_duration_seconds'], 2))
            raise RuntimeError(f"Source data reading failed: {str(e)}")
    
    def write_target_table_data(self, df: DataFrame, target_config: ConnectionConfig, 
                               table_name: str, write_mode: str = 'overwrite',
                               structured_logger: StructuredLogger = None) -> Dict[str, Any]:
        """Write data to target database table with proper data type handling and volume tracking."""
        if not structured_logger:
            structured_logger = StructuredLogger("data_migration")
            
        write_start_time = time.time()
        volume_stats = {
            'rows_written': 0,
            'bytes_written': 0,
            'write_duration_seconds': 0,
            'write_throughput_rows_per_sec': 0,
            'write_throughput_mb_per_sec': 0
        }
        
        try:
            # Get row count before writing for statistics
            row_count = df.count()
            volume_stats['rows_written'] = row_count
            
            structured_logger.info("Writing data to target table", 
                                 table_name=f"{target_config.schema}.{table_name}",
                                 rows_to_write=row_count,
                                 write_mode=write_mode)
            
            # Configure JDBC options for better write performance
            jdbc_options = {
                'batchsize': '10000',
                'isolationLevel': 'READ_UNCOMMITTED'
            }
            
            # Add engine-specific write optimizations
            engine_type = target_config.engine_type.lower()
            
            if engine_type == 'postgresql':
                jdbc_options.update({
                    'stringtype': 'unspecified',
                    'reWriteBatchedInserts': 'true'
                })
            elif engine_type == 'sqlserver':
                jdbc_options.update({
                    'bulkCopyBatchSize': '10000',
                    'bulkCopyTimeout': '600'
                })
            elif engine_type == 'oracle':
                jdbc_options.update({
                    'oracle.jdbc.batchUpdateException': 'true'
                })
            
            # Write data to target table
            self.connection_manager.write_table_data(
                df=df,
                connection_config=target_config,
                table_name=table_name,
                mode=write_mode,
                **jdbc_options
            )
            
            # Calculate final statistics
            write_end_time = time.time()
            volume_stats['write_duration_seconds'] = write_end_time - write_start_time
            
            # Estimate bytes written using improved estimation
            if row_count > 0:
                volume_stats['bytes_written'] = estimate_dataframe_size(df)
            
            # Calculate throughput
            if volume_stats['write_duration_seconds'] > 0:
                volume_stats['write_throughput_rows_per_sec'] = row_count / volume_stats['write_duration_seconds']
                volume_stats['write_throughput_mb_per_sec'] = (volume_stats['bytes_written'] / 1024 / 1024) / volume_stats['write_duration_seconds']
            
            structured_logger.info("Successfully wrote data to target table", 
                                 table_name=table_name,
                                 rows_written=volume_stats['rows_written'],
                                 duration_seconds=round(volume_stats['write_duration_seconds'], 2),
                                 throughput_rows_per_sec=round(volume_stats['write_throughput_rows_per_sec'], 2),
                                 estimated_mb=round(volume_stats['bytes_written'] / 1024 / 1024, 2))
            
            return volume_stats
            
        except Exception as e:
            volume_stats['write_duration_seconds'] = time.time() - write_start_time
            structured_logger.error("Failed to write target table data", 
                                  table_name=table_name, 
                                  error=str(e),
                                  duration_seconds=round(volume_stats['write_duration_seconds'], 2))
            raise RuntimeError(f"Target data writing failed: {str(e)}")
        
        return volume_stats
    
    def _validate_and_transform_cross_database_data(self, source_df: DataFrame, 
                                                   source_config: ConnectionConfig,
                                                   target_config: ConnectionConfig, 
                                                   table_name: str) -> DataFrame:
        """Validate schema compatibility and transform data for cross-database replication."""
        try:
            logger.info(f"Validating and transforming data for cross-database replication: {table_name}")
            
            # Step 1: Get target table schema for validation
            try:
                target_schema = self.connection_manager.get_table_schema(target_config, table_name)
                logger.info(f"Retrieved target schema for validation: {table_name}")
            except Exception as e:
                logger.warning(f"Could not retrieve target schema for {table_name}: {str(e)}")
                logger.warning("Proceeding with data transformation without schema validation")
                target_schema = None
            
            # Step 2: Validate schema compatibility if target schema is available
            if target_schema:
                validation_result = self.schema_validator.validate_schema_compatibility(
                    source_df.schema, target_schema, 
                    source_config.engine_type, target_config.engine_type, 
                    table_name
                )
                
                # Log validation report
                compatibility_report = self.schema_validator.generate_compatibility_report(
                    validation_result, table_name
                )
                logger.info(f"Schema compatibility report:\n{compatibility_report}")
                
                # Handle validation errors
                if not validation_result['is_compatible']:
                    error_msg = f"Schema compatibility validation failed for table {table_name}. "
                    error_msg += f"Errors: {len(validation_result['errors'])}, "
                    error_msg += f"Missing columns: {validation_result['missing_columns']}, "
                    error_msg += f"Type mismatches: {len(validation_result['type_mismatches'])}"
                    
                    logger.error(error_msg)
                    raise RuntimeError(error_msg)
                
                # Log warnings if any
                if validation_result['warnings']:
                    for warning in validation_result['warnings']:
                        logger.warning(f"Schema validation warning for {table_name}: {warning}")
            
            # Step 3: Apply data type transformations
            logger.info(f"Applying data type transformations for {source_config.engine_type} -> {target_config.engine_type}")
            transformed_df = self.data_type_mapper.transform_dataframe_types(
                source_df, source_config.engine_type, target_config.engine_type
            )
            
            # Step 4: Validate transformation results
            if transformed_df.count() != source_df.count():
                raise RuntimeError(
                    f"Data transformation resulted in row count change for table {table_name}: "
                    f"original={source_df.count()}, transformed={transformed_df.count()}"
                )
            
            logger.info(f"Successfully validated and transformed data for table {table_name}")
            return transformed_df
            
        except Exception as e:
            logger.error(f"Cross-database data validation and transformation failed for {table_name}: {str(e)}")
            raise RuntimeError(f"Cross-database transformation failed: {str(e)}")
    
    def validate_cross_database_compatibility(self, source_config: ConnectionConfig,
                                            target_config: ConnectionConfig,
                                            table_names: List[str]) -> Dict[str, Any]:
        """Validate overall cross-database compatibility before starting migration."""
        logger.info("Validating cross-database compatibility assumptions")
        
        try:
            # Validate cross-database assumptions
            compatibility_summary = self.schema_validator.validate_cross_database_assumptions(
                source_config, target_config, table_names
            )
            
            # Log compatibility summary
            logger.info(f"Cross-database compatibility validation completed:")
            logger.info(f"  Overall compatible: {compatibility_summary['overall_compatible']}")
            logger.info(f"  Tables to validate: {len(table_names)}")
            
            if compatibility_summary['unsupported_mappings']:
                logger.warning(f"Unsupported mappings detected: {compatibility_summary['unsupported_mappings']}")
            
            if compatibility_summary['recommendations']:
                logger.info("Recommendations:")
                for recommendation in compatibility_summary['recommendations']:
                    logger.info(f"  • {recommendation}")
            
            return compatibility_summary
            
        except Exception as e:
            logger.error(f"Cross-database compatibility validation failed: {str(e)}")
            raise RuntimeError(f"Compatibility validation failed: {str(e)}")
    
    def perform_full_load_migration(self, source_config: ConnectionConfig, 
                                   target_config: ConnectionConfig, 
                                   table_name: str) -> FullLoadProgress:
        """Perform full-load migration for a single table."""
        progress = FullLoadProgress(table_name=table_name)
        progress.start_time = time.time()
        progress.status = 'in_progress'
        
        # Store progress in tracker
        self.progress_tracker[table_name] = progress
        
        try:
            logger.info(f"Starting full-load migration for table: {table_name}")
            
            # Validate cross-database compatibility if needed
            if self.data_type_mapper.is_cross_database_replication(
                source_config.engine_type, target_config.engine_type
            ):
                compatibility_result = self.validate_cross_database_compatibility(
                    source_config, target_config, [table_name]
                )
                if not compatibility_result['overall_compatible']:
                    raise RuntimeError(
                        f"Cross-database compatibility validation failed for table {table_name}"
                    )
            
            # Step 1: Get total row count for progress tracking
            progress.total_rows = self.get_table_row_count(source_config, table_name)
            
            # Step 2: Read source data
            logger.info(f"Reading source data for table: {table_name}")
            source_df = self.read_source_table_data(source_config, table_name)
            
            # Step 3: Handle cross-database type compatibility if needed
            if self.data_type_mapper.is_cross_database_replication(
                source_config.engine_type, target_config.engine_type
            ):
                logger.info(f"Cross-database replication detected: {source_config.engine_type} -> {target_config.engine_type}")
                logger.info("Performing cross-database data transformation and validation")
                
                # Validate schema compatibility assumptions
                source_df = self._validate_and_transform_cross_database_data(
                    source_df, source_config, target_config, table_name
                )
            else:
                logger.info(f"Same-engine replication: {source_config.engine_type} -> {target_config.engine_type}")
            
            # Step 4: Write to target database
            logger.info(f"Writing data to target table: {table_name}")
            self.write_target_table_data(source_df, target_config, table_name, 'overwrite')
            
            # Step 5: Verify data was written successfully
            target_row_count = self.get_table_row_count(target_config, table_name)
            
            if target_row_count != progress.total_rows:
                raise RuntimeError(
                    f"Row count mismatch: source={progress.total_rows}, target={target_row_count}"
                )
            
            # Step 6: Update progress
            progress.processed_rows = progress.total_rows
            progress.status = 'completed'
            progress.end_time = time.time()
            
            logger.info(
                f"Full-load migration completed for {table_name}: "
                f"{progress.total_rows:,} rows in {progress.duration_seconds:.2f} seconds "
                f"({progress.rows_per_second:.2f} rows/sec)"
            )
            
            # Clean up cached DataFrame
            source_df.unpersist()
            
            return progress
            
        except Exception as e:
            progress.status = 'failed'
            progress.error_message = str(e)
            progress.end_time = time.time()
            
            logger.error(
                f"Full-load migration failed for {table_name} after {progress.duration_seconds:.2f} seconds: {str(e)}"
            )
            raise RuntimeError(f"Full-load migration failed for {table_name}: {str(e)}")
    
    def perform_full_load_migration_batch(self, source_config: ConnectionConfig,
                                         target_config: ConnectionConfig,
                                         table_names: List[str]) -> Dict[str, FullLoadProgress]:
        """Perform full-load migration for multiple tables."""
        results = {}
        total_tables = len(table_names)
        
        logger.info(f"Starting full-load migration for {total_tables} tables: {', '.join(table_names)}")
        
        for i, table_name in enumerate(table_names, 1):
            try:
                logger.info(f"Processing table {i}/{total_tables}: {table_name}")
                
                progress = self.perform_full_load_migration(
                    source_config=source_config,
                    target_config=target_config,
                    table_name=table_name
                )
                
                results[table_name] = progress
                
                logger.info(
                    f"Completed table {i}/{total_tables}: {table_name} "
                    f"({progress.total_rows:,} rows, {progress.duration_seconds:.2f}s)"
                )
                
            except Exception as e:
                logger.error(f"Failed to migrate table {table_name}: {str(e)}")
                
                # Create failed progress entry
                failed_progress = FullLoadProgress(table_name=table_name)
                failed_progress.status = 'failed'
                failed_progress.error_message = str(e)
                failed_progress.end_time = time.time()
                results[table_name] = failed_progress
                
                # Continue with next table instead of failing entire batch
                continue
        
        # Log summary
        successful_tables = [name for name, progress in results.items() if progress.status == 'completed']
        failed_tables = [name for name, progress in results.items() if progress.status == 'failed']
        
        total_rows_migrated = sum(
            progress.processed_rows for progress in results.values() 
            if progress.status == 'completed'
        )
        
        logger.info(
            f"Full-load migration batch completed: "
            f"{len(successful_tables)}/{total_tables} tables successful, "
            f"{total_rows_migrated:,} total rows migrated"
        )
        
        if failed_tables:
            logger.warning(f"Failed tables: {', '.join(failed_tables)}")
        
        return results
    
    def get_migration_progress(self, table_name: str) -> Optional[FullLoadProgress]:
        """Get current migration progress for a table."""
        return self.progress_tracker.get(table_name)
    
    def get_all_migration_progress(self) -> Dict[str, FullLoadProgress]:
        """Get migration progress for all tables."""
        return self.progress_tracker.copy()
    
    def log_migration_summary(self, results: Dict[str, FullLoadProgress]) -> None:
        """Log detailed migration summary."""
        logger.info("=== Full-Load Migration Summary ===")
        
        for table_name, progress in results.items():
            status_symbol = "✓" if progress.status == 'completed' else "✗"
            
            logger.info(
                f"{status_symbol} {table_name}: {progress.status.upper()} - "
                f"{progress.processed_rows:,}/{progress.total_rows:,} rows "
                f"({progress.duration_seconds:.2f}s, {progress.rows_per_second:.2f} rows/sec)"
            )
            
            if progress.error_message:
                logger.error(f"  Error: {progress.error_message}")
        
        # Overall statistics
        total_tables = len(results)
        successful_tables = sum(1 for p in results.values() if p.status == 'completed')
        total_rows = sum(p.processed_rows for p in results.values() if p.status == 'completed')
        total_duration = sum(p.duration_seconds for p in results.values())
        
        logger.info(f"Overall: {successful_tables}/{total_tables} tables successful")
        logger.info(f"Total rows migrated: {total_rows:,}")
        logger.info(f"Total duration: {total_duration:.2f} seconds")
        logger.info("=== End Migration Summary ===")


class IncrementalDataMigrator:
    """Handles incremental data migration operations using job bookmarks."""
    
    def __init__(self, spark_session: SparkSession, connection_manager: JdbcConnectionManager,
                 bookmark_manager: JobBookmarkManager):
        self.spark = spark_session
        self.connection_manager = connection_manager
        self.bookmark_manager = bookmark_manager
        self.column_detector = IncrementalColumnDetector()
        self.data_type_mapper = DataTypeMapper()
        self.schema_validator = SchemaCompatibilityValidator(self.data_type_mapper)
        self.progress_tracker = {}
    
    def detect_and_validate_incremental_strategy(self, connection_config: ConnectionConfig,
                                               table_name: str) -> Dict[str, Any]:
        """Detect and validate the best incremental loading strategy for a table."""
        try:
            # Get table schema
            schema = self.connection_manager.get_table_schema(connection_config, table_name)
            
            # Detect incremental strategy
            strategy_info = self.column_detector.detect_incremental_strategy(schema, table_name)
            
            # Validate the detected column if not using hash strategy
            if strategy_info['strategy'] != 'hash' and strategy_info['column']:
                is_valid = self.column_detector.validate_incremental_column(
                    self.connection_manager, connection_config, table_name,
                    strategy_info['column'], strategy_info['strategy']
                )
                
                if not is_valid:
                    logger.warning(
                        f"Incremental column validation failed for {table_name}. "
                        f"Falling back to hash-based strategy."
                    )
                    strategy_info = {
                        'strategy': 'hash',
                        'column': None,
                        'confidence': 0.8,
                        'reason': 'Column validation failed, using hash-based strategy'
                    }
            
            return strategy_info
            
        except Exception as e:
            logger.error(f"Failed to detect incremental strategy for {table_name}: {str(e)}")
            # Fallback to hash strategy
            return {
                'strategy': 'hash',
                'column': None,
                'confidence': 0.5,
                'reason': f'Strategy detection failed: {str(e)}'
            }
    
    def build_incremental_query(self, connection_config: ConnectionConfig, table_name: str,
                              strategy_info: Dict[str, Any], bookmark_state: JobBookmarkState) -> str:
        """Build SQL query for incremental data extraction."""
        full_table_name = f"{connection_config.schema}.{table_name}"
        
        if bookmark_state.is_first_run:
            # First run - return all data
            return f"SELECT * FROM {full_table_name}"
        
        strategy = strategy_info['strategy']
        column_name = strategy_info['column']
        
        if strategy == 'timestamp':
            # Timestamp-based incremental query
            last_value = bookmark_state.last_processed_value
            if last_value:
                return f"""
                SELECT * FROM {full_table_name}
                WHERE {column_name} > '{last_value}'
                ORDER BY {column_name}
                """
            else:
                return f"SELECT * FROM {full_table_name}"
        
        elif strategy == 'primary_key':
            # Primary key-based incremental query
            last_value = bookmark_state.last_processed_value
            if last_value:
                return f"""
                SELECT * FROM {full_table_name}
                WHERE {column_name} > {last_value}
                ORDER BY {column_name}
                """
            else:
                return f"SELECT * FROM {full_table_name}"
        
        elif strategy == 'hash':
            # Hash-based strategy - need to compare row hashes
            # This is more complex and requires storing row hashes
            # For now, we'll do a full comparison (can be optimized later)
            return f"SELECT * FROM {full_table_name}"
        
        else:
            raise ValueError(f"Unsupported incremental strategy: {strategy}")
    
    def get_current_max_value(self, connection_config: ConnectionConfig, table_name: str,
                            strategy_info: Dict[str, Any]) -> Any:
        """Get the current maximum value for the incremental column."""
        strategy = strategy_info['strategy']
        column_name = strategy_info['column']
        
        if strategy in ['timestamp', 'primary_key'] and column_name:
            try:
                full_table_name = f"{connection_config.schema}.{table_name}"
                max_query = f"SELECT MAX({column_name}) as max_value FROM {full_table_name}"
                
                df = self.connection_manager.read_table_data(
                    connection_config=connection_config,
                    table_name=table_name,
                    query=max_query
                )
                
                result = df.collect()[0]
                max_value = result['max_value']
                
                logger.info(f"Current max value for {table_name}.{column_name}: {max_value}")
                return max_value
                
            except Exception as e:
                logger.error(f"Failed to get max value for {table_name}.{column_name}: {str(e)}")
                return None
        
        return None
    
    def perform_hash_based_incremental_load(self, source_config: ConnectionConfig,
                                          target_config: ConnectionConfig, table_name: str,
                                          bookmark_state: JobBookmarkState) -> IncrementalLoadProgress:
        """Perform hash-based incremental loading."""
        progress = IncrementalLoadProgress(
            table_name=table_name,
            incremental_strategy='hash'
        )
        progress.start_time = time.time()
        progress.status = 'in_progress'
        
        try:
            logger.info(f"Performing hash-based incremental load for table: {table_name}")
            
            # Read source data with row hashes
            full_table_name = f"{source_config.schema}.{table_name}"
            source_df = self.connection_manager.read_table_data(
                connection_config=source_config,
                table_name=table_name
            )
            
            # Add row hash column
            columns = source_df.columns
            source_df_with_hash = source_df.withColumn(
                'row_hash',
                hash(concat_ws('|', *[col(c) for c in columns]))
            )
            
            if bookmark_state.is_first_run:
                # First run - migrate all data
                delta_df = source_df
                progress.delta_rows = source_df.count()
                logger.info(f"First run: migrating all {progress.delta_rows:,} rows")
            else:
                # Compare with existing data to find changes
                # This is a simplified implementation - in production you might want
                # to store row hashes in a separate tracking table
                logger.info("Hash-based comparison not fully implemented - performing full refresh")
                delta_df = source_df
                progress.delta_rows = source_df.count()
            
            if progress.delta_rows > 0:
                # Apply cross-database transformations if needed
                if self.data_type_mapper.is_cross_database_replication(
                    source_config.engine_type, target_config.engine_type
                ):
                    logger.info(f"Applying cross-database transformations for hash-based incremental data: {table_name}")
                    delta_df = self._apply_cross_database_transformations(
                        delta_df, source_config, target_config, table_name
                    )
                
                # Write delta data to target
                self.connection_manager.write_table_data(
                    df=delta_df,
                    connection_config=target_config,
                    table_name=table_name,
                    mode='overwrite' if bookmark_state.is_first_run else 'append'
                )
                
                progress.processed_rows = progress.delta_rows
            
            progress.status = 'completed'
            progress.end_time = time.time()
            
            logger.info(
                f"Hash-based incremental load completed for {table_name}: "
                f"{progress.processed_rows:,} rows in {progress.duration_seconds:.2f} seconds"
            )
            
            return progress
            
        except Exception as e:
            progress.status = 'failed'
            progress.error_message = str(e)
            progress.end_time = time.time()
            
            logger.error(f"Hash-based incremental load failed for {table_name}: {str(e)}")
            raise RuntimeError(f"Hash-based incremental load failed: {str(e)}")
    
    def perform_incremental_load_migration(self, source_config: ConnectionConfig,
                                         target_config: ConnectionConfig,
                                         table_name: str) -> IncrementalLoadProgress:
        """Perform incremental load migration for a single table."""
        try:
            logger.info(f"Starting incremental load migration for table: {table_name}")
            
            # Step 1: Detect and validate incremental strategy
            strategy_info = self.detect_and_validate_incremental_strategy(source_config, table_name)
            
            # Step 2: Initialize bookmark state
            bookmark_state = self.bookmark_manager.initialize_bookmark_state(
                table_name=table_name,
                incremental_strategy=strategy_info['strategy'],
                incremental_column=strategy_info['column']
            )
            
            # Step 3: Handle hash-based strategy separately
            if strategy_info['strategy'] == 'hash':
                return self.perform_hash_based_incremental_load(
                    source_config, target_config, table_name, bookmark_state
                )
            
            # Step 4: Initialize progress tracking
            progress = IncrementalLoadProgress(
                table_name=table_name,
                incremental_strategy=strategy_info['strategy'],
                incremental_column=strategy_info['column'],
                bookmark_state=bookmark_state.to_dict()
            )
            progress.start_time = time.time()
            progress.status = 'in_progress'
            
            # Store progress in tracker
            self.progress_tracker[table_name] = progress
            
            # Step 5: Get current max value from source
            current_max_value = self.get_current_max_value(source_config, table_name, strategy_info)
            progress.current_max_value = current_max_value
            progress.last_processed_value = bookmark_state.last_processed_value
            
            # Step 6: Build incremental query
            incremental_query = self.build_incremental_query(
                source_config, table_name, strategy_info, bookmark_state
            )
            
            logger.info(f"Incremental query for {table_name}: {incremental_query}")
            
            # Step 7: Read incremental data
            delta_df = self.connection_manager.read_table_data(
                connection_config=source_config,
                table_name=table_name,
                query=incremental_query
            )
            
            # Step 8: Count delta rows
            progress.delta_rows = delta_df.count()
            
            if progress.delta_rows == 0:
                logger.info(f"No new data found for table {table_name}")
                progress.processed_rows = 0
                progress.status = 'completed'
                progress.end_time = time.time()
                return progress
            
            logger.info(f"Found {progress.delta_rows:,} new/changed rows for table {table_name}")
            
            # Step 9: Handle cross-database type compatibility if needed
            if self.data_type_mapper.is_cross_database_replication(
                source_config.engine_type, target_config.engine_type
            ):
                logger.info(f"Applying cross-database transformations for incremental data: {table_name}")
                delta_df = self._apply_cross_database_transformations(
                    delta_df, source_config, target_config, table_name
                )
            
            # Step 10: Write delta data to target
            write_mode = 'overwrite' if bookmark_state.is_first_run else 'append'
            self.connection_manager.write_table_data(
                df=delta_df,
                connection_config=target_config,
                table_name=table_name,
                mode=write_mode
            )
            
            progress.processed_rows = progress.delta_rows
            
            # Step 11: Update bookmark state
            if current_max_value is not None:
                self.bookmark_manager.update_bookmark_state(
                    table_name=table_name,
                    new_max_value=current_max_value,
                    processed_rows=progress.processed_rows
                )
            
            progress.status = 'completed'
            progress.end_time = time.time()
            
            logger.info(
                f"Incremental load migration completed for {table_name}: "
                f"{progress.processed_rows:,} rows in {progress.duration_seconds:.2f} seconds "
                f"({progress.rows_per_second:.2f} rows/sec)"
            )
            
            return progress
            
        except Exception as e:
            logger.error(f"Incremental load migration failed for {table_name}: {str(e)}")
            
            # Create failed progress entry
            failed_progress = IncrementalLoadProgress(
                table_name=table_name,
                incremental_strategy='unknown'
            )
            failed_progress.status = 'failed'
            failed_progress.error_message = str(e)
            failed_progress.end_time = time.time()
            
            self.progress_tracker[table_name] = failed_progress
            raise RuntimeError(f"Incremental load migration failed for {table_name}: {str(e)}")
    
    def _apply_cross_database_transformations(self, df: DataFrame, 
                                            source_config: ConnectionConfig,
                                            target_config: ConnectionConfig, 
                                            table_name: str) -> DataFrame:
        """Apply cross-database data transformations for incremental data."""
        try:
            logger.info(f"Applying cross-database transformations for incremental data: {table_name}")
            
            # Apply data type transformations
            transformed_df = self.data_type_mapper.transform_dataframe_types(
                df, source_config.engine_type, target_config.engine_type
            )
            
            # Validate transformation results
            original_count = df.count()
            transformed_count = transformed_df.count()
            
            if transformed_count != original_count:
                raise RuntimeError(
                    f"Cross-database transformation resulted in row count change for incremental data in table {table_name}: "
                    f"original={original_count}, transformed={transformed_count}"
                )
            
            logger.info(f"Successfully applied cross-database transformations for incremental data: {table_name}")
            return transformed_df
            
        except Exception as e:
            logger.error(f"Cross-database transformation failed for incremental data in table {table_name}: {str(e)}")
            raise RuntimeError(f"Cross-database transformation failed: {str(e)}")
    
    def perform_incremental_load_migration_batch(self, source_config: ConnectionConfig,
                                               target_config: ConnectionConfig,
                                               table_names: List[str]) -> Dict[str, IncrementalLoadProgress]:
        """Perform incremental load migration for multiple tables."""
        results = {}
        total_tables = len(table_names)
        
        logger.info(f"Starting incremental load migration for {total_tables} tables: {', '.join(table_names)}")
        
        # Validate cross-database compatibility if needed
        if self.data_type_mapper.is_cross_database_replication(
            source_config.engine_type, target_config.engine_type
        ):
            logger.info("Validating cross-database compatibility for incremental migration")
            compatibility_result = self.schema_validator.validate_cross_database_assumptions(
                source_config, target_config, table_names
            )
            if not compatibility_result['overall_compatible']:
                raise RuntimeError(
                    f"Cross-database compatibility validation failed for incremental migration"
                )
        
        for i, table_name in enumerate(table_names, 1):
            try:
                logger.info(f"Processing table {i}/{total_tables}: {table_name}")
                
                progress = self.perform_incremental_load_migration(
                    source_config=source_config,
                    target_config=target_config,
                    table_name=table_name
                )
                
                results[table_name] = progress
                
                logger.info(
                    f"Completed table {i}/{total_tables}: {table_name} "
                    f"({progress.processed_rows:,} rows, {progress.duration_seconds:.2f}s)"
                )
                
            except Exception as e:
                logger.error(f"Failed to migrate table {table_name}: {str(e)}")
                
                # Create failed progress entry
                failed_progress = IncrementalLoadProgress(
                    table_name=table_name,
                    incremental_strategy='unknown'
                )
                failed_progress.status = 'failed'
                failed_progress.error_message = str(e)
                failed_progress.end_time = time.time()
                results[table_name] = failed_progress
                
                # Continue with next table instead of failing entire batch
                continue
        
        # Log summary
        successful_tables = [name for name, progress in results.items() if progress.status == 'completed']
        failed_tables = [name for name, progress in results.items() if progress.status == 'failed']
        
        total_rows_migrated = sum(
            progress.processed_rows for progress in results.values()
            if progress.status == 'completed'
        )
        
        logger.info(
            f"Incremental load migration batch completed: "
            f"{len(successful_tables)}/{total_tables} tables successful, "
            f"{total_rows_migrated:,} total rows migrated"
        )
        
        if failed_tables:
            logger.warning(f"Failed tables: {', '.join(failed_tables)}")
        
        return results
    
    def get_migration_progress(self, table_name: str) -> Optional[IncrementalLoadProgress]:
        """Get current migration progress for a table."""
        return self.progress_tracker.get(table_name)
    
    def get_all_migration_progress(self) -> Dict[str, IncrementalLoadProgress]:
        """Get migration progress for all tables."""
        return self.progress_tracker.copy()
    
    def log_migration_summary(self, results: Dict[str, IncrementalLoadProgress]) -> None:
        """Log detailed incremental migration summary."""
        logger.info("=== Incremental Load Migration Summary ===")
        
        for table_name, progress in results.items():
            status_symbol = "✓" if progress.status == 'completed' else "✗"
            
            logger.info(
                f"{status_symbol} {table_name}: {progress.status.upper()} - "
                f"Strategy: {progress.incremental_strategy} "
                f"(column: {progress.incremental_column or 'N/A'}) - "
                f"{progress.processed_rows:,} rows "
                f"({progress.duration_seconds:.2f}s, {progress.rows_per_second:.2f} rows/sec)"
            )
            
            if progress.error_message:
                logger.error(f"  Error: {progress.error_message}")
        
        # Overall statistics
        total_tables = len(results)
        successful_tables = sum(1 for p in results.values() if p.status == 'completed')
        total_rows = sum(p.processed_rows for p in results.values() if p.status == 'completed')
        total_duration = sum(p.duration_seconds for p in results.values())
        
        logger.info(f"Overall: {successful_tables}/{total_tables} tables successful")
        logger.info(f"Total rows migrated: {total_rows:,}")
        logger.info(f"Total duration: {total_duration:.2f} seconds")
        logger.info("=== End Incremental Migration Summary ===")


def test_database_connections(spark: SparkSession, job_config: JobConfig) -> None:
    """Test database connections and validate connectivity."""
    # Initialize connection manager with retry handler
    retry_handler = ConnectionRetryHandler(max_retries=3, base_delay=2.0)
    connection_manager = JdbcConnectionManager(spark, retry_handler)
    
    # Test database connections
    logger.info("Testing database connections...")
    
    # Test source connection
    source_test_result = connection_manager.test_connection(job_config.source_connection)
    if source_test_result['connection_valid']:
        logger.info(f"Source connection test passed for {job_config.source_connection.engine_type}")
    else:
        error_msg = f"Source connection test failed: {source_test_result.get('error_message', 'Unknown error')}"
        logger.error(error_msg)
        raise RuntimeError(error_msg)
    
    # Test target connection
    target_test_result = connection_manager.test_connection(job_config.target_connection)
    if target_test_result['connection_valid']:
        logger.info(f"Target connection test passed for {job_config.target_connection.engine_type}")
    else:
        error_msg = f"Target connection test failed: {target_test_result.get('error_message', 'Unknown error')}"
        logger.error(error_msg)
        raise RuntimeError(error_msg)
    
    # Cache connection information for reuse
    connection_manager.cache_connection_info(job_config.source_connection, source_test_result)
    connection_manager.cache_connection_info(job_config.target_connection, target_test_result)
    
    logger.info("All database connections validated successfully")


def perform_full_load_data_migration(spark: SparkSession, job_config: JobConfig) -> Dict[str, FullLoadProgress]:
    """Perform full-load data migration for all configured tables."""
    logger.info("Starting full-load data migration process")
    
    # Initialize connection manager and data migrator
    retry_handler = ConnectionRetryHandler(max_retries=3, base_delay=2.0)
    connection_manager = JdbcConnectionManager(spark, retry_handler)
    data_migrator = FullLoadDataMigrator(spark, connection_manager)
    
    try:
        # Perform full-load migration for all tables
        migration_results = data_migrator.perform_full_load_migration_batch(
            source_config=job_config.source_connection,
            target_config=job_config.target_connection,
            table_names=job_config.tables
        )
        
        # Log detailed summary
        data_migrator.log_migration_summary(migration_results)
        
        # Check if any migrations failed
        failed_migrations = [
            table_name for table_name, progress in migration_results.items()
            if progress.status == 'failed'
        ]
        
        if failed_migrations:
            logger.warning(
                f"Full-load migration completed with {len(failed_migrations)} failures: "
                f"{', '.join(failed_migrations)}"
            )
        else:
            logger.info("Full-load migration completed successfully for all tables")
        
        return migration_results
        
    except Exception as e:
        logger.error(f"Full-load data migration process failed: {str(e)}")
        raise RuntimeError(f"Full-load migration failed: {str(e)}")


def perform_incremental_data_migration(spark: SparkSession, glue_context: GlueContext,
                                     job_config: JobConfig) -> Dict[str, IncrementalLoadProgress]:
    """Perform incremental data migration for all configured tables using job bookmarks."""
    logger.info("Starting incremental data migration process")
    
    # Initialize connection manager and bookmark manager
    retry_handler = ConnectionRetryHandler(max_retries=3, base_delay=2.0)
    connection_manager = JdbcConnectionManager(spark, retry_handler)
    bookmark_manager = JobBookmarkManager(glue_context, job_config.job_name)
    
    # Initialize incremental data migrator
    incremental_migrator = IncrementalDataMigrator(spark, connection_manager, bookmark_manager)
    
    try:
        # Perform incremental migration for all tables
        migration_results = incremental_migrator.perform_incremental_load_migration_batch(
            source_config=job_config.source_connection,
            target_config=job_config.target_connection,
            table_names=job_config.tables
        )
        
        # Log detailed summary
        incremental_migrator.log_migration_summary(migration_results)
        
        # Check if any migrations failed
        failed_migrations = [
            table_name for table_name, progress in migration_results.items()
            if progress.status == 'failed'
        ]
        
        if failed_migrations:
            logger.warning(
                f"Incremental migration completed with {len(failed_migrations)} failures: "
                f"{', '.join(failed_migrations)}"
            )
        else:
            logger.info("Incremental migration completed successfully for all tables")
        
        return migration_results
        
    except Exception as e:
        logger.error(f"Incremental data migration process failed: {str(e)}")
        raise RuntimeError(f"Incremental migration failed: {str(e)}")


def determine_migration_mode(glue_context: GlueContext, job_name: str, table_names: List[str]) -> str:
    """Determine whether to perform full-load or incremental migration based on job bookmarks."""
    try:
        # Check if any bookmarks exist for the tables
        has_existing_bookmarks = False
        
        for table_name in table_names:
            bookmark_key = f"{job_name}_{table_name}"
            try:
                existing_state = glue_context.get_bookmark_state(bookmark_key)
                if existing_state and existing_state.get('bookmark'):
                    has_existing_bookmarks = True
                    break
            except Exception:
                # Ignore errors when checking bookmark state
                continue
        
        if has_existing_bookmarks:
            logger.info("Existing job bookmarks found - performing incremental migration")
            return 'incremental'
        else:
            logger.info("No existing job bookmarks found - performing full-load migration")
            return 'full_load'
            
    except Exception as e:
        logger.warning(f"Failed to determine migration mode: {str(e)}. Defaulting to full-load.")
        return 'full_load'


def test_database_connections_with_recovery(spark: SparkSession, job_config: JobConfig, 
                                          error_recovery_manager: ErrorRecoveryManager,
                                          performance_monitor: PerformanceMonitor = None) -> None:
    """Test database connections with enhanced error handling, recovery, and network-aware connectivity."""
    # Initialize connection manager with enhanced retry handler
    retry_handler = ConnectionRetryHandler(max_retries=3, base_delay=2.0, max_delay=30.0)
    glue_context = GlueContext(spark.sparkContext)
    connection_manager = JdbcConnectionManager(glue_context, retry_handler)
    
    structured_logger = StructuredLogger(job_config.job_name)
    structured_logger.info("Testing database connections with network-aware recovery support")
    
    # Log network configuration summary
    network_summary = job_config.get_network_summary()
    structured_logger.info("Network configuration summary", **network_summary)
    
    # Test source connection with network-aware validation
    structured_logger.info("Testing source connection with network validation", 
                         engine_type=job_config.source_connection.engine_type,
                         cross_vpc=job_config.source_connection.requires_cross_vpc_connection(),
                         glue_connection=job_config.source_connection.get_glue_connection_name())
    
    source_start_time = time.time()
    try:
        # Use network-aware connection validation if enabled
        if job_config.validate_connections:
            source_glue_connection = job_config.source_connection.get_glue_connection_name() or ''
            source_test_result = connection_manager.validate_connection_with_network_check(
                job_config.source_connection, 
                source_glue_connection,
                job_config.connection_timeout_seconds
            )
            
            if not source_test_result:
                raise RuntimeError("Source connection network validation failed")
        else:
            # Fallback to basic connection test
            source_test_result = connection_manager.test_connection(job_config.source_connection)
            if not source_test_result['connection_valid']:
                raise RuntimeError(source_test_result.get('error_message', 'Connection validation failed'))
        
        source_duration = time.time() - source_start_time
        structured_logger.info("Source database connection successful", 
                             duration_seconds=round(source_duration, 2),
                             network_validated=job_config.validate_connections)
        
        # Record successful connection metrics
        if performance_monitor:
            performance_monitor.record_connection_attempt(
                "source", job_config.source_connection.engine_type, True, source_duration
            )
        
    except Exception as e:
        source_duration = time.time() - source_start_time
        
        # Record failed connection metrics
        if performance_monitor:
            performance_monitor.record_connection_attempt(
                "source", job_config.source_connection.engine_type, False, source_duration
            )
            performance_monitor.record_error("connection", "source_connection_test", str(e))
        
        # Handle network-specific errors
        error_info = error_recovery_manager.handle_database_connection_error(
            e, job_config.source_connection, "source_connection_test"
        )
        
        # Attempt recovery
        if error_recovery_manager.attempt_graceful_recovery(error_info):
            structured_logger.info("Retrying source connection after recovery attempt")
            try:
                if job_config.validate_connections:
                    source_glue_connection = job_config.source_connection.get_glue_connection_name() or ''
                    retry_result = connection_manager.validate_connection_with_network_check(
                        job_config.source_connection, 
                        source_glue_connection,
                        job_config.connection_timeout_seconds
                    )
                    if not retry_result:
                        raise RuntimeError("Source connection failed after recovery")
                else:
                    retry_result = connection_manager.test_connection(job_config.source_connection)
                    if not retry_result['connection_valid']:
                        raise RuntimeError(f"Source connection failed after recovery: {retry_result.get('error_message')}")
            except Exception as retry_error:
                structured_logger.error("Source database connection failed after recovery", 
                                      error=str(retry_error), duration_seconds=round(source_duration, 2))
                raise RuntimeError(f"Source database connection failed after recovery: {str(retry_error)}")
        else:
            structured_logger.error("Source database connection failed", 
                                  error=str(e), duration_seconds=round(source_duration, 2))
            raise RuntimeError(f"Source database connection failed: {str(e)}")
    
    # Test target connection with network-aware validation
    structured_logger.info("Testing target connection with network validation", 
                         engine_type=job_config.target_connection.engine_type,
                         cross_vpc=job_config.target_connection.requires_cross_vpc_connection(),
                         glue_connection=job_config.target_connection.get_glue_connection_name())
    
    target_start_time = time.time()
    try:
        # Use network-aware connection validation if enabled
        if job_config.validate_connections:
            target_glue_connection = job_config.target_connection.get_glue_connection_name() or ''
            target_test_result = connection_manager.validate_connection_with_network_check(
                job_config.target_connection, 
                target_glue_connection,
                job_config.connection_timeout_seconds
            )
            
            if not target_test_result:
                raise RuntimeError("Target connection network validation failed")
        else:
            # Fallback to basic connection test
            target_test_result = connection_manager.test_connection(job_config.target_connection)
            if not target_test_result['connection_valid']:
                raise RuntimeError(target_test_result.get('error_message', 'Connection validation failed'))
        
        target_duration = time.time() - target_start_time
        structured_logger.info("Target database connection successful", 
                             duration_seconds=round(target_duration, 2),
                             network_validated=job_config.validate_connections)
        
        # Record successful connection metrics
        if performance_monitor:
            performance_monitor.record_connection_attempt(
                "target", job_config.target_connection.engine_type, True, target_duration
            )
        
    except Exception as e:
        target_duration = time.time() - target_start_time
        
        # Record failed connection metrics
        if performance_monitor:
            performance_monitor.record_connection_attempt(
                "target", job_config.target_connection.engine_type, False, target_duration
            )
            performance_monitor.record_error("connection", "target_connection_test", str(e))
        
        # Handle network-specific errors
        error_info = error_recovery_manager.handle_database_connection_error(
            e, job_config.target_connection, "target_connection_test"
        )
        
        # Attempt recovery
        if error_recovery_manager.attempt_graceful_recovery(error_info):
            structured_logger.info("Retrying target connection after recovery attempt")
            try:
                if job_config.validate_connections:
                    target_glue_connection = job_config.target_connection.get_glue_connection_name() or ''
                    retry_result = connection_manager.validate_connection_with_network_check(
                        job_config.target_connection, 
                        target_glue_connection,
                        job_config.connection_timeout_seconds
                    )
                    if not retry_result:
                        raise RuntimeError("Target connection failed after recovery")
                else:
                    retry_result = connection_manager.test_connection(job_config.target_connection)
                    if not retry_result['connection_valid']:
                        raise RuntimeError(f"Target connection failed after recovery: {retry_result.get('error_message')}")
            except Exception as retry_error:
                structured_logger.error("Target database connection failed after recovery", 
                                      error=str(retry_error), duration_seconds=round(target_duration, 2))
                raise RuntimeError(f"Target database connection failed after recovery: {str(retry_error)}")
        else:
            structured_logger.error("Target database connection failed", 
                                  error=str(e), duration_seconds=round(target_duration, 2))
            raise RuntimeError(f"Target database connection failed: {str(e)}")
    
    # Cache connection information for reuse (if using basic test_connection)
    if not job_config.validate_connections:
        if isinstance(source_test_result, dict):
            connection_manager.cache_connection_info(job_config.source_connection, source_test_result)
        if isinstance(target_test_result, dict):
            connection_manager.cache_connection_info(job_config.target_connection, target_test_result)
    
    structured_logger.info("All database connections validated successfully with network-aware recovery support",
                         cross_vpc_connections=job_config.has_cross_vpc_connections())


def perform_full_load_data_migration_with_recovery(spark: SparkSession, job_config: JobConfig,
                                                 error_recovery_manager: ErrorRecoveryManager,
                                                 performance_monitor: PerformanceMonitor = None) -> Dict[str, FullLoadProgress]:
    """Perform full-load data migration with enhanced error handling and recovery."""
    structured_logger = StructuredLogger(job_config.job_name)
    structured_logger.info("Starting full-load data migration process with recovery support")
    
    # Initialize migration processor with enhanced retry handler
    retry_handler = ConnectionRetryHandler(max_retries=3, base_delay=2.0, max_delay=60.0)
    migration_processor = DataMigrationProcessor(spark, retry_handler)
    
    migration_results = {}
    
    for table_name in job_config.tables:
        structured_logger.info("Starting full-load migration for table", table_name=table_name)
        
        # Start monitoring for this table
        table_metrics = None
        if performance_monitor:
            table_metrics = performance_monitor.start_table_processing(table_name)
        
        try:
            # Attempt migration with error handling
            progress = migration_processor.perform_full_load_migration(
                job_config.source_connection,
                job_config.target_connection,
                table_name
            )
            migration_results[table_name] = progress
            
            # Complete monitoring for successful table
            if performance_monitor and progress.status == 'completed':
                performance_monitor.complete_table_processing(
                    table_name, 
                    progress.processed_rows, 
                    getattr(progress, 'bytes_processed', 0)
                )
            elif performance_monitor and progress.status == 'failed':
                performance_monitor.fail_table_processing(
                    table_name, 
                    getattr(progress, 'error_message', 'Migration failed')
                )
            
        except Exception as e:
            # Handle migration error
            error_info = error_recovery_manager.handle_data_processing_error(
                e, table_name, "full_load_migration", {
                    'source_engine': job_config.source_connection.engine_type,
                    'target_engine': job_config.target_connection.engine_type
                }
            )
            
            # Create failed progress object
            failed_progress = FullLoadProgress(table_name)
            failed_progress.status = 'failed'
            failed_progress.error_message = str(e)
            failed_progress.end_time = time.time()
            migration_results[table_name] = failed_progress
            
            # Attempt recovery if error is retryable
            if error_recovery_manager.attempt_graceful_recovery(error_info, {
                'table_name': table_name,
                'migration_type': 'full_load'
            }):
                logger.info(f"Retrying full-load migration for table {table_name} after recovery")
                try:
                    progress = migration_processor.perform_full_load_migration(
                        job_config.source_connection,
                        job_config.target_connection,
                        table_name
                    )
                    migration_results[table_name] = progress
                    logger.info(f"Full-load migration succeeded for table {table_name} after recovery")
                    
                except Exception as retry_error:
                    logger.error(f"Full-load migration failed for table {table_name} after recovery: {str(retry_error)}")
                    failed_progress.error_message = f"Failed after recovery: {str(retry_error)}"
            else:
                logger.error(f"Full-load migration failed for table {table_name}, no recovery possible")
    
    # Log summary
    successful_tables = [name for name, progress in migration_results.items() if progress.status == 'completed']
    failed_tables = [name for name, progress in migration_results.items() if progress.status == 'failed']
    
    logger.info(
        f"Full-load migration completed: {len(successful_tables)} successful, "
        f"{len(failed_tables)} failed out of {len(job_config.tables)} total tables"
    )
    
    if failed_tables:
        logger.warning(f"Failed tables: {', '.join(failed_tables)}")
    
    return migration_results


def perform_incremental_data_migration_with_recovery(spark: SparkSession, glue_context: GlueContext,
                                                   job_config: JobConfig, 
                                                   error_recovery_manager: ErrorRecoveryManager,
                                                   performance_monitor: PerformanceMonitor = None) -> Dict[str, IncrementalLoadProgress]:
    """Perform incremental data migration with enhanced error handling and recovery."""
    structured_logger = StructuredLogger(job_config.job_name)
    structured_logger.info("Starting incremental data migration process with recovery support")
    
    # Initialize migration processor with enhanced retry handler
    retry_handler = ConnectionRetryHandler(max_retries=3, base_delay=2.0, max_delay=60.0)
    migration_processor = DataMigrationProcessor(spark, retry_handler)
    
    migration_results = {}
    
    for table_name in job_config.tables:
        structured_logger.info("Starting incremental migration for table", table_name=table_name)
        
        # Start monitoring for this table
        table_metrics = None
        if performance_monitor:
            table_metrics = performance_monitor.start_table_processing(table_name)
        
        try:
            # Attempt incremental migration with error handling
            progress = migration_processor.perform_incremental_load_migration(
                job_config.source_connection,
                job_config.target_connection,
                table_name
            )
            migration_results[table_name] = progress
            
            # Complete monitoring for successful table
            if performance_monitor and progress.status == 'completed':
                performance_monitor.complete_table_processing(
                    table_name, 
                    progress.processed_rows, 
                    getattr(progress, 'bytes_processed', 0)
                )
            elif performance_monitor and progress.status == 'failed':
                performance_monitor.fail_table_processing(
                    table_name, 
                    getattr(progress, 'error_message', 'Migration failed')
                )
            
        except Exception as e:
            # Handle migration error
            error_info = error_recovery_manager.handle_data_processing_error(
                e, table_name, "incremental_migration", {
                    'source_engine': job_config.source_connection.engine_type,
                    'target_engine': job_config.target_connection.engine_type,
                    'job_name': job_config.job_name
                }
            )
            
            # Record error in monitoring
            if performance_monitor:
                performance_monitor.record_error("data_processing", "incremental_migration", str(e))
                performance_monitor.fail_table_processing(table_name, str(e))
            
            # Create failed progress object
            failed_progress = IncrementalLoadProgress(table_name, "unknown")
            failed_progress.status = 'failed'
            failed_progress.error_message = str(e)
            failed_progress.end_time = time.time()
            migration_results[table_name] = failed_progress
            
            # Attempt recovery if error is retryable
            if error_recovery_manager.attempt_graceful_recovery(error_info, {
                'table_name': table_name,
                'migration_type': 'incremental'
            }):
                logger.info(f"Retrying incremental migration for table {table_name} after recovery")
                try:
                    progress = migration_processor.perform_incremental_load_migration(
                        job_config.source_connection,
                        job_config.target_connection,
                        table_name
                    )
                    migration_results[table_name] = progress
                    logger.info(f"Incremental migration succeeded for table {table_name} after recovery")
                    
                except Exception as retry_error:
                    logger.error(f"Incremental migration failed for table {table_name} after recovery: {str(retry_error)}")
                    failed_progress.error_message = f"Failed after recovery: {str(retry_error)}"
            else:
                logger.error(f"Incremental migration failed for table {table_name}, no recovery possible")
    
    # Log summary
    successful_tables = [name for name, progress in migration_results.items() if progress.status == 'completed']
    failed_tables = [name for name, progress in migration_results.items() if progress.status == 'failed']
    
    logger.info(
        f"Incremental migration completed: {len(successful_tables)} successful, "
        f"{len(failed_tables)} failed out of {len(job_config.tables)} total tables"
    )
    
    if failed_tables:
        logger.warning(f"Failed tables: {', '.join(failed_tables)}")
    
    return migration_results