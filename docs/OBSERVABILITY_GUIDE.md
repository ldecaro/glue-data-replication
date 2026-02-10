# AWS Glue Data Replication - Observability Guide

This guide covers the comprehensive observability features built into the AWS Glue Data Replication system, including logging, monitoring, alerting, and troubleshooting capabilities.

## Overview

The CloudFormation template automatically creates a complete observability stack that includes:

- **CloudWatch Log Groups**: Structured logging for job execution, output, and errors
- **CloudWatch Dashboard**: Real-time monitoring and visualization
- **CloudWatch Alarms**: Proactive alerting for failures and performance issues
- **Log Insights Queries**: Pre-built queries for log analysis
- **SNS Notifications**: Email alerts for critical events

## CloudWatch Log Groups

### 1. Main Job Logs (`/aws-glue/jobs/{JobName}`)
**Purpose**: General job execution logs, system messages, and Spark driver logs
**Retention**: Configurable via `LogRetentionDays` parameter (default: 30 days)
**Content**:
- Job startup and initialization
- Spark driver logs and metrics
- Connection establishment logs
- Job completion status

### 2. Output Logs (`/aws-glue/jobs/output/{JobName}`)
**Purpose**: Application-specific logs from your PySpark script
**Retention**: Configurable via `LogRetentionDays` parameter (default: 30 days)
**Content**:
- Data processing progress
- Record counts and statistics
- Custom application logs
- Performance metrics

### 3. Error Logs (`/aws-glue/jobs/error/{JobName}`)
**Purpose**: Error messages, exceptions, and failure details
**Retention**: Configurable via `ErrorLogRetentionDays` parameter (default: 90 days)
**Content**:
- Exception stack traces
- Connection failures
- Data validation errors
- Critical system errors

## CloudWatch Dashboard

### Dashboard Components

The automatically created dashboard includes four main widgets:

#### 1. Task Metrics Widget
**Metrics Displayed**:
- `glue.driver.aggregate.numCompletedTasks`: Successfully completed tasks
- `glue.driver.aggregate.numFailedTasks`: Failed tasks
- `glue.driver.aggregate.numKilledTasks`: Killed/cancelled tasks

**Use Case**: Monitor job progress and identify task-level failures

#### 2. Resource Utilization Widget
**Metrics Displayed**:
- `glue.driver.jvm.heap.usage`: JVM heap utilization percentage
- `glue.driver.jvm.heap.used`: JVM heap memory used (bytes)
- `glue.driver.system.cpuUtilization`: CPU utilization percentage

**Use Case**: Monitor resource consumption and identify performance bottlenecks

#### 3. I/O Metrics Widget
**Metrics Displayed**:
- `glue.driver.BlockManager.disk.diskSpaceUsed_MB`: Disk space usage
- `glue.driver.s3.filesystem.read_bytes`: S3 read throughput
- `glue.driver.s3.filesystem.write_bytes`: S3 write throughput

**Use Case**: Monitor data transfer rates and storage utilization

#### 4. Recent Errors Widget
**Content**: Log Insights query showing recent ERROR messages
**Use Case**: Quick identification of recent issues and failures

### Accessing the Dashboard

1. **Via CloudFormation Output**: Check the `CloudWatchDashboard` output for direct link
2. **Via AWS Console**: Navigate to CloudWatch → Dashboards → `{JobName}-glue-monitoring`
3. **Via CLI**:
   ```bash
   aws cloudwatch get-dashboard --dashboard-name {JobName}-glue-monitoring
   ```

## CloudWatch Alarms

### 1. Job Failure Alarm
**Metric**: `glue.driver.aggregate.numFailedTasks`
**Threshold**: ≥ 1 failed task
**Evaluation**: 1 period of 5 minutes
**Purpose**: Immediate notification when job tasks fail

### 2. High Memory Usage Alarm
**Metric**: `glue.driver.jvm.heap.usage`
**Threshold**: > 85% heap utilization
**Evaluation**: 2 consecutive periods of 5 minutes
**Purpose**: Early warning for memory pressure

### 3. Long Duration Alarm
**Metric**: `glue.driver.aggregate.elapsedTime`
**Threshold**: > 1 hour (3,600,000 milliseconds)
**Evaluation**: 1 period of 5 minutes
**Purpose**: Detect jobs running longer than expected

### Alarm Actions

When `AlarmNotificationEmail` parameter is provided:
- **AlarmActions**: Send email when alarm triggers
- **OKActions**: Send email when alarm returns to OK state
- **SNS Topic**: Automatically created for notifications

## Log Insights Queries

### Pre-built Saved Queries

#### 1. Error Analysis Query (`{JobName}-error-analysis`)
```sql
fields @timestamp, @message, @logStream
| filter @message like /ERROR/ or @message like /Exception/ or @message like /Failed/
| sort @timestamp desc
| limit 100
```
**Purpose**: Identify and analyze error patterns

#### 2. Performance Analysis Query (`{JobName}-performance-analysis`)
```sql
fields @timestamp, @message
| filter @message like /records processed/ or @message like /execution time/ or @message like /memory usage/
| sort @timestamp desc
| limit 50
```
**Purpose**: Monitor job performance metrics

#### 3. Data Flow Analysis Query (`{JobName}-data-flow-analysis`)
```sql
fields @timestamp, @message
| filter @message like /table/ or @message like /rows/ or @message like /processed/
| stats count() by bin(5m)
| sort @timestamp desc
```
**Purpose**: Analyze data processing patterns over time

### Custom Queries

#### Find Connection Issues
```sql
fields @timestamp, @message
| filter @message like /connection/ or @message like /timeout/ or @message like /refused/
| sort @timestamp desc
```

#### Monitor Data Volume
```sql
fields @timestamp, @message
| filter @message like /processed/ and @message like /records/
| parse @message "processed * records" as record_count
| stats sum(record_count) by bin(5m)
```

#### Track Job Phases
```sql
fields @timestamp, @message
| filter @message like /Starting/ or @message like /Completed/ or @message like /Processing/
| sort @timestamp asc
```

## Configuration Parameters

### Log Retention Configuration
```json
{
  "ParameterKey": "LogRetentionDays",
  "ParameterValue": "30"  // Standard logs retention
},
{
  "ParameterKey": "ErrorLogRetentionDays", 
  "ParameterValue": "90"  // Error logs retention (typically longer)
}
```

### Observability Feature Toggles
```json
{
  "ParameterKey": "EnableCloudWatchDashboard",
  "ParameterValue": "YES"  // Create monitoring dashboard
},
{
  "ParameterKey": "EnableCloudWatchAlarms",
  "ParameterValue": "YES"  // Create monitoring alarms
}
```

### Notification Configuration
```json
{
  "ParameterKey": "AlarmNotificationEmail",
  "ParameterValue": "admin@company.com"  // Email for alarm notifications
},
{
  "ParameterKey": "JobDurationThresholdMinutes",
  "ParameterValue": "60"  // Alarm threshold for long-running jobs
}
```

## Monitoring Best Practices

### 1. Log Analysis
- **Regular Review**: Check error logs weekly for patterns
- **Retention Policy**: Keep error logs longer than standard logs
- **Log Levels**: Use appropriate log levels in your PySpark script
- **Structured Logging**: Include job context in log messages

### 2. Dashboard Usage
- **Real-time Monitoring**: Use dashboard during job execution
- **Historical Analysis**: Review trends over time
- **Resource Planning**: Use utilization metrics for capacity planning
- **Performance Tuning**: Identify bottlenecks from I/O metrics

### 3. Alarm Configuration
- **Threshold Tuning**: Adjust thresholds based on job characteristics
- **Notification Routing**: Use different emails for different severity levels
- **Escalation**: Configure multiple notification channels
- **Testing**: Regularly test alarm notifications

### 4. Cost Optimization
- **Log Retention**: Balance retention needs with storage costs
- **Dashboard Refresh**: Use appropriate refresh intervals
- **Alarm Evaluation**: Optimize evaluation periods to reduce costs
- **Metric Filtering**: Focus on essential metrics

## Troubleshooting Guide

### Common Issues and Solutions

#### High Memory Usage Alarms
**Symptoms**: Memory alarm triggering frequently
**Investigation**:
```sql
fields @timestamp, @message
| filter @message like /heap/ or @message like /memory/ or @message like /GC/
| sort @timestamp desc
```
**Solutions**:
- Increase worker memory (`WorkerType`: G.1X → G.2X)
- Optimize data processing logic
- Implement data partitioning
- Add more workers (`NumberOfWorkers`)

#### Job Failure Analysis
**Symptoms**: Job failure alarms or failed task metrics
**Investigation**:
```sql
fields @timestamp, @message, @logStream
| filter @message like /ERROR/ or @message like /FAILED/
| sort @timestamp desc
| limit 20
```
**Solutions**:
- Check connection strings and credentials
- Verify network connectivity
- Review JDBC driver compatibility
- Validate source/target schemas

#### Performance Issues
**Symptoms**: Long duration alarms or slow processing
**Investigation**:
```sql
fields @timestamp, @message
| filter @message like /stage/ or @message like /task/ or @message like /shuffle/
| stats count() by bin(1m)
```
**Solutions**:
- Optimize Spark configuration
- Implement data caching strategies
- Review join operations
- Consider data partitioning

#### Connection Problems
**Symptoms**: Connection timeout errors in logs
**Investigation**:
```sql
fields @timestamp, @message
| filter @message like /connection/ or @message like /timeout/
| sort @timestamp desc
```
**Solutions**:
- Verify security group rules
- Check VPC endpoint configuration
- Validate connection strings
- Test network connectivity

## Advanced Monitoring

### Custom Metrics
Add custom metrics to your PySpark script:
```python
import boto3

cloudwatch = boto3.client('cloudwatch')

# Custom metric example
cloudwatch.put_metric_data(
    Namespace='GlueJob/DataReplication',
    MetricData=[
        {
            'MetricName': 'RecordsProcessed',
            'Value': record_count,
            'Unit': 'Count',
            'Dimensions': [
                {
                    'Name': 'JobName',
                    'Value': job_name
                },
                {
                    'Name': 'TableName', 
                    'Value': table_name
                }
            ]
        }
    ]
)
```

### Log Correlation
Correlate logs across different log groups:
```sql
fields @timestamp, @message, @logStream
| filter @requestId = "specific-request-id"
| sort @timestamp asc
```

### Performance Baselines
Establish performance baselines:
- Record typical job duration
- Monitor normal resource utilization
- Track data processing rates
- Document expected error rates

## Integration with Other AWS Services

### AWS X-Ray (Optional)
Enable distributed tracing for detailed performance analysis:
```python
# Add to Glue job arguments
'--enable-continuous-cloudwatch-log': 'true',
'--enable-metrics': 'true',
'--enable-observability-metrics': 'true'
```

### AWS CloudTrail
Monitor API calls and configuration changes:
- Track Glue job modifications
- Monitor IAM role changes
- Audit S3 access patterns

### AWS Config
Monitor configuration compliance:
- Glue job configuration drift
- IAM role policy changes
- S3 bucket policy modifications

## Security Considerations

### Log Data Protection
- **Sensitive Data**: Avoid logging passwords or sensitive information
- **Encryption**: Enable CloudWatch Logs encryption
- **Access Control**: Use IAM policies to restrict log access
- **Retention**: Set appropriate retention periods for compliance

### Monitoring Access
- **Principle of Least Privilege**: Grant minimal required permissions
- **Role-based Access**: Use IAM roles for different user types
- **Audit Trail**: Monitor who accesses monitoring data
- **Cross-account Access**: Carefully control cross-account monitoring

## Related Documentation

- **[Progress Tracking Guide](PROGRESS_TRACKING_GUIDE.md)**: Real-time progress monitoring, metrics configuration, and troubleshooting
- **[Performance Monitoring Guide](PERFORMANCE_MONITORING_GUIDE.md)**: CloudWatch dashboard setup and performance optimization
- **[Error Handling Guide](ERROR_HANDLING_GUIDE.md)**: Error classification and recovery mechanisms

This comprehensive observability setup ensures you have full visibility into your Glue data replication jobs, enabling proactive monitoring, quick troubleshooting, and continuous optimization.