# AWS Glue Data Replication - Deployment Guide

This guide covers the complete deployment process for the AWS Glue data replication solution with its new modular architecture, including fixes for mock connection issues and VPC endpoint configuration.

## Prerequisites

- AWS CLI configured with appropriate permissions
- S3 bucket for hosting CloudFormation templates and Glue scripts
- VPC with private subnets (if using cross-VPC configuration)
- Database connection details

## Template Size Limitation

**Important**: The CloudFormation template exceeds the 51,200 character limit for direct uploads and must be hosted in S3.

## Enhanced Deployment Process

The deployment process has been streamlined with automatic S3 asset management:

### Key Features
- **Single Command Deployment**: Complete deployment in one command
- **Automatic S3 Management**: Creates bucket and uploads all assets automatically
- **Parameter Processing**: Automatically updates parameter files with correct S3 paths
- **Template Hosting**: Uses S3-hosted CloudFormation template (required for large templates)
- **Comprehensive Validation**: Validates AWS CLI, bucket format, and modular structure

### Available Scripts
- `deploy.sh`: Enhanced deployment script with automatic S3 uploads and validation
- `infrastructure/scripts/upload-modular-assets.sh`: Manual upload of modular structure (optional)
- `infrastructure/scripts/upload-assets.sh`: Legacy single-file upload script

## Deployment Steps

### 1. Deploy with Automatic Asset Upload (Recommended)

```bash
# For new stack deployment
./deploy.sh -s your-glue-replication-stack -b [your-bucket-name] -p examples/your-parameters.json

# For stack updates
./deploy.sh -s your-glue-replication-stack -b [your-bucket-name] -p examples/your-parameters.json --update

# Validate template only
./deploy.sh -s your-glue-replication-stack -b [your-bucket-name] -p examples/your-parameters.json --validate-only

# Dry run to see what would be deployed
./deploy.sh -s your-glue-replication-stack -b [your-bucket-name] -p examples/your-parameters.json --dry-run
```

The deploy script automatically:
- Creates S3 bucket if it doesn't exist
- Uploads CloudFormation template to S3
- Uploads complete modular Glue job structure
- Updates parameter file with correct S3 paths
- Deploys using S3-hosted template

### 2. Manual Asset Upload (Optional)

If you prefer to upload assets separately:

#### Option A: Upload Complete Modular Structure (Recommended)
```bash
# Upload the complete modular Glue job structure
./infrastructure/scripts/upload-modular-assets.sh [your-bucket-name] --include-drivers
```

This script uploads:
- Main entry point: `src/glue_job/main.py`
- All supporting modules in `src/glue_job/`
- Configuration files from `config/`
- JDBC drivers (if `--include-drivers` is specified)

#### Option B: Manual Upload
```bash
# Upload the main Glue script
aws s3 cp src/glue_job/main.py s3://[your-bucket-name]/src/glue_job/main.py

# Upload all supporting modules
aws s3 sync src/ s3://[your-bucket-name]/src/ --exclude "*.pyc" --exclude "__pycache__/*"

# Upload configuration files
aws s3 sync config/ s3://[your-bucket-name]/config/
```

#### Option C: Legacy Single-File Upload (Deprecated)
```bash
# For backward compatibility only - not recommended
./infrastructure/scripts/upload-assets.sh [your-bucket-name]
```

### 3. Upload JDBC Drivers to S3

```bash
# Example for SQL Server JDBC driver
aws s3 cp jdbc-drivers/sqlserver/mssql-jdbc-12.2.0.jre11.jar s3://[your-bucket-name]/jdbc-drivers/sqlserver/12.2.0.jre11/mssql-jdbc-12.2.0.jre11.jar
```
For more details, please check [Driver Download and Storage.](docs/DATABASE_CONFIGURATION_GUIDE.md#driver-download-and-storage).

### 3. Configure Glue Connections and AWS Secrets Manager (Optional)

The solution supports AWS Glue Connections for enhanced security and centralized connection management. When using Glue Connections, database credentials are automatically stored in AWS Secrets Manager for enhanced security.

#### Glue Connection Configuration Options

**Option 1: Create New Glue Connections with Secrets Manager Integration**
```json
{
  "ParameterKey": "CreateSourceConnection",
  "ParameterValue": "true"
},
{
  "ParameterKey": "CreateTargetConnection", 
  "ParameterValue": "true"
}
```

**Option 2: Use Existing Glue Connections**
```json
{
  "ParameterKey": "UseSourceConnection",
  "ParameterValue": "my-existing-oracle-connection"
},
{
  "ParameterKey": "UseTargetConnection",
  "ParameterValue": "my-existing-postgres-connection"
}
```

**Option 3: Mixed Configuration**
```json
{
  "ParameterKey": "CreateSourceConnection",
  "ParameterValue": "true"
},
{
  "ParameterKey": "UseTargetConnection",
  "ParameterValue": "existing-target-connection"
}
```

#### AWS Secrets Manager Integration

When `CreateSourceConnection=true` or `CreateTargetConnection=true`, the system automatically:

1. **Creates AWS Secrets Manager secrets** at `/aws-glue/{connection-name}`
2. **Stores credentials securely** in JSON format: `{"username": "user", "password": "pass"}`
3. **Configures Glue Connections** to reference secrets instead of storing credentials directly
4. **Provides enhanced security** through encrypted credential storage and fine-grained access control

#### Required IAM Permissions

The CloudFormation template automatically includes the necessary IAM permissions for Secrets Manager operations:

```json
{
  "Effect": "Allow",
  "Action": [
    "secretsmanager:CreateSecret",
    "secretsmanager:GetSecretValue",
    "secretsmanager:PutSecretValue", 
    "secretsmanager:DescribeSecret"
  ],
  "Resource": "arn:aws:secretsmanager:*:*:secret:/aws-glue/*"
}
```

#### Parameter Validation Rules

- `CreateSourceConnection` and `UseSourceConnection` are mutually exclusive
- `CreateTargetConnection` and `UseTargetConnection` are mutually exclusive  
- Glue Connection parameters only apply to JDBC engines (`oracle`, `sqlserver`, `postgresql`, `db2`)
- When source or target engine is `iceberg`, Glue Connection parameters are ignored with warnings

### 4. Configure Parameters

Update your parameter file (e.g., `examples/sqlserver-to-sqlserver-parameters.json`) with your specific values:

```json
[
  {
    "ParameterKey": "JobName",
    "ParameterValue": "your-job-name"
  },
  {
    "ParameterKey": "SourceJdbcDriverS3Path",
    "ParameterValue": "s3://[your-bucket-name]/jdbc-drivers/sqlserver/12.2.0.jre11/mssql-jdbc-12.2.0.jre11.jar"
  },
  {
    "ParameterKey": "CreateSourceGlueVpcEndpoint",
    "ParameterValue": "YES"
  }
]
```

**Note**: The `GlueJobScriptS3Path` parameter will be automatically updated by the deploy script to point to the correct S3 location (`s3://bucket/src/glue_job/main.py`).
```

### 5. Deploy CloudFormation Stack

The enhanced deploy script handles the complete deployment process:

```bash
# Deploy new stack
./deploy.sh -s your-glue-replication-stack -b [your-bucket-name] -p examples/your-parameters.json

# Update existing stack
./deploy.sh -s your-glue-replication-stack -b [your-bucket-name] -p examples/your-parameters.json --update
```

**Advanced Options:**
```bash
# Skip upload if assets already in S3
./deploy.sh -s your-stack -b your-bucket -p your-params.json --skip-upload

# Validate template only
./deploy.sh -s your-stack -b your-bucket -p your-params.json --validate-only

# Dry run to see what would be deployed
./deploy.sh -s your-stack -b your-bucket -p your-params.json --dry-run
```

### 6. Verify Deployment

#### Check Stack Status:
```bash
aws cloudformation describe-stacks --stack-name your-glue-replication-stack
```

#### Verify VPC Endpoints (if using private subnets):
```bash
aws ec2 describe-vpc-endpoints \
  --filters "Name=service-name,Values=com.amazonaws.us-east-1.glue" \
           "Name=vpc-id,Values=your-vpc-id"
```

#### Verify Glue Job Creation:
```bash
aws glue get-job --job-name your-job-name
```

### 6. Test Glue Job

```bash
aws glue start-job-run --job-name your-job-name
```

## Configuration Options

### VPC Endpoint Configuration

For jobs running in private subnets, enable VPC endpoints:

- **CreateSourceGlueVpcEndpoint**: `YES` (recommended for private subnets)
- **CreateTargetGlueVpcEndpoint**: `YES` (if using separate target VPC)
- **CreateSourceS3VpcEndpoint**: `YES` (if JDBC drivers are in private S3 access)
- **CreateTargetS3VpcEndpoint**: `YES` (if using separate target VPC)

### Security Group Requirements

Ensure your security groups allow:
- **Port 443 (HTTPS)**: For VPC endpoint access to AWS services
- **Database ports**: For database connectivity (e.g., 1433 for SQL Server, 5432 for PostgreSQL)

Example security group rule for VPC endpoints:
```yaml
- IpProtocol: tcp
  FromPort: 443
  ToPort: 443
  CidrIp: 10.0.0.0/16  # Your VPC CIDR
```

## Performance Configuration

The AWS Glue Data Replication solution includes advanced performance optimization features designed to handle large-scale migrations efficiently. This section covers when and how to use these features.

### Overview of Performance Features

- **Adaptive Counting Strategies**: Eliminate redundant source reads for large datasets
- **Real-Time Progress Tracking**: Monitor migration progress with configurable intervals
- **Detailed CloudWatch Metrics**: Track migration phases and processing rates
- **Optimized for Scale**: Handle datasets from megabytes to terabytes efficiently

### Performance Parameters

#### CountingStrategy

Controls how row counts are obtained during migration.

**Values**: `immediate`, `deferred`, `auto` (default: `auto`)

```json
{
  "ParameterKey": "CountingStrategy",
  "ParameterValue": "auto"
}
```

**Strategy Descriptions**:

- **immediate**: Counts rows from source using SQL COUNT(*) before writing
  - Uses fast SQL `SELECT COUNT(*)` query (database-optimized)
  - Progress percentage available from the start
  - Minimal performance overhead for all dataset sizes
  
- **deferred**: Counts rows from target after writing
  - Reads source data only once
  - Progress percentage available after write completes
  - Use only when SQL COUNT(*) is not available or fails
  
- **auto** (recommended): Uses SQL COUNT(*) for all sources
  - Executes fast SQL `SELECT COUNT(*)` query before write
  - JDBC databases: Uses database statistics and indexes
  - Iceberg tables: Reads from manifest metadata (no data scan)
  - Progress percentage available from the start for all migrations
  - Falls back to deferred counting only if SQL COUNT fails

**Counting Strategy Impact on Monitoring**:

With the SQL COUNT(*) optimization, all counting strategies now provide full monitoring visibility:

| Metric | Immediate/Auto Counting | Deferred Counting (fallback) |
|--------|------------------------|------------------------------|
| **Rows Processed** | ✅ Available during write | ✅ Available during write |
| **Rows/Second** | ✅ Available during write | ✅ Available during write |
| **Progress Percentage** | ✅ Available from start | ❌ Shows 0% until write completes |
| **ETA (Time Remaining)** | ✅ Calculated accurately | ❌ Cannot calculate until end |
| **Final Row Count** | ✅ Same | ✅ Same |

**Key Benefit**: With the SQL COUNT(*) optimization, `auto` mode now uses immediate counting for all dataset sizes. SQL COUNT(*) queries are fast because:
- **JDBC databases**: Execute count server-side using indexes and statistics
- **Iceberg tables**: Read from manifest metadata, not data files

This means progress percentage and ETA are now available from the start for all migrations, regardless of dataset size.

**Recommendation**: Use `auto` (default) for all migrations. Deferred counting is only used as a fallback if SQL COUNT(*) fails (e.g., connection issues).

#### ProgressUpdateInterval

Controls how frequently progress metrics are emitted to CloudWatch and logs.

**Values**: Integer (seconds), Default: `60`

```json
{
  "ParameterKey": "ProgressUpdateInterval",
  "ParameterValue": "60"
}
```

**Recommended Values**:
- **30 seconds**: High-frequency updates for short jobs (<30 minutes)
- **60 seconds**: Standard frequency for most jobs (default)
- **120+ seconds**: Reduced frequency for very long jobs (>2 hours)

**Considerations**:
- More frequent updates provide better visibility but increase CloudWatch API calls
- Does not impact migration performance
- Affects CloudWatch costs (minimal impact)

#### BatchSizeThreshold

Threshold parameter for backward compatibility. With the SQL COUNT(*) optimization, this parameter is no longer used for strategy selection.

**Values**: Integer (rows), Default: `1000000`

```json
{
  "ParameterKey": "BatchSizeThreshold",
  "ParameterValue": "1000000"
}
```

**Note**: This parameter is retained for backward compatibility but is no longer used for counting strategy selection. The `auto` mode now always uses SQL COUNT(*) regardless of dataset size, since SQL COUNT queries are fast for all sizes.

#### EnableDetailedMetrics

Enables comprehensive CloudWatch metrics for migration monitoring.

**Values**: `true`, `false`, Default: `true`

```json
{
  "ParameterKey": "EnableDetailedMetrics",
  "ParameterValue": "true"
}
```

**Metrics Published** (when enabled):
- Migration progress metrics (rows processed, processing rate, progress percentage)
- Counting strategy metrics (strategy type, count duration, row count)
- Migration phase metrics (read duration, write duration, count duration)
- Per-table migration status

**When to Disable**:
- Cost-sensitive environments where metric costs are a concern
- Development/testing where detailed metrics aren't needed
- High-frequency job runs where metric costs accumulate

### When to Use Each Counting Strategy

With the SQL COUNT(*) optimization, the `auto` strategy is now recommended for all use cases. The following guidance is provided for special scenarios:

#### Use Auto Strategy (Recommended for All Cases):

The `auto` strategy now uses SQL COUNT(*) for all sources, providing:
- Fast row counting using database statistics (JDBC) or manifest metadata (Iceberg)
- Progress percentage available from the start
- No performance penalty regardless of dataset size
- Automatic fallback to deferred counting if SQL COUNT fails

**Example Configuration**:
```json
{
  "ParameterKey": "CountingStrategy",
  "ParameterValue": "auto"
},
{
  "ParameterKey": "WorkerType",
  "ParameterValue": "G.2X"
},
{
  "ParameterKey": "NumberOfWorkers",
  "ParameterValue": "5"
},
{
  "ParameterKey": "ProgressUpdateInterval",
  "ParameterValue": "60"
}
```

**Expected Behavior**:
- SQL COUNT(*) executes in <1 second (uses database/Iceberg metadata)
- Progress percentage shown from start
- Optimal performance for all dataset sizes

#### Use Immediate Counting When:

- **Explicit Control**: You want to ensure SQL COUNT is always used
- **Debugging**: Troubleshooting counting-related issues
- **Validation**: Verifying row counts before write

**Example Configuration**:
```json
{
  "ParameterKey": "CountingStrategy",
  "ParameterValue": "immediate"
},
{
  "ParameterKey": "WorkerType",
  "ParameterValue": "G.1X"
},
{
  "ParameterKey": "NumberOfWorkers",
  "ParameterValue": "2"
},
{
  "ParameterKey": "ProgressUpdateInterval",
  "ParameterValue": "30"
}
```

**Expected Behavior**:
- Row count available immediately via SQL COUNT(*)
- Progress percentage shown from start
- Same performance as auto mode

#### Use Deferred Counting When:

- **SQL COUNT Unavailable**: Source database doesn't support efficient COUNT(*)
- **Connection Issues**: SQL COUNT consistently fails due to connection problems
- **Legacy Compatibility**: Maintaining behavior from previous versions

**Example Configuration**:
```json
{
  "ParameterKey": "CountingStrategy",
  "ParameterValue": "deferred"
},
{
  "ParameterKey": "WorkerType",
  "ParameterValue": "G.2X"
},
{
  "ParameterKey": "NumberOfWorkers",
  "ParameterValue": "10"
},
{
  "ParameterKey": "ProgressUpdateInterval",
  "ParameterValue": "60"
}
```

**Expected Behavior**:
- No initial row count (shows as "unknown")
- Progress updates show rows processed but not percentage
- Final row count available after write completes

### Guidance for Large Dataset Migrations

When migrating large datasets (1TB+), the SQL COUNT(*) optimization ensures optimal performance without special configuration:

#### 1. Use Auto Counting (Default)

```json
{
  "ParameterKey": "CountingStrategy",
  "ParameterValue": "auto"
}
```

SQL COUNT(*) is fast for all dataset sizes because it uses database statistics (JDBC) or manifest metadata (Iceberg), not data scanning.

#### 2. Increase Worker Resources

```json
{
  "ParameterKey": "WorkerType",
  "ParameterValue": "G.2X"
},
{
  "ParameterKey": "NumberOfWorkers",
  "ParameterValue": "10"
}
```

Use G.2X workers with 10+ workers for parallel processing of large datasets.

#### 3. Adjust Progress Update Interval

```json
{
  "ParameterKey": "ProgressUpdateInterval",
  "ParameterValue": "120"
}
```

For long-running jobs (>2 hours), reduce update frequency to minimize CloudWatch API calls.

#### 4. Enable Detailed Metrics

```json
{
  "ParameterKey": "EnableDetailedMetrics",
  "ParameterValue": "true"
}
```

Monitor migration phases to identify bottlenecks and optimize future runs.

#### 5. Increase Timeout

```json
{
  "ParameterKey": "Timeout",
  "ParameterValue": "2880"
}
```

Set timeout to 2880 minutes (48 hours) or higher for very large datasets.

**Performance Characteristics with SQL COUNT(*)**:

| Dataset Size | SQL COUNT Time | Progress Visibility |
|--------------|----------------|---------------------|
| 1M rows | <1 second | Full from start |
| 100M rows | <1 second | Full from start |
| 1B rows | <1 second | Full from start |

SQL COUNT(*) performance is independent of dataset size because it reads from database statistics or Iceberg metadata, not the actual data.

### Progress Tracking Configuration

#### Understanding Progress Updates

Progress updates are emitted at regular intervals during data transfer:

```json
{
  "event": "migration_progress",
  "table_name": "transactions",
  "load_type": "full",
  "rows_processed": 1500000,
  "progress_percentage": 75.0,
  "rows_per_second": 25000,
  "eta_seconds": 60
}
```

**Key Fields**:
- `rows_processed`: Number of rows written so far
- `progress_percentage`: Completion percentage (if total known)
- `rows_per_second`: Current processing rate
- `eta_seconds`: Estimated time to completion (if total known)

#### Configuring Update Frequency

Balance between visibility and cost:

**High Frequency (30 seconds)**:
- Better visibility for short jobs
- More CloudWatch API calls
- Recommended for jobs <30 minutes

**Standard Frequency (60 seconds)**:
- Good balance for most jobs
- Default setting
- Recommended for jobs 30 minutes - 2 hours

**Low Frequency (120+ seconds)**:
- Reduced CloudWatch costs
- Sufficient for long jobs
- Recommended for jobs >2 hours

#### Monitoring Progress in CloudWatch

1. Navigate to CloudWatch Metrics
2. Select namespace: `AWS/Glue/DataReplication`
3. View metrics by dimension:
   - `table_name`: Per-table metrics
   - `load_type`: Full vs incremental load metrics
   - `strategy_type`: Counting strategy performance

### Example Configurations

#### Example 1: Large Oracle to PostgreSQL Migration

**Scenario**: Migrating 500GB of data across 10 tables, largest table has 50 million rows

```json
{
  "ParameterKey": "CountingStrategy",
  "ParameterValue": "auto"
},
{
  "ParameterKey": "ProgressUpdateInterval",
  "ParameterValue": "60"
},
{
  "ParameterKey": "EnableDetailedMetrics",
  "ParameterValue": "true"
},
{
  "ParameterKey": "WorkerType",
  "ParameterValue": "G.2X"
},
{
  "ParameterKey": "NumberOfWorkers",
  "ParameterValue": "10"
},
{
  "ParameterKey": "Timeout",
  "ParameterValue": "2880"
}
```

**Expected Results**:
- SQL COUNT(*) completes in <1 second per table
- Progress percentage available from start for all tables
- Progress updates every 60 seconds
- Detailed metrics for performance analysis

#### Example 2: Small SQL Server to PostgreSQL Migration

**Scenario**: Migrating 5GB of data across 20 tables, largest table has 500,000 rows

```json
{
  "ParameterKey": "CountingStrategy",
  "ParameterValue": "auto"
},
{
  "ParameterKey": "ProgressUpdateInterval",
  "ParameterValue": "30"
},
{
  "ParameterKey": "EnableDetailedMetrics",
  "ParameterValue": "true"
},
{
  "ParameterKey": "WorkerType",
  "ParameterValue": "G.1X"
},
{
  "ParameterKey": "NumberOfWorkers",
  "ParameterValue": "2"
}
```

**Expected Results**:
- Progress percentage available from start
- Frequent updates for better visibility
- Cost-effective worker configuration

#### Example 3: Mixed Workload Migration

**Scenario**: Migrating 100 tables of varying sizes (1MB to 100GB)

```json
{
  "ParameterKey": "CountingStrategy",
  "ParameterValue": "auto"
},
{
  "ParameterKey": "ProgressUpdateInterval",
  "ParameterValue": "60"
},
{
  "ParameterKey": "EnableDetailedMetrics",
  "ParameterValue": "true"
},
{
  "ParameterKey": "WorkerType",
  "ParameterValue": "G.2X"
},
{
  "ParameterKey": "NumberOfWorkers",
  "ParameterValue": "5"
}
```

**Expected Results**:
- SQL COUNT(*) used for all tables (fast regardless of size)
- Progress percentage available from start for all tables
- Balanced performance across workload

### Performance Monitoring

#### CloudWatch Metrics

When `EnableDetailedMetrics` is enabled, the following metrics are published:

**Migration Progress Metrics**:
- `RowsProcessed`: Number of rows processed
- `RowsPerSecond`: Processing rate
- `ProgressPercentage`: Completion percentage

**Counting Strategy Metrics**:
- `CountingStrategyType`: Strategy used (immediate/deferred)
- `CountDuration`: Time taken to count rows
- `RowCount`: Total rows counted

**Migration Phase Metrics**:
- `ReadDuration`: Time spent reading from source
- `WriteDuration`: Time spent writing to target
- `CountDuration`: Time spent counting rows
- `MigrationStatus`: Overall status (success/failed/in_progress)

#### Structured Logs

All performance-related events are logged with structured format:

**Migration Start**:
```json
{
  "event": "migration_start",
  "table_name": "transactions",
  "source_engine": "oracle",
  "target_engine": "postgresql",
  "counting_strategy": "deferred"
}
```

**Progress Update**:
```json
{
  "event": "migration_progress",
  "table_name": "transactions",
  "rows_processed": 1500000,
  "rows_per_second": 25000,
  "progress_percentage": 75.0
}
```

**Migration Complete**:
```json
{
  "event": "migration_complete",
  "table_name": "transactions",
  "total_rows": 2000000,
  "duration_seconds": 80,
  "rows_per_second": 25000
}
```

### Cost Considerations

#### CloudWatch Metrics Costs

With `EnableDetailedMetrics: true` and default settings:
- ~3-5 metrics per progress update
- Updates every 60 seconds
- For a 1-hour job: ~180-300 metric data points
- Estimated cost: $0.01-0.02 per job

#### Optimization Tips

1. **Reduce Update Frequency**: Increase `ProgressUpdateInterval` to 120+ seconds for long jobs
2. **Disable for Dev/Test**: Set `EnableDetailedMetrics: false` in non-production environments
3. **Use Auto Strategy**: Avoid manual configuration overhead and get optimal performance

#### Worker Costs

Performance configuration affects worker selection:

**Small Datasets** (G.1X, 2 workers):
- ~$0.44/hour per worker
- Total: ~$0.88/hour

**Large Datasets** (G.2X, 10 workers):
- ~$0.88/hour per worker
- Total: ~$8.80/hour

**Time Savings**: Deferred counting can reduce job duration by 66%, potentially saving more in worker costs than the configuration adds.

### Troubleshooting Performance Issues

#### Issue: Progress Percentage Shows 0% Throughout Migration

**Cause**: Using explicit deferred counting strategy or SQL COUNT(*) failed

**Solution**: 
1. Check logs for SQL COUNT failure messages
2. Verify database connectivity for COUNT queries
3. Use `auto` strategy (default) which uses SQL COUNT(*) with fallback
4. If SQL COUNT consistently fails, investigate connection issues

#### Issue: Migration Slower Than Expected

**Cause**: Network latency, database performance, or worker configuration

**Solution**: 
1. Check CloudWatch logs for phase durations
2. Verify network connectivity (VPC endpoints, security groups)
3. Increase worker count or use larger worker type
4. Check source/target database performance

#### Issue: High CloudWatch Costs

**Cause**: Too frequent progress updates

**Solution**: Increase `ProgressUpdateInterval` to 120+ seconds for long-running jobs

#### Issue: No Progress Updates in Logs

**Cause**: Progress tracking disabled or failing

**Solution**:
1. Verify `EnableDetailedMetrics` is set to `true`
2. Check CloudWatch logs for progress tracking errors
3. Verify IAM permissions for CloudWatch metrics

### Migration from Existing Configurations

If you have existing parameter files without performance parameters, add these defaults:

```json
{
  "ParameterKey": "CountingStrategy",
  "ParameterValue": "auto"
},
{
  "ParameterKey": "ProgressUpdateInterval",
  "ParameterValue": "60"
},
{
  "ParameterKey": "BatchSizeThreshold",
  "ParameterValue": "1000000"
},
{
  "ParameterKey": "EnableDetailedMetrics",
  "ParameterValue": "true"
}
```

**Backward Compatibility**: If these parameters are not specified, the system uses the defaults above, ensuring existing configurations continue to work.

### Additional Resources

- **Performance Examples**: See `examples/PERFORMANCE_CONFIGURATION_EXAMPLES.md` for detailed examples
- **Parameter Reference**: See `docs/PARAMETER_REFERENCE.md` for all parameter descriptions
- **Monitoring Guide**: See `docs/PERFORMANCE_MONITORING_GUIDE.md` for CloudWatch dashboard setup
- **API Reference**: See `docs/API_REFERENCE.md` for component documentation

## Troubleshooting

### Common Issues

1. **Template Size Error**: 
   - Error: `Member must have length less than or equal to 51200`
   - Solution: Use S3-hosted template URL instead of local file

2. **Glue API Timeout**:
   - Error: `Connect timeout on endpoint URL: "https://glue.us-east-1.amazonaws.com/"`
   - Solution: Enable Glue VPC endpoint (`CreateSourceGlueVpcEndpoint: YES`)

3. **Mock Subnet Error**:
   - Error: `The subnet ID 'subnet-mock' does not exist`
   - Solution: Ensure you're using the updated Glue script without mock bypasses

4. **IAM Permission Issues**:
   - Error: `User: ... is not authorized to perform: glue:GetConnection`
   - Solution: Verify IAM role has necessary Glue permissions

5. **AWS Secrets Manager Permission Issues** (when using Glue Connections):
   - Error: `User: ... is not authorized to perform: secretsmanager:CreateSecret`
   - Solution: Ensure the Glue job execution role has AWS Secrets Manager permissions
   - Required permissions: `secretsmanager:CreateSecret`, `secretsmanager:GetSecretValue`, `secretsmanager:PutSecretValue`, `secretsmanager:DescribeSecret`
   - Note: These permissions are automatically included in the CloudFormation template when using `CreateSourceConnection=true` or `CreateTargetConnection=true`

### Monitoring

Monitor your Glue job through:
- **CloudWatch Logs**: `/aws-glue/jobs/your-job-name`
- **CloudWatch Metrics**: Custom metrics published by the job
- **CloudWatch Dashboard**: Created automatically if enabled
- **CloudWatch Alarms**: For job failure notifications

## Cost Considerations

### VPC Endpoints
- **Interface endpoints**: ~$0.01/hour per endpoint per AZ + data processing charges
- **Gateway endpoints**: Free (S3, DynamoDB)

### Glue Job
- **Worker costs**: Based on worker type and number (e.g., G.1X workers)
- **Job duration**: Charged per second with 1-minute minimum

## Best Practices

1. **Use private subnets** with VPC endpoints for security
2. **Enable CloudWatch monitoring** for observability
3. **Set appropriate timeouts** based on data volume
4. **Use job bookmarks** for incremental processing (see [Bookmark Details](docs/BOOKMARK_DETAILS.md) for comprehensive guide)
5. **Test with small datasets** before full production runs
6. **Monitor costs** through AWS Cost Explorer

## Project Structure

```
aws-glue-data-replication/
├── src/
│   └── glue_job/                           # Modular Glue job components
│       ├── main.py                         # Entry point (replaces legacy monolithic script)
│       ├── config/                         # Configuration management
│       │   ├── job_config.py                # Job configuration dataclasses
│       │   ├── database_engines.py          # Database engine management
│       │   └── parsers.py                   # Configuration parsing
│       ├── database/                       # Database operations
│       │   ├── connection_manager.py        # Connection management
│       │   ├── schema_validator.py          # Schema validation
│       │   ├── migration.py                 # Data migration logic
│       │   └── incremental_detector.py      # Incremental processing
│       ├── storage/                        # Storage and bookmarks
│       │   ├── s3_bookmark.py               # S3 bookmark operations
│       │   └── bookmark_manager.py          # Bookmark lifecycle
│       ├── monitoring/                     # Observability
│       │   ├── logging.py                   # Structured logging
│       │   ├── metrics.py                   # CloudWatch metrics
│       │   └── progress.py                  # Progress tracking
│       ├── network/                        # Network and error handling
│       │   ├── error_handler.py             # Error classification
│       │   └── retry_handler.py             # Retry mechanisms
│       └── utils/                          # Utilities
│           └── s3_utils.py                  # S3 operations
├── infrastructure/
│   ├── cloudformation/                     # CloudFormation templates
│   │   └── glue-data-replication.yaml      # Main CloudFormation template
│   ├── scripts/                            # Deployment scripts
│   │   ├── upload-modular-assets.sh         # Upload modular structure
│   │   ├── upload-assets.sh                 # Legacy upload script
│   │   └── get-rds-network-info.sh          # Network configuration helper
│   └── iam/                                # IAM policies
├── tests/                                  # Test suites
├── docs/                                   # Documentation
├── examples/                               # Configuration examples
├── config/                                 # Static configuration files
└── jdbc-drivers/                           # JDBC driver files
```

## Modular Architecture Benefits

- **Maintainability**: Each module has a single responsibility
- **Testability**: Individual modules can be tested in isolation
- **Reusability**: Modules can be imported and used independently
- **Scalability**: New features can be added as separate modules
- **Debugging**: Easier to locate and fix issues in specific modules

## Support

For issues or questions:
1. Check the troubleshooting section above
2. Review CloudWatch logs for detailed error messages
3. Verify all prerequisites are met
4. Ensure S3 bucket permissions allow Glue service access

---

**Note**: Replace `[your-bucket-name]` with your actual S3 bucket name throughout all commands and configuration files.