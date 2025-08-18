# CloudFormation Parameter Reference

This document provides a comprehensive reference for all parameters available in the AWS Glue Data Replication CloudFormation template.

## Required Parameters

These parameters must be provided for every deployment:

### Job Configuration

| Parameter | Type | Description | Constraints | Example |
|-----------|------|-------------|-------------|---------|
| `JobName` | String | Unique name for the Glue data replication job instance | 1-255 characters, alphanumeric, hyphens, underscores only | `oracle-to-postgres-replication` |

### Database Engine Configuration

| Parameter | Type | Description | Allowed Values | Example |
|-----------|------|-------------|----------------|---------|
| `SourceEngineType` | String | Source database engine type | `oracle`, `sqlserver`, `postgresql`, `db2` | `oracle` |
| `TargetEngineType` | String | Target database engine type | `oracle`, `sqlserver`, `postgresql`, `db2` | `postgresql` |

### Database Connection Configuration

| Parameter | Type | Description | Constraints | Example |
|-----------|------|-------------|-------------|---------|
| `SourceDatabase` | String | Source database name | 1-128 characters, alphanumeric, hyphens, underscores, periods | `ORCL` |
| `TargetDatabase` | String | Target database name | 1-128 characters, alphanumeric, hyphens, underscores, periods | `target_db` |
| `SourceSchema` | String | Source database schema name | 1-128 characters, alphanumeric, hyphens, underscores, periods | `HR` |
| `TargetSchema` | String | Target database schema name | 1-128 characters, alphanumeric, hyphens, underscores, periods | `public` |
| `TableNames` | CommaDelimitedList | Comma-separated list of table names to replicate | Valid table names | `employees,departments,locations` |

### Database Credentials

| Parameter | Type | Description | Constraints | Example |
|-----------|------|-------------|-------------|---------|
| `SourceDbUser` | String | Source database username | 1-128 characters | `hr_user` |
| `SourceDbPassword` | String | Source database password | 1-128 characters, NoEcho=true | `********` |
| `TargetDbUser` | String | Target database username | 1-128 characters | `postgres` |
| `TargetDbPassword` | String | Target database password | 1-128 characters, NoEcho=true | `********` |

### JDBC Connection Strings

| Parameter | Type | Description | Format | Example |
|-----------|------|-------------|--------|---------|
| `SourceConnectionString` | String | JDBC connection string for source database | `jdbc:engine://host:port/database` or `jdbc:engine://host:port;databaseName=database` | `jdbc:oracle:thin:@host:1521:ORCL` |
| `TargetConnectionString` | String | JDBC connection string for target database | `jdbc:engine://host:port/database` or `jdbc:engine://host:port;databaseName=database` | `jdbc:postgresql://host:5432/target_db` |

### S3 Asset Paths

| Parameter | Type | Description | Format | Example |
|-----------|------|-------------|--------|---------|
| `SourceJdbcDriverS3Path` | String | S3 path to source database JDBC driver JAR file | `s3://bucket/path/driver.jar` | `s3://bucket/drivers/ojdbc11.jar` |
| `TargetJdbcDriverS3Path` | String | S3 path to target database JDBC driver JAR file | `s3://bucket/path/driver.jar` | `s3://bucket/drivers/postgresql.jar` |
| `GlueJobScriptS3Path` | String | S3 path to the PySpark Glue job script | `s3://bucket/path/script.py` | `s3://bucket/scripts/glue_data_replication.py` |

## Optional Parameters

These parameters have default values and can be omitted if the defaults are acceptable:

### Glue Job Configuration

| Parameter | Type | Description | Default | Range/Values | Example |
|-----------|------|-------------|---------|--------------|---------|
| `MaxRetries` | Number | Maximum number of retries for the Glue job | `1` | 0-10 | `3` |
| `Timeout` | Number | Timeout for the Glue job in minutes | `2880` | 1-2880 (48 hours) | `1440` |
| `MaxConcurrentRuns` | Number | Maximum number of concurrent runs for the Glue job | `1` | 1-1000 | `2` |
| `WorkerType` | String | Type of predefined worker for the Glue job | `G.1X` | `Standard`, `G.1X`, `G.2X`, `G.025X` | `G.2X` |
| `NumberOfWorkers` | Number | Number of workers for the Glue job | `2` | 2-299 | `4` |

### Source Network Configuration (Cross-VPC Access)

| Parameter | Type | Description | Default | Format | Example |
|-----------|------|-------------|---------|--------|---------|
| `SourceVpcId` | String | VPC ID where source database resides | `''` (empty) | `vpc-xxxxxxxx` or empty | `vpc-12345678` |
| `SourceSubnetIds` | CommaDelimitedList | Comma-separated list of subnet IDs for source database access | `''` (empty) | `subnet-xxxxxxxx,subnet-yyyyyyyy` | `subnet-123,subnet-456` |
| `SourceSecurityGroupIds` | CommaDelimitedList | Comma-separated list of security group IDs for source database access | `''` (empty) | `sg-xxxxxxxx,sg-yyyyyyyy` | `sg-12345678` |
| `CreateSourceS3VpcEndpoint` | String | Create S3 VPC endpoint in source VPC for private subnet access | `NO` | `YES`, `NO` | `YES` |

### Target Network Configuration (Cross-VPC Access)

| Parameter | Type | Description | Default | Format | Example |
|-----------|------|-------------|---------|--------|---------|
| `TargetVpcId` | String | VPC ID where target database resides | `''` (empty) | `vpc-xxxxxxxx` or empty | `vpc-87654321` |
| `TargetSubnetIds` | CommaDelimitedList | Comma-separated list of subnet IDs for target database access | `''` (empty) | `subnet-xxxxxxxx,subnet-yyyyyyyy` | `subnet-789,subnet-012` |
| `TargetSecurityGroupIds` | CommaDelimitedList | Comma-separated list of security group IDs for target database access | `''` (empty) | `sg-xxxxxxxx,sg-yyyyyyyy` | `sg-87654321` |
| `CreateTargetS3VpcEndpoint` | String | Create S3 VPC endpoint in target VPC for private subnet access | `NO` | `YES`, `NO` | `YES` |

### Observability Configuration (Optional)

| Parameter | Type | Description | Default | Range/Values | Example |
|-----------|------|-------------|---------|--------------|---------|
| `LogRetentionDays` | Number | Number of days to retain CloudWatch logs | `30` | 1, 3, 5, 7, 14, 30, 60, 90, 120, 150, 180, 365, 400, 545, 731, 1827, 3653 | `90` |
| `ErrorLogRetentionDays` | Number | Number of days to retain error logs (typically longer) | `90` | 1, 3, 5, 7, 14, 30, 60, 90, 120, 150, 180, 365, 400, 545, 731, 1827, 3653 | `180` |
| `EnableCloudWatchDashboard` | String | Create CloudWatch Dashboard for monitoring | `YES` | `YES`, `NO` | `YES` |
| `EnableCloudWatchAlarms` | String | Create CloudWatch Alarms for monitoring | `YES` | `YES`, `NO` | `YES` |
| `AlarmNotificationEmail` | String | Email address for alarm notifications (optional) | `''` (empty) | Valid email or empty | `admin@company.com` |
| `JobDurationThresholdMinutes` | Number | Threshold in minutes for long-running job alarm | `60` | 5-2880 | `120` |

## Parameter Dependencies

### Network Configuration Dependencies

When configuring cross-VPC access, the following dependencies apply:

1. **Source Network Configuration**: If `SourceVpcId` is specified, then `SourceSubnetIds` and `SourceSecurityGroupIds` are required.
2. **Target Network Configuration**: If `TargetVpcId` is specified, then `TargetSubnetIds` and `TargetSecurityGroupIds` are required.
3. **S3 VPC Endpoints**: `CreateSourceS3VpcEndpoint` and `CreateTargetS3VpcEndpoint` are only effective when the corresponding VPC configuration is provided.

### Worker Type Dependencies

When using `WorkerType = 'Standard'`, the `NumberOfWorkers` parameter behaves differently:
- For Standard workers: Can be set to any value between 2-299
- For G.1X, G.2X, G.025X workers: Must be between 2-299

## Parameter Validation

The CloudFormation template includes built-in validation for all parameters:

### String Pattern Validation
- Job names, database names, and schema names must contain only alphanumeric characters, hyphens, underscores, and periods
- JDBC connection strings must follow the proper format starting with `jdbc:`
- S3 paths must be valid S3 URIs
- VPC and subnet IDs must follow AWS resource ID patterns

### Numeric Range Validation
- All numeric parameters have defined minimum and maximum values
- Timeout values are limited to 48 hours (2880 minutes)
- Worker counts are limited to AWS Glue service limits

### Enum Validation
- Database engine types are restricted to supported values
- Worker types are limited to AWS Glue supported worker types
- Boolean-like parameters use YES/NO values for clarity

## Common Parameter Combinations

### Same VPC Deployment (Minimal Configuration)
```json
{
  "JobName": "my-replication-job",
  "SourceEngineType": "oracle",
  "TargetEngineType": "postgresql",
  // ... other required parameters
  // No network parameters needed
}
```

### Cross-VPC Source Database
```json
{
  "JobName": "cross-vpc-source-job",
  // ... other required parameters
  "SourceVpcId": "vpc-12345678",
  "SourceSubnetIds": "subnet-123,subnet-456",
  "SourceSecurityGroupIds": "sg-12345678",
  "CreateSourceS3VpcEndpoint": "YES"
}
```

### High-Performance Configuration
```json
{
  "JobName": "high-perf-job",
  // ... other required parameters
  "WorkerType": "G.2X",
  "NumberOfWorkers": "10",
  "MaxConcurrentRuns": "3",
  "Timeout": "1440"
}
```

### Development/Testing Configuration
```json
{
  "JobName": "dev-test-job",
  // ... other required parameters
  "WorkerType": "G.025X",
  "NumberOfWorkers": "2",
  "MaxRetries": "0",
  "Timeout": "60"
}
```

## CloudFormation Deployment Requirements

### IAM Capabilities
The CloudFormation template creates named IAM resources (roles with custom names), which requires the `CAPABILITY_NAMED_IAM` capability when deploying:

```bash
aws cloudformation create-stack \
  --stack-name my-data-replication \
  --template-body file://cloudformation/glue-data-replication.yaml \
  --parameters file://examples/parameters.json \
  --capabilities CAPABILITY_NAMED_IAM
```

**Important**: Use `CAPABILITY_NAMED_IAM`, not `CAPABILITY_IAM`, as the template creates:
- IAM Role: `${JobName}-glue-job-role`
- IAM Role: `${JobName}-route-table-lookup-role` (when VPC endpoints are created)

## Parameter Best Practices

### Security
- Always use strong passwords for database credentials
- Consider using AWS Secrets Manager for password management
- Restrict security groups to minimum required access
- Use private subnets with VPC endpoints when possible

### Performance
- Choose appropriate worker types based on data volume
- Increase worker count for large datasets
- Set reasonable timeout values based on expected job duration
- Use G.2X workers for memory-intensive operations

### Cost Optimization
- Use G.025X workers for small datasets or development
- Set appropriate timeout values to avoid runaway jobs
- Limit concurrent runs to control costs
- Monitor CloudWatch metrics to optimize worker allocation

### Reliability
- Set appropriate retry counts based on data criticality
- Use multiple subnets across availability zones
- Configure proper security group rules
- Test network connectivity before production deployment