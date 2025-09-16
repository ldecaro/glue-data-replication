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
| `SourceEngineType` | String | Source database engine type | `oracle`, `sqlserver`, `postgresql`, `db2`, `iceberg` | `oracle` |
| `TargetEngineType` | String | Target database engine type | `oracle`, `sqlserver`, `postgresql`, `db2`, `iceberg` | `postgresql` |

### Database Connection Configuration

| Parameter | Type | Description | Constraints | Example |
|-----------|------|-------------|-------------|---------|
| `SourceDatabase` | String | Source database name | 1-128 characters, alphanumeric, hyphens, underscores, periods | `ORCL` |
| `TargetDatabase` | String | Target database name | 1-128 characters, alphanumeric, hyphens, underscores, periods | `target_db` |
| `SourceSchema` | String | Source database schema name | 1-128 characters, alphanumeric, hyphens, underscores, periods | `HR` |
| `TargetSchema` | String | Target database schema name | 1-128 characters, alphanumeric, hyphens, underscores, periods | `public` |
| `TableNames` | CommaDelimitedList | Comma-separated list of table names to replicate | Valid table names | `employees,departments,locations` |
| `ManualBookmarkConfig` | String | JSON string with manual bookmark configurations per table (optional) | Valid JSON object or empty | `{"employees":"updated_at","customers":"customer_id"}` |

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
| `SourceJdbcDriverS3Path` | String | S3 path to source database JDBC driver JAR file (not required for Iceberg) | `s3://bucket/path/driver.jar` | `s3://bucket/drivers/ojdbc11.jar` |
| `TargetJdbcDriverS3Path` | String | S3 path to target database JDBC driver JAR file (not required for Iceberg) | `s3://bucket/path/driver.jar` | `s3://bucket/drivers/postgresql.jar` |
| `GlueJobScriptS3Path` | String | S3 path to the PySpark Glue job script | `s3://bucket/path/script.py` | `s3://bucket/src/glue_job/main.py` |

### Iceberg Configuration (Required when using Iceberg engine)

| Parameter | Type | Description | Constraints | Example |
|-----------|------|-------------|-------------|---------|
| `SourceDatabaseName` | String | Glue Data Catalog database name for Iceberg source | 1-255 characters, alphanumeric, hyphens, underscores only | `analytics_db` |
| `SourceTableName` | String | Iceberg table name for source | 1-255 characters, alphanumeric, hyphens, underscores only | `customer_events` |
| `SourceWarehouseLocation` | String | S3 warehouse location for Iceberg source tables | Valid S3 URI | `s3://my-datalake/warehouse/` |
| `TargetDatabaseName` | String | Glue Data Catalog database name for Iceberg target | 1-255 characters, alphanumeric, hyphens, underscores only | `processed_data` |
| `TargetTableName` | String | Iceberg table name for target | 1-255 characters, alphanumeric, hyphens, underscores only | `aggregated_metrics` |
| `TargetWarehouseLocation` | String | S3 warehouse location for Iceberg target tables | Valid S3 URI | `s3://analytics-lake/warehouse/` |

### Iceberg Configuration (Optional)

| Parameter | Type | Description | Default | Format | Example |
|-----------|------|-------------|---------|--------|---------|
| `SourceCatalogId` | String | AWS account ID for cross-account Glue Data Catalog access (source) | Current account | 12-digit AWS account ID or empty | `123456789012` |
| `TargetCatalogId` | String | AWS account ID for cross-account Glue Data Catalog access (target) | Current account | 12-digit AWS account ID or empty | `987654321098` |
| `SourceFormatVersion` | String | Iceberg table format version for source | `2` | `1`, `2` | `2` |
| `TargetFormatVersion` | String | Iceberg table format version for target | `2` | `1`, `2` | `2` |

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

### Manual Bookmark Configuration (Optional)

| Parameter | Type | Description | Default | Format | Example |
|-----------|------|-------------|---------|--------|---------|
| `ManualBookmarkConfig` | String | JSON configuration for manual bookmark column specification per table | `'{}'` (empty object) | JSON object with table configurations | See detailed format below |

#### Manual Bookmark Configuration Format

The `ManualBookmarkConfig` parameter accepts a JSON string that specifies manual bookmark configurations for specific tables. This allows you to override the automatic incremental column detection for tables that require specific bookmark columns.

**When to Use Manual Configuration:**
- Tables with non-standard timestamp column names
- Tables with multiple timestamp columns where automatic detection might choose incorrectly
- Tables requiring specific primary key columns for optimal incremental performance
- Business logic requirements for specific incremental columns

**JSON Structure:**
```json
{
  "table_name_1": "column_name_to_use",
  "table_name_2": "column_name_to_use"
}
```

**Validation Rules:**
- Table names must contain only alphanumeric characters and underscores
- Table names cannot start with a number
- Column names must contain only alphanumeric characters and underscores
- Column names cannot start with a number
- Column must exist in the target table (validated via JDBC metadata)
- If validation fails, the system falls back to automatic detection

**JDBC Data Type to Strategy Mapping:**

| JDBC Data Type | Bookmark Strategy | Use Case |
|----------------|-------------------|----------|
| `TIMESTAMP`, `TIMESTAMP_WITH_TIMEZONE`, `DATE`, `DATETIME`, `TIME` | `timestamp` | Change tracking with temporal data |
| `INTEGER`, `BIGINT`, `SMALLINT`, `TINYINT`, `SERIAL`, `BIGSERIAL` | `primary_key` | Append-only tables with sequential IDs |
| `VARCHAR`, `CHAR`, `TEXT`, `CLOB`, `DECIMAL`, `NUMERIC`, `FLOAT`, `DOUBLE`, `BOOLEAN`, `BINARY`, `VARBINARY`, `BLOB` | `hash` | Tables without suitable timestamp or ID columns |

**Configuration Examples:**

*Single Table Configuration:*
```json
{
  "employees": "last_modified_date"
}
```

*Multiple Tables Configuration:*
```json
{
  "employees": "updated_at",
  "orders": "order_id",
  "products": "last_change_timestamp"
}
```

*Common Use Cases by Database:*

**PostgreSQL Examples:**
```json
{
  "users": "updated_at",
  "sessions": "session_id",
  "audit_log": "event_timestamp"
}
```

**Oracle Examples:**
```json
{
  "employees": "last_modified",
  "departments": "dept_id",
  "audit_trail": "audit_date"
}
```

**SQL Server Examples:**
```json
{
  "customers": "modified_date",
  "orders": "order_id",
  "inventory": "last_updated"
}
```

**Mixed Strategy Examples:**
```json
{
  "transaction_log": "transaction_timestamp",
  "user_accounts": "account_id",
  "configuration": "config_key"
}
```

**Troubleshooting Manual Configuration:**

| Issue | Symptoms | Solution |
|-------|----------|----------|
| Configuration ignored | Automatic detection used instead | Check JSON syntax, verify column exists in database |
| Performance degradation | Slow job initialization | Reduce number of manual configurations, check database connectivity |
| Validation errors | Warnings in CloudWatch logs | Verify table/column names match database schema exactly |
| Inconsistent behavior | Some tables work, others don't | Check case sensitivity, schema context, and naming conventions |

**Best Practices:**
- Start with critical tables only, use automatic detection for others
- Test configurations in development environment first
- Monitor CloudWatch logs for validation results
- Document why specific columns were chosen for manual configuration
- Use external JSON files for complex configurations and version control

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

### Iceberg Configuration Dependencies

When using Iceberg as source or target engine, the following dependencies apply:

1. **Iceberg Source Configuration**: If `SourceEngineType` is `iceberg`, then `SourceDatabaseName`, `SourceTableName`, and `SourceWarehouseLocation` are required. `SourceJdbcDriverS3Path` is not required.
2. **Iceberg Target Configuration**: If `TargetEngineType` is `iceberg`, then `TargetDatabaseName`, `TargetTableName`, and `TargetWarehouseLocation` are required. `TargetJdbcDriverS3Path` is not required.
3. **Cross-Account Access**: `SourceCatalogId` and `TargetCatalogId` are only needed when accessing Glue Data Catalog in a different AWS account within the same region.
4. **JDBC Parameters**: Traditional JDBC parameters (`Host`, `Port`, `Database`, `Username`, `Password`, `JdbcDriverS3Path`) are not applicable when using Iceberg engine type.

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

### Iceberg as Target (Traditional Database to Data Lake)
```json
{
  "JobName": "sqlserver-to-iceberg-job",
  "SourceEngineType": "oracle",
  "SourceHost": "oracle-db.company.com",
  "SourcePort": "1521",
  "SourceDatabase": "PROD",
  "SourceUsername": "etl_user",
  "SourcePassword": "oracle_password",
  "SourceJdbcDriverS3Path": "s3://my-bucket/drivers/ojdbc8.jar",
  
  "TargetEngineType": "iceberg",
  "TargetDatabaseName": "analytics_db",
  "TargetTableName": "customer_orders",
  "TargetWarehouseLocation": "s3://my-datalake/warehouse/",
  
  "WorkerType": "G.2X",
  "NumberOfWorkers": "5"
}
```

### Iceberg as Source (Data Lake to Traditional Database)
```json
{
  "JobName": "iceberg-to-postgres-job",
  "SourceEngineType": "iceberg",
  "SourceDatabaseName": "processed_analytics",
  "SourceTableName": "daily_metrics",
  "SourceWarehouseLocation": "s3://analytics-lake/warehouse/",
  
  "TargetEngineType": "postgresql",
  "TargetHost": "reporting-db.company.com",
  "TargetPort": "5432",
  "TargetDatabase": "reporting",
  "TargetUsername": "reporting_user",
  "TargetPassword": "postgres_password",
  "TargetJdbcDriverS3Path": "s3://my-bucket/drivers/postgresql.jar",
  
  "WorkerType": "G.1X",
  "NumberOfWorkers": "2"
}
```

### Cross-Account Iceberg Access
```json
{
  "JobName": "cross-account-iceberg-job",
  "SourceEngineType": "sqlserver",
  "SourceHost": "source-db.company.com",
  "SourcePort": "1433",
  "SourceDatabase": "Operations",
  "SourceUsername": "etl_user",
  "SourcePassword": "source_password",
  "SourceJdbcDriverS3Path": "s3://my-bucket/drivers/mssql-jdbc.jar",
  
  "TargetEngineType": "iceberg",
  "TargetDatabaseName": "shared_analytics",
  "TargetTableName": "operational_data",
  "TargetWarehouseLocation": "s3://shared-datalake/warehouse/",
  "TargetCatalogId": "987654321098",
  
  "WorkerType": "G.2X",
  "NumberOfWorkers": "3"
}
```

## CloudFormation Deployment Requirements

### IAM Capabilities
The CloudFormation template creates named IAM resources (roles with custom names), which requires the `CAPABILITY_NAMED_IAM` capability when deploying:

```bash
aws cloudformation create-stack \
  --stack-name my-data-replication \
  --template-body file://infrastructure/cloudformation/glue-data-replication.yaml \
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