# CloudFormation Parameter Reference

This document provides a comprehensive reference for all parameters available in the AWS Glue Data Replication CloudFormation template, along with configuration examples for various database replication scenarios.

## Table of Contents

- [Required Parameters](#required-parameters)
- [Optional Parameters](#optional-parameters)
- [Iceberg Configuration](#iceberg-configuration)
- [Glue Connection Configuration](#glue-connection-configuration)
- [Network Configuration](#network-configuration)
- [Performance Optimization](#performance-optimization)
- [Observability Configuration](#observability-configuration)
- [Manual Bookmark Configuration](#manual-bookmark-configuration)
- [Parameter Dependencies](#parameter-dependencies)
- [Configuration Examples](#configuration-examples)
- [Troubleshooting](#troubleshooting)

---

## Required Parameters

These parameters must be provided for every deployment.

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
| `SourceSchema` | String | Source database schema name | 0-128 characters (optional for Iceberg) | `HR` |
| `TargetSchema` | String | Target database schema name | 0-128 characters (optional for Iceberg) | `public` |
| `TableNames` | CommaDelimitedList | Comma-separated list of table names to replicate | Valid table names | `employees,departments,locations` |

### Database Credentials (Required for JDBC engines)

| Parameter | Type | Description | Constraints | Example |
|-----------|------|-------------|-------------|---------|
| `SourceDbUser` | String | Source database username | 0-128 characters | `hr_user` |
| `SourceDbPassword` | String | Source database password | 0-128 characters, NoEcho=true | `********` |
| `TargetDbUser` | String | Target database username | 0-128 characters | `postgres` |
| `TargetDbPassword` | String | Target database password | 0-128 characters, NoEcho=true | `********` |

### JDBC Connection Strings (Required for JDBC engines)

| Parameter | Type | Description | Format | Example |
|-----------|------|-------------|--------|---------|
| `SourceConnectionString` | String | JDBC connection string for source database | `jdbc:engine://host:port/database` | `jdbc:oracle:thin:@//host:1521/ORCL` |
| `TargetConnectionString` | String | JDBC connection string for target database | `jdbc:engine://host:port/database` | `jdbc:postgresql://host:5432/target_db` |

### S3 Asset Paths

| Parameter | Type | Description | Format | Example |
|-----------|------|-------------|--------|---------|
| `GlueJobScriptS3Path` | String | S3 path to the PySpark Glue job main script | `s3://bucket/path/main.py` | `s3://bucket/src/glue_job/main.py` |
| `GlueJobModulesS3Path` | String | S3 path to Python modules zip file (optional) | `s3://bucket/path/modules.zip` | `s3://bucket/modules.zip` |
| `SourceJdbcDriverS3Path` | String | S3 path to source JDBC driver JAR (not required for Iceberg) | `s3://bucket/path/driver.jar` | `s3://bucket/drivers/ojdbc11.jar` |
| `TargetJdbcDriverS3Path` | String | S3 path to target JDBC driver JAR (not required for Iceberg) | `s3://bucket/path/driver.jar` | `s3://bucket/drivers/postgresql.jar` |

---

## Optional Parameters

These parameters have default values and can be omitted if the defaults are acceptable.

### Glue Job Configuration

| Parameter | Type | Description | Default | Range/Values | Example |
|-----------|------|-------------|---------|--------------|---------|
| `MaxRetries` | Number | Maximum number of retries for the Glue job | `1` | 0-10 | `3` |
| `Timeout` | Number | Timeout for the Glue job in minutes | `2880` | 1-2880 (48 hours) | `1440` |
| `MaxConcurrentRuns` | Number | Maximum number of concurrent runs | `1` | 1-1000 | `2` |
| `WorkerType` | String | Type of predefined worker for the Glue job | `G.1X` | `Standard`, `G.1X`, `G.2X`, `G.025X` | `G.2X` |
| `NumberOfWorkers` | Number | Number of workers for the Glue job | `2` | 2-299 | `4` |

#### Worker Type Selection Guide

| Type | vCPU | Memory | Use Case |
|------|------|--------|----------|
| `G.025X` | 2 | 4 GB | Development, small datasets |
| `G.1X` | 4 | 16 GB | Standard workloads (default) |
| `G.2X` | 8 | 32 GB | Memory-intensive, large datasets |
| `Standard` | 4 | 16 GB | Legacy compatibility |

---

## Iceberg Configuration

Apache Iceberg is supported as both source and target database engine. These parameters are required when using Iceberg engines.

### Iceberg Parameters

| Parameter | Type | Description | Default | Example |
|-----------|------|-------------|---------|---------|
| `SourceWarehouseLocation` | String | S3 warehouse location for source Iceberg tables | `''` | `s3://my-datalake/warehouse/` |
| `TargetWarehouseLocation` | String | S3 warehouse location for target Iceberg tables | `''` | `s3://analytics-lake/warehouse/` |
| `SourceCatalogId` | String | AWS account ID for cross-account Glue Data Catalog access (source) | Current account | `123456789012` |
| `TargetCatalogId` | String | AWS account ID for cross-account Glue Data Catalog access (target) | Current account | `987654321098` |
| `SourceFormatVersion` | String | Iceberg format version for source tables | `2` | `1` or `2` |
| `TargetFormatVersion` | String | Iceberg format version for target tables | `2` | `1` or `2` |

### Iceberg Configuration Notes

- **Automatic Incremental Detection**: Iceberg sources automatically detect and use identifier-field-ids for incremental loading when available
- **Cross-Account Access**: When using `CatalogId`, ensure proper IAM permissions and resource-based policies
- **Format Version**: Version 2 is recommended for better performance and features
- **JDBC Parameters**: Traditional JDBC parameters are not applicable when using Iceberg engine type

---

## Glue Connection Configuration

AWS Glue Connections provide managed connection capabilities for JDBC databases with centralized credential management and AWS Secrets Manager integration.

### Glue Connection Parameters

| Parameter | Type | Description | Default | Example |
|-----------|------|-------------|---------|---------|
| `CreateSourceConnection` | String | Create new Glue Connection for source with Secrets Manager | `false` | `true` |
| `CreateTargetConnection` | String | Create new Glue Connection for target with Secrets Manager | `false` | `true` |
| `UseSourceConnection` | String | Name of existing Glue Connection for source | `''` | `my-oracle-connection` |
| `UseTargetConnection` | String | Name of existing Glue Connection for target | `''` | `my-postgres-connection` |

### Connection Strategy Options

| Strategy | Parameters | Description |
|----------|------------|-------------|
| **Create New** | `CreateSourceConnection=true` | Creates connection with Secrets Manager integration |
| **Use Existing** | `UseSourceConnection=name` | Uses pre-configured Glue Connection |
| **Direct JDBC** | Neither specified | Traditional direct database connection |

### AWS Secrets Manager Integration

When creating new Glue Connections (`Create*Connection=true`):

1. **Secret Path**: `/aws-glue/{connection-name}`
2. **Secret Format**: `{"username": "user", "password": "pass"}`
3. **Benefits**:
   - Credentials encrypted at rest using AWS KMS
   - Fine-grained access control through IAM policies
   - Audit trail via AWS CloudTrail
   - Support for credential rotation

### Required IAM Permissions for Secrets Manager

```json
{
  "Version": "2012-10-17",
  "Statement": [
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
  ]
}
```

### Validation Rules

- `CreateSourceConnection` and `UseSourceConnection` are **mutually exclusive**
- `CreateTargetConnection` and `UseTargetConnection` are **mutually exclusive**
- Glue Connection parameters only apply to JDBC engines (`oracle`, `sqlserver`, `postgresql`, `db2`)
- Parameters are **ignored** for Iceberg engines (with warnings logged)

---

## Network Configuration

Configure cross-VPC access for databases in different VPCs or private subnets.

### Source Network Parameters

| Parameter | Type | Description | Default | Example |
|-----------|------|-------------|---------|---------|
| `SourceVpcId` | String | VPC ID where source database resides | `''` | `vpc-12345678` |
| `SourceSubnetIds` | CommaDelimitedList | Subnet IDs for source database access | `''` | `subnet-123,subnet-456` |
| `SourceSecurityGroupIds` | CommaDelimitedList | Security group IDs for source access | `''` | `sg-12345678` |
| `SourceAvailabilityZone` | String | Availability zone for source subnet | `''` | `us-east-1a` |
| `CreateSourceS3VpcEndpoint` | String | Create S3 VPC endpoint in source VPC | `NO` | `YES` |
| `CreateSourceGlueVpcEndpoint` | String | Create Glue VPC endpoint in source VPC | `YES` | `YES` |

### Target Network Parameters

| Parameter | Type | Description | Default | Example |
|-----------|------|-------------|---------|---------|
| `TargetVpcId` | String | VPC ID where target database resides | `''` | `vpc-87654321` |
| `TargetSubnetIds` | CommaDelimitedList | Subnet IDs for target database access | `''` | `subnet-789,subnet-012` |
| `TargetSecurityGroupIds` | CommaDelimitedList | Security group IDs for target access | `''` | `sg-87654321` |
| `TargetAvailabilityZone` | String | Availability zone for target subnet | `''` | `us-east-1b` |
| `CreateTargetS3VpcEndpoint` | String | Create S3 VPC endpoint in target VPC | `NO` | `YES` |
| `CreateTargetGlueVpcEndpoint` | String | Create Glue VPC endpoint in target VPC | `YES` | `YES` |

### Network Configuration Scenarios

| Configuration | VPC Endpoints Required |
|---------------|----------------------|
| Same VPC | None |
| Cross-VPC | Glue VPC endpoint |
| Private Subnets | Glue + S3 VPC endpoints |

---

## Performance Optimization

Parameters for optimizing migration performance, especially for large datasets.

### Counting Strategy Parameters

| Parameter | Type | Description | Default | Values |
|-----------|------|-------------|---------|--------|
| `CountingStrategy` | String | Strategy for counting rows during migration | `auto` | `immediate`, `deferred`, `auto` |
| `ProgressUpdateInterval` | Number | Interval in seconds for progress updates | `60` | 10-600 |
| `BatchSizeThreshold` | Number | Row count threshold for strategy selection (deprecated) | `1000000` | 100000-10000000 |
| `EnableDetailedMetrics` | String | Enable detailed CloudWatch metrics | `true` | `true`, `false` |

#### Counting Strategy Values

| Value | Behavior | Use Case |
|-------|----------|----------|
| `auto` | Uses SQL COUNT(*) for all sources (recommended) | All scenarios |
| `immediate` | Explicit SQL COUNT before write | When explicit control needed |
| `deferred` | Count from target after write | Fallback when SQL COUNT fails |

### Partitioned Read Parameters

Enable parallel JDBC reads for large datasets (1TB+) by splitting data extraction across multiple connections.

| Parameter | Type | Description | Default | Range |
|-----------|------|-------------|---------|-------|
| `EnablePartitionedReads` | String | Enable parallel JDBC reads | `disabled` | `auto`, `disabled` |
| `PartitionedReadConfig` | String | JSON configuration for per-table partition settings | `''` | JSON object |
| `DefaultNumPartitions` | Number | Default number of parallel partitions (0=auto) | `0` | 0-200 |
| `DefaultFetchSize` | Number | JDBC fetch size (rows per round-trip) | `10000` | 100-100000 |

#### EnablePartitionedReads Values

| Value | Behavior |
|-------|----------|
| `disabled` | Single-connection JDBC reads (default, reliable for all tables) |
| `auto` | Auto-detect partition columns from primary keys or indexed numeric columns |

#### PartitionedReadConfig JSON Format

```json
{
  "table_name": {
    "partition_column": "column_name",
    "num_partitions": 20,
    "fetch_size": 10000,
    "lower_bound": 1,
    "upper_bound": 1000000
  }
}
```

| Field | Required | Description |
|-------|----------|-------------|
| `partition_column` | No | Numeric column to partition on (auto-detected if omitted) |
| `num_partitions` | No | Number of parallel partitions (auto-calculated if omitted) |
| `fetch_size` | No | JDBC fetch size for this table |
| `lower_bound` | No | Minimum value of partition column (auto-detected if omitted) |
| `upper_bound` | No | Maximum value of partition column (auto-detected if omitted) |

#### Auto-Detection Logic

**Full Load**: Queries database metadata for numeric primary key columns, falls back to indexed numeric columns.

**Incremental Load**: Uses the bookmark's incremental column as the partition column (more efficient).

#### Partition Count Calculation

When `num_partitions` is `0` (auto):
```
optimal_partitions = MIN(100, MAX(2, row_count / 500000))
```

| Row Count | Calculated Partitions |
|-----------|----------------------|
| < 1M | 2 |
| 1M - 5M | 2-10 |
| 5M - 50M | 10-100 |
| > 50M | 100 (max) |

#### When to Use Partitioned Reads

| Scenario | Recommendation |
|----------|----------------|
| Tables < 1M rows | Not needed |
| Tables 1M - 10M rows | Optional |
| Tables 10M - 100M rows | Recommended |
| Tables > 100M rows | Strongly recommended |
| Tables > 1B rows | Required |

### Write Parallelism

Write operations are automatically parallelized based on estimated row count:

| Row Count | Target Write Partitions |
|-----------|------------------------|
| > 5M rows | 20 |
| > 1M rows | 10 |
| > 100K rows | 5 |
| ≤ 100K rows | 2 |

---

## Observability Configuration

Parameters for monitoring, logging, and alerting.

### Logging Parameters

| Parameter | Type | Description | Default | Values |
|-----------|------|-------------|---------|--------|
| `LogRetentionDays` | Number | Days to retain CloudWatch logs | `30` | 1, 3, 5, 7, 14, 30, 60, 90, 120, 150, 180, 365, 400, 545, 731, 1827, 3653 |
| `ErrorLogRetentionDays` | Number | Days to retain error logs | `90` | Same as above |

### Dashboard and Alarm Parameters

| Parameter | Type | Description | Default | Values |
|-----------|------|-------------|---------|--------|
| `EnableCloudWatchDashboard` | String | Create CloudWatch Dashboard | `YES` | `YES`, `NO` |
| `EnableCloudWatchAlarms` | String | Create CloudWatch Alarms | `YES` | `YES`, `NO` |
| `AlarmNotificationEmail` | String | Email for alarm notifications | `''` | Valid email or empty |
| `JobDurationThresholdMinutes` | Number | Threshold for long-running job alarm | `60` | 5-2880 |

---

## Manual Bookmark Configuration

Override automatic incremental column detection for specific tables.

### ManualBookmarkConfig Parameter

| Parameter | Type | Description | Default | Max Length |
|-----------|------|-------------|---------|------------|
| `ManualBookmarkConfig` | String | JSON string with manual bookmark configurations | `''` | 4096 |

### JSON Format

```json
{
  "table_name": "column_name"
}
```

### Examples

**Single Table:**
```json
{
  "employees": "last_modified_date"
}
```

**Multiple Tables:**
```json
{
  "employees": "updated_at",
  "orders": "order_id",
  "products": "last_change_timestamp"
}
```

### JDBC Data Type to Strategy Mapping

| JDBC Data Type | Bookmark Strategy | Use Case |
|----------------|-------------------|----------|
| `TIMESTAMP`, `DATE`, `DATETIME`, `TIME` | `timestamp` | Change tracking with temporal data |
| `INTEGER`, `BIGINT`, `SMALLINT`, `SERIAL` | `primary_key` | Append-only tables with sequential IDs |
| `VARCHAR`, `DECIMAL`, `BOOLEAN`, etc. | `hash` | Tables without suitable timestamp or ID |

### When to Use Manual Configuration

- Tables with non-standard timestamp column names
- Tables with multiple timestamp columns
- Tables requiring specific primary key columns for performance
- Business logic requirements for specific incremental columns

---

## Parameter Dependencies

### Network Configuration Dependencies

- If `SourceVpcId` is specified, `SourceSubnetIds` and `SourceSecurityGroupIds` are required
- If `TargetVpcId` is specified, `TargetSubnetIds` and `TargetSecurityGroupIds` are required
- S3/Glue VPC endpoints only effective when corresponding VPC configuration is provided

### Glue Connection Dependencies

- `CreateSourceConnection` and `UseSourceConnection` are mutually exclusive
- `CreateTargetConnection` and `UseTargetConnection` are mutually exclusive
- When creating connections, all standard JDBC parameters must be provided
- When using existing connections, the named connection must exist in AWS Glue

### Iceberg Configuration Dependencies

- If `SourceEngineType` is `iceberg`: `SourceWarehouseLocation` is required, JDBC parameters are ignored
- If `TargetEngineType` is `iceberg`: `TargetWarehouseLocation` is required, JDBC parameters are ignored
- Glue Connection parameters are ignored for Iceberg engines

---

## Configuration Examples

Parameter files use the CloudFormation parameter file format - an array of `ParameterKey`/`ParameterValue` objects:

```json
[
  {"ParameterKey": "ParameterName", "ParameterValue": "value"},
  ...
]
```

### Traditional Database Examples

#### Oracle to PostgreSQL Migration
```json
[
  {"ParameterKey": "JobName", "ParameterValue": "oracle-to-postgres-migration"},
  {"ParameterKey": "SourceEngineType", "ParameterValue": "oracle"},
  {"ParameterKey": "TargetEngineType", "ParameterValue": "postgresql"},
  {"ParameterKey": "SourceDatabase", "ParameterValue": "ORCL"},
  {"ParameterKey": "SourceSchema", "ParameterValue": "HR"},
  {"ParameterKey": "TargetDatabase", "ParameterValue": "analytics"},
  {"ParameterKey": "TargetSchema", "ParameterValue": "public"},
  {"ParameterKey": "TableNames", "ParameterValue": "employees,departments,locations"},
  {"ParameterKey": "SourceConnectionString", "ParameterValue": "jdbc:oracle:thin:@//oracle-host:1521/ORCL"},
  {"ParameterKey": "TargetConnectionString", "ParameterValue": "jdbc:postgresql://postgres-host:5432/analytics"},
  {"ParameterKey": "SourceDbUser", "ParameterValue": "hr_user"},
  {"ParameterKey": "SourceDbPassword", "ParameterValue": "oracle_password"},
  {"ParameterKey": "TargetDbUser", "ParameterValue": "postgres_user"},
  {"ParameterKey": "TargetDbPassword", "ParameterValue": "postgres_password"},
  {"ParameterKey": "SourceJdbcDriverS3Path", "ParameterValue": "s3://my-bucket/drivers/ojdbc11.jar"},
  {"ParameterKey": "TargetJdbcDriverS3Path", "ParameterValue": "s3://my-bucket/drivers/postgresql-42.6.0.jar"},
  {"ParameterKey": "GlueJobScriptS3Path", "ParameterValue": "s3://my-bucket/src/glue_job/main.py"},
  {"ParameterKey": "WorkerType", "ParameterValue": "G.1X"},
  {"ParameterKey": "NumberOfWorkers", "ParameterValue": "4"}
]
```

#### SQL Server to SQL Server Replication
```json
[
  {"ParameterKey": "JobName", "ParameterValue": "sqlserver-replication"},
  {"ParameterKey": "SourceEngineType", "ParameterValue": "sqlserver"},
  {"ParameterKey": "TargetEngineType", "ParameterValue": "sqlserver"},
  {"ParameterKey": "SourceDatabase", "ParameterValue": "ProductionDB"},
  {"ParameterKey": "SourceSchema", "ParameterValue": "dbo"},
  {"ParameterKey": "TargetDatabase", "ParameterValue": "ReportingDB"},
  {"ParameterKey": "TargetSchema", "ParameterValue": "dbo"},
  {"ParameterKey": "TableNames", "ParameterValue": "orders,customers,products"},
  {"ParameterKey": "SourceConnectionString", "ParameterValue": "jdbc:sqlserver://source-server:1433;databaseName=ProductionDB"},
  {"ParameterKey": "TargetConnectionString", "ParameterValue": "jdbc:sqlserver://target-server:1433;databaseName=ReportingDB"},
  {"ParameterKey": "SourceDbUser", "ParameterValue": "etl_user"},
  {"ParameterKey": "SourceDbPassword", "ParameterValue": "source_password"},
  {"ParameterKey": "TargetDbUser", "ParameterValue": "etl_user"},
  {"ParameterKey": "TargetDbPassword", "ParameterValue": "target_password"},
  {"ParameterKey": "SourceJdbcDriverS3Path", "ParameterValue": "s3://my-bucket/drivers/mssql-jdbc-12.2.0.jre11.jar"},
  {"ParameterKey": "TargetJdbcDriverS3Path", "ParameterValue": "s3://my-bucket/drivers/mssql-jdbc-12.2.0.jre11.jar"},
  {"ParameterKey": "GlueJobScriptS3Path", "ParameterValue": "s3://my-bucket/src/glue_job/main.py"}
]
```

### Iceberg Examples

#### SQL Server to Iceberg (Data Lake Ingestion)
```json
[
  {"ParameterKey": "JobName", "ParameterValue": "sqlserver-to-iceberg"},
  {"ParameterKey": "SourceEngineType", "ParameterValue": "sqlserver"},
  {"ParameterKey": "TargetEngineType", "ParameterValue": "iceberg"},
  {"ParameterKey": "SourceDatabase", "ParameterValue": "Operations"},
  {"ParameterKey": "SourceSchema", "ParameterValue": "dbo"},
  {"ParameterKey": "TargetDatabase", "ParameterValue": "analytics_db"},
  {"ParameterKey": "TableNames", "ParameterValue": "transactions,customer_events"},
  {"ParameterKey": "SourceConnectionString", "ParameterValue": "jdbc:sqlserver://source-server:1433;databaseName=Operations"},
  {"ParameterKey": "SourceDbUser", "ParameterValue": "etl_user"},
  {"ParameterKey": "SourceDbPassword", "ParameterValue": "source_password"},
  {"ParameterKey": "SourceJdbcDriverS3Path", "ParameterValue": "s3://my-bucket/drivers/mssql-jdbc-12.2.0.jre11.jar"},
  {"ParameterKey": "TargetWarehouseLocation", "ParameterValue": "s3://my-datalake/warehouse/"},
  {"ParameterKey": "TargetFormatVersion", "ParameterValue": "2"},
  {"ParameterKey": "GlueJobScriptS3Path", "ParameterValue": "s3://my-bucket/src/glue_job/main.py"},
  {"ParameterKey": "WorkerType", "ParameterValue": "G.2X"},
  {"ParameterKey": "NumberOfWorkers", "ParameterValue": "5"}
]
```

#### Iceberg to PostgreSQL (Data Lake Export)
```json
[
  {"ParameterKey": "JobName", "ParameterValue": "iceberg-to-postgres"},
  {"ParameterKey": "SourceEngineType", "ParameterValue": "iceberg"},
  {"ParameterKey": "TargetEngineType", "ParameterValue": "postgresql"},
  {"ParameterKey": "SourceDatabase", "ParameterValue": "processed_analytics"},
  {"ParameterKey": "TargetDatabase", "ParameterValue": "reporting"},
  {"ParameterKey": "TargetSchema", "ParameterValue": "public"},
  {"ParameterKey": "TableNames", "ParameterValue": "aggregated_metrics"},
  {"ParameterKey": "SourceWarehouseLocation", "ParameterValue": "s3://analytics-lake/warehouse/"},
  {"ParameterKey": "SourceFormatVersion", "ParameterValue": "2"},
  {"ParameterKey": "TargetConnectionString", "ParameterValue": "jdbc:postgresql://reporting-db:5432/reporting"},
  {"ParameterKey": "TargetDbUser", "ParameterValue": "reporting_user"},
  {"ParameterKey": "TargetDbPassword", "ParameterValue": "postgres_password"},
  {"ParameterKey": "TargetJdbcDriverS3Path", "ParameterValue": "s3://my-bucket/drivers/postgresql-42.6.0.jar"},
  {"ParameterKey": "GlueJobScriptS3Path", "ParameterValue": "s3://my-bucket/src/glue_job/main.py"}
]
```

#### Cross-Account Iceberg Access
```json
[
  {"ParameterKey": "JobName", "ParameterValue": "cross-account-iceberg"},
  {"ParameterKey": "SourceEngineType", "ParameterValue": "iceberg"},
  {"ParameterKey": "TargetEngineType", "ParameterValue": "iceberg"},
  {"ParameterKey": "SourceDatabase", "ParameterValue": "shared_analytics"},
  {"ParameterKey": "TargetDatabase", "ParameterValue": "local_analytics"},
  {"ParameterKey": "TableNames", "ParameterValue": "customer_events"},
  {"ParameterKey": "SourceWarehouseLocation", "ParameterValue": "s3://shared-datalake/warehouse/"},
  {"ParameterKey": "SourceCatalogId", "ParameterValue": "123456789012"},
  {"ParameterKey": "TargetWarehouseLocation", "ParameterValue": "s3://local-datalake/warehouse/"},
  {"ParameterKey": "GlueJobScriptS3Path", "ParameterValue": "s3://my-bucket/src/glue_job/main.py"}
]
```

### Glue Connection Examples

#### Create New Connections with Secrets Manager
```json
[
  {"ParameterKey": "JobName", "ParameterValue": "oracle-to-postgres-with-secrets"},
  {"ParameterKey": "SourceEngineType", "ParameterValue": "oracle"},
  {"ParameterKey": "TargetEngineType", "ParameterValue": "postgresql"},
  {"ParameterKey": "CreateSourceConnection", "ParameterValue": "true"},
  {"ParameterKey": "CreateTargetConnection", "ParameterValue": "true"},
  {"ParameterKey": "SourceDatabase", "ParameterValue": "ORCL"},
  {"ParameterKey": "SourceSchema", "ParameterValue": "HR"},
  {"ParameterKey": "TargetDatabase", "ParameterValue": "target_db"},
  {"ParameterKey": "TargetSchema", "ParameterValue": "public"},
  {"ParameterKey": "TableNames", "ParameterValue": "employees,departments"},
  {"ParameterKey": "SourceConnectionString", "ParameterValue": "jdbc:oracle:thin:@//oracle-host:1521/ORCL"},
  {"ParameterKey": "SourceDbUser", "ParameterValue": "hr_user"},
  {"ParameterKey": "SourceDbPassword", "ParameterValue": "oracle_password"},
  {"ParameterKey": "SourceJdbcDriverS3Path", "ParameterValue": "s3://my-bucket/drivers/ojdbc11.jar"},
  {"ParameterKey": "TargetConnectionString", "ParameterValue": "jdbc:postgresql://postgres-host:5432/target_db"},
  {"ParameterKey": "TargetDbUser", "ParameterValue": "postgres_user"},
  {"ParameterKey": "TargetDbPassword", "ParameterValue": "postgres_password"},
  {"ParameterKey": "TargetJdbcDriverS3Path", "ParameterValue": "s3://my-bucket/drivers/postgresql.jar"},
  {"ParameterKey": "GlueJobScriptS3Path", "ParameterValue": "s3://my-bucket/src/glue_job/main.py"}
]
```

#### Use Existing Glue Connections
```json
[
  {"ParameterKey": "JobName", "ParameterValue": "sqlserver-existing-connections"},
  {"ParameterKey": "SourceEngineType", "ParameterValue": "sqlserver"},
  {"ParameterKey": "TargetEngineType", "ParameterValue": "sqlserver"},
  {"ParameterKey": "UseSourceConnection", "ParameterValue": "production-sqlserver-connection"},
  {"ParameterKey": "UseTargetConnection", "ParameterValue": "reporting-sqlserver-connection"},
  {"ParameterKey": "SourceDatabase", "ParameterValue": "ProductionDB"},
  {"ParameterKey": "SourceSchema", "ParameterValue": "dbo"},
  {"ParameterKey": "TargetDatabase", "ParameterValue": "ReportingDB"},
  {"ParameterKey": "TargetSchema", "ParameterValue": "dbo"},
  {"ParameterKey": "TableNames", "ParameterValue": "orders,customers,products"},
  {"ParameterKey": "GlueJobScriptS3Path", "ParameterValue": "s3://my-bucket/src/glue_job/main.py"}
]
```


### Performance Optimization Examples

#### Large Dataset with Partitioned Reads
```json
[
  {"ParameterKey": "JobName", "ParameterValue": "large-dataset-migration"},
  {"ParameterKey": "SourceEngineType", "ParameterValue": "oracle"},
  {"ParameterKey": "TargetEngineType", "ParameterValue": "postgresql"},
  {"ParameterKey": "SourceDatabase", "ParameterValue": "PROD"},
  {"ParameterKey": "SourceSchema", "ParameterValue": "SALES"},
  {"ParameterKey": "TargetDatabase", "ParameterValue": "analytics"},
  {"ParameterKey": "TargetSchema", "ParameterValue": "public"},
  {"ParameterKey": "TableNames", "ParameterValue": "transactions,customer_history,product_catalog"},
  {"ParameterKey": "SourceConnectionString", "ParameterValue": "jdbc:oracle:thin:@//oracle-host:1521/PROD"},
  {"ParameterKey": "TargetConnectionString", "ParameterValue": "jdbc:postgresql://postgres-host:5432/analytics"},
  {"ParameterKey": "SourceDbUser", "ParameterValue": "etl_user"},
  {"ParameterKey": "SourceDbPassword", "ParameterValue": "oracle_password"},
  {"ParameterKey": "TargetDbUser", "ParameterValue": "postgres_user"},
  {"ParameterKey": "TargetDbPassword", "ParameterValue": "postgres_password"},
  {"ParameterKey": "SourceJdbcDriverS3Path", "ParameterValue": "s3://my-bucket/drivers/ojdbc11.jar"},
  {"ParameterKey": "TargetJdbcDriverS3Path", "ParameterValue": "s3://my-bucket/drivers/postgresql.jar"},
  {"ParameterKey": "GlueJobScriptS3Path", "ParameterValue": "s3://my-bucket/src/glue_job/main.py"},
  {"ParameterKey": "WorkerType", "ParameterValue": "G.2X"},
  {"ParameterKey": "NumberOfWorkers", "ParameterValue": "10"},
  {"ParameterKey": "EnablePartitionedReads", "ParameterValue": "auto"},
  {"ParameterKey": "DefaultNumPartitions", "ParameterValue": "0"},
  {"ParameterKey": "DefaultFetchSize", "ParameterValue": "10000"},
  {"ParameterKey": "CountingStrategy", "ParameterValue": "auto"},
  {"ParameterKey": "ProgressUpdateInterval", "ParameterValue": "60"},
  {"ParameterKey": "EnableDetailedMetrics", "ParameterValue": "true"}
]
```

#### Custom Partitioned Read Configuration
```json
[
  {"ParameterKey": "JobName", "ParameterValue": "custom-partitioned-reads"},
  {"ParameterKey": "EnablePartitionedReads", "ParameterValue": "auto"},
  {"ParameterKey": "PartitionedReadConfig", "ParameterValue": "{\"orders\": {\"partition_column\": \"order_id\", \"num_partitions\": 30}, \"order_items\": {\"partition_column\": \"order_item_id\", \"num_partitions\": 50, \"fetch_size\": 20000}}"}
]
```

### Cross-VPC Network Configuration

#### Cross-VPC with VPC Endpoints
```json
[
  {"ParameterKey": "JobName", "ParameterValue": "cross-vpc-replication"},
  {"ParameterKey": "SourceEngineType", "ParameterValue": "oracle"},
  {"ParameterKey": "TargetEngineType", "ParameterValue": "postgresql"},
  {"ParameterKey": "SourceVpcId", "ParameterValue": "vpc-source123"},
  {"ParameterKey": "SourceSubnetIds", "ParameterValue": "subnet-src1,subnet-src2"},
  {"ParameterKey": "SourceSecurityGroupIds", "ParameterValue": "sg-oracle-access"},
  {"ParameterKey": "SourceAvailabilityZone", "ParameterValue": "us-east-1a"},
  {"ParameterKey": "CreateSourceS3VpcEndpoint", "ParameterValue": "YES"},
  {"ParameterKey": "CreateSourceGlueVpcEndpoint", "ParameterValue": "YES"},
  {"ParameterKey": "TargetVpcId", "ParameterValue": "vpc-target456"},
  {"ParameterKey": "TargetSubnetIds", "ParameterValue": "subnet-tgt1,subnet-tgt2"},
  {"ParameterKey": "TargetSecurityGroupIds", "ParameterValue": "sg-postgres-access"},
  {"ParameterKey": "TargetAvailabilityZone", "ParameterValue": "us-east-1b"},
  {"ParameterKey": "CreateTargetS3VpcEndpoint", "ParameterValue": "YES"},
  {"ParameterKey": "CreateTargetGlueVpcEndpoint", "ParameterValue": "YES"}
]
```

### Manual Bookmark Configuration

#### With Manual Bookmark Columns
```json
[
  {"ParameterKey": "JobName", "ParameterValue": "manual-bookmark-job"},
  {"ParameterKey": "SourceEngineType", "ParameterValue": "sqlserver"},
  {"ParameterKey": "TargetEngineType", "ParameterValue": "sqlserver"},
  {"ParameterKey": "TableNames", "ParameterValue": "employees,orders,products,audit_log"},
  {"ParameterKey": "ManualBookmarkConfig", "ParameterValue": "{\"employees\": \"last_modified_date\", \"orders\": \"order_timestamp\", \"audit_log\": \"event_time\"}"}
]
```

---

## Troubleshooting

### Common Issues

| Issue | Symptoms | Solution |
|-------|----------|----------|
| Progress shows 0% | Using deferred counting or SQL COUNT failed | Check logs for SQL COUNT failure; use `auto` strategy |
| High CloudWatch costs | Too frequent progress updates | Increase `ProgressUpdateInterval` to 120+ seconds |
| Migration slower than expected | Network latency or worker configuration | Check phase durations; increase workers or worker type |
| Out of memory errors | Insufficient worker resources | Increase `WorkerType` or `NumberOfWorkers` |
| Partitioned reads not working | No suitable partition column found | Check logs; specify `partition_column` in config |
| Uneven partition performance | Data skew in partition column | Use different partition column or specify manual bounds |
| Too many database connections | Too many partitions configured | Reduce `num_partitions` or `DefaultNumPartitions` |
| Glue Connection creation fails | Missing Secrets Manager permissions | Add required IAM permissions to Glue job role |
| Connection not found | Nonexistent connection specified | Create the connection or use correct name |

### Parameter Validation Errors

| Error | Cause | Resolution |
|-------|-------|------------|
| Mutually exclusive parameters | Both Create and Use connection specified | Remove one of the conflicting parameters |
| Missing JDBC parameters | Connection creation without required params | Provide all required JDBC parameters |
| Invalid connection name | Special characters in connection name | Use alphanumeric, hyphens, underscores only |
| Iceberg parameter ignored | Glue Connection params with Iceberg engine | Remove Glue params or change engine type |

---

## CloudFormation Deployment

### Required Capabilities

The template creates named IAM resources, requiring `CAPABILITY_NAMED_IAM`:

```bash
aws cloudformation create-stack \
  --stack-name my-data-replication \
  --template-body file://infrastructure/cloudformation/glue-data-replication.yaml \
  --parameters file://my-parameters.json \
  --capabilities CAPABILITY_NAMED_IAM
```

### Created IAM Resources

- `${JobName}-glue-job-role` - Main Glue job execution role
- `${JobName}-route-table-lookup-role` - VPC endpoint route table lookup (when VPC endpoints created)

---

## Best Practices

### Security
- Use AWS Secrets Manager for credentials (`CreateSourceConnection=true`)
- Enable VPC endpoints for private connectivity
- Restrict security groups to minimum required access
- Rotate credentials regularly

### Performance
- Use `auto` counting strategy (SQL COUNT is fast for all sizes)
- Enable partitioned reads for tables > 10M rows
- Choose appropriate worker type based on data volume
- Monitor CloudWatch metrics for optimization opportunities

### Cost Optimization
- Use `G.025X` workers for development/small datasets
- Reduce progress update frequency for long jobs
- Disable detailed metrics in development environments
- Set appropriate timeout values to avoid runaway jobs

### Reliability
- Set appropriate retry counts based on data criticality
- Use multiple subnets across availability zones
- Configure proper security group rules
- Test network connectivity before production deployment
