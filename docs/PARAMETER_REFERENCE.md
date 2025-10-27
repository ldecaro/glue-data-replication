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

### Glue Connection Configuration (Optional for JDBC Databases)

| Parameter | Type | Description | Constraints | Example |
|-----------|------|-------------|-------------|---------|
| `CreateSourceConnection` | String | Create a new Glue Connection for source database during job execution with AWS Secrets Manager integration | `true`, `false` | `true` |
| `CreateTargetConnection` | String | Create a new Glue Connection for target database during job execution with AWS Secrets Manager integration | `true`, `false` | `false` |
| `UseSourceConnection` | String | Name of existing Glue Connection to use for source database | Valid Glue Connection name or empty | `my-oracle-connection` |
| `UseTargetConnection` | String | Name of existing Glue Connection to use for target database | Valid Glue Connection name or empty | `my-postgres-connection` |

**Important Notes:**
- Glue Connection parameters are only applicable to JDBC database engines (`oracle`, `sqlserver`, `postgresql`, `db2`)
- When source or target engine is `iceberg`, Glue Connection parameters are ignored with a warning
- `CreateSourceConnection` and `UseSourceConnection` are mutually exclusive
- `CreateTargetConnection` and `UseTargetConnection` are mutually exclusive
- When creating connections (`Create*Connection=true`), all standard JDBC parameters must be provided
- When using existing connections (`Use*Connection=name`), the connection must exist in AWS Glue
- **AWS Secrets Manager Integration**: When creating new Glue Connections, database credentials are automatically stored in AWS Secrets Manager at path `/aws-glue/{connection-name}` for enhanced security

### S3 Asset Paths

| Parameter | Type | Description | Format | Example |
|-----------|------|-------------|--------|---------|
| `SourceJdbcDriverS3Path` | String | S3 path to source database JDBC driver JAR file (not required for Iceberg or when using Glue Connections) | `s3://bucket/path/driver.jar` | `s3://bucket/drivers/ojdbc11.jar` |
| `TargetJdbcDriverS3Path` | String | S3 path to target database JDBC driver JAR file (not required for Iceberg or when using Glue Connections) | `s3://bucket/path/driver.jar` | `s3://bucket/drivers/postgresql.jar` |
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

### Glue Connection Configuration Dependencies

When using Glue Connections for JDBC databases, the following dependencies apply:

1. **Mutually Exclusive Parameters**: 
   - `CreateSourceConnection` and `UseSourceConnection` cannot both be specified
   - `CreateTargetConnection` and `UseTargetConnection` cannot both be specified
2. **Engine Type Restrictions**: Glue Connection parameters are only valid for JDBC engines (`oracle`, `sqlserver`, `postgresql`, `db2`)
3. **Connection Creation Requirements**: When `CreateSourceConnection=true` or `CreateTargetConnection=true`, all standard JDBC parameters must be provided
4. **Connection Usage Requirements**: When `UseSourceConnection` or `UseTargetConnection` is specified, the named connection must exist in AWS Glue
5. **Iceberg Engine Behavior**: When source or target engine is `iceberg`, all Glue Connection parameters are ignored with warnings logged
6. **AWS Secrets Manager Requirements**: When creating new Glue Connections, the Glue job execution role must have permissions for AWS Secrets Manager operations (`secretsmanager:CreateSecret`, `secretsmanager:GetSecretValue`, `secretsmanager:PutSecretValue`)
7. **Secret Naming Convention**: Secrets are created with standardized names following the pattern `/aws-glue/{connection-name}` to ensure consistency and avoid conflicts

### AWS Secrets Manager Integration

When creating new Glue Connections (`CreateSourceConnection=true` or `CreateTargetConnection=true`), the system automatically integrates with AWS Secrets Manager for secure credential storage:

#### Automatic Secret Creation
- **Secret Path**: `/aws-glue/{connection-name}` where `{connection-name}` is generated based on job name and timestamp
- **Secret Format**: JSON object with keys `"username"` and `"password"`
- **Secret Value Example**:
  ```json
  {
    "username": "database_user",
    "password": "database_password"
  }
  ```

#### Required IAM Permissions
The Glue job execution role must include the following AWS Secrets Manager permissions:

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

#### Security Benefits
- **Credential Isolation**: Database passwords are not stored in Glue Connection metadata
- **Encryption at Rest**: Secrets are encrypted using AWS KMS
- **Access Control**: Fine-grained IAM permissions control access to secrets
- **Audit Trail**: All secret access is logged in AWS CloudTrail
- **Rotation Support**: Secrets can be rotated independently of Glue Connections

#### Error Handling
- **Permission Validation**: System validates Secrets Manager permissions before creating secrets
- **Cleanup on Failure**: If Glue Connection creation fails, associated secrets are automatically cleaned up
- **Retry Logic**: Transient Secrets Manager failures are retried with exponential backoff
- **Detailed Logging**: All Secrets Manager operations are logged for troubleshooting

### Iceberg Configuration Dependencies

When using Iceberg as source or target engine, the following dependencies apply:

1. **Iceberg Source Configuration**: If `SourceEngineType` is `iceberg`, then `SourceDatabaseName`, `SourceTableName`, and `SourceWarehouseLocation` are required. `SourceJdbcDriverS3Path` is not required.
2. **Iceberg Target Configuration**: If `TargetEngineType` is `iceberg`, then `TargetDatabaseName`, `TargetTableName`, and `TargetWarehouseLocation` are required. `TargetJdbcDriverS3Path` is not required.
3. **Cross-Account Access**: `SourceCatalogId` and `TargetCatalogId` are only needed when accessing Glue Data Catalog in a different AWS account within the same region.
4. **JDBC Parameters**: Traditional JDBC parameters (`Host`, `Port`, `Database`, `Username`, `Password`, `JdbcDriverS3Path`) are not applicable when using Iceberg engine type.
5. **Glue Connection Compatibility**: Glue Connection parameters are ignored when using Iceberg engines.

### Worker Type Dependencies

When using `WorkerType = 'Standard'`, the `NumberOfWorkers` parameter behaves differently:
- For Standard workers: Can be set to any value between 2-299
- For G.1X, G.2X, G.025X workers: Must be between 2-299

## Parameter Validation Rules

### Glue Connection Parameter Validation

The system enforces strict validation rules for Glue Connection parameters to ensure proper configuration:

#### Mutual Exclusivity Rules
1. **Source Connection Exclusivity**: `CreateSourceConnection` and `UseSourceConnection` cannot both be specified
   - Valid: `CreateSourceConnection=true, UseSourceConnection=""` 
   - Valid: `CreateSourceConnection=false, UseSourceConnection="my-connection"`
   - Invalid: `CreateSourceConnection=true, UseSourceConnection="my-connection"`

2. **Target Connection Exclusivity**: `CreateTargetConnection` and `UseTargetConnection` cannot both be specified
   - Valid: `CreateTargetConnection=true, UseTargetConnection=""`
   - Valid: `CreateTargetConnection=false, UseTargetConnection="my-connection"`
   - Invalid: `CreateTargetConnection=true, UseTargetConnection="my-connection"`

#### Engine Type Compatibility Rules
1. **JDBC Engine Support**: Glue Connection parameters are only valid for JDBC engines:
   - Supported: `oracle`, `sqlserver`, `postgresql`, `db2`
   - Not supported: `iceberg` (parameters ignored with warnings)

2. **Parameter Requirements by Connection Strategy**:
   - **Create Connection Strategy** (`Create*Connection=true`): All standard JDBC parameters required
   - **Use Existing Strategy** (`Use*Connection=name`): Connection must exist in AWS Glue
   - **Direct JDBC Strategy** (no Glue parameters): Standard JDBC parameters required

#### Connection Creation Requirements
When `CreateSourceConnection=true` or `CreateTargetConnection=true`, the following parameters are mandatory:
- Connection string (e.g., `SourceConnectionString`)
- Database credentials (e.g., `SourceDbUser`, `SourceDbPassword`)
- JDBC driver S3 path (e.g., `SourceJdbcDriverS3Path`)
- Database and schema names

#### Connection Usage Requirements  
When `UseSourceConnection` or `UseTargetConnection` is specified:
- The named Glue Connection must exist in the same AWS account and region
- The connection must be accessible by the Glue job execution role
- Database and schema names are still required for table operations

#### Error Handling for Invalid Configurations
| Configuration Error | System Behavior | Resolution |
|-------------------|------------------|------------|
| Mutually exclusive parameters provided | Job fails during parameter parsing | Remove one of the conflicting parameters |
| Glue Connection parameters with Iceberg engine | Parameters ignored, warnings logged | Remove Glue parameters or change engine type |
| Missing JDBC parameters for connection creation | Job fails during validation | Provide all required JDBC parameters |
| Nonexistent connection specified | Job fails during connection setup | Create the connection or use correct name |
| Invalid connection name format | Job fails during parameter parsing | Use valid Glue Connection naming conventions |
| Insufficient Secrets Manager permissions | Job fails during secret creation | Add required IAM permissions to Glue job role |
| Secret creation failure | Job fails with cleanup of partial resources | Check IAM permissions and AWS service limits |
| Secret name conflicts | Job fails during secret creation | Use unique job names or wait for timestamp uniqueness |

### General Parameter Validation

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

### Create New Glue Connections for Both Source and Target (with AWS Secrets Manager)
```json
{
  "JobName": "oracle-to-postgres-with-glue-connections",
  "SourceEngineType": "oracle",
  "TargetEngineType": "postgresql",
  "CreateSourceConnection": "true",
  "CreateTargetConnection": "true",
  
  // Standard JDBC parameters required for connection creation
  // Credentials will be automatically stored in AWS Secrets Manager
  "SourceConnectionString": "jdbc:oracle:thin:@oracle-host:1521:ORCL",
  "SourceDbUser": "hr_user",
  "SourceDbPassword": "oracle_password",
  "SourceJdbcDriverS3Path": "s3://my-bucket/drivers/ojdbc11.jar",
  "SourceDatabase": "ORCL",
  "SourceSchema": "HR",
  
  "TargetConnectionString": "jdbc:postgresql://postgres-host:5432/target_db",
  "TargetDbUser": "postgres_user", 
  "TargetDbPassword": "postgres_password",
  "TargetJdbcDriverS3Path": "s3://my-bucket/drivers/postgresql.jar",
  "TargetDatabase": "target_db",
  "TargetSchema": "public",
  
  "TableNames": "employees,departments,locations",
  
  // Glue job configuration
  "WorkerType": "G.1X",
  "NumberOfWorkers": "2",
  "MaxRetries": "1",
  "Timeout": "60"
}
```

**Note**: When `CreateSourceConnection=true` and `CreateTargetConnection=true`, the system will:
1. Create AWS Secrets Manager secrets at `/aws-glue/{job-name}-source-{timestamp}` and `/aws-glue/{job-name}-target-{timestamp}`
2. Store database credentials securely in JSON format: `{"username": "user", "password": "pass"}`
3. Create Glue Connections that reference these secrets instead of storing credentials directly
4. Validate IAM permissions for Secrets Manager operations before proceeding

### Use Existing Glue Connections
```json
{
  "JobName": "sqlserver-replication-existing-connections",
  "SourceEngineType": "sqlserver",
  "TargetEngineType": "sqlserver",
  "UseSourceConnection": "production-sqlserver-connection",
  "UseTargetConnection": "reporting-sqlserver-connection",
  
  // Standard database configuration still required
  "SourceDatabase": "ProductionDB",
  "SourceSchema": "dbo",
  "TargetDatabase": "ReportingDB", 
  "TargetSchema": "dbo",
  "TableNames": "orders,customers,products",
  
  // ... other parameters
}
```

### Mixed Connection Types (Source Glue, Target Direct JDBC)
```json
{
  "JobName": "mixed-connection-types",
  "SourceEngineType": "oracle",
  "TargetEngineType": "postgresql",
  "UseSourceConnection": "oracle-prod-connection",
  "CreateTargetConnection": "false",
  
  // Source uses existing Glue Connection (minimal config needed)
  "SourceDatabase": "PROD",
  "SourceSchema": "HR",
  
  // Target uses direct JDBC (full config required)
  "TargetConnectionString": "jdbc:postgresql://new-postgres:5432/analytics",
  "TargetDbUser": "analytics_user",
  "TargetDbPassword": "postgres_password", 
  "TargetJdbcDriverS3Path": "s3://my-bucket/drivers/postgresql.jar",
  "TargetDatabase": "analytics",
  "TargetSchema": "public",
  
  "TableNames": "employees,departments",
  // ... other parameters
}
```

### Iceberg with Glue Connection Parameters (Parameters Ignored)
```json
{
  "JobName": "iceberg-to-postgres-with-ignored-glue-params",
  "SourceEngineType": "iceberg",
  "TargetEngineType": "postgresql",
  
  // These parameters will be ignored for Iceberg source with warnings
  "CreateSourceConnection": "true",
  "UseSourceConnection": "some-connection",
  
  // Iceberg source configuration (required)
  "SourceDatabaseName": "analytics_db",
  "SourceTableName": "customer_events", 
  "SourceWarehouseLocation": "s3://datalake/warehouse/",
  
  // Target uses Glue Connection (valid for JDBC)
  "UseTargetConnection": "postgres-reporting-connection",
  "TargetDatabase": "reporting",
  "TargetSchema": "public",
  "TableNames": "processed_events",
  
  "WorkerType": "G.1X",
  "NumberOfWorkers": "2"
}
```

**Warning**: The system will log warnings about ignored Glue Connection parameters for Iceberg engines but will continue execution normally.

## Advanced Glue Connection Examples

### Enterprise Production Setup with Secrets Manager
```json
{
  "JobName": "prod-oracle-to-sqlserver-replication",
  "SourceEngineType": "oracle",
  "TargetEngineType": "sqlserver",
  
  // Create secure connections with Secrets Manager integration
  "CreateSourceConnection": "true",
  "CreateTargetConnection": "true",
  
  // Source Oracle configuration
  "SourceConnectionString": "jdbc:oracle:thin:@prod-oracle.company.com:1521:PROD",
  "SourceDbUser": "etl_service_account",
  "SourceDbPassword": "SecureOraclePassword123!",
  "SourceJdbcDriverS3Path": "s3://company-drivers/oracle/ojdbc11.jar",
  "SourceDatabase": "PROD",
  "SourceSchema": "SALES",
  
  // Target SQL Server configuration  
  "TargetConnectionString": "jdbc:sqlserver://analytics-sqlserver.company.com:1433;databaseName=Analytics",
  "TargetDbUser": "analytics_writer",
  "TargetDbPassword": "SecureSQLServerPassword456!",
  "TargetJdbcDriverS3Path": "s3://company-drivers/sqlserver/mssql-jdbc-12.2.0.jre11.jar",
  "TargetDatabase": "Analytics",
  "TargetSchema": "dbo",
  
  // Table configuration with manual bookmarks
  "TableNames": "customers,orders,order_items,products",
  "ManualBookmarkConfig": "{\"customers\":\"last_modified\",\"orders\":\"order_date\",\"order_items\":\"created_at\"}",
  
  // High-performance configuration
  "WorkerType": "G.2X",
  "NumberOfWorkers": "8",
  "MaxConcurrentRuns": "2",
  "MaxRetries": "2",
  "Timeout": "480",
  
  // Cross-VPC network configuration
  "SourceVpcId": "vpc-prod12345",
  "SourceSubnetIds": "subnet-prod123,subnet-prod456",
  "SourceSecurityGroupIds": "sg-oracle-access",
  "CreateSourceS3VpcEndpoint": "YES",
  
  "TargetVpcId": "vpc-analytics789",
  "TargetSubnetIds": "subnet-analytics123,subnet-analytics456", 
  "TargetSecurityGroupIds": "sg-sqlserver-access",
  "CreateTargetS3VpcEndpoint": "YES",
  
  // Enhanced monitoring
  "EnableCloudWatchDashboard": "YES",
  "EnableCloudWatchAlarms": "YES",
  "AlarmNotificationEmail": "data-engineering@company.com",
  "JobDurationThresholdMinutes": "240",
  "LogRetentionDays": "90",
  "ErrorLogRetentionDays": "365"
}
```

### Development Environment with Mixed Connection Types
```json
{
  "JobName": "dev-mixed-connection-testing",
  "SourceEngineType": "postgresql",
  "TargetEngineType": "postgresql",
  
  // Source uses existing Glue Connection (shared dev resource)
  "UseSourceConnection": "dev-postgres-shared-connection",
  "SourceDatabase": "development",
  "SourceSchema": "public",
  
  // Target creates new connection for isolated testing
  "CreateTargetConnection": "true",
  "TargetConnectionString": "jdbc:postgresql://dev-postgres-isolated:5432/test_target",
  "TargetDbUser": "test_user",
  "TargetDbPassword": "dev_password_123",
  "TargetJdbcDriverS3Path": "s3://dev-assets/drivers/postgresql-42.6.0.jar",
  "TargetDatabase": "test_target",
  "TargetSchema": "testing",
  
  // Small dataset configuration
  "TableNames": "sample_users,sample_orders",
  
  // Minimal resource allocation for development
  "WorkerType": "G.025X",
  "NumberOfWorkers": "2",
  "MaxRetries": "0",
  "Timeout": "30",
  
  // Development monitoring
  "EnableCloudWatchDashboard": "NO",
  "EnableCloudWatchAlarms": "NO",
  "LogRetentionDays": "7"
}
```

### Cross-Account Scenario with Existing Connections
```json
{
  "JobName": "cross-account-data-sharing",
  "SourceEngineType": "oracle",
  "TargetEngineType": "postgresql",
  
  // Both connections pre-exist in different accounts/regions
  "UseSourceConnection": "shared-oracle-prod-connection",
  "UseTargetConnection": "partner-postgres-connection",
  
  // Minimal configuration since connections are pre-configured
  "SourceDatabase": "SHARED_DATA",
  "SourceSchema": "EXPORT",
  "TargetDatabase": "partner_data",
  "TargetSchema": "imports",
  
  "TableNames": "shared_customers,shared_transactions",
  
  // Conservative settings for cross-account operations
  "WorkerType": "G.1X", 
  "NumberOfWorkers": "3",
  "MaxRetries": "3",
  "Timeout": "180",
  
  // Enhanced error monitoring for cross-account reliability
  "EnableCloudWatchAlarms": "YES",
  "AlarmNotificationEmail": "integration-team@company.com",
  "JobDurationThresholdMinutes": "120"
}
```

### Disaster Recovery Setup with Secrets Manager
```json
{
  "JobName": "dr-oracle-to-oracle-replication",
  "SourceEngineType": "oracle", 
  "TargetEngineType": "oracle",
  
  // Create both connections for DR independence
  "CreateSourceConnection": "true",
  "CreateTargetConnection": "true",
  
  // Primary site Oracle (source)
  "SourceConnectionString": "jdbc:oracle:thin:@primary-oracle.company.com:1521:PROD",
  "SourceDbUser": "dr_replication_user",
  "SourceDbPassword": "PrimaryOraclePassword789!",
  "SourceJdbcDriverS3Path": "s3://dr-assets/drivers/ojdbc11.jar",
  "SourceDatabase": "PROD",
  "SourceSchema": "CRITICAL_DATA",
  
  // DR site Oracle (target)
  "TargetConnectionString": "jdbc:oracle:thin:@dr-oracle.company.com:1521:DRPROD", 
  "TargetDbUser": "dr_target_user",
  "TargetDbPassword": "DROraclePassword012!",
  "TargetJdbcDriverS3Path": "s3://dr-assets/drivers/ojdbc11.jar",
  "TargetDatabase": "DRPROD",
  "TargetSchema": "CRITICAL_DATA",
  
  // Critical business tables with timestamp-based incremental
  "TableNames": "transactions,customer_accounts,audit_log,financial_records",
  "ManualBookmarkConfig": "{\"transactions\":\"transaction_timestamp\",\"customer_accounts\":\"last_updated\",\"audit_log\":\"event_time\",\"financial_records\":\"record_date\"}",
  
  // High availability configuration
  "WorkerType": "G.2X",
  "NumberOfWorkers": "10",
  "MaxConcurrentRuns": "1",
  "MaxRetries": "5", 
  "Timeout": "720",
  
  // Cross-region network configuration
  "SourceVpcId": "vpc-primary123",
  "SourceSubnetIds": "subnet-primary1,subnet-primary2,subnet-primary3",
  "SourceSecurityGroupIds": "sg-oracle-primary",
  "CreateSourceS3VpcEndpoint": "YES",
  
  "TargetVpcId": "vpc-dr456",
  "TargetSubnetIds": "subnet-dr1,subnet-dr2,subnet-dr3",
  "TargetSecurityGroupIds": "sg-oracle-dr", 
  "CreateTargetS3VpcEndpoint": "YES",
  
  // Critical system monitoring
  "EnableCloudWatchDashboard": "YES",
  "EnableCloudWatchAlarms": "YES", 
  "AlarmNotificationEmail": "critical-systems@company.com",
  "JobDurationThresholdMinutes": "360",
  "LogRetentionDays": "365",
  "ErrorLogRetentionDays": "2555"
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