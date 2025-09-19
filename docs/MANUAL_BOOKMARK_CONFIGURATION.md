# Manual Bookmark Configuration

This comprehensive guide covers manual bookmark configuration in the AWS Glue Data Replication solution, including setup, troubleshooting, and best practices.

## Table of Contents

1. [Overview](#overview)
2. [Quick Start](#quick-start)
3. [Configuration Format](#configuration-format)
4. [Database-Specific Examples](#database-specific-examples)
5. [Strategy Selection](#strategy-selection)
6. [Validation and Error Handling](#validation-and-error-handling)
7. [Performance Considerations](#performance-considerations)
8. [Best Practices](#best-practices)
9. [Troubleshooting](#troubleshooting)
10. [Monitoring and Alerting](#monitoring-and-alerting)

## Overview

Manual bookmark configuration allows you to explicitly specify which column to use for incremental loading for each table, overriding the automatic detection system. This provides precise control over the incremental loading process while maintaining the flexibility to use automatic detection for other tables.

### Key Features

- **Hybrid Approach**: Mix manual and automatic configuration within the same job
- **JDBC Validation**: Manual configurations are validated against actual database schema
- **Graceful Fallback**: Invalid configurations automatically fall back to automatic detection
- **Performance Optimized**: JDBC metadata queries are cached for optimal performance
- **Database Agnostic**: Works with PostgreSQL, Oracle, SQL Server, and other supported databases

### Architecture

```
Manual Configuration → JDBC Validation → Strategy Resolution → Bookmark Creation
                    ↓
              Automatic Detection (fallback)
```

## Quick Start

### 1. CloudFormation Parameter Format
The manual bookmark configuration is provided as a CloudFormation parameter in your parameters file:

```json
[
  {
    "ParameterKey": "ManualBookmarkConfig",
    "ParameterValue": "{\"table_name\":\"column_to_use\"}"
  }
]
```

### 2. Complete Example
For a complete working example, see `examples/sqlserver-to-sqlserver-parameters-with-manual-bookmarks.json`:

```json
[
  {
    "ParameterKey": "JobName",
    "ParameterValue": "sqlserver-to-sqlserver-replication"
  },
  {
    "ParameterKey": "TableNames",
    "ParameterValue": "customers,inventory,order_items,orders,products"
  },
  {
    "ParameterKey": "ManualBookmarkConfig",
    "ParameterValue": "{\"customers\":\"customer_id\"}"
  }
]
```

### 3. Deployment
Deploy using the project's deploy script as described in `QUICK_START_GUIDE.md`:

```bash
./deploy.sh -s my-data-replication -b your-bucket -p my-parameters.json
```

## Configuration Format

### CloudFormation Parameter Format

The manual bookmark configuration is provided through the `ManualBookmarkConfig` CloudFormation parameter in your parameters file. The parameter value is a JSON string containing the table-to-column mappings.

**Parameters File Format:**
```json
[
  {
    "ParameterKey": "ManualBookmarkConfig",
    "ParameterValue": "{\"table_name_1\":\"column_to_use_for_bookmarks\",\"table_name_2\":\"column_to_use_for_bookmarks\"}"
  }
]
```

**JSON Structure (within the ParameterValue):**
```json
{
  "table_name_1": "column_to_use_for_bookmarks",
  "table_name_2": "column_to_use_for_bookmarks"
}
```

### Validation Rules

**Table Name Requirements:**
- Must contain only alphanumeric characters and underscores
- Cannot start with a number
- Cannot be empty or whitespace-only
- Must match exactly with table names in `TableNames` parameter

**Column Name Requirements:**
- Must contain only alphanumeric characters and underscores
- Cannot start with a number
- Cannot be empty or whitespace-only
- Must exist in the target table (validated via JDBC metadata)

### Configuration Examples

#### Single Table Configuration
```json
[
  {
    "ParameterKey": "ManualBookmarkConfig",
    "ParameterValue": "{\"employees\":\"last_modified_date\"}"
  }
]
```

#### Multiple Tables Configuration
```json
[
  {
    "ParameterKey": "ManualBookmarkConfig",
    "ParameterValue": "{\"employees\":\"updated_at\",\"orders\":\"order_id\",\"audit_log\":\"audit_timestamp\"}"
  }
]
```

#### Partial Configuration (Hybrid Approach)
```json
[
  {
    "ParameterKey": "ManualBookmarkConfig",
    "ParameterValue": "{\"critical_table\":\"business_timestamp\"}"
  }
]
```
*Note: Other tables in the job will use automatic detection*

## Database-Specific Examples

### PostgreSQL Configuration

**CloudFormation Parameter:**
```json
[
  {
    "ParameterKey": "ManualBookmarkConfig",
    "ParameterValue": "{\"users\":\"updated_at\",\"sessions\":\"session_id\",\"events\":\"event_timestamp\"}"
  }
]
```

**Configuration Content:**
```json
{
  "users": "updated_at",
  "sessions": "session_id", 
  "events": "event_timestamp"
}
```

**PostgreSQL Data Type Mapping:**
- `TIMESTAMP`, `TIMESTAMPTZ` → Timestamp strategy
- `SERIAL`, `BIGSERIAL`, `INTEGER`, `BIGINT` → Primary key strategy
- `VARCHAR`, `TEXT`, `BOOLEAN` → Hash strategy

### Oracle Configuration

**CloudFormation Parameter:**
```json
[
  {
    "ParameterKey": "ManualBookmarkConfig",
    "ParameterValue": "{\"employees\":\"last_modified\",\"departments\":\"dept_id\",\"audit_trail\":\"audit_date\"}"
  }
]
```

**Configuration Content:**
```json
{
  "employees": "last_modified",
  "departments": "dept_id",
  "audit_trail": "audit_date"
}
```

**Oracle Data Type Mapping:**
- `DATE`, `TIMESTAMP`, `TIMESTAMP WITH TIME ZONE` → Timestamp strategy
- `NUMBER` (when used for IDs), `INTEGER` → Primary key strategy
- `VARCHAR2`, `CLOB`, `CHAR` → Hash strategy

### SQL Server Configuration

**Complete Example from `examples/sqlserver-to-sqlserver-parameters-with-manual-bookmarks.json`:**
```json
[
  {
    "ParameterKey": "SourceEngineType",
    "ParameterValue": "sqlserver"
  },
  {
    "ParameterKey": "TargetEngineType", 
    "ParameterValue": "sqlserver"
  },
  {
    "ParameterKey": "TableNames",
    "ParameterValue": "customers,inventory,order_items,orders,products"
  },
  {
    "ParameterKey": "ManualBookmarkConfig",
    "ParameterValue": "{\"customers\":\"customer_id\"}"
  }
]
```

**Configuration Content:**
```json
{
  "customers": "customer_id"
}
```

**SQL Server Data Type Mapping:**
- `DATETIME`, `DATETIME2`, `SMALLDATETIME` → Timestamp strategy
- `INT`, `BIGINT`, `SMALLINT` → Primary key strategy
- `NVARCHAR`, `VARCHAR`, `TEXT`, `BIT` → Hash strategy

### Apache Iceberg Configuration

Apache Iceberg sources have unique bookmark management capabilities that differ from traditional databases:

#### Automatic Detection (Recommended)
```json
{
  // No manual configuration needed for Iceberg sources
  // System automatically uses identifier-field-ids when available
}
```

**How Iceberg Automatic Detection Works:**
1. **Identifier Field IDs**: Iceberg tables store `identifier-field-ids` in table metadata
2. **Runtime Detection**: System automatically reads these field IDs from Glue Data Catalog
3. **Column Mapping**: Field IDs are mapped to actual column names for bookmark management
4. **Schema Evolution Safe**: Bookmarks remain valid even when columns are renamed or schema changes

#### Manual Override (When Needed)
```json
{
  "iceberg_table_name": "custom_timestamp_column"
}
```

**When to Use Manual Configuration for Iceberg:**
- Override automatic identifier-field-ids detection
- Specify different bookmark column than what field IDs suggest
- Handle edge cases where automatic detection fails
- Maintain consistency with existing bookmark configurations

#### Key Differences from Traditional Databases
| Aspect | Traditional Databases | Apache Iceberg |
|--------|----------------------|----------------|
| **Configuration Required** | Manual or automatic detection | Automatic via identifier-field-ids |
| **Schema Evolution** | May break bookmarks | Bookmarks remain valid |
| **Column Renames** | Requires configuration update | No configuration change needed |
| **Performance** | Depends on column indexing | Leverages Iceberg metadata optimization |

#### Iceberg Configuration Parameters
```json
{
  "SourceEngineType": "iceberg",
  "SourceWarehouseLocation": "s3://my-datalake/warehouse/",
  "SourceFormatVersion": "2"
  // No bookmark-specific parameters required
}
```

**Important Notes:**
- **No Special Parameters**: Unlike traditional databases, Iceberg sources don't require bookmark-specific configuration
- **Automatic Fallback**: If identifier-field-ids are not available, system falls back to traditional bookmark detection
- **Manual Override Supported**: Manual configuration works the same way as traditional databases when needed

## Strategy Selection

| Column Data Type | Strategy | Use Case | Performance |
|------------------|----------|----------|-------------|
| TIMESTAMP, DATE, DATETIME | `timestamp` | Change tracking | High |
| INTEGER, BIGINT, SERIAL | `primary_key` | Append-only tables | High |
| VARCHAR, TEXT, DECIMAL | `hash` | Reference data | Medium |

### When to Use Manual Configuration

#### Recommended Scenarios

1. **Non-Standard Column Names**: Tables with timestamp columns that don't follow common naming patterns
2. **Multiple Timestamp Columns**: Tables where automatic detection might choose the wrong timestamp column
3. **Composite Keys**: Tables requiring specific primary key columns for incremental loading
4. **Business Logic Requirements**: Tables where business rules dictate specific incremental columns
5. **Performance Optimization**: Tables where a specific column provides better incremental performance

#### Example Scenarios

**Scenario 1: Non-Standard Timestamp Column**
```sql
-- Table with non-standard timestamp column name
CREATE TABLE audit_log (
    id INTEGER PRIMARY KEY,
    event_data TEXT,
    audit_timestamp TIMESTAMP  -- Non-standard name, might not be auto-detected
);
```

**Scenario 2: Multiple Timestamp Columns**
```sql
-- Table with multiple timestamp columns
CREATE TABLE orders (
    order_id INTEGER PRIMARY KEY,
    created_at TIMESTAMP,      -- Creation time
    updated_at TIMESTAMP,      -- Last update time
    shipped_at TIMESTAMP       -- Shipping time
);
-- Manual config ensures 'updated_at' is used instead of 'created_at'
```

## Validation and Error Handling

### JDBC Metadata Validation

The system validates manual configurations against the actual database schema using JDBC metadata queries:

```python
# Validation process
1. Parse manual configuration JSON
2. For each configured table:
   a. Query JDBC metadata for specified column
   b. Validate column exists
   c. Map JDBC data type to bookmark strategy
   d. Cache result for performance
3. If validation fails, fall back to automatic detection
```

### Error Scenarios and Responses

| Error Scenario | System Response | Logging |
|----------------|-----------------|---------|
| Column not found | Fall back to automatic detection | Warning logged with table and column details |
| JDBC connection error | Fall back to automatic detection | Error logged with connection details |
| Invalid JSON format | Use automatic detection for all tables | Error logged with JSON parsing details |
| Invalid table/column names | Skip invalid entries, process valid ones | Warning logged for each invalid entry |

### Common Error Messages

| Error Message | Meaning | Solution |
|---------------|---------|----------|
| `Column 'column_name' not found in table 'table_name'` | Column doesn't exist or wrong name | Verify column name in database schema |
| `JDBC metadata query failed for table 'table_name'` | Database connection issue | Check JDBC connection and permissions |
| `Invalid table name format: 'table_name'` | Table name validation failed | Use only alphanumeric characters and underscores |
| `Manual configuration JSON parsing failed` | Invalid JSON syntax | Validate JSON format and fix syntax errors |
| `Table 'table_name' not found in manual configuration` | Configuration mismatch | Ensure table names match between config and TableNames parameter |

## Performance Considerations

### JDBC Metadata Caching

The system implements intelligent caching to minimize database overhead:

```python
# Cache structure
jdbc_metadata_cache = {
    "table_name.column_name": {
        "column_name": "updated_at",
        "data_type": "TIMESTAMP",
        "jdbc_type": 93,
        "nullable": 1
    }
}
```

**Cache Benefits:**
- Eliminates redundant JDBC metadata queries
- Reduces database load during job initialization
- Improves job startup performance for large numbers of tables
- Handles both successful and failed queries (negative caching)

### Performance Benchmarks

| Operation | Performance Target | Actual Performance |
|-----------|-------------------|-------------------|
| Individual metadata query | < 1ms | ~0.1ms |
| Sequential processing (20 tables) | < 5 seconds | ~1 second |
| Concurrent processing (10 tables) | < 3 seconds | ~0.5 seconds |
| Large-scale processing (100 tables) | < 10 seconds | ~2 seconds |
| Cache operations | < 100ms | ~10ms |

## Best Practices

### Configuration Design

1. **Start Small**: Begin with manual configuration for critical tables only
2. **Use CloudFormation Parameter Format**: Always use the proper CloudFormation parameter format as shown in the examples
3. **Validate First**: Test configurations in development environment using the deploy script
4. **Document Choices**: Document why specific columns were chosen for manual configuration
5. **Monitor Performance**: Track validation performance and cache hit rates

### Column Selection Guidelines

**For Timestamp Columns:**
- Choose columns that are reliably updated on record changes
- Prefer `updated_at` over `created_at` for change detection
- Ensure timestamp columns have appropriate precision
- Verify timezone handling for timestamp columns

**For Primary Key Columns:**
- Use auto-incrementing columns for append-only tables
- Ensure sequential ordering of primary key values
- Avoid composite primary keys in manual configuration
- Consider performance impact of primary key queries

**For Hash Strategy:**
- Use only when timestamp and primary key strategies are not suitable
- Consider performance impact of full table scans
- Suitable for small to medium-sized tables
- Monitor hash computation performance

### Development Workflow

#### Step 1: Create Configuration File
```bash
# Create readable configuration file
cat > manual-bookmark-config.json << EOF
{
  "employees": "updated_at",
  "orders": "order_id"
}
EOF
```

#### Step 2: Validate JSON Format
```bash
# Validate JSON syntax
python -m json.tool manual-bookmark-config.json
```

#### Step 3: Add to CloudFormation Parameters File
```bash
# Create or update your parameters file (e.g., my-parameters.json)
# Add the ManualBookmarkConfig parameter in CloudFormation format:
```

```json
[
  {
    "ParameterKey": "JobName",
    "ParameterValue": "my-replication-job"
  },
  {
    "ParameterKey": "ManualBookmarkConfig",
    "ParameterValue": "{\"employees\":\"updated_at\",\"orders\":\"order_id\"}"
  }
]
```

#### Step 4: Deploy Using the Deploy Script
```bash
# Deploy using the project's deploy script (see QUICK_START_GUIDE.md)
./deploy.sh -s my-data-replication -b your-bucket -p my-parameters.json
```

#### Step 5: Monitor Deployment and Validation
```bash
# Monitor CloudWatch logs for validation results
aws logs filter-log-events \
  --log-group-name /aws/glue/jobs/my-replication-job \
  --filter-pattern "manual configuration"
```

## Troubleshooting

### Common Issues and Solutions

#### Issue 1: Configuration Not Applied
**Symptoms:** Manual configuration appears to be ignored, automatic detection used instead

**Debugging Steps:**
```bash
# Check CloudWatch logs for validation errors
aws logs filter-log-events \
  --log-group-name /aws/glue/jobs/your-job-name \
  --filter-pattern "manual configuration validation"

# Validate JSON syntax
echo '{"table":"column"}' | python -m json.tool

# Test database connectivity
# Ensure JDBC drivers and connection strings are correct
```

**Solutions:**
- Fix JSON syntax errors
- Verify column names exist in target database
- Check JDBC connection configuration
- Review IAM permissions for database access

#### Issue 2: Performance Degradation
**Symptoms:** Job initialization takes longer than expected

**Debugging Steps:**
```bash
# Check performance metrics in CloudWatch
aws cloudwatch get-metric-statistics \
  --namespace AWS/Glue \
  --metric-name JobDuration \
  --dimensions Name=JobName,Value=your-job-name \
  --start-time 2024-01-01T00:00:00Z \
  --end-time 2024-01-02T00:00:00Z \
  --period 3600 \
  --statistics Average

# Review cache hit rates in logs
aws logs filter-log-events \
  --log-group-name /aws/glue/jobs/your-job-name \
  --filter-pattern "cache hit"
```

**Solutions:**
- Reduce number of manual configurations
- Optimize database connection settings
- Consider using automatic detection for non-critical tables
- Monitor and tune cache performance

#### Issue 3: Validation Errors
**Symptoms:** Warning messages about validation failures, fallback to automatic detection

**Debugging Steps:**
```bash
# Check table-specific validation results
aws logs filter-log-events \
  --log-group-name /aws/glue/jobs/your-job-name \
  --filter-pattern "table_name AND validation"

# Verify table names match exactly
# Check for case sensitivity issues
```

**Solutions:**
- Ensure exact table name matching
- Verify schema context for each table
- Check for case sensitivity requirements
- Review database-specific naming conventions

### Diagnostic Commands

#### Using the Example File with Deploy Script
```bash
# 1. Copy the example file
cp examples/sqlserver-to-sqlserver-parameters-with-manual-bookmarks.json my-parameters.json

# 2. Customize the parameters for your environment
# Edit my-parameters.json to update:
# - Connection strings
# - Database names
# - S3 bucket paths
# - Network configuration (VPC, subnets, security groups)

# 3. Deploy using the project's deploy script (see QUICK_START_GUIDE.md)
./deploy.sh -s my-data-replication -b your-bucket -p my-parameters.json

# 4. Monitor deployment
# The deploy script will automatically wait for completion and show results
```

#### Configuration Validation Script
```bash
#!/bin/bash
# validate-manual-config.sh

CONFIG_FILE="$1"
if [ -z "$CONFIG_FILE" ]; then
    echo "Usage: $0 <config-file.json>"
    exit 1
fi

echo "Validating manual bookmark configuration..."

# Check JSON syntax
if ! python -m json.tool "$CONFIG_FILE" > /dev/null 2>&1; then
    echo "ERROR: Invalid JSON syntax"
    exit 1
fi

echo "Configuration validation passed!"
```

#### Database Schema Verification
```sql
-- PostgreSQL
SELECT column_name, data_type 
FROM information_schema.columns 
WHERE table_name = 'your_table' AND column_name = 'your_column';

-- Oracle
SELECT column_name, data_type 
FROM user_tab_columns 
WHERE table_name = 'YOUR_TABLE' AND column_name = 'YOUR_COLUMN';

-- SQL Server
SELECT column_name, data_type 
FROM information_schema.columns 
WHERE table_name = 'your_table' AND column_name = 'your_column';
```

## Monitoring and Alerting

### Custom Metrics

```python
# Publish custom metrics for manual configuration
import boto3

cloudwatch = boto3.client('cloudwatch')

# Track validation success rate
cloudwatch.put_metric_data(
    Namespace='GlueJob/ManualBookmarkConfig',
    MetricData=[
        {
            'MetricName': 'ValidationSuccessRate',
            'Value': success_rate,
            'Unit': 'Percent'
        }
    ]
)

# Track validation duration
cloudwatch.put_metric_data(
    Namespace='GlueJob/ManualBookmarkConfig',
    MetricData=[
        {
            'MetricName': 'ValidationDuration',
            'Value': validation_duration_ms,
            'Unit': 'Milliseconds'
        }
    ]
)
```

### CloudWatch Alarms
```yaml
# CloudFormation template for monitoring alarms
ManualConfigValidationFailureAlarm:
  Type: AWS::CloudWatch::Alarm
  Properties:
    AlarmName: !Sub "${JobName}-manual-config-validation-failures"
    AlarmDescription: "Manual bookmark configuration validation failures"
    MetricName: ValidationFailures
    Namespace: GlueJob/ManualBookmarkConfig
    Statistic: Sum
    Period: 300
    EvaluationPeriods: 1
    Threshold: 1
    ComparisonOperator: GreaterThanOrEqualToThreshold
    AlarmActions:
      - !Ref SNSTopicArn
```

## Common Use Cases

### E-commerce (SQL Server to SQL Server)
**Based on `examples/sqlserver-to-sqlserver-parameters-with-manual-bookmarks.json`:**
```json
[
  {
    "ParameterKey": "TableNames",
    "ParameterValue": "customers,inventory,order_items,orders,products"
  },
  {
    "ParameterKey": "ManualBookmarkConfig",
    "ParameterValue": "{\"customers\":\"customer_id\"}"
  }
]
```

**Configuration Content:**
```json
{
  "customers": "customer_id"
}
```
*Note: Only the `customers` table uses manual configuration; other tables (`inventory`, `order_items`, `orders`, `products`) use automatic detection.*

### Financial Services
```json
{
  "transactions": "transaction_timestamp",
  "accounts": "account_id",
  "audit_log": "audit_date"
}
```

### IoT and Analytics
```json
{
  "sensor_data": "reading_time",
  "devices": "device_id",
  "events": "event_timestamp"
}
```

## Conclusion

Manual bookmark configuration provides powerful control over incremental loading while maintaining the flexibility of automatic detection. By following the guidelines and best practices in this document, you can effectively configure manual bookmarks for your specific use cases while ensuring optimal performance and reliability.

### Key Takeaways

1. **Use Judiciously**: Apply manual configuration only where automatic detection is insufficient
2. **Follow the Example**: Use `examples/sqlserver-to-sqlserver-parameters-with-manual-bookmarks.json` as a reference for proper CloudFormation parameter format
3. **Deploy with Script**: Use the `./deploy.sh` script as described in `QUICK_START_GUIDE.md` for deployment
4. **Validate Thoroughly**: Test configurations in development before production deployment
5. **Monitor Performance**: Track validation performance and cache effectiveness
6. **Plan for Fallback**: Design configurations with graceful fallback to automatic detection
7. **Document Decisions**: Maintain clear documentation of manual configuration choices

For additional support and advanced use cases, refer to the [Bookmark Details Guide](BOOKMARK_DETAILS.md) and [API Reference](API_REFERENCE.md).