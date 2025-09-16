# Apache Iceberg Usage Guide

This guide provides comprehensive information on using Apache Iceberg as a database engine in the AWS Glue Data Replication solution, including configuration, optimization, and best practices.

## Overview

Apache Iceberg is a modern, open table format designed for large-scale data storage and analytics in data lakes. Unlike traditional JDBC-based database engines, Iceberg operates through the AWS Glue Data Catalog and uses Spark's native Iceberg integration.

### Key Benefits of Iceberg

- **Schema Evolution**: Add, drop, update, or rename columns without rewriting data
- **Time Travel**: Query data as it existed at any point in time
- **ACID Transactions**: Full ACID compliance for data consistency
- **Performance**: Optimized for analytical workloads with efficient file pruning
- **Scalability**: Designed for petabyte-scale data lakes

## Processing Mode Behavior

### Automatic Full Load and Incremental Processing

#### How It Works

1. **First Run (Full Load)**: When no bookmark exists for a table, the system automatically performs a full load
2. **Subsequent Runs (Incremental)**: After the first successful run, the system uses bookmarks to perform incremental loads
3. **Bookmark Management**: Uses the same S3-based bookmark persistence as other database engines

#### Key Behavior

```python
# The system automatically determines processing mode:
if bookmark_state.is_first_run:
    # Performs full load automatically
    logger.info("First run detected - performing full load")
else:
    # Performs incremental load using bookmarks
    logger.info("Incremental load using bookmark")
```

#### Iceberg-Specific Enhancements

While the processing mode logic is identical to other engines, Iceberg provides enhanced bookmark management:

- **Identifier Field IDs**: Uses Iceberg's built-in field identification system for more robust bookmarking
- **Automatic Fallback**: Falls back to traditional bookmark columns if identifier-field-ids are not available
- **Schema Evolution Support**: Handles schema changes gracefully during incremental processing

#### Forcing a Full Reload

To force a full reload (reset to first run behavior):
1. Delete the bookmark state from S3 for the specific table
2. The next job execution will automatically detect this as a first run and perform a full load

## Configuration Requirements

**For complete, ready-to-use configuration examples, see the `examples/` directory and `examples/README.md` for detailed guidance on all available Iceberg configurations.**

### Required Parameters

When using Iceberg as either source or target, the following parameters are required:

| Parameter | Description | Example |
|-----------|-------------|---------|
| `database_name` | Glue Data Catalog database name | `my_data_lake` |
| `table_name` | Iceberg table name | `customer_data` |
| `warehouse_location` | S3 location for table data | `s3://my-datalake-bucket/warehouse/` |

### Optional Parameters

| Parameter | Description | Default | Example |
|-----------|-------------|---------|---------|
| `catalog_id` | AWS account ID for cross-account catalog access | Current account | `123456789012` |
| `format_version` | Iceberg table format version | `2` | `2` |

### Configuration Examples

For complete configuration examples with all required parameters, refer to the example files in the `examples/` directory:

#### Iceberg as Source Engine
- **Basic Configuration**: `examples/iceberg-source-basic-parameters.json` - Iceberg source to PostgreSQL with automatic bookmark detection
- **Cross-Account Access**: `examples/iceberg-cross-account-source-parameters.json` - Reading from Iceberg table in different AWS account

#### Iceberg as Target Engine  
- **Basic Configuration**: `examples/sqlserver-to-iceberg-parameters.json` - SQL Server to Iceberg with automatic table creation
- **Cross-Account Access**: `examples/iceberg-cross-account-target-parameters.json` - Writing to Iceberg table in different AWS account

#### Advanced Scenarios
- **Iceberg to Iceberg**: `examples/iceberg-to-iceberg-parameters.json` - Data processing between different warehouse locations
- **Multi-Region**: `examples/iceberg-multi-region-parameters.json` - Global data consolidation across regions

**Note**: All examples use CloudFormation parameter format with `ParameterKey` and `ParameterValue` structure. The system automatically handles full load on first run and incremental load on subsequent runs - no special configuration is needed.

### IAM Permissions

The Glue job execution role requires the following permissions for Iceberg operations:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "glue:GetDatabase",
                "glue:GetTable",
                "glue:CreateTable",
                "glue:UpdateTable",
                "glue:GetPartitions"
            ],
            "Resource": "*"
        },
        {
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:PutObject",
                "s3:DeleteObject",
                "s3:ListBucket"
            ],
            "Resource": [
                "arn:aws:s3:::your-warehouse-bucket/*",
                "arn:aws:s3:::your-warehouse-bucket"
            ]
        }
    ]
}
```

## Using Iceberg as Target

When using Iceberg as a target database engine, data from traditional databases is replicated into Iceberg tables stored in S3.

### Configuration Example

For complete configuration examples, see `examples/sqlserver-to-iceberg-parameters.json` which demonstrates SQL Server to Iceberg replication with all required CloudFormation parameters.

### Automatic Table Creation

When the target Iceberg table doesn't exist, the system automatically:

1. **Analyzes Source Schema**: Reads JDBC metadata from the source table
2. **Maps Data Types**: Converts JDBC types to appropriate Iceberg types
3. **Creates Table**: Creates the Iceberg table in the Glue Data Catalog
4. **Sets Identifier Fields**: Configures identifier-field-ids for bookmark management

### Data Type Mapping

| JDBC Type | Iceberg Type | Notes |
|-----------|--------------|-------|
| VARCHAR, CHAR, TEXT | string | All text types map to string |
| INTEGER, SMALLINT | int | Integer types |
| BIGINT | long | Long integer |
| DECIMAL, NUMERIC | decimal(precision,scale) | Preserves precision and scale |
| FLOAT | float | Single precision |
| DOUBLE | double | Double precision |
| BOOLEAN | boolean | Boolean values |
| DATE | date | Date only |
| TIMESTAMP | timestamp | Date and time |
| BINARY, VARBINARY | binary | Binary data |

## Using Iceberg as Source

When using Iceberg as a source database engine, data from Iceberg tables is replicated to traditional databases or other Iceberg tables.

### Configuration Example

For complete configuration examples, see `examples/iceberg-source-basic-parameters.json` which demonstrates Iceberg to PostgreSQL replication with all required CloudFormation parameters.

### Bookmark Management

Iceberg tables support advanced bookmark management through identifier-field-ids:

1. **Identifier Field IDs**: Uses Iceberg's built-in field identification system
2. **Automatic Detection**: Automatically detects bookmark columns from table metadata
3. **Fallback Support**: Falls back to traditional bookmark methods if identifier-field-ids are not available

## Key Differences: Source vs Target

### Iceberg as Target

**Advantages:**
- Automatic table creation with schema inference
- Optimized for analytical workloads
- Built-in data versioning and time travel
- Efficient storage with file-level metadata

**Considerations:**
- Requires S3 storage for table data
- Best suited for append-heavy workloads
- Schema evolution capabilities

**Use Cases:**
- Data lake ingestion from operational databases
- Creating analytical datasets from transactional systems
- Building data warehouses with historical data retention

### Iceberg as Source

**Advantages:**
- Access to historical data through time travel
- Efficient reading with predicate pushdown
- Schema evolution transparency
- Consistent reads across concurrent operations

**Considerations:**
- Requires existing Iceberg tables in Glue Data Catalog
- May need data type conversion for target systems
- Bookmark management depends on table structure

**Use Cases:**
- Exporting processed data to operational systems
- Synchronizing data lake content with reporting databases
- Creating data marts from centralized data lakes

## Configuration Examples

For complete, ready-to-use configuration examples, refer to the files in the `examples/` directory. All examples use the proper CloudFormation parameter format and include all required parameters.

### Basic Examples
- **SQL Server to Iceberg**: See `examples/sqlserver-to-iceberg-parameters.json`
- **Iceberg to PostgreSQL**: See `examples/iceberg-source-basic-parameters.json`

### Advanced Examples  
- **Cross-Account Iceberg Target**: See `examples/iceberg-cross-account-target-parameters.json`
- **Cross-Account Iceberg Source**: See `examples/iceberg-cross-account-source-parameters.json`
- **Iceberg to Iceberg**: See `examples/iceberg-to-iceberg-parameters.json`
- **Multi-Region Setup**: See `examples/iceberg-multi-region-parameters.json`

For detailed information about all available examples and their use cases, see `examples/README.md`.

## Cross-Account Access

### Overview

Cross-account access allows you to read from or write to Iceberg tables in a different AWS account's Glue Data Catalog within the same region. This is useful for shared data lakes and multi-account architectures.

### Configuration Examples

For complete cross-account configuration examples, see:
- **Cross-Account Source**: `examples/iceberg-cross-account-source-parameters.json`
- **Cross-Account Target**: `examples/iceberg-cross-account-target-parameters.json`

These examples show the proper CloudFormation parameter format for setting `SourceCatalogId` and `TargetCatalogId` parameters.

### Required IAM Permissions

The Glue job execution role needs the following permissions for cross-account access:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "glue:GetDatabase",
                "glue:GetTable",
                "glue:CreateTable",
                "glue:UpdateTable",
                "glue:GetPartitions"
            ],
            "Resource": [
                "arn:aws:glue:region:123456789012:catalog",
                "arn:aws:glue:region:123456789012:database/*",
                "arn:aws:glue:region:123456789012:table/*"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:PutObject",
                "s3:DeleteObject",
                "s3:ListBucket"
            ],
            "Resource": [
                "arn:aws:s3:::shared-bucket/*",
                "arn:aws:s3:::shared-bucket"
            ]
        }
    ]
}
```

## Warehouse Location Strategies

### Single Bucket Strategy

Use a single S3 bucket with organized prefixes:

```
s3://company-datalake/
├── warehouse/
│   ├── database1/
│   │   ├── table1/
│   │   └── table2/
│   └── database2/
│       ├── table3/
│       └── table4/
└── metadata/
```

**Configuration:**
```json
{
    "ParameterKey": "TargetWarehouseLocation",
    "ParameterValue": "s3://company-datalake/warehouse/"
}
```

### Multi-Bucket Strategy

Use separate buckets for different environments or business units:

```
Production: s3://prod-analytics-datalake/warehouse/
Staging: s3://staging-analytics-datalake/warehouse/
Development: s3://dev-analytics-datalake/warehouse/
```

### Regional Strategy

Use region-specific buckets for multi-region deployments:

```
US East: s3://global-analytics-us-east-1/warehouse/
US West: s3://global-analytics-us-west-2/warehouse/
EU: s3://global-analytics-eu-west-1/warehouse/
```

## Performance Optimization and Best Practices

### Partitioning Strategies

#### Time-Based Partitioning

**Daily Partitioning** - Best for high-volume, time-series data with daily query patterns:

```sql
-- Partition by date for daily aggregations
PARTITIONED BY (DAYS(event_timestamp))
```

**Use Cases:**
- Event logs and clickstream data
- Transaction records
- IoT sensor data
- Daily reporting requirements

**Monthly Partitioning** - Best for moderate-volume data with monthly analysis patterns:

```sql
-- Partition by month for monthly reports
PARTITIONED BY (MONTHS(created_date))
```

**Hourly Partitioning** - Best for real-time analytics and streaming data:

```sql
-- Partition by hour for real-time analytics
PARTITIONED BY (HOURS(event_timestamp))
```

#### Geographic Partitioning

```sql
-- Partition by geographic region
PARTITIONED BY (region)
```

**Benefits:**
- Improved query performance for regional analysis
- Data locality for compliance requirements
- Reduced data scanning for region-specific queries

#### Multi-Level Partitioning

```sql
-- Multi-level partitioning for complex queries
PARTITIONED BY (region, DAYS(event_date))
```

**Best Practices:**
- Order partitions by query frequency (most common first)
- Limit to 2-3 partition levels to avoid over-partitioning
- Monitor partition count and size distribution

### File Size Optimization

#### Target File Sizes: 128MB - 1GB

**Small Files (< 128MB):**
- **Problems:** High metadata overhead, slow queries
- **Solutions:** Enable file compaction, increase batch sizes

**Large Files (> 1GB):**
- **Problems:** Slow individual file processing, memory issues
- **Solutions:** Reduce batch sizes, implement file splitting

#### File Compaction Strategies

```json
{
    "IcebergProperties": {
        "write.target-file-size-bytes": "134217728",
        "write.delete.target-file-size-bytes": "67108864"
    }
}
```

### Compression Settings

#### Recommended Compression by Use Case

**Real-time Analytics (Speed Priority):**
```json
{
    "CompressionCodec": "snappy",
    "Benefits": "Fast compression/decompression",
    "TradeOff": "Larger file sizes"
}
```

**Archival Storage (Size Priority):**
```json
{
    "CompressionCodec": "gzip",
    "Benefits": "Maximum compression ratio",
    "TradeOff": "Slower processing"
}
```

**Balanced Approach:**
```json
{
    "CompressionCodec": "zstd",
    "Benefits": "Good compression with reasonable speed",
    "TradeOff": "Moderate CPU usage"
}
```

### Schema Design Optimization

#### Data Type Optimization

```sql
-- Use appropriate precision for numeric data
DECIMAL(10,2)  -- For currency (better than DOUBLE)
BIGINT         -- For IDs and counters
INT            -- For smaller ranges
SMALLINT       -- For very small ranges (status codes)

-- Use fixed-length for known sizes
CHAR(2)        -- For country codes
VARCHAR(255)   -- For variable-length strings
STRING         -- For unlimited text (use sparingly)

-- Use appropriate temporal precision
DATE           -- For date-only data
TIMESTAMP      -- For precise timestamps
```

### Best Practices

#### Performance Optimization

1. **Partitioning Strategy**
   - Use date-based partitioning for time-series data
   - Consider high-cardinality columns for partition keys
   - Avoid over-partitioning (too many small files)

2. **File Size Management**
   - Target 128MB-1GB file sizes for optimal performance
   - Use Iceberg's automatic compaction features
   - Monitor file count and size distribution

3. **Schema Design**
   - Use appropriate data types for storage efficiency
   - Consider column ordering for better compression
   - Plan for schema evolution requirements

### Data Management

1. **Automatic Processing Mode**
   - System automatically performs full load on first run, incremental on subsequent runs
   - Use timestamp or sequence columns for bookmarks
   - Leverage Iceberg's identifier-field-ids when available
   - Monitor bookmark progression and reset when needed

2. **Data Quality**
   - Implement data validation before writing to Iceberg
   - Use Iceberg's ACID properties for consistency
   - Monitor data freshness and completeness

3. **Storage Optimization**
   - Enable S3 server-side encryption
   - Use appropriate S3 storage classes
   - Implement lifecycle policies for old data

### Monitoring and Troubleshooting

1. **CloudWatch Metrics**
   - Monitor job execution times and success rates
   - Track data volume and throughput metrics
   - Set up alarms for job failures

2. **Logging**
   - Enable detailed logging for Iceberg operations
   - Monitor Glue Data Catalog API calls
   - Track S3 access patterns and errors

## Troubleshooting Common Issues

### Processing Mode Confusion

**Issue**: Documentation examples show a `ProcessingMode` parameter that doesn't work

**Solution**: The `ProcessingMode` parameter does not exist in the actual implementation. The system automatically:
- Performs full load on first run (when no bookmark exists)
- Performs incremental load on subsequent runs (using bookmark state)

**Note**: This is consistent behavior across all database engines, including Iceberg.

### Table Creation Failures

**Issue**: Iceberg table creation fails with permission errors

**Solution**:
1. Verify IAM permissions for Glue Data Catalog operations
2. Check S3 bucket permissions for warehouse location
3. Ensure Glue database exists before table creation

**Example Error**:
```
AccessDeniedException: User is not authorized to perform: glue:CreateTable
```

**Resolution**:
```json
{
    "Effect": "Allow",
    "Action": [
        "glue:CreateTable",
        "glue:UpdateTable"
    ],
    "Resource": "arn:aws:glue:region:account:table/database/*"
}
```

### Schema Compatibility Issues

**Issue**: Data type mapping errors between source and Iceberg

**Solution**:
1. Review JDBC to Iceberg type mapping
2. Consider custom type conversion logic
3. Validate source data types and precision

**Example Error**:
```
Unsupported data type conversion: JDBC CLOB to Iceberg
```

**Resolution**:
- Convert CLOB columns to VARCHAR in source query
- Use CAST operations in source SQL for type conversion

### Bookmark Management Problems

**Issue**: Incremental processing not working with Iceberg source

**Solution**:
1. Verify identifier-field-ids are set correctly
2. Check bookmark column data type compatibility
3. Ensure bookmark values are monotonically increasing

**Example Error**:
```
No suitable bookmark column found in Iceberg table
```

**Resolution**:
1. Add identifier-field-ids to existing table:
```sql
ALTER TABLE glue_catalog.database.table 
SET TBLPROPERTIES ('identifier-field-ids'='1,2')
```

### Cross-Account Access Issues

**Issue**: Cannot access Glue Data Catalog in different account

**Solution**:
1. Verify catalog_id parameter is correct
2. Check cross-account IAM permissions
3. Ensure Glue resource policies allow access

**Example Error**:
```
AccessDeniedException: Cross-account access denied for catalog
```

**Resolution**:
```json
{
    "Effect": "Allow",
    "Principal": {
        "AWS": "arn:aws:iam::source-account:role/GlueJobRole"
    },
    "Action": [
        "glue:GetDatabase",
        "glue:GetTable"
    ],
    "Resource": "*"
}
```

### Performance Issues

**Issue**: Slow read/write performance with Iceberg tables

**Solution**:
1. Check file size distribution and compaction
2. Optimize partition strategy
3. Increase Glue job worker count and type

**Monitoring Queries**:
```sql
-- Check file count and sizes
SELECT file_path, file_size_in_bytes 
FROM glue_catalog.database.table$files;

-- Check partition information
SELECT partition, record_count, file_count
FROM glue_catalog.database.table$partitions;
```

## Maintenance Operations

### Regular Maintenance Schedule

#### Daily Operations
```bash
# Compact small files
CALL system.rewrite_data_files('database.table_name');

# Update table statistics
ANALYZE TABLE database.table_name COMPUTE STATISTICS;
```

#### Weekly Operations
```bash
# Remove old snapshots (keep 7 days)
CALL system.expire_snapshots('database.table_name', TIMESTAMP '2024-01-08 00:00:00');

# Rewrite manifests
CALL system.rewrite_manifests('database.table_name');
```

#### Monthly Operations
```bash
# Full table optimization
CALL system.rewrite_data_files(
    table => 'database.table_name',
    strategy => 'sort',
    sort_order => 'event_timestamp DESC'
);

# Remove orphaned files
CALL system.remove_orphan_files('database.table_name');
```

### Automated Maintenance

#### AWS Glue Job for Maintenance
```python
# Automated maintenance job
def run_iceberg_maintenance():
    # Compact files
    spark.sql(f"CALL system.rewrite_data_files('{table_name}')")
    
    # Expire old snapshots
    expire_timestamp = datetime.now() - timedelta(days=7)
    spark.sql(f"CALL system.expire_snapshots('{table_name}', TIMESTAMP '{expire_timestamp}')")
    
    # Remove orphaned files
    spark.sql(f"CALL system.remove_orphan_files('{table_name}')")
```

## Monitoring and Alerting

### Key Metrics to Monitor

#### Performance Metrics
```json
{
    "JobExecutionTime": {
        "Threshold": "> 2x baseline",
        "Action": "Investigate performance degradation"
    },
    "DataThroughput": {
        "Threshold": "< 50% of baseline",
        "Action": "Check resource allocation"
    },
    "ErrorRate": {
        "Threshold": "> 1%",
        "Action": "Review error logs"
    }
}
```

#### Storage Metrics
```json
{
    "TableSize": {
        "Monitor": "Daily growth rate",
        "Alert": "Unexpected size increases"
    },
    "FileCount": {
        "Monitor": "Small file accumulation",
        "Alert": "> 10,000 files per partition"
    },
    "PartitionCount": {
        "Monitor": "Partition proliferation",
        "Alert": "> 1,000 partitions per table"
    }
}
```

### CloudWatch Dashboards

#### Custom Metrics
```python
# Custom CloudWatch metrics
def publish_iceberg_metrics(table_name, file_count, avg_file_size):
    cloudwatch = boto3.client('cloudwatch')
    
    cloudwatch.put_metric_data(
        Namespace='Iceberg/Tables',
        MetricData=[
            {
                'MetricName': 'FileCount',
                'Dimensions': [{'Name': 'TableName', 'Value': table_name}],
                'Value': file_count,
                'Unit': 'Count'
            },
            {
                'MetricName': 'AverageFileSize',
                'Dimensions': [{'Name': 'TableName', 'Value': table_name}],
                'Value': avg_file_size,
                'Unit': 'Bytes'
            }
        ]
    )
```

## Cost Optimization

### Storage Costs

1. **File Compaction**
   - Regular compaction reduces file count
   - Improves query performance and reduces costs
   - Use Iceberg's automatic compaction features

2. **Data Lifecycle**
   - Implement S3 lifecycle policies
   - Archive old data to cheaper storage classes
   - Use Iceberg's snapshot expiration

3. **Compression**
   - Use appropriate compression codecs (Snappy, GZIP, LZ4)
   - Balance compression ratio vs. query performance
   - Monitor storage usage and compression effectiveness

### Compute Costs

1. **Right-sizing Workers**
   - Use appropriate Glue worker types for workload
   - Scale workers based on data volume
   - Monitor resource utilization

#### Worker Configuration by Data Volume

```json
{
    "SmallJobs": {
        "WorkerType": "G.1X",
        "NumberOfWorkers": 2,
        "DataSize": "< 1GB"
    },
    "MediumJobs": {
        "WorkerType": "G.2X", 
        "NumberOfWorkers": 4,
        "DataSize": "1-10GB"
    },
    "LargeJobs": {
        "WorkerType": "G.4X",
        "NumberOfWorkers": 8,
        "DataSize": "> 10GB"
    }
}
```

## Advanced Features

### Time Travel Queries

When using Iceberg as source, you can query historical data by setting the SourceQuery parameter:

```json
{
    "ParameterKey": "SourceQuery", 
    "ParameterValue": "SELECT * FROM glue_catalog.analytics_db.customer_orders FOR SYSTEM_TIME AS OF '2024-01-01 00:00:00'"
}
```

### Schema Evolution

Iceberg supports schema evolution without data rewriting:

```sql
-- Add new column
ALTER TABLE glue_catalog.analytics_db.customer_orders 
ADD COLUMN customer_segment string;

-- Rename column
ALTER TABLE glue_catalog.analytics_db.customer_orders 
RENAME COLUMN old_name TO new_name;
```

### Partition Evolution

Change partitioning strategy without rewriting data:

```sql
-- Change partition spec
ALTER TABLE glue_catalog.analytics_db.customer_orders 
SET PARTITION SPEC (days(order_date), bucket(16, customer_id));
```

## Integration with Other AWS Services

### AWS Lake Formation

Iceberg tables integrate with Lake Formation for fine-grained access control:

1. Register S3 locations with Lake Formation
2. Grant table-level permissions through Lake Formation
3. Use Lake Formation tags for data classification

### Amazon Athena

Query Iceberg tables directly from Athena:

```sql
SELECT * FROM "analytics_db"."customer_orders" 
WHERE order_date >= current_date - interval '30' day;
```

### AWS Glue Studio

Use Glue Studio visual editor with Iceberg:

1. Create Glue connections for Iceberg catalogs
2. Use visual transforms for data processing
3. Configure Iceberg-specific job properties

## Migration Strategies

### From Traditional Databases to Iceberg

1. **Assessment Phase**
   - Analyze source table structures and data volumes
   - Identify partitioning strategies
   - Plan schema evolution requirements

2. **Initial Load**
   - First run automatically performs full load for historical data migration
   - Optimize for large data transfers
   - Validate data integrity after migration

3. **Ongoing Synchronization**
   - Subsequent runs automatically perform incremental synchronization
   - Monitor bookmark progression
   - Handle schema changes gracefully

### From Other Table Formats to Iceberg

1. **Hive/Parquet Migration**
   - Use Iceberg's migration utilities
   - Preserve existing partitioning where possible
   - Plan for metadata migration

2. **Delta Lake Migration**
   - Export Delta tables to Parquet
   - Import into Iceberg with schema preservation
   - Migrate time travel and versioning features

## Cost Optimization

### Storage Costs

1. **File Compaction**
   - Regular compaction reduces file count
   - Improves query performance and reduces costs
   - Use Iceberg's automatic compaction features

2. **Data Lifecycle**
   - Implement S3 lifecycle policies
   - Archive old data to cheaper storage classes
   - Use Iceberg's snapshot expiration

3. **Compression**
   - Use appropriate compression codecs (Snappy, GZIP, LZ4)
   - Balance compression ratio vs. query performance
   - Monitor storage usage and compression effectiveness

### Compute Costs

1. **Right-sizing Workers**
   - Use appropriate Glue worker types for workload
   - Scale workers based on data volume
   - Monitor resource utilization

2. **Query Optimization**
   - Use predicate pushdown for efficient filtering
   - Leverage Iceberg's metadata for query planning
   - Optimize partition pruning

## Security Considerations

### Data Encryption

1. **S3 Encryption**
   - Use S3 server-side encryption (SSE-S3 or SSE-KMS)
   - Configure encryption at bucket or object level
   - Manage KMS keys for fine-grained control

2. **In-Transit Encryption**
   - Enable HTTPS for all S3 communications
   - Use TLS for Glue Data Catalog API calls
   - Secure JDBC connections with SSL

### Access Control

1. **IAM Policies**
   - Use least-privilege access principles
   - Separate read and write permissions
   - Implement resource-based policies

2. **Lake Formation Integration**
   - Use Lake Formation for table-level access control
   - Implement column-level security where needed
   - Audit data access through CloudTrail

### Data Privacy

1. **PII Handling**
   - Identify and classify sensitive data
   - Implement data masking or tokenization
   - Use separate encryption keys for sensitive tables

2. **Compliance**
   - Implement data retention policies
   - Support right-to-be-forgotten requirements
   - Maintain audit trails for data access

## Conclusion

Apache Iceberg provides a modern, scalable solution for data lake storage and analytics. By integrating Iceberg with the AWS Glue Data Replication solution, you can:

- Build robust data pipelines with ACID guarantees
- Implement efficient incremental processing
- Support schema evolution without downtime
- Leverage advanced analytics capabilities

Follow the best practices and troubleshooting guidance in this document to successfully implement Iceberg-based data replication in your environment.

For additional support and advanced configurations, refer to the [Apache Iceberg documentation](https://iceberg.apache.org/docs/latest/) and [AWS Glue documentation](https://docs.aws.amazon.com/glue/).