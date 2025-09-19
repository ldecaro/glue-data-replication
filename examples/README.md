# Configuration Examples

This directory contains example parameter files for various database replication scenarios using the AWS Glue data replication system. The examples demonstrate different engine combinations, processing modes, and configuration options.

## Iceberg Configuration Examples

Apache Iceberg is supported as both source and target database engine. The following examples demonstrate various Iceberg configuration scenarios:

### Iceberg as Source Engine

| File | Description | Use Case |
|------|-------------|----------|
| `iceberg-source-basic-parameters.json` | Iceberg source to PostgreSQL | Replication from Iceberg to PostgreSQL with automatic bookmark detection |
| `iceberg-cross-account-source-parameters.json` | Cross-account Iceberg source | Reading from Iceberg table in different AWS account |

**Note**: All Iceberg source configurations automatically detect and use identifier-field-ids for incremental loading when available. No special configuration is needed to enable incremental processing - it's determined at runtime based on the Iceberg table metadata.

### Iceberg as Target Engine

| File | Description | Use Case |
|------|-------------|----------|
| `sqlserver-to-iceberg-parameters.json` | SQL Server to Iceberg basic target configuration | SQL Server to Iceberg with automatic table creation and bookmark management |
| `iceberg-cross-account-target-parameters.json` | Cross-account Iceberg target | Writing to Iceberg table in different AWS account |

### Advanced Iceberg Scenarios

| File | Description | Use Case |
|------|-------------|----------|
| `iceberg-to-iceberg-parameters.json` | Iceberg to Iceberg transformation | Data processing between different warehouse locations |
| `iceberg-multi-region-parameters.json` | Multi-region Iceberg replication | Global data consolidation across regions |

## Traditional Database Examples

### Oracle Examples
- `oracle-to-oracle-parameters.json` - Oracle to Oracle replication
- `oracle-to-postgresql-parameters.json` - Oracle to PostgreSQL migration

### SQL Server Examples
- `sqlserver-to-postgresql-parameters.json` - SQL Server to PostgreSQL migration
- `sqlserver-to-sqlserver-parameters.json` - SQL Server to SQL Server replication

### PostgreSQL Examples
- Various target configurations with PostgreSQL as destination

### DB2 Examples
- `db2-to-postgresql-parameters.json` - DB2 to PostgreSQL migration

## Manual Bookmark Configuration Examples

Manual bookmark configuration allows you to override automatic incremental column detection for specific tables. This provides precise control over the incremental loading process.

### Manual Bookmark Configuration Files

| File | Description | Use Case |
|------|-------------|----------|
| `sqlserver-to-sqlserver-parameters-with-manual-bookmarks.json` | SQLServer to SQLServer replication | Single-table manual configuration (customers) and all other tables using automatic bookmark column identification |

### Manual Configuration Patterns

**Single Table Configuration:**
```json
{
  "employees": {
    "table_name": "employees",
    "column_name": "last_modified_date"
  }
}
```

**Multiple Tables with Mixed Strategies:**
```json
{
  "orders": {
    "table_name": "orders",
    "column_name": "updated_at"
  },
  "customers": {
    "table_name": "customers", 
    "column_name": "customer_id"
  }
}
```

**Database-Specific Examples:**
- PostgreSQL: Uses `updated_at`, `user_id` patterns
- Oracle: Uses `last_modified`, `dept_id` patterns  
- SQL Server: Uses `modified_date`, `order_id` patterns

### When to Use Manual Configuration

- Tables with non-standard timestamp column names
- Tables with multiple timestamp columns where automatic detection might choose incorrectly
- Tables requiring specific primary key columns for optimal performance
- Business logic requirements for specific incremental columns

## Configuration Documentation

### Comprehensive Guides
- See `../docs/ICEBERG_USAGE_GUIDE.md` - Complete Iceberg configuration, optimization, and best practices guide
- See `../docs/MANUAL_BOOKMARK_CONFIGURATION.md` - Complete manual bookmark configuration guide
- See `../docs/JDBC_DATA_TYPE_MAPPING_REFERENCE.md` - JDBC data type to strategy mapping reference


## Parameter Categories

### Required Parameters

#### For All Engines
- `JobName` - Unique identifier for the Glue job
- `ProcessingMode` - "full-load" or "incremental"
- `Description` - Human-readable job description

#### For Source Engines (Traditional Databases)
- `SourceEngine` - Database engine type (oracle, postgresql, mysql, sqlserver, db2)
- `SourceHost` - Database server hostname
- `SourcePort` - Database server port
- `SourceDatabase` - Database name
- `SourceUsername` - Database username
- `SourcePassword` - Database password
- `SourceJdbcDriverS3Path` - S3 path to JDBC driver JAR file
- `SourceQuery` - SQL query to extract data

#### For Source Engines (Iceberg)
- `SourceEngine` - Set to "iceberg"
- `SourceDatabaseName` - Glue Data Catalog database name
- `SourceTableName` - Iceberg table name
- `SourceWarehouseLocation` - S3 warehouse location
- `SourceCatalogId` - AWS account ID for cross-account access (optional)
- `SourceFormatVersion` - Iceberg format version (recommended: "2")

#### For Target Engines (Traditional Databases)
- `TargetEngine` - Database engine type
- `TargetHost` - Database server hostname
- `TargetPort` - Database server port
- `TargetDatabase` - Database name
- `TargetUsername` - Database username
- `TargetPassword` - Database password
- `TargetJdbcDriverS3Path` - S3 path to JDBC driver JAR file
- `TargetTableName` - Target table name

#### For Target Engines (Iceberg)
- `TargetEngine` - Set to "iceberg"
- `TargetDatabaseName` - Glue Data Catalog database name
- `TargetTableName` - Iceberg table name
- `TargetWarehouseLocation` - S3 warehouse location
- `TargetCatalogId` - AWS account ID for cross-account access (optional)
- `TargetFormatVersion` - Iceberg format version (recommended: "2")

### Bookmark Configuration
- `BookmarkS3Bucket` - S3 bucket for storing job bookmarks
- `BookmarkS3Prefix` - S3 prefix for bookmark files

### Glue Job Configuration
- `WorkerType` - Glue worker type (G.1X, G.2X, G.4X, G.8X)
- `NumberOfWorkers` - Number of Glue workers
- `MaxConcurrentRuns` - Maximum concurrent job executions
- `Timeout` - Job timeout in minutes

### Monitoring Configuration
- `EnableCloudWatchMetrics` - Enable CloudWatch metrics (YES/NO)
- `EnableCloudWatchLogs` - Enable CloudWatch logs (YES/NO)
- `LogLevel` - Logging level (DEBUG, INFO, WARN, ERROR)

### Network Configuration
- `SourceVpcId` - VPC ID for source database (if applicable)
- `SourceSubnetIds` - Comma-separated subnet IDs for source
- `SourceSecurityGroupIds` - Comma-separated security group IDs for source
- `SourceAvailabilityZone` - Availability zone for source
- `CreateSourceGlueVpcEndpoint` - Create Glue VPC endpoint for source (YES/NO)
- `CreateSourceS3VpcEndpoint` - Create S3 VPC endpoint for source (YES/NO)
- `TargetVpcId` - VPC ID for target database (if applicable)
- `TargetSubnetIds` - Comma-separated subnet IDs for target
- `TargetSecurityGroupIds` - Comma-separated security group IDs for target
- `TargetAvailabilityZone` - Availability zone for target
- `CreateTargetGlueVpcEndpoint` - Create Glue VPC endpoint for target (YES/NO)
- `CreateTargetS3VpcEndpoint` - Create S3 VPC endpoint for target (YES/NO)

## Usage Instructions

### 1. Choose the Right Example
Select the parameter file that matches your source and target engine combination and processing requirements.

### 2. Customize Parameters
Update the example file with your specific:
- Database connection details
- S3 bucket names and paths
- Network configuration
- Resource requirements

### 3. Validate Configuration
Ensure all required parameters are provided and valid for your environment.

### 4. Deploy and Test
Use the parameter file with your deployment process and test with a small dataset first.

## Iceberg-Specific Considerations

### Cross-Account Access
When using `CatalogId` for cross-account access:
- Ensure proper IAM permissions are configured
- Verify resource-based policies allow access
- Cross-account access works within the same AWS region only

### Warehouse Locations
- Use consistent S3 bucket naming conventions
- Consider data locality and access patterns
- Implement appropriate S3 lifecycle policies

### Performance Optimization
- Choose appropriate worker types based on data volume
- Use Iceberg format version 2 for better performance
- Consider partitioning strategies for large tables

### Bookmark Management
- **Iceberg Sources**: Automatically detect and use identifier-field-ids for incremental processing
- **Traditional Databases**: Use manual bookmark configuration or automatic column detection
- **Fallback Support**: System automatically falls back to traditional bookmark methods if identifier-field-ids are not available
- **Configuration**: No special parameters needed for Iceberg incremental loading - it's automatic
- Monitor bookmark performance for large tables

## Support and Troubleshooting

For detailed configuration guidance and troubleshooting:
1. Review the `../docs/ICEBERG_USAGE_GUIDE.md` for comprehensive setup instructions, optimization, and best practices
3. Check CloudWatch logs for detailed error information
4. Validate IAM permissions and network connectivity

## Contributing

When adding new example configurations:
1. Follow the existing naming convention
2. Include comprehensive comments and descriptions
3. Test the configuration in a development environment
4. Update this README with the new example details