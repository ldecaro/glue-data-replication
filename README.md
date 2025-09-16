# AWS Glue Data Replication Solution

A comprehensive AWS Glue-based data replication solution that supports full-load and incremental data migration across multiple database types with cross-VPC network connectivity. Built with a modular architecture for maintainability and extensibility.

## Features

- **Multi-Database Support**: Oracle, SQL Server, PostgreSQL, DB2, Apache Iceberg
- **Cross-VPC Connectivity**: Secure database access across different VPCs
- **Incremental Processing**: Uses Glue job bookmarks for efficient data synchronization with automatic incremental column detection and manual bookmark configuration ([details](docs/BOOKMARK_DETAILS.md))
- **Comprehensive Monitoring**: CloudWatch metrics, dashboards, and alarms
- **Network Security**: VPC endpoints for private subnet access to AWS services
- **Error Handling**: Robust error recovery and retry mechanisms with table-level isolation ([details](docs/ERROR_HANDLING_GUIDE.md))
- **Performance Optimization**: Configurable worker types and parallel processing
- **Modular Architecture**: Clean separation of concerns with focused, maintainable modules

## Architecture

### High-Level Architecture
```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Source DB     │    │   AWS Glue Job   │    │   Target DB     │
│  (Any VPC)      │◄──►│  (Private Subnet)│◄──►│  (Any VPC)      │
└─────────────────┘    └──────────────────┘    └─────────────────┘
                              │
                              ▼
                       ┌──────────────────┐
                       │  VPC Endpoints   │
                       │  - Glue API      │
                       │  - S3 (optional) │
                       └──────────────────┘
```

### Modular Code Architecture
```
src/glue_job/
├── main.py                    # Entry point and orchestration
├── config/                    # Configuration management
│   ├── job_config.py          # Job configuration dataclasses
│   ├── database_engines.py    # Database engine management
│   └── parsers.py             # Configuration parsing
├── database/                  # Database operations
│   ├── connection_manager.py  # Connection management
│   ├── schema_validator.py    # Schema validation
│   ├── migration.py           # Data migration logic
│   └── incremental_detector.py # Incremental processing
├── storage/                   # Storage and bookmarks
│   ├── s3_bookmark.py         # S3 bookmark operations
│   └── bookmark_manager.py    # Bookmark lifecycle
├── monitoring/                # Observability
│   ├── logging.py             # Structured logging
│   ├── metrics.py             # CloudWatch metrics
│   └── progress.py            # Progress tracking
├── network/                   # Network and error handling
│   ├── error_handler.py       # Error classification
│   └── retry_handler.py       # Retry mechanisms
└── utils/                     # Utilities
    └── s3_utils.py            # S3 operations
```

## Quick Start

### Prerequisites

- AWS CLI configured with appropriate permissions
- S3 bucket for hosting templates and scripts (will be created automatically if needed)
- Database connection details
- VPC configuration (if using cross-VPC setup)

### 1. Clone Repository

```bash
git clone <repository-url>
cd glue-data-replication
```

### 2. Deploy with Automatic Asset Upload

```bash
# Single command deployment (uploads assets automatically)
./deploy.sh -s my-glue-replication -b [your-bucket-name] -p my-parameters.json
```

**Or manually upload assets first (optional):**
```bash
# Upload modular Glue job structure
./infrastructure/scripts/upload-modular-assets.sh [your-bucket-name] --include-drivers

# Then deploy without upload
./deploy.sh -s my-glue-replication -b [your-bucket-name] -p my-parameters.json --skip-upload
```

### 3. Configure Parameters

Copy and modify the example parameter file:

```bash
cp examples/sqlserver-to-sqlserver-parameters.json my-parameters.json
# Edit my-parameters.json with your specific values
```

**Note**: The `GlueJobScriptS3Path` parameter will be automatically updated by the deploy script to point to the correct S3 location.

### 4. Deploy Stack

```bash
./deploy.sh -s my-glue-replication -b [your-bucket-name] -p my-parameters.json
```

### 5. Run Job

```bash
aws glue start-job-run --job-name my-job-name
```

## Documentation

### Core Documentation
- **[Deployment Guide](DEPLOYMENT_GUIDE.md)**: Complete deployment instructions
- **[Quick Start Guide](QUICK_START_GUIDE.md)**: Step-by-step setup walkthrough
- **[Architecture Guide](docs/ARCHITECTURE.md)**: Technical architecture and design decisions

### Configuration and Setup
- **[Parameter Reference](docs/PARAMETER_REFERENCE.md)**: Complete parameter documentation
- **[Database Configuration Guide](docs/DATABASE_CONFIGURATION_GUIDE.md)**: Database-specific setup
- **[Iceberg Usage Guide](docs/ICEBERG_USAGE_GUIDE.md)**: Apache Iceberg configuration and best practices
- **[Network Configuration Guide](docs/NETWORK_CONFIGURATION_GUIDE.md)**: VPC and networking setup
- **[Bookmark Details](docs/BOOKMARK_DETAILS.md)**: Job bookmark system and incremental loading strategies
- **[Manual Bookmark Configuration](docs/MANUAL_BOOKMARK_CONFIGURATION.md)**: Comprehensive guide for manual bookmark configuration
- **[JDBC Data Type Mapping Reference](docs/JDBC_DATA_TYPE_MAPPING_REFERENCE.md)**: Complete JDBC data type to strategy mapping


### Operations and Monitoring
- **[Error Handling Guide](docs/ERROR_HANDLING_GUIDE.md)**: Comprehensive error handling during data transfer
- **[Observability Guide](docs/OBSERVABILITY_GUIDE.md)**: Monitoring and alerting setup
- **[Testing Guide](docs/TESTING_GUIDE.md)**: Testing procedures and validation
- **[DevOps Deployment Guide](docs/DEVOPS_DEPLOYMENT_GUIDE.md)**: CI/CD and automation

### API Documentation
- **[Module API Reference](#module-api-reference)**: Public interfaces for all modules

## Project Structure

```
aws-glue-data-replication/
├── src/
│   └── glue_job/                           # Modular Glue job components
│       ├── main.py                         # Entry point
│       ├── config/                         # Configuration management
│       ├── database/                       # Database operations
│       ├── storage/                        # Storage and bookmarks
│       ├── monitoring/                     # Observability
│       ├── network/                        # Network and error handling
│       └── utils/                          # Utilities
├── infrastructure/
│   ├── cloudformation/                     # CloudFormation templates
│   ├── scripts/                            # Deployment scripts
│   └── iam/                                # IAM policies
├── tests/                                  # Test suites
├── docs/                                   # Documentation
├── examples/                               # Configuration examples
└── config/                                 # Static configuration files
```

## Module API Reference

### Configuration Modules (`glue_job.config`)

#### JobConfig
```python
from glue_job.config.job_config import JobConfig, ConnectionConfig, NetworkConfig

# Core configuration dataclasses
config = JobConfig(
    job_name="my-job",
    source_config=ConnectionConfig(...),
    target_config=ConnectionConfig(...)
)
```

#### DatabaseEngineManager
```python
from glue_job.config.database_engines import DatabaseEngineManager

engine_manager = DatabaseEngineManager()
driver_class = engine_manager.get_driver_class("sqlserver")
connection_url = engine_manager.build_connection_url(config)
```

### Database Modules (`glue_job.database`)

#### ConnectionManager
```python
from glue_job.database.connection_manager import JdbcConnectionManager

conn_manager = JdbcConnectionManager(glue_context)
df = conn_manager.create_connection(connection_config)
```

#### DataMigrator
```python
from glue_job.database.migration import FullLoadDataMigrator, IncrementalDataMigrator

# Full load migration
full_migrator = FullLoadDataMigrator(glue_context)
metrics = full_migrator.execute_full_load(source_df, target_config)

# Incremental migration
incr_migrator = IncrementalDataMigrator(glue_context)
metrics = incr_migrator.execute_incremental_load(source_df, target_config, bookmark)
```

### Storage Modules (`glue_job.storage`)

#### BookmarkManager
```python
from glue_job.storage.bookmark_manager import JobBookmarkManager

bookmark_manager = JobBookmarkManager(s3_config)
state = bookmark_manager.get_bookmark_state("table_name")
bookmark_manager.update_bookmark_state("table_name", new_state)
```

#### Manual Bookmark Configuration
```python
from glue_job.storage.manual_bookmark_config import ManualBookmarkConfig, BookmarkStrategyResolver

# Create manual configurations
manual_configs = {
    "employees": ManualBookmarkConfig("employees", "updated_at"),
    "orders": ManualBookmarkConfig("orders", "order_id")
}

# Initialize strategy resolver
resolver = BookmarkStrategyResolver(manual_configs, structured_logger)

# Resolve strategy for a table
strategy, column, is_manual = resolver.resolve_strategy("employees", jdbc_connection)
```

#### S3BookmarkStorage
```python
from glue_job.storage.s3_bookmark import S3BookmarkStorage

s3_storage = S3BookmarkStorage(s3_config)
bookmark = s3_storage.read_bookmark("table_name")
s3_storage.write_bookmark(bookmark_state)
```

### Monitoring Modules (`glue_job.monitoring`)

#### StructuredLogger
```python
from glue_job.monitoring.logging import StructuredLogger

logger = StructuredLogger("my-job")
logger.info("Processing started", extra={"table": "users", "rows": 1000})
```

#### MetricsPublisher
```python
from glue_job.monitoring.metrics import CloudWatchMetricsPublisher

metrics = CloudWatchMetricsPublisher()
metrics.publish_metric("RowsProcessed", 1000, "Count")
```

### Network Modules (`glue_job.network`)

#### ErrorHandler
```python
from glue_job.network.error_handler import NetworkErrorHandler
from glue_job.network.retry_handler import ConnectionRetryHandler

error_handler = NetworkErrorHandler()
retry_handler = ConnectionRetryHandler(max_retries=3)
```

### Utility Modules (`glue_job.utils`)

#### S3Utilities
```python
from glue_job.utils.s3_utils import S3PathUtilities, EnhancedS3ParallelOperations

s3_utils = S3PathUtilities()
valid_path = s3_utils.validate_s3_path("s3://bucket/key")

s3_ops = EnhancedS3ParallelOperations()
result = s3_ops.parallel_upload(files, bucket)
```

## Supported Databases

| Database | Engine Type | JDBC Driver Required | Notes |
|----------|-------------|---------------------|-------|
| Oracle | `oracle` | Oracle JDBC Driver | Traditional JDBC connection |
| SQL Server | `sqlserver` | Microsoft JDBC Driver | Traditional JDBC connection |
| PostgreSQL | `postgresql` | PostgreSQL JDBC Driver | Traditional JDBC connection |
| IBM DB2 | `db2` | IBM DB2 JDBC Driver | Traditional JDBC connection |
| Apache Iceberg | `iceberg` | No | Uses Glue Data Catalog and Spark |

## Configuration Options

### Network Configuration

- **Same VPC**: Simple configuration, no VPC endpoints needed
- **Cross-VPC**: Requires Glue network connections and VPC endpoints
- **Private Subnets**: Requires VPC endpoints for AWS service access

### VPC Endpoints

For jobs running in private subnets:

- **Glue VPC Endpoint**: Required for Glue API access (`CreateSourceGlueVpcEndpoint: YES`)
- **S3 VPC Endpoint**: Optional for JDBC driver access (`CreateSourceS3VpcEndpoint: YES`)

### Worker Configuration

| Worker Type | vCPU | Memory | Use Case |
|-------------|------|--------|----------|
| G.1X | 4 | 16 GB | Standard workloads |
| G.2X | 8 | 32 GB | Memory-intensive |
| G.025X | 2 | 4 GB | Light workloads |

## Monitoring

The solution includes comprehensive monitoring:

- **CloudWatch Logs**: Job execution logs with structured logging
- **CloudWatch Metrics**: Custom metrics for job performance
- **CloudWatch Dashboard**: Visual monitoring interface
- **CloudWatch Alarms**: Automated failure notifications

## Security Features

- **IAM Roles**: Least-privilege access for Glue jobs
- **VPC Endpoints**: Private connectivity to AWS services
- **Security Groups**: Network-level access control
- **Encryption**: Support for encrypted databases and S3 buckets

## Cost Optimization

- **Job Bookmarks**: Incremental processing reduces data transfer
- **Worker Scaling**: Configurable worker count and type
- **VPC Endpoints**: Optional based on security requirements
- **Log Retention**: Configurable retention periods

## Troubleshooting

### Common Issues

1. **Template Size Error**: Use S3-hosted template URL
2. **Glue API Timeout**: Enable Glue VPC endpoint
3. **Mock Subnet Error**: Update to latest Glue script
4. **Permission Denied**: Verify IAM role permissions

See the [Deployment Guide](DEPLOYMENT_GUIDE.md) for detailed troubleshooting steps.

## Examples

### SQL Server to SQL Server
```bash
# See examples/sqlserver-to-sqlserver-parameters.json
```

### Oracle to Oracle
```bash
# Configure source as oracle, target as oracle
# Ensure JDBC driver is uploaded to S3
# See examples/oracle-to-oracle-parameters.json
```

### Cross-VPC Replication
```bash
# Configure SourceVpcId and TargetVpcId parameters
# Enable appropriate VPC endpoints (AWS Glue & Amazon S3)
```

### Iceberg Table Replication
```bash
# Iceberg as target (traditional database to Iceberg)
# Configure target engine as "iceberg" with warehouse location
# See examples/sqlserver-to-iceberg-parameters.json
```

### Manual Bookmark Configuration
```bash
# SQLServer to SQLServer with manual bookmark configuration
# See examples/sqlserver-to-sqlserver-parameters-with-manual-bookmarks.json

# Manual bookmark is a configuration in the json file to define which tables and columns are used for bookmark. If not set, automatic column bookmark identification takes place. Example:

#  {
#    "ParameterKey": "ManualBookmarkConfig",
#    "ParameterValue": "{\"customers\":\"customer_id\"}"
#  }

```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Test thoroughly
5. Submit a pull request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Support

For issues or questions:
1. Check the documentation in the `docs/` directory
2. Review CloudWatch logs for error details
3. Verify configuration parameters
4. Ensure all prerequisites are met

---

**Important**: Replace `[your-bucket-name]` with your actual S3 bucket name in all commands and configuration files.