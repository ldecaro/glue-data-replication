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

### 3. Configure Parameters

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

### 4. Deploy CloudFormation Stack

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

### 5. Verify Deployment

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