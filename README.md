# AWS Glue Data Replication Solution

A comprehensive AWS Glue-based data replication solution that supports full-load and incremental data migration across multiple database types with cross-VPC network connectivity.

## Features

- **Multi-Database Support**: Oracle, SQL Server, PostgreSQL, DB2
- **Cross-VPC Connectivity**: Secure database access across different VPCs
- **Incremental Processing**: Uses Glue job bookmarks for efficient data synchronization
- **Comprehensive Monitoring**: CloudWatch metrics, dashboards, and alarms
- **Network Security**: VPC endpoints for private subnet access to AWS services
- **Error Handling**: Robust error recovery and retry mechanisms
- **Performance Optimization**: Configurable worker types and parallel processing

## Architecture

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

## Quick Start

### Prerequisites

- AWS CLI configured with appropriate permissions
- S3 bucket for hosting templates and scripts
- Database connection details
- VPC configuration (if using cross-VPC setup)

### 1. Clone Repository

```bash
git clone <repository-url>
cd glue-data-replication
```

### 2. Upload Assets to S3

```bash
# Upload CloudFormation template (required due to size)
aws s3 cp cloudformation/glue-data-replication.yaml s3://[your-bucket-name]/cloudformation/glue-data-replication.yaml

# Upload Glue script
aws s3 cp scripts/glue_data_replication.py s3://[your-bucket-name]/scripts/glue_data_replication.py

# Upload JDBC drivers (example for SQL Server)
aws s3 cp jdbc-drivers/sqlserver/mssql-jdbc-12.2.0.jre11.jar s3://[your-bucket-name]/jdbc-drivers/sqlserver/12.2.0.jre11/mssql-jdbc-12.2.0.jre11.jar
```

### 3. Configure Parameters

Copy and modify the example parameter file:

```bash
cp examples/sqlserver-to-sqlserver-parameters.json my-parameters.json
# Edit my-parameters.json with your specific values
```

### 4. Deploy Stack

```bash
aws cloudformation create-stack \
  --stack-name my-glue-replication \
  --template-url https://s3.amazonaws.com/[your-bucket-name]/cloudformation/glue-data-replication.yaml \
  --parameters file://my-parameters.json \
  --capabilities CAPABILITY_NAMED_IAM
```

### 5. Run Job

```bash
aws glue start-job-run --job-name my-job-name
```

## Documentation

- **[Deployment Guide](DEPLOYMENT_GUIDE.md)**: Complete deployment instructions
- **[VPC Endpoint Fix](GLUE_VPC_ENDPOINT_FIX.md)**: Troubleshooting private subnet connectivity
- **[Mock Connection Fix](GLUE_JOB_MOCK_CONNECTION_FIX.md)**: Resolving connection validation issues

## Supported Databases

| Database | Engine Type | JDBC Driver Required |
|----------|-------------|---------------------|
| Oracle | `oracle` | Oracle JDBC Driver |
| SQL Server | `sqlserver` | Microsoft JDBC Driver |
| PostgreSQL | `postgresql` | PostgreSQL JDBC Driver |
| IBM DB2 | `db2` | IBM DB2 JDBC Driver |

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

### Oracle to PostgreSQL
```bash
# Configure source as Oracle, target as PostgreSQL
# Ensure both JDBC drivers are uploaded to S3
```

### Cross-VPC Replication
```bash
# Configure SourceVpcId and TargetVpcId parameters
# Enable appropriate VPC endpoints
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