# AWS Glue Data Replication - Deployment Guide

This guide covers the complete deployment process for the AWS Glue data replication solution, including fixes for mock connection issues and VPC endpoint configuration.

## Prerequisites

- AWS CLI configured with appropriate permissions
- S3 bucket for hosting CloudFormation templates and Glue scripts
- VPC with private subnets (if using cross-VPC configuration)
- Database connection details

## Template Size Limitation

**Important**: The CloudFormation template exceeds the 51,200 character limit for direct uploads and must be hosted in S3.

## Deployment Steps

### 1. Upload CloudFormation Template to S3

```bash
# Upload the CloudFormation template to your S3 bucket
aws s3 cp cloudformation/glue-data-replication.yaml s3://[your-bucket-name]/cloudformation/glue-data-replication.yaml
```

### 2. Upload Glue Script to S3

```bash
# Upload the Glue PySpark script to your S3 bucket
aws s3 cp scripts/glue_data_replication.py s3://[your-bucket-name]/scripts/glue_data_replication.py
```

### 3. Upload JDBC Drivers to S3

```bash
# Example for SQL Server JDBC driver
aws s3 cp jdbc-drivers/sqlserver/mssql-jdbc-12.2.0.jre11.jar s3://[your-bucket-name]/jdbc-drivers/sqlserver/12.2.0.jre11/mssql-jdbc-12.2.0.jre11.jar
```

### 4. Configure Parameters

Update your parameter file (e.g., `examples/sqlserver-to-sqlserver-parameters.json`) with your specific values:

```json
[
  {
    "ParameterKey": "JobName",
    "ParameterValue": "your-job-name"
  },
  {
    "ParameterKey": "GlueJobScriptS3Path",
    "ParameterValue": "s3://[your-bucket-name]/scripts/glue_data_replication.py"
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

### 5. Deploy CloudFormation Stack

#### For New Stack Deployment:
```bash
aws cloudformation create-stack \
  --stack-name your-glue-replication-stack \
  --template-url https://s3.amazonaws.com/[your-bucket-name]/cloudformation/glue-data-replication.yaml \
  --parameters file://examples/your-parameters.json \
  --capabilities CAPABILITY_NAMED_IAM
```

#### For Stack Updates:
```bash
aws cloudformation update-stack \
  --stack-name your-glue-replication-stack \
  --template-url https://s3.amazonaws.com/[your-bucket-name]/cloudformation/glue-data-replication.yaml \
  --parameters file://examples/your-parameters.json \
  --capabilities CAPABILITY_NAMED_IAM
```

### 6. Verify Deployment

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

### 7. Test Glue Job

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
4. **Use job bookmarks** for incremental processing
5. **Test with small datasets** before full production runs
6. **Monitor costs** through AWS Cost Explorer

## File Structure

```
glue-data-replication/
├── cloudformation/
│   └── glue-data-replication.yaml          # Main CloudFormation template
├── scripts/
│   └── glue_data_replication.py            # Glue PySpark script
├── examples/
│   └── sqlserver-to-sqlserver-parameters.json  # Example parameters
├── jdbc-drivers/
│   └── [database-type]/                    # JDBC driver files
└── docs/
    ├── DEPLOYMENT_GUIDE.md                 # This file
    ├── GLUE_VPC_ENDPOINT_FIX.md           # VPC endpoint troubleshooting
    └── GLUE_JOB_MOCK_CONNECTION_FIX.md    # Mock connection fix
```

## Support

For issues or questions:
1. Check the troubleshooting section above
2. Review CloudWatch logs for detailed error messages
3. Verify all prerequisites are met
4. Ensure S3 bucket permissions allow Glue service access

---

**Note**: Replace `[your-bucket-name]` with your actual S3 bucket name throughout all commands and configuration files.