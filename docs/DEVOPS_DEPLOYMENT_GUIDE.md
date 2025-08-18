# DevOps Deployment Guide

This guide provides detailed instructions for DevOps engineers to deploy and manage the AWS Glue Data Replication system using Infrastructure as Code principles.

## Prerequisites

### AWS Account Requirements

- AWS account with appropriate service limits for Glue jobs
- AWS CLI configured with deployment credentials
- CloudFormation service enabled in target regions
- S3 bucket for storing JDBC drivers and artifacts

### Required Tools

- AWS CLI v2.0 or later
- Git for version control
- Text editor for parameter file customization
- (Optional) AWS CloudFormation CLI for advanced template validation

## IAM Permissions

### DevOps Engineer Permissions

DevOps engineers need specific IAM permissions to deploy the CloudFormation stack. Apply the policy from `iam/devops-policy.json` to your deployment user or role.

#### Policy Overview

The DevOps policy includes permissions for:

1. **CloudFormation Operations**
   - Stack creation, updates, and deletion
   - Template validation and resource description
   - Stack event monitoring

2. **IAM Management**
   - Role and policy creation for Glue jobs
   - Role assumption permissions
   - Policy attachment and detachment

3. **Glue Service Management**
   - Job creation, updates, and deletion
   - Job configuration and monitoring

4. **S3 Access**
   - JDBC driver storage and retrieval
   - Glue asset management

#### Applying the DevOps Policy

**Option 1: Attach to existing IAM user**
```bash
aws iam put-user-policy \
  --user-name your-devops-user \
  --policy-name GlueDataReplicationDeployment \
  --policy-document file://iam/devops-policy.json
```

**Option 2: Create dedicated deployment role**
```bash
# Create the role
aws iam create-role \
  --role-name GlueDataReplicationDeploymentRole \
  --assume-role-policy-document '{
    "Version": "2012-10-17",
    "Statement": [
      {
        "Effect": "Allow",
        "Principal": {
          "AWS": "arn:aws:iam::ACCOUNT-ID:user/your-user"
        },
        "Action": "sts:AssumeRole"
      }
    ]
  }'

# Attach the policy
aws iam put-role-policy \
  --role-name GlueDataReplicationDeploymentRole \
  --policy-name GlueDataReplicationDeployment \
  --policy-document file://iam/devops-policy.json
```

### Service-Linked Roles

The CloudFormation template automatically creates the necessary service roles for Glue job execution. No additional service-linked role configuration is required.

## Pre-Deployment Setup

### 1. JDBC Driver Preparation

Before deploying the stack, ensure JDBC drivers are available in S3:

```bash
# Create S3 bucket for drivers (if not exists)
aws s3 mb s3://your-glue-assets-bucket

# Upload Glue job script
aws s3 cp scripts/glue_data_replication.py s3://your-glue-assets-bucket/scripts/glue_data_replication.py

# Upload JDBC drivers using recommended structure
aws s3 cp ojdbc11.jar s3://your-glue-assets-bucket/jdbc-drivers/oracle/21.7.0.0/ojdbc11.jar
aws s3 cp mssql-jdbc-12.2.0.jre11.jar s3://your-glue-assets-bucket/jdbc-drivers/sqlserver/12.2.0.jre11/mssql-jdbc-12.2.0.jre11.jar
aws s3 cp postgresql-42.6.0.jar s3://your-glue-assets-bucket/jdbc-drivers/postgresql/42.6.0/postgresql-42.6.0.jar
aws s3 cp db2jcc4.jar s3://your-glue-assets-bucket/jdbc-drivers/db2/11.5.8.0/db2jcc4.jar
```

### 2. Network Configuration

The system supports multiple network connectivity scenarios. Choose the appropriate configuration based on your database locations:

#### Same VPC (Default)
- No additional network configuration required
- Both databases accessible from Glue execution environment
- Simplest deployment option

#### Cross-VPC Configuration
- Required when databases are in different VPCs
- Requires additional CloudFormation parameters
- See [Network Configuration Guide](NETWORK_CONFIGURATION_GUIDE.md) for detailed setup

**For On-Premises Databases:**
- Configure VPN or Direct Connect to AWS
- Set up cross-VPC connectivity if database VPC differs from Glue VPC
- Configure appropriate security groups for database access
- Verify DNS resolution across network boundaries

**For RDS/Aurora Databases:**
- Identify VPC location of each database
- Configure cross-VPC parameters if databases are in different VPCs
- Set up security groups to allow Glue ENI access
- Configure S3 VPC endpoints if databases are in private subnets
- Verify subnet routing and availability zones

**Network Parameter Planning:**

Before deployment, gather the following information for each database in a different VPC:

| Information Required | Example | Purpose |
|---------------------|---------|----------|
| VPC ID | `vpc-12345678` | Identifies target VPC |
| Subnet IDs | `subnet-123,subnet-456` | ENI placement locations |
| Security Group IDs | `sg-database123` | Network access control |
| Subnet Type | Private/Public | Determines S3 VPC endpoint need |
| Availability Zones | `us-east-1a,us-east-1b` | High availability planning |

### 3. Database Preparation

**Source Database:**
- Verify user has read permissions on required tables
- Test JDBC connectivity from a similar environment
- Ensure database is accessible during planned migration windows

**Target Database:**
- Create target schema and tables (schema migration assumed complete)
- Verify user has write permissions
- Test JDBC connectivity
- Ensure sufficient storage space

## Deployment Process

### Step 1: Template Validation

Always validate the CloudFormation template before deployment:

```bash
aws cloudformation validate-template \
  --template-body file://cloudformation/glue-data-replication.yaml
```

### Step 2: Parameter File Creation

Create a parameters file based on your environment and network configuration. Choose the appropriate template based on your network scenario:

#### Same VPC Configuration (Default)

```json
[
  {
    "ParameterKey": "JobName",
    "ParameterValue": "prod-oracle-to-postgres"
  },
  {
    "ParameterKey": "SourceEngineType",
    "ParameterValue": "oracle"
  },
  {
    "ParameterKey": "TargetEngineType",
    "ParameterValue": "postgresql"
  },
  {
    "ParameterKey": "SourceDatabase",
    "ParameterValue": "PRODDB"
  },
  {
    "ParameterKey": "TargetDatabase",
    "ParameterValue": "analytics_db"
  },
  {
    "ParameterKey": "SourceSchema",
    "ParameterValue": "SALES"
  },
  {
    "ParameterKey": "TargetSchema",
    "ParameterValue": "public"
  },
  {
    "ParameterKey": "TableNames",
    "ParameterValue": "customers,orders,order_items,products"
  },
  {
    "ParameterKey": "SourceDbUser",
    "ParameterValue": "replication_user"
  },
  {
    "ParameterKey": "SourceDbPassword",
    "ParameterValue": "your-secure-source-password"
  },
  {
    "ParameterKey": "TargetDbUser",
    "ParameterValue": "postgres"
  },
  {
    "ParameterKey": "TargetDbPassword",
    "ParameterValue": "your-secure-target-password"
  },
  {
    "ParameterKey": "SourceConnectionString",
    "ParameterValue": "jdbc:oracle:thin:@prod-oracle.company.com:1521:PRODDB"
  },
  {
    "ParameterKey": "TargetConnectionString",
    "ParameterValue": "jdbc:postgresql://analytics-postgres.company.com:5432/analytics_db"
  },
  {
    "ParameterKey": "SourceJdbcDriverS3Path",
    "ParameterValue": "s3://company-glue-assets/jdbc-drivers/oracle/21.7.0.0/ojdbc11.jar"
  },
  {
    "ParameterKey": "TargetJdbcDriverS3Path",
    "ParameterValue": "s3://company-glue-assets/jdbc-drivers/postgresql/42.6.0/postgresql-42.6.0.jar"
  }
]
```

#### Cross-VPC Configuration Example

```json
[
  {
    "ParameterKey": "JobName",
    "ParameterValue": "cross-vpc-oracle-to-postgres"
  },
  {
    "ParameterKey": "SourceEngineType",
    "ParameterValue": "oracle"
  },
  {
    "ParameterKey": "TargetEngineType",
    "ParameterValue": "postgresql"
  },
  {
    "ParameterKey": "SourceDatabase",
    "ParameterValue": "PRODDB"
  },
  {
    "ParameterKey": "TargetDatabase",
    "ParameterValue": "analytics_db"
  },
  {
    "ParameterKey": "SourceSchema",
    "ParameterValue": "SALES"
  },
  {
    "ParameterKey": "TargetSchema",
    "ParameterValue": "public"
  },
  {
    "ParameterKey": "TableNames",
    "ParameterValue": "customers,orders,order_items,products"
  },
  {
    "ParameterKey": "SourceDbUser",
    "ParameterValue": "replication_user"
  },
  {
    "ParameterKey": "SourceDbPassword",
    "ParameterValue": "your-secure-source-password"
  },
  {
    "ParameterKey": "TargetDbUser",
    "ParameterValue": "postgres"
  },
  {
    "ParameterKey": "TargetDbPassword",
    "ParameterValue": "your-secure-target-password"
  },
  {
    "ParameterKey": "SourceConnectionString",
    "ParameterValue": "jdbc:oracle:thin:@prod-oracle.company.com:1521:PRODDB"
  },
  {
    "ParameterKey": "TargetConnectionString",
    "ParameterValue": "jdbc:postgresql://analytics-postgres.company.com:5432/analytics_db"
  },
  {
    "ParameterKey": "SourceJdbcDriverS3Path",
    "ParameterValue": "s3://company-glue-assets/jdbc-drivers/oracle/21.7.0.0/ojdbc11.jar"
  },
  {
    "ParameterKey": "TargetJdbcDriverS3Path",
    "ParameterValue": "s3://company-glue-assets/jdbc-drivers/postgresql/42.6.0/postgresql-42.6.0.jar"
  },
  {
    "ParameterKey": "SourceVpcId",
    "ParameterValue": "vpc-prod-oracle-123"
  },
  {
    "ParameterKey": "SourceSubnetIds",
    "ParameterValue": "subnet-prod-db-1a,subnet-prod-db-1b"
  },
  {
    "ParameterKey": "SourceSecurityGroupIds",
    "ParameterValue": "sg-oracle-glue-access"
  },
  {
    "ParameterKey": "CreateSourceS3VpcEndpoint",
    "ParameterValue": "YES"
  },
  {
    "ParameterKey": "TargetVpcId",
    "ParameterValue": "vpc-analytics-456"
  },
  {
    "ParameterKey": "TargetSubnetIds",
    "ParameterValue": "subnet-analytics-1a,subnet-analytics-1b"
  },
  {
    "ParameterKey": "TargetSecurityGroupIds",
    "ParameterValue": "sg-postgres-glue-access"
  },
  {
    "ParameterKey": "CreateTargetS3VpcEndpoint",
    "ParameterValue": "NO"
  }
]
```

#### Network Parameter Guidelines

**When to include network parameters:**
- Source database is in a different VPC than Glue execution environment
- Target database is in a different VPC than Glue execution environment
- Databases are in private subnets requiring S3 VPC endpoints

**Parameter selection criteria:**

| Parameter | Required When | Example Value |
|-----------|---------------|---------------|
| `SourceVpcId` | Source DB in different VPC | `vpc-12345678` |
| `SourceSubnetIds` | Source DB in different VPC | `subnet-123,subnet-456` |
| `SourceSecurityGroupIds` | Source DB in different VPC | `sg-database-access` |
| `CreateSourceS3VpcEndpoint` | Source DB in private subnet | `YES` or `NO` |
| `TargetVpcId` | Target DB in different VPC | `vpc-87654321` |
| `TargetSubnetIds` | Target DB in different VPC | `subnet-789,subnet-012` |
| `TargetSecurityGroupIds` | Target DB in different VPC | `sg-database-access` |
| `CreateTargetS3VpcEndpoint` | Target DB in private subnet | `YES` or `NO` |

### Step 3: Stack Deployment

Deploy the CloudFormation stack:

```bash
# Create new stack
aws cloudformation create-stack \
  --stack-name glue-data-replication-prod \
  --template-body file://cloudformation/glue-data-replication.yaml \
  --parameters file://parameters/prod-parameters.json \
  --capabilities CAPABILITY_NAMED_IAM \
  --tags Key=Environment,Value=Production Key=Project,Value=DataReplication

# Monitor deployment progress
aws cloudformation describe-stack-events \
  --stack-name glue-data-replication-prod \
  --query 'StackEvents[?ResourceStatus!=`CREATE_COMPLETE`]'
```

### Step 4: Deployment Verification

Verify the deployment was successful:

```bash
# Check stack status
aws cloudformation describe-stacks \
  --stack-name glue-data-replication-prod \
  --query 'Stacks[0].StackStatus'

# List created resources
aws cloudformation list-stack-resources \
  --stack-name glue-data-replication-prod

# Verify Glue job creation
aws glue get-job --job-name prod-oracle-to-postgres
```

## Post-Deployment Configuration

### 1. Initial Job Execution

Run the job for the first time to perform full-load migration:

```bash
# Start job run
JOB_RUN_ID=$(aws glue start-job-run \
  --job-name prod-oracle-to-postgres \
  --query 'JobRunId' \
  --output text)

echo "Job run started with ID: $JOB_RUN_ID"

# Monitor job progress
aws glue get-job-run \
  --job-name prod-oracle-to-postgres \
  --run-id $JOB_RUN_ID \
  --query 'JobRun.JobRunState'
```

### 2. Schedule Configuration

Set up job scheduling using AWS Glue triggers:

```bash
# Create daily trigger
aws glue create-trigger \
  --name prod-oracle-to-postgres-daily \
  --type SCHEDULED \
  --schedule "cron(0 2 * * ? *)" \
  --actions JobName=prod-oracle-to-postgres \
  --start-on-creation
```

### 3. Monitoring Setup

Configure CloudWatch alarms for job monitoring:

```bash
# Create alarm for job failures
aws cloudwatch put-metric-alarm \
  --alarm-name "GlueJob-prod-oracle-to-postgres-Failures" \
  --alarm-description "Alert on Glue job failures" \
  --metric-name "glue.driver.aggregate.numFailedTasks" \
  --namespace "AWS/Glue" \
  --statistic Sum \
  --period 300 \
  --threshold 1 \
  --comparison-operator GreaterThanOrEqualToThreshold \
  --dimensions Name=JobName,Value=prod-oracle-to-postgres \
  --evaluation-periods 1 \
  --alarm-actions arn:aws:sns:region:account:alert-topic
```

## Environment Management

### Development Environment

For development and testing:

```bash
# Use smaller instance types and reduced monitoring
aws cloudformation create-stack \
  --stack-name glue-data-replication-dev \
  --template-body file://cloudformation/glue-data-replication.yaml \
  --parameters file://parameters/dev-parameters.json \
  --capabilities CAPABILITY_NAMED_IAM \
  --tags Key=Environment,Value=Development
```

### Production Environment

For production deployments:

- Use dedicated VPC and subnets
- Enable detailed CloudWatch monitoring
- Configure backup and disaster recovery
- Implement proper change management processes

### Multi-Region Deployment

For multi-region setups:

```bash
# Deploy to multiple regions
for region in us-east-1 us-west-2 eu-west-1; do
  aws cloudformation create-stack \
    --region $region \
    --stack-name glue-data-replication-prod-$region \
    --template-body file://cloudformation/glue-data-replication.yaml \
    --parameters file://parameters/prod-$region-parameters.json \
    --capabilities CAPABILITY_NAMED_IAM
done
```

## Maintenance and Updates

### Stack Updates

Update existing stacks when configuration changes:

```bash
# Update stack with new parameters
aws cloudformation update-stack \
  --stack-name glue-data-replication-prod \
  --template-body file://cloudformation/glue-data-replication.yaml \
  --parameters file://parameters/prod-parameters-updated.json \
  --capabilities CAPABILITY_NAMED_IAM
```

### JDBC Driver Updates

Update JDBC drivers by uploading new versions to S3 and updating stack parameters:

```bash
# Upload new driver version
aws s3 cp ojdbc11-new-version.jar s3://company-glue-assets/jdbc-drivers/oracle/21.8.0.0/ojdbc11.jar

# Update stack with new driver path
# (Update parameters file with new S3 path and run stack update)
```

### Job Code Updates

Update the PySpark job code:

```bash
# Upload updated script to S3
aws s3 cp scripts/glue_data_replication.py s3://company-glue-assets/scripts/

# Update Glue job
aws glue update-job \
  --job-name prod-oracle-to-postgres \
  --job-update ScriptLocation=s3://company-glue-assets/scripts/glue_data_replication.py
```

## Troubleshooting

### Common Deployment Issues

1. **IAM Permission Errors**
   ```
   Error: User is not authorized to perform: iam:CreateRole
   ```
   - Verify DevOps policy is correctly applied
   - Check policy resource ARNs match your account

2. **CloudFormation Template Errors**
   ```
   Error: Template format error
   ```
   - Validate template syntax
   - Check parameter constraints

3. **Resource Naming Conflicts**
   ```
   Error: Role already exists
   ```
   - Use unique job names
   - Clean up previous failed deployments

### Network Connectivity Troubleshooting

#### Pre-Deployment Network Validation

Before deploying cross-VPC configurations, validate network connectivity:

```bash
# Test database connectivity from EC2 instance in target subnet
# 1. Launch test EC2 instance
aws ec2 run-instances \
  --image-id ami-12345678 \
  --instance-type t3.micro \
  --subnet-id subnet-target-123 \
  --security-group-ids sg-test-connectivity

# 2. Test database connection
ssh ec2-user@test-instance
telnet oracle-host 1521
telnet postgres-host 5432

# 3. Test S3 connectivity (if using VPC endpoint)
aws s3 ls s3://your-jdbc-bucket/
```

#### Common Network Issues

**1. Glue Connection Creation Failures**

```bash
# Verify VPC and subnet configuration
aws ec2 describe-vpcs --vpc-ids vpc-12345678
aws ec2 describe-subnets --subnet-ids subnet-12345678

# Check security group rules
aws ec2 describe-security-groups --group-ids sg-12345678

# Validate availability zones
aws ec2 describe-availability-zones --zone-names us-east-1a
```

**2. ENI Creation Issues**

```bash
# Check available IP addresses in subnet
aws ec2 describe-subnets --subnet-ids subnet-12345678 \
  --query 'Subnets[0].AvailableIpAddressCount'

# Check service limits
aws service-quotas get-service-quota \
  --service-code ec2 \
  --quota-code L-DF5E4CA3

# List existing ENIs in subnet
aws ec2 describe-network-interfaces \
  --filters "Name=subnet-id,Values=subnet-12345678"
```

**3. S3 VPC Endpoint Issues**

```bash
# Check existing VPC endpoints
aws ec2 describe-vpc-endpoints \
  --filters "Name=vpc-id,Values=vpc-12345678"

# Verify route table associations
aws ec2 describe-route-tables \
  --filters "Name=association.subnet-id,Values=subnet-12345678"

# Test S3 access from private subnet
# (Run from EC2 instance in private subnet)
curl -I https://s3.amazonaws.com/your-bucket/test-file
```

**4. Security Group Configuration**

```bash
# Create security group for database access
aws ec2 create-security-group \
  --group-name glue-database-access \
  --description "Allow Glue access to database" \
  --vpc-id vpc-12345678

# Add inbound rule for database port
aws ec2 authorize-security-group-ingress \
  --group-id sg-database \
  --protocol tcp \
  --port 1521 \
  --source-group sg-glue-eni

# Verify security group rules
aws ec2 describe-security-groups \
  --group-ids sg-database \
  --query 'SecurityGroups[0].IpPermissions'
```

#### Network Monitoring and Diagnostics

```bash
# Enable VPC Flow Logs for network troubleshooting
aws ec2 create-flow-logs \
  --resource-type VPC \
  --resource-ids vpc-12345678 \
  --traffic-type ALL \
  --log-destination-type cloud-watch-logs \
  --log-group-name VPCFlowLogs

# Monitor Glue connection usage
aws glue get-connection --name source-connection
aws glue get-connection --name target-connection

# Check CloudWatch logs for network errors
aws logs filter-log-events \
  --log-group-name /aws-glue/jobs/output \
  --filter-pattern "Connection" \
  --start-time $(date -d '1 hour ago' +%s)000
```

### Monitoring and Alerting

Set up comprehensive monitoring:

```bash
# Create dashboard for job monitoring
aws cloudwatch put-dashboard \
  --dashboard-name GlueDataReplication \
  --dashboard-body file://monitoring/dashboard.json

# Set up log group retention
aws logs put-retention-policy \
  --log-group-name /aws-glue/jobs/output \
  --retention-in-days 30
```

### Backup and Recovery

Implement backup strategies:

- **Configuration Backup**: Store parameter files in version control
- **State Backup**: Regular snapshots of job bookmark data
- **Code Backup**: Version control for all scripts and templates

## Security Best Practices

### Credential Management

- Use AWS Secrets Manager for database passwords
- Rotate credentials regularly
- Implement least-privilege access

### Network Security

- Use VPC endpoints for S3 access
- Implement security groups with minimal required access
- Enable VPC Flow Logs for network monitoring

### Audit and Compliance

- Enable CloudTrail for API call logging
- Use AWS Config for resource compliance monitoring
- Implement regular security assessments

## Cost Optimization

### Resource Sizing

- Monitor Glue job resource utilization
- Adjust worker types and counts based on workload
- Use spot instances where appropriate

### Scheduling Optimization

- Schedule jobs during off-peak hours
- Implement job dependencies to avoid resource conflicts
- Use incremental loading to reduce processing time

### Storage Optimization

- Implement S3 lifecycle policies for JDBC drivers
- Use appropriate S3 storage classes
- Clean up old job logs and artifacts

## Support and Escalation

### Internal Support Process

1. Check CloudWatch logs for detailed error information
2. Review this deployment guide for common issues
3. Consult AWS Glue documentation for service-specific problems
4. Escalate to AWS Support for infrastructure issues

### Documentation Updates

Keep this guide updated with:
- New deployment patterns
- Lessons learned from production deployments
- Updated IAM policies and permissions
- New troubleshooting scenarios

For questions or improvements to this guide, contact the DevOps team or submit a pull request to the repository.