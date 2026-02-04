# Quick Start Guide - AWS Glue Data Replication

This guide walks you through deploying the modular data replication system step by step.

## Prerequisites Checklist

Before starting, ensure you have:
- [ ] AWS CLI installed and configured with appropriate permissions
- [ ] An S3 bucket for storing assets
- [ ] Source and target databases (RDS instances or accessible databases)
- [ ] JDBC drivers downloaded for your database types
- [ ] PowerShell (Windows) or Bash (Linux/macOS) terminal access

## Step-by-Step Deployment

### Step 1: Clone and Setup
```bash
git clone <repository-url>
cd aws-glue-data-replication
```

### Step 2: Deploy with Automatic Upload

```bash
# Complete deployment in one command
./deploy.sh -s my-data-replication -b your-bucket -p my-parameters.json
```

**Optional: Manual Asset Upload First**
```bash
# Upload the complete modular structure
./infrastructure/scripts/upload-modular-assets.sh your-bucket --include-drivers

# Then deploy without upload
./deploy.sh -s my-data-replication -b your-bucket -p my-parameters.json --skip-upload
```

### Step 3: Get Network Configuration (VPC Databases Only)

**🔍 Do I need this step?**
- **YES** - If your databases are RDS instances in a VPC (most production setups)
- **NO** - If your databases are publicly accessible (not recommended for production)

**How to check**: In AWS RDS Console, look at your database's "Connectivity & security" tab. If it shows a VPC ID, you need network configuration.

#### Option A: Use the PowerShell Script (Windows)
```powershell
# Replace "database-1" with your actual RDS instance identifier
.\scripts\get-rds-network-info.ps1 -RdsInstance "database-1"
```

**Expected Output:**
```
SourceVpcId: vpc-12345678
SourceSubnetIds: subnet-abc123,subnet-def456  
SourceSecurityGroupIds: sg-87654321
SourceAvailabilityZone: us-east-1a
```

#### Option B: Manual Commands (If Script Fails)
```bash
# 1. Get your RDS instance's subnet IDs
aws rds describe-db-instances --db-instance-identifier database-1 --query 'DBInstances[0].DBSubnetGroup.Subnets[*].SubnetIdentifier' --output text

# 2. Get VPC ID  
aws rds describe-db-instances --db-instance-identifier database-1 --query 'DBInstances[0].DBSubnetGroup.VpcId' --output text

# 3. Get security group IDs
aws rds describe-db-instances --db-instance-identifier database-1 --query 'DBInstances[0].VpcSecurityGroups[*].VpcSecurityGroupId' --output text

# 4. Get availability zone (replace subnet-12345678 with first subnet from step 1)
aws ec2 describe-subnets --subnet-ids subnet-12345678 --query 'Subnets[0].AvailabilityZone' --output text
```

#### Option C: Use AWS Console
1. Go to RDS Console → Databases → Your Database
2. Click "Connectivity & security" tab
3. Note the VPC ID and Subnets
4. Go to VPC Console → Subnets → Find your subnet → Note the Availability Zone

### Step 4: Create Parameters File

Copy and customize the example parameters file:

```bash
# Copy the example
cp examples/sqlserver-to-sqlserver-parameters-simple.json my-parameters.json
```

Edit `my-parameters.json` and replace these values:

#### Required Changes:
```json
{
  "ParameterKey": "JobName",
  "ParameterValue": "my-unique-job-name"  // Change this
},
{
  "ParameterKey": "SourceConnectionString", 
  "ParameterValue": "jdbc:sqlserver://YOUR-RDS-ENDPOINT:1433;databaseName=YOUR-DB"  // Change this
},
{
  "ParameterKey": "TargetConnectionString",
  "ParameterValue": "jdbc:sqlserver://YOUR-RDS-ENDPOINT:1433;databaseName=YOUR-DB"  // Change this  
},
{
  "ParameterKey": "SourceJdbcDriverS3Path",
  "ParameterValue": "s3://YOUR-BUCKET/jdbc-drivers/sqlserver/12.2.0.jre11/mssql-jdbc-12.2.0.jre11.jar"  // Change this
},
{
  "ParameterKey": "GlueJobScriptS3Path", 
  "ParameterValue": "s3://YOUR-BUCKET/src/glue_job/main.py"  // UPDATED: New modular entry point
}
```

**Important**: The `GlueJobScriptS3Path` now points to the new modular entry point at `src/glue_job/main.py` instead of the old monolithic script.

#### Network Configuration (If you did Step 3):
Replace these placeholder values with the actual values from Step 3:
```json
{
  "ParameterKey": "SourceVpcId",
  "ParameterValue": "vpc-12345678"  // From Step 3
},
{
  "ParameterKey": "SourceSubnetIds",
  "ParameterValue": "subnet-abc123,subnet-def456"  // From Step 3
},
{
  "ParameterKey": "SourceSecurityGroupIds", 
  "ParameterValue": "sg-87654321"  // From Step 3
},
{
  "ParameterKey": "SourceAvailabilityZone",
  "ParameterValue": "us-east-1a"  // From Step 3
}
```

**If you don't need network configuration**: Remove or leave empty the network-related parameters (SourceVpcId, SourceSubnetIds, etc.).

### Step 5: Deploy CloudFormation Stack

The deployment is handled automatically by the deploy script in Step 2. If you need to deploy separately:

```bash
./deploy.sh -s my-data-replication -b your-bucket -p my-parameters.json
```

**Monitor deployment:**
The deploy script will automatically wait for completion and show the results.

### Step 6: Run the Glue Job

```bash
# Start the job
aws glue start-job-run --job-name my-unique-job-name

# Monitor job status
aws glue get-job-runs --job-name my-unique-job-name --query 'JobRuns[0].JobRunState'
```

## Common Issues and Solutions

### Issue: "AvailabilityZone cannot be null or empty"
**Solution**: You need to provide network configuration. Go back to Step 3 and get the availability zone for your subnet.

### Issue: "Unable to resolve any valid connection"  
**Solution**: Your databases are in a VPC but you didn't provide network configuration. Complete Step 3 and redeploy.

### Issue: Script returns "None" for availability zone
**Solution**: Use the manual commands in Step 3, Option B, or check the AWS Console (Option C).

### Issue: "Connection timed out"
**Solution**: Check that your security groups allow inbound traffic on the database port (1433 for SQL Server) from the Glue job.

### Issue: "Access Denied" on S3
**Solution**: Verify your S3 bucket name and paths are correct in the parameters file.

## Verification

After successful deployment, verify everything works:

1. **Check CloudFormation**: Stack should show "CREATE_COMPLETE"
2. **Check Glue Job**: Should appear in AWS Glue Console
3. **Check Connections**: If you provided network config, connections should be created
4. **Run Test**: Start a job run and monitor the logs

## Next Steps

- Monitor job execution in CloudWatch Logs
- Set up CloudWatch alarms for job failures
- Configure incremental loading for ongoing replication (see [Bookmark Details](docs/BOOKMARK_DETAILS.md))
- Review the [Observability Guide](docs/OBSERVABILITY_GUIDE.md) for monitoring setup

## Getting Help

If you encounter issues:
1. Check the [Troubleshooting](#troubleshooting) section in README.md
2. Review CloudWatch logs for detailed error messages
3. Verify all parameters are correctly set
4. Ensure security groups allow required network traffic

## Summary Checklist

- [ ] Assets uploaded to S3
- [ ] Network configuration obtained (if needed)
- [ ] Parameters file customized with your values
- [ ] CloudFormation stack deployed successfully
- [ ] Glue job created and accessible
- [ ] Test job run completed successfully

Once all items are checked, your data replication system is ready for production use!
</text>
</invoke>