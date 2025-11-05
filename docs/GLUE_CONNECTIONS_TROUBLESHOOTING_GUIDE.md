# AWS Glue Connections and Secrets Manager Troubleshooting Guide

This guide provides comprehensive troubleshooting information for AWS Glue Connections and AWS Secrets Manager integration in the AWS Glue Data Replication solution.

## Table of Contents

1. [Common Issues Overview](#common-issues-overview)
2. [Glue Connection Issues](#glue-connection-issues)
3. [AWS Secrets Manager Issues](#aws-secrets-manager-issues)
4. [Parameter Validation Issues](#parameter-validation-issues)
5. [Network and Connectivity Issues](#network-and-connectivity-issues)
6. [Permission and IAM Issues](#permission-and-iam-issues)
7. [Diagnostic Tools and Commands](#diagnostic-tools-and-commands)
8. [Best Practices for Prevention](#best-practices-for-prevention)

## Common Issues Overview

### Issue Categories

| Category | Frequency | Impact | Typical Resolution Time |
|----------|-----------|--------|------------------------|
| Parameter Validation | High | Low | < 5 minutes |
| IAM Permissions | Medium | High | 15-30 minutes |
| Network Connectivity | Medium | High | 30-60 minutes |
| Glue Connection Creation | Low | Medium | 10-20 minutes |
| Secrets Manager Integration | Low | Medium | 15-30 minutes |

### Quick Diagnostic Checklist

Before diving into specific troubleshooting, verify these common requirements:

- [ ] AWS CLI is configured with appropriate credentials
- [ ] IAM role has required permissions for Glue and Secrets Manager
- [ ] VPC and security group configurations allow database access
- [ ] Database is accessible from the Glue job's network environment
- [ ] Parameter file contains valid and non-conflicting parameters

## Glue Connection Issues

### Connection Creation Failures

#### Issue: Failed to create Glue Connection
```
Error: Failed to create Glue Connection 'my-source-connection'
Cause: InvalidInputException: Connection input is invalid
```

**Root Causes:**
1. Invalid JDBC connection string format
2. Missing required connection properties
3. Invalid VPC or subnet configuration
4. Security group restrictions

**Diagnostic Steps:**
```bash
# Verify JDBC connection string format
echo "JDBC URL: jdbc:oracle:thin:@hostname:1521:SID"
echo "JDBC URL: jdbc:postgresql://hostname:5432/database"
echo "JDBC URL: jdbc:sqlserver://hostname:1433;databaseName=mydb"

# Test Glue Connection creation manually
aws glue create-connection \
    --connection-input '{
        "Name": "test-connection",
        "ConnectionType": "JDBC",
        "ConnectionProperties": {
            "JDBC_CONNECTION_URL": "jdbc:postgresql://myhost:5432/mydb",
            "USERNAME": "myuser",
            "PASSWORD": "mypassword"
        }
    }'
```

**Solutions:**
1. **Validate JDBC URL Format:**
   ```bash
   # Oracle format
   jdbc:oracle:thin:@hostname:port:SID
   jdbc:oracle:thin:@hostname:port/SERVICE_NAME
   
   # PostgreSQL format
   jdbc:postgresql://hostname:port/database
   
   # SQL Server format
   jdbc:sqlserver://hostname:port;databaseName=database
   
   # DB2 format
   jdbc:db2://hostname:port/database
   ```

2. **Check Network Configuration:**
   ```bash
   # Verify VPC and subnet exist
   aws ec2 describe-vpcs --vpc-ids vpc-12345678
   aws ec2 describe-subnets --subnet-ids subnet-12345678
   
   # Check security group rules
   aws ec2 describe-security-groups --group-ids sg-12345678
   ```

3. **Validate Connection Properties:**
   ```json
   {
     "JDBC_CONNECTION_URL": "jdbc:postgresql://myhost:5432/mydb",
     "USERNAME": "myuser",
     "PASSWORD": "mypassword",
     "JDBC_ENFORCE_SSL": "false"
   }
   ```

#### Issue: Connection timeout during creation
```
Error: Connection creation timed out after 300 seconds
```

**Root Causes:**
1. Network connectivity issues
2. Database server not responding
3. Security group blocking access
4. VPC routing problems

**Diagnostic Steps:**
```bash
# Test network connectivity from Glue environment
# Create a test Glue job to verify connectivity
aws glue start-job-run \
    --job-name connectivity-test \
    --arguments '{
        "--database-host": "myhost",
        "--database-port": "5432"
    }'

# Check VPC route tables
aws ec2 describe-route-tables --filters "Name=vpc-id,Values=vpc-12345678"
```

**Solutions:**
1. **Verify Security Group Rules:**
   ```bash
   # Add outbound rule for database port
   aws ec2 authorize-security-group-egress \
       --group-id sg-12345678 \
       --protocol tcp \
       --port 5432 \
       --cidr 10.0.0.0/8
   ```

2. **Check Database Server Status:**
   ```bash
   # Test database connectivity
   telnet database-host 5432
   nc -zv database-host 5432
   ```

3. **Validate VPC Configuration:**
   - Ensure subnets have proper routing to database
   - Verify NAT Gateway or Internet Gateway configuration
   - Check VPC endpoints for AWS services

### Connection Usage Failures

#### Issue: Glue Connection not found
```
Error: Glue Connection 'my-connection' does not exist
```

**Root Causes:**
1. Connection name typo or case sensitivity
2. Connection in different AWS region
3. Connection in different AWS account
4. Connection was deleted

**Diagnostic Steps:**
```bash
# List all Glue Connections
aws glue get-connections

# Search for connection by name pattern
aws glue get-connections --filter '{"ConnectionType": "JDBC"}'

# Check specific connection
aws glue get-connection --name "my-connection"
```

**Solutions:**
1. **Verify Connection Name:**
   ```bash
   # List connections with grep
   aws glue get-connections --query 'ConnectionList[].Name' --output text | grep -i connection
   ```

2. **Check Region and Account:**
   ```bash
   # Verify current region
   aws configure get region
   
   # Check account ID
   aws sts get-caller-identity
   ```

3. **Recreate Connection if Necessary:**
   ```bash
   # Create connection with correct parameters
   aws glue create-connection --connection-input file://connection-config.json
   ```

#### Issue: Connection validation failed
```
Error: Connection validation failed for 'my-connection'
Cause: Unable to establish JDBC connection
```

**Root Causes:**
1. Invalid database credentials
2. Database server unavailable
3. Network connectivity issues
4. SSL/TLS configuration problems

**Diagnostic Steps:**
```bash
# Test connection manually
aws glue get-connection --name "my-connection"

# Check connection properties
aws glue get-connection --name "my-connection" \
    --query 'Connection.ConnectionProperties'
```

**Solutions:**
1. **Validate Credentials:**
   ```bash
   # Test database login manually
   psql -h hostname -p 5432 -U username -d database
   ```

2. **Check SSL Configuration:**
   ```json
   {
     "JDBC_ENFORCE_SSL": "true",
     "CUSTOM_JDBC_CERT": "s3://bucket/path/to/cert.pem"
   }
   ```

3. **Update Connection Properties:**
   ```bash
   aws glue update-connection \
       --name "my-connection" \
       --connection-input file://updated-connection.json
   ```

## AWS Secrets Manager Issues

### Secret Creation Failures

#### Issue: Failed to create AWS Secrets Manager secret
```
Error: Failed to create AWS Secrets Manager secret for connection 'my-connection'
Cause: AccessDenied: User is not authorized to perform secretsmanager:CreateSecret
```

**Root Causes:**
1. Insufficient IAM permissions
2. Secret name already exists
3. KMS key access denied
4. Service unavailable

**Diagnostic Steps:**
```bash
# Check current IAM permissions
aws iam get-role-policy --role-name GlueJobExecutionRole --policy-name SecretsManagerPolicy

# Test secret creation manually
aws secretsmanager create-secret \
    --name "/aws-glue/test-secret" \
    --description "Test secret for troubleshooting" \
    --secret-string '{"username":"test","password":"test"}'

# List existing secrets
aws secretsmanager list-secrets --filters Key=name,Values=/aws-glue/
```

**Solutions:**
1. **Add Required IAM Permissions:**
   ```json
   {
     "Version": "2012-10-17",
     "Statement": [
       {
         "Effect": "Allow",
         "Action": [
           "secretsmanager:CreateSecret",
           "secretsmanager:GetSecretValue",
           "secretsmanager:DescribeSecret",
           "secretsmanager:DeleteSecret"
         ],
         "Resource": "arn:aws:secretsmanager:*:*:secret:/aws-glue/*"
       }
     ]
   }
   ```

2. **Check Secret Name Conflicts:**
   ```bash
   # Check if secret already exists
   aws secretsmanager describe-secret --secret-id "/aws-glue/my-connection"
   
   # Delete existing secret if needed
   aws secretsmanager delete-secret \
       --secret-id "/aws-glue/my-connection" \
       --force-delete-without-recovery
   ```

3. **Validate KMS Permissions:**
   ```json
   {
     "Effect": "Allow",
     "Action": [
       "kms:Decrypt",
       "kms:GenerateDataKey"
     ],
     "Resource": "arn:aws:kms:*:*:key/*",
     "Condition": {
       "StringEquals": {
         "kms:ViaService": "secretsmanager.*.amazonaws.com"
       }
     }
   }
   ```

#### Issue: Secret cleanup failed after connection creation failure
```
Error: Failed to cleanup secret '/aws-glue/my-connection' after connection creation failure
```

**Root Causes:**
1. Insufficient delete permissions
2. Secret is in use by other resources
3. Secret has deletion protection enabled

**Diagnostic Steps:**
```bash
# Check secret status
aws secretsmanager describe-secret --secret-id "/aws-glue/my-connection"

# List secret versions
aws secretsmanager list-secret-version-ids --secret-id "/aws-glue/my-connection"
```

**Solutions:**
1. **Manual Secret Cleanup:**
   ```bash
   # Force delete secret
   aws secretsmanager delete-secret \
       --secret-id "/aws-glue/my-connection" \
       --force-delete-without-recovery
   ```

2. **Check Resource Dependencies:**
   ```bash
   # Find resources using the secret
   aws resourcegroupstaggingapi get-resources \
       --resource-type-filters "secretsmanager:secret" \
       --tag-filters Key=Purpose,Values=GlueConnection
   ```

### Secret Access Issues

#### Issue: Access denied when retrieving secret
```
Error: Access denied when accessing secret '/aws-glue/my-connection'
Cause: User is not authorized to perform secretsmanager:GetSecretValue
```

**Root Causes:**
1. Missing GetSecretValue permission
2. Resource-based policy restrictions
3. KMS key access denied
4. Cross-account access issues

**Diagnostic Steps:**
```bash
# Test secret access
aws secretsmanager get-secret-value --secret-id "/aws-glue/my-connection"

# Check secret policy
aws secretsmanager describe-secret --secret-id "/aws-glue/my-connection" \
    --query 'ResourcePolicy'

# Verify KMS key permissions
aws kms describe-key --key-id alias/aws/secretsmanager
```

**Solutions:**
1. **Add GetSecretValue Permission:**
   ```json
   {
     "Effect": "Allow",
     "Action": [
       "secretsmanager:GetSecretValue",
       "secretsmanager:DescribeSecret"
     ],
     "Resource": "arn:aws:secretsmanager:*:*:secret:/aws-glue/*"
   }
   ```

2. **Update Secret Resource Policy:**
   ```bash
   aws secretsmanager put-resource-policy \
       --secret-id "/aws-glue/my-connection" \
       --resource-policy file://secret-policy.json
   ```

3. **Validate KMS Access:**
   ```bash
   # Test KMS key access
   aws kms decrypt --ciphertext-blob fileb://test-encrypted-data
   ```

## Parameter Validation Issues

### Mutual Exclusivity Errors

#### Issue: Conflicting connection parameters
```
Error: Cannot specify both createSourceConnection and useSourceConnection
```

**Root Causes:**
1. Both create and use parameters specified
2. Parameter file contains conflicting values
3. CloudFormation template error

**Diagnostic Steps:**
```bash
# Check parameter file for conflicts
grep -E "(create|use)(Source|Target)Connection" parameters.json

# Validate CloudFormation parameters
aws cloudformation validate-template --template-body file://template.yaml
```

**Solutions:**
1. **Use Only One Strategy Per Connection:**
   ```json
   {
     "createSourceConnection": "true",
     "useTargetConnection": "existing-target-connection"
   }
   ```

2. **Remove Conflicting Parameters:**
   ```json
   // Correct - Create source, use existing target
   {
     "createSourceConnection": "true",
     "useTargetConnection": "my-target-connection"
   }
   
   // Incorrect - Conflicting parameters
   {
     "createSourceConnection": "true",
     "useSourceConnection": "my-source-connection"
   }
   ```

### Parameter Format Errors

#### Issue: Invalid parameter format
```
Error: Parameter 'createSourceConnection' must be 'true' or 'false'
```

**Root Causes:**
1. Incorrect boolean format
2. Case sensitivity issues
3. Extra whitespace or characters

**Diagnostic Steps:**
```bash
# Check parameter values
jq '.[] | select(.ParameterKey | contains("Connection"))' parameters.json

# Validate JSON format
python -m json.tool parameters.json
```

**Solutions:**
1. **Use Correct Boolean Format:**
   ```json
   {
     "ParameterKey": "createSourceConnection",
     "ParameterValue": "true"
   }
   ```

2. **Validate Parameter File:**
   ```bash
   # Check for common issues
   grep -n "true\|false" parameters.json | grep -v '"true"\|"false"'
   ```

## Network and Connectivity Issues

### VPC Configuration Problems

#### Issue: Glue job cannot reach database through Glue Connection
```
Error: Connection timeout when using Glue Connection 'my-connection'
```

**Root Causes:**
1. Incorrect VPC configuration in Glue Connection
2. Security group rules blocking access
3. Route table configuration issues
4. NAT Gateway or Internet Gateway problems

**Diagnostic Steps:**
```bash
# Check Glue Connection network configuration
aws glue get-connection --name "my-connection" \
    --query 'Connection.PhysicalConnectionRequirements'

# Verify VPC and subnet configuration
aws ec2 describe-vpcs --vpc-ids vpc-12345678
aws ec2 describe-subnets --subnet-ids subnet-12345678

# Check route tables
aws ec2 describe-route-tables --filters "Name=vpc-id,Values=vpc-12345678"
```

**Solutions:**
1. **Update Glue Connection Network Configuration:**
   ```json
   {
     "PhysicalConnectionRequirements": {
       "SubnetId": "subnet-12345678",
       "SecurityGroupIdList": ["sg-12345678"],
       "AvailabilityZone": "us-east-1a"
     }
   }
   ```

2. **Configure Security Group Rules:**
   ```bash
   # Add outbound rule for database port
   aws ec2 authorize-security-group-egress \
       --group-id sg-12345678 \
       --protocol tcp \
       --port 5432 \
       --source-group sg-database-sg
   ```

3. **Verify Route Table Configuration:**
   ```bash
   # Check routes to database subnet
   aws ec2 describe-route-tables \
       --route-table-ids rtb-12345678 \
       --query 'RouteTables[].Routes'
   ```

### Cross-VPC Connectivity Issues

#### Issue: Cannot connect to database in different VPC
```
Error: Network unreachable when connecting to cross-VPC database
```

**Root Causes:**
1. Missing VPC peering connection
2. Incorrect route table configuration
3. Security group rules not allowing cross-VPC access
4. DNS resolution issues

**Diagnostic Steps:**
```bash
# Check VPC peering connections
aws ec2 describe-vpc-peering-connections \
    --filters "Name=status-code,Values=active"

# Verify DNS resolution
nslookup database-hostname

# Test connectivity from Glue environment
# (Create test Glue job for network testing)
```

**Solutions:**
1. **Configure VPC Peering:**
   ```bash
   # Create VPC peering connection
   aws ec2 create-vpc-peering-connection \
       --vpc-id vpc-source \
       --peer-vpc-id vpc-target
   
   # Accept peering connection
   aws ec2 accept-vpc-peering-connection \
       --vpc-peering-connection-id pcx-12345678
   ```

2. **Update Route Tables:**
   ```bash
   # Add route to peer VPC
   aws ec2 create-route \
       --route-table-id rtb-12345678 \
       --destination-cidr-block 10.1.0.0/16 \
       --vpc-peering-connection-id pcx-12345678
   ```

3. **Configure Cross-VPC Security Groups:**
   ```bash
   # Allow access from source VPC security group
   aws ec2 authorize-security-group-ingress \
       --group-id sg-database \
       --protocol tcp \
       --port 5432 \
       --source-group sg-glue-connection
   ```

## Permission and IAM Issues

### Insufficient Glue Permissions

#### Issue: Access denied for Glue Connection operations
```
Error: User is not authorized to perform glue:CreateConnection
```

**Root Causes:**
1. Missing Glue service permissions
2. Resource-level restrictions
3. Condition-based policy restrictions
4. Cross-account access issues

**Diagnostic Steps:**
```bash
# Check current IAM role policies
aws iam list-attached-role-policies --role-name GlueJobExecutionRole

# Get policy details
aws iam get-policy-version \
    --policy-arn arn:aws:iam::123456789012:policy/GlueJobPolicy \
    --version-id v1

# Test Glue permissions
aws glue get-connections --max-results 1
```

**Solutions:**
1. **Add Required Glue Permissions:**
   ```json
   {
     "Version": "2012-10-17",
     "Statement": [
       {
         "Effect": "Allow",
         "Action": [
           "glue:CreateConnection",
           "glue:GetConnection",
           "glue:GetConnections",
           "glue:UpdateConnection",
           "glue:DeleteConnection"
         ],
         "Resource": "*"
       }
     ]
   }
   ```

2. **Attach AWS Managed Policy:**
   ```bash
   aws iam attach-role-policy \
       --role-name GlueJobExecutionRole \
       --policy-arn arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole
   ```

### Cross-Service Permission Issues

#### Issue: Glue cannot access Secrets Manager
```
Error: Glue Connection cannot retrieve credentials from Secrets Manager
```

**Root Causes:**
1. Missing cross-service permissions
2. KMS key access denied
3. Resource-based policy restrictions
4. VPC endpoint configuration issues

**Diagnostic Steps:**
```bash
# Test Secrets Manager access from Glue role
aws sts assume-role --role-arn arn:aws:iam::123456789012:role/GlueJobExecutionRole \
    --role-session-name test-session

# Test secret access with assumed role
aws secretsmanager get-secret-value --secret-id "/aws-glue/my-connection"
```

**Solutions:**
1. **Add Cross-Service Permissions:**
   ```json
   {
     "Version": "2012-10-17",
     "Statement": [
       {
         "Effect": "Allow",
         "Action": [
           "secretsmanager:GetSecretValue"
         ],
         "Resource": "arn:aws:secretsmanager:*:*:secret:/aws-glue/*"
       },
       {
         "Effect": "Allow",
         "Action": [
           "kms:Decrypt"
         ],
         "Resource": "*",
         "Condition": {
           "StringEquals": {
             "kms:ViaService": "secretsmanager.*.amazonaws.com"
           }
         }
       }
     ]
   }
   ```

2. **Configure VPC Endpoint for Secrets Manager:**
   ```bash
   aws ec2 create-vpc-endpoint \
       --vpc-id vpc-12345678 \
       --service-name com.amazonaws.us-east-1.secretsmanager \
       --vpc-endpoint-type Interface \
       --subnet-ids subnet-12345678 \
       --security-group-ids sg-12345678
   ```

## Diagnostic Tools and Commands

### Glue Connection Diagnostics

```bash
# List all Glue Connections
aws glue get-connections --output table

# Get specific connection details
aws glue get-connection --name "my-connection" --output json

# Test connection creation
aws glue create-connection --connection-input '{
  "Name": "test-connection",
  "ConnectionType": "JDBC",
  "ConnectionProperties": {
    "JDBC_CONNECTION_URL": "jdbc:postgresql://localhost:5432/test",
    "USERNAME": "test",
    "PASSWORD": "test"
  }
}'

# Delete test connection
aws glue delete-connection --connection-name "test-connection"
```

### Secrets Manager Diagnostics

```bash
# List secrets with Glue prefix
aws secretsmanager list-secrets \
    --filters Key=name,Values=/aws-glue/ \
    --output table

# Get secret details
aws secretsmanager describe-secret --secret-id "/aws-glue/my-connection"

# Test secret creation
aws secretsmanager create-secret \
    --name "/aws-glue/test-secret" \
    --secret-string '{"username":"test","password":"test"}'

# Test secret retrieval
aws secretsmanager get-secret-value --secret-id "/aws-glue/test-secret"

# Clean up test secret
aws secretsmanager delete-secret \
    --secret-id "/aws-glue/test-secret" \
    --force-delete-without-recovery
```

### Network Diagnostics

```bash
# Check VPC configuration
aws ec2 describe-vpcs --vpc-ids vpc-12345678

# Check subnet configuration
aws ec2 describe-subnets --subnet-ids subnet-12345678

# Check security groups
aws ec2 describe-security-groups --group-ids sg-12345678

# Check route tables
aws ec2 describe-route-tables --filters "Name=vpc-id,Values=vpc-12345678"

# Check VPC endpoints
aws ec2 describe-vpc-endpoints --filters "Name=vpc-id,Values=vpc-12345678"
```

### IAM Permission Diagnostics

```bash
# Check role policies
aws iam list-attached-role-policies --role-name GlueJobExecutionRole

# Get policy document
aws iam get-role-policy --role-name GlueJobExecutionRole --policy-name InlinePolicy

# Simulate policy evaluation
aws iam simulate-principal-policy \
    --policy-source-arn arn:aws:iam::123456789012:role/GlueJobExecutionRole \
    --action-names glue:CreateConnection \
    --resource-arns "*"
```

## Best Practices for Prevention

### Parameter Configuration

1. **Use Parameter Validation:**
   ```json
   {
     "Parameters": {
       "createSourceConnection": {
         "Type": "String",
         "AllowedValues": ["true", "false"],
         "Default": "false"
       }
     }
   }
   ```

2. **Implement Parameter Constraints:**
   ```yaml
   Conditions:
     CreateSourceConnection: !Equals [!Ref createSourceConnection, "true"]
     UseSourceConnection: !Not [!Equals [!Ref useSourceConnection, ""]]
     
   Rules:
     MutuallyExclusiveSourceConnection:
       RuleCondition: !And
         - !Condition CreateSourceConnection
         - !Condition UseSourceConnection
       Assertions:
         - Assert: false
           AssertDescription: "Cannot specify both createSourceConnection and useSourceConnection"
   ```

### Security Configuration

1. **Use Least Privilege IAM Policies:**
   ```json
   {
     "Version": "2012-10-17",
     "Statement": [
       {
         "Effect": "Allow",
         "Action": [
           "glue:GetConnection"
         ],
         "Resource": "arn:aws:glue:*:*:connection/glue-data-replication-*"
       }
     ]
   }
   ```

2. **Implement Resource Tagging:**
   ```json
   {
     "Tags": [
       {"Key": "Project", "Value": "GlueDataReplication"},
       {"Key": "Environment", "Value": "Production"},
       {"Key": "Owner", "Value": "DataTeam"}
     ]
   }
   ```

### Network Configuration

1. **Use Dedicated Security Groups:**
   ```bash
   # Create dedicated security group for Glue Connections
   aws ec2 create-security-group \
       --group-name glue-connection-sg \
       --description "Security group for Glue Connections"
   ```

2. **Implement VPC Endpoints:**
   ```bash
   # Create VPC endpoint for Secrets Manager
   aws ec2 create-vpc-endpoint \
       --vpc-id vpc-12345678 \
       --service-name com.amazonaws.region.secretsmanager \
       --vpc-endpoint-type Interface
   ```

### Monitoring and Alerting

1. **Set Up CloudWatch Alarms:**
   ```bash
   aws cloudwatch put-metric-alarm \
       --alarm-name "GlueConnectionFailures" \
       --alarm-description "Alert on Glue Connection failures" \
       --metric-name "ConnectionFailures" \
       --namespace "AWS/Glue" \
       --statistic Sum \
       --period 300 \
       --threshold 1 \
       --comparison-operator GreaterThanOrEqualToThreshold
   ```

2. **Configure CloudTrail Logging:**
   ```json
   {
     "EventSelectors": [
       {
         "ReadWriteType": "All",
         "IncludeManagementEvents": true,
         "DataResources": [
           {
             "Type": "AWS::Glue::Connection",
             "Values": ["arn:aws:glue:*:*:connection/*"]
           },
           {
             "Type": "AWS::SecretsManager::Secret",
             "Values": ["arn:aws:secretsmanager:*:*:secret:/aws-glue/*"]
           }
         ]
       }
     ]
   }
   ```

This troubleshooting guide provides comprehensive coverage of common issues and their resolutions for AWS Glue Connections and Secrets Manager integration. Regular review and updates of this guide will help maintain its effectiveness as the system evolves.