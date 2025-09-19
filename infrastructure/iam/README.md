# DevOps IAM Policy for AWS Glue Data Replication

## Overview

This directory contains the IAM policy required for DevOps engineers to deploy the AWS Glue Data Replication CloudFormation template. The policy follows the principle of least privilege and grants only the minimum permissions necessary for successful deployment.

## Policy File

- `devops-policy.json` - IAM policy document with minimum required permissions

## Policy Permissions

The DevOps policy grants the following permissions:

### CloudFormation Stack Operations
- Create, update, and delete CloudFormation stacks with names matching `glue-data-replication-*`
- Describe stack status, events, and resources
- Validate CloudFormation templates
- List stacks and stack resources

### IAM Role and Policy Management
- Create and manage IAM roles with names matching `GlueDataReplication*`
- Create and manage IAM policies with names matching `GlueDataReplication*`
- Attach/detach policies to roles
- Pass roles to AWS services (required for Glue job execution)

### AWS Glue Job Management
- Create, update, and delete Glue jobs with names matching `glue-data-replication-*`
- Get job details and list jobs
- Update job configurations

### S3 Access for JDBC Drivers
- Read access to S3 buckets containing JDBC drivers (buckets with `jdbc-drivers` in the name)
- Read access to S3 buckets for Glue assets (buckets with `glue-assets` in the name)
- List bucket contents and get bucket location

## Deployment Instructions

### Step 1: Create IAM Policy

1. Log in to the AWS Management Console with administrative privileges
2. Navigate to IAM > Policies
3. Click "Create policy"
4. Select the "JSON" tab
5. Copy and paste the contents of `devops-policy.json`
6. Click "Next: Tags" (add tags if desired)
7. Click "Next: Review"
8. Enter policy name: `GlueDataReplicationDevOpsPolicy`
9. Enter description: "Minimum permissions for deploying AWS Glue Data Replication CloudFormation template"
10. Click "Create policy"

### Step 2: Attach Policy to DevOps User/Role

**For IAM Users:**
1. Navigate to IAM > Users
2. Select the DevOps engineer's user account
3. Click "Add permissions"
4. Select "Attach existing policies directly"
5. Search for and select `GlueDataReplicationDevOpsPolicy`
6. Click "Next: Review" and then "Add permissions"

**For IAM Roles:**
1. Navigate to IAM > Roles
2. Select the DevOps role
3. Click "Attach policies"
4. Search for and select `GlueDataReplicationDevOpsPolicy`
5. Click "Attach policy"

### Step 3: Deploy CloudFormation Template

Once the policy is attached, DevOps engineers can deploy the CloudFormation template:

```bash
aws cloudformation create-stack \
  --stack-name glue-data-replication-example \
  --template-body file://infrastructure/cloudformation/glue-data-replication.yaml \
  --parameters ParameterKey=JobName,ParameterValue=example-job \
               ParameterKey=SourceEngineType,ParameterValue=oracle \
               ParameterKey=TargetEngineType,ParameterValue=postgresql \
               # ... other required parameters
  --capabilities CAPABILITY_NAMED_IAM
```

## Security Considerations

### Resource Naming Conventions
The policy uses specific naming patterns to limit scope:
- CloudFormation stacks must be named `glue-data-replication-*`
- IAM roles and policies must be named `GlueDataReplication*`
- Glue jobs must be named `glue-data-replication-*`

### S3 Bucket Access
- Access is limited to buckets containing `jdbc-drivers` or `glue-assets` in their names
- Only read permissions are granted (GetObject, ListBucket)
- No write or delete permissions on S3 resources

### Cross-Account Considerations
- The policy uses `*` for account IDs in ARNs to support cross-account deployments
- In production environments, consider restricting to specific account IDs
- Review and adjust resource ARNs based on your organization's naming conventions

## Troubleshooting

### Common Permission Issues

**Error: "User is not authorized to perform: iam:PassRole"**
- Ensure the IAM user/role has the `iam:PassRole` permission included in the policy
- Verify the resource ARN pattern matches your IAM role naming convention

**Error: "Access Denied when calling CreateStack"**
- Verify the CloudFormation stack name follows the `glue-data-replication-*` pattern
- Ensure the user has the `cloudformation:CreateStack` permission

**Error: "Access Denied when accessing S3 bucket"**
- Verify the S3 bucket name contains `jdbc-drivers` or `glue-assets`
- Check that the JDBC driver files exist in the specified S3 locations

### Policy Validation

To validate the policy before deployment:

```bash
aws iam simulate-principal-policy \
  --policy-source-arn arn:aws:iam::ACCOUNT-ID:user/USERNAME \
  --action-names cloudformation:CreateStack \
  --resource-arns arn:aws:cloudformation:us-east-1:ACCOUNT-ID:stack/glue-data-replication-test/*
```

## Policy Updates

When updating the CloudFormation template with new AWS resources:
1. Review the new resources and required permissions
2. Update the `devops-policy.json` file accordingly
3. Create a new version of the IAM policy
4. Test the updated policy in a development environment
5. Deploy the updated policy to production

## Support

For questions or issues related to the DevOps policy:
1. Review the CloudFormation template to understand required permissions
2. Check AWS CloudTrail logs for specific permission denials
3. Validate resource naming conventions match the policy patterns
4. Test policy changes in a development environment before production deployment