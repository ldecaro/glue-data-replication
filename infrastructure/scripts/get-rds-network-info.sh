#!/bin/bash

# Script to get RDS network information for Glue job configuration
# Usage: ./get-rds-network-info.sh <rds-instance-identifier>

if [ $# -eq 0 ]; then
    echo "Usage: $0 <rds-instance-identifier>"
    echo "Example: $0 database-1"
    exit 1
fi

RDS_INSTANCE="$1"

echo "Getting network information for RDS instance: $RDS_INSTANCE"
echo "=================================================="

# Get VPC ID
echo "VPC ID:"
VPC_ID=$(aws rds describe-db-instances \
  --db-instance-identifier "$RDS_INSTANCE" \
  --query 'DBInstances[0].DBSubnetGroup.VpcId' \
  --output text)
echo "  $VPC_ID"

# Get Subnet IDs
echo ""
echo "Subnet IDs:"
aws rds describe-db-instances \
  --db-instance-identifier "$RDS_INSTANCE" \
  --query 'DBInstances[0].DBSubnetGroup.Subnets[*].[SubnetIdentifier,AvailabilityZone.Name]' \
  --output table

SUBNET_IDS=$(aws rds describe-db-instances \
  --db-instance-identifier "$RDS_INSTANCE" \
  --query 'DBInstances[0].DBSubnetGroup.Subnets[*].SubnetIdentifier' \
  --output text | tr '\t' ',')
echo "Comma-separated: $SUBNET_IDS"

# Get Security Group IDs
echo ""
echo "Security Group IDs:"
aws rds describe-db-instances \
  --db-instance-identifier "$RDS_INSTANCE" \
  --query 'DBInstances[0].VpcSecurityGroups[*].[VpcSecurityGroupId,Status]' \
  --output table

SECURITY_GROUP_IDS=$(aws rds describe-db-instances \
  --db-instance-identifier "$RDS_INSTANCE" \
  --query 'DBInstances[0].VpcSecurityGroups[*].VpcSecurityGroupId' \
  --output text | tr '\t' ',')
echo "Comma-separated: $SECURITY_GROUP_IDS"

echo ""
echo "=================================================="
echo "Parameters for CloudFormation:"
echo "=================================================="
echo "SourceVpcId: $VPC_ID"
echo "SourceSubnetIds: $SUBNET_IDS"
echo "SourceSecurityGroupIds: $SECURITY_GROUP_IDS"
echo "TargetVpcId: $VPC_ID"
echo "TargetSubnetIds: $SUBNET_IDS"
echo "TargetSecurityGroupIds: $SECURITY_GROUP_IDS"

echo ""
echo "=================================================="
echo "Security Group Rules Check:"
echo "=================================================="
for sg in $(echo $SECURITY_GROUP_IDS | tr ',' ' '); do
    echo "Security Group: $sg"
    echo "Inbound rules:"
    aws ec2 describe-security-groups \
      --group-ids "$sg" \
      --query 'SecurityGroups[0].IpPermissions[*].[IpProtocol,FromPort,ToPort,IpRanges[0].CidrIp]' \
      --output table
    echo ""
done
</text>
</invoke>