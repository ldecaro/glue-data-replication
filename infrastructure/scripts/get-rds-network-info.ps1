# PowerShell script to get RDS network information for Glue job configuration
# Usage: .\get-rds-network-info.ps1 -RdsInstance "database-1"

param(
    [Parameter(Mandatory=$true)]
    [string]$RdsInstance
)

Write-Host "Getting network information for RDS instance: $RdsInstance" -ForegroundColor Green
Write-Host "==================================================" -ForegroundColor Green

try {
    # Get VPC ID
    Write-Host "VPC ID:" -ForegroundColor Yellow
    $VpcId = aws rds describe-db-instances --db-instance-identifier $RdsInstance --query 'DBInstances[0].DBSubnetGroup.VpcId' --output text
    Write-Host "  $VpcId"

    # Get Subnet IDs and Availability Zones
    Write-Host ""
    Write-Host "Subnet IDs and Availability Zones:" -ForegroundColor Yellow
    aws rds describe-db-instances --db-instance-identifier $RdsInstance --query 'DBInstances[0].DBSubnetGroup.Subnets[*].[SubnetIdentifier,AvailabilityZone.Name]' --output table

    $SubnetIds = aws rds describe-db-instances --db-instance-identifier $RdsInstance --query 'DBInstances[0].DBSubnetGroup.Subnets[*].SubnetIdentifier' --output text
    $SubnetIdsCommaSeparated = $SubnetIds -replace '\s+', ','
    Write-Host "Comma-separated: $SubnetIdsCommaSeparated"
    
    # Get first subnet's AZ for reference
    $FirstSubnetAz = aws rds describe-db-instances --db-instance-identifier $RdsInstance --query 'DBInstances[0].DBSubnetGroup.Subnets[0].AvailabilityZone.Name' --output text
    Write-Host "First subnet AZ: $FirstSubnetAz" -ForegroundColor Cyan
    
    # If AZ is None or empty, try to get it directly from the subnet
    if ($FirstSubnetAz -eq "None" -or $FirstSubnetAz -eq "" -or $FirstSubnetAz -eq $null) {
        Write-Host "AZ not found in RDS info, querying subnet directly..." -ForegroundColor Yellow
        $FirstSubnetId = ($SubnetIds -split '\s+')[0]
        if ($FirstSubnetId) {
            $FirstSubnetAz = aws ec2 describe-subnets --subnet-ids $FirstSubnetId --query 'Subnets[0].AvailabilityZone' --output text
            Write-Host "First subnet AZ (from EC2): $FirstSubnetAz" -ForegroundColor Cyan
        }
    }

    # Get Security Group IDs
    Write-Host ""
    Write-Host "Security Group IDs:" -ForegroundColor Yellow
    aws rds describe-db-instances --db-instance-identifier $RdsInstance --query 'DBInstances[0].VpcSecurityGroups[*].[VpcSecurityGroupId,Status]' --output table

    $SecurityGroupIds = aws rds describe-db-instances --db-instance-identifier $RdsInstance --query 'DBInstances[0].VpcSecurityGroups[*].VpcSecurityGroupId' --output text
    $SecurityGroupIdsCommaSeparated = $SecurityGroupIds -replace '\s+', ','
    Write-Host "Comma-separated: $SecurityGroupIdsCommaSeparated"

    Write-Host ""
    Write-Host "==================================================" -ForegroundColor Green
    Write-Host "Parameters for CloudFormation:" -ForegroundColor Green
    Write-Host "==================================================" -ForegroundColor Green
    Write-Host "SourceVpcId: $VpcId" -ForegroundColor Cyan
    Write-Host "SourceSubnetIds: $SubnetIdsCommaSeparated" -ForegroundColor Cyan
    Write-Host "SourceSecurityGroupIds: $SecurityGroupIdsCommaSeparated" -ForegroundColor Cyan
    Write-Host "SourceAvailabilityZone: $FirstSubnetAz" -ForegroundColor Cyan
    Write-Host "TargetVpcId: $VpcId" -ForegroundColor Cyan
    Write-Host "TargetSubnetIds: $SubnetIdsCommaSeparated" -ForegroundColor Cyan
    Write-Host "TargetSecurityGroupIds: $SecurityGroupIdsCommaSeparated" -ForegroundColor Cyan
    Write-Host "TargetAvailabilityZone: $FirstSubnetAz" -ForegroundColor Cyan

    Write-Host ""
    Write-Host "==================================================" -ForegroundColor Green
    Write-Host "Security Group Rules Check:" -ForegroundColor Green
    Write-Host "==================================================" -ForegroundColor Green
    
    $SecurityGroupList = $SecurityGroupIds -split '\s+'
    foreach ($sg in $SecurityGroupList) {
        if ($sg -ne "") {
            Write-Host "Security Group: $sg" -ForegroundColor Yellow
            Write-Host "Inbound rules:"
            aws ec2 describe-security-groups --group-ids $sg --query 'SecurityGroups[0].IpPermissions[*].[IpProtocol,FromPort,ToPort,IpRanges[0].CidrIp]' --output table
            Write-Host ""
        }
    }

    Write-Host ""
    Write-Host "==================================================" -ForegroundColor Green
    Write-Host "Next Steps:" -ForegroundColor Green
    Write-Host "==================================================" -ForegroundColor Green
    Write-Host "1. Update your parameters file with the values above"
    Write-Host "2. Ensure security group allows:"
    Write-Host "   - Inbound: Port 1433 (SQL Server) from Glue"
    Write-Host "   - Outbound: Port 443 (HTTPS) to 0.0.0.0/0 for S3"
    Write-Host "3. Redeploy CloudFormation stack"
    Write-Host "4. Test the Glue job"

} catch {
    Write-Error "Failed to get RDS network information: $_"
    Write-Host "Make sure:"
    Write-Host "1. AWS CLI is configured with proper credentials"
    Write-Host "2. RDS instance identifier '$RdsInstance' exists"
    Write-Host "3. You have permissions to describe RDS instances and security groups"
}