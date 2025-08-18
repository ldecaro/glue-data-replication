#!/bin/bash

# AWS Glue Data Replication - Asset Upload Script
# This script uploads the Glue job script and JDBC drivers to S3

set -e

# Configuration
BUCKET_NAME=""
SCRIPT_PATH="scripts/glue_data_replication.py"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to print colored output
print_info() {
    echo -e "${YELLOW}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Function to show usage
show_usage() {
    echo "Usage: $0 <s3-bucket-name>"
    echo ""
    echo "Example:"
    echo "  $0 my-glue-assets-bucket"
    echo ""
    echo "This script uploads:"
    echo "  - Glue job script to s3://bucket/scripts/glue_data_replication.py"
    echo "  - Optionally upload JDBC drivers (you'll be prompted)"
}

# Check if bucket name is provided
if [ $# -eq 0 ]; then
    print_error "S3 bucket name is required"
    show_usage
    exit 1
fi

BUCKET_NAME=$1

# Validate bucket name format
if [[ ! $BUCKET_NAME =~ ^[a-z0-9][a-z0-9-]*[a-z0-9]$ ]]; then
    print_error "Invalid S3 bucket name format"
    echo "Bucket names must:"
    echo "  - Be 3-63 characters long"
    echo "  - Contain only lowercase letters, numbers, and hyphens"
    echo "  - Start and end with a letter or number"
    exit 1
fi

print_info "Starting asset upload to S3 bucket: $BUCKET_NAME"

# Check if AWS CLI is configured
if ! aws sts get-caller-identity > /dev/null 2>&1; then
    print_error "AWS CLI is not configured or credentials are invalid"
    echo "Please run 'aws configure' to set up your credentials"
    exit 1
fi

# Check if bucket exists, create if it doesn't
print_info "Checking if S3 bucket exists..."
if ! aws s3 ls "s3://$BUCKET_NAME" > /dev/null 2>&1; then
    print_info "Bucket doesn't exist. Creating S3 bucket: $BUCKET_NAME"
    
    # Get AWS region
    AWS_REGION=$(aws configure get region)
    if [ -z "$AWS_REGION" ]; then
        AWS_REGION="us-east-1"
        print_info "No region configured, using default: $AWS_REGION"
    fi
    
    if [ "$AWS_REGION" = "us-east-1" ]; then
        aws s3 mb "s3://$BUCKET_NAME"
    else
        aws s3 mb "s3://$BUCKET_NAME" --region "$AWS_REGION"
    fi
    
    print_success "Created S3 bucket: $BUCKET_NAME"
else
    print_success "S3 bucket exists: $BUCKET_NAME"
fi

# Upload Glue job script
print_info "Uploading Glue job script..."
if [ ! -f "$SCRIPT_PATH" ]; then
    print_error "Glue job script not found: $SCRIPT_PATH"
    echo "Please ensure you're running this script from the project root directory"
    exit 1
fi

aws s3 cp "$SCRIPT_PATH" "s3://$BUCKET_NAME/scripts/glue_data_replication.py"
print_success "Uploaded Glue job script to s3://$BUCKET_NAME/scripts/glue_data_replication.py"

# Ask about JDBC drivers
echo ""
read -p "Do you want to upload JDBC drivers? (y/N): " upload_drivers

if [[ $upload_drivers =~ ^[Yy]$ ]]; then
    print_info "JDBC driver upload selected"
    
    # Check for common JDBC driver files
    drivers_found=false
    
    # Oracle driver
    if [ -f "ojdbc11.jar" ]; then
        print_info "Found Oracle JDBC driver: ojdbc11.jar"
        aws s3 cp ojdbc11.jar "s3://$BUCKET_NAME/jdbc-drivers/oracle/21.7.0.0/ojdbc11.jar"
        print_success "Uploaded Oracle JDBC driver"
        drivers_found=true
    fi
    
    # SQL Server driver
    if ls mssql-jdbc-*.jar 1> /dev/null 2>&1; then
        for driver in mssql-jdbc-*.jar; do
            print_info "Found SQL Server JDBC driver: $driver"
            version=$(echo "$driver" | sed 's/mssql-jdbc-\(.*\)\.jar/\1/')
            aws s3 cp "$driver" "s3://$BUCKET_NAME/jdbc-drivers/sqlserver/$version/$driver"
            print_success "Uploaded SQL Server JDBC driver"
            drivers_found=true
        done
    fi
    
    # PostgreSQL driver
    if ls postgresql-*.jar 1> /dev/null 2>&1; then
        for driver in postgresql-*.jar; do
            print_info "Found PostgreSQL JDBC driver: $driver"
            version=$(echo "$driver" | sed 's/postgresql-\(.*\)\.jar/\1/')
            aws s3 cp "$driver" "s3://$BUCKET_NAME/jdbc-drivers/postgresql/$version/$driver"
            print_success "Uploaded PostgreSQL JDBC driver"
            drivers_found=true
        done
    fi
    
    # DB2 driver
    if [ -f "db2jcc4.jar" ]; then
        print_info "Found DB2 JDBC driver: db2jcc4.jar"
        aws s3 cp db2jcc4.jar "s3://$BUCKET_NAME/jdbc-drivers/db2/11.5.8.0/db2jcc4.jar"
        print_success "Uploaded DB2 JDBC driver"
        drivers_found=true
    fi
    
    if [ "$drivers_found" = false ]; then
        print_info "No JDBC driver files found in current directory"
        echo "Please download and place JDBC drivers in the current directory, then run:"
        echo ""
        echo "# Oracle"
        echo "aws s3 cp ojdbc11.jar s3://$BUCKET_NAME/jdbc-drivers/oracle/21.7.0.0/ojdbc11.jar"
        echo ""
        echo "# SQL Server"
        echo "aws s3 cp mssql-jdbc-12.2.0.jre11.jar s3://$BUCKET_NAME/jdbc-drivers/sqlserver/12.2.0.jre11/mssql-jdbc-12.2.0.jre11.jar"
        echo ""
        echo "# PostgreSQL"
        echo "aws s3 cp postgresql-42.6.0.jar s3://$BUCKET_NAME/jdbc-drivers/postgresql/42.6.0/postgresql-42.6.0.jar"
        echo ""
        echo "# DB2"
        echo "aws s3 cp db2jcc4.jar s3://$BUCKET_NAME/jdbc-drivers/db2/11.5.8.0/db2jcc4.jar"
    fi
else
    print_info "Skipping JDBC driver upload"
    echo "You can upload JDBC drivers later using the commands in the documentation"
fi

# Show final S3 structure
echo ""
print_info "Current S3 bucket structure:"
aws s3 ls "s3://$BUCKET_NAME" --recursive --human-readable

echo ""
print_success "Asset upload completed!"
echo ""
echo "Next steps:"
echo "1. Update your parameter files to use: s3://$BUCKET_NAME"
echo "2. Deploy the CloudFormation stack"
echo "3. Run the Glue job"