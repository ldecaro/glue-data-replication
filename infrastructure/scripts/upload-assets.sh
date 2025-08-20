#!/bin/bash

# AWS Glue Data Replication - Asset Upload Script
# This script uploads the Glue job script and JDBC drivers to S3

set -e

# Configuration
BUCKET_NAME=""
SCRIPT_PATH="dist/main.py"
MODULES_PATH="dist/glue-job-modules.zip"
STANDALONE_PATH="dist/main-standalone.py"

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
    echo "  - Packaged Glue job modules to s3://bucket/glue-modules/glue-job-modules.zip"
    echo "  - Main Glue job script to s3://bucket/glue-scripts/main.py"
    echo "  - Standalone script (alternative) to s3://bucket/glue-scripts/main-standalone.py"
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

# Package modules if not already done
if [ ! -f "$MODULES_PATH" ] || [ ! -f "$SCRIPT_PATH" ]; then
    print_info "Packaging Glue job modules..."
    
    # Ensure dist directory exists
    mkdir -p dist
    
    # Create modules zip (excluding main.py)
    if [ -d "src/glue_job" ]; then
        print_info "Creating modules zip file..."
        CURRENT_DIR="$(pwd)"
        cd src && zip -r "$CURRENT_DIR/dist/glue-job-modules.zip" glue_job/ -x "glue_job/main.py" && cd ..
        
        print_info "Copying main script..."
        cp src/glue_job/main.py dist/
        
        # Create standalone version if it doesn't exist
        if [ ! -f "$STANDALONE_PATH" ]; then
            print_info "Creating standalone script..."
            cat > "$STANDALONE_PATH" << 'EOF'
#!/usr/bin/env python3
"""
Standalone AWS Glue Data Replication Script
This version includes sys.path modifications to work with uploaded modules.
"""

import sys
import os

# Add current directory and common paths to Python path for module discovery
sys.path.insert(0, '/tmp')
sys.path.insert(0, '/tmp/glue_job')
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Try to add the extracted zip location to path
try:
    import zipfile
    import tempfile
    
    # Look for the modules zip file in the same directory as the script
    script_dir = os.path.dirname(os.path.abspath(__file__))
    zip_path = os.path.join(script_dir, 'glue-job-modules.zip')
    
    if os.path.exists(zip_path):
        # Extract to temp directory and add to path
        temp_dir = tempfile.mkdtemp()
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(temp_dir)
        sys.path.insert(0, temp_dir)
        print(f"Extracted modules from {zip_path} to {temp_dir}")
except Exception as e:
    print(f"Warning: Could not extract modules zip: {e}")

EOF
            # Append the original main.py content, but skip the first few lines (shebang, docstring)
            tail -n +10 "src/glue_job/main.py" >> "$STANDALONE_PATH"
        fi
        
        print_success "Created Glue job packages"
    else
        print_error "Source directory 'src/glue_job' not found"
        echo "Please ensure you're running this script from the project root directory"
        echo "Expected structure:"
        echo "  src/"
        echo "    glue_job/"
        echo "      main.py"
        echo "      config/"
        echo "      database/"
        echo "      ..."
        exit 1
    fi
else
    print_info "Using existing packaged modules"
fi

# Upload Glue job modules
print_info "Uploading Glue job modules..."
aws s3 cp "$MODULES_PATH" "s3://$BUCKET_NAME/glue-modules/glue-job-modules.zip"
print_success "Uploaded modules to s3://$BUCKET_NAME/glue-modules/glue-job-modules.zip"

# Upload main script
print_info "Uploading main Glue job script..."
aws s3 cp "$SCRIPT_PATH" "s3://$BUCKET_NAME/glue-scripts/main.py"
print_success "Uploaded main script to s3://$BUCKET_NAME/glue-scripts/main.py"

# Upload standalone script (alternative)
if [ -f "$STANDALONE_PATH" ]; then
    print_info "Uploading standalone script (alternative)..."
    aws s3 cp "$STANDALONE_PATH" "s3://$BUCKET_NAME/glue-scripts/main-standalone.py"
    print_success "Uploaded standalone script to s3://$BUCKET_NAME/glue-scripts/main-standalone.py"
fi

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
echo "Deployment options:"
echo ""
echo "Option 1 (Recommended): Use modular approach with --extra-py-files"
echo "  - GlueJobScriptS3Path: s3://$BUCKET_NAME/glue-scripts/main.py"
echo "  - GlueJobModulesS3Path: s3://$BUCKET_NAME/glue-modules/glue-job-modules.zip"
echo ""
echo "Option 2: Use standalone script (simpler but larger)"
echo "  - GlueJobScriptS3Path: s3://$BUCKET_NAME/glue-scripts/main-standalone.py"
echo "  - GlueJobModulesS3Path: (leave empty)"
echo ""
echo "Next steps:"
echo "1. Update your parameter files with the appropriate S3 paths above"
echo "2. Deploy the CloudFormation stack"
echo "3. Run the Glue job"