#!/bin/bash

# AWS Glue Data Replication - Modular Asset Upload Script
# This script uploads the refactored Glue job modules and JDBC drivers to S3

set -e

# Configuration
BUCKET_NAME=""
SRC_DIR="src"
MAIN_SCRIPT="src/glue_job/main.py"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Function to show usage
show_usage() {
    cat << EOF
AWS Glue Data Replication - Modular Asset Upload Script

Usage: $0 <s3-bucket-name> [options]

Required:
    s3-bucket-name          S3 bucket name for asset storage

Options:
    --include-drivers       Also upload JDBC drivers (interactive)
    --dry-run              Show what would be uploaded without executing
    -h, --help             Show this help message

Examples:
    $0 my-glue-assets-bucket
    $0 my-glue-assets-bucket --include-drivers
    $0 my-glue-assets-bucket --dry-run

This script uploads the complete modular Glue job structure:
    - src/glue_job/main.py (main entry point)
    - src/glue_job/config/ (configuration modules)
    - src/glue_job/database/ (database modules)
    - src/glue_job/monitoring/ (monitoring modules)
    - src/glue_job/network/ (network modules)
    - src/glue_job/storage/ (storage modules)
    - src/glue_job/utils/ (utility modules)
    - Optionally: JDBC drivers

The main script will be uploaded to: s3://bucket/src/glue_job/main.py
EOF
}

# Parse command line arguments
INCLUDE_DRIVERS=false
DRY_RUN=false

while [[ $# -gt 0 ]]; do
    case $1 in
        --include-drivers)
            INCLUDE_DRIVERS=true
            shift
            ;;
        --dry-run)
            DRY_RUN=true
            shift
            ;;
        -h|--help)
            show_usage
            exit 0
            ;;
        -*)
            print_error "Unknown option: $1"
            show_usage
            exit 1
            ;;
        *)
            if [[ -z "$BUCKET_NAME" ]]; then
                BUCKET_NAME="$1"
            else
                print_error "Too many arguments"
                show_usage
                exit 1
            fi
            shift
            ;;
    esac
done

# Check if bucket name is provided
if [[ -z "$BUCKET_NAME" ]]; then
    print_error "S3 bucket name is required"
    show_usage
    exit 1
fi

# Validate bucket name format
if [[ ! $BUCKET_NAME =~ ^[a-z0-9][a-z0-9-]*[a-z0-9]$ ]]; then
    print_error "Invalid S3 bucket name format"
    echo "Bucket names must:"
    echo "  - Be 3-63 characters long"
    echo "  - Contain only lowercase letters, numbers, and hyphens"
    echo "  - Start and end with a letter or number"
    exit 1
fi

print_info "Starting modular asset upload to S3 bucket: $BUCKET_NAME"

# Check if we're in the right directory
if [[ ! -d "$SRC_DIR" ]]; then
    print_error "Source directory not found: $SRC_DIR"
    echo "Please ensure you're running this script from the project root directory"
    exit 1
fi

if [[ ! -f "$MAIN_SCRIPT" ]]; then
    print_error "Main Glue script not found: $MAIN_SCRIPT"
    echo "Please ensure the project has been refactored to the new modular structure"
    exit 1
fi

# Check if AWS CLI is configured (skip in dry-run mode)
if [[ "$DRY_RUN" == false ]]; then
    if ! aws sts get-caller-identity > /dev/null 2>&1; then
        print_error "AWS CLI is not configured or credentials are invalid"
        echo "Please run 'aws configure' to set up your credentials"
        exit 1
    fi
fi

# Dry run mode - show what would be uploaded
if [[ "$DRY_RUN" == true ]]; then
    print_info "DRY RUN MODE - Would upload the following files:"
    echo ""
    
    print_info "Python modules from $SRC_DIR/:"
    find "$SRC_DIR" -name "*.py" -type f | while read -r file; do
        s3_path="s3://$BUCKET_NAME/$file"
        echo "  $file -> $s3_path"
    done
    
    if [[ "$INCLUDE_DRIVERS" == true ]]; then
        echo ""
        print_info "JDBC drivers (if found):"
        
        # Check for common JDBC driver files
        if [[ -f "ojdbc11.jar" ]]; then
            echo "  ojdbc11.jar -> s3://$BUCKET_NAME/jdbc-drivers/oracle/21.7.0.0/ojdbc11.jar"
        fi
        
        if ls mssql-jdbc-*.jar 1> /dev/null 2>&1; then
            for driver in mssql-jdbc-*.jar; do
                version=$(echo "$driver" | sed 's/mssql-jdbc-\(.*\)\.jar/\1/')
                echo "  $driver -> s3://$BUCKET_NAME/jdbc-drivers/sqlserver/$version/$driver"
            done
        fi
        
        if ls postgresql-*.jar 1> /dev/null 2>&1; then
            for driver in postgresql-*.jar; do
                version=$(echo "$driver" | sed 's/postgresql-\(.*\)\.jar/\1/')
                echo "  $driver -> s3://$BUCKET_NAME/jdbc-drivers/postgresql/$version/$driver"
            done
        fi
        
        if [[ -f "db2jcc4.jar" ]]; then
            echo "  db2jcc4.jar -> s3://$BUCKET_NAME/jdbc-drivers/db2/11.5.8.0/db2jcc4.jar"
        fi
    fi
    
    echo ""
    print_success "Dry run completed"
    exit 0
fi

# Check if bucket exists, create if it doesn't
print_info "Checking if S3 bucket exists..."
if ! aws s3 ls "s3://$BUCKET_NAME" > /dev/null 2>&1; then
    print_info "Bucket doesn't exist. Creating S3 bucket: $BUCKET_NAME"
    
    # Get AWS region
    AWS_REGION=$(aws configure get region)
    if [[ -z "$AWS_REGION" ]]; then
        AWS_REGION="us-east-1"
        print_info "No region configured, using default: $AWS_REGION"
    fi
    
    if [[ "$AWS_REGION" = "us-east-1" ]]; then
        aws s3 mb "s3://$BUCKET_NAME"
    else
        aws s3 mb "s3://$BUCKET_NAME" --region "$AWS_REGION"
    fi
    
    print_success "Created S3 bucket: $BUCKET_NAME"
else
    print_success "S3 bucket exists: $BUCKET_NAME"
fi

# Upload all Python modules from src/ directory
print_info "Uploading Python modules from $SRC_DIR/..."
uploaded_count=0

find "$SRC_DIR" -name "*.py" -type f | while read -r file; do
    print_info "Uploading: $file"
    aws s3 cp "$file" "s3://$BUCKET_NAME/$file"
    if [[ $? -eq 0 ]]; then
        echo "  ✓ Uploaded: $file -> s3://$BUCKET_NAME/$file"
        ((uploaded_count++))
    else
        print_error "Failed to upload: $file"
        exit 1
    fi
done

print_success "Uploaded Python modules from $SRC_DIR/"

# Upload JDBC drivers if requested
if [[ "$INCLUDE_DRIVERS" == true ]]; then
    print_info "Uploading JDBC drivers..."
    
    drivers_found=false
    
    # Oracle driver
    if [[ -f "ojdbc11.jar" ]]; then
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
    if [[ -f "db2jcc4.jar" ]]; then
        print_info "Found DB2 JDBC driver: db2jcc4.jar"
        aws s3 cp db2jcc4.jar "s3://$BUCKET_NAME/jdbc-drivers/db2/11.5.8.0/db2jcc4.jar"
        print_success "Uploaded DB2 JDBC driver"
        drivers_found=true
    fi
    
    if [[ "$drivers_found" == false ]]; then
        print_warning "No JDBC driver files found in current directory"
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
fi

# Show final S3 structure
echo ""
print_info "Current S3 bucket structure:"
aws s3 ls "s3://$BUCKET_NAME" --recursive --human-readable

echo ""
print_success "Modular asset upload completed!"
echo ""
print_info "Key uploaded files:"
echo "  - Main script: s3://$BUCKET_NAME/src/glue_job/main.py"
echo "  - Configuration modules: s3://$BUCKET_NAME/src/glue_job/config/"
echo "  - Database modules: s3://$BUCKET_NAME/src/glue_job/database/"
echo "  - Monitoring modules: s3://$BUCKET_NAME/src/glue_job/monitoring/"
echo "  - Network modules: s3://$BUCKET_NAME/src/glue_job/network/"
echo "  - Storage modules: s3://$BUCKET_NAME/src/glue_job/storage/"
echo "  - Utility modules: s3://$BUCKET_NAME/src/glue_job/utils/"
echo ""
echo "Next steps:"
echo "1. Update your parameter files to use: s3://$BUCKET_NAME/src/glue_job/main.py"
echo "2. Deploy the CloudFormation stack using deploy.sh"
echo "3. Run the Glue job to test the modular structure"
echo ""
echo "Example parameter update:"
echo '{'
echo '  "ParameterKey": "GlueJobScriptS3Path",'
echo "  \"ParameterValue\": \"s3://$BUCKET_NAME/src/glue_job/main.py\""
echo '}'