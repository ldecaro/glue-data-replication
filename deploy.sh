#!/bin/bash

# AWS Glue Data Replication Deployment Script
# This script automates the deployment of the data replication system

set -e  # Exit on any error

# Default values
STACK_NAME=""
PARAMETERS_FILE=""
REGION="us-east-1"
PROFILE=""
VALIDATE_ONLY=false
UPDATE_STACK=false
DRY_RUN=false
BUCKET_NAME=""
SKIP_UPLOAD=false

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
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

# Function to suggest unique bucket names
suggest_bucket_names() {
    local base_name="$1"
    local account_id="$2"
    
    print_status "Here are some unique bucket name suggestions:"
    echo "  1. ${base_name}-${account_id}"
    echo "  2. ${base_name}-$(date +%Y%m%d-%H%M%S)"
    echo "  3. ${base_name}-$(openssl rand -hex 4 2>/dev/null || echo "$(date +%s)")"
    echo "  4. ${account_id}-${base_name}"
    echo "  5. ${base_name}-glue-$(date +%Y%m%d)"
}

# Function to show usage
show_usage() {
    cat << EOF
AWS Glue Data Replication Deployment Script

Usage: $0 [OPTIONS]

Required Options:
    -s, --stack-name STACK_NAME     Name of the CloudFormation stack
    -p, --parameters-file FILE      Path to parameters JSON file
    -b, --bucket BUCKET_NAME        S3 bucket for assets (will be created if needed)

Optional Options:
    -r, --region REGION             AWS region (default: us-east-1)
    --profile PROFILE               AWS CLI profile (default: default)
    -u, --update                    Update existing stack instead of creating new
    -v, --validate-only             Only validate template, don't deploy
    -d, --dry-run                   Show what would be deployed without executing
    --skip-upload                   Skip S3 upload (assume assets already uploaded)
    -h, --help                      Show this help message

Examples:
    # Deploy new stack with automatic uploads
    $0 -s my-replication-stack -p examples/oracle-to-postgresql-parameters.json -b my-glue-assets

    # Update existing stack
    $0 -s my-replication-stack -p examples/oracle-to-postgresql-parameters.json -b my-glue-assets --update

    # Deploy without uploading (assets already in S3)
    $0 -s my-replication-stack -p examples/oracle-to-postgresql-parameters.json -b my-glue-assets --skip-upload

    # Validate template only
    $0 -s my-replication-stack -p examples/oracle-to-postgresql-parameters.json -b my-glue-assets --validate-only

This script will:
1. Upload CloudFormation template to S3
2. Upload modular Glue job structure to S3
3. Update parameter file with correct S3 paths
4. Deploy or update the CloudFormation stack

EOF
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -s|--stack-name)
            STACK_NAME="$2"
            shift 2
            ;;
        -p|--parameters-file)
            PARAMETERS_FILE="$2"
            shift 2
            ;;
        -b|--bucket)
            BUCKET_NAME="$2"
            shift 2
            ;;
        -r|--region)
            REGION="$2"
            shift 2
            ;;
        --profile)
            PROFILE="$2"
            shift 2
            ;;
        -u|--update)
            UPDATE_STACK=true
            shift
            ;;
        -v|--validate-only)
            VALIDATE_ONLY=true
            shift
            ;;
        -d|--dry-run)
            DRY_RUN=true
            shift
            ;;
        --skip-upload)
            SKIP_UPLOAD=true
            shift
            ;;
        -h|--help)
            show_usage
            exit 0
            ;;
        *)
            print_error "Unknown option: $1"
            show_usage
            exit 1
            ;;
    esac
done

# Validate required parameters
if [[ -z "$STACK_NAME" ]]; then
    print_error "Stack name is required. Use -s or --stack-name option."
    show_usage
    exit 1
fi

if [[ -z "$PARAMETERS_FILE" ]]; then
    print_error "Parameters file is required. Use -p or --parameters-file option."
    show_usage
    exit 1
fi

if [[ -z "$BUCKET_NAME" ]]; then
    print_error "S3 bucket name is required. Use -b or --bucket option."
    show_usage
    exit 1
fi

# Check if files exist
TEMPLATE_FILE="infrastructure/cloudformation/glue-data-replication.yaml"
if [[ ! -f "$TEMPLATE_FILE" ]]; then
    print_error "CloudFormation template not found: $TEMPLATE_FILE"
    exit 1
fi

if [[ ! -f "$PARAMETERS_FILE" ]]; then
    print_error "Parameters file not found: $PARAMETERS_FILE"
    exit 1
fi

# Check if the new modular structure exists
MAIN_SCRIPT="src/glue_job/main.py"
if [[ ! -f "$MAIN_SCRIPT" ]]; then
    print_error "Main Glue script not found: $MAIN_SCRIPT"
    print_error "Please ensure the project has been refactored to the new modular structure"
    exit 1
fi

# Set AWS CLI options
if [[ -n "$PROFILE" ]]; then
    AWS_CLI_OPTS="--region $REGION --profile $PROFILE"
else
    AWS_CLI_OPTS="--region $REGION"
fi

print_status "Starting deployment process..."
print_status "Stack Name: $STACK_NAME"
print_status "Parameters File: $PARAMETERS_FILE"
print_status "S3 Bucket: $BUCKET_NAME"
print_status "Region: $REGION"
print_status "Profile: $PROFILE"

# Validate AWS CLI configuration
print_status "Validating AWS CLI configuration..."
if ! aws sts get-caller-identity $AWS_CLI_OPTS > /dev/null 2>&1; then
    print_error "AWS CLI not configured properly or invalid credentials"
    exit 1
fi

ACCOUNT_ID=$(aws sts get-caller-identity $AWS_CLI_OPTS --query Account --output text)
print_success "AWS CLI configured. Account ID: $ACCOUNT_ID"

# Validate bucket name format
if [[ ! $BUCKET_NAME =~ ^[a-z0-9][a-z0-9-]*[a-z0-9]$ ]]; then
    print_error "Invalid S3 bucket name format"
    echo "Bucket names must:"
    echo "  - Be 3-63 characters long"
    echo "  - Contain only lowercase letters, numbers, and hyphens"
    echo "  - Start and end with a letter or number"
    exit 1
fi

# Create temporary parameters file with updated paths
TEMP_PARAMS_FILE=$(mktemp)
trap "rm -f $TEMP_PARAMS_FILE" EXIT

# Copy original parameters and update S3 paths
cp "$PARAMETERS_FILE" "$TEMP_PARAMS_FILE"

# Update S3 paths in parameters file
print_status "Updating parameter file with S3 paths..."
python3 -c "
import json
import sys

# Read parameters file
with open('$TEMP_PARAMS_FILE', 'r') as f:
    params = json.load(f)

# Update GlueJobScriptS3Path
for param in params:
    if param.get('ParameterKey') == 'GlueJobScriptS3Path':
        param['ParameterValue'] = 's3://$BUCKET_NAME/glue-scripts/main.py'
        print('Updated GlueJobScriptS3Path to: s3://$BUCKET_NAME/glue-scripts/main.py')
        break
else:
    # Add parameter if it doesn't exist
    params.append({
        'ParameterKey': 'GlueJobScriptS3Path',
        'ParameterValue': 's3://$BUCKET_NAME/glue-scripts/main.py'
    })
    print('Added GlueJobScriptS3Path: s3://$BUCKET_NAME/glue-scripts/main.py')

# Update or add GlueJobModulesS3Path
for param in params:
    if param.get('ParameterKey') == 'GlueJobModulesS3Path':
        param['ParameterValue'] = 's3://$BUCKET_NAME/glue-modules/glue-job-modules.zip'
        print('Updated GlueJobModulesS3Path to: s3://$BUCKET_NAME/glue-modules/glue-job-modules.zip')
        break
else:
    # Add parameter if it doesn't exist
    params.append({
        'ParameterKey': 'GlueJobModulesS3Path',
        'ParameterValue': 's3://$BUCKET_NAME/glue-modules/glue-job-modules.zip'
    })
    print('Added GlueJobModulesS3Path: s3://$BUCKET_NAME/glue-modules/glue-job-modules.zip')

# Write updated parameters
with open('$TEMP_PARAMS_FILE', 'w') as f:
    json.dump(params, f, indent=2)
"

echo "AWS CLI OPTS: "$AWS_CLI_OPTS

# Upload assets to S3 (unless skipped)
if [[ "$SKIP_UPLOAD" == false ]]; then
    print_status "Uploading assets to S3..."
    
    # Check if bucket exists, create if it doesn't
    print_status "Checking if S3 bucket exists..."
    
    # Use s3 ls as the primary check since it works reliably
    if aws s3 ls "s3://$BUCKET_NAME" $AWS_CLI_OPTS > /dev/null 2>&1; then
        print_success "S3 bucket exists and is accessible: $BUCKET_NAME"
    else
        print_status "S3 bucket does not exist. Creating: $BUCKET_NAME"
        
        # Create bucket with appropriate configuration for region
        if [[ "$REGION" = "us-east-1" ]]; then
            CREATE_OUTPUT=$(aws s3 mb "s3://$BUCKET_NAME" $AWS_CLI_OPTS 2>&1)
            CREATE_EXIT_CODE=$?
        else
            CREATE_OUTPUT=$(aws s3 mb "s3://$BUCKET_NAME" $AWS_CLI_OPTS --create-bucket-configuration LocationConstraint=$REGION 2>&1)
            CREATE_EXIT_CODE=$?
        fi
        
        if [[ $CREATE_EXIT_CODE -eq 0 ]]; then
            print_success "Created S3 bucket: $BUCKET_NAME"
        else
            print_error "Failed to create S3 bucket: $BUCKET_NAME"
            
            # Check for specific error types
            if echo "$CREATE_OUTPUT" | grep -q "BucketAlreadyExists"; then
                print_error "The bucket name '$BUCKET_NAME' is already taken globally by another AWS account."
                print_error "S3 bucket names must be globally unique across all AWS accounts."
                print_error "Please choose a different bucket name and try again."
                echo ""
                suggest_bucket_names "$BUCKET_NAME" "$ACCOUNT_ID"
            elif echo "$CREATE_OUTPUT" | grep -q "BucketAlreadyOwnedByYou"; then
                print_error "The bucket '$BUCKET_NAME' already exists in your account but in a different region."
                print_error "Either use the existing bucket or choose a different name."
            elif echo "$CREATE_OUTPUT" | grep -q "AccessDenied"; then
                print_error "Access denied. You may not have permissions to create S3 buckets."
            else
                print_error "Unknown error occurred:"
                print_error "$CREATE_OUTPUT"
            fi
            exit 1
        fi
        
        # Wait a moment for bucket to be fully available
        print_status "Waiting for bucket to be fully available..."
        sleep 2
        
        # Verify bucket was created successfully
        if aws s3api head-bucket --bucket "$BUCKET_NAME" $AWS_CLI_OPTS > /dev/null 2>&1; then
            print_success "Bucket creation verified: $BUCKET_NAME"
        else
            print_error "Bucket creation verification failed: $BUCKET_NAME"
            exit 1
        fi
    fi
    
    # Upload CloudFormation template
    print_status "Uploading CloudFormation template..."
    TEMPLATE_S3_KEY="cloudformation/glue-data-replication.yaml"
    aws s3 cp "$TEMPLATE_FILE" "s3://$BUCKET_NAME/$TEMPLATE_S3_KEY" $AWS_CLI_OPTS
    print_success "Uploaded CloudFormation template to s3://$BUCKET_NAME/$TEMPLATE_S3_KEY"
    
    # Package Glue job modules using dedicated script
    print_status "Packaging Glue job modules..."
    if [[ ! -f "dist/glue-job-modules.zip" ]] || [[ ! -f "dist/main.py" ]]; then
        print_status "Running packaging script..."
        if [[ -x "infrastructure/scripts/package-glue-modules.sh" ]]; then
            ./infrastructure/scripts/package-glue-modules.sh
        else
            print_warning "Packaging script not found or not executable, using fallback method"
            mkdir -p dist
            CURRENT_DIR="$(pwd)"
            cd src && zip -r "$CURRENT_DIR/dist/glue-job-modules.zip" glue_job/ -x "glue_job/main.py" && cd ..
            cp src/glue_job/main.py dist/
        fi
        print_success "Created Glue job packages"
    else
        print_status "Using existing Glue job packages"
    fi
    
    # Upload main script
    print_status "Uploading Glue job main script..."
    aws s3 cp "dist/main.py" "s3://$BUCKET_NAME/glue-scripts/main.py" $AWS_CLI_OPTS
    print_success "Uploaded main script to s3://$BUCKET_NAME/glue-scripts/main.py"
    
    # Upload modules zip
    print_status "Uploading Glue job modules..."
    aws s3 cp "dist/glue-job-modules.zip" "s3://$BUCKET_NAME/glue-modules/glue-job-modules.zip" $AWS_CLI_OPTS
    print_success "Uploaded modules to s3://$BUCKET_NAME/glue-modules/glue-job-modules.zip"
    
    # Upload configuration files
    if [[ -d "config" ]]; then
        print_status "Uploading configuration files..."
        aws s3 sync config/ "s3://$BUCKET_NAME/config/" $AWS_CLI_OPTS
        print_success "Uploaded configuration files"
    fi
    
    print_success "All assets uploaded to S3"
else
    print_status "Skipping S3 upload (--skip-upload specified)"
    TEMPLATE_S3_KEY="cloudformation/glue-data-replication.yaml"
fi

# Set template URL for deployment
TEMPLATE_URL="https://s3.amazonaws.com/$BUCKET_NAME/$TEMPLATE_S3_KEY"
if [[ "$REGION" != "us-east-1" ]]; then
    TEMPLATE_URL="https://s3-$REGION.amazonaws.com/$BUCKET_NAME/$TEMPLATE_S3_KEY"
fi

print_status "Using template URL: $TEMPLATE_URL"

# Validate S3 paths in parameters file
print_status "Validating S3 paths in parameters file..."
GLUE_SCRIPT_S3_PATH=$(cat "$TEMP_PARAMS_FILE" | jq -r '.[] | select(.ParameterKey=="GlueJobScriptS3Path") | .ParameterValue')
GLUE_MODULES_S3_PATH=$(cat "$TEMP_PARAMS_FILE" | jq -r '.[] | select(.ParameterKey=="GlueJobModulesS3Path") | .ParameterValue')

if [[ -n "$GLUE_SCRIPT_S3_PATH" && "$GLUE_SCRIPT_S3_PATH" != "null" ]]; then
    if [[ "$GLUE_SCRIPT_S3_PATH" == *"/glue-scripts/main.py" ]]; then
        print_success "GlueJobScriptS3Path correctly configured: $GLUE_SCRIPT_S3_PATH"
    else
        print_warning "GlueJobScriptS3Path has custom location: $GLUE_SCRIPT_S3_PATH"
    fi
else
    print_warning "GlueJobScriptS3Path not found in parameters file"
fi

if [[ -n "$GLUE_MODULES_S3_PATH" && "$GLUE_MODULES_S3_PATH" != "null" ]]; then
    if [[ "$GLUE_MODULES_S3_PATH" == *"/glue-modules/glue-job-modules.zip" ]]; then
        print_success "GlueJobModulesS3Path correctly configured: $GLUE_MODULES_S3_PATH"
    else
        print_warning "GlueJobModulesS3Path has custom location: $GLUE_MODULES_S3_PATH"
    fi
else
    print_warning "GlueJobModulesS3Path not found in parameters file"
fi

# Validate CloudFormation template
print_status "Validating CloudFormation template..."
if aws cloudformation validate-template $AWS_CLI_OPTS --template-url "$TEMPLATE_URL" > /dev/null 2>&1; then
    print_success "CloudFormation template is valid"
else
    print_error "CloudFormation template validation failed"
    aws cloudformation validate-template $AWS_CLI_OPTS --template-url "$TEMPLATE_URL"
    exit 1
fi

# If validate-only flag is set, exit here
if [[ "$VALIDATE_ONLY" == true ]]; then
    print_success "Template validation completed successfully"
    exit 0
fi

# Check if stack exists
print_status "Checking if stack exists..."
STACK_EXISTS=false
if aws cloudformation describe-stacks $AWS_CLI_OPTS --stack-name "$STACK_NAME" > /dev/null 2>&1; then
    STACK_EXISTS=true
    STACK_STATUS=$(aws cloudformation describe-stacks $AWS_CLI_OPTS --stack-name "$STACK_NAME" --query 'Stacks[0].StackStatus' --output text)
    print_status "Stack exists with status: $STACK_STATUS"
else
    print_status "Stack does not exist"
fi

# Determine operation type
OPERATION=""
if [[ "$STACK_EXISTS" == true ]]; then
    if [[ "$UPDATE_STACK" == true ]]; then
        OPERATION="update"
    else
        print_error "Stack already exists. Use --update flag to update existing stack."
        exit 1
    fi
else
    if [[ "$UPDATE_STACK" == true ]]; then
        print_error "Cannot update non-existent stack. Remove --update flag to create new stack."
        exit 1
    fi
    OPERATION="create"
fi

# Show what will be deployed (dry run)
if [[ "$DRY_RUN" == true ]]; then
    print_status "DRY RUN - Would execute the following:"
    echo "Operation: $OPERATION stack"
    echo "Stack Name: $STACK_NAME"
    echo "Template URL: $TEMPLATE_URL"
    echo "Parameters: $TEMP_PARAMS_FILE"
    echo "Region: $REGION"
    echo "Profile: $PROFILE"
    
    print_status "Parameters that would be used:"
    cat "$TEMP_PARAMS_FILE" | jq -r '.[] | "  \(.ParameterKey): \(.ParameterValue)"'
    
    print_success "Dry run completed"
    exit 0
fi

# Execute deployment
print_status "Starting stack $OPERATION..."

if [[ "$OPERATION" == "create" ]]; then
    aws cloudformation create-stack $AWS_CLI_OPTS \
        --stack-name "$STACK_NAME" \
        --template-url "$TEMPLATE_URL" \
        --parameters file://$TEMP_PARAMS_FILE \
        --capabilities CAPABILITY_NAMED_IAM \
        --tags Key=DeployedBy,Value="$(whoami)" Key=DeploymentDate,Value="$(date -u +%Y-%m-%dT%H:%M:%SZ)" Key=S3Bucket,Value="$BUCKET_NAME" \
        --enable-termination-protection
    
    print_success "Stack creation initiated"
    
elif [[ "$OPERATION" == "update" ]]; then
    aws cloudformation update-stack $AWS_CLI_OPTS \
        --stack-name "$STACK_NAME" \
        --template-url "$TEMPLATE_URL" \
        --parameters file://$TEMP_PARAMS_FILE \
        --capabilities CAPABILITY_NAMED_IAM
    
    print_success "Stack update initiated"
fi

# Wait for stack operation to complete
print_status "Waiting for stack $OPERATION to complete..."
print_warning "This may take several minutes. You can monitor progress in the AWS Console."

if [[ "$OPERATION" == "create" ]]; then
    WAIT_CONDITION="stack-create-complete"
elif [[ "$OPERATION" == "update" ]]; then
    WAIT_CONDITION="stack-update-complete"
fi

if aws cloudformation wait $WAIT_CONDITION $AWS_CLI_OPTS --stack-name "$STACK_NAME"; then
    print_success "Stack $OPERATION completed successfully!"
    
    # Get stack outputs
    print_status "Stack outputs:"
    aws cloudformation describe-stacks $AWS_CLI_OPTS \
        --stack-name "$STACK_NAME" \
        --query 'Stacks[0].Outputs[*].[OutputKey,OutputValue]' \
        --output table
    
    # Get created resources
    print_status "Created resources:"
    aws cloudformation list-stack-resources $AWS_CLI_OPTS \
        --stack-name "$STACK_NAME" \
        --query 'StackResourceSummaries[*].[ResourceType,LogicalResourceId,PhysicalResourceId,ResourceStatus]' \
        --output table
    
    # Show next steps
    print_status "Next steps:"
    echo "1. Verify the Glue job was created successfully"
    echo "2. Test database connectivity"
    echo "3. Run the job for initial full-load migration"
    echo "4. Set up job scheduling if needed"
    echo "5. Configure monitoring and alerting"
    echo ""
    echo "Note: If you haven't uploaded the modular Glue job assets yet, run:"
    echo "  ./infrastructure/scripts/upload-modular-assets.sh <bucket-name>"
    
    # Show S3 bucket contents
    print_status "S3 bucket contents:"
    aws s3 ls "s3://$BUCKET_NAME" --recursive --human-readable $AWS_CLI_OPTS
    
    # Extract job name from parameters for convenience
    JOB_NAME=$(cat "$TEMP_PARAMS_FILE" | jq -r '.[] | select(.ParameterKey=="JobName") | .ParameterValue')
    if [[ -n "$JOB_NAME" ]]; then
        echo ""
        print_status "To run the job manually:"
        echo "aws glue start-job-run --job-name $JOB_NAME $AWS_CLI_OPTS"
    fi
    
else
    print_error "Stack $OPERATION failed!"
    
    # Show recent stack events for troubleshooting
    print_status "Recent stack events:"
    aws cloudformation describe-stack-events $AWS_CLI_OPTS \
        --stack-name "$STACK_NAME" \
        --query 'StackEvents[0:10].[Timestamp,ResourceStatus,ResourceType,LogicalResourceId,ResourceStatusReason]' \
        --output table
    
    exit 1
fi

print_success "Deployment completed successfully!"
print_status "Assets are stored in S3 bucket: $BUCKET_NAME"
print_status "CloudFormation template: s3://$BUCKET_NAME/$TEMPLATE_S3_KEY"
print_status "Glue job script: s3://$BUCKET_NAME/glue-scripts/main.py"
print_status "Glue job modules: s3://$BUCKET_NAME/glue-modules/glue-job-modules.zip"