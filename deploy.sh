#!/bin/bash

# AWS Glue Data Replication Deployment Script
# This script automates the deployment of the data replication system

set -e  # Exit on any error

# Default values
STACK_NAME=""
PARAMETERS_FILE=""
REGION="us-east-1"
PROFILE="default"
VALIDATE_ONLY=false
UPDATE_STACK=false
DRY_RUN=false

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

# Function to show usage
show_usage() {
    cat << EOF
AWS Glue Data Replication Deployment Script

Usage: $0 [OPTIONS]

Required Options:
    -s, --stack-name STACK_NAME     Name of the CloudFormation stack
    -p, --parameters-file FILE      Path to parameters JSON file

Optional Options:
    -r, --region REGION             AWS region (default: us-east-1)
    --profile PROFILE               AWS CLI profile (default: default)
    -u, --update                    Update existing stack instead of creating new
    -v, --validate-only             Only validate template, don't deploy
    -d, --dry-run                   Show what would be deployed without executing
    -h, --help                      Show this help message

Examples:
    # Deploy new stack
    $0 -s my-replication-stack -p examples/oracle-to-postgresql-parameters.json

    # Update existing stack
    $0 -s my-replication-stack -p examples/oracle-to-postgresql-parameters.json --update

    # Validate template only
    $0 -s my-replication-stack -p examples/oracle-to-postgresql-parameters.json --validate-only

    # Deploy to specific region with custom profile
    $0 -s my-replication-stack -p examples/oracle-to-postgresql-parameters.json -r us-west-2 --profile production

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

# Check if files exist
TEMPLATE_FILE="cloudformation/glue-data-replication.yaml"
if [[ ! -f "$TEMPLATE_FILE" ]]; then
    print_error "CloudFormation template not found: $TEMPLATE_FILE"
    exit 1
fi

if [[ ! -f "$PARAMETERS_FILE" ]]; then
    print_error "Parameters file not found: $PARAMETERS_FILE"
    exit 1
fi

# Set AWS CLI options
AWS_CLI_OPTS="--region $REGION --profile $PROFILE"

print_status "Starting deployment process..."
print_status "Stack Name: $STACK_NAME"
print_status "Parameters File: $PARAMETERS_FILE"
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

# Validate CloudFormation template
print_status "Validating CloudFormation template..."
if aws cloudformation validate-template $AWS_CLI_OPTS --template-body file://$TEMPLATE_FILE > /dev/null 2>&1; then
    print_success "CloudFormation template is valid"
else
    print_error "CloudFormation template validation failed"
    aws cloudformation validate-template $AWS_CLI_OPTS --template-body file://$TEMPLATE_FILE
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
    echo "Template: $TEMPLATE_FILE"
    echo "Parameters: $PARAMETERS_FILE"
    echo "Region: $REGION"
    echo "Profile: $PROFILE"
    
    print_status "Parameters that would be used:"
    cat "$PARAMETERS_FILE" | jq -r '.[] | "  \(.ParameterKey): \(.ParameterValue)"'
    
    print_success "Dry run completed"
    exit 0
fi

# Execute deployment
print_status "Starting stack $OPERATION..."

if [[ "$OPERATION" == "create" ]]; then
    aws cloudformation create-stack $AWS_CLI_OPTS \
        --stack-name "$STACK_NAME" \
        --template-body file://$TEMPLATE_FILE \
        --parameters file://$PARAMETERS_FILE \
        --capabilities CAPABILITY_IAM \
        --tags Key=DeployedBy,Value="$(whoami)" Key=DeploymentDate,Value="$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
        --enable-termination-protection
    
    print_success "Stack creation initiated"
    
elif [[ "$OPERATION" == "update" ]]; then
    aws cloudformation update-stack $AWS_CLI_OPTS \
        --stack-name "$STACK_NAME" \
        --template-body file://$TEMPLATE_FILE \
        --parameters file://$PARAMETERS_FILE \
        --capabilities CAPABILITY_IAM
    
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
    
    # Extract job name from parameters for convenience
    JOB_NAME=$(cat "$PARAMETERS_FILE" | jq -r '.[] | select(.ParameterKey=="JobName") | .ParameterValue')
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