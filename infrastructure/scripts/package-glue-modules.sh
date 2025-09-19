#!/bin/bash

# Package Glue Job Modules Script
# This script packages the modular Glue job structure for AWS Glue deployment

set -e

# Default values
OUTPUT_DIR="dist"
PACKAGE_NAME="glue-job-modules"
CLEAN=true  # Default to cleaning to ensure fresh builds

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

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

show_usage() {
    cat << EOF
Package Glue Job Modules Script

Usage: $0 [OPTIONS]

Options:
    -o, --output-dir DIR        Output directory for packages (default: dist)
    -n, --package-name NAME     Package name (default: glue-job-modules)
    -c, --clean                 Clean output directory before packaging (default)
    --no-clean                  Skip cleaning output directory
    -h, --help                  Show this help message

This script creates:
1. A zip file with all Python modules for --extra-py-files
2. A standalone main.py with embedded imports for single-file deployment

EOF
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -o|--output-dir)
            OUTPUT_DIR="$2"
            shift 2
            ;;
        -n|--package-name)
            PACKAGE_NAME="$2"
            shift 2
            ;;
        -c|--clean)
            CLEAN=true
            shift
            ;;
        --no-clean)
            CLEAN=false
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

# Check if source directory exists
if [[ ! -d "src/glue_job" ]]; then
    print_error "Source directory 'src/glue_job' not found"
    print_error "Please run this script from the project root directory"
    exit 1
fi

# Create output directory
if [[ "$CLEAN" == true && -d "$OUTPUT_DIR" ]]; then
    print_status "Cleaning output directory: $OUTPUT_DIR"
    rm -rf "$OUTPUT_DIR"
fi

print_status "Creating output directory: $OUTPUT_DIR"
mkdir -p "$OUTPUT_DIR"

if [[ ! -d "$OUTPUT_DIR" ]]; then
    print_error "Failed to create output directory: $OUTPUT_DIR"
    exit 1
fi

print_status "Packaging Glue job modules..."
print_status "Output directory: $OUTPUT_DIR"
print_status "Package name: $PACKAGE_NAME"

# Create temporary directory for packaging
TEMP_DIR=$(mktemp -d)
trap "rm -rf $TEMP_DIR" EXIT

# Copy source files to temporary directory
print_status "Copying source files..."
cp -r src/glue_job "$TEMP_DIR/"

# Remove __pycache__ directories and .pyc files
find "$TEMP_DIR" -name "__pycache__" -type d -exec rm -rf {} + 2>/dev/null || true
find "$TEMP_DIR" -name "*.pyc" -delete 2>/dev/null || true

# Create zip file for --extra-py-files
print_status "Creating Python modules zip file..."
# Store the current working directory
CURRENT_DIR="$(pwd)"
cd "$TEMP_DIR"
zip -r "$CURRENT_DIR/$OUTPUT_DIR/${PACKAGE_NAME}.zip" glue_job/ -x "glue_job/main.py"
cd - > /dev/null

print_success "Created: $OUTPUT_DIR/${PACKAGE_NAME}.zip"

# Copy main.py separately
print_status "Copying main script..."
cp "src/glue_job/main.py" "$OUTPUT_DIR/main.py"
print_success "Created: $OUTPUT_DIR/main.py"

# Create a standalone version with embedded modules (alternative approach)
print_status "Creating standalone version with sys.path modification..."

cat > "$OUTPUT_DIR/main-standalone.py" << 'EOF'
#!/usr/bin/env python3
"""
Standalone AWS Glue Data Replication Script

This version modifies sys.path to include the uploaded modules directory.
"""

import sys
import os

# Add the modules directory to Python path
# AWS Glue extracts zip files to /tmp/
sys.path.insert(0, '/tmp')

# Now import the original main module
EOF

# Append the original main.py content, but skip the first few lines (shebang, docstring)
tail -n +10 "src/glue_job/main.py" >> "$OUTPUT_DIR/main-standalone.py"

print_success "Created: $OUTPUT_DIR/main-standalone.py"

# Create deployment instructions
cat > "$OUTPUT_DIR/DEPLOYMENT_INSTRUCTIONS.md" << EOF
# Glue Job Module Deployment Instructions

This directory contains packaged Glue job modules for deployment.

## Files Created

1. **${PACKAGE_NAME}.zip** - Python modules package for --extra-py-files parameter
2. **main.py** - Original main script (requires zip file)
3. **main-standalone.py** - Modified main script with sys.path adjustment

## Deployment Options

### Option 1: Using --extra-py-files (Recommended)

Upload both files to S3 and configure Glue job:
- ScriptLocation: s3://bucket/main.py
- --extra-py-files: s3://bucket/${PACKAGE_NAME}.zip

### Option 2: Standalone Script

Upload only the standalone script:
- ScriptLocation: s3://bucket/main-standalone.py
- Upload ${PACKAGE_NAME}.zip to the same S3 directory

### S3 Upload Commands

\`\`\`bash
# Upload modules package
aws s3 cp ${PACKAGE_NAME}.zip s3://your-bucket/glue-modules/

# Upload main script
aws s3 cp main.py s3://your-bucket/glue-scripts/

# Or upload standalone version
aws s3 cp main-standalone.py s3://your-bucket/glue-scripts/main.py
\`\`\`

### CloudFormation Parameters

For Option 1:
- GlueJobScriptS3Path: s3://your-bucket/glue-scripts/main.py
- Add --extra-py-files parameter with s3://your-bucket/glue-modules/${PACKAGE_NAME}.zip

For Option 2:
- GlueJobScriptS3Path: s3://your-bucket/glue-scripts/main.py
- Ensure ${PACKAGE_NAME}.zip is in s3://your-bucket/glue-scripts/

## Generated on: $(date)
EOF

print_success "Created: $OUTPUT_DIR/DEPLOYMENT_INSTRUCTIONS.md"

# Verify all expected files were created
print_status "Verifying package contents..."
expected_files=(
    "$OUTPUT_DIR/${PACKAGE_NAME}.zip"
    "$OUTPUT_DIR/main.py"
    "$OUTPUT_DIR/main-standalone.py"
    "$OUTPUT_DIR/DEPLOYMENT_INSTRUCTIONS.md"
)

all_files_exist=true
for file in "${expected_files[@]}"; do
    if [[ -f "$file" ]]; then
        file_size=$(ls -lh "$file" | awk '{print $5}')
        print_success "✓ $(basename "$file") ($file_size)"
    else
        print_error "✗ $(basename "$file") - MISSING"
        all_files_exist=false
    fi
done

if [[ "$all_files_exist" == false ]]; then
    print_error "Some files are missing. Packaging may have failed."
    exit 1
fi

print_status "Zip file contents (first 20 files):"
if command -v unzip >/dev/null 2>&1; then
    unzip -l "$OUTPUT_DIR/${PACKAGE_NAME}.zip" | head -20
else
    python3 -c "
import zipfile
with zipfile.ZipFile('$OUTPUT_DIR/${PACKAGE_NAME}.zip', 'r') as z:
    files = z.namelist()[:20]
    for f in files:
        print(f'  {f}')
    if len(z.namelist()) > 20:
        print(f'  ... and {len(z.namelist()) - 20} more files')
"
fi

print_success "Packaging completed successfully!"
print_status "Next steps:"
echo "1. Upload the packages to S3 using: ./infrastructure/scripts/upload-assets.sh <bucket-name>"
echo "2. Or deploy directly using: ./deploy.sh -s <stack-name> -p <parameters-file> -b <bucket-name>"
echo "3. The Glue job will now have access to all required modules"
echo ""
echo "See $OUTPUT_DIR/DEPLOYMENT_INSTRUCTIONS.md for detailed instructions"