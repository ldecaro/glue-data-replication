# AWS Glue Data Replication - Asset Upload Script (PowerShell)
# This script uploads the Glue job script and JDBC drivers to S3

param(
    [Parameter(Mandatory=$true)]
    [string]$BucketName
)

# Configuration
$ScriptPath = "scripts/glue_data_replication.py"

# Function to print colored output
function Write-Info {
    param([string]$Message)
    Write-Host "[INFO] $Message" -ForegroundColor Yellow
}

function Write-Success {
    param([string]$Message)
    Write-Host "[SUCCESS] $Message" -ForegroundColor Green
}

function Write-Error {
    param([string]$Message)
    Write-Host "[ERROR] $Message" -ForegroundColor Red
}

# Function to show usage
function Show-Usage {
    Write-Host "Usage: .\upload-assets.ps1 -BucketName <s3-bucket-name>"
    Write-Host ""
    Write-Host "Example:"
    Write-Host "  .\upload-assets.ps1 -BucketName my-glue-assets-bucket"
    Write-Host ""
    Write-Host "This script uploads:"
    Write-Host "  - Glue job script to s3://bucket/scripts/glue_data_replication.py"
    Write-Host "  - Optionally upload JDBC drivers (you'll be prompted)"
}

# Validate bucket name format
if ($BucketName -notmatch '^[a-z0-9][a-z0-9-]*[a-z0-9]$') {
    Write-Error "Invalid S3 bucket name format"
    Write-Host "Bucket names must:"
    Write-Host "  - Be 3-63 characters long"
    Write-Host "  - Contain only lowercase letters, numbers, and hyphens"
    Write-Host "  - Start and end with a letter or number"
    exit 1
}

Write-Info "Starting asset upload to S3 bucket: $BucketName"

# Check if AWS CLI is configured
try {
    aws sts get-caller-identity | Out-Null
    if ($LASTEXITCODE -ne 0) {
        throw "AWS CLI error"
    }
} catch {
    Write-Error "AWS CLI is not configured or credentials are invalid"
    Write-Host "Please run 'aws configure' to set up your credentials"
    exit 1
}

# Check if bucket exists, create if it doesn't
Write-Info "Checking if S3 bucket exists..."
try {
    aws s3 ls "s3://$BucketName" | Out-Null
    if ($LASTEXITCODE -ne 0) {
        throw "Bucket doesn't exist"
    }
    Write-Success "S3 bucket exists: $BucketName"
} catch {
    Write-Info "Bucket doesn't exist. Creating S3 bucket: $BucketName"
    
    # Get AWS region
    $AwsRegion = aws configure get region
    if ([string]::IsNullOrEmpty($AwsRegion)) {
        $AwsRegion = "us-east-1"
        Write-Info "No region configured, using default: $AwsRegion"
    }
    
    if ($AwsRegion -eq "us-east-1") {
        aws s3 mb "s3://$BucketName"
    } else {
        aws s3 mb "s3://$BucketName" --region $AwsRegion
    }
    
    if ($LASTEXITCODE -eq 0) {
        Write-Success "Created S3 bucket: $BucketName"
    } else {
        Write-Error "Failed to create S3 bucket"
        exit 1
    }
}

# Upload Glue job script
Write-Info "Uploading Glue job script..."
if (-not (Test-Path $ScriptPath)) {
    Write-Error "Glue job script not found: $ScriptPath"
    Write-Host "Please ensure you're running this script from the project root directory"
    exit 1
}

aws s3 cp $ScriptPath "s3://$BucketName/scripts/glue_data_replication.py"
if ($LASTEXITCODE -eq 0) {
    Write-Success "Uploaded Glue job script to s3://$BucketName/scripts/glue_data_replication.py"
} else {
    Write-Error "Failed to upload Glue job script"
    exit 1
}

# Ask about JDBC drivers
Write-Host ""
$uploadDrivers = Read-Host "Do you want to upload JDBC drivers? (y/N)"

if ($uploadDrivers -match '^[Yy]$') {
    Write-Info "JDBC driver upload selected"
    
    $driversFound = $false
    
    # Oracle driver
    if (Test-Path "ojdbc11.jar") {
        Write-Info "Found Oracle JDBC driver: ojdbc11.jar"
        aws s3 cp "ojdbc11.jar" "s3://$BucketName/jdbc-drivers/oracle/21.7.0.0/ojdbc11.jar"
        if ($LASTEXITCODE -eq 0) {
            Write-Success "Uploaded Oracle JDBC driver"
            $driversFound = $true
        }
    }
    
    # SQL Server driver
    $sqlServerDrivers = Get-ChildItem -Name "mssql-jdbc-*.jar" -ErrorAction SilentlyContinue
    foreach ($driver in $sqlServerDrivers) {
        Write-Info "Found SQL Server JDBC driver: $driver"
        $version = $driver -replace 'mssql-jdbc-(.*)\.jar', '$1'
        aws s3 cp $driver "s3://$BucketName/jdbc-drivers/sqlserver/$version/$driver"
        if ($LASTEXITCODE -eq 0) {
            Write-Success "Uploaded SQL Server JDBC driver"
            $driversFound = $true
        }
    }
    
    # PostgreSQL driver
    $postgresDrivers = Get-ChildItem -Name "postgresql-*.jar" -ErrorAction SilentlyContinue
    foreach ($driver in $postgresDrivers) {
        Write-Info "Found PostgreSQL JDBC driver: $driver"
        $version = $driver -replace 'postgresql-(.*)\.jar', '$1'
        aws s3 cp $driver "s3://$BucketName/jdbc-drivers/postgresql/$version/$driver"
        if ($LASTEXITCODE -eq 0) {
            Write-Success "Uploaded PostgreSQL JDBC driver"
            $driversFound = $true
        }
    }
    
    # DB2 driver
    if (Test-Path "db2jcc4.jar") {
        Write-Info "Found DB2 JDBC driver: db2jcc4.jar"
        aws s3 cp "db2jcc4.jar" "s3://$BucketName/jdbc-drivers/db2/11.5.8.0/db2jcc4.jar"
        if ($LASTEXITCODE -eq 0) {
            Write-Success "Uploaded DB2 JDBC driver"
            $driversFound = $true
        }
    }
    
    if (-not $driversFound) {
        Write-Info "No JDBC driver files found in current directory"
        Write-Host "Please download and place JDBC drivers in the current directory, then run:"
        Write-Host ""
        Write-Host "# Oracle"
        Write-Host "aws s3 cp ojdbc11.jar s3://$BucketName/jdbc-drivers/oracle/21.7.0.0/ojdbc11.jar"
        Write-Host ""
        Write-Host "# SQL Server"
        Write-Host "aws s3 cp mssql-jdbc-12.2.0.jre11.jar s3://$BucketName/jdbc-drivers/sqlserver/12.2.0.jre11/mssql-jdbc-12.2.0.jre11.jar"
        Write-Host ""
        Write-Host "# PostgreSQL"
        Write-Host "aws s3 cp postgresql-42.6.0.jar s3://$BucketName/jdbc-drivers/postgresql/42.6.0/postgresql-42.6.0.jar"
        Write-Host ""
        Write-Host "# DB2"
        Write-Host "aws s3 cp db2jcc4.jar s3://$BucketName/jdbc-drivers/db2/11.5.8.0/db2jcc4.jar"
    }
} else {
    Write-Info "Skipping JDBC driver upload"
    Write-Host "You can upload JDBC drivers later using the commands in the documentation"
}

# Show final S3 structure
Write-Host ""
Write-Info "Current S3 bucket structure:"
aws s3 ls "s3://$BucketName" --recursive --human-readable

Write-Host ""
Write-Success "Asset upload completed!"
Write-Host ""
Write-Host "Next steps:"
Write-Host "1. Update your parameter files to use: s3://$BucketName"
Write-Host "2. Deploy the CloudFormation stack"
Write-Host "3. Run the Glue job"