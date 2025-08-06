# AWS Glue Data Replication System

A comprehensive data replication solution using AWS Glue that supports full-load and incremental data migration across multiple database types. The system leverages AWS Glue job bookmarks for efficient incremental loading and uses Infrastructure as Code (CloudFormation) for deployment.

## Features

- **Multi-Database Support**: Oracle, SQL Server, PostgreSQL, and DB2
- **Cross-Database Replication**: Replicate data between different database types
- **Incremental Loading**: Efficient delta processing using AWS Glue job bookmarks
- **Infrastructure as Code**: Complete CloudFormation-based deployment
- **PySpark Implementation**: Portable code without Glue-specific dependencies
- **Comprehensive Monitoring**: CloudWatch integration for metrics and logging

## Quick Start

### Prerequisites

1. AWS CLI configured with appropriate permissions
2. JDBC drivers uploaded to S3 (see [JDBC Driver Setup](#jdbc-driver-setup))
3. Source and target databases accessible from AWS Glue
4. IAM permissions for CloudFormation deployment (see [DevOps Guide](docs/DEVOPS_DEPLOYMENT_GUIDE.md))

### Basic Deployment

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd aws-glue-data-replication
   ```

2. **Upload JDBC drivers to S3**
   ```bash
   # Example for Oracle driver
   aws s3 cp ojdbc11.jar s3://your-bucket/jdbc-drivers/oracle/21.7.0.0/ojdbc11.jar
   ```

3. **Deploy the CloudFormation stack**
   ```bash
   aws cloudformation create-stack \
     --stack-name my-data-replication \
     --template-body file://cloudformation/glue-data-replication.yaml \
     --parameters file://examples/parameters.json \
     --capabilities CAPABILITY_IAM
   ```

4. **Run the Glue job**
   ```bash
   aws glue start-job-run --job-name my-data-replication-job
   ```

## Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Source DB     │    │   AWS Glue      │    │   Target DB     │
│                 │    │                 │    │                 │
│ • Oracle        │────│ • PySpark Job   │────│ • Oracle        │
│ • SQL Server    │    │ • Job Bookmarks │    │ • SQL Server    │
│ • PostgreSQL    │    │ • JDBC Drivers  │    │ • PostgreSQL    │
│ • DB2           │    │ • Monitoring    │    │ • DB2           │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                              │
                       ┌─────────────────┐
                       │   S3 Storage    │
                       │                 │
                       │ • JDBC Drivers  │
                       │ • Job Scripts   │
                       └─────────────────┘
```

## Network Connectivity

The system supports flexible network configurations for databases located in different VPCs or network environments. This section covers network connectivity options and configuration requirements.

### Network Scenarios

The system supports four main network connectivity scenarios:

1. **Same VPC**: Both source and target databases are in the same VPC as the Glue job (default)
2. **Cross-VPC Source**: Source database in a different VPC, target in the same VPC
3. **Cross-VPC Target**: Target database in a different VPC, source in the same VPC
4. **Cross-VPC Both**: Both databases in different VPCs from each other and from Glue

### Network Configuration Decision Matrix

| Scenario | Source Location | Target Location | Required Configuration | S3 VPC Endpoint Needed |
|----------|----------------|----------------|----------------------|------------------------|
| Same VPC | Glue VPC | Glue VPC | None (default) | No |
| Cross-VPC Source | Different VPC | Glue VPC | Source network config | If source in private subnet |
| Cross-VPC Target | Glue VPC | Different VPC | Target network config | If target in private subnet |
| Cross-VPC Both | Different VPC A | Different VPC B | Both network configs | If either in private subnet |
| Hybrid | Different VPC | Same VPC | Source network config only | If source in private subnet |

### When to Use S3 VPC Endpoints

Set `CreateSourceS3VpcEndpoint` or `CreateTargetS3VpcEndpoint` to `YES` when:

- Database is in a **private subnet** without internet gateway access
- Database VPC doesn't have existing S3 VPC endpoint
- Glue job needs to download JDBC drivers from S3 through the database's VPC

Leave as `NO` (default) when:
- Database is in a **public subnet** with internet gateway access
- Database VPC already has S3 VPC endpoint configured
- Database is in the same VPC as Glue job

### Network Configuration Examples

#### Example 1: Same VPC (No Network Configuration)
```json
[
  {
    "ParameterKey": "JobName",
    "ParameterValue": "same-vpc-replication"
  },
  {
    "ParameterKey": "SourceEngineType",
    "ParameterValue": "oracle"
  },
  {
    "ParameterKey": "TargetEngineType",
    "ParameterValue": "postgresql"
  }
  // ... other standard parameters
  // No network parameters needed
]
```

#### Example 2: Cross-VPC Source Database
```json
[
  {
    "ParameterKey": "JobName",
    "ParameterValue": "cross-vpc-source-replication"
  },
  {
    "ParameterKey": "SourceVpcId",
    "ParameterValue": "vpc-12345678"
  },
  {
    "ParameterKey": "SourceSubnetIds",
    "ParameterValue": "subnet-12345678,subnet-87654321"
  },
  {
    "ParameterKey": "SourceSecurityGroupIds",
    "ParameterValue": "sg-12345678"
  },
  {
    "ParameterKey": "CreateSourceS3VpcEndpoint",
    "ParameterValue": "YES"
  }
  // ... other parameters
]
```

#### Example 3: Cross-VPC Both Databases
```json
[
  {
    "ParameterKey": "JobName",
    "ParameterValue": "cross-vpc-both-replication"
  },
  {
    "ParameterKey": "SourceVpcId",
    "ParameterValue": "vpc-source123"
  },
  {
    "ParameterKey": "SourceSubnetIds",
    "ParameterValue": "subnet-src1,subnet-src2"
  },
  {
    "ParameterKey": "SourceSecurityGroupIds",
    "ParameterValue": "sg-source123"
  },
  {
    "ParameterKey": "CreateSourceS3VpcEndpoint",
    "ParameterValue": "YES"
  },
  {
    "ParameterKey": "TargetVpcId",
    "ParameterValue": "vpc-target456"
  },
  {
    "ParameterKey": "TargetSubnetIds",
    "ParameterValue": "subnet-tgt1,subnet-tgt2"
  },
  {
    "ParameterKey": "TargetSecurityGroupIds",
    "ParameterValue": "sg-target456"
  },
  {
    "ParameterKey": "CreateTargetS3VpcEndpoint",
    "ParameterValue": "NO"
  }
  // ... other parameters
]
```

### Security Group Requirements

When configuring cross-VPC connectivity, ensure security groups allow the following traffic:

#### Database Security Groups

| Database | Port | Protocol | Source | Purpose |
|----------|------|----------|--------|---------|
| Oracle | 1521 | TCP | Glue ENI Security Group | Database connection |
| SQL Server | 1433 | TCP | Glue ENI Security Group | Database connection |
| PostgreSQL | 5432 | TCP | Glue ENI Security Group | Database connection |
| DB2 | 50000 | TCP | Glue ENI Security Group | Database connection |

#### Glue ENI Security Groups (Auto-created)

| Direction | Port | Protocol | Destination | Purpose |
|-----------|------|----------|-------------|---------|
| Outbound | Database Port | TCP | Database Security Group | Database access |
| Outbound | 443 | TCP | 0.0.0.0/0 | S3 API calls |
| Outbound | 53 | UDP | 0.0.0.0/0 | DNS resolution |

### Network Troubleshooting Guide

#### Connection Issues

**Problem**: Glue job fails with "Connection timed out"

**Diagnosis Steps**:
1. Verify VPC ID and subnet IDs are correct
2. Check security group rules allow database port
3. Verify route tables have proper routing
4. Test connectivity from EC2 instance in same subnet

**Solutions**:
```bash
# Test connectivity from EC2 in same subnet
telnet database-host 1521

# Check security group rules
aws ec2 describe-security-groups --group-ids sg-12345678

# Verify route tables
aws ec2 describe-route-tables --filters "Name=association.subnet-id,Values=subnet-12345678"
```

**Problem**: "Unable to download JDBC driver from S3"

**Diagnosis Steps**:
1. Check if database is in private subnet
2. Verify S3 VPC endpoint configuration
3. Check VPC endpoint policy allows S3 access

**Solutions**:
- Set `CreateSourceS3VpcEndpoint` or `CreateTargetS3VpcEndpoint` to `YES`
- Verify existing S3 VPC endpoint policies
- Check route table associations for VPC endpoint

#### Security Group Issues

**Problem**: "Connection refused" errors

**Common Causes**:
- Security group doesn't allow inbound traffic on database port
- Wrong source security group specified
- Network ACLs blocking traffic

**Resolution**:
```bash
# Add security group rule for database access
aws ec2 authorize-security-group-ingress \
  --group-id sg-database123 \
  --protocol tcp \
  --port 1521 \
  --source-group sg-glue456
```

#### VPC Endpoint Issues

**Problem**: S3 access fails from private subnet

**Diagnosis**:
```bash
# Check VPC endpoint exists
aws ec2 describe-vpc-endpoints --filters "Name=vpc-id,Values=vpc-12345678"

# Verify route table associations
aws ec2 describe-route-tables --filters "Name=route.destination-prefix-list-id,Values=pl-*"
```

**Resolution**:
- Ensure VPC endpoint is associated with correct route tables
- Verify VPC endpoint policy allows required S3 actions
- Check subnet route tables include VPC endpoint routes

#### ENI Creation Issues

**Problem**: "Failed to create network interface"

**Common Causes**:
- Insufficient IP addresses in subnet
- Service limits exceeded
- Subnet in unsupported AZ

**Resolution**:
```bash
# Check available IPs in subnet
aws ec2 describe-subnets --subnet-ids subnet-12345678 --query 'Subnets[0].AvailableIpAddressCount'

# Check service limits
aws service-quotas get-service-quota --service-code ec2 --quota-code L-DF5E4CA3
```

### Best Practices

#### Network Design
- Use dedicated subnets for Glue connections
- Implement least-privilege security group rules
- Use VPC endpoints for private subnet connectivity
- Plan IP address space for ENI creation

#### Security
- Restrict database security groups to Glue ENI security groups only
- Use separate security groups for different environments
- Regularly audit security group rules
- Enable VPC Flow Logs for network monitoring

#### Performance
- Place databases and Glue in same AZ when possible
- Use multiple subnets across AZs for high availability
- Monitor network latency and throughput
- Consider Direct Connect for on-premises databases

## Configuration

### CloudFormation Parameters

| Parameter | Type | Description | Required | Example |
|-----------|------|-------------|----------|---------|
| `JobName` | String | Unique name for the Glue job instance | Yes | `my-data-replication` |
| `SourceEngineType` | String | Source database engine (oracle, sqlserver, postgresql, db2) | Yes | `oracle` |
| `TargetEngineType` | String | Target database engine (oracle, sqlserver, postgresql, db2) | Yes | `postgresql` |
| `SourceDatabase` | String | Source database name | Yes | `ORCL` |
| `TargetDatabase` | String | Target database name | Yes | `target_db` |
| `SourceSchema` | String | Source schema name | Yes | `HR` |
| `TargetSchema` | String | Target schema name | Yes | `public` |
| `TableNames` | String | Comma-separated list of tables | Yes | `employees,departments` |
| `SourceDbUser` | String | Source database username | Yes | `hr_user` |
| `SourceDbPassword` | String | Source database password (SecureString) | Yes | `********` |
| `TargetDbUser` | String | Target database username | Yes | `postgres` |
| `TargetDbPassword` | String | Target database password (SecureString) | Yes | `********` |
| `SourceConnectionString` | String | JDBC connection string for source | Yes | `jdbc:oracle:thin:@host:1521:ORCL` |
| `TargetConnectionString` | String | JDBC connection string for target | Yes | `jdbc:postgresql://host:5432/target_db` |
| `SourceJdbcDriverS3Path` | String | S3 path to source JDBC driver | Yes | `s3://bucket/drivers/ojdbc11.jar` |
| `TargetJdbcDriverS3Path` | String | S3 path to target JDBC driver | Yes | `s3://bucket/drivers/postgresql.jar` |

#### Network Configuration Parameters (Optional)

| Parameter | Type | Description | Required | Example |
|-----------|------|-------------|----------|----------|
| `SourceVpcId` | String | VPC ID where source database resides | No | `vpc-12345678` |
| `SourceSubnetIds` | String | Comma-separated subnet IDs for source access | No | `subnet-123,subnet-456` |
| `SourceSecurityGroupIds` | String | Comma-separated security group IDs for source | No | `sg-12345678` |
| `CreateSourceS3VpcEndpoint` | String | Create S3 VPC endpoint in source VPC (YES/NO) | No | `YES` (default: `NO`) |
| `TargetVpcId` | String | VPC ID where target database resides | No | `vpc-87654321` |
| `TargetSubnetIds` | String | Comma-separated subnet IDs for target access | No | `subnet-789,subnet-012` |
| `TargetSecurityGroupIds` | String | Comma-separated security group IDs for target | No | `sg-87654321` |
| `CreateTargetS3VpcEndpoint` | String | Create S3 VPC endpoint in target VPC (YES/NO) | No | `YES` (default: `NO`) |

### Example Parameters File

```json
[
  {
    "ParameterKey": "JobName",
    "ParameterValue": "oracle-to-postgres-replication"
  },
  {
    "ParameterKey": "SourceEngineType",
    "ParameterValue": "oracle"
  },
  {
    "ParameterKey": "TargetEngineType",
    "ParameterValue": "postgresql"
  },
  {
    "ParameterKey": "SourceDatabase",
    "ParameterValue": "ORCL"
  },
  {
    "ParameterKey": "TargetDatabase",
    "ParameterValue": "target_db"
  },
  {
    "ParameterKey": "SourceSchema",
    "ParameterValue": "HR"
  },
  {
    "ParameterKey": "TargetSchema",
    "ParameterValue": "public"
  },
  {
    "ParameterKey": "TableNames",
    "ParameterValue": "employees,departments,locations"
  },
  {
    "ParameterKey": "SourceDbUser",
    "ParameterValue": "hr_user"
  },
  {
    "ParameterKey": "SourceDbPassword",
    "ParameterValue": "your-secure-password"
  },
  {
    "ParameterKey": "TargetDbUser",
    "ParameterValue": "postgres"
  },
  {
    "ParameterKey": "TargetDbPassword",
    "ParameterValue": "your-secure-password"
  },
  {
    "ParameterKey": "SourceConnectionString",
    "ParameterValue": "jdbc:oracle:thin:@oracle-host:1521:ORCL"
  },
  {
    "ParameterKey": "TargetConnectionString",
    "ParameterValue": "jdbc:postgresql://postgres-host:5432/target_db"
  },
  {
    "ParameterKey": "SourceJdbcDriverS3Path",
    "ParameterValue": "s3://my-glue-assets/jdbc-drivers/oracle/21.7.0.0/ojdbc11.jar"
  },
  {
    "ParameterKey": "TargetJdbcDriverS3Path",
    "ParameterValue": "s3://my-glue-assets/jdbc-drivers/postgresql/42.6.0/postgresql-42.6.0.jar"
  }
]
```

## JDBC Driver Setup

### Supported Database Engines

| Engine | Driver Class | Default Port | Supported Versions |
|--------|--------------|--------------|-------------------|
| Oracle | `oracle.jdbc.OracleDriver` | 1521 | 11g, 12c, 18c, 19c, 21c |
| SQL Server | `com.microsoft.sqlserver.jdbc.SQLServerDriver` | 1433 | 2012, 2014, 2016, 2017, 2019, 2022 |
| PostgreSQL | `org.postgresql.Driver` | 5432 | 9.6, 10, 11, 12, 13, 14, 15 |
| DB2 | `com.ibm.db2.jcc.DB2Driver` | 50000 | 10.5, 11.1, 11.5 |

### JDBC Driver Downloads

| Database | Recommended Version | JAR Filename | Download URL |
|----------|-------------------|--------------|--------------|
| Oracle | 21.7.0.0 | `ojdbc11.jar` | [Oracle Downloads](https://download.oracle.com/otn-pub/otn_software/jdbc/217/ojdbc11.jar) |
| SQL Server | 12.2.0.jre11 | `mssql-jdbc-12.2.0.jre11.jar` | [Maven Central](https://repo1.maven.org/maven2/com/microsoft/sqlserver/mssql-jdbc/12.2.0.jre11/mssql-jdbc-12.2.0.jre11.jar) |
| PostgreSQL | 42.6.0 | `postgresql-42.6.0.jar` | [Maven Central](https://repo1.maven.org/maven2/org/postgresql/postgresql/42.6.0/postgresql-42.6.0.jar) |
| DB2 | 11.5.8.0 | `db2jcc4.jar` | [Maven Central](https://repo1.maven.org/maven2/com/ibm/db2/jcc/11.5.8.0/jcc-11.5.8.0.jar) |

### S3 Storage Structure

Organize JDBC drivers in S3 using this recommended structure:

```
s3://your-bucket/
└── jdbc-drivers/
    ├── oracle/
    │   └── 21.7.0.0/
    │       └── ojdbc11.jar
    ├── sqlserver/
    │   └── 12.2.0.jre11/
    │       └── mssql-jdbc-12.2.0.jre11.jar
    ├── postgresql/
    │   └── 42.6.0/
    │       └── postgresql-42.6.0.jar
    └── db2/
        └── 11.5.8.0/
            └── db2jcc4.jar
```

## Data Replication Process

### Full Load (Initial Run)

1. **Job Initialization**: Glue job starts with no existing bookmarks
2. **Connection Establishment**: JDBC connections to source and target databases
3. **Schema Validation**: Verify table structures and compatibility
4. **Data Extraction**: Read complete table data from source
5. **Data Transformation**: Apply cross-database type mappings if needed
6. **Data Loading**: Write data to target database
7. **Bookmark Creation**: Save processing state for future incremental runs

### Incremental Load (Subsequent Runs)

1. **Bookmark Retrieval**: Load previous processing state
2. **Delta Detection**: Identify changed records using:
   - Timestamp columns (preferred)
   - Primary key ranges
   - Row hash comparison (fallback)
3. **Delta Extraction**: Read only changed records
4. **Data Processing**: Transform and load delta records
5. **Bookmark Update**: Save new processing state

### Cross-Database Compatibility

The system supports replication between different database types with these assumptions:

- **Schema Migration**: Target schema must be pre-created and compatible
- **Data Type Mapping**: Automatic conversion between compatible types
- **Constraint Handling**: Target constraints must accommodate source data
- **Index Management**: Target indexes should be optimized for the workload

## Monitoring and Logging

### CloudWatch Integration

- **Job Execution Metrics**: Duration, success/failure rates
- **Data Volume Metrics**: Records processed, data transfer rates
- **Error Tracking**: Detailed error logs with context
- **Performance Monitoring**: Resource utilization and bottlenecks

### Log Analysis

Logs are structured with the following format:
```
TIMESTAMP - LOGGER - LEVEL - [FUNCTION:LINE] - MESSAGE
```

Key log categories:
- `CONNECTION`: Database connection events
- `PROCESSING`: Data processing progress
- `BOOKMARK`: Job bookmark operations
- `ERROR`: Error conditions and recovery attempts

## Troubleshooting

### Common Issues

1. **Connection Timeouts**
   - Verify network connectivity from Glue to databases
   - Check security group and firewall rules
   - Validate connection strings and credentials

2. **JDBC Driver Not Found**
   - Ensure drivers are uploaded to correct S3 paths
   - Verify Glue job has S3 read permissions
   - Check driver compatibility with database versions

3. **Schema Mismatch Errors**
   - Compare source and target table structures
   - Verify data type compatibility
   - Check for missing columns or constraints

4. **Performance Issues**
   - Monitor Glue job resource allocation
   - Consider table partitioning for large datasets
   - Optimize incremental column selection

### Debug Mode

Enable debug logging by setting the Glue job parameter:
```json
{
  "--enable-continuous-cloudwatch-log": "true",
  "--enable-metrics": "true"
}
```

## Security Considerations

- **Credential Management**: Use AWS Secrets Manager for database passwords
- **Network Security**: Implement VPC endpoints and security groups
- **IAM Permissions**: Follow principle of least privilege
- **Data Encryption**: Enable encryption in transit and at rest

## Testing

The project includes comprehensive test suites to ensure reliability and functionality.

### Test Structure

```
tests/
├── __init__.py                      # Test package initialization
├── test_cloudformation_integration.py  # CloudFormation deployment tests
├── test_data_setup.py               # Test data generation utilities
├── test_end_to_end.py              # End-to-end integration tests
├── test_glue_data_replication.py   # Unit tests for PySpark components
├── test_logging_monitoring.py      # Logging and monitoring tests
├── test_minimal.py                 # Basic functionality tests
├── simple_test.py                  # Simple setup verification
├── run_tests.py                    # Unit test runner
├── run_cloudformation_tests.py     # CloudFormation test runner
└── run_end_to_end_tests.py         # End-to-end test runner
```

### Running Tests

#### Quick Test (from project root)
```bash
# Run all test suites
python run_tests.py all

# Run specific test types
python run_tests.py unit           # Unit tests only
python run_tests.py cloudformation # CloudFormation tests only
python run_tests.py e2e            # End-to-end tests only
python run_tests.py simple         # Simple verification test
python run_tests.py logging        # Logging and monitoring tests
```

#### Detailed Test Execution
```bash
# Unit tests with detailed output
python tests/run_tests.py

# CloudFormation integration tests
python tests/run_cloudformation_tests.py

# End-to-end tests with specific engines
python tests/run_end_to_end_tests.py --engines oracle,postgresql --scenarios full,incremental

# Simple setup verification
python tests/simple_test.py
```

### Test Categories

#### Unit Tests (`test_glue_data_replication.py`)
- Database connection functions with mock connections
- Data transformation logic for all database types
- Incremental loading logic and job bookmark state management
- Configuration parsing and validation
- JDBC driver loading and management
- Error handling and recovery mechanisms

#### CloudFormation Tests (`test_cloudformation_integration.py`)
- Template syntax and parameter validation
- IAM role and policy creation
- Glue job creation and configuration
- Stack deployment and resource validation

#### End-to-End Tests (`test_end_to_end.py`)
- Full-load replication scenarios
- Incremental load testing with different strategies
- Cross-database replication validation
- Data accuracy and integrity verification

#### Monitoring Tests (`test_logging_monitoring.py`)
- Structured logging functionality
- CloudWatch metrics publishing
- Performance monitoring components
- Error tracking and reporting

### Test Requirements

Install test dependencies:
```bash
pip install -r requirements-test.txt
```

Test dependencies include:
- `unittest` (built-in)
- `moto` for AWS service mocking
- `pandas` for data manipulation
- `faker` for test data generation
- `boto3` for AWS SDK testing

### Continuous Integration

The test suite is designed to run in CI/CD environments:
- All tests use mocked AWS services (no real AWS resources required)
- Database connections are mocked for unit testing
- Test data is generated programmatically
- Comprehensive coverage reporting

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality (see [Testing](#testing) section)
5. Run the test suite: `python run_tests.py all`
6. Submit a pull request

## Support

For issues and questions:
- Check the [Troubleshooting](#troubleshooting) section
- Review CloudWatch logs for detailed error information
- Consult the [DevOps Deployment Guide](docs/DEVOPS_DEPLOYMENT_GUIDE.md) for deployment issues

## License

This project is licensed under the MIT License - see the LICENSE file for details.