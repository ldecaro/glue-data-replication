# Technology Stack

## Core Technologies

### AWS Services
- **AWS Glue**: Primary compute platform for data processing jobs
- **AWS S3**: Storage for scripts, modules, JDBC drivers, and bookmarks
- **CloudFormation**: Infrastructure as Code for deployment
- **CloudWatch**: Logging, metrics, dashboards, and alarms
- **VPC Endpoints**: Private connectivity for cross-VPC scenarios

### Programming Languages
- **Python 3**: Primary language for Glue jobs and utilities
- **PySpark**: Data processing framework within Glue
- **Bash**: Deployment and packaging scripts
- **JSON**: Configuration files and parameter definitions
- **YAML**: CloudFormation templates

### Data Processing
- **Apache Spark**: Underlying engine for data transformations
- **JDBC**: Database connectivity for traditional databases
- **Apache Iceberg**: Modern table format for data lakes
- **Pandas**: Data manipulation in test utilities

### Database Support
- **Oracle**: Traditional enterprise database
- **SQL Server**: Microsoft database platform
- **PostgreSQL**: Open-source relational database
- **IBM DB2**: Enterprise database platform
- **Apache Iceberg**: Modern data lake table format

## Build System & Commands

### Deployment Commands
```bash
# Full deployment with asset upload
./deploy.sh -s <stack-name> -b <bucket-name> -p <parameters-file>

# Update existing stack
./deploy.sh -s <stack-name> -b <bucket-name> -p <parameters-file> --update

# Validate template only
./deploy.sh -s <stack-name> -b <bucket-name> -p <parameters-file> --validate-only

# Deploy without uploading assets
./deploy.sh -s <stack-name> -b <bucket-name> -p <parameters-file> --skip-upload
```

### Asset Management
```bash
# Package Glue modules
./infrastructure/scripts/package-glue-modules.sh

# Upload modular assets
./infrastructure/scripts/upload-modular-assets.sh <bucket-name>

# Upload assets with drivers
./infrastructure/scripts/upload-modular-assets.sh <bucket-name> --include-drivers
```

### Testing Commands
```bash
# Run all tests
python -m pytest tests/ -v

# Run specific test categories
python -m pytest tests/test_config.py -v
python -m pytest tests/test_database.py -v
python -m pytest tests/test_storage.py -v

# Run integration tests
python tests/integration_test_end_to_end.py
```

### Development Utilities
```bash
# Debug specific components
python debug_test.py
python debug_storage_tests.py
python debug_network_test.py

# Validate CloudFormation templates
python tests/validate_template.py
```

## Framework Patterns

### Configuration Management
- **Dataclasses**: Type-safe configuration objects
- **JSON Configuration**: External configuration files for engines and drivers
- **Parameter Validation**: Built-in validation for all configuration objects

### Error Handling
- **Custom Exceptions**: Engine-specific error types (e.g., IcebergEngineError)
- **Retry Mechanisms**: Configurable retry logic for network operations
- **Table-Level Isolation**: Failures in one table don't affect others

### Testing Approach
- **pytest**: Primary testing framework
- **Mocking**: Extensive use of mocks for AWS services
- **Integration Tests**: End-to-end testing with real AWS resources
- **Validation Scripts**: Automated template and configuration validation

### Code Organization
- **Modular Architecture**: Clear separation of concerns
- **Dependency Injection**: Configurable components
- **Factory Patterns**: Engine-specific implementations
- **Strategy Pattern**: Different bookmark and migration strategies