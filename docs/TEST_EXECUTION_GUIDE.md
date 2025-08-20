# Test Execution Quick Reference

This guide provides quick commands for running tests in the AWS Glue Data Replication project.

## Prerequisites

```bash
# Install test dependencies
pip install -r requirements-test.txt

# Ensure Python path is set (run from project root)
export PYTHONPATH="${PYTHONPATH}:$(pwd)/src"
```

## Quick Test Commands

### Run All Tests
```bash
# Run comprehensive test suite
python tests/run_tests.py

# Run all unit tests with pytest
python -m pytest tests/test_*.py -v
```

### Run Individual Module Tests
```bash
# Configuration module
python -m pytest tests/test_config.py -v

# Storage module  
python -m pytest tests/test_storage.py -v

# Database module
python -m pytest tests/test_database.py -v

# Monitoring module
python -m pytest tests/test_monitoring.py -v

# Network module
python -m pytest tests/test_network.py -v

# Utils module
python -m pytest tests/test_utils.py -v
```

### Run Specific Test Classes
```bash
# Test specific functionality
python -m pytest tests/test_config.py::TestJobConfig -v
python -m pytest tests/test_storage.py::TestS3BookmarkStorage -v
python -m pytest tests/test_database.py::TestJdbcConnectionManager -v
```

### Run with Coverage
```bash
# Generate coverage report
python -m pytest tests/ --cov=src/glue_job --cov-report=html --cov-report=term

# View coverage report
open htmlcov/index.html  # macOS
xdg-open htmlcov/index.html  # Linux
```

## Integration Tests

### Prerequisites
```bash
# Configure AWS credentials
aws configure
# OR
export AWS_ACCESS_KEY_ID=your_key
export AWS_SECRET_ACCESS_KEY=your_secret
export AWS_DEFAULT_REGION=us-east-1

# Set test bucket (optional)
export TEST_S3_BUCKET=my-integration-test-bucket
```

### Run Integration Tests
```bash
# S3 bookmark integration tests
python tests/run_s3_integration_tests.py

# End-to-end tests
python tests/run_end_to_end_tests.py

# CloudFormation integration tests
python tests/run_cloudformation_tests.py
```

## Debugging Tests

### Verbose Output
```bash
# Maximum verbosity with output capture disabled
python -m pytest tests/test_storage.py -v -s

# Show local variables in tracebacks
python -m pytest tests/test_storage.py -v -l

# Drop into debugger on failures
python -m pytest tests/test_storage.py --pdb
```

### Run Failed Tests Only
```bash
# Re-run only failed tests from last execution
python -m pytest tests/ --lf

# Re-run failed tests first, then continue with rest
python -m pytest tests/ --ff
```

## Performance Testing
```bash
# Show test execution times
python -m pytest tests/ --durations=10

# Run tests in parallel (requires pytest-xdist)
pip install pytest-xdist
python -m pytest tests/ -n auto
```

## Validation Scripts
```bash
# Validate module imports
python tests/validate_tests.py

# Validate test suite completeness
python tests/test_suite.py
```

## Common Issues

### Import Errors
```bash
# Ensure you're in project root and PYTHONPATH is set
cd /path/to/aws-glue-data-replication
export PYTHONPATH="${PYTHONPATH}:$(pwd)/src"
```

### AWS Credential Issues
```bash
# Verify AWS credentials
aws sts get-caller-identity

# Test S3 access
aws s3 ls
```

### Mock Issues
If tests fail with import errors, ensure mock setup is correct in test files. All test files should have this at the top:

```python
# Mock PySpark and AWS Glue imports for testing
mock_modules = [
    'awsglue', 'awsglue.utils', 'awsglue.context', 'awsglue.job',
    'pyspark', 'pyspark.context', 'pyspark.sql', 'pyspark.sql.types',
    'pyspark.sql.functions', 'boto3'
]

for module in mock_modules:
    sys.modules[module] = MagicMock()
```

## Development Workflow

### Before Committing
```bash
# Run validation and unit tests
python tests/validate_tests.py
python -m pytest tests/test_*.py -x

# Run with coverage check
python -m pytest tests/ --cov=src/glue_job --cov-fail-under=85
```

### Before Merging to Main
```bash
# Run full test suite including integration tests
./scripts/dev-test.sh

# Or manually:
python tests/run_tests.py
python tests/run_s3_integration_tests.py
```

For detailed testing information, see the [comprehensive Testing Guide](TESTING_GUIDE.md).