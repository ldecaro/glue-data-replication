# Project Structure

## Directory Organization

### Core Source Code (`src/`)
```
src/glue_job/                           # Main Glue job modules
├── main.py                             # Entry point and orchestration
├── config/                             # Configuration management
│   ├── job_config.py                   # Job configuration dataclasses
│   ├── database_engines.py             # Database engine management
│   ├── parsers.py                      # Configuration parsing
│   ├── iceberg_connection_handler.py   # Iceberg-specific connections
│   ├── iceberg_models.py               # Iceberg data models
│   └── iceberg_schema_manager.py       # Iceberg schema operations
├── database/                           # Database operations
│   ├── connection_manager.py           # Connection management
│   ├── schema_validator.py             # Schema validation
│   ├── migration.py                    # Data migration logic
│   └── incremental_detector.py         # Incremental processing
├── storage/                            # Storage and bookmarks
│   ├── s3_bookmark.py                  # S3 bookmark operations
│   ├── bookmark_manager.py             # Bookmark lifecycle
│   └── manual_bookmark_config.py       # Manual bookmark configuration
├── monitoring/                         # Observability
│   ├── logging.py                      # Structured logging
│   ├── metrics.py                      # CloudWatch metrics
│   └── progress.py                     # Progress tracking
├── network/                            # Network and error handling
│   ├── error_handler.py                # Error classification
│   └── retry_handler.py                # Retry mechanisms
└── utils/                              # Utilities
    └── s3_utils.py                     # S3 operations
```

### Infrastructure (`infrastructure/`)
```
infrastructure/
├── cloudformation/
│   └── glue-data-replication.yaml      # Main CloudFormation template
├── scripts/                            # Deployment and utility scripts
│   ├── package-glue-modules.sh         # Package Python modules
│   ├── upload-assets.sh                # Upload assets to S3
│   ├── upload-modular-assets.sh        # Upload modular structure
│   ├── get-rds-network-info.sh         # Network discovery utility
│   └── get-rds-network-info.ps1        # Windows version
└── iam/                                # IAM policies
    ├── devops-policy.json              # DevOps deployment policy
    └── README.md                       # IAM setup instructions
```

### Configuration (`config/`)
```
config/
├── database_engines.json              # Database engine definitions
└── jdbc_drivers.json                  # JDBC driver configurations
```

### Documentation (`docs/`)
```
docs/
├── API_REFERENCE.md                   # Module API documentation
├── ARCHITECTURE.md                    # Technical architecture
├── DATABASE_CONFIGURATION_GUIDE.md   # Database setup guide
├── DEPLOYMENT_GUIDE.md                # Deployment instructions
├── ERROR_HANDLING_GUIDE.md            # Error handling patterns
├── ICEBERG_USAGE_GUIDE.md             # Iceberg-specific guide
├── NETWORK_CONFIGURATION_GUIDE.md     # VPC and networking
├── OBSERVABILITY_GUIDE.md             # Monitoring and logging
├── PARAMETER_REFERENCE.md             # CloudFormation parameters
└── TESTING_GUIDE.md                   # Testing procedures
```

### Examples (`examples/`)
```
examples/
├── README.md                          # Example configurations overview
├── oracle-to-postgresql-parameters.json
├── sqlserver-to-sqlserver-parameters.json
├── iceberg-source-basic-parameters.json
├── iceberg-to-iceberg-parameters.json
└── [various other parameter examples]
```

### Testing (`tests/`)
```
tests/
├── conftest.py                        # pytest configuration
├── mocks/                             # Mock implementations
│   ├── glue_data_replication.py       # Glue service mocks
│   └── s3_parallel_operations.py      # S3 operation mocks
├── test_config.py                     # Configuration tests
├── test_database.py                   # Database operation tests
├── test_storage.py                    # Storage and bookmark tests
├── test_network.py                    # Network handling tests
├── test_monitoring.py                 # Monitoring tests
├── integration_test_*.py              # Integration test suites
└── validate_*.py                      # Validation utilities
```

## File Naming Conventions

### Python Files
- **Modules**: `snake_case.py` (e.g., `connection_manager.py`)
- **Classes**: `PascalCase` (e.g., `JobBookmarkManager`)
- **Functions**: `snake_case` (e.g., `initialize_bookmark_state`)
- **Constants**: `UPPER_SNAKE_CASE` (e.g., `DEFAULT_TIMEOUT`)

### Configuration Files
- **JSON**: `kebab-case.json` (e.g., `database-engines.json`)
- **YAML**: `kebab-case.yaml` (e.g., `glue-data-replication.yaml`)
- **Parameters**: `engine-to-engine-parameters.json` format

### Scripts
- **Bash**: `kebab-case.sh` (e.g., `package-glue-modules.sh`)
- **PowerShell**: `kebab-case.ps1` (e.g., `get-rds-network-info.ps1`)

### Documentation
- **Markdown**: `UPPER_SNAKE_CASE.md` (e.g., `README.md`, `API_REFERENCE.md`)

## Module Dependencies

### Import Patterns
```python
# Standard library imports first
import sys
import logging
from typing import Dict, Any

# Third-party imports
from dataclasses import dataclass

# AWS/Glue imports (with conditional handling)
try:
    from awsglue.context import GlueContext
    from pyspark.sql import SparkSession
except ImportError:
    # Mock for local development
    pass

# Local imports (relative)
from .config.job_config import JobConfig
from .database.connection_manager import ConnectionManager
```

### Module Boundaries
- **config/**: No dependencies on other modules (pure configuration)
- **database/**: Depends on config, may use utils
- **storage/**: Depends on config and utils
- **monitoring/**: Minimal dependencies, primarily logging
- **network/**: Depends on monitoring for logging
- **utils/**: No dependencies on other modules (pure utilities)

## Build Artifacts (`dist/`)
```
dist/                                  # Generated during build
├── main.py                           # Standalone main script
├── glue-job-modules.zip              # Python modules package
├── main-standalone.py                # Self-contained version
└── DEPLOYMENT_INSTRUCTIONS.md        # Generated deployment guide
```

## Key Architectural Principles

### Separation of Concerns
- Each module has a single, well-defined responsibility
- Configuration is centralized and type-safe
- Database operations are abstracted from engine specifics
- Error handling is consistent across all modules

### Testability
- All modules can be tested independently
- Extensive mocking for AWS services
- Clear separation between business logic and AWS SDK calls
- Integration tests validate end-to-end functionality

### Maintainability
- Consistent naming conventions across all files
- Clear module boundaries and dependencies
- Comprehensive documentation for all public interfaces
- Version-controlled configuration files