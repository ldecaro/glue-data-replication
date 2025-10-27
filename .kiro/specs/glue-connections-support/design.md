# Design Document

## Overview

This design adds support for AWS Glue Connections to the existing AWS Glue Data Replication solution, specifically for JDBC databases (Oracle, SQL Server, PostgreSQL, DB2). The feature allows users to either create new Glue Connections during job execution or use existing pre-configured Glue Connections, while maintaining full backward compatibility with the existing direct JDBC connection approach. Iceberg connections will continue using the existing connection mechanism unchanged.

## Architecture

### High-Level Architecture

The Glue Connections support will be integrated into the existing connection management layer without disrupting the current architecture:

```
┌─────────────────────────────────────────────────────────────────┐
│                    Parameter Processing Layer                    │
├─────────────────────────────────────────────────────────────────┤
│  JobConfigurationParser (Enhanced)                              │
│  - Parse createSourceConnection/createTargetConnection          │
│  - Parse useSourceConnection/useTargetConnection                │
│  - Validate parameter combinations                              │
│  - Route to appropriate connection strategy                     │
└─────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────┐
│                  Connection Management Layer                     │
├─────────────────────────────────────────────────────────────────┤
│  UnifiedConnectionManager (Enhanced)                            │
│  ├─ GlueConnectionManager (Enhanced)                            │
│  │  ├─ create_glue_connection()                                 │
│  │  ├─ use_existing_glue_connection()                           │
│  │  └─ setup_jdbc_with_connection() (existing)                  │
│  ├─ JdbcConnectionManager (existing)                            │
│  └─ IcebergConnectionHandler (unchanged)                       │
└─────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────┐
│                    Database Access Layer                        │
├─────────────────────────────────────────────────────────────────┤
│  JDBC Databases          │         Iceberg Tables              │
│  - Oracle                │         - Existing mechanism        │
│  - SQL Server            │         - No changes required       │
│  - PostgreSQL            │                                     │
│  - DB2                   │                                     │
└─────────────────────────────────────────────────────────────────┘
```

### Connection Strategy Decision Matrix

| Source Engine | Target Engine | createConnection | useConnection | Strategy |
|---------------|---------------|------------------|---------------|----------|
| JDBC          | JDBC          | true/false       | name/empty    | Glue Connection or Direct JDBC |
| JDBC          | Iceberg       | true/false       | name/empty    | Source: Glue/JDBC, Target: Iceberg |
| Iceberg       | JDBC          | ignored          | ignored       | Source: Iceberg, Target: Glue/JDBC |
| Iceberg       | Iceberg       | ignored          | ignored       | Both: Iceberg (existing) |

## Components and Interfaces

### 1. Enhanced Parameter Processing

#### New Parameters
```json
{
  "createSourceConnection": "true|false",
  "createTargetConnection": "true|false", 
  "useSourceConnection": "glue-connection-name",
  "useTargetConnection": "glue-connection-name"
}
```

#### Parameter Validation Rules
- `createSourceConnection` and `useSourceConnection` are mutually exclusive
- `createTargetConnection` and `useTargetConnection` are mutually exclusive
- Glue Connection parameters are ignored for Iceberg engines (with warning)
- When `createConnection=true`, all standard JDBC parameters must be present
- When `useConnection=name`, only the connection name is required

### 2. Enhanced JobConfigurationParser

#### New Methods
```python
class JobConfigurationParser:
    @classmethod
    def parse_glue_connection_params(cls, args: Dict[str, str], connection_type: str) -> GlueConnectionConfig:
        """Parse Glue Connection parameters for source or target"""
        
    @classmethod
    def validate_glue_connection_params(cls, args: Dict[str, str]) -> None:
        """Validate Glue Connection parameter combinations"""
        
    @classmethod
    def create_connection_config_with_glue_support(cls, args: Dict[str, str], 
                                                  connection_type: str) -> ConnectionConfig:
        """Create ConnectionConfig with Glue Connection support"""
```

#### Enhanced ConnectionConfig
```python
@dataclass
class GlueConnectionConfig:
    """Configuration for Glue Connection operations"""
    create_connection: bool = False
    use_existing_connection: Optional[str] = None
    connection_strategy: str = "direct_jdbc"  # "create_glue", "use_glue", "direct_jdbc"

@dataclass 
class ConnectionConfig:
    # Existing fields...
    glue_connection_config: Optional[GlueConnectionConfig] = None
    
    def uses_glue_connection(self) -> bool:
        """Check if this connection uses Glue Connection"""
        
    def get_glue_connection_strategy(self) -> str:
        """Get the Glue Connection strategy"""
```

### 3. Enhanced GlueConnectionManager

#### New Methods
```python
class GlueConnectionManager:
    def create_glue_connection(self, connection_config: ConnectionConfig, 
                             connection_name: str) -> str:
        """Create a new Glue Connection from JDBC parameters with Secrets Manager integration"""
        
    def create_secrets_manager_secret(self, connection_name: str, username: str, password: str) -> str:
        """Create AWS Secrets Manager secret for database credentials"""
        
    def validate_glue_connection_exists(self, connection_name: str) -> bool:
        """Validate that a Glue Connection exists"""
        
    def setup_jdbc_with_glue_connection_strategy(self, connection_config: ConnectionConfig) -> Dict[str, Any]:
        """Setup JDBC using the appropriate Glue Connection strategy"""
```

#### Connection Creation Logic with Secrets Manager
```python
def create_glue_connection(self, connection_config: ConnectionConfig, connection_name: str) -> str:
    """
    Creates a Glue Connection with AWS Secrets Manager integration:
    1. Create AWS Secrets Manager secret at path "/aws-glue/{connection_name}"
    2. Store username and password in JSON format with keys "username" and "password"
    3. Create Glue Connection with the following properties:
       - Connection Type: JDBC
       - JDBC URL: from connection_config.connection_string
       - Username/Password: Reference to Secrets Manager secret
       - Physical Connection Requirements: from network_config if available
    """
    
def create_secrets_manager_secret(self, connection_name: str, username: str, password: str) -> str:
    """
    Creates AWS Secrets Manager secret:
    - Secret Name: "/aws-glue/{connection_name}"
    - Secret Value: {"username": username, "password": password}
    - Returns: Secret ARN for Glue Connection reference
    """
```

### 4. AWS Secrets Manager Integration

#### SecretsManagerHandler
```python
class SecretsManagerHandler:
    def __init__(self, region_name: str):
        """Initialize Secrets Manager client"""
        
    def create_secret(self, secret_name: str, username: str, password: str) -> str:
        """Create a new secret with database credentials"""
        
    def validate_secret_permissions(self) -> bool:
        """Validate IAM permissions for Secrets Manager operations"""
        
    def generate_secret_name(self, connection_name: str) -> str:
        """Generate standardized secret name: /aws-glue/{connection_name}"""
```

#### Secret Structure
```json
{
  "username": "database_username",
  "password": "database_password"
}
```

#### Glue Connection with Secrets Manager Reference
```python
def create_glue_connection_with_secrets(self, connection_config: ConnectionConfig, 
                                      connection_name: str) -> str:
    """
    Enhanced connection creation flow:
    1. Validate Secrets Manager permissions
    2. Create secret at /aws-glue/{connection_name}
    3. Create Glue Connection referencing the secret
    4. Return connection name for usage
    """
```

### 5. Enhanced UnifiedConnectionManager

#### Connection Strategy Routing
```python
class UnifiedConnectionManager:
    def _determine_connection_strategy(self, connection_config: ConnectionConfig) -> str:
        """Determine connection strategy based on engine type and Glue config"""
        if self.is_iceberg_engine(connection_config.engine_type):
            return "iceberg"
        elif connection_config.uses_glue_connection():
            return connection_config.get_glue_connection_strategy()
        else:
            return "direct_jdbc"
            
    def create_connection(self, connection_config: ConnectionConfig) -> Any:
        """Enhanced connection creation with Glue Connection and Secrets Manager support"""
        strategy = self._determine_connection_strategy(connection_config)
        
        if strategy == "iceberg":
            return self._create_iceberg_connection(connection_config)
        elif strategy in ["create_glue", "use_glue"]:
            return self._create_jdbc_connection_with_glue_and_secrets(connection_config)
        else:
            return self._create_jdbc_connection(connection_config)
```

## Data Models

### GlueConnectionConfig
```python
@dataclass
class GlueConnectionConfig:
    """Configuration for Glue Connection operations"""
    create_connection: bool = False
    use_existing_connection: Optional[str] = None
    
    @property
    def connection_strategy(self) -> str:
        if self.create_connection:
            return "create_glue"
        elif self.use_existing_connection:
            return "use_glue"
        else:
            return "direct_jdbc"
    
    def validate(self) -> None:
        """Validate Glue Connection configuration"""
        if self.create_connection and self.use_existing_connection:
            raise ValueError("Cannot both create and use existing Glue Connection")
```

### Enhanced Parameter Structure
```json
{
  "traditional_jdbc_params": {
    "SOURCE_CONNECTION_STRING": "jdbc:oracle:thin:@host:1521:db",
    "SOURCE_DB_USER": "username",
    "SOURCE_DB_PASSWORD": "password",
    "SOURCE_JDBC_DRIVER_S3_PATH": "s3://bucket/driver.jar"
  },
  "glue_connection_params": {
    "createSourceConnection": "true",
    "useSourceConnection": "",
    "createTargetConnection": "false", 
    "useTargetConnection": "my-existing-connection"
  },
  "iceberg_params": {
    "SOURCE_WAREHOUSE_LOCATION": "s3://bucket/warehouse/",
    "TARGET_WAREHOUSE_LOCATION": "s3://bucket/warehouse/"
  }
}
```

## Error Handling

### Error Categories

#### 1. Parameter Validation Errors
```python
class GlueConnectionParameterError(ValueError):
    """Raised when Glue Connection parameters are invalid"""
    
class MutuallyExclusiveParameterError(GlueConnectionParameterError):
    """Raised when mutually exclusive parameters are provided"""
```

#### 2. Glue Connection Operation Errors
```python
class GlueConnectionCreationError(GlueConnectionError):
    """Raised when Glue Connection creation fails"""
    
class GlueConnectionNotFoundError(GlueConnectionError):
    """Raised when specified Glue Connection doesn't exist"""
```

#### 3. AWS Secrets Manager Errors
```python
class SecretsManagerError(Exception):
    """Base class for Secrets Manager related errors"""
    
class SecretCreationError(SecretsManagerError):
    """Raised when secret creation fails"""
    
class SecretsManagerPermissionError(SecretsManagerError):
    """Raised when IAM permissions are insufficient for Secrets Manager operations"""
```

#### 3. Engine Compatibility Errors
```python
class IcebergGlueConnectionWarning(UserWarning):
    """Warning when Glue Connection parameters provided for Iceberg engine"""
```

### Error Handling Strategy

1. **Parameter Validation**: Fail fast during parameter parsing
2. **Secrets Manager Operations**: Validate permissions before creating secrets, retry transient failures
3. **Connection Creation**: Retry with exponential backoff, clean up secrets on failure
4. **Connection Usage**: Validate existence before attempting to use
5. **Iceberg Compatibility**: Log warnings but continue execution
6. **Fallback Strategy**: Fall back to direct JDBC if Glue Connection or Secrets Manager fails

## Testing Strategy

### Unit Tests

#### 1. Parameter Parsing Tests
```python
class TestGlueConnectionParameterParsing:
    def test_create_connection_parameters(self):
        """Test parsing of createSourceConnection/createTargetConnection"""
        
    def test_use_connection_parameters(self):
        """Test parsing of useSourceConnection/useTargetConnection"""
        
    def test_mutually_exclusive_validation(self):
        """Test validation of mutually exclusive parameters"""
        
    def test_iceberg_parameter_ignored(self):
        """Test that Glue Connection params are ignored for Iceberg"""
```

#### 2. Connection Strategy Tests
```python
class TestConnectionStrategy:
    def test_strategy_determination(self):
        """Test connection strategy determination logic"""
        
    def test_jdbc_with_glue_creation(self):
        """Test JDBC connection with Glue Connection creation"""
        
    def test_jdbc_with_existing_glue(self):
        """Test JDBC connection with existing Glue Connection"""
```

#### 3. Error Handling Tests
```python
class TestGlueConnectionErrorHandling:
    def test_connection_creation_failure(self):
        """Test handling of Glue Connection creation failures"""
        
    def test_nonexistent_connection_usage(self):
        """Test handling of nonexistent Glue Connection usage"""
        
    def test_parameter_validation_errors(self):
        """Test parameter validation error scenarios"""

class TestSecretsManagerIntegration:
    def test_secret_creation_success(self):
        """Test successful secret creation with proper JSON structure"""
        
    def test_secret_creation_failure(self):
        """Test handling of secret creation failures"""
        
    def test_secrets_manager_permission_validation(self):
        """Test IAM permission validation for Secrets Manager"""
        
    def test_secret_name_generation(self):
        """Test proper secret name generation with /aws-glue/ prefix"""
```

### Integration Tests

#### 1. End-to-End Connection Tests
- Test complete flow with `createSourceConnection=true`
- Test complete flow with `useSourceConnection=existing-connection`
- Test mixed scenarios (source uses Glue, target uses direct JDBC)
- Test Iceberg + JDBC combinations with Glue Connection parameters

#### 2. AWS Service Integration Tests
- Test actual Glue Connection creation via AWS API
- Test retrieval of existing Glue Connections
- Test connection validation with real Glue Connections
- Test end-to-end secret creation and Glue Connection integration
- Test IAM permission validation for both Glue and Secrets Manager
- Test secret cleanup on connection creation failure

### Performance Tests

#### 1. Connection Creation Performance
- Measure time to create Glue Connections
- Compare performance vs direct JDBC connections
- Test concurrent connection creation scenarios

#### 2. Connection Reuse Performance  
- Measure performance of using existing Glue Connections
- Test connection caching effectiveness

## Implementation Phases

### Phase 1: Core Infrastructure (Tasks 1-3)
1. **Parameter Processing Enhancement**
   - Add new Glue Connection parameters to parser
   - Implement parameter validation logic
   - Add GlueConnectionConfig data model

2. **Connection Configuration Enhancement**
   - Extend ConnectionConfig with Glue Connection support
   - Add connection strategy determination logic
   - Implement parameter compatibility validation

3. **Basic Glue Connection Operations with Secrets Manager**
   - Implement AWS Secrets Manager integration for credential storage
   - Implement Glue Connection creation functionality with secret references
   - Implement existing Glue Connection usage
   - Add error handling for both Glue Connec

### Phase 2: Integration and Validation (Tasks 4-6)
4. **UnifiedConnectionManager Integration**
   - Integrate Glue Connection support into connection routing
   - Implement connection strategy selection logic
   - Add comprehensive error handling

5. **Validation and Testing**
   - Implement connection validation for Glue Connections
   - Add comprehensive unit tests
   - Implement integration tests

6. **Iceberg Compatibility**
   - Ensure Iceberg engines ignore Glue Connection parameters
   - Add appropriate warnings for invalid combinations
   - Test mixed engine scenarios

### Phase 3: Documentation and Examples (Tasks 7-8)
7. **Documentation Updates**
   - Update parameter reference documentation
   - Add Glue Connection usage examples
   - Update troubleshooting guides

8. **Example Configurations**
   - Create example parameter files with Glue Connections
   - Add CloudFormation template examples
   - Update deployment guides

## Backward Compatibility

### Compatibility Guarantees

1. **Existing Parameter Files**: All existing parameter files will continue to work unchanged
2. **Direct JDBC Connections**: Existing direct JDBC connection logic remains unchanged
3. **Iceberg Connections**: Iceberg connection mechanism remains completely unchanged
4. **API Compatibility**: All existing public APIs maintain their signatures

### Migration Path

1. **No Migration Required**: Existing deployments continue working without changes
2. **Opt-in Enhancement**: Users can gradually adopt Glue Connections by adding new parameters
3. **Mixed Usage**: Users can use Glue Connections for some connections and direct JDBC for others

### Default Behavior

- When no Glue Connection parameters are provided: Use existing direct JDBC behavior
- When Glue Connection parameters are provided for Iceberg: Log warning and ignore
- When invalid parameter combinations are provided: Fail with clear error message

This design ensures that the Glue Connections feature integrates seamlessly with the existing architecture while providing powerful new capabilities for JDBC database connections.