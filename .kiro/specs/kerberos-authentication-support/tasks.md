# Implementation Plan

- [x] 1. Add Kerberos parameters to CloudFormation template
  - Add six new optional parameters (SourceKerberosSPN, SourceKerberosDomain, SourceKerberosKDC, TargetKerberosSPN, TargetKerberosDomain, TargetKerberosKDC) to infrastructure/cloudformation/glue-data-replication.yaml
  - Include proper parameter validation patterns and constraints for each parameter
  - Add parameter descriptions explaining Kerberos authentication usage
  - _Requirements: 1.1, 2.1, 6.2_

- [x] 2. Create KerberosConfig data class and validation
  - [x] 2.1 Implement KerberosConfig dataclass in src/glue_job/config/kerberos_config.py
    - Create dataclass with spn, domain, and kdc fields
    - Implement is_complete() method to check if all parameters are provided
    - Implement validate() method with SPN, domain, and KDC format validation
    - Implement to_glue_properties() method for Glue Connection integration
    - _Requirements: 1.1, 2.1, 4.1, 4.2_

  - [x] 2.2 Create Kerberos-specific exception classes
    - Implement KerberosAuthenticationError base exception
    - Implement KerberosConfigurationError for validation failures
    - Implement KerberosConnectionError for connection issues
    - Implement KerberosEngineCompatibilityError for unsupported engines
    - _Requirements: 4.2, 4.4_

- [x] 3. Extend ConnectionConfig with Kerberos support
  - [x] 3.1 Add kerberos_config field to ConnectionConfig dataclass
    - Add Optional[KerberosConfig] field to existing ConnectionConfig in src/glue_job/config/job_config.py
    - Implement uses_kerberos_authentication() method
    - Implement get_authentication_method() method
    - Update validation logic to handle Kerberos configuration
    - _Requirements: 1.1, 2.1, 3.1, 3.2_

  - [x] 3.2 Update ConnectionConfig validation for mixed authentication modes
    - Modify _validate_jdbc_config() to handle Kerberos authentication scenarios
    - Ensure backward compatibility with existing username/password authentication
    - Add validation for engine compatibility with Kerberos
    - _Requirements: 3.1, 3.2, 3.5, 4.3_

- [x] 4. Enhance JobConfigurationParser for Kerberos parameters
  - [x] 4.1 Add Kerberos parameter constants and parsing logic
    - Add KERBEROS_PARAMS constant list to JobConfigurationParser class
    - Update ALL_OPTIONAL_PARAMS to include Kerberos parameters
    - Implement parse_kerberos_config() method for source and target parsing
    - _Requirements: 1.1, 2.1, 4.1_

  - [x] 4.2 Integrate Kerberos parsing into connection configuration creation
    - Update create_connection_config_with_glue_support() method to parse Kerberos configuration
    - Add Kerberos configuration to ConnectionConfig creation for both source and target
    - Implement proper logging for Kerberos configuration detection
    - _Requirements: 1.1, 2.1, 3.1, 3.2, 5.1_

  - [x] 4.3 Add Kerberos parameter validation and engine compatibility checks
    - Implement validation to ensure Kerberos parameters are only used with supported engines
    - Add warnings for partial Kerberos configuration (missing parameters)
    - Validate that Iceberg engines don't use Kerberos parameters
    - _Requirements: 3.5, 4.3, 4.4_

- [x] 5. Create KerberosConnectionBuilder component
  - [x] 5.1 Implement KerberosConnectionBuilder class
    - Create new file src/glue_job/config/kerberos_connection_builder.py
    - Implement build_kerberos_connection_properties() method
    - Implement validate_engine_kerberos_support() method
    - Add proper error handling and logging for Kerberos connection building
    - _Requirements: 1.1, 2.1, 4.3, 5.1_

  - [x] 5.2 Implement Glue Connection property generation for Kerberos
    - Generate proper JDBC_CONNECTION_URL with Kerberos properties
    - Set AUTHENTICATION_TYPE to 'KERBEROS' when Kerberos is configured
    - Include Kerberos-specific properties (SPN, Domain, KDC) in connection properties
    - Handle SSL/encryption settings appropriately for Kerberos connections
    - _Requirements: 1.1, 2.1, 4.4_

- [x] 6. Enhance GlueConnectionManager with Kerberos support
  - [x] 6.1 Integrate KerberosConnectionBuilder into GlueConnectionManager
    - Add KerberosConnectionBuilder instance to GlueConnectionManager initialization
    - Update create_glue_connection() method to detect and handle Kerberos authentication
    - Implement create_glue_connection_with_kerberos() method for Kerberos-enabled connections
    - _Requirements: 1.1, 2.1, 4.4_

  - [x] 6.2 Update connection creation logic for mixed authentication scenarios
    - Modify connection creation to handle different authentication methods for source vs target
    - Ensure proper fallback to standard authentication when Kerberos is not configured
    - Add comprehensive logging for authentication method selection
    - _Requirements: 3.1, 3.2, 3.3, 5.1_

  - [x] 6.3 Implement Kerberos-specific error handling and retry logic
    - Add Kerberos authentication error detection and classification
    - Implement appropriate retry strategies for Kerberos authentication failures
    - Provide detailed error messages for Kerberos configuration and connection issues
    - _Requirements: 4.2, 4.4, 5.1_

- [x] 7. Update parameter validation and engine compatibility
  - [x] 7.1 Enhance database engine validation for Kerberos support
    - Update DatabaseEngineManager to include Kerberos compatibility information
    - Add validation methods for Kerberos engine support
    - Implement engine-specific Kerberos configuration validation
    - _Requirements: 4.3, 4.4_

  - [x] 7.2 Update parameter validation logic for Kerberos scenarios
    - Modify parameter validation to handle optional Kerberos parameters
    - Ensure proper validation when mixing Kerberos and standard authentication
    - Add validation for complete vs partial Kerberos configuration
    - _Requirements: 1.1, 2.1, 4.1, 4.2_

- [ ] 8. Create comprehensive example parameter files
  - [x] 8.1 Create Kerberos authentication example files
    - Create examples/sqlserver-to-sqlserver-kerberos-parameters.json with Kerberos authentication for both source and target
    - Create examples/oracle-to-postgresql-mixed-auth-parameters.json with Kerberos source and standard target
    - Create examples/db2-to-db2-kerberos-parameters.json demonstrating DB2 Kerberos authentication
    - _Requirements: 6.2_

  - [x] 8.2 Update existing example documentation
    - Update examples/README.md to include Kerberos authentication examples
    - Add explanations for Kerberos parameter usage and configuration
    - Include troubleshooting tips for common Kerberos configuration issues
    - _Requirements: 6.1, 6.4_

- [x] 9. Update comprehensive documentation
  - [x] 9.1 Create Kerberos configuration guide
    - Create docs/KERBEROS_AUTHENTICATION_GUIDE.md with detailed setup instructions
    - Include prerequisites for Kerberos authentication in AWS Glue environments
    - Document supported database engines and their Kerberos configuration requirements
    - Add step-by-step configuration examples for each supported engine
    - _Requirements: 6.1, 6.3_

  - [x] 9.2 Update existing documentation with Kerberos information
    - Update docs/DATABASE_CONFIGURATION_GUIDE.md to include Kerberos authentication sections
    - Update docs/PARAMETER_REFERENCE.md to document new Kerberos parameters
    - Update README.md to mention Kerberos authentication support
    - Add Kerberos troubleshooting section to docs/ERROR_HANDLING_GUIDE.md
    - _Requirements: 6.1, 6.4, 6.5_

- [x] 10. Implement monitoring and logging enhancements
  - [x] 10.1 Add Kerberos-specific logging and metrics
    - Update StructuredLogger to include Kerberos authentication context
    - Add CloudWatch metrics for Kerberos authentication success/failure rates
    - Implement secure logging that doesn't expose Kerberos credentials
    - _Requirements: 5.1, 5.2, 5.3_

  - [x] 10.2 Enhance error reporting for Kerberos authentication
    - Update error handling to provide specific Kerberos troubleshooting guidance
    - Add monitoring for common Kerberos configuration issues
    - Implement audit logging for Kerberos authentication attempts
    - _Requirements: 5.1, 5.4, 5.5_

- [ ]* 11. Create comprehensive unit tests for Kerberos functionality
  - Write unit tests for KerberosConfig validation and methods
  - Test JobConfigurationParser Kerberos parameter parsing logic
  - Test KerberosConnectionBuilder connection property generation
  - Test GlueConnectionManager Kerberos integration
  - Test error handling and exception scenarios
  - _Requirements: 1.1, 2.1, 4.1, 4.2_

- [ ]* 12. Create integration tests for mixed authentication scenarios
  - Test source Kerberos with target standard authentication
  - Test source standard with target Kerberos authentication
  - Test both source and target using Kerberos authentication
  - Test fallback behavior with incomplete Kerberos configuration
  - Test engine compatibility validation
  - _Requirements: 3.1, 3.2, 3.3, 4.3_

- [x] 13. Fix CloudFormation Kerberos connection validation issues
  - [x] 13.1 Add AWS Secrets Manager secrets for Kerberos connections
    - Create SourceKerberosSecret and TargetKerberosSecret resources in CloudFormation template
    - Store username and password from SourceDbUser/SourceDbPassword and TargetDbUser/TargetDbPassword parameters
    - Follow the same pattern as SourceDatabaseSecret and TargetDatabaseSecret
    - Add proper conditions to only create secrets when Kerberos connections are needed
    - _Requirements: 1.1, 2.1, 4.2_

  - [x] 13.2 Update Kerberos connection properties to match JDBC connection pattern
    - Add SECRET_ID property to SourceKerberosConnection and TargetKerberosConnection
    - Add DependsOn clauses to ensure secrets are created before connections
    - Ensure all JDBC connection properties are included (JDBC_CONNECTION_URL, JDBC_DRIVER_JAR_URI, JDBC_DRIVER_CLASS_NAME)
    - Maintain the same structure as SourceJdbcConnection and TargetJdbcConnection for consistency
    - _Requirements: 1.1, 2.1, 4.2, 4.4_

  - [x] 13.3 Update connection conditions and dependencies
    - Ensure ShouldCreateSourceKerberosConnection and ShouldCreateTargetKerberosConnection conditions work correctly
    - Add proper dependencies between Kerberos connections and their secrets
    - Verify that Kerberos connections are only created when all required parameters are provided
    - Test that the connections pass AWS Glue validation
    - _Requirements: 1.1, 2.1, 4.1, 4.2_

- [x] 14. Fix Kerberos connection usage in job configuration
  - [x] 14.1 Update job parameters to automatically use created Kerberos connections
    - Modify USE_SOURCE_CONNECTION parameter to automatically reference created Kerberos connection when ShouldCreateSourceKerberosConnection is true
    - Modify USE_TARGET_CONNECTION parameter to automatically reference created Kerberos connection when ShouldCreateTargetKerberosConnection is true
    - Ensure job uses existing Kerberos connections instead of trying to create new ones at runtime
    - _Requirements: 1.1, 2.1, 3.1, 3.2_

  - [x] 14.2 Update IAM permissions for Kerberos connections
    - Add Kerberos connection ARNs to Glue connection permissions in IAM role
    - Ensure job has proper permissions to access created Kerberos connections
    - Verify Secrets Manager permissions cover Kerberos secrets
    - _Requirements: 1.1, 2.1, 4.2_

- [x] 15. Fix parameter name mismatch between CloudFormation and job parsing
  - [x] 15.1 Fix Glue Connection parameter names in CloudFormation template
    - Change --CREATE_SOURCE_CONNECTION to --createSourceConnection
    - Change --CREATE_TARGET_CONNECTION to --createTargetConnection  
    - Change --USE_SOURCE_CONNECTION to --useSourceConnection
    - Change --USE_TARGET_CONNECTION to --useTargetConnection
    - Ensure parameter names match what the job parsing code expects
    - _Requirements: 1.1, 2.1, 3.1, 3.2_

- [x] 16. Fix mutually exclusive parameter conflict for Kerberos connections
  - [x] 16.1 Prevent createConnection and useConnection parameters from being passed simultaneously
    - Set createSourceConnection to 'false' when ShouldCreateSourceKerberosConnection is true
    - Set createTargetConnection to 'false' when ShouldCreateTargetKerberosConnection is true
    - Ensure only useSourceConnection/useTargetConnection are passed for Kerberos scenarios
    - Resolve parameter validation conflict between create and use connection parameters
    - _Requirements: 1.1, 2.1, 3.1, 3.2_

- [x] 17. Fix CloudFormation conditions to prevent creating both JDBC and Kerberos connections
  - [x] 17.1 Make JDBC and Kerberos connection conditions mutually exclusive
    - Add !Not [!Condition HasSourceKerberos] to ShouldCreateSourceConnection condition
    - Add !Not [!Condition HasTargetKerberos] to ShouldCreateTargetConnection condition
    - Update ShouldCreateSourceKerberosConnection to be independent of ShouldCreateSourceConnection
    - Update ShouldCreateTargetKerberosConnection to be independent of ShouldCreateTargetConnection
    - Ensure only one type of connection (JDBC OR Kerberos) is created, not both
    - _Requirements: 1.1, 2.1, 3.1, 3.2_

- [x] 18. Fix GlueConnectionRetryHandler parameter passing bug
  - [x] 18.1 Fix duplicate connection_name parameter in execute_with_retry calls
    - Fix get_glue_connection method to pass connection_name as positional argument
    - Fix create_glue_connection method to pass connection_name as positional argument
    - Fix validate_connection_with_retry method to pass connection_name as positional argument
    - Fix create_secrets_manager_secret method to pass connection_name as positional argument
    - Resolve "got multiple values for argument 'connection_name'" error
    - _Requirements: 1.1, 2.1, 4.2, 4.4_

- [x] 19. Fix Kerberos authentication detection in Glue Connection properties
  - [x] 19.1 Add AUTHENTICATION_TYPE property to Kerberos connections in CloudFormation
    - Add AUTHENTICATION_TYPE: 'KERBEROS' to SourceKerberosConnection properties
    - Add AUTHENTICATION_TYPE: 'KERBEROS' to TargetKerberosConnection properties
    - Ensure job code can detect Kerberos authentication from Glue Connection properties
    - Enable proper Kerberos environment setup when using existing Glue Connections
    - _Requirements: 1.1, 2.1, 4.2, 4.4_

- [x] 20. Fix SQL Server JDBC URL for Kerberos authentication
  - [x] 20.1 Add Kerberos parameters to SQL Server connection strings
    - Update SourceConnectionString to include integratedSecurity=true and authenticationScheme=JavaKerberos
    - Update TargetConnectionString to include integratedSecurity=true and authenticationScheme=JavaKerberos
    - Ensure SQL Server JDBC driver uses Kerberos authentication instead of username/password
    - Enable proper Kerberos authentication at the JDBC driver level
    - _Requirements: 1.1, 2.1, 4.2, 4.4_

- [x] 21. Fix connection manager to always retrieve credentials even with Kerberos
  - [x] 21.1 Remove credential skipping logic for Kerberos connections
    - Change logic to always retrieve username/password credentials regardless of authentication type
    - Ensure Secrets Manager credentials are available as fallback even when Kerberos is detected
    - Update logging to indicate credentials are retrieved for fallback purposes
    - Allow JDBC driver to use both Kerberos environment and credentials as needed
    - _Requirements: 1.1, 2.1, 4.2, 4.4_

- [x] 22. Add Kerberos environment debugging and verification
  - [x] 22.1 Enable Kerberos debug logging and property verification
    - Enable java.security.krb5.debug=true for detailed Kerberos debugging
    - Add verification logging to confirm Java system properties are actually set
    - Log krb5.conf file contents for debugging realm configuration issues
    - Add comprehensive logging to diagnose "Cannot locate default realm" errors
    - _Requirements: 4.2, 4.4, 5.1_

- [x] 23. Clean up redundant job parameters when using Glue Connections
  - [x] 23.1 Remove redundant database connection parameters when using Glue Connections
    - Set SOURCE_DB_USER/SOURCE_DB_PASSWORD to empty when using Glue Connections (credentials are in the connection)
    - Set TARGET_DB_USER/TARGET_DB_PASSWORD to empty when using Glue Connections (credentials are in the connection)
    - Set SOURCE_CONNECTION_STRING/TARGET_CONNECTION_STRING to empty when using Glue Connections (URL is in the connection)
    - Set SOURCE_JDBC_DRIVER_S3_PATH/TARGET_JDBC_DRIVER_S3_PATH to empty when using Glue Connections (driver info is in the connection)
    - Set Kerberos parameters to empty when using Kerberos Glue Connections (Kerberos config is in the connection)
    - Keep database/schema names as they're still needed for table operations
    - _Requirements: 1.1, 2.1, 3.1, 3.2_

  - [x] 23.2 Update parameter validation logic for Glue Connection usage
    - Modify _validate_jdbc_parameters_with_kerberos_support to detect Glue Connection usage
    - Make JDBC parameters conditionally required based on Glue Connection strategy
    - When using Glue Connections: only require database/schema parameters
    - When NOT using Glue Connections: require all JDBC parameters (connection string, driver, credentials)
    - Skip authentication parameter validation when using Glue Connections (credentials are in the connection)
    - _Requirements: 1.1, 2.1, 3.1, 3.2_

  - [x] 23.3 Fix validation logic to properly detect empty parameters from Glue Connection usage
    - Add fallback detection: if connection parameters are empty, assume Glue Connection usage
    - Skip parameter validation when connection string, username, and password are all empty
    - This handles cases where Glue Connection detection fails but parameters were cleared by CloudFormation
    - _Requirements: 1.1, 2.1, 3.1, 3.2_

  - [x] 23.4 Fix Kerberos parameter passing for environment setup
    - Always pass Kerberos parameters when provided, regardless of Glue Connection usage
    - Remove conditional logic that emptied Kerberos parameters for Kerberos Glue Connections
    - Kerberos parameters are needed for environment setup even when using Glue Connections
    - _Requirements: 1.1, 2.1, 4.2, 4.4_

  - [x] 23.5 Completely omit redundant parameters instead of passing empty values
    - Use !Ref 'AWS::NoValue' to completely omit parameters when using Glue Connections
    - Don't pass SOURCE_DB_USER, SOURCE_DB_PASSWORD, SOURCE_CONNECTION_STRING, SOURCE_JDBC_DRIVER_S3_PATH when using Glue Connections
    - Clean up job parameter list to only include necessary parameters
    - _Requirements: 1.1, 2.1, 3.1, 3.2_

  - [x] 23.6 Improve validation logic to properly detect Glue Connection usage
    - Directly check useSourceConnection and createSourceConnection parameters
    - Remove complex Glue Connection parsing logic in validation
    - Properly detect when Glue Connections are being used based on actual parameter values
    - Skip parameter validation when useSourceConnection or createSourceConnection is true
    - _Requirements: 1.1, 2.1, 3.1, 3.2_

  - [x] 23.7 Fix connection string validation for Glue Connection usage
    - Skip connection string validation in create_job_config when using Glue Connections
    - Add check for uses_glue_connection() before calling validate_connection_string
    - Prevent "Connection string cannot be empty" errors when connection string is omitted for Glue Connections
    - Allow job configuration creation to succeed when using Glue Connections with omitted parameters
    - _Requirements: 1.1, 2.1, 3.1, 3.2_

- [x] 24. Fix Kerberos credential distribution between driver and executors
  - [x] 24.1 Remove manual ticket acquisition methods from Kerberos environment setup
    - Remove complex LoginModule and CallbackHandler implementations from kerberos_environment.py
    - Remove manual ticket acquisition attempts using kinit subprocess calls
    - Remove GSS-API ticket acquisition methods
    - Simplify Kerberos environment setup to focus only on environment configuration
    - _Requirements: 1.1, 2.1, 4.2, 4.4_

  - [x] 24.2 Ensure proper credential distribution to Spark executors
    - Fix the credential distribution issue where executors receive credentials_provided=False while driver gets credentials_provided=True
    - Ensure that username/password credentials are properly distributed to all Spark executors
    - Verify that Kerberos environment setup (krb5.conf, Java properties) is applied consistently across driver and executors
    - Allow JDBC driver to handle ticket acquisition automatically on both driver and executor nodes
    - _Requirements: 1.1, 2.1, 4.2, 4.4_

  - [x] 24.3 Simplify Kerberos environment configuration
    - Keep only essential environment setup: krb5.conf configuration and Java system properties
    - Remove all manual ticket acquisition logic since JDBC driver handles this automatically
    - Focus on ensuring consistent environment setup across driver and executors
    - Maintain proper logging for debugging credential distribution issues
    - _Requirements: 1.1, 2.1, 4.2, 4.4_

  - [x] 24.4 Verify JDBC driver automatic ticket acquisition works on executors
    - Ensure that when executors receive proper credentials and environment setup, JDBC driver can obtain Kerberos tickets automatically
    - Test that the same ticket acquisition process that works on driver also works on executors
    - Validate that the "Found ticket for user" logs appear on both driver and executor nodes
    - Confirm that data access works properly after fixing credential distribution
    - _Requirements: 1.1, 2.1, 4.2, 4.4_

- [x] 25. Fix Kerberos environment setup timing in main job flow
  - [x] 25.1 Add Kerberos environment setup call in main() function
    - Add _setup_kerberos_environment_if_needed() function to main.py
    - Call it AFTER job config parsing but BEFORE execute_migration_workflow()
    - Ensure krb5.conf is created and Java system properties are set before any JDBC connections
    - Handle both source and target Kerberos configurations with realm deduplication
    - Pass username, password, and keytab_s3_path to setup_kerberos_environment()
    - Fix "Cannot locate default realm" error by ensuring environment is ready before JDBC driver needs it
    - _Requirements: 1.1, 2.1, 4.2, 4.4_