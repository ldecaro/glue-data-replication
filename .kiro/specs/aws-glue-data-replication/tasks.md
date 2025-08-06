++++++++# Implementation Plan

- [x] 1. Create project structure and core configuration files
  - Set up directory structure for CloudFormation templates, PySpark scripts, and IAM policies
  - Create configuration files for database engine mappings and JDBC driver specifications
  - _Requirements: 2.1, 2.2_

- [x] 2. Implement CloudFormation template with parameters
  - Create CloudFormation template with all required parameters (job name, engine types, connection details, JDBC paths)
  - Define parameter validation rules and constraints for database engines and connection strings
  - _Requirements: 2.1, 2.2, 3.1, 3.2, 5.5_

- [x] 3. Create IAM role and policies in CloudFormation
  - Implement IAM role resource for Glue job execution with necessary permissions
  - Create IAM policy with permissions for Glue, S3, CloudWatch, and job bookmarks
  - _Requirements: 9.1, 9.2, 9.5_

- [x] 4. Add Glue job resource to CloudFormation template
  - Create AWS::Glue::Job resource with proper configuration for PySpark script
  - Configure job bookmark settings and JDBC driver paths from S3
  - Link IAM role to Glue job and set up parameter passing
  - _Requirements: 1.3, 2.2, 3.3, 5.2_

- [x] 5. Implement core PySpark job structure and configuration
  - Create main PySpark script with job configuration parsing from CloudFormation parameters
  - Implement database engine detection and JDBC driver loading logic
  - Create connection configuration classes and validation functions
  - _Requirements: 7.1, 5.1, 5.4_

- [x] 6. Implement JDBC connection management
  - Create database connection functions for all supported engines (Oracle, SQLServer, PostgreSQL, Db2)
  - Implement connection string building logic for each database type
  - Add connection validation and error handling with retry mechanisms
  - _Requirements: 4.1, 4.2, 4.3, 4.4, 4.5, 7.2_

- [x] 7. Implement full-load data migration functionality
  - Create functions to read complete table data from source databases using JDBC
  - Implement data writing logic to target databases with proper data type handling
  - Add progress tracking and logging for full-load operations
  - _Requirements: 1.1, 6.3_

- [x] 8. Implement incremental loading with job bookmarks
  - Create job bookmark initialization and state management functions
  - Implement incremental column detection (timestamp, primary key, hash-based strategies)
  - Build delta data extraction logic using job bookmark state
  - _Requirements: 1.2, 1.3, 1.4_

- [x] 9. Add cross-database type compatibility and data transformation
  - Implement data type mapping functions between different database engines
  - Create data transformation logic for cross-database replication scenarios
  - Add validation to ensure schema compatibility assumptions are met
  - _Requirements: 6.1, 6.2, 6.3, 6.5_

- [x] 10. Implement error handling and recovery mechanisms
  - Create comprehensive error handling for database connection failures
  - Add retry logic with exponential backoff for transient errors
  - Implement graceful failure handling with detailed error logging
  - _Requirements: 7.4, 8.5_

- [x] 11. Create DevOps IAM policy file
  - Write separate IAM policy document defining minimum permissions for CloudFormation deployment
  - Include permissions for stack operations, IAM management, Glue job creation, and S3 access
  - Document policy usage and deployment instructions
  - _Requirements: 9.3, 9.4_

- [x] 12. Add comprehensive logging and monitoring
  - Implement structured logging throughout the PySpark job with appropriate log levels
  - Add CloudWatch metrics for job execution tracking and performance monitoring
  - Create logging for data volume statistics and processing times
  - _Requirements: 1.4, 7.1_

- [x] 13. Create unit tests for PySpark job components
  - Write unit tests for database connection functions using mock connections
  - Test data transformation logic with sample datasets for each database type
  - Create tests for incremental loading logic and job bookmark state management
  - _Requirements: 4.5, 6.4, 1.3_

- [x] 14. Create integration tests for CloudFormation deployment
  - Write tests to validate CloudFormation template syntax and parameter validation
  - Create integration tests for IAM role and policy creation
  - Test Glue job creation and configuration through CloudFormation
  - _Requirements: 2.3, 9.1, 9.2_

- [x] 15. Create end-to-end testing framework
  - Implement test data setup for multiple database types with sample schemas
  - Create automated tests for full-load and incremental load scenarios
  - Add cross-database replication testing for all supported engine combinations
  - _Requirements: 6.4, 1.1, 1.2_

- [x] 16. Add documentation and deployment guides
  - Create README with setup instructions and parameter descriptions
  - Write deployment guide for DevOps engineers with IAM policy requirements
  - Document supported database configurations and JDBC driver requirements
  - _Requirements: 9.3, 5.5, 4.5_

- [x] 17. Add cross-VPC network connection parameters to CloudFormation
  - Add optional source network configuration parameters (VPC ID, subnet IDs, security group IDs)
  - Add optional target network configuration parameters (VPC ID, subnet IDs, security group IDs)
  - Add CreateSourceS3VpcEndpoint parameter with default value NO
  - Add CreateTargetS3VpcEndpoint parameter with default value NO
  - Create parameter conditions to make network configurations optional
  - Add parameter validation for network configuration consistency
  - _Requirements: 8.1, 8.5_

- [x] 18. Implement Glue connection resources for cross-VPC access
  - Create conditional AWS::Glue::Connection resource for source database network access
  - Create conditional AWS::Glue::Connection resource for target database network access
  - Configure connection properties with VPC, subnets, and security groups
  - Add connection validation and testing capabilities
  - _Requirements: 8.2, 8.3, 8.6_

- [x] 20. Add VPC endpoint resources for S3 access
  - Create conditional AWS::EC2::VPCEndpoint for S3 access in source database VPC (when CreateSourceS3VpcEndpoint=YES)
  - Create conditional AWS::EC2::VPCEndpoint for S3 access in target database VPC (when CreateTargetS3VpcEndpoint=YES)
  - Configure endpoint policies to allow JDBC driver access from specified S3 bucket
  - Associate with route tables for database subnets in respective VPCs
  - Add endpoint validation and connectivity testing
  - Set default behavior to NOT create endpoints (parameters default to NO)
  - _Requirements: 8.5_

- [x] 21. Update IAM roles for network connectivity permissions
  - Add Glue connection usage permissions to Glue job IAM role
  - Add EC2 network interface creation/deletion permissions for VPC access
  - Add VPC endpoint access permissions for S3 connectivity
  - Update DevOps policy with network resource management permissions
  - _Requirements: 9.6, 8.6_

- [x] 22. Enhance PySpark job for network-aware database connections
  - Implement get_glue_connection() function to retrieve cross-VPC connection details
  - Add validate_network_connectivity() function to test database connectivity before processing
  - Update setup_jdbc_with_connection() to use Glue connections when specified
  - Add network configuration parsing from CloudFormation parameters
  - _Requirements: 8.6_

- [x] 23. Add network-specific error handling and recovery
  - Implement error handling for Glue connection failures with detailed diagnostics
  - Add retry logic for cross-VPC routing issues and security group blocking
  - Create validation for subnet accessibility and VPC endpoint functionality
  - Add ENI creation failure handling with service limit checks
  - _Requirements: 8.6, 8.7_

- [x] 24. Create network connectivity tests
  - Write unit tests for network configuration parsing and validation
  - Create integration tests for Glue connection creation and usage
  - Add end-to-end tests for cross-VPC database connectivity scenarios
  - Test security group rule validation and VPC endpoint functionality
  - _Requirements: 8.1, 8.2, 8.3, 8.6_

- [x] 25. Update documentation for network connectivity features
  - Add network configuration examples for different VPC scenarios
  - Document security group requirements and port configurations
  - Create guidance on when to set CreateSourceS3VpcEndpoint/CreateTargetS3VpcEndpoint to YES
  - Document the difference between private subnets (need VPC endpoints) vs public subnets (use internet gateway)
  - Create troubleshooting guide for cross-VPC connectivity issues
  - Add network configuration decision matrix to deployment guide
  - _Requirements: 8.1, 8.5, 8.7_