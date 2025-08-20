# Implementation Plan

- [x] 1. Create S3BookmarkStorage class with core functionality





  - Implement S3BookmarkStorage class with boto3 client configuration
  - Add S3BookmarkConfig dataclass for configuration management
  - Implement basic S3 read/write operations with error handling
  - Add retry logic with exponential backoff for S3 operations
  - _Requirements: 1.1, 1.2, 1.3, 1.4, 5.5_

- [x] 2. Implement S3 bucket detection and path utilities





  - Create utility function to extract S3 bucket name from JDBC driver paths
  - Implement S3 key generation for bookmark files using job name and table name
  - Add validation for S3 path formats and bucket accessibility
  - Create helper methods for S3 path manipulation and validation
  - _Requirements: 2.1, 2.2, 2.3, 2.4_

- [x] 3. Enhance JobBookmarkState class for S3 compatibility





  - Add new fields to JobBookmarkState: job_name, created_timestamp, updated_timestamp, version, s3_key
  - Implement to_s3_dict() method for S3-compatible JSON serialization with ISO timestamps
  - Implement from_s3_dict() class method for deserializing S3 bookmark data
  - Add validate_s3_data() method to validate bookmark JSON structure and handle missing fields
  - _Requirements: 3.2, 3.3, 3.4, 5.3_

- [x] 4. Implement S3 bookmark read operations with error handling





  - Add async _read_bookmark_from_s3() method to S3BookmarkStorage class
  - Implement JSON parsing and validation for bookmark files
  - Add error handling for S3 access denied, not found, and timeout scenarios
  - Implement fallback logic when S3 read operations fail
  - _Requirements: 1.2, 1.3, 5.1, 5.2_

- [x] 5. Implement S3 bookmark write operations with error handling





  - Add async _write_bookmark_to_s3() method to S3BookmarkStorage class
  - Implement JSON serialization and S3 upload functionality
  - Add error handling for S3 write failures with appropriate logging
  - Ensure write operations don't block job execution on failure
  - _Requirements: 1.1, 5.2, 7.4_

- [x] 6. Enhance JobBookmarkManager constructor and initialization





  - Modify JobBookmarkManager constructor to accept source and target JDBC S3 paths
  - Implement S3 bucket detection logic using JDBC driver paths
  - Initialize S3BookmarkStorage instance with detected bucket configuration
  - Add fallback initialization for in-memory bookmarks when S3 is unavailable
  - _Requirements: 2.1, 2.2, 2.4, 4.1, 4.4_

- [x] 7. Update initialize_bookmark_state method for S3 integration





  - Modify initialize_bookmark_state() to attempt reading from S3 first
  - Implement logic to handle first-run detection based on S3 bookmark existence
  - Add error handling and fallback to in-memory bookmarks when S3 fails
  - Ensure backward compatibility with existing job configurations
  - _Requirements: 1.2, 1.3, 1.4, 4.2, 4.3_

- [x] 8. Update update_bookmark_state method for S3 persistence





  - Modify update_bookmark_state() to write bookmark data to S3 after processing
  - Implement asynchronous S3 write operations to avoid blocking job execution
  - Add error handling for S3 write failures with appropriate logging
  - Maintain in-memory bookmark state as backup when S3 operations fail
  - _Requirements: 1.1, 5.2, 7.4_

- [x] 9. Implement comprehensive error handling and resilience





  - Add _handle_s3_error() method for centralized S3 error handling
  - Implement graceful degradation to in-memory bookmarks on S3 failures
  - Add detection and handling of corrupted bookmark JSON files
  - Implement bookmark file deletion for corrupted data with full load fallback
  - _Requirements: 5.1, 5.2, 5.3, 5.4_

- [x] 10. Add structured logging and monitoring for S3 operations










  - Implement structured logging for all S3 bookmark operations (read/write/delete)
  - Add logging for S3 bucket detection and validation processes
  - Implement performance logging for S3 operation latency
  - Add CloudWatch custom metrics for bookmark operation success/failure counts
  - _Requirements: 6.1, 6.2, 6.3, 6.4, 6.5_

- [x] 11. Implement parallel S3 operations for multiple tables






  - Add support for parallel bookmark reading during job initialization
  - Implement batch S3 write operations for multiple table bookmarks
  - Add asynchronous processing to avoid blocking data operations
  - Optimize S3 operations for jobs processing many tables simultaneously
  - _Requirements: 7.1, 7.2, 7.5_

- [x] 12. Update main job execution flow to use enhanced bookmark manager
  - Modify job initialization to pass JDBC S3 paths to JobBookmarkManager constructor
  - Update table processing loops to use enhanced bookmark state management
  - Ensure S3 bookmark operations are properly integrated into existing job flow
  - Maintain backward compatibility with existing job parameter structure
  - _Requirements: 4.1, 4.2, 4.5_

- [x] 13. Create comprehensive unit tests for S3BookmarkStorage
  - Write unit tests for S3BookmarkStorage class with mocked S3 client
  - Test all error scenarios: access denied, not found, timeouts, corrupted data
  - Test JSON serialization/deserialization with various data types
  - Test retry logic and exponential backoff functionality
  - _Requirements: 5.1, 5.2, 5.3, 5.5_

- [x] 14. Create unit tests for enhanced JobBookmarkManager
  - Write unit tests for S3 bucket extraction from JDBC driver paths
  - Test bookmark state transitions from first run to incremental loading
  - Test fallback mechanisms to in-memory bookmarks
  - Test integration between JobBookmarkManager and S3BookmarkStorage
  - _Requirements: 2.1, 2.2, 4.3, 5.4_

- [x] 15. Create integration tests for S3 bookmark persistence
  - Write integration tests using real S3 bucket and IAM permissions
  - Test bookmark persistence across multiple simulated job executions
  - Test recovery scenarios from corrupted bookmark files
  - Test performance with multiple tables and concurrent operations
  - _Requirements: 1.1, 1.4, 5.3, 7.1_