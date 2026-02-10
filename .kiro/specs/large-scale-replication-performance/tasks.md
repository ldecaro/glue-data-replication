# Implementation Plan

- [x] 1. Implement counting strategy component
  - Create CountingStrategy class with strategy selection logic
  - Implement immediate counting method for small datasets
  - Implement deferred counting method that queries target after write
  - Add configuration dataclass for strategy parameters
  - _Requirements: 1.1, 1.2, 1.3, 1.4, 4.1, 4.2, 4.3, 4.4, 4.5_

- [x] 2. Implement streaming progress tracker
  - Create StreamingProgressTracker class with real-time update capabilities
  - Add load_type parameter to distinguish full vs incremental loads
  - Implement progress update emission at configurable intervals
  - Add ETA calculation based on current processing rate
  - Implement progress percentage calculation
  - Add integration with CloudWatch metrics publisher
  - _Requirements: 2.1, 2.2, 2.3, 2.4, 2.5, 8.1_

- [x] 3. Enhance FullLoadProgress data model
  - Add counting_strategy field to track which strategy was used
  - Add rows_counted_at field to indicate when counting occurred
  - Add phase-specific duration fields (read, write, count)
  - Add progress_updates_count field
  - Add effective_rows_per_second property
  - _Requirements: 1.4, 1.5, 5.1, 5.2, 5.3_

- [x] 4. Integrate deferred counting into FullLoadDataMigrator
  - Modify perform_full_load_migration to use CountingStrategy
  - Remove immediate df.count() call before write
  - Add deferred counting after write completion
  - Update progress object with counting strategy information
  - Add structured logging for strategy selection
  - _Requirements: 1.1, 1.2, 1.3, 1.4, 4.4_

- [x] 5. Integrate streaming progress into write operations
  - Add StreamingProgressTracker initialization in perform_full_load_migration
  - Integrate progress updates during _write_target_data_with_engine_support
  - Emit progress metrics at configured intervals
  - Update progress tracker on write completion
  - _Requirements: 2.1, 2.2, 2.3, 2.4, 2.5, 5.2_

- [x] 6. Add new CloudWatch metrics for migration progress
  - Implement publish_migration_progress_metrics method with load_type parameter
  - Add metrics for rows_processed, rows_per_second, progress_percentage
  - Add table_name and load_type dimensions to all migration metrics
  - Ensure metrics work for both full load and incremental load operations
  - _Requirements: 2.2, 3.1, 3.2, 3.5, 8.2, 8.5_

- [x] 7. Add CloudWatch metrics for counting strategy
  - Implement publish_counting_strategy_metrics method
  - Add metrics for strategy_type, count_duration, row_count
  - Add table_name and strategy_type dimensions
  - _Requirements: 3.1, 3.5, 4.4_

- [x] 8. Add CloudWatch metrics for migration phases
  - Implement publish_migration_phase_metrics method with load_type parameter
  - Add metrics for read_duration, write_duration, count_duration
  - Add phase and load_type dimensions (read, write, count) x (full, incremental)
  - Add migration_status metric per table with load_type dimension
  - _Requirements: 3.1, 3.3, 3.4, 3.5, 8.2, 8.5_

- [x] 9. Implement Iceberg-specific optimizations
  - Add Iceberg metadata-based row counting in deferred strategy
  - Ensure deferred strategy is used for Iceberg sources
  - Add Iceberg-specific progress tracking
  - Add engine_type indicators in structured logs
  - _Requirements: 6.1, 6.2, 6.3, 6.4, 6.5_

- [x] 10. Add configuration support to JobConfig
  - Create MigrationPerformanceConfig dataclass
  - Add counting_strategy configuration parameter
  - Add progress_tracking configuration parameter
  - Add enable_detailed_metrics configuration parameter
  - Add validation for all new configuration parameters
  - _Requirements: 7.1, 7.2, 7.3, 7.4, 7.5_

- [x] 11. Enhance structured logging for migration operations
  - Add log messages for migration start with engine types
  - Add progress log messages every 100,000 rows
  - Add log messages for migration completion with metrics
  - Add error logging with full context
  - Ensure consistent structured logging format
  - _Requirements: 5.1, 5.2, 5.3, 5.4, 5.5_

- [x] 12. Implement error handling for deferred counting
  - Add try-catch around deferred counting operations
  - Implement fallback to estimated count on failure
  - Add error metrics for counting failures
  - Add warning logs for counting issues
  - _Requirements: 1.5, 5.4_

- [x] 13. Implement error handling for progress tracking
  - Add try-catch around progress update operations
  - Implement graceful degradation if tracking fails
  - Add error metrics for tracking failures
  - Continue migration even if progress tracking fails
  - _Requirements: 2.1, 5.4_

- [x] 14. Update main.py to use new configuration
  - Parse new configuration parameters from job arguments
  - Initialize MigrationPerformanceConfig
  - Pass configuration to FullLoadDataMigrator
  - Add validation for configuration parameters
  - _Requirements: 7.1, 7.2, 7.3, 7.4, 7.5_

- [x] 15. Update CloudFormation template with new parameters
  - Add CountingStrategy parameter (immediate, deferred, auto)
  - Add ProgressUpdateInterval parameter (default 60 seconds)
  - Add BatchSizeThreshold parameter (default 1000000)
  - Add EnableDetailedMetrics parameter (default true)
  - Add parameter descriptions and validation
  - _Requirements: 7.1, 7.2, 7.3, 7.4_

- [x] 16. Create example parameter files
  - Create example for large dataset migration (deferred counting)
  - Create example for small dataset migration (immediate counting)
  - Create example with custom progress tracking configuration
  - Add comments explaining each parameter
  - _Requirements: 4.3, 7.1, 7.2, 7.3_

- [x] 17. Update API documentation
  - Document CountingStrategy class and methods
  - Document StreamingProgressTracker class and methods
  - Document new configuration parameters
  - Document new CloudWatch metrics
  - Add usage examples for each component
  - _Requirements: All requirements (documentation)_

- [x] 18. Create monitoring guide
  - Document all new CloudWatch metrics
  - Provide CloudWatch dashboard JSON template
  - Add sample alarm configurations
  - Document how to interpret progress metrics
  - Add troubleshooting section
  - _Requirements: 3.1, 3.2, 3.3, 3.4, 3.5_

- [x] 19. Update deployment guide
  - Add section on performance configuration
  - Document when to use each counting strategy
  - Add guidance for large dataset migrations
  - Document progress tracking configuration
  - _Requirements: 4.1, 4.2, 4.3, 7.1, 7.2, 7.3_


- [x] 20. Integrate streaming progress into IncrementalDataMigrator
  - Add StreamingProgressTracker initialization in perform_incremental_load_migration
  - Set load_type to 'incremental' for progress tracker
  - Integrate progress updates during incremental write operations
  - Emit progress metrics at configured intervals
  - Update progress tracker on incremental write completion
  - _Requirements: 8.1, 8.2, 8.3_

- [x] 21. Add incremental load metrics to CloudWatch
  - Publish delta_rows metric for incremental loads
  - Publish bookmark_update_status metric
  - Add incremental_column dimension to metrics
  - Ensure all metrics include load_type='incremental' dimension
  - _Requirements: 8.2, 8.4, 8.5_

- [x] 22. Enhance incremental load logging
  - Add structured log for incremental load start with bookmark state
  - Add progress logs every 100,000 delta rows
  - Add log for incremental load completion with delta rows and bookmark update
  - Include incremental_column and last_processed_value in logs
  - _Requirements: 8.3, 8.4_

- [x] 23. Optimize immediate counting with SQL COUNT(*)
  - Implement execute_immediate_count_sql method in CountingStrategy
  - For JDBC sources: Execute `SELECT COUNT(*) FROM schema.table` via connection manager
  - For Iceberg sources: Execute `SELECT COUNT(*) FROM catalog.db.table` via Spark SQL
  - Update auto strategy to always use immediate counting (SQL COUNT is fast for all sizes)
  - Add fallback to deferred counting if SQL COUNT fails
  - Update unit tests for new SQL-based counting
  - _Requirements: 1.2, 1.3, 4.1, 4.2, 4.5_

- [x] 24. Update documentation for SQL COUNT optimization
  - Update DEPLOYMENT_GUIDE.md to reflect that auto now uses immediate for all sizes
  - Update PERFORMANCE_MONITORING_GUIDE.md counting strategy section
  - Remove references to size-based strategy selection (no longer needed)
  - Document that progress percentage is now available from start for all migrations
  - _Requirements: 4.1, 4.2 (documentation)_
