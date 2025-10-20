# Product Overview

## AWS Glue Data Replication Solution

A comprehensive AWS Glue-based data replication solution that supports full-load and incremental data migration across multiple database types with cross-VPC network connectivity.

### Core Purpose
- **Multi-Database Support**: Oracle, SQL Server, PostgreSQL, DB2, Apache Iceberg
- **Cross-VPC Connectivity**: Secure database access across different VPCs
- **Incremental Processing**: Uses Glue job bookmarks for efficient data synchronization
- **Enterprise-Grade**: Comprehensive monitoring, error handling, and security features

### Key Features
- **Incremental Processing**: Automatic incremental column detection and manual bookmark configuration
- **Network Security**: VPC endpoints for private subnet access to AWS services
- **Error Handling**: Robust error recovery and retry mechanisms with table-level isolation
- **Performance Optimization**: Configurable worker types and parallel processing
- **Modular Architecture**: Clean separation of concerns with focused, maintainable modules
- **Comprehensive Monitoring**: CloudWatch metrics, dashboards, and alarms

### Target Use Cases
- Database migration between different engines
- Cross-VPC data replication
- Incremental data synchronization
- Data lake ingestion (via Iceberg support)
- Enterprise data integration workflows

### Architecture Approach
- **Modular Design**: Separate modules for config, database, storage, monitoring, network, and utilities
- **Engine-Agnostic**: Supports both traditional JDBC databases and modern Iceberg tables
- **Cloud-Native**: Built specifically for AWS Glue with S3 integration
- **Scalable**: Configurable worker types and parallel processing capabilities