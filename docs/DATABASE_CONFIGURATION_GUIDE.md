# Database Configuration Guide

This guide provides detailed information about supported database configurations, JDBC driver requirements, and connection setup for the AWS Glue Data Replication system.

## Supported Database Engines

The system supports five major database engines with cross-database replication capabilities:

### Oracle Database

**Supported Versions**: 11g, 12c, 18c, 19c, 21c

**JDBC Driver Requirements**:
- **Driver Class**: `oracle.jdbc.OracleDriver`
- **Recommended Version**: 21.7.0.0
- **JAR Filename**: `ojdbc11.jar`
- **Maven Coordinates**: `com.oracle.database.jdbc:ojdbc11:21.7.0.0`

**Connection String Format**:
```
jdbc:oracle:thin:@{host}:{port}:{database}
```

**Example Connection Strings**:
```
# Standard connection
jdbc:oracle:thin:@oracle-server:1521:ORCL

# Service name format
jdbc:oracle:thin:@//oracle-server:1521/XEPDB1

# RAC connection
jdbc:oracle:thin:@(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST=rac1)(PORT=1521))(ADDRESS=(PROTOCOL=TCP)(HOST=rac2)(PORT=1521)))(CONNECT_DATA=(SERVICE_NAME=ORCL)))
```

**Configuration Notes**:
- Default port: 1521
- Supports both SID and Service Name formats
- Compatible with Oracle RAC configurations
- Requires Oracle client libraries for advanced features

### Microsoft SQL Server

**Supported Versions**: 2012, 2014, 2016, 2017, 2019, 2022

**JDBC Driver Requirements**:
- **Driver Class**: `com.microsoft.sqlserver.jdbc.SQLServerDriver`
- **Recommended Version**: 12.2.0.jre11
- **JAR Filename**: `mssql-jdbc-12.2.0.jre11.jar`
- **Maven Coordinates**: `com.microsoft.sqlserver:mssql-jdbc:12.2.0.jre11`

**Connection String Format**:
```
jdbc:sqlserver://{host}:{port};databaseName={database}
```

**Example Connection Strings**:
```
# Standard connection
jdbc:sqlserver://sqlserver-host:1433;databaseName=AdventureWorks

# Named instance
jdbc:sqlserver://sqlserver-host\\SQLEXPRESS:1433;databaseName=TestDB

# Windows Authentication (requires additional configuration)
jdbc:sqlserver://sqlserver-host:1433;databaseName=TestDB;integratedSecurity=true

# Always Encrypted support
jdbc:sqlserver://sqlserver-host:1433;databaseName=TestDB;columnEncryptionSetting=Enabled
```

**Configuration Notes**:
- Default port: 1433
- Supports named instances
- Compatible with Always Encrypted features
- Supports both SQL Server and Windows authentication

### PostgreSQL

**Supported Versions**: 9.6, 10, 11, 12, 13, 14, 15

**JDBC Driver Requirements**:
- **Driver Class**: `org.postgresql.Driver`
- **Recommended Version**: 42.6.0
- **JAR Filename**: `postgresql-42.6.0.jar`
- **Maven Coordinates**: `org.postgresql:postgresql:42.6.0`

**Connection String Format**:
```
jdbc:postgresql://{host}:{port}/{database}
```

**Example Connection Strings**:
```
# Standard connection
jdbc:postgresql://postgres-host:5432/mydb

# SSL connection
jdbc:postgresql://postgres-host:5432/mydb?ssl=true&sslmode=require

# Connection with schema
jdbc:postgresql://postgres-host:5432/mydb?currentSchema=public

# Connection pooling parameters
jdbc:postgresql://postgres-host:5432/mydb?prepareThreshold=0&preparedStatementCacheQueries=0
```

**Configuration Notes**:
- Default port: 5432
- Excellent SSL/TLS support
- Compatible with Amazon RDS PostgreSQL
- Supports connection pooling parameters

### IBM Db2

**Supported Versions**: 10.5, 11.1, 11.5

**JDBC Driver Requirements**:
- **Driver Class**: `com.ibm.db2.jcc.DB2Driver`
- **Recommended Version**: 11.5.8.0
- **JAR Filename**: `db2jcc4.jar`
- **Maven Coordinates**: `com.ibm.db2:jcc:11.5.8.0`

**Connection String Format**:
```
jdbc:db2://{host}:{port}/{database}
```

**Example Connection Strings**:
```
# Standard connection
jdbc:db2://db2-host:50000/SAMPLE

# Connection with additional properties
jdbc:db2://db2-host:50000/SAMPLE:user=db2user;password=db2pass;

# Secure connection
jdbc:db2://db2-host:50001/SAMPLE:sslConnection=true;
```

**Configuration Notes**:
- Default port: 50000
- Supports SSL connections
- Compatible with Db2 on Cloud
- Requires specific driver configuration for optimal performance

### Apache Iceberg

**Supported Versions**: Format version 1 and 2

**JDBC Driver Requirements**:
- **Driver Class**: Not applicable (uses Spark native integration)
- **JAR Dependencies**: Built into AWS Glue 3.0+ runtime
- **Additional JARs**: None required

**Configuration Format**:
```
Engine Type: iceberg
Database Name: {glue_catalog_database}
Table Name: {iceberg_table_name}
Warehouse Location: s3://{bucket}/{prefix}/
```

**Example Configurations**:
```json
{
  "SourceEngine": "iceberg",
  "SourceDatabaseName": "analytics_db",
  "SourceTableName": "customer_events",
  "SourceWarehouseLocation": "s3://my-datalake/warehouse/"
}
```

**Configuration Notes**:
- No JDBC driver required - uses Glue Data Catalog and Spark
- Requires S3 warehouse location for table data storage
- Supports cross-account Glue Data Catalog access with catalog_id
- Automatic table creation when used as target
- Built-in support for schema evolution and time travel
- Optimized for analytical workloads and large-scale data processing

## JDBC Driver Management

### Driver Download and Storage

#### Oracle JDBC Driver

```bash
# Download Oracle JDBC driver (requires Oracle account)
wget https://download.oracle.com/otn-pub/otn_software/jdbc/217/ojdbc11.jar

# Upload to S3
aws s3 cp ojdbc11.jar s3://your-bucket/jdbc-drivers/oracle/21.7.0.0/ojdbc11.jar
```

#### SQL Server JDBC Driver

```bash
# Download from Maven Central
wget https://repo1.maven.org/maven2/com/microsoft/sqlserver/mssql-jdbc/12.2.0.jre11/mssql-jdbc-12.2.0.jre11.jar

# Upload to S3
aws s3 cp mssql-jdbc-12.2.0.jre11.jar s3://your-bucket/jdbc-drivers/sqlserver/12.2.0.jre11/mssql-jdbc-12.2.0.jre11.jar
```

#### PostgreSQL JDBC Driver

```bash
# Download from Maven Central
wget https://repo1.maven.org/maven2/org/postgresql/postgresql/42.6.0/postgresql-42.6.0.jar

# Upload to S3
aws s3 cp postgresql-42.6.0.jar s3://your-bucket/jdbc-drivers/postgresql/42.6.0/postgresql-42.6.0.jar
```

#### Db2 JDBC Driver

```bash
# Download from Maven Central
wget https://repo1.maven.org/maven2/com/ibm/db2/jcc/11.5.8.0/jcc-11.5.8.0.jar

# Upload to S3
aws s3 cp jcc-11.5.8.0.jar s3://your-bucket/jdbc-drivers/db2/11.5.8.0/db2jcc4.jar
```

### S3 Storage Best Practices

**Recommended Directory Structure**:
```
s3://your-glue-assets/
├── jdbc-drivers/
│   ├── oracle/
│   │   ├── 21.7.0.0/
│   │   │   └── ojdbc11.jar
│   │   └── 21.8.0.0/
│   │       └── ojdbc11.jar
│   ├── sqlserver/
│   │   └── 12.2.0.jre11/
│   │       └── mssql-jdbc-12.2.0.jre11.jar
│   ├── postgresql/
│   │   └── 42.6.0/
│   │       └── postgresql-42.6.0.jar
│   └── db2/
│       └── 11.5.8.0/
│           └── db2jcc4.jar
└── src/
    └── glue_job/
        ├── main.py
        ├── config/
        ├── database/
        ├── storage/
        └── utils/
```

**Version Management**:
- Keep multiple driver versions for compatibility
- Use semantic versioning in directory names
- Implement lifecycle policies for old versions
- Document driver compatibility matrices

## Iceberg Configuration

### Glue Data Catalog Setup

**Prerequisites**:
1. AWS Glue Data Catalog database must exist
2. S3 warehouse location must be accessible
3. Proper IAM permissions for Glue and S3 operations

**Creating Glue Database**:
```bash
# Create database in Glue Data Catalog
aws glue create-database \
  --database-input Name=analytics_db,Description="Analytics data lake database"
```

**Required IAM Permissions**:
```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "glue:GetDatabase",
                "glue:GetTable",
                "glue:CreateTable",
                "glue:UpdateTable",
                "glue:GetPartitions"
            ],
            "Resource": "*"
        },
        {
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:PutObject",
                "s3:DeleteObject",
                "s3:ListBucket"
            ],
            "Resource": [
                "arn:aws:s3:::your-warehouse-bucket/*",
                "arn:aws:s3:::your-warehouse-bucket"
            ]
        }
    ]
}
```

### Warehouse Location Configuration

**S3 Bucket Setup**:
```bash
# Create S3 bucket for Iceberg warehouse
aws s3 mb s3://my-iceberg-warehouse

# Set up bucket versioning (recommended)
aws s3api put-bucket-versioning \
  --bucket my-iceberg-warehouse \
  --versioning-configuration Status=Enabled

# Configure server-side encryption
aws s3api put-bucket-encryption \
  --bucket my-iceberg-warehouse \
  --server-side-encryption-configuration '{
    "Rules": [{
      "ApplyServerSideEncryptionByDefault": {
        "SSEAlgorithm": "AES256"
      }
    }]
  }'
```

**Recommended Directory Structure**:
```
s3://my-iceberg-warehouse/
├── analytics_db/
│   ├── customer_events/
│   │   ├── data/
│   │   └── metadata/
│   ├── transaction_history/
│   │   ├── data/
│   │   └── metadata/
│   └── daily_aggregates/
│       ├── data/
│       └── metadata/
└── processed_data/
    ├── ml_features/
    └── reporting_views/
```

### Cross-Account Configuration

**Source Account Setup** (where Glue job runs):
```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "glue:GetDatabase",
                "glue:GetTable"
            ],
            "Resource": [
                "arn:aws:glue:region:target-account:catalog",
                "arn:aws:glue:region:target-account:database/*",
                "arn:aws:glue:region:target-account:table/*"
            ]
        }
    ]
}
```

**Target Account Setup** (where Iceberg tables reside):
```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {
                "AWS": "arn:aws:iam::source-account:role/GlueJobRole"
            },
            "Action": [
                "glue:GetDatabase",
                "glue:GetTable",
                "glue:CreateTable",
                "glue:UpdateTable"
            ],
            "Resource": "*"
        }
    ]
}
```

### Table Creation and Schema Management

**Automatic Table Creation**:
When using Iceberg as target, tables are automatically created with:
- Schema inferred from source JDBC metadata
- Identifier-field-ids configured for bookmark management
- Format version 2 for optimal performance
- Proper data type mapping from source to Iceberg types

**Manual Table Creation** (optional):
```sql
CREATE TABLE glue_catalog.analytics_db.customer_events (
    event_id bigint,
    customer_id string,
    event_type string,
    event_timestamp timestamp,
    event_data string
) USING iceberg
TBLPROPERTIES (
    'format-version'='2',
    'identifier-field-ids'='1'
);
```

### Performance Optimization

**Partitioning Strategy**:
```sql
-- Date-based partitioning
CREATE TABLE glue_catalog.analytics_db.events (
    event_id bigint,
    event_date date,
    event_data string
) USING iceberg
PARTITIONED BY (days(event_date));

-- Multi-level partitioning
CREATE TABLE glue_catalog.analytics_db.transactions (
    transaction_id bigint,
    customer_id string,
    transaction_date date,
    amount decimal(10,2)
) USING iceberg
PARTITIONED BY (days(transaction_date), bucket(16, customer_id));
```

**File Size Optimization**:
- Target 128MB-1GB file sizes
- Enable automatic compaction
- Monitor file count and distribution

## Cross-Database Replication Scenarios

### Supported Replication Patterns

| Source | Target | Complexity | Notes |
|--------|--------|------------|-------|
| Oracle → PostgreSQL | Medium | Common migration pattern, good type mapping |
| SQL Server → PostgreSQL | Medium | Well-supported, minimal data loss |
| Oracle → SQL Server | Medium | Enterprise-to-enterprise migration |
| PostgreSQL → Oracle | High | Requires careful type mapping |
| Any → Same | Low | Homogeneous replication, best performance |
| Db2 → PostgreSQL | High | Legacy modernization, complex types |
| SQL Server → Oracle | High | Cross-platform enterprise migration |
| **Any JDBC → Iceberg** | **Low** | **Data lake ingestion, automatic table creation** |
| **Iceberg → Any JDBC** | **Medium** | **Data lake to operational systems** |
| **Iceberg → Iceberg** | **Low** | **Data lake transformations, cross-account** |

### Data Type Mapping

#### Oracle to PostgreSQL

| Oracle Type | PostgreSQL Type | Notes |
|-------------|-----------------|-------|
| VARCHAR2(n) | VARCHAR(n) | Direct mapping |
| NUMBER(p,s) | NUMERIC(p,s) | Precision preserved |
| DATE | TIMESTAMP | Oracle DATE includes time |
| TIMESTAMP | TIMESTAMP | Direct mapping |
| CLOB | TEXT | Large text objects |
| BLOB | BYTEA | Binary data |
| RAW | BYTEA | Raw binary data |

#### SQL Server to PostgreSQL

| SQL Server Type | PostgreSQL Type | Notes |
|-----------------|-----------------|-------|
| NVARCHAR(n) | VARCHAR(n) | Unicode support |
| INT | INTEGER | Direct mapping |
| BIGINT | BIGINT | Direct mapping |
| DATETIME | TIMESTAMP | Direct mapping |
| UNIQUEIDENTIFIER | UUID | Requires UUID extension |
| VARBINARY | BYTEA | Binary data |
| TEXT | TEXT | Large text |

#### PostgreSQL to Oracle

| PostgreSQL Type | Oracle Type | Notes |
|-----------------|-------------|-------|
| VARCHAR(n) | VARCHAR2(n) | Direct mapping |
| INTEGER | NUMBER(10) | Integer precision |
| BIGINT | NUMBER(19) | Long integer |
| TIMESTAMP | TIMESTAMP | Direct mapping |
| UUID | RAW(16) | Binary representation |
| BYTEA | BLOB | Binary large objects |
| TEXT | CLOB | Large text objects |

#### JDBC to Iceberg

| JDBC Type | Iceberg Type | Notes |
|-----------|--------------|-------|
| VARCHAR, CHAR, TEXT | string | All text types map to string |
| INTEGER, SMALLINT | int | Integer types |
| BIGINT | long | Long integer |
| DECIMAL, NUMERIC | decimal(precision,scale) | Preserves precision and scale |
| FLOAT | float | Single precision |
| DOUBLE | double | Double precision |
| BOOLEAN | boolean | Boolean values |
| DATE | date | Date only |
| TIMESTAMP | timestamp | Date and time |
| TIME | time | Time only |
| BINARY, VARBINARY | binary | Binary data |

#### Iceberg to JDBC

| Iceberg Type | Target JDBC Type | Notes |
|--------------|------------------|-------|
| string | VARCHAR(max_length) | Length determined by target DB |
| int | INTEGER | Standard integer |
| long | BIGINT | Long integer |
| decimal(p,s) | DECIMAL(p,s) | Preserves precision and scale |
| float | FLOAT | Single precision |
| double | DOUBLE | Double precision |
| boolean | BOOLEAN | Boolean values |
| date | DATE | Date only |
| timestamp | TIMESTAMP | Date and time |
| time | TIME | Time only |
| binary | VARBINARY | Binary data |

### Schema Compatibility Requirements

**Pre-Migration Assumptions**:
1. Target schema structure already exists
2. Table definitions are compatible
3. Primary keys and indexes are created
4. Constraints are properly defined
5. Data types are mapped appropriately

**Validation Checklist**:
- [ ] All source tables exist in target
- [ ] Column names and types are compatible
- [ ] Primary key constraints match
- [ ] Foreign key relationships are preserved
- [ ] Index strategies are optimized for target
- [ ] Character encoding is consistent

## Connection Configuration

### Security Considerations

#### SSL/TLS Configuration

**PostgreSQL SSL Example**:
```
jdbc:postgresql://host:5432/db?ssl=true&sslmode=require&sslcert=client-cert.pem&sslkey=client-key.pem&sslrootcert=ca-cert.pem
```

**SQL Server Encryption Example**:
```
jdbc:sqlserver://host:1433;databaseName=db;encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;
```

**Oracle SSL Example**:
```
jdbc:oracle:thin:@(DESCRIPTION=(ADDRESS=(PROTOCOL=tcps)(HOST=host)(PORT=2484))(CONNECT_DATA=(SERVICE_NAME=service))(SECURITY=(SSL_SERVER_CERT_DN="CN=host")))
```

#### Authentication Methods

**Database Authentication**:
- Standard username/password
- Encrypted password storage recommended
- Regular credential rotation

**Integrated Authentication**:
- Windows Authentication (SQL Server)
- Kerberos authentication
- LDAP integration

**Cloud Authentication**:
- AWS IAM database authentication
- Azure Active Directory
- Google Cloud IAM

### Connection Pooling

#### Recommended Settings

**High-Volume Workloads**:
```properties
# Connection pool settings
initialPoolSize=5
minPoolSize=5
maxPoolSize=20
acquireIncrement=2
maxIdleTime=300
checkoutTimeout=30000
```

**Low-Latency Requirements**:
```properties
# Optimized for speed
initialPoolSize=10
minPoolSize=10
maxPoolSize=50
acquireIncrement=5
maxIdleTime=180
checkoutTimeout=10000
```

### Network Configuration

#### Firewall Rules

**Source Database Access**:
```bash
# Allow Glue service IP ranges
# (Replace with actual Glue service IP ranges for your region)
iptables -A INPUT -s 10.0.0.0/8 -p tcp --dport 1521 -j ACCEPT  # Oracle
iptables -A INPUT -s 10.0.0.0/8 -p tcp --dport 1433 -j ACCEPT  # SQL Server
iptables -A INPUT -s 10.0.0.0/8 -p tcp --dport 5432 -j ACCEPT  # PostgreSQL
iptables -A INPUT -s 10.0.0.0/8 -p tcp --dport 50000 -j ACCEPT # Db2
```

**Security Group Configuration**:
```json
{
  "GroupName": "glue-database-access",
  "Description": "Allow Glue access to databases",
  "SecurityGroupRules": [
    {
      "IpProtocol": "tcp",
      "FromPort": 1521,
      "ToPort": 1521,
      "CidrIp": "10.0.0.0/16",
      "Description": "Oracle database access"
    },
    {
      "IpProtocol": "tcp",
      "FromPort": 1433,
      "ToPort": 1433,
      "CidrIp": "10.0.0.0/16",
      "Description": "SQL Server database access"
    }
  ]
}
```

## Performance Optimization

### Database-Specific Tuning

#### Oracle Optimization

```sql
-- Enable parallel query
ALTER SESSION SET parallel_degree_policy = AUTO;

-- Optimize for bulk operations
ALTER SESSION SET db_file_multiblock_read_count = 128;

-- Enable result cache
ALTER SESSION SET result_cache_mode = FORCE;
```

#### SQL Server Optimization

```sql
-- Enable read committed snapshot
ALTER DATABASE YourDB SET READ_COMMITTED_SNAPSHOT ON;

-- Optimize for bulk operations
ALTER DATABASE YourDB SET AUTO_UPDATE_STATISTICS_ASYNC ON;

-- Enable page compression
ALTER TABLE YourTable REBUILD WITH (DATA_COMPRESSION = PAGE);
```

#### PostgreSQL Optimization

```sql
-- Increase work memory for sorting
SET work_mem = '256MB';

-- Enable parallel workers
SET max_parallel_workers_per_gather = 4;

-- Optimize for bulk operations
SET synchronous_commit = OFF;
```

#### Db2 Optimization

```sql
-- Enable optimization
SET CURRENT DEGREE = 'ANY';

-- Optimize buffer pools
ALTER BUFFERPOOL IBMDEFAULTBP SIZE 10000;

-- Enable compression
ALTER TABLE YourTable COMPRESS YES;
```

### JDBC Connection Optimization

#### Connection String Parameters

**Oracle Performance Parameters**:
```
jdbc:oracle:thin:@host:1521:db?defaultRowPrefetch=1000&defaultBatchValue=1000&useFetchSizeWithLongColumn=true
```

**SQL Server Performance Parameters**:
```
jdbc:sqlserver://host:1433;databaseName=db;selectMethod=cursor;responseBuffering=adaptive;packetSize=8192
```

**PostgreSQL Performance Parameters**:
```
jdbc:postgresql://host:5432/db?prepareThreshold=0&defaultRowFetchSize=1000&reWriteBatchedInserts=true
```

**Db2 Performance Parameters**:
```
jdbc:db2://host:50000/db:blockingReadConnectionTimeout=0;queryDataSize=65536;
```

## Troubleshooting

### Common Connection Issues

#### Oracle Connection Problems

**TNS Listener Issues**:
```bash
# Test TNS connectivity
tnsping ORCL

# Check listener status
lsnrctl status
```

**Common Error Messages**:
- `ORA-12541: TNS:no listener` - Check listener configuration
- `ORA-12514: TNS:listener does not currently know of service` - Verify service name
- `ORA-01017: invalid username/password` - Check credentials

#### SQL Server Connection Problems

**Network Connectivity**:
```bash
# Test port connectivity
telnet sqlserver-host 1433

# Check SQL Server configuration
sqlcmd -S server -U user -P password -Q "SELECT @@VERSION"
```

**Common Error Messages**:
- `Login failed for user` - Check authentication method
- `Cannot open database` - Verify database exists and permissions
- `Connection timeout expired` - Check network and firewall settings

#### PostgreSQL Connection Problems

**Connection Testing**:
```bash
# Test connection
psql -h postgres-host -p 5432 -U user -d database

# Check server status
pg_isready -h postgres-host -p 5432
```

**Common Error Messages**:
- `FATAL: password authentication failed` - Check credentials
- `FATAL: database does not exist` - Verify database name
- `could not connect to server` - Check network connectivity

#### Db2 Connection Problems

**Connection Testing**:
```bash
# Test connection
db2 connect to database user username using password

# Check instance status
db2pd -inst
```

**Common Error Messages**:
- `SQL30081N: Communication error` - Check network connectivity
- `SQL1013N: Database not found` - Verify database name
- `SQL30082N: Security processing failed` - Check authentication

### Performance Troubleshooting

#### Slow Query Performance

1. **Enable Query Logging**:
   - Oracle: Enable SQL trace
   - SQL Server: Use Query Store
   - PostgreSQL: Enable log_statement
   - Db2: Enable statement event monitor

2. **Analyze Execution Plans**:
   - Check for full table scans
   - Verify index usage
   - Look for blocking operations

3. **Monitor Resource Usage**:
   - CPU utilization
   - Memory consumption
   - I/O patterns
   - Network latency

#### Connection Pool Issues

**Symptoms**:
- Connection timeouts
- Pool exhaustion errors
- High connection creation overhead

**Solutions**:
- Adjust pool sizing parameters
- Implement connection validation
- Monitor pool metrics
- Optimize connection lifecycle

## Best Practices

### Security Best Practices

1. **Credential Management**:
   - Use AWS Secrets Manager
   - Implement credential rotation
   - Avoid hardcoded passwords

2. **Network Security**:
   - Use VPC endpoints
   - Implement security groups
   - Enable encryption in transit

3. **Access Control**:
   - Principle of least privilege
   - Regular access reviews
   - Audit trail maintenance

### Performance Best Practices

1. **Connection Management**:
   - Use connection pooling
   - Optimize connection parameters
   - Monitor connection health

2. **Query Optimization**:
   - Use appropriate indexes
   - Optimize WHERE clauses
   - Implement query caching

3. **Data Transfer**:
   - Use bulk operations
   - Implement parallel processing
   - Optimize batch sizes

### Monitoring Best Practices

1. **Database Monitoring**:
   - Track connection counts
   - Monitor query performance
   - Alert on error conditions

2. **Application Monitoring**:
   - Log connection events
   - Track processing metrics
   - Monitor resource usage

3. **Infrastructure Monitoring**:
   - Network latency tracking
   - Resource utilization alerts
   - Capacity planning metrics

This guide provides comprehensive information for configuring and optimizing database connections in the AWS Glue Data Replication system. For specific issues not covered here, consult the database vendor documentation or contact your database administrator.