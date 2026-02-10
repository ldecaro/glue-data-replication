# Test Table Generation Examples

This guide shows how to generate test tables for your source database using the `generate_test_tables.py` script.

## Prerequisites

### Python Requirements

The test data generation requires the following Python packages:

```bash
pip install faker pandas
```

Or install all test dependencies:

```bash
pip install -r tests/requirements-test.txt
```

### Required Files

The `generate_test_tables.py` script depends on `test_data_setup.py` for data generation. Both files are located in the `tests/` directory:

- `tests/generate_test_tables.py` - Main script for DDL and data generation
- `tests/test_data_setup.py` - Contains `DataGenerator` and `DataExporter` classes

**Important**: Run the script from the project root directory to ensure imports work correctly:

```bash
cd /path/to/glue-datareplication
python tests/generate_test_tables.py --engine oracle --schema HR --with-data
```

If you see the error `Could not import TestDataGenerator`, you're likely running from the wrong directory or missing the `test_data_setup.py` file.

## Quick Start

### 1. Generate DDL Only (No Data)

```bash
# Oracle
python tests/generate_test_tables.py --engine oracle --schema HR

# SQL Server  
python tests/generate_test_tables.py --engine sqlserver --schema dbo

# PostgreSQL
python tests/generate_test_tables.py --engine postgresql --schema public

# DB2
python tests/generate_test_tables.py --engine db2 --schema TESTDB
```

### 2. Generate DDL with Sample Data

```bash
# Small dataset (100 customers, 500 orders, 200 products)
python tests/generate_test_tables.py --engine oracle --schema HR --with-data

# Medium dataset (1K customers, 5K orders, 1K products)
python tests/generate_test_tables.py --engine postgresql --schema public --with-data --data-size medium

# Large dataset (10K customers, 50K orders, 5K products)
python tests/generate_test_tables.py --engine sqlserver --schema dbo --with-data --data-size large
```

### 3. Save to File

```bash
# Generate Oracle tables and save to file
python tests/generate_test_tables.py --engine oracle --schema HR --with-data --output oracle_test_tables.sql

# Generate PostgreSQL tables with DROP statements
python tests/generate_test_tables.py --engine postgresql --schema public --with-data --drop-tables --output postgres_test_tables.sql
```

### 4. Generate Specific Tables Only

```bash
# Only customers and orders tables
python tests/generate_test_tables.py --engine oracle --schema HR --tables customers orders --with-data

# Only products table
python tests/generate_test_tables.py --engine sqlserver --schema dbo --tables products --with-data
```

## Available Tables

The script generates these test tables:

1. **customers** - Customer master data with demographics
2. **orders** - Order transactions with status tracking  
3. **products** - Product catalog with pricing
4. **inventory** - Stock levels by warehouse
5. **order_items** - Order line items with quantities

## Database-Specific Examples

### Oracle Example

```bash
python tests/generate_test_tables.py \
  --engine oracle \
  --schema HR \
  --with-data \
  --data-size medium \
  --drop-tables \
  --output oracle_hr_tables.sql
```

This generates:
- Oracle-specific data types (VARCHAR2, NUMBER, TIMESTAMP)
- Proper Oracle date/timestamp literals
- CASCADE CONSTRAINTS on DROP statements

### SQL Server Example

```bash
python tests/generate_test_tables.py \
  --engine sqlserver \
  --schema dbo \
  --with-data \
  --data-size small \
  --output sqlserver_test_tables.sql
```

This generates:
- SQL Server data types (NVARCHAR, INT, DATETIME2, BIT)
- Proper SQL Server date literals
- IF EXISTS checks on DROP statements

### PostgreSQL Example

```bash
python tests/generate_test_tables.py \
  --engine postgresql \
  --schema public \
  --with-data \
  --data-size large \
  --output postgres_test_tables.sql
```

This generates:
- PostgreSQL data types (VARCHAR, INTEGER, BOOLEAN)
- Standard SQL date literals
- CASCADE on DROP statements

## Running the Generated SQL

### Oracle
```sql
-- Connect as HR user
sqlplus hr/password@database

-- Run the generated script
@oracle_hr_tables.sql
```

### SQL Server
```sql
-- Connect to database
sqlcmd -S server -d database -U user -P password

-- Run the generated script
:r sqlserver_test_tables.sql
GO
```

### PostgreSQL
```sql
-- Connect to database
psql -h host -d database -U user

-- Run the generated script
\i postgres_test_tables.sql
```

## Data Sizes

| Size | Customers | Orders | Products | Order Items | Total Records |
|------|-----------|--------|----------|-------------|---------------|
| Small | 100 | 500 | 200 | ~1,500 | ~2,300 |
| Medium | 1,000 | 5,000 | 1,000 | ~15,000 | ~22,000 |
| Large | 10,000 | 50,000 | 5,000 | ~150,000 | ~215,000 |

## Sample Output Structure

```sql
-- Test Tables for ORACLE
-- Generated on 2024-01-15 10:30:00
-- Schema: HR

-- Drop existing tables
DROP TABLE HR.order_items CASCADE CONSTRAINTS;
DROP TABLE HR.inventory CASCADE CONSTRAINTS;
DROP TABLE HR.products CASCADE CONSTRAINTS;
DROP TABLE HR.orders CASCADE CONSTRAINTS;
DROP TABLE HR.customers CASCADE CONSTRAINTS;

-- Create tables
-- Table: customers
CREATE TABLE HR.customers (
    customer_id NUMBER(10) NOT NULL,
    first_name VARCHAR2(50) NOT NULL,
    last_name VARCHAR2(50) NOT NULL,
    email VARCHAR2(100),
    -- ... more columns
    CONSTRAINT pk_customers PRIMARY KEY (customer_id)
);

-- Sample data
-- Data for customers
INSERT INTO HR.customers (customer_id, first_name, last_name, email, ...) 
VALUES (1, 'John', 'Doe', 'john.doe@email.com', ...);
-- ... more INSERT statements
```


## Loading CSV Data into Database

After generating CSV test data using `test_data_setup.py`, you can load it into your database using the Java-based data loaders.

### Prerequisites

1. **Java JDK 8 or higher**
   ```bash
   java -version
   javac -version
   ```

2. **JDBC Driver** for your database (place in `tests/` directory):
   - **SQL Server**: [mssql-jdbc-12.2.0.jre11.jar](https://learn.microsoft.com/en-us/sql/connect/jdbc/download-microsoft-jdbc-driver-for-sql-server)
   - **Oracle**: [ojdbc8.jar](https://www.oracle.com/database/technologies/jdbc-ucp-122-downloads.html)
   - **PostgreSQL**: [postgresql-42.6.0.jar](https://jdbc.postgresql.org/download/)
   - **DB2**: [db2jcc4.jar](https://www.ibm.com/support/pages/db2-jdbc-driver-versions-and-downloads)

3. **CSV Files**: Generate test data first:
   ```bash
   python tests/test_data_setup.py --size xlarge --db-type sqlserver
   ```

### CSV File Requirements

#### File Naming Convention
CSV files should follow this pattern:
- `{size}_{table_name}.csv`
- Examples: `xlarge_customers.csv`, `medium_orders.csv`, `small_products.csv`

#### CSV Format
- First row must contain column headers
- Values can be quoted or unquoted
- NULL values: empty string, "null", or "NULL"
- Date format: `YYYY-MM-DD`
- Timestamp format: `YYYY-MM-DD HH:MM:SS` or `YYYY-MM-DDTHH:MM:SS`
- Boolean values: `1`/`0`, `true`/`false`, `t`/`f`, `yes`/`no`

#### Example CSV
```csv
customer_id,first_name,last_name,email,is_active,created_at
1,John,Doe,john@example.com,1,2024-01-15 10:30:00
2,Jane,Smith,jane@example.com,0,2024-01-16 14:20:00
```

### Table Loading Order

The loaders automatically load tables in the correct order to respect foreign key dependencies:

1. `customers`
2. `products`
3. `orders`
4. `inventory`
5. `order_items`

### TestDataLoader.java - Single-Threaded Loader

A simple, reliable loader for smaller datasets or when debugging is needed.

#### Compilation

```bash
cd tests
javac TestDataLoader.java
```

#### Command Line Arguments

```
java -cp ".:driver.jar" TestDataLoader <engine> <csv_dir> <jdbc_url> <username> <password> [schema] [table1 table2 ...]
```

| Argument | Required | Description | Example |
|----------|----------|-------------|---------|
| engine | Yes | Database engine type | `sqlserver`, `oracle`, `postgresql`, `db2` |
| csv_dir | Yes | Directory containing CSV files | `./test_data` |
| jdbc_url | Yes | JDBC connection URL | `jdbc:sqlserver://localhost:1433;databaseName=testdb` |
| username | Yes | Database username | `sa`, `hr`, `postgres` |
| password | Yes | Database password | `YourPassword` |
| schema | No | Schema name (defaults based on engine) | `dbo`, `HR`, `public` |
| tables | No | Specific tables to load (space-separated) | `customers orders products` |

#### Usage Examples

**SQL Server:**
```bash
java -cp ".:mssql-jdbc-12.2.0.jre11.jar" TestDataLoader \
  sqlserver \
  ./test_data \
  "jdbc:sqlserver://localhost:1433;databaseName=testdb;encrypt=false" \
  sa \
  YourPassword \
  dbo
```

**Oracle:**
```bash
java -cp ".:ojdbc8.jar" TestDataLoader \
  oracle \
  ./test_data \
  "jdbc:oracle:thin:@localhost:1521:ORCL" \
  hr \
  hr \
  HR
```

**PostgreSQL:**
```bash
java -cp ".:postgresql-42.6.0.jar" TestDataLoader \
  postgresql \
  ./test_data \
  "jdbc:postgresql://localhost:5432/testdb" \
  postgres \
  postgres \
  public
```

**DB2:**
```bash
java -cp ".:db2jcc4.jar" TestDataLoader \
  db2 \
  ./test_data \
  "jdbc:db2://localhost:50000/testdb" \
  db2inst1 \
  password \
  DB2INST1
```

**Load specific tables only:**
```bash
java -cp ".:mssql-jdbc-12.2.0.jre11.jar" TestDataLoader \
  sqlserver \
  ./test_data \
  "jdbc:sqlserver://localhost:1433;databaseName=testdb;encrypt=false" \
  sa \
  YourPassword \
  dbo \
  customers orders
```

### TestDataLoaderMultiThreaded.java - High Performance Loader

The multi-threaded loader provides 3-5x faster performance compared to the single-threaded version by using parallel batch processing with 4 worker threads.

#### Compilation

```bash
# Compile the multi-threaded loader
javac -cp ".:tests/lib/*" tests/TestDataLoaderMultiThreaded.java

# Or on Windows
javac -cp ".;tests/lib/*" tests/TestDataLoaderMultiThreaded.java
```

#### Usage

```bash
java -cp ".:tests/lib/*" TestDataLoaderMultiThreaded \
  <jdbc_url> \
  <database> \
  <username> \
  <password> \
  <csv_directory> \
  [table_prefix1] [table_prefix2] ...

# Or on Windows
java -cp ".;tests/lib/*" TestDataLoaderMultiThreaded ^
  <jdbc_url> ^
  <database> ^
  <username> ^
  <password> ^
  <csv_directory> ^
  [table_prefix1] [table_prefix2] ...
```

#### Examples

**Load all xlarge tables into SQL Server:**
```bash
java -cp ".:tests/lib/*" TestDataLoaderMultiThreaded \
  "jdbc:sqlserver://myserver.database.windows.net:1433" \
  "testdb" \
  "admin" \
  "MyPassword123!" \
  "tests/test_data" \
  xlarge
```

**Load specific tables (customers and orders only):**
```bash
java -cp ".:tests/lib/*" TestDataLoaderMultiThreaded \
  "jdbc:sqlserver://myserver.database.windows.net:1433" \
  "testdb" \
  "admin" \
  "MyPassword123!" \
  "tests/test_data" \
  xlarge_customers \
  xlarge_orders
```

**Load medium dataset into PostgreSQL:**
```bash
java -cp ".:tests/lib/*" TestDataLoaderMultiThreaded \
  "jdbc:postgresql://localhost:5432/testdb" \
  "testdb" \
  "postgres" \
  "password" \
  "tests/test_data" \
  medium
```

**Load small dataset into Oracle:**
```bash
java -cp ".:tests/lib/*" TestDataLoaderMultiThreaded \
  "jdbc:oracle:thin:@localhost:1521:ORCL" \
  "HR" \
  "hr_user" \
  "password" \
  "tests/test_data" \
  small
```

#### Performance Characteristics

**Multi-threaded Loader (4 worker threads):**
- **Small dataset** (~2,300 records): ~2-5 seconds
- **Medium dataset** (~22,000 records): ~15-30 seconds
- **Large dataset** (~215,000 records): ~2-5 minutes
- **XLarge dataset** (~6.6M records): ~30-60 minutes

**Performance Features:**
- 4 parallel worker threads for concurrent batch processing
- Connection pooling for efficient database connections
- Async batch execution with queue-based work distribution
- Real-time progress monitoring with throughput metrics
- Automatic data type detection and conversion
- Table-level isolation (failures in one table don't affect others)

#### Output Example

```
=== Multi-Threaded CSV Data Loader ===
Worker threads: 4
Batch size: 1000

Found 5 CSV files matching prefix 'xlarge':
  - xlarge_customers.csv (1,100,000 records)
  - xlarge_orders.csv (5,000,000 records)
  - xlarge_products.csv (500,000 records)
  - xlarge_inventory.csv (1,000,000 records)
  - xlarge_order_items.csv (15,007,053 records)

Loading: xlarge_customers.csv -> dbo.customers
Progress: Read 100,000 | Inserted 100,000 (25,000 rec/sec) | Queue: 0
Progress: Read 200,000 | Inserted 200,000 (28,571 rec/sec) | Queue: 0
...
✓ Completed: xlarge_customers.csv (1,100,000 records in 42.3s, avg 26,005 rec/sec)

Loading: xlarge_orders.csv -> dbo.orders
Progress: Read 500,000 | Inserted 500,000 (31,250 rec/sec) | Queue: 2
Progress: Read 1,000,000 | Inserted 1,000,000 (33,333 rec/sec) | Queue: 1
...
✓ Completed: xlarge_orders.csv (5,000,000 records in 3m 12s, avg 26,041 rec/sec)

=== Load Summary ===
Total files processed: 5
Total records loaded: 22,607,053
Total time: 14m 32s
Average throughput: 25,932 records/sec
```

#### Error Handling

The loader provides detailed error messages:

```
Loading: xlarge_orders.csv -> dbo.orders
Progress: Read 10,000 | Inserted 0 (0 rec/sec) | Queue: 6
SQL Error: Conversion failed when converting date and/or time from character string.
Worker 1 error: Conversion failed when converting date and/or time...
✗ Failed: xlarge_orders.csv (Error: Batch execution failed)
```

**Common Issues:**
- **Date/Time conversion errors**: Regenerate CSV files with correct datetime format
- **Connection timeout**: Increase connection timeout in JDBC URL
- **Out of memory**: Reduce batch size or increase JVM heap size with `-Xmx4g`
- **Deadlocks**: Reduce number of worker threads

#### Shell Script Wrapper

For convenience, use the provided shell script:

```bash
# Make executable
chmod +x tests/load_test_data.sh

# Load data
./tests/load_test_data.sh \
  "jdbc:sqlserver://myserver:1433" \
  "testdb" \
  "admin" \
  "password" \
  "tests/test_data" \
  xlarge
```

### Progress Output

Both loaders provide real-time progress updates:

```
================================================================================
Test Data Loader
================================================================================
Engine:    sqlserver
Schema:    dbo
JDBC URL:  jdbc:sqlserver://localhost:1433;databaseName=testdb
CSV Dir:   ./test_data
================================================================================

Connecting to database...
✅ Connected successfully

Found 5 CSV files to load

Loading: xlarge_customers.csv -> dbo.customers
  Progress: 5,000 records (2,500 records/sec)
  Progress: 10,000 records (2,450 records/sec)
  ✅ Completed: 10,000 records in 4.08 seconds (2,451 records/sec)

Loading: xlarge_orders.csv -> dbo.orders
  Progress: 5,000 records (3,200 records/sec)
  ...
  ✅ Completed: 50,000 records in 15.87 seconds (3,151 records/sec)

================================================================================
✅ Data loading completed successfully!
Total records inserted: 215,000
================================================================================
```

### Performance Tuning

#### Batch Size
Default: 1000 records per batch. Modify in code:
```java
private static final int BATCH_SIZE = 1000;  // Increase for better performance
```

#### Progress Interval
Default: Updates every 5000 records. Modify in code:
```java
private static final int PROGRESS_INTERVAL = 5000;  // Adjust as needed
```

#### Memory Settings
For large datasets, increase JVM heap size:
```bash
java -Xmx2g -cp ".:driver.jar" TestDataLoader sqlserver ./test_data "jdbc:..." user pass schema
```

### Integration Workflow

Complete workflow from data generation to database loading:

#### 1. Generate Test Data
```bash
cd tests
python test_data_setup.py --db-type sqlserver --size large
```

This creates CSV files: `large_customers.csv`, `large_orders.csv`, `large_products.csv`, `large_inventory.csv`, `large_order_items.csv`

#### 2. Create Database Tables
```bash
python generate_test_tables.py --engine sqlserver --schema dbo --drop-tables --output create_tables.sql
```

#### 3. Run SQL Script
```bash
sqlcmd -S localhost -d testdb -U sa -P YourPassword -i create_tables.sql
```

#### 4. Load Data
```bash
java -cp ".:mssql-jdbc-12.2.0.jre11.jar" TestDataLoader \
  sqlserver ./test_data \
  "jdbc:sqlserver://localhost:1433;databaseName=testdb;encrypt=false" \
  sa YourPassword dbo
```

## Integration with AWS Glue Parameters

After generating your test tables, update your CloudFormation parameters:

```json
{
  "ParameterKey": "SourceSchema",
  "ParameterValue": "HR"
},
{
  "ParameterKey": "TableNames", 
  "ParameterValue": "customers,orders,products,inventory,order_items"
}
```

## Troubleshooting

### Missing DataGenerator / DataExporter

If you see the warning:
```
Warning: Could not import TestDataGenerator. Data generation will not be available.
```

**Solutions:**

1. **Run from project root**:
   ```bash
   cd /path/to/glue-datareplication
   python tests/generate_test_tables.py --engine oracle --schema HR --with-data
   ```

2. **Install required packages**:
   ```bash
   pip install faker pandas
   ```

3. **Check file exists**:
   ```bash
   ls tests/test_data_setup.py
   ```

### Data Loader Issues

#### Error: "No suitable driver found"
- Ensure JDBC driver JAR is in classpath
- Check driver file name matches the `-cp` argument

#### Error: "Table does not exist"
- Create tables first using `generate_test_tables.py`
- Verify schema name is correct

#### Error: "Foreign key constraint violation"
- Ensure parent tables are loaded first
- Check CSV file naming (loader sorts by table dependencies)

#### Error: "Data type mismatch"
- Verify CSV data format matches table schema
- Check date/timestamp formats
- Ensure boolean values are `1`/`0` for Oracle/DB2

#### Date/Time conversion errors
- Regenerate CSV files with correct datetime format
- Check timestamp format matches database expectations

#### Connection timeout
- Increase connection timeout in JDBC URL
- Check network connectivity to database

#### Out of memory
- Reduce batch size or increase JVM heap size with `-Xmx4g`

#### Deadlocks (multi-threaded loader)
- Reduce number of worker threads

### Permission Issues

Ensure your database user has CREATE TABLE permissions in the target schema.

### Large Dataset Performance

For large datasets, consider:
- Running during off-peak hours
- Using batch INSERT statements
- Monitoring database resources
- Increasing `BATCH_SIZE` (try 5000 or 10000)
- Disabling database indexes during load, rebuild after
- Using faster storage (SSD)
- Increasing database memory allocation
