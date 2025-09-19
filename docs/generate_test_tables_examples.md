# Test Table Generation Examples

This guide shows how to generate test tables for your source database using the `generate_test_tables.py` script.

## Quick Start

### 1. Generate DDL Only (No Data)

```bash
# Oracle
python generate_test_tables.py --engine oracle --schema HR

# SQL Server  
python generate_test_tables.py --engine sqlserver --schema dbo

# PostgreSQL
python generate_test_tables.py --engine postgresql --schema public

# DB2
python generate_test_tables.py --engine db2 --schema TESTDB
```

### 2. Generate DDL with Sample Data

```bash
# Small dataset (100 customers, 500 orders, 200 products)
python generate_test_tables.py --engine oracle --schema HR --with-data

# Medium dataset (1K customers, 5K orders, 1K products)
python generate_test_tables.py --engine postgresql --schema public --with-data --data-size medium

# Large dataset (10K customers, 50K orders, 5K products)
python generate_test_tables.py --engine sqlserver --schema dbo --with-data --data-size large
```

### 3. Save to File

```bash
# Generate Oracle tables and save to file
python generate_test_tables.py --engine oracle --schema HR --with-data --output oracle_test_tables.sql

# Generate PostgreSQL tables with DROP statements
python generate_test_tables.py --engine postgresql --schema public --with-data --drop-tables --output postgres_test_tables.sql
```

### 4. Generate Specific Tables Only

```bash
# Only customers and orders tables
python generate_test_tables.py --engine oracle --schema HR --tables customers orders --with-data

# Only products table
python generate_test_tables.py --engine sqlserver --schema dbo --tables products --with-data
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
python generate_test_tables.py \
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
python generate_test_tables.py \
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
python generate_test_tables.py \
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

### Missing TestDataGenerator
If you see a warning about TestDataGenerator, make sure you're running from the project root:
```bash
cd /path/to/aws-glue-data-replication
python generate_test_tables.py --engine oracle --schema HR
```

### Permission Issues
Ensure your database user has CREATE TABLE permissions in the target schema.

### Large Dataset Performance
For large datasets, consider:
- Running during off-peak hours
- Using batch INSERT statements
- Monitoring database resources