# JDBC Data Type to Bookmark Strategy Mapping Reference

This document provides a comprehensive reference for how JDBC data types are mapped to bookmark strategies in the AWS Glue Data Replication solution's manual bookmark configuration feature.

## Table of Contents

1. [Overview](#overview)
2. [Bookmark Strategies](#bookmark-strategies)
3. [JDBC Data Type Mapping](#jdbc-data-type-mapping)
4. [Database-Specific Mappings](#database-specific-mappings)
5. [Strategy Selection Guidelines](#strategy-selection-guidelines)
6. [Performance Considerations](#performance-considerations)
7. [Examples by Use Case](#examples-by-use-case)

## Overview

When using manual bookmark configuration, the system queries JDBC metadata to determine the data type of the specified column and automatically selects the most appropriate bookmark strategy. This ensures optimal incremental loading performance while maintaining data consistency.

### Mapping Process

```
Manual Configuration → JDBC Metadata Query → Data Type Detection → Strategy Selection → Bookmark Creation
```

The mapping process follows these steps:
1. Parse manual bookmark configuration JSON
2. Query JDBC metadata for specified column
3. Retrieve column data type information
4. Map JDBC data type to bookmark strategy
5. Create bookmark state with selected strategy

## Bookmark Strategies

### Timestamp Strategy
- **Use Case**: Tables with reliable timestamp columns that are updated when records change
- **Performance**: Excellent for incremental loading with time-based filtering
- **Requirements**: Column must contain timestamp/date values that increase with changes
- **SQL Pattern**: `WHERE timestamp_column > ?`

### Primary Key Strategy  
- **Use Case**: Append-only tables with sequential primary keys or auto-incrementing IDs
- **Performance**: Excellent for tables where new records have higher ID values
- **Requirements**: Column must contain sequential numeric values
- **SQL Pattern**: `WHERE id_column > ?`

### Hash Strategy
- **Use Case**: Tables without suitable timestamp or sequential ID columns
- **Performance**: Requires full table scan to detect changes
- **Requirements**: Any column that can be used to generate a hash of the entire table
- **SQL Pattern**: Full table scan with hash comparison

## JDBC Data Type Mapping

### Complete Mapping Table

| JDBC Data Type | Java Type Constant | Bookmark Strategy | Rationale |
|----------------|-------------------|-------------------|-----------|
| `TIMESTAMP` | `Types.TIMESTAMP` | `timestamp` | Ideal for change tracking |
| `TIMESTAMP_WITH_TIMEZONE` | `Types.TIMESTAMP_WITH_TIMEZONE` | `timestamp` | Timezone-aware timestamps |
| `DATE` | `Types.DATE` | `timestamp` | Date-based incremental loading |
| `DATETIME` | `Types.TIMESTAMP` | `timestamp` | MySQL/SQL Server datetime |
| `TIME` | `Types.TIME` | `timestamp` | Time-based incremental loading |
| `INTEGER` | `Types.INTEGER` | `primary_key` | Sequential integer IDs |
| `BIGINT` | `Types.BIGINT` | `primary_key` | Large sequential IDs |
| `SMALLINT` | `Types.SMALLINT` | `primary_key` | Small sequential IDs |
| `TINYINT` | `Types.TINYINT` | `primary_key` | Tiny sequential IDs |
| `SERIAL` | `Types.INTEGER` | `primary_key` | PostgreSQL auto-increment |
| `BIGSERIAL` | `Types.BIGINT` | `primary_key` | PostgreSQL big auto-increment |
| `VARCHAR` | `Types.VARCHAR` | `hash` | Variable-length strings |
| `CHAR` | `Types.CHAR` | `hash` | Fixed-length strings |
| `TEXT` | `Types.LONGVARCHAR` | `hash` | Large text fields |
| `CLOB` | `Types.CLOB` | `hash` | Character large objects |
| `DECIMAL` | `Types.DECIMAL` | `hash` | Decimal numbers |
| `NUMERIC` | `Types.NUMERIC` | `hash` | Numeric values |
| `FLOAT` | `Types.FLOAT` | `hash` | Floating-point numbers |
| `DOUBLE` | `Types.DOUBLE` | `hash` | Double-precision numbers |
| `BOOLEAN` | `Types.BOOLEAN` | `hash` | Boolean values |
| `BIT` | `Types.BIT` | `hash` | Bit values |
| `BINARY` | `Types.BINARY` | `hash` | Binary data |
| `VARBINARY` | `Types.VARBINARY` | `hash` | Variable binary data |
| `BLOB` | `Types.BLOB` | `hash` | Binary large objects |

### Strategy Distribution

```
Timestamp Strategy (7 types):
├── TIMESTAMP
├── TIMESTAMP_WITH_TIMEZONE  
├── DATE
├── DATETIME
├── TIME
└── (Database-specific timestamp variants)

Primary Key Strategy (6 types):
├── INTEGER
├── BIGINT
├── SMALLINT
├── TINYINT
├── SERIAL
└── BIGSERIAL

Hash Strategy (12+ types):
├── VARCHAR
├── CHAR
├── TEXT
├── CLOB
├── DECIMAL
├── NUMERIC
├── FLOAT
├── DOUBLE
├── BOOLEAN
├── BIT
├── BINARY
├── VARBINARY
├── BLOB
└── (Other types)
```

## Database-Specific Mappings

### PostgreSQL

| PostgreSQL Type | JDBC Type | Bookmark Strategy | Notes |
|-----------------|-----------|-------------------|-------|
| `TIMESTAMP` | `TIMESTAMP` | `timestamp` | Standard timestamp |
| `TIMESTAMPTZ` | `TIMESTAMP_WITH_TIMEZONE` | `timestamp` | Timezone-aware |
| `DATE` | `DATE` | `timestamp` | Date only |
| `TIME` | `TIME` | `timestamp` | Time only |
| `SERIAL` | `INTEGER` | `primary_key` | Auto-incrementing 4-byte |
| `BIGSERIAL` | `BIGINT` | `primary_key` | Auto-incrementing 8-byte |
| `SMALLSERIAL` | `SMALLINT` | `primary_key` | Auto-incrementing 2-byte |
| `INTEGER` | `INTEGER` | `primary_key` | 4-byte integer |
| `BIGINT` | `BIGINT` | `primary_key` | 8-byte integer |
| `VARCHAR(n)` | `VARCHAR` | `hash` | Variable-length string |
| `TEXT` | `LONGVARCHAR` | `hash` | Unlimited text |
| `BOOLEAN` | `BOOLEAN` | `hash` | True/false values |
| `DECIMAL` | `DECIMAL` | `hash` | Exact decimal |
| `NUMERIC` | `NUMERIC` | `hash` | Exact numeric |

**PostgreSQL Example:**
```json
{
  "user_activity": "created_at",  // TIMESTAMPTZ → timestamp strategy
  "user_accounts": "user_id",     // SERIAL → primary_key strategy
  "user_profiles": "username"     // VARCHAR → hash strategy
}
```

### Oracle

| Oracle Type | JDBC Type | Bookmark Strategy | Notes |
|-------------|-----------|-------------------|-------|
| `DATE` | `TIMESTAMP` | `timestamp` | Oracle DATE includes time |
| `TIMESTAMP` | `TIMESTAMP` | `timestamp` | Fractional seconds |
| `TIMESTAMP WITH TIME ZONE` | `TIMESTAMP_WITH_TIMEZONE` | `timestamp` | Timezone-aware |
| `TIMESTAMP WITH LOCAL TIME ZONE` | `TIMESTAMP_WITH_TIMEZONE` | `timestamp` | Local timezone |
| `NUMBER(p,0)` | `INTEGER` or `BIGINT` | `primary_key` | Integer when scale=0 |
| `NUMBER(p,s)` | `DECIMAL` | `hash` | Decimal when scale>0 |
| `INTEGER` | `INTEGER` | `primary_key` | Integer type |
| `VARCHAR2(n)` | `VARCHAR` | `hash` | Variable-length string |
| `CHAR(n)` | `CHAR` | `hash` | Fixed-length string |
| `CLOB` | `CLOB` | `hash` | Character large object |
| `BLOB` | `BLOB` | `hash` | Binary large object |

**Oracle Example:**
```json
{
  "employees": "last_modified",   // DATE → timestamp strategy
  "departments": "department_id", // NUMBER(10,0) → primary_key strategy
  "job_history": "job_title"      // VARCHAR2 → hash strategy
}
```

### SQL Server

| SQL Server Type | JDBC Type | Bookmark Strategy | Notes |
|-----------------|-----------|-------------------|-------|
| `DATETIME` | `TIMESTAMP` | `timestamp` | Legacy datetime |
| `DATETIME2` | `TIMESTAMP` | `timestamp` | Enhanced datetime |
| `SMALLDATETIME` | `TIMESTAMP` | `timestamp` | Small datetime |
| `DATE` | `DATE` | `timestamp` | Date only |
| `TIME` | `TIME` | `timestamp` | Time only |
| `DATETIMEOFFSET` | `TIMESTAMP_WITH_TIMEZONE` | `timestamp` | Timezone offset |
| `INT` | `INTEGER` | `primary_key` | 4-byte integer |
| `BIGINT` | `BIGINT` | `primary_key` | 8-byte integer |
| `SMALLINT` | `SMALLINT` | `primary_key` | 2-byte integer |
| `TINYINT` | `TINYINT` | `primary_key` | 1-byte integer |
| `IDENTITY` | `INTEGER` or `BIGINT` | `primary_key` | Auto-incrementing |
| `NVARCHAR(n)` | `VARCHAR` | `hash` | Unicode string |
| `VARCHAR(n)` | `VARCHAR` | `hash` | ANSI string |
| `NCHAR(n)` | `CHAR` | `hash` | Fixed Unicode string |
| `TEXT` | `LONGVARCHAR` | `hash` | Large text |
| `BIT` | `BIT` | `hash` | Boolean/bit |
| `DECIMAL(p,s)` | `DECIMAL` | `hash` | Exact decimal |
| `FLOAT` | `FLOAT` | `hash` | Floating-point |

**SQL Server Example:**
```json
{
  "orders": "modified_date",    // DATETIME2 → timestamp strategy
  "customers": "customer_id",   // INT IDENTITY → primary_key strategy
  "products": "product_name"    // NVARCHAR → hash strategy
}
```

### MySQL

| MySQL Type | JDBC Type | Bookmark Strategy | Notes |
|------------|-----------|-------------------|-------|
| `TIMESTAMP` | `TIMESTAMP` | `timestamp` | Auto-updating timestamp |
| `DATETIME` | `TIMESTAMP` | `timestamp` | Date and time |
| `DATE` | `DATE` | `timestamp` | Date only |
| `TIME` | `TIME` | `timestamp` | Time only |
| `INT` | `INTEGER` | `primary_key` | 4-byte integer |
| `BIGINT` | `BIGINT` | `primary_key` | 8-byte integer |
| `SMALLINT` | `SMALLINT` | `primary_key` | 2-byte integer |
| `TINYINT` | `TINYINT` | `primary_key` | 1-byte integer |
| `AUTO_INCREMENT` | `INTEGER` or `BIGINT` | `primary_key` | Auto-incrementing |
| `VARCHAR(n)` | `VARCHAR` | `hash` | Variable-length string |
| `CHAR(n)` | `CHAR` | `hash` | Fixed-length string |
| `TEXT` | `LONGVARCHAR` | `hash` | Large text |
| `DECIMAL(p,s)` | `DECIMAL` | `hash` | Exact decimal |
| `FLOAT` | `FLOAT` | `hash` | Floating-point |
| `DOUBLE` | `DOUBLE` | `hash` | Double-precision |
| `BOOLEAN` | `BOOLEAN` | `hash` | Boolean values |

## Strategy Selection Guidelines

### When to Use Timestamp Strategy

**Ideal Scenarios:**
- Tables with `updated_at`, `modified_date`, or similar columns
- Audit tables with timestamp tracking
- Event tables with occurrence timestamps
- Tables where records are updated (not just inserted)

**Column Requirements:**
- Column is updated whenever the record changes
- Timestamp values are monotonically increasing
- Column is indexed for performance
- Timezone handling is consistent

**Example Tables:**
```sql
-- User activity tracking
CREATE TABLE user_sessions (
    session_id SERIAL PRIMARY KEY,
    user_id INTEGER,
    login_time TIMESTAMP,
    last_activity TIMESTAMP,  -- Good for timestamp strategy
    session_data TEXT
);

-- Order management
CREATE TABLE orders (
    order_id INTEGER PRIMARY KEY,
    customer_id INTEGER,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,     -- Good for timestamp strategy
    order_status VARCHAR(50)
);
```

### When to Use Primary Key Strategy

**Ideal Scenarios:**
- Append-only tables (no updates, only inserts)
- Tables with auto-incrementing primary keys
- Log tables with sequential IDs
- Queue tables with sequential processing

**Column Requirements:**
- Column contains sequential numeric values
- New records always have higher values than existing records
- Column is the primary key or unique identifier
- No gaps in sequence are acceptable for incremental loading

**Example Tables:**
```sql
-- Event logging
CREATE TABLE event_log (
    log_id BIGSERIAL PRIMARY KEY,  -- Good for primary_key strategy
    event_type VARCHAR(100),
    event_data JSONB,
    created_at TIMESTAMP
);

-- Message queue
CREATE TABLE message_queue (
    message_id SERIAL PRIMARY KEY,  -- Good for primary_key strategy
    queue_name VARCHAR(100),
    message_body TEXT,
    created_at TIMESTAMP
);
```

### When to Use Hash Strategy

**Ideal Scenarios:**
- Reference/lookup tables that change infrequently
- Configuration tables
- Tables without suitable timestamp or ID columns
- Small to medium-sized tables where full scan is acceptable

**Column Requirements:**
- Any column that can be used for hash generation
- Table size should be manageable for full scans
- Changes to the table are infrequent
- Data consistency is more important than performance

**Example Tables:**
```sql
-- Configuration settings
CREATE TABLE app_config (
    config_key VARCHAR(100) PRIMARY KEY,  -- Good for hash strategy
    config_value TEXT,
    description TEXT
);

-- Reference data
CREATE TABLE country_codes (
    country_code CHAR(2) PRIMARY KEY,     -- Good for hash strategy
    country_name VARCHAR(100),
    region VARCHAR(50)
);
```

## Performance Considerations

### Strategy Performance Comparison

| Strategy | Query Performance | Initialization Cost | Scalability | Best For |
|----------|------------------|-------------------|-------------|----------|
| `timestamp` | Excellent | Low | High | Large, frequently updated tables |
| `primary_key` | Excellent | Low | High | Large, append-only tables |
| `hash` | Poor | High | Low | Small, infrequently changed tables |

### Performance Optimization Tips

**Timestamp Strategy:**
```sql
-- Ensure timestamp columns are indexed
CREATE INDEX idx_orders_updated_at ON orders(updated_at);

-- Use appropriate timestamp precision
ALTER TABLE orders ALTER COLUMN updated_at TYPE TIMESTAMP(3);  -- Millisecond precision
```

**Primary Key Strategy:**
```sql
-- Ensure primary key is properly indexed (usually automatic)
CREATE TABLE events (
    event_id BIGSERIAL PRIMARY KEY,  -- Automatically indexed
    event_data JSONB
);

-- Monitor for sequence gaps
SELECT 
    event_id,
    LAG(event_id) OVER (ORDER BY event_id) as prev_id,
    event_id - LAG(event_id) OVER (ORDER BY event_id) as gap
FROM events 
WHERE event_id - LAG(event_id) OVER (ORDER BY event_id) > 1;
```

**Hash Strategy:**
```sql
-- Keep tables small for hash strategy
SELECT 
    schemaname,
    tablename,
    n_tup_ins + n_tup_upd + n_tup_del as total_changes,
    n_live_tup as current_rows
FROM pg_stat_user_tables 
WHERE tablename = 'config_table';

-- Consider partitioning for larger tables
CREATE TABLE large_config (
    config_type VARCHAR(50),
    config_key VARCHAR(100),
    config_value TEXT
) PARTITION BY LIST (config_type);
```

## Examples by Use Case

### E-commerce Platform

```json
{
  "orders": "updated_at",           // TIMESTAMP → timestamp strategy
  "order_items": "item_id",         // BIGSERIAL → primary_key strategy
  "product_categories": "category_code"  // VARCHAR → hash strategy
}
```

### Financial System

```json
{
  "transactions": "transaction_timestamp",  // TIMESTAMP → timestamp strategy
  "account_balances": "balance_id",         // BIGINT → primary_key strategy
  "currency_rates": "currency_pair"         // VARCHAR → hash strategy
}
```

### IoT Data Platform

```json
{
  "sensor_readings": "reading_timestamp",  // TIMESTAMP → timestamp strategy
  "device_events": "event_id",             // BIGSERIAL → primary_key strategy
  "device_types": "device_type_code"       // VARCHAR → hash strategy
}
```

### Healthcare System

```json
{
  "patient_visits": "visit_datetime",    // DATETIME → timestamp strategy
  "medical_records": "record_id",        // INT IDENTITY → primary_key strategy
  "diagnosis_codes": "icd_code"          // VARCHAR → hash strategy
}
```

## Validation and Testing

### JDBC Metadata Query Example

```python
def get_column_metadata(connection, table_name, column_name):
    """Query JDBC metadata for column information."""
    try:
        metadata = connection.getMetaData()
        result_set = metadata.getColumns(None, None, table_name, column_name)
        
        if result_set.next():
            return {
                'column_name': result_set.getString('COLUMN_NAME'),
                'data_type': result_set.getString('TYPE_NAME'),
                'jdbc_type': result_set.getInt('DATA_TYPE'),
                'column_size': result_set.getInt('COLUMN_SIZE'),
                'nullable': result_set.getInt('NULLABLE')
            }
        return None
    except Exception as e:
        logger.error(f"Failed to query metadata: {e}")
        return None

def map_jdbc_type_to_strategy(jdbc_type_name):
    """Map JDBC data type to bookmark strategy."""
    JDBC_TYPE_TO_STRATEGY = {
        # Timestamp types
        'TIMESTAMP': 'timestamp',
        'TIMESTAMP_WITH_TIMEZONE': 'timestamp',
        'DATE': 'timestamp',
        'DATETIME': 'timestamp',
        'TIME': 'timestamp',
        
        # Integer types
        'INTEGER': 'primary_key',
        'BIGINT': 'primary_key',
        'SMALLINT': 'primary_key',
        'TINYINT': 'primary_key',
        'SERIAL': 'primary_key',
        'BIGSERIAL': 'primary_key',
        
        # Other types default to hash
        'VARCHAR': 'hash',
        'CHAR': 'hash',
        'TEXT': 'hash',
        'CLOB': 'hash',
        'DECIMAL': 'hash',
        'NUMERIC': 'hash',
        'FLOAT': 'hash',
        'DOUBLE': 'hash',
        'BOOLEAN': 'hash',
        'BIT': 'hash',
        'BINARY': 'hash',
        'VARBINARY': 'hash',
        'BLOB': 'hash'
    }
    
    return JDBC_TYPE_TO_STRATEGY.get(jdbc_type_name.upper(), 'hash')
```

### Testing Strategy Selection

```python
def test_strategy_selection():
    """Test JDBC type to strategy mapping."""
    test_cases = [
        ('TIMESTAMP', 'timestamp'),
        ('DATE', 'timestamp'),
        ('INTEGER', 'primary_key'),
        ('BIGINT', 'primary_key'),
        ('VARCHAR', 'hash'),
        ('DECIMAL', 'hash'),
        ('UNKNOWN_TYPE', 'hash')  # Default fallback
    ]
    
    for jdbc_type, expected_strategy in test_cases:
        actual_strategy = map_jdbc_type_to_strategy(jdbc_type)
        assert actual_strategy == expected_strategy, \
            f"Expected {expected_strategy} for {jdbc_type}, got {actual_strategy}"
    
    print("All strategy mapping tests passed!")
```

## Conclusion

This reference guide provides comprehensive information about JDBC data type to bookmark strategy mapping. Key takeaways:

1. **Timestamp Strategy**: Best for tables with reliable timestamp columns
2. **Primary Key Strategy**: Ideal for append-only tables with sequential IDs
3. **Hash Strategy**: Fallback for tables without suitable timestamp or ID columns
4. **Database-Specific**: Each database has unique type mappings and considerations
5. **Performance**: Strategy selection significantly impacts incremental loading performance

For implementation details, refer to the [Manual Bookmark Configuration](MANUAL_BOOKMARK_CONFIGURATION.md).