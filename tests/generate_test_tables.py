#!/usr/bin/env python3
"""
Test Table Generator for AWS Glue Data Replication

This script generates CREATE TABLE statements and sample data for setting up
test tables in source databases. Supports Oracle, SQL Server, PostgreSQL, and DB2.

Usage:
    python generate_test_tables.py --engine oracle --schema HR --output tables.sql
    python generate_test_tables.py --engine postgresql --schema public --with-data
    python generate_test_tables.py --engine sqlserver --schema dbo --data-size medium
"""

import argparse
import sys
import os
from datetime import datetime, date, time
from typing import Dict, List, Any, Optional
from dataclasses import dataclass

# Add current directory to path for imports
sys.path.insert(0, os.path.dirname(__file__))

try:
    from test_data_setup import TestDataGenerator
except ImportError as e:
    print(f"Warning: Could not import TestDataGenerator ({e}). Data generation will be limited.")
    TestDataGenerator = None


@dataclass
class TableColumn:
    """Represents a database table column."""
    name: str
    data_type: str
    nullable: bool = True
    primary_key: bool = False
    default_value: Optional[str] = None


@dataclass
class TableSchema:
    """Represents a database table schema."""
    name: str
    columns: List[TableColumn]
    primary_key: Optional[str] = None
    incremental_column: Optional[str] = None


class DatabaseDDLGenerator:
    """Generates DDL statements for different database engines."""
    
    def __init__(self, engine: str, schema: str = None):
        """Initialize the DDL generator for a specific database engine."""
        self.engine = engine.lower()
        self.schema = schema
        
        # Database-specific type mappings
        self.type_mappings = {
            'oracle': {
                'INTEGER': 'NUMBER(10)',
                'BIGINT': 'NUMBER(19)',
                'VARCHAR(50)': 'VARCHAR2(50)',
                'VARCHAR(100)': 'VARCHAR2(100)',
                'VARCHAR(200)': 'VARCHAR2(200)',
                'VARCHAR(20)': 'VARCHAR2(20)',
                'VARCHAR(10)': 'VARCHAR2(10)',
                'TIMESTAMP': 'TIMESTAMP',
                'DATE': 'DATE',
                'TIME': 'TIMESTAMP',
                'DECIMAL(10,2)': 'NUMBER(10,2)',
                'DECIMAL(8,2)': 'NUMBER(8,2)',
                'DECIMAL(5,2)': 'NUMBER(5,2)',
                'DECIMAL(5,4)': 'NUMBER(5,4)',
                'TEXT': 'CLOB',
                'BOOLEAN': 'NUMBER(1)'
            },
            'sqlserver': {
                'INTEGER': 'INT',
                'BIGINT': 'BIGINT',
                'VARCHAR(50)': 'NVARCHAR(50)',
                'VARCHAR(100)': 'NVARCHAR(100)',
                'VARCHAR(200)': 'NVARCHAR(200)',
                'VARCHAR(20)': 'NVARCHAR(20)',
                'VARCHAR(10)': 'NVARCHAR(10)',
                'TIMESTAMP': 'DATETIME2',
                'DATE': 'DATE',
                'TIME': 'TIME',
                'DECIMAL(10,2)': 'DECIMAL(10,2)',
                'DECIMAL(8,2)': 'DECIMAL(8,2)',
                'DECIMAL(5,2)': 'DECIMAL(5,2)',
                'DECIMAL(5,4)': 'DECIMAL(5,4)',
                'TEXT': 'NVARCHAR(MAX)',
                'BOOLEAN': 'BIT'
            },
            'postgresql': {
                'INTEGER': 'INTEGER',
                'BIGINT': 'BIGINT',
                'VARCHAR(50)': 'VARCHAR(50)',
                'VARCHAR(100)': 'VARCHAR(100)',
                'VARCHAR(200)': 'VARCHAR(200)',
                'VARCHAR(20)': 'VARCHAR(20)',
                'VARCHAR(10)': 'VARCHAR(10)',
                'TIMESTAMP': 'TIMESTAMP',
                'DATE': 'DATE',
                'TIME': 'TIME',
                'DECIMAL(10,2)': 'DECIMAL(10,2)',
                'DECIMAL(8,2)': 'DECIMAL(8,2)',
                'DECIMAL(5,2)': 'DECIMAL(5,2)',
                'DECIMAL(5,4)': 'DECIMAL(5,4)',
                'TEXT': 'TEXT',
                'BOOLEAN': 'BOOLEAN'
            },
            'db2': {
                'INTEGER': 'INTEGER',
                'BIGINT': 'BIGINT',
                'VARCHAR(50)': 'VARCHAR(50)',
                'VARCHAR(100)': 'VARCHAR(100)',
                'VARCHAR(200)': 'VARCHAR(200)',
                'VARCHAR(20)': 'VARCHAR(20)',
                'VARCHAR(10)': 'VARCHAR(10)',
                'TIMESTAMP': 'TIMESTAMP',
                'DATE': 'DATE',
                'TIME': 'TIME',
                'DECIMAL(10,2)': 'DECIMAL(10,2)',
                'DECIMAL(8,2)': 'DECIMAL(8,2)',
                'DECIMAL(5,2)': 'DECIMAL(5,2)',
                'DECIMAL(5,4)': 'DECIMAL(5,4)',
                'TEXT': 'CLOB',
                'BOOLEAN': 'SMALLINT'
            }
        }
    
    def get_table_schemas(self) -> Dict[str, TableSchema]:
        """Get predefined test table schemas."""
        return {
            'customers': TableSchema(
                name='customers',
                columns=[
                    TableColumn('customer_id', 'INTEGER', False, True),
                    TableColumn('first_name', 'VARCHAR(50)', False),
                    TableColumn('last_name', 'VARCHAR(50)', False),
                    TableColumn('email', 'VARCHAR(100)', True),
                    TableColumn('phone', 'VARCHAR(20)', True),
                    TableColumn('address_line1', 'VARCHAR(200)', True),
                    TableColumn('address_line2', 'VARCHAR(200)', True),
                    TableColumn('city', 'VARCHAR(50)', True),
                    TableColumn('state', 'VARCHAR(20)', True),
                    TableColumn('postal_code', 'VARCHAR(20)', True),
                    TableColumn('country', 'VARCHAR(10)', True),
                    TableColumn('date_of_birth', 'DATE', True),
                    TableColumn('customer_segment', 'VARCHAR(20)', True),
                    TableColumn('registration_source', 'VARCHAR(20)', True),
                    TableColumn('is_active', 'BOOLEAN', True),
                    TableColumn('credit_limit', 'DECIMAL(10,2)', True),
                    TableColumn('created_at', 'TIMESTAMP', False),
                    TableColumn('updated_at', 'TIMESTAMP', False),
                    TableColumn('last_login_at', 'TIMESTAMP', True)
                ],
                primary_key='customer_id',
                incremental_column='updated_at'
            ),
            'orders': TableSchema(
                name='orders',
                columns=[
                    TableColumn('order_id', 'INTEGER', False, True),
                    TableColumn('customer_id', 'INTEGER', False),
                    TableColumn('order_number', 'VARCHAR(20)', False),
                    TableColumn('order_date', 'DATE', False),
                    TableColumn('order_time', 'TIME', True),
                    TableColumn('total_amount', 'DECIMAL(10,2)', False),
                    TableColumn('tax_amount', 'DECIMAL(10,2)', True),
                    TableColumn('shipping_amount', 'DECIMAL(8,2)', True),
                    TableColumn('discount_amount', 'DECIMAL(8,2)', True),
                    TableColumn('currency', 'VARCHAR(10)', True),
                    TableColumn('status', 'VARCHAR(20)', False),
                    TableColumn('payment_method', 'VARCHAR(20)', True),
                    TableColumn('shipping_method', 'VARCHAR(20)', True),
                    TableColumn('warehouse_code', 'VARCHAR(10)', True),
                    TableColumn('notes', 'TEXT', True),
                    TableColumn('created_at', 'TIMESTAMP', False),
                    TableColumn('last_modified', 'TIMESTAMP', False),
                    TableColumn('shipped_at', 'TIMESTAMP', True)
                ],
                primary_key='order_id',
                incremental_column='last_modified'
            ),
            'products': TableSchema(
                name='products',
                columns=[
                    TableColumn('product_id', 'INTEGER', False, True),
                    TableColumn('sku', 'VARCHAR(50)', False),
                    TableColumn('product_name', 'VARCHAR(100)', False),
                    TableColumn('category', 'VARCHAR(50)', True),
                    TableColumn('subcategory', 'VARCHAR(50)', True),
                    TableColumn('brand', 'VARCHAR(50)', True),
                    TableColumn('price', 'DECIMAL(8,2)', False),
                    TableColumn('cost', 'DECIMAL(8,2)', True),
                    TableColumn('weight_kg', 'DECIMAL(8,2)', True),
                    TableColumn('dimensions_cm', 'VARCHAR(50)', True),
                    TableColumn('color', 'VARCHAR(20)', True),
                    TableColumn('product_size', 'VARCHAR(10)', True),
                    TableColumn('description', 'TEXT', True),
                    TableColumn('features', 'TEXT', True),
                    TableColumn('is_active', 'BOOLEAN', True),
                    TableColumn('is_featured', 'BOOLEAN', True),
                    TableColumn('minimum_stock_level', 'INTEGER', True),
                    TableColumn('supplier_id', 'INTEGER', True),
                    TableColumn('warranty_months', 'INTEGER', True),
                    TableColumn('created_date', 'DATE', False),
                    TableColumn('last_updated', 'DATE', True)
                ],
                primary_key='product_id',
                incremental_column='last_updated'
            ),
            'inventory': TableSchema(
                name='inventory',
                columns=[
                    TableColumn('inventory_id', 'VARCHAR(50)', False, True),
                    TableColumn('product_id', 'INTEGER', False),
                    TableColumn('warehouse_code', 'VARCHAR(10)', False),
                    TableColumn('quantity_on_hand', 'INTEGER', False),
                    TableColumn('quantity_reserved', 'INTEGER', True),
                    TableColumn('quantity_available', 'INTEGER', True),
                    TableColumn('reorder_point', 'INTEGER', True),
                    TableColumn('max_stock_level', 'INTEGER', True),
                    TableColumn('unit_cost', 'DECIMAL(8,2)', True),
                    TableColumn('last_count_date', 'DATE', True),
                    TableColumn('last_received_date', 'DATE', True),
                    TableColumn('last_shipped_date', 'DATE', True),
                    TableColumn('location_code', 'VARCHAR(20)', True),
                    TableColumn('batch_number', 'VARCHAR(20)', True),
                    TableColumn('expiry_date', 'DATE', True)
                ],
                primary_key='inventory_id',
                incremental_column='last_count_date'
            ),
            'order_items': TableSchema(
                name='order_items',
                columns=[
                    TableColumn('order_item_id', 'INTEGER', False, True),
                    TableColumn('order_id', 'INTEGER', False),
                    TableColumn('product_id', 'INTEGER', False),
                    TableColumn('line_number', 'INTEGER', False),
                    TableColumn('quantity', 'INTEGER', False),
                    TableColumn('unit_price', 'DECIMAL(8,2)', False),
                    TableColumn('line_total', 'DECIMAL(10,2)', False),
                    TableColumn('discount_percentage', 'DECIMAL(5,2)', True),
                    TableColumn('discount_amount', 'DECIMAL(8,2)', True),
                    TableColumn('tax_rate', 'DECIMAL(5,4)', True),
                    TableColumn('tax_amount', 'DECIMAL(8,2)', True),
                    TableColumn('warehouse_code', 'VARCHAR(10)', True),
                    TableColumn('shipped_quantity', 'INTEGER', True),
                    TableColumn('backorder_quantity', 'INTEGER', True),
                    TableColumn('unit_cost', 'DECIMAL(8,2)', True),
                    TableColumn('created_at', 'TIMESTAMP', False),
                    TableColumn('updated_at', 'TIMESTAMP', True)
                ],
                primary_key='order_item_id',
                incremental_column='updated_at'
            )
        }
    
    def generate_create_table_ddl(self, table_schema: TableSchema) -> str:
        """Generate CREATE TABLE DDL for the specified engine."""
        type_map = self.type_mappings.get(self.engine, self.type_mappings['postgresql'])
        
        # Build table name with schema if provided
        full_table_name = f"{self.schema}.{table_schema.name}" if self.schema else table_schema.name
        
        # Generate column definitions
        column_defs = []
        for col in table_schema.columns:
            col_type = type_map.get(col.data_type, col.data_type)
            
            # Build column definition
            col_def = f"    {col.name} {col_type}"
            
            # Add inline check constraint for boolean columns in Oracle
            if self.engine == 'oracle' and col.data_type == 'BOOLEAN':
                col_def += " CHECK ({} IN (0,1))".format(col.name)
            
            # Add NOT NULL constraint
            if not col.nullable:
                col_def += " NOT NULL"
            
            # Add default value if specified
            if col.default_value:
                col_def += f" DEFAULT {col.default_value}"
            
            column_defs.append(col_def)
        
        # Add check constraints for boolean columns in DB2 (Oracle done inline)
        if self.engine == 'db2':
            for col in table_schema.columns:
                if col.data_type == 'BOOLEAN':
                    check_constraint = f"    CONSTRAINT chk_{table_schema.name}_{col.name} CHECK ({col.name} IN (0,1))"
                    column_defs.append(check_constraint)
        
        # Add primary key constraint
        if table_schema.primary_key:
            pk_constraint = f"    CONSTRAINT pk_{table_schema.name} PRIMARY KEY ({table_schema.primary_key})"
            column_defs.append(pk_constraint)
        
        # Build CREATE TABLE statement
        ddl = f"CREATE TABLE {full_table_name} (\n"
        ddl += ",\n".join(column_defs)
        ddl += "\n);"
        
        return ddl
    
    def generate_drop_table_ddl(self, table_name: str) -> str:
        """Generate DROP TABLE DDL."""
        full_table_name = f"{self.schema}.{table_name}" if self.schema else table_name
        
        if self.engine == 'oracle':
            return f"DROP TABLE {full_table_name} CASCADE CONSTRAINTS;"
        elif self.engine == 'sqlserver':
            return f"DROP TABLE IF EXISTS {full_table_name};"
        else:  # PostgreSQL, DB2
            return f"DROP TABLE IF EXISTS {full_table_name} CASCADE;"
    
    def generate_insert_statements(self, table_name: str, data: List[Dict[str, Any]]) -> List[str]:
        """Generate INSERT statements for sample data."""
        if not data:
            return []
        
        full_table_name = f"{self.schema}.{table_name}" if self.schema else table_name
        insert_statements = []
        
        for record in data:
            columns = list(record.keys())
            values = []
            
            for value in record.values():
                if value is None:
                    values.append('NULL')
                elif isinstance(value, str):
                    # Escape single quotes
                    escaped_value = value.replace("'", "''")
                    values.append(f"'{escaped_value}'")
                elif isinstance(value, (datetime, date)):
                    if self.engine == 'oracle':
                        if isinstance(value, datetime):
                            values.append(f"TO_TIMESTAMP('{value.strftime('%Y-%m-%d %H:%M:%S')}', 'YYYY-MM-DD HH24:MI:SS')")
                        else:
                            values.append(f"TO_DATE('{value.strftime('%Y-%m-%d')}', 'YYYY-MM-DD')")
                    elif self.engine == 'sqlserver':
                        values.append(f"'{value.isoformat()}'")
                    else:  # PostgreSQL, DB2
                        values.append(f"'{value.isoformat()}'")
                elif isinstance(value, time):
                    # Handle time objects separately
                    if self.engine == 'oracle':
                        # Convert time to timestamp for Oracle
                        time_str = value.strftime('%H:%M:%S')
                        values.append(f"TO_TIMESTAMP('1970-01-01 {time_str}', 'YYYY-MM-DD HH24:MI:SS')")
                    else:
                        values.append(f"'{value.isoformat()}'")
                elif isinstance(value, bool):
                    if self.engine in ['oracle', 'db2']:
                        values.append('1' if value else '0')
                    elif self.engine == 'sqlserver':
                        values.append('1' if value else '0')
                    else:  # PostgreSQL
                        values.append('TRUE' if value else 'FALSE')
                else:
                    values.append(str(value))
            
            insert_sql = f"INSERT INTO {full_table_name} ({', '.join(columns)}) VALUES ({', '.join(values)});"
            insert_statements.append(insert_sql)
        
        return insert_statements


def main():
    """Main function for generating test tables."""
    parser = argparse.ArgumentParser(
        description='Generate test tables for AWS Glue Data Replication testing',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Generate Oracle DDL only
  python generate_test_tables.py --engine oracle --schema HR

  # Generate PostgreSQL DDL with sample data
  python generate_test_tables.py --engine postgresql --schema public --with-data

  # Generate SQL Server DDL with medium dataset
  python generate_test_tables.py --engine sqlserver --schema dbo --with-data --data-size medium

  # Save to file
  python generate_test_tables.py --engine oracle --schema HR --output oracle_tables.sql
        """
    )
    
    parser.add_argument('--engine', 
                       choices=['oracle', 'sqlserver', 'postgresql', 'db2'],
                       required=True,
                       help='Target database engine')
    
    parser.add_argument('--schema',
                       help='Database schema name (e.g., HR, public, dbo)')
    
    parser.add_argument('--with-data',
                       action='store_true',
                       help='Include sample data INSERT statements')
    
    parser.add_argument('--data-size',
                       choices=['small', 'medium', 'large'],
                       default='small',
                       help='Size of sample dataset (default: small)')
    
    parser.add_argument('--tables',
                       nargs='+',
                       help='Specific tables to generate (default: all)')
    
    parser.add_argument('--output', '-o',
                       help='Output file path (default: stdout)')
    
    parser.add_argument('--drop-tables',
                       action='store_true',
                       help='Include DROP TABLE statements')
    
    parser.add_argument('--transaction-blocks',
                       action='store_true',
                       help='Use explicit BEGIN/END transaction blocks (Oracle PL/SQL)')
    
    args = parser.parse_args()
    
    # Initialize DDL generator
    generator = DatabaseDDLGenerator(args.engine, args.schema)
    
    # Get table schemas
    all_schemas = generator.get_table_schemas()
    
    # Filter tables if specified
    if args.tables:
        schemas_to_generate = {name: schema for name, schema in all_schemas.items() 
                             if name in args.tables}
    else:
        schemas_to_generate = all_schemas
    
    # Generate output
    output_lines = []
    
    # Add header comment
    output_lines.append(f"-- Test Tables for {args.engine.upper()}")
    output_lines.append(f"-- Generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    if args.schema:
        output_lines.append(f"-- Schema: {args.schema}")
    output_lines.append("")
    
    # Generate DROP statements if requested
    if args.drop_tables:
        output_lines.append("-- Drop existing tables")
        for table_name in reversed(list(schemas_to_generate.keys())):  # Reverse order for dependencies
            drop_ddl = generator.generate_drop_table_ddl(table_name)
            output_lines.append(drop_ddl)
        output_lines.append("")
    
    # Generate CREATE TABLE statements
    output_lines.append("-- Create tables")
    for table_name, schema in schemas_to_generate.items():
        output_lines.append(f"-- Table: {table_name}")
        create_ddl = generator.generate_create_table_ddl(schema)
        output_lines.append(create_ddl)
        output_lines.append("")
    
    # Generate sample data if requested
    if args.with_data and TestDataGenerator:
        output_lines.append("-- Sample data")
        
        # Determine data size
        data_sizes = {
            'small': {'customers': 100, 'orders': 500, 'products': 200},
            'medium': {'customers': 1000, 'orders': 5000, 'products': 1000},
            'large': {'customers': 10000, 'orders': 50000, 'products': 5000}
        }
        
        counts = data_sizes[args.data_size]
        
        # Generate test data
        data_generator = TestDataGenerator(seed=42)
        
        # Generate data for each table
        sample_data = {}
        if 'customers' in schemas_to_generate:
            sample_data['customers'] = data_generator.generate_customers_data(counts['customers'])
        
        if 'products' in schemas_to_generate:
            sample_data['products'] = data_generator.generate_products_data(counts['products'])
        
        if 'orders' in schemas_to_generate:
            customer_count = counts['customers'] if 'customers' in schemas_to_generate else 100
            sample_data['orders'] = data_generator.generate_orders_data(counts['orders'], customer_count)
        
        if 'inventory' in schemas_to_generate:
            product_count = counts['products'] if 'products' in schemas_to_generate else 200
            sample_data['inventory'] = data_generator.generate_inventory_data(product_count)
        
        if 'order_items' in schemas_to_generate:
            order_count = counts['orders'] if 'orders' in schemas_to_generate else 500
            product_count = counts['products'] if 'products' in schemas_to_generate else 200
            sample_data['order_items'] = data_generator.generate_order_items_data(order_count, product_count)
        
        # Add transaction management for Oracle
        if args.engine == 'oracle':
            if args.transaction_blocks:
                output_lines.append("-- Begin explicit transaction block")
                output_lines.append("BEGIN")
                output_lines.append("")
            else:
                output_lines.append("-- Begin transaction (implicit - Oracle starts transaction on first DML)")
                output_lines.append("")
        
        # Generate INSERT statements with batch commits for Oracle
        batch_size = 100 if args.engine == 'oracle' else 0  # Commit every 100 inserts for Oracle
        insert_count = 0
        
        for table_name in schemas_to_generate.keys():
            if table_name in sample_data:
                output_lines.append(f"-- Data for {table_name}")
                insert_statements = generator.generate_insert_statements(table_name, sample_data[table_name])
                
                # Add batch commits for Oracle if dataset is large
                if args.engine == 'oracle' and len(insert_statements) > batch_size:
                    for i, stmt in enumerate(insert_statements):
                        output_lines.append(stmt)
                        insert_count += 1
                        
                        # Add intermediate commit every batch_size inserts
                        if insert_count % batch_size == 0:
                            if args.transaction_blocks:
                                output_lines.append("    COMMIT; -- Batch commit")
                            else:
                                output_lines.append("COMMIT; -- Batch commit")
                            output_lines.append("")
                else:
                    output_lines.extend(insert_statements)
                    insert_count += len(insert_statements)
                
                output_lines.append("")
        
        # Add final commit for Oracle
        if args.engine == 'oracle':
            if args.transaction_blocks:
                output_lines.append("    -- Final commit")
                output_lines.append("    COMMIT;")
                output_lines.append("END;")
                output_lines.append("/")
                output_lines.append("")
            else:
                output_lines.append("-- Final commit")
                output_lines.append("COMMIT;")
                output_lines.append("")
    
    # Output results
    output_content = "\n".join(output_lines)
    
    if args.output:
        with open(args.output, 'w', encoding='utf-8') as f:
            f.write(output_content)
        print(f"Generated DDL saved to: {args.output}")
        
        # Print summary
        table_count = len(schemas_to_generate)
        print(f"Tables generated: {table_count}")
        if args.with_data:
            total_records = sum(len(data) for data in sample_data.values()) if 'sample_data' in locals() else 0
            print(f"Sample records: {total_records}")
    else:
        print(output_content)


if __name__ == '__main__':
    main()