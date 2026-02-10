#!/usr/bin/env python3
"""
Generate Test Tables Script

This script generates database-specific DDL (CREATE TABLE statements) and optionally
populates them with test data using the TestDataGenerator.

Usage:
    python generate_test_tables.py --engine oracle --schema HR
    python generate_test_tables.py --engine sqlserver --schema dbo --with-data
    python generate_test_tables.py --engine postgresql --schema public --with-data --data-size large
"""

import argparse
import sys
import os
from datetime import datetime
from typing import Dict, List, Optional

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from test_data_setup import DataGenerator, DataExporter
except ImportError:
    print("Warning: Could not import TestDataGenerator from test_data_setup. Data generation will not be available.")
    DataGenerator = None
    DataExporter = None


class TableDDLGenerator:
    """Generates database-specific DDL for test tables."""
    
    def __init__(self, engine: str, schema: str):
        self.engine = engine.lower()
        self.schema = schema
        
        # Define table schemas
        self.table_definitions = {
            'customers': {
                'columns': [
                    ('customer_id', 'INT', 'PRIMARY KEY'),
                    ('first_name', 'VARCHAR(50)', 'NOT NULL'),
                    ('last_name', 'VARCHAR(50)', 'NOT NULL'),
                    ('email', 'VARCHAR(100)', ''),
                    ('phone', 'VARCHAR(20)', ''),
                    ('address_line1', 'VARCHAR(200)', ''),
                    ('address_line2', 'VARCHAR(200)', ''),
                    ('city', 'VARCHAR(100)', ''),
                    ('state', 'VARCHAR(50)', ''),
                    ('postal_code', 'VARCHAR(20)', ''),
                    ('country', 'VARCHAR(2)', ''),
                    ('date_of_birth', 'DATE', ''),
                    ('customer_segment', 'VARCHAR(20)', ''),
                    ('registration_source', 'VARCHAR(20)', ''),
                    ('is_active', 'BOOLEAN', ''),
                    ('credit_limit', 'DECIMAL(15,2)', ''),
                    ('created_at', 'TIMESTAMP', ''),
                    ('updated_at', 'TIMESTAMP', ''),
                    ('last_login_at', 'TIMESTAMP', '')
                ]
            },
            'orders': {
                'columns': [
                    ('order_id', 'INT', 'PRIMARY KEY'),
                    ('customer_id', 'INT', 'NOT NULL'),
                    ('order_number', 'VARCHAR(20)', 'NOT NULL'),
                    ('order_date', 'DATE', 'NOT NULL'),
                    ('order_time', 'TIME', ''),
                    ('total_amount', 'DECIMAL(15,2)', 'NOT NULL'),
                    ('tax_amount', 'DECIMAL(15,2)', ''),
                    ('shipping_amount', 'DECIMAL(15,2)', ''),
                    ('discount_amount', 'DECIMAL(15,2)', ''),
                    ('currency', 'VARCHAR(3)', ''),
                    ('status', 'VARCHAR(20)', ''),
                    ('payment_method', 'VARCHAR(50)', ''),
                    ('shipping_method', 'VARCHAR(50)', ''),
                    ('warehouse_code', 'VARCHAR(10)', ''),
                    ('notes', 'TEXT', ''),
                    ('created_at', 'TIMESTAMP', ''),
                    ('last_modified', 'TIMESTAMP', ''),
                    ('shipped_at', 'TIMESTAMP', '')
                ],
                'foreign_keys': [
                    ('customer_id', 'customers', 'customer_id')
                ]
            },
            'products': {
                'columns': [
                    ('product_id', 'INT', 'PRIMARY KEY'),
                    ('sku', 'VARCHAR(50)', 'NOT NULL'),
                    ('product_name', 'VARCHAR(200)', 'NOT NULL'),
                    ('category', 'VARCHAR(50)', ''),
                    ('subcategory', 'VARCHAR(50)', ''),
                    ('brand', 'VARCHAR(100)', ''),
                    ('price', 'DECIMAL(15,2)', 'NOT NULL'),
                    ('cost', 'DECIMAL(15,2)', ''),
                    ('weight_kg', 'DECIMAL(10,2)', ''),
                    ('dimensions_cm', 'VARCHAR(50)', ''),
                    ('color', 'VARCHAR(30)', ''),
                    ('size', 'VARCHAR(10)', ''),
                    ('description', 'TEXT', ''),
                    ('features', 'TEXT', ''),
                    ('is_active', 'BOOLEAN', ''),
                    ('is_featured', 'BOOLEAN', ''),
                    ('minimum_stock_level', 'INT', ''),
                    ('supplier_id', 'INT', ''),
                    ('warranty_months', 'INT', ''),
                    ('created_date', 'DATE', ''),
                    ('last_updated', 'DATE', '')
                ]
            },
            'inventory': {
                'columns': [
                    ('inventory_id', 'VARCHAR(20)', 'PRIMARY KEY'),
                    ('product_id', 'INT', 'NOT NULL'),
                    ('warehouse_code', 'VARCHAR(10)', 'NOT NULL'),
                    ('quantity_on_hand', 'INT', 'NOT NULL'),
                    ('quantity_reserved', 'INT', ''),
                    ('quantity_available', 'INT', ''),
                    ('reorder_point', 'INT', ''),
                    ('max_stock_level', 'INT', ''),
                    ('unit_cost', 'DECIMAL(15,2)', ''),
                    ('last_count_date', 'DATE', ''),
                    ('last_received_date', 'DATE', ''),
                    ('last_shipped_date', 'DATE', ''),
                    ('location_code', 'VARCHAR(20)', ''),
                    ('batch_number', 'VARCHAR(50)', ''),
                    ('expiry_date', 'DATE', '')
                ],
                'foreign_keys': [
                    ('product_id', 'products', 'product_id')
                ]
            },
            'order_items': {
                'columns': [
                    ('order_item_id', 'INT', 'PRIMARY KEY'),
                    ('order_id', 'INT', 'NOT NULL'),
                    ('product_id', 'INT', 'NOT NULL'),
                    ('line_number', 'INT', 'NOT NULL'),
                    ('quantity', 'INT', 'NOT NULL'),
                    ('unit_price', 'DECIMAL(15,2)', 'NOT NULL'),
                    ('line_total', 'DECIMAL(15,2)', 'NOT NULL'),
                    ('discount_percentage', 'DECIMAL(5,2)', ''),
                    ('discount_amount', 'DECIMAL(15,2)', ''),
                    ('tax_rate', 'DECIMAL(5,4)', ''),
                    ('tax_amount', 'DECIMAL(15,2)', ''),
                    ('warehouse_code', 'VARCHAR(10)', ''),
                    ('shipped_quantity', 'INT', ''),
                    ('backorder_quantity', 'INT', ''),
                    ('unit_cost', 'DECIMAL(15,2)', ''),
                    ('created_at', 'TIMESTAMP', ''),
                    ('updated_at', 'TIMESTAMP', '')
                ],
                'foreign_keys': [
                    ('order_id', 'orders', 'order_id'),
                    ('product_id', 'products', 'product_id')
                ]
            }
        }
    
    def _convert_datatype(self, generic_type: str) -> str:
        """Convert generic data type to engine-specific type."""
        type_mappings = {
            'oracle': {
                'INT': 'NUMBER(10)',
                'VARCHAR': 'VARCHAR2',
                'TEXT': 'CLOB',
                'BOOLEAN': 'NUMBER(1)',
                'DECIMAL': 'NUMBER',
                'TIMESTAMP': 'TIMESTAMP',
                'DATE': 'DATE',
                'TIME': 'TIMESTAMP'
            },
            'sqlserver': {
                'INT': 'INT',
                'VARCHAR': 'NVARCHAR',
                'TEXT': 'NVARCHAR(MAX)',
                'BOOLEAN': 'BIT',
                'DECIMAL': 'DECIMAL',
                'TIMESTAMP': 'DATETIME2',
                'DATE': 'DATE',
                'TIME': 'TIME'
            },
            'postgresql': {
                'INT': 'INTEGER',
                'VARCHAR': 'VARCHAR',
                'TEXT': 'TEXT',
                'BOOLEAN': 'BOOLEAN',
                'DECIMAL': 'DECIMAL',
                'TIMESTAMP': 'TIMESTAMP',
                'DATE': 'DATE',
                'TIME': 'TIME'
            },
            'db2': {
                'INT': 'INTEGER',
                'VARCHAR': 'VARCHAR',
                'TEXT': 'CLOB',
                'BOOLEAN': 'SMALLINT',
                'DECIMAL': 'DECIMAL',
                'TIMESTAMP': 'TIMESTAMP',
                'DATE': 'DATE',
                'TIME': 'TIME'
            }
        }
        
        mappings = type_mappings.get(self.engine, type_mappings['postgresql'])
        
        # Handle types with parameters (e.g., VARCHAR(50))
        for generic, specific in mappings.items():
            if generic_type.startswith(generic):
                # Preserve parameters
                if '(' in generic_type:
                    params = generic_type[generic_type.index('('):]
                    return specific + params
                return specific
        
        return generic_type
    
    def generate_drop_statements(self, tables: Optional[List[str]] = None) -> str:
        """Generate DROP TABLE statements."""
        if tables is None:
            tables = list(self.table_definitions.keys())
        
        # Reverse order for foreign key dependencies
        tables_reversed = list(reversed(tables))
        
        drop_statements = []
        drop_statements.append(f"-- Drop existing tables")
        
        for table_name in tables_reversed:
            full_table_name = f"{self.schema}.{table_name}"
            
            if self.engine == 'oracle':
                drop_statements.append(f"DROP TABLE {full_table_name} CASCADE CONSTRAINTS;")
            elif self.engine == 'sqlserver':
                drop_statements.append(f"IF OBJECT_ID('{full_table_name}', 'U') IS NOT NULL DROP TABLE {full_table_name};")
            elif self.engine == 'postgresql':
                drop_statements.append(f"DROP TABLE IF EXISTS {full_table_name} CASCADE;")
            elif self.engine == 'db2':
                drop_statements.append(f"DROP TABLE {full_table_name};")
        
        return '\n'.join(drop_statements) + '\n'
    
    def generate_create_statements(self, tables: Optional[List[str]] = None) -> str:
        """Generate CREATE TABLE statements."""
        if tables is None:
            tables = list(self.table_definitions.keys())
        
        create_statements = []
        create_statements.append(f"-- Create tables")
        
        for table_name in tables:
            if table_name not in self.table_definitions:
                print(f"Warning: Unknown table '{table_name}', skipping...")
                continue
            
            table_def = self.table_definitions[table_name]
            full_table_name = f"{self.schema}.{table_name}"
            
            create_statements.append(f"\n-- Table: {table_name}")
            create_statements.append(f"CREATE TABLE {full_table_name} (")
            
            # Add columns
            column_defs = []
            for col_name, col_type, constraints in table_def['columns']:
                engine_type = self._convert_datatype(col_type)
                col_def = f"    {col_name} {engine_type}"
                if constraints:
                    col_def += f" {constraints}"
                column_defs.append(col_def)
            
            create_statements.append(',\n'.join(column_defs))
            create_statements.append(");")
            
            # Add foreign keys if any
            if 'foreign_keys' in table_def:
                for fk_col, ref_table, ref_col in table_def['foreign_keys']:
                    fk_name = f"fk_{table_name}_{fk_col}"
                    ref_full_table = f"{self.schema}.{ref_table}"
                    
                    if self.engine == 'sqlserver':
                        create_statements.append(
                            f"ALTER TABLE {full_table_name} ADD CONSTRAINT {fk_name} "
                            f"FOREIGN KEY ({fk_col}) REFERENCES {ref_full_table}({ref_col});"
                        )
                    else:
                        create_statements.append(
                            f"ALTER TABLE {full_table_name} ADD CONSTRAINT {fk_name} "
                            f"FOREIGN KEY ({fk_col}) REFERENCES {ref_full_table}({ref_col});"
                        )
        
        return '\n'.join(create_statements) + '\n'
    
    def generate_ddl(self, tables: Optional[List[str]] = None, drop_tables: bool = False) -> str:
        """Generate complete DDL script."""
        ddl = []
        ddl.append(f"-- Test Tables for {self.engine.upper()}")
        ddl.append(f"-- Generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        ddl.append(f"-- Schema: {self.schema}")
        ddl.append("")
        
        if drop_tables:
            ddl.append(self.generate_drop_statements(tables))
            ddl.append("")
        
        ddl.append(self.generate_create_statements(tables))
        
        return '\n'.join(ddl)


def main():
    parser = argparse.ArgumentParser(
        description='Generate test tables DDL and optionally populate with data',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Generate DDL only
  python generate_test_tables.py --engine oracle --schema HR
  
  # Generate DDL with sample data
  python generate_test_tables.py --engine sqlserver --schema dbo --with-data
  
  # Generate specific tables with large dataset
  python generate_test_tables.py --engine postgresql --schema public --tables customers orders --with-data --data-size large
  
  # Save to file
  python generate_test_tables.py --engine oracle --schema HR --with-data --output oracle_test.sql
        """
    )
    
    parser.add_argument('--engine', required=True, 
                       choices=['oracle', 'sqlserver', 'postgresql', 'db2'],
                       help='Database engine type')
    parser.add_argument('--schema', required=True,
                       help='Schema name (e.g., HR, dbo, public)')
    parser.add_argument('--tables', nargs='+',
                       help='Specific tables to generate (default: all)')
    parser.add_argument('--with-data', action='store_true',
                       help='Generate sample data INSERT statements')
    parser.add_argument('--data-size', choices=['small', 'medium', 'large'],
                       default='small',
                       help='Size of sample data (default: small)')
    parser.add_argument('--drop-tables', action='store_true',
                       help='Include DROP TABLE statements')
    parser.add_argument('--output', '-o',
                       help='Output file path (default: stdout)')
    
    args = parser.parse_args()
    
    # Generate DDL
    print(f"Generating DDL for {args.engine} (schema: {args.schema})...", file=sys.stderr)
    generator = TableDDLGenerator(args.engine, args.schema)
    ddl = generator.generate_ddl(args.tables, args.drop_tables)
    
    # Generate data if requested
    data_sql = ""
    if args.with_data:
        if DataGenerator is None:
            print("Error: TestDataGenerator not available. Cannot generate data.", file=sys.stderr)
            sys.exit(1)
        
        print(f"Generating {args.data_size} sample data...", file=sys.stderr)
        
        # Define data sizes
        data_sizes = {
            'small': {'customers': 100, 'orders': 500, 'products': 200},
            'medium': {'customers': 1000, 'orders': 5000, 'products': 1000},
            'large': {'customers': 10000, 'orders': 50000, 'products': 5000}
        }
        
        counts = data_sizes[args.data_size]
        data_gen = DataGenerator(seed=42)
        
        # Generate data
        customers = data_gen.generate_customers_data(counts['customers'])
        orders = data_gen.generate_orders_data(counts['orders'], counts['customers'])
        products = data_gen.generate_products_data(counts['products'])
        inventory = data_gen.generate_inventory_data(counts['products'])
        order_items = data_gen.generate_order_items_data(counts['orders'], counts['products'])
        
        dataset = {
            'customers': customers,
            'orders': orders,
            'products': products,
            'inventory': inventory,
            'order_items': order_items
        }
        
        # Filter tables if specified
        if args.tables:
            dataset = {k: v for k, v in dataset.items() if k in args.tables}
        
        print(f"  Generated {len(customers)} customers", file=sys.stderr)
        print(f"  Generated {len(orders)} orders", file=sys.stderr)
        print(f"  Generated {len(products)} products", file=sys.stderr)
        print(f"  Generated {len(inventory)} inventory records", file=sys.stderr)
        print(f"  Generated {len(order_items)} order items", file=sys.stderr)
        
        # Export to SQL
        exporter = DataExporter(output_dir='/tmp')
        sql_file = exporter.export_to_sql(dataset, args.engine)
        
        with open(sql_file, 'r') as f:
            data_sql = f.read()
        
        # Clean up temp file
        os.remove(sql_file)
    
    # Combine DDL and data
    full_script = ddl
    if data_sql:
        full_script += "\n\n-- Sample Data\n" + data_sql
    
    # Output
    if args.output:
        with open(args.output, 'w') as f:
            f.write(full_script)
        print(f"✅ Generated script saved to: {args.output}", file=sys.stderr)
    else:
        print(full_script)
    
    print(f"✅ DDL generation completed successfully!", file=sys.stderr)


if __name__ == '__main__':
    main()
