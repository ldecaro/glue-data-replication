#!/usr/bin/env python3
"""
Test Data Setup for End-to-End Testing

This module provides utilities for setting up realistic test data across
multiple database types for comprehensive end-to-end testing of the
AWS Glue Data Replication system.

Features:
- Realistic sample data generation
- Cross-database schema compatibility testing
- Incremental data change simulation
- Data volume scaling for performance testing
"""

import json
import csv
import sqlite3
import pandas as pd
import sys
import os
from datetime import datetime, timedelta, date
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass, asdict
import random
import uuid
from faker import Faker

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


@dataclass
class TestDatasetConfig:
    """Configuration for test dataset generation."""
    name: str
    row_count: int
    incremental_percentage: float  # Percentage of data that changes in incremental loads
    data_distribution: Dict[str, Any]  # Distribution parameters for data generation


class TestDataGenerator:
    """Generates realistic test data for multiple database scenarios."""
    
    def __init__(self, seed: int = 42):
        """Initialize the test data generator with a seed for reproducibility."""
        self.fake = Faker()
        Faker.seed(seed)
        random.seed(seed)
        
        # Define realistic data distributions
        self.data_distributions = {
            'customer_segments': ['Premium', 'Standard', 'Basic'],
            'order_statuses': ['pending', 'processing', 'shipped', 'delivered', 'cancelled'],
            'product_categories': ['Electronics', 'Clothing', 'Home & Garden', 'Sports', 'Books', 'Automotive'],
            'warehouse_codes': ['WH001', 'WH002', 'WH003', 'WH004', 'WH005'],
            'countries': ['US', 'CA', 'UK', 'DE', 'FR', 'JP', 'AU'],
            'currencies': ['USD', 'CAD', 'GBP', 'EUR', 'JPY', 'AUD']
        }
    
    def generate_customers_data(self, count: int) -> List[Dict[str, Any]]:
        """Generate realistic customer data."""
        customers = []
        base_date = datetime.now() - timedelta(days=365)
        
        for i in range(1, count + 1):
            created_date = base_date + timedelta(days=random.randint(0, 365))
            updated_date = created_date + timedelta(
                days=random.randint(0, (datetime.now() - created_date).days)
            )
            
            customer = {
                'customer_id': i,
                'first_name': self.fake.first_name(),
                'last_name': self.fake.last_name(),
                'email': self.fake.email(),
                'phone': self.fake.phone_number()[:20],  # Limit length
                'address_line1': self.fake.street_address(),
                'address_line2': self.fake.secondary_address() if random.random() < 0.3 else None,
                'city': self.fake.city(),
                'state': self.fake.state_abbr(),
                'postal_code': self.fake.postcode(),
                'country': random.choice(self.data_distributions['countries']),
                'date_of_birth': self.fake.date_of_birth(minimum_age=18, maximum_age=80),
                'customer_segment': random.choice(self.data_distributions['customer_segments']),
                'registration_source': random.choice(['web', 'mobile', 'store', 'referral']),
                'is_active': random.choice([True, False]),
                'credit_limit': round(random.uniform(1000, 50000), 2),
                'created_at': created_date,
                'updated_at': updated_date,
                'last_login_at': updated_date - timedelta(days=random.randint(0, 30)) if random.random() < 0.8 else None
            }
            customers.append(customer)
        
        return customers
    
    def generate_orders_data(self, count: int, customer_count: int) -> List[Dict[str, Any]]:
        """Generate realistic order data."""
        orders = []
        base_date = datetime.now() - timedelta(days=180)
        
        for i in range(1, count + 1):
            order_date = base_date + timedelta(days=random.randint(0, 180))
            created_at = order_date + timedelta(hours=random.randint(0, 23), minutes=random.randint(0, 59))
            
            # Simulate order modifications
            last_modified = created_at + timedelta(
                hours=random.randint(0, 72),
                minutes=random.randint(0, 59)
            )
            
            order = {
                'order_id': i,
                'customer_id': random.randint(1, customer_count),
                'order_number': f'ORD-{i:06d}',
                'order_date': order_date.date(),
                'order_time': created_at.time(),
                'total_amount': round(random.uniform(10.00, 2000.00), 2),
                'tax_amount': 0,  # Will be calculated
                'shipping_amount': round(random.uniform(0, 50.00), 2),
                'discount_amount': round(random.uniform(0, 100.00), 2) if random.random() < 0.3 else 0,
                'currency': random.choice(self.data_distributions['currencies']),
                'status': random.choice(self.data_distributions['order_statuses']),
                'payment_method': random.choice(['credit_card', 'debit_card', 'paypal', 'bank_transfer']),
                'shipping_method': random.choice(['standard', 'express', 'overnight', 'pickup']),
                'warehouse_code': random.choice(self.data_distributions['warehouse_codes']),
                'notes': self.fake.text(max_nb_chars=200) if random.random() < 0.2 else None,
                'created_at': created_at,
                'last_modified': last_modified,
                'shipped_at': last_modified + timedelta(days=random.randint(1, 5)) if random.random() < 0.7 else None
            }
            
            # Calculate tax (8% of subtotal)
            subtotal = order['total_amount'] - order['shipping_amount'] - order['discount_amount']
            order['tax_amount'] = round(subtotal * 0.08, 2)
            
            orders.append(order)
        
        return orders
    
    def generate_products_data(self, count: int) -> List[Dict[str, Any]]:
        """Generate realistic product data."""
        products = []
        base_date = datetime.now() - timedelta(days=730)  # 2 years of products
        
        for i in range(1, count + 1):
            created_date = base_date + timedelta(days=random.randint(0, 730))
            category = random.choice(self.data_distributions['product_categories'])
            
            product = {
                'product_id': i,
                'sku': f'SKU-{category[:3].upper()}-{i:05d}',
                'product_name': self._generate_product_name(category),
                'category': category,
                'subcategory': self._generate_subcategory(category),
                'brand': self.fake.company(),
                'price': round(random.uniform(5.00, 1000.00), 2),
                'cost': 0,  # Will be calculated
                'weight_kg': round(random.uniform(0.1, 50.0), 2),
                'dimensions_cm': f"{random.randint(5, 100)}x{random.randint(5, 100)}x{random.randint(5, 100)}",
                'color': random.choice(['Black', 'White', 'Red', 'Blue', 'Green', 'Yellow', 'Gray', 'Brown']),
                'size': random.choice(['XS', 'S', 'M', 'L', 'XL', 'XXL']) if category == 'Clothing' else None,
                'description': self.fake.text(max_nb_chars=500),
                'features': self._generate_product_features(category),
                'is_active': random.choice([True, False]),
                'is_featured': random.random() < 0.1,
                'minimum_stock_level': random.randint(5, 50),
                'supplier_id': random.randint(1, 20),
                'warranty_months': random.choice([0, 6, 12, 24, 36]),
                'created_date': created_date.date(),
                'last_updated': (created_date + timedelta(days=random.randint(0, 30))).date()
            }
            
            # Calculate cost (60-80% of price)
            product['cost'] = round(product['price'] * random.uniform(0.6, 0.8), 2)
            
            products.append(product)
        
        return products
    
    def generate_inventory_data(self, product_count: int) -> List[Dict[str, Any]]:
        """Generate realistic inventory data."""
        inventory = []
        warehouses = self.data_distributions['warehouse_codes']
        
        inventory_id = 1
        for product_id in range(1, product_count + 1):
            # Each product may be in multiple warehouses
            num_warehouses = random.randint(1, min(3, len(warehouses)))
            selected_warehouses = random.sample(warehouses, num_warehouses)
            
            for warehouse_code in selected_warehouses:
                last_count_date = datetime.now().date() - timedelta(days=random.randint(0, 30))
                
                inventory_record = {
                    'inventory_id': f'INV-{inventory_id:06d}',
                    'product_id': product_id,
                    'warehouse_code': warehouse_code,
                    'quantity_on_hand': random.randint(0, 500),
                    'quantity_reserved': random.randint(0, 50),
                    'quantity_available': 0,  # Will be calculated
                    'reorder_point': random.randint(10, 100),
                    'max_stock_level': random.randint(200, 1000),
                    'unit_cost': round(random.uniform(5.00, 500.00), 2),
                    'last_count_date': last_count_date,
                    'last_received_date': last_count_date - timedelta(days=random.randint(1, 14)),
                    'last_shipped_date': last_count_date - timedelta(days=random.randint(0, 7)),
                    'location_code': f'{warehouse_code}-{random.choice(["A", "B", "C"])}{random.randint(1, 20):02d}',
                    'batch_number': f'BATCH-{random.randint(1000, 9999)}' if random.random() < 0.5 else None,
                    'expiry_date': (datetime.now() + timedelta(days=random.randint(30, 365))).date() if random.random() < 0.3 else None
                }
                
                # Calculate available quantity
                inventory_record['quantity_available'] = max(0, 
                    inventory_record['quantity_on_hand'] - inventory_record['quantity_reserved']
                )
                
                inventory.append(inventory_record)
                inventory_id += 1
        
        return inventory
    
    def generate_order_items_data(self, order_count: int, product_count: int) -> List[Dict[str, Any]]:
        """Generate realistic order items data."""
        order_items = []
        item_id = 1
        
        for order_id in range(1, order_count + 1):
            # Each order has 1-5 items
            num_items = random.randint(1, 5)
            selected_products = random.sample(range(1, product_count + 1), 
                                            min(num_items, product_count))
            
            for line_number, product_id in enumerate(selected_products, 1):
                quantity = random.randint(1, 10)
                unit_price = round(random.uniform(10.00, 500.00), 2)
                
                order_item = {
                    'order_item_id': item_id,
                    'order_id': order_id,
                    'product_id': product_id,
                    'line_number': line_number,
                    'quantity': quantity,
                    'unit_price': unit_price,
                    'line_total': round(quantity * unit_price, 2),
                    'discount_percentage': random.uniform(0, 20) if random.random() < 0.2 else 0,
                    'discount_amount': 0,  # Will be calculated
                    'tax_rate': 0.08,
                    'tax_amount': 0,  # Will be calculated
                    'warehouse_code': random.choice(self.data_distributions['warehouse_codes']),
                    'shipped_quantity': quantity if random.random() < 0.9 else random.randint(0, quantity),
                    'backorder_quantity': 0,  # Will be calculated
                    'unit_cost': round(unit_price * random.uniform(0.6, 0.8), 2),
                    'created_at': datetime.now() - timedelta(days=random.randint(0, 180)),
                    'updated_at': datetime.now() - timedelta(days=random.randint(0, 30))
                }
                
                # Calculate derived fields
                order_item['discount_amount'] = round(
                    order_item['line_total'] * order_item['discount_percentage'] / 100, 2
                )
                taxable_amount = order_item['line_total'] - order_item['discount_amount']
                order_item['tax_amount'] = round(taxable_amount * order_item['tax_rate'], 2)
                order_item['backorder_quantity'] = order_item['quantity'] - order_item['shipped_quantity']
                
                order_items.append(order_item)
                item_id += 1
        
        return order_items
    
    def _generate_product_name(self, category: str) -> str:
        """Generate realistic product names based on category."""
        # Define book types separately to avoid backslash in f-string
        book_types = ["Fiction", "Non-Fiction", "Educational", "Children's"]
        
        name_templates = {
            'Electronics': [
                f'{self.fake.company()} {random.choice(["Wireless", "Bluetooth", "Smart", "Digital"])} {random.choice(["Headphones", "Speaker", "Camera", "Watch", "Phone"])}',
                f'{random.choice(["Professional", "Gaming", "Portable"])} {random.choice(["Laptop", "Monitor", "Keyboard", "Mouse", "Tablet"])}'
            ],
            'Clothing': [
                f'{random.choice(["Premium", "Classic", "Modern", "Vintage"])} {random.choice(["Cotton", "Wool", "Silk", "Denim"])} {random.choice(["Shirt", "Pants", "Dress", "Jacket", "Sweater"])}',
                f'{random.choice(["Casual", "Formal", "Sport"])} {random.choice(["T-Shirt", "Jeans", "Shoes", "Hat", "Scarf"])}'
            ],
            'Home & Garden': [
                f'{random.choice(["Deluxe", "Standard", "Compact"])} {random.choice(["Kitchen", "Bathroom", "Garden", "Living Room"])} {random.choice(["Set", "Organizer", "Tool", "Accessory"])}',
                f'{random.choice(["Stainless Steel", "Wooden", "Plastic", "Glass"])} {random.choice(["Table", "Chair", "Lamp", "Vase", "Mirror"])}'
            ],
            'Sports': [
                f'{random.choice(["Professional", "Amateur", "Youth"])} {random.choice(["Basketball", "Soccer", "Tennis", "Golf", "Running"])} {random.choice(["Equipment", "Gear", "Accessories"])}',
                f'{random.choice(["Indoor", "Outdoor", "Water"])} {random.choice(["Sports", "Fitness", "Recreation"])} {random.choice(["Kit", "Set", "Tool"])}'
            ],
            'Books': [
                f'{random.choice(["Complete", "Essential", "Advanced", "Beginner"])} {random.choice(["Guide", "Manual", "Handbook", "Reference"])} to {random.choice(["Programming", "Cooking", "Gardening", "Photography", "Business"])}',
                f'{random.choice(book_types)} {random.choice(["Novel", "Textbook", "Workbook", "Collection"])}'
            ],
            'Automotive': [
                f'{random.choice(["Premium", "Standard", "Heavy-Duty"])} {random.choice(["Car", "Truck", "Motorcycle"])} {random.choice(["Parts", "Accessories", "Tools", "Fluids"])}',
                f'{random.choice(["Universal", "OEM", "Aftermarket"])} {random.choice(["Engine", "Brake", "Tire", "Battery", "Filter"])} {random.choice(["Component", "Kit", "System"])}'
            ]
        }
        
        templates = name_templates.get(category, [f'{category} Product'])
        return random.choice(templates)
    
    def _generate_subcategory(self, category: str) -> str:
        """Generate realistic subcategories based on main category."""
        subcategories = {
            'Electronics': ['Audio', 'Video', 'Computing', 'Mobile', 'Gaming', 'Smart Home'],
            'Clothing': ['Men\'s', 'Women\'s', 'Children\'s', 'Accessories', 'Footwear', 'Outerwear'],
            'Home & Garden': ['Kitchen', 'Bathroom', 'Living Room', 'Bedroom', 'Garden', 'Tools'],
            'Sports': ['Team Sports', 'Individual Sports', 'Fitness', 'Outdoor', 'Water Sports', 'Winter Sports'],
            'Books': ['Fiction', 'Non-Fiction', 'Educational', 'Reference', 'Children\'s', 'Technical'],
            'Automotive': ['Engine Parts', 'Body Parts', 'Electronics', 'Fluids', 'Tools', 'Accessories']
        }
        
        return random.choice(subcategories.get(category, ['General']))
    
    def _generate_product_features(self, category: str) -> str:
        """Generate realistic product features based on category."""
        features = {
            'Electronics': ['Wireless connectivity', 'Long battery life', 'High resolution', 'Water resistant', 'Voice control'],
            'Clothing': ['Machine washable', 'Wrinkle resistant', 'Breathable fabric', 'UV protection', 'Moisture wicking'],
            'Home & Garden': ['Easy assembly', 'Dishwasher safe', 'Weather resistant', 'Energy efficient', 'Space saving'],
            'Sports': ['Lightweight', 'Durable construction', 'Professional grade', 'All weather', 'Ergonomic design'],
            'Books': ['Illustrated', 'Updated edition', 'Comprehensive coverage', 'Step-by-step instructions', 'Expert authored'],
            'Automotive': ['OEM quality', 'Easy installation', 'Corrosion resistant', 'High performance', 'Long lasting']
        }
        
        category_features = features.get(category, ['High quality', 'Reliable', 'Durable'])
        selected_features = random.sample(category_features, random.randint(2, 4))
        return ', '.join(selected_features)
    
    def create_incremental_changes(self, existing_data: Dict[str, List[Dict]], 
                                 change_percentage: float = 0.1) -> Dict[str, Dict[str, List[Dict]]]:
        """Create incremental changes to existing data for testing incremental loads."""
        changes = {
            'updates': {},
            'inserts': {},
            'deletes': {}
        }
        
        current_time = datetime.now()
        
        # Customer updates
        if 'customers' in existing_data:
            customers = existing_data['customers']
            num_updates = max(1, int(len(customers) * change_percentage))
            customers_to_update = random.sample(customers, num_updates)
            
            updated_customers = []
            for customer in customers_to_update:
                updated_customer = customer.copy()
                
                # Randomly update some fields
                if random.random() < 0.5:
                    updated_customer['email'] = self.fake.email()
                if random.random() < 0.3:
                    updated_customer['phone'] = self.fake.phone_number()[:20]
                if random.random() < 0.2:
                    updated_customer['address_line1'] = self.fake.street_address()
                
                updated_customer['updated_at'] = current_time
                updated_customer['last_login_at'] = current_time - timedelta(hours=random.randint(1, 24))
                
                updated_customers.append(updated_customer)
            
            changes['updates']['customers'] = updated_customers
        
        # New orders
        if 'orders' in existing_data:
            existing_orders = existing_data['orders']
            max_order_id = max(order['order_id'] for order in existing_orders) if existing_orders else 0
            max_customer_id = max(order['customer_id'] for order in existing_orders) if existing_orders else 1
            
            num_new_orders = random.randint(1, 5)
            new_orders = []
            
            for i in range(num_new_orders):
                order_id = max_order_id + i + 1
                order_date = current_time.date()
                
                new_order = {
                    'order_id': order_id,
                    'customer_id': random.randint(1, max_customer_id),
                    'order_number': f'ORD-{order_id:06d}',
                    'order_date': order_date,
                    'order_time': current_time.time(),
                    'total_amount': round(random.uniform(25.00, 500.00), 2),
                    'tax_amount': 0,
                    'shipping_amount': round(random.uniform(5.00, 25.00), 2),
                    'discount_amount': 0,
                    'currency': 'USD',
                    'status': 'pending',
                    'payment_method': random.choice(['credit_card', 'debit_card', 'paypal']),
                    'shipping_method': 'standard',
                    'warehouse_code': random.choice(self.data_distributions['warehouse_codes']),
                    'notes': None,
                    'created_at': current_time,
                    'last_modified': current_time,
                    'shipped_at': None
                }
                
                # Calculate tax
                subtotal = new_order['total_amount'] - new_order['shipping_amount']
                new_order['tax_amount'] = round(subtotal * 0.08, 2)
                
                new_orders.append(new_order)
            
            changes['inserts']['orders'] = new_orders
        
        # Product updates
        if 'products' in existing_data:
            products = existing_data['products']
            num_updates = max(1, int(len(products) * change_percentage * 0.5))  # Fewer product updates
            products_to_update = random.sample(products, num_updates)
            
            updated_products = []
            for product in products_to_update:
                updated_product = product.copy()
                
                # Update price and cost
                if random.random() < 0.7:
                    price_change = random.uniform(-0.2, 0.3)  # -20% to +30%
                    updated_product['price'] = round(product['price'] * (1 + price_change), 2)
                    updated_product['cost'] = round(updated_product['price'] * random.uniform(0.6, 0.8), 2)
                
                # Update stock levels
                if random.random() < 0.5:
                    updated_product['minimum_stock_level'] = random.randint(5, 50)
                
                updated_product['last_updated'] = current_time.date()
                updated_products.append(updated_product)
            
            changes['updates']['products'] = updated_products
        
        # Inventory updates
        if 'inventory' in existing_data:
            inventory = existing_data['inventory']
            num_updates = max(1, int(len(inventory) * change_percentage))
            inventory_to_update = random.sample(inventory, num_updates)
            
            updated_inventory = []
            for inv_record in inventory_to_update:
                updated_record = inv_record.copy()
                
                # Simulate stock movements
                quantity_change = random.randint(-50, 100)
                updated_record['quantity_on_hand'] = max(0, inv_record['quantity_on_hand'] + quantity_change)
                updated_record['quantity_reserved'] = random.randint(0, min(20, updated_record['quantity_on_hand']))
                updated_record['quantity_available'] = max(0, 
                    updated_record['quantity_on_hand'] - updated_record['quantity_reserved']
                )
                
                updated_record['last_count_date'] = current_time.date()
                if quantity_change > 0:
                    updated_record['last_received_date'] = current_time.date()
                elif quantity_change < 0:
                    updated_record['last_shipped_date'] = current_time.date()
                
                updated_inventory.append(updated_record)
            
            changes['updates']['inventory'] = updated_inventory
        
        return changes


class TestDataExporter:
    """Exports test data to various formats for different database engines."""
    
    def __init__(self, output_dir: str = 'test_data'):
        """Initialize the exporter with output directory."""
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
    
    def export_to_csv(self, data: Dict[str, List[Dict]], prefix: str = '') -> Dict[str, str]:
        """Export test data to CSV files."""
        file_paths = {}
        
        for table_name, records in data.items():
            if not records:
                continue
            
            filename = f"{prefix}{table_name}.csv" if prefix else f"{table_name}.csv"
            file_path = os.path.join(self.output_dir, filename)
            
            # Convert datetime objects to strings for CSV export
            processed_records = []
            for record in records:
                processed_record = {}
                for key, value in record.items():
                    if isinstance(value, (datetime, date)):
                        processed_record[key] = value.isoformat()
                    elif value is None:
                        processed_record[key] = ''
                    else:
                        processed_record[key] = value
                processed_records.append(processed_record)
            
            # Write to CSV
            if processed_records:
                df = pd.DataFrame(processed_records)
                df.to_csv(file_path, index=False)
                file_paths[table_name] = file_path
        
        return file_paths
    
    def export_to_json(self, data: Dict[str, List[Dict]], filename: str = 'test_data.json') -> str:
        """Export test data to JSON format."""
        file_path = os.path.join(self.output_dir, filename)
        
        # Convert datetime objects to strings for JSON serialization
        json_data = {}
        for table_name, records in data.items():
            json_records = []
            for record in records:
                json_record = {}
                for key, value in record.items():
                    if isinstance(value, (datetime, date)):
                        json_record[key] = value.isoformat()
                    else:
                        json_record[key] = value
                json_records.append(json_record)
            json_data[table_name] = json_records
        
        with open(file_path, 'w') as f:
            json.dump(json_data, f, indent=2, default=str)
        
        return file_path
    
    def export_to_sql(self, data: Dict[str, List[Dict]], engine_type: str = 'postgresql') -> str:
        """Export test data as SQL INSERT statements."""
        filename = f"test_data_{engine_type}.sql"
        file_path = os.path.join(self.output_dir, filename)
        
        with open(file_path, 'w') as f:
            f.write(f"-- Test data for {engine_type.upper()}\n")
            f.write(f"-- Generated on {datetime.now().isoformat()}\n\n")
            
            for table_name, records in data.items():
                if not records:
                    continue
                
                f.write(f"-- Data for table: {table_name}\n")
                
                for record in records:
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
                            values.append(f"'{value.isoformat()}'")
                        elif isinstance(value, bool):
                            values.append('TRUE' if value else 'FALSE')
                        else:
                            values.append(str(value))
                    
                    insert_sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(values)});\n"
                    f.write(insert_sql)
                
                f.write("\n")
        
        return file_path


def main():
    """Main function for generating test data."""
    print("Generating comprehensive test data for end-to-end testing...")
    
    # Initialize generator
    generator = TestDataGenerator(seed=42)
    
    # Generate datasets of different sizes
    datasets = {
        'small': {'customers': 100, 'orders': 500, 'products': 200},
        'medium': {'customers': 1000, 'orders': 5000, 'products': 1000},
        'large': {'customers': 10000, 'orders': 50000, 'products': 5000}
    }
    
    exporter = TestDataExporter()
    
    for dataset_name, counts in datasets.items():
        print(f"\nGenerating {dataset_name} dataset...")
        
        # Generate core data
        customers = generator.generate_customers_data(counts['customers'])
        orders = generator.generate_orders_data(counts['orders'], counts['customers'])
        products = generator.generate_products_data(counts['products'])
        inventory = generator.generate_inventory_data(counts['products'])
        order_items = generator.generate_order_items_data(counts['orders'], counts['products'])
        
        dataset = {
            'customers': customers,
            'orders': orders,
            'products': products,
            'inventory': inventory,
            'order_items': order_items
        }
        
        print(f"  - Generated {len(customers)} customers")
        print(f"  - Generated {len(orders)} orders")
        print(f"  - Generated {len(products)} products")
        print(f"  - Generated {len(inventory)} inventory records")
        print(f"  - Generated {len(order_items)} order items")
        
        # Export to different formats
        csv_files = exporter.export_to_csv(dataset, f"{dataset_name}_")
        json_file = exporter.export_to_json(dataset, f"{dataset_name}_test_data.json")
        sql_file = exporter.export_to_sql(dataset, 'postgresql')
        
        print(f"  - Exported to CSV: {len(csv_files)} files")
        print(f"  - Exported to JSON: {json_file}")
        print(f"  - Exported to SQL: {sql_file}")
        
        # Generate incremental changes
        changes = generator.create_incremental_changes(dataset)
        incremental_file = exporter.export_to_json(changes, f"{dataset_name}_incremental_changes.json")
        print(f"  - Generated incremental changes: {incremental_file}")
    
    print("\n✅ Test data generation completed successfully!")
    print(f"All files saved to: {exporter.output_dir}")


if __name__ == '__main__':
    main()