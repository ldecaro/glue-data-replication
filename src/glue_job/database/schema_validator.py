"""
Schema validation and data type mapping for AWS Glue Data Replication.

This module provides schema compatibility validation and data type mapping
between different database engines for cross-database replication.
"""

import logging
from typing import Dict, List, Any
# Conditional imports for PySpark (only available in Glue runtime)
try:
    from pyspark.sql import DataFrame
    from pyspark.sql.types import StructType
    from pyspark.sql.functions import col
except ImportError:
    # Mock classes for local development/testing
    class DataFrame:
        pass
    class StructType:
        pass
    def col(name):
        return name

# Import from other modules
from ..config.job_config import ConnectionConfig

logger = logging.getLogger(__name__)


class DataTypeMapper:
    """Handles data type mapping between different database engines."""
    
    # Comprehensive data type mappings for cross-database compatibility
    TYPE_MAPPINGS = {
        'oracle_to_postgresql': {
            'NUMBER': 'NUMERIC',
            'VARCHAR2': 'VARCHAR',
            'NVARCHAR2': 'VARCHAR',
            'CHAR': 'CHAR',
            'NCHAR': 'CHAR',
            'DATE': 'TIMESTAMP',
            'TIMESTAMP': 'TIMESTAMP',
            'CLOB': 'TEXT',
            'NCLOB': 'TEXT',
            'BLOB': 'BYTEA',
            'RAW': 'BYTEA',
            'LONG': 'TEXT',
            'LONG RAW': 'BYTEA',
            'BINARY_FLOAT': 'REAL',
            'BINARY_DOUBLE': 'DOUBLE PRECISION'
        },
        'oracle_to_sqlserver': {
            'NUMBER': 'NUMERIC',
            'VARCHAR2': 'NVARCHAR',
            'NVARCHAR2': 'NVARCHAR',
            'CHAR': 'CHAR',
            'NCHAR': 'NCHAR',
            'DATE': 'DATETIME2',
            'TIMESTAMP': 'DATETIME2',
            'CLOB': 'NTEXT',
            'NCLOB': 'NTEXT',
            'BLOB': 'VARBINARY',
            'RAW': 'VARBINARY',
            'LONG': 'NTEXT',
            'LONG RAW': 'VARBINARY',
            'BINARY_FLOAT': 'REAL',
            'BINARY_DOUBLE': 'FLOAT'
        },
        'oracle_to_db2': {
            'NUMBER': 'DECIMAL',
            'VARCHAR2': 'VARCHAR',
            'NVARCHAR2': 'VARCHAR',
            'CHAR': 'CHAR',
            'NCHAR': 'CHAR',
            'DATE': 'TIMESTAMP',
            'TIMESTAMP': 'TIMESTAMP',
            'CLOB': 'CLOB',
            'NCLOB': 'CLOB',
            'BLOB': 'BLOB',
            'RAW': 'VARBINARY',
            'LONG': 'CLOB',
            'LONG RAW': 'BLOB',
            'BINARY_FLOAT': 'REAL',
            'BINARY_DOUBLE': 'DOUBLE'
        },
        'sqlserver_to_postgresql': {
            'INT': 'INTEGER',
            'BIGINT': 'BIGINT',
            'SMALLINT': 'SMALLINT',
            'TINYINT': 'SMALLINT',
            'DECIMAL': 'NUMERIC',
            'NUMERIC': 'NUMERIC',
            'FLOAT': 'DOUBLE PRECISION',
            'REAL': 'REAL',
            'VARCHAR': 'VARCHAR',
            'NVARCHAR': 'VARCHAR',
            'CHAR': 'CHAR',
            'NCHAR': 'CHAR',
            'TEXT': 'TEXT',
            'NTEXT': 'TEXT',
            'DATETIME': 'TIMESTAMP',
            'DATETIME2': 'TIMESTAMP',
            'DATE': 'DATE',
            'TIME': 'TIME',
            'BIT': 'BOOLEAN',
            'VARBINARY': 'BYTEA',
            'IMAGE': 'BYTEA',
            'MONEY': 'NUMERIC',
            'SMALLMONEY': 'NUMERIC',
            'UNIQUEIDENTIFIER': 'UUID'
        },
        'sqlserver_to_oracle': {
            'INT': 'NUMBER',
            'BIGINT': 'NUMBER',
            'SMALLINT': 'NUMBER',
            'TINYINT': 'NUMBER',
            'DECIMAL': 'NUMBER',
            'NUMERIC': 'NUMBER',
            'FLOAT': 'BINARY_DOUBLE',
            'REAL': 'BINARY_FLOAT',
            'VARCHAR': 'VARCHAR2',
            'NVARCHAR': 'NVARCHAR2',
            'CHAR': 'CHAR',
            'NCHAR': 'NCHAR',
            'TEXT': 'CLOB',
            'NTEXT': 'NCLOB',
            'DATETIME': 'TIMESTAMP',
            'DATETIME2': 'TIMESTAMP',
            'DATE': 'DATE',
            'TIME': 'TIMESTAMP',
            'BIT': 'NUMBER',
            'VARBINARY': 'RAW',
            'IMAGE': 'BLOB',
            'MONEY': 'NUMBER',
            'SMALLMONEY': 'NUMBER',
            'UNIQUEIDENTIFIER': 'RAW'
        },
        'sqlserver_to_db2': {
            'INT': 'INTEGER',
            'BIGINT': 'BIGINT',
            'SMALLINT': 'SMALLINT',
            'TINYINT': 'SMALLINT',
            'DECIMAL': 'DECIMAL',
            'NUMERIC': 'NUMERIC',
            'FLOAT': 'DOUBLE',
            'REAL': 'REAL',
            'VARCHAR': 'VARCHAR',
            'NVARCHAR': 'VARCHAR',
            'CHAR': 'CHAR',
            'NCHAR': 'CHAR',
            'TEXT': 'CLOB',
            'NTEXT': 'CLOB',
            'DATETIME': 'TIMESTAMP',
            'DATETIME2': 'TIMESTAMP',
            'DATE': 'DATE',
            'TIME': 'TIME',
            'BIT': 'SMALLINT',
            'VARBINARY': 'VARBINARY',
            'IMAGE': 'BLOB',
            'MONEY': 'DECIMAL',
            'SMALLMONEY': 'DECIMAL',
            'UNIQUEIDENTIFIER': 'CHAR'
        },
        'postgresql_to_oracle': {
            'INTEGER': 'NUMBER',
            'BIGINT': 'NUMBER',
            'SMALLINT': 'NUMBER',
            'NUMERIC': 'NUMBER',
            'DECIMAL': 'NUMBER',
            'REAL': 'BINARY_FLOAT',
            'DOUBLE PRECISION': 'BINARY_DOUBLE',
            'VARCHAR': 'VARCHAR2',
            'CHAR': 'CHAR',
            'TEXT': 'CLOB',
            'DATE': 'DATE',
            'TIME': 'TIMESTAMP',
            'TIMESTAMP': 'TIMESTAMP',
            'BOOLEAN': 'NUMBER',
            'BYTEA': 'BLOB',
            'UUID': 'RAW',
            'JSON': 'CLOB',
            'JSONB': 'CLOB'
        },
        'postgresql_to_sqlserver': {
            'INTEGER': 'INT',
            'BIGINT': 'BIGINT',
            'SMALLINT': 'SMALLINT',
            'NUMERIC': 'NUMERIC',
            'DECIMAL': 'DECIMAL',
            'REAL': 'REAL',
            'DOUBLE PRECISION': 'FLOAT',
            'VARCHAR': 'NVARCHAR',
            'CHAR': 'CHAR',
            'TEXT': 'NTEXT',
            'DATE': 'DATE',
            'TIME': 'TIME',
            'TIMESTAMP': 'DATETIME2',
            'BOOLEAN': 'BIT',
            'BYTEA': 'VARBINARY',
            'UUID': 'UNIQUEIDENTIFIER',
            'JSON': 'NTEXT',
            'JSONB': 'NTEXT'
        },
        'postgresql_to_db2': {
            'INTEGER': 'INTEGER',
            'BIGINT': 'BIGINT',
            'SMALLINT': 'SMALLINT',
            'NUMERIC': 'DECIMAL',
            'DECIMAL': 'DECIMAL',
            'REAL': 'REAL',
            'DOUBLE PRECISION': 'DOUBLE',
            'VARCHAR': 'VARCHAR',
            'CHAR': 'CHAR',
            'TEXT': 'CLOB',
            'DATE': 'DATE',
            'TIME': 'TIME',
            'TIMESTAMP': 'TIMESTAMP',
            'BOOLEAN': 'SMALLINT',
            'BYTEA': 'BLOB',
            'UUID': 'CHAR',
            'JSON': 'CLOB',
            'JSONB': 'CLOB'
        },
        'db2_to_postgresql': {
            'INTEGER': 'INTEGER',
            'BIGINT': 'BIGINT',
            'SMALLINT': 'SMALLINT',
            'DECIMAL': 'NUMERIC',
            'NUMERIC': 'NUMERIC',
            'REAL': 'REAL',
            'DOUBLE': 'DOUBLE PRECISION',
            'VARCHAR': 'VARCHAR',
            'CHAR': 'CHAR',
            'CLOB': 'TEXT',
            'DATE': 'DATE',
            'TIME': 'TIME',
            'TIMESTAMP': 'TIMESTAMP',
            'BLOB': 'BYTEA',
            'VARBINARY': 'BYTEA',
            'GRAPHIC': 'VARCHAR',
            'VARGRAPHIC': 'VARCHAR'
        },
        'db2_to_oracle': {
            'INTEGER': 'NUMBER',
            'BIGINT': 'NUMBER',
            'SMALLINT': 'NUMBER',
            'DECIMAL': 'NUMBER',
            'NUMERIC': 'NUMBER',
            'REAL': 'BINARY_FLOAT',
            'DOUBLE': 'BINARY_DOUBLE',
            'VARCHAR': 'VARCHAR2',
            'CHAR': 'CHAR',
            'CLOB': 'CLOB',
            'DATE': 'DATE',
            'TIME': 'TIMESTAMP',
            'TIMESTAMP': 'TIMESTAMP',
            'BLOB': 'BLOB',
            'VARBINARY': 'RAW',
            'GRAPHIC': 'NVARCHAR2',
            'VARGRAPHIC': 'NVARCHAR2'
        },
        'db2_to_sqlserver': {
            'INTEGER': 'INT',
            'BIGINT': 'BIGINT',
            'SMALLINT': 'SMALLINT',
            'DECIMAL': 'DECIMAL',
            'NUMERIC': 'NUMERIC',
            'REAL': 'REAL',
            'DOUBLE': 'FLOAT',
            'VARCHAR': 'NVARCHAR',
            'CHAR': 'CHAR',
            'CLOB': 'NTEXT',
            'DATE': 'DATE',
            'TIME': 'TIME',
            'TIMESTAMP': 'DATETIME2',
            'BLOB': 'VARBINARY',
            'VARBINARY': 'VARBINARY',
            'GRAPHIC': 'NVARCHAR',
            'VARGRAPHIC': 'NVARCHAR'
        }
    }
    
    # Data transformation functions for specific type conversions
    TRANSFORMATION_FUNCTIONS = {
        'oracle_to_postgresql': {
            'DATE': lambda col: col.cast('timestamp'),
            'NUMBER': lambda col: col.cast('decimal(38,10)'),
            'CLOB': lambda col: col.cast('string')
        },
        'sqlserver_to_postgresql': {
            'BIT': lambda col: col.cast('boolean'),
            'DATETIME': lambda col: col.cast('timestamp'),
            'UNIQUEIDENTIFIER': lambda col: col.cast('string')
        },
        'postgresql_to_oracle': {
            'BOOLEAN': lambda col: col.cast('int'),
            'UUID': lambda col: col.cast('string'),
            'JSON': lambda col: col.cast('string'),
            'JSONB': lambda col: col.cast('string')
        },
        'postgresql_to_sqlserver': {
            'UUID': lambda col: col.cast('string'),
            'JSON': lambda col: col.cast('string'),
            'JSONB': lambda col: col.cast('string')
        }
    }
    
    @classmethod
    def get_mapping_key(cls, source_engine: str, target_engine: str) -> str:
        """Generate mapping key for source-target engine combination."""
        return f"{source_engine.lower()}_to_{target_engine.lower()}"
    
    @classmethod
    def map_data_type(cls, source_engine: str, target_engine: str, source_type: str) -> str:
        """Map data type from source engine to target engine."""
        # If same engine, no mapping needed
        if source_engine.lower() == target_engine.lower():
            return source_type
        
        mapping_key = cls.get_mapping_key(source_engine, target_engine)
        type_mapping = cls.TYPE_MAPPINGS.get(mapping_key, {})
        
        # Return mapped type or original if no mapping exists
        mapped_type = type_mapping.get(source_type.upper(), source_type)
        
        if mapped_type == source_type and mapping_key in cls.TYPE_MAPPINGS:
            logger.warning(
                f"No type mapping found for {source_type} in {source_engine} -> {target_engine} conversion. "
                f"Using original type: {source_type}"
            )
        
        return mapped_type
    
    @classmethod
    def is_cross_database_replication(cls, source_engine: str, target_engine: str) -> bool:
        """Check if this is a cross-database replication scenario."""
        return source_engine.lower() != target_engine.lower()
    
    @classmethod
    def get_supported_mappings(cls) -> List[str]:
        """Get list of all supported database engine mappings."""
        return list(cls.TYPE_MAPPINGS.keys())
    
    @classmethod
    def is_mapping_supported(cls, source_engine: str, target_engine: str) -> bool:
        """Check if mapping between source and target engines is supported."""
        mapping_key = cls.get_mapping_key(source_engine, target_engine)
        return mapping_key in cls.TYPE_MAPPINGS
    
    @classmethod
    def transform_dataframe_types(cls, df: DataFrame, source_engine: str, target_engine: str) -> DataFrame:
        """Transform DataFrame column types for cross-database compatibility."""
        if not cls.is_cross_database_replication(source_engine, target_engine):
            return df
        
        mapping_key = cls.get_mapping_key(source_engine, target_engine)
        transformation_funcs = cls.TRANSFORMATION_FUNCTIONS.get(mapping_key, {})
        
        if not transformation_funcs:
            logger.info(f"No specific transformations needed for {mapping_key}")
            return df
        
        transformed_df = df
        
        for field in df.schema.fields:
            field_type = str(field.dataType).upper()
            
            # Check if this field type needs transformation
            for source_type, transform_func in transformation_funcs.items():
                if source_type in field_type:
                    logger.info(f"Transforming column {field.name} from {field_type} using {source_type} transformation")
                    transformed_df = transformed_df.withColumn(field.name, transform_func(col(field.name)))
                    break
        
        return transformed_df


class SchemaCompatibilityValidator:
    """Validates schema compatibility between source and target databases for cross-database replication."""
    
    def __init__(self, data_type_mapper: DataTypeMapper):
        self.data_type_mapper = data_type_mapper
    
    def validate_schema_compatibility(self, source_schema: StructType, target_schema: StructType,
                                    source_engine: str, target_engine: str, table_name: str) -> Dict[str, Any]:
        """Validate that source and target schemas are compatible for replication."""
        validation_result = {
            'is_compatible': True,
            'warnings': [],
            'errors': [],
            'column_mappings': {},
            'missing_columns': [],
            'extra_columns': [],
            'type_mismatches': []
        }
        
        try:
            logger.info(f"Validating schema compatibility for table {table_name}: {source_engine} -> {target_engine}")
            
            # Create column dictionaries for easier comparison
            source_columns = {field.name.lower(): field for field in source_schema.fields}
            target_columns = {field.name.lower(): field for field in target_schema.fields}
            
            # Check for missing columns in target
            for col_name, source_field in source_columns.items():
                if col_name not in target_columns:
                    validation_result['missing_columns'].append(col_name)
                    validation_result['errors'].append(
                        f"Column '{col_name}' exists in source but missing in target schema"
                    )
                    validation_result['is_compatible'] = False
            
            # Check for extra columns in target (warnings only)
            for col_name in target_columns:
                if col_name not in source_columns:
                    validation_result['extra_columns'].append(col_name)
                    validation_result['warnings'].append(
                        f"Column '{col_name}' exists in target but not in source schema"
                    )
            
            # Validate data type compatibility for matching columns
            for col_name, source_field in source_columns.items():
                if col_name in target_columns:
                    target_field = target_columns[col_name]
                    
                    # Get string representations of data types
                    source_type_str = self._get_type_string(source_field.dataType)
                    target_type_str = self._get_type_string(target_field.dataType)
                    
                    # Map source type to expected target type
                    expected_target_type = self.data_type_mapper.map_data_type(
                        source_engine, target_engine, source_type_str
                    )
                    
                    # Check if target type is compatible
                    if not self._are_types_compatible(target_type_str, expected_target_type):
                        mismatch_info = {
                            'column': col_name,
                            'source_type': source_type_str,
                            'target_type': target_type_str,
                            'expected_type': expected_target_type
                        }
                        validation_result['type_mismatches'].append(mismatch_info)
                        validation_result['errors'].append(
                            f"Type mismatch for column '{col_name}': source={source_type_str}, "
                            f"target={target_type_str}, expected={expected_target_type}"
                        )
                        validation_result['is_compatible'] = False
                    
                    # Store column mapping
                    validation_result['column_mappings'][col_name] = {
                        'source_type': source_type_str,
                        'target_type': target_type_str,
                        'mapped_type': expected_target_type
                    }
            
            # Log validation results
            if validation_result['is_compatible']:
                logger.info(f"Schema validation passed for table {table_name}")
                if validation_result['warnings']:
                    logger.warning(f"Schema validation warnings for table {table_name}: {len(validation_result['warnings'])} warnings")
            else:
                logger.error(f"Schema validation failed for table {table_name}: {len(validation_result['errors'])} errors")
            
            return validation_result
            
        except Exception as e:
            logger.error(f"Schema validation failed with exception for table {table_name}: {str(e)}")
            validation_result['is_compatible'] = False
            validation_result['errors'].append(f"Validation exception: {str(e)}")
            return validation_result
    
    def _get_type_string(self, data_type) -> str:
        """Convert Spark DataType to string representation."""
        type_str = str(data_type)
        
        # Normalize common type representations
        type_mappings = {
            'StringType': 'VARCHAR',
            'IntegerType': 'INTEGER',
            'LongType': 'BIGINT',
            'DoubleType': 'DOUBLE',
            'FloatType': 'FLOAT',
            'BooleanType': 'BOOLEAN',
            'TimestampType': 'TIMESTAMP',
            'DateType': 'DATE',
            'BinaryType': 'BINARY',
            'DecimalType': 'DECIMAL'
        }
        
        for spark_type, db_type in type_mappings.items():
            if spark_type in type_str:
                return db_type
        
        return type_str.upper()
    
    def _are_types_compatible(self, actual_type: str, expected_type: str) -> bool:
        """Check if actual and expected types are compatible."""
        # Normalize types for comparison
        actual = actual_type.upper().strip()
        expected = expected_type.upper().strip()
        
        # Exact match
        if actual == expected:
            return True
        
        # Define compatible type groups
        compatible_groups = [
            {'VARCHAR', 'NVARCHAR', 'TEXT', 'STRING', 'CHAR', 'NCHAR'},
            {'INTEGER', 'INT', 'SMALLINT', 'BIGINT', 'NUMBER'},
            {'DECIMAL', 'NUMERIC', 'NUMBER'},
            {'FLOAT', 'REAL', 'DOUBLE', 'DOUBLE PRECISION'},
            {'TIMESTAMP', 'DATETIME', 'DATETIME2'},
            {'BYTEA', 'VARBINARY', 'BINARY', 'BLOB', 'RAW'},
            {'BOOLEAN', 'BIT'},
            {'DATE'},
            {'TIME'},
            {'UUID', 'UNIQUEIDENTIFIER'}
        ]
        
        # Check if both types are in the same compatibility group
        for group in compatible_groups:
            if actual in group and expected in group:
                return True
        
        # Special cases for partial matches
        if 'VARCHAR' in actual and 'VARCHAR' in expected:
            return True
        if 'DECIMAL' in actual and 'DECIMAL' in expected:
            return True
        if 'NUMERIC' in actual and 'NUMERIC' in expected:
            return True
        
        return False
    
    def generate_compatibility_report(self, validation_result: Dict[str, Any], table_name: str) -> str:
        """Generate a human-readable compatibility report."""
        report_lines = [
            f"Schema Compatibility Report for Table: {table_name}",
            "=" * 60,
            f"Overall Status: {'COMPATIBLE' if validation_result['is_compatible'] else 'INCOMPATIBLE'}",
            ""
        ]
        
        if validation_result['errors']:
            report_lines.extend([
                "ERRORS:",
                "-" * 20
            ])
            for error in validation_result['errors']:
                report_lines.append(f"  • {error}")
            report_lines.append("")
        
        if validation_result['warnings']:
            report_lines.extend([
                "WARNINGS:",
                "-" * 20
            ])
            for warning in validation_result['warnings']:
                report_lines.append(f"  • {warning}")
            report_lines.append("")
        
        if validation_result['column_mappings']:
            report_lines.extend([
                "COLUMN MAPPINGS:",
                "-" * 20
            ])
            for col_name, mapping in validation_result['column_mappings'].items():
                report_lines.append(
                    f"  {col_name}: {mapping['source_type']} -> {mapping['target_type']} "
                    f"(mapped: {mapping['mapped_type']})"
                )
            report_lines.append("")
        
        if validation_result['type_mismatches']:
            report_lines.extend([
                "TYPE MISMATCHES:",
                "-" * 20
            ])
            for mismatch in validation_result['type_mismatches']:
                report_lines.append(
                    f"  {mismatch['column']}: Expected {mismatch['expected_type']}, "
                    f"Found {mismatch['target_type']}"
                )
            report_lines.append("")
        
        return "\n".join(report_lines)
    
    def validate_cross_database_assumptions(self, source_config: ConnectionConfig, 
                                          target_config: ConnectionConfig,
                                          table_names: List[str]) -> Dict[str, Any]:
        """Validate assumptions for cross-database replication scenarios."""
        validation_summary = {
            'overall_compatible': True,
            'table_results': {},
            'unsupported_mappings': [],
            'recommendations': []
        }
        
        # Check if the engine mapping is supported
        if not self.data_type_mapper.is_mapping_supported(source_config.engine_type, target_config.engine_type):
            mapping_key = self.data_type_mapper.get_mapping_key(source_config.engine_type, target_config.engine_type)
            validation_summary['unsupported_mappings'].append(mapping_key)
            validation_summary['overall_compatible'] = False
            validation_summary['recommendations'].append(
                f"Mapping {source_config.engine_type} -> {target_config.engine_type} is not fully supported. "
                "Manual schema verification is recommended."
            )
        
        # Add general recommendations for cross-database replication
        validation_summary['recommendations'].extend([
            "Ensure target database schema and tables are created before running replication",
            "Verify that target table indexes and constraints are properly configured",
            "Test with a small subset of data before full migration",
            "Monitor data type conversions for potential data loss"
        ])
        
        if self.data_type_mapper.is_cross_database_replication(source_config.engine_type, target_config.engine_type):
            validation_summary['recommendations'].append(
                "Cross-database replication detected. Schema migration should be completed before data replication."
            )
        
        logger.info(
            f"Cross-database validation summary: {source_config.engine_type} -> {target_config.engine_type}, "
            f"Compatible: {validation_summary['overall_compatible']}"
        )
        
        return validation_summary