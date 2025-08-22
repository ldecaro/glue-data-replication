"""
Iceberg Schema Manager

This module provides the IcebergSchemaManager class for managing
Iceberg table schema operations, including JDBC metadata conversion,
data type mapping, and schema validation.
"""

import logging
from typing import Dict, List, Any, Optional, Union

# Conditional imports for PySpark (only available in Glue runtime)
try:
    from pyspark.sql.types import (
        StructType, StructField, StringType, IntegerType, LongType,
        FloatType, DoubleType, BooleanType, DateType, TimestampType,
        BinaryType, DecimalType, DataType
    )
    from pyspark.sql import DataFrame
    # Mock ResultSetMetaData for type hints (not available in PySpark)
    class ResultSetMetaData:
        def getColumnCount(self) -> int: pass
        def getColumnName(self, column: int) -> str: pass
        def getColumnTypeName(self, column: int) -> str: pass
        def getPrecision(self, column: int) -> int: pass
        def getScale(self, column: int) -> int: pass
        def isNullable(self, column: int) -> int: pass
except ImportError:
    # Mock classes for local development/testing
    class StructType:
        def __init__(self, fields=None):
            self.fields = fields or []

    class StructField:
        def __init__(self, name, dataType, nullable=True):
            self.name = name
            self.dataType = dataType
            self.nullable = nullable

    class DataType:
        pass

    class StringType(DataType):
        pass

    class IntegerType(DataType):
        pass

    class LongType(DataType):
        pass

    class FloatType(DataType):
        pass

    class DoubleType(DataType):
        pass

    class BooleanType(DataType):
        pass

    class DateType(DataType):
        pass

    class TimestampType(DataType):
        pass

    class BinaryType(DataType):
        pass

    class DecimalType(DataType):
        def __init__(self, precision=10, scale=0):
            self.precision = precision
            self.scale = scale

    class DataFrame:
        pass
    class ResultSetMetaData:
        def getColumnCount(self) -> int: pass
        def getColumnName(self, column: int) -> str: pass
        def getColumnTypeName(self, column: int) -> str: pass
        def getPrecision(self, column: int) -> int: pass
        def getScale(self, column: int) -> int: pass
        def isNullable(self, column: int) -> int: pass

from .iceberg_models import (
    IcebergSchema, IcebergSchemaField, IcebergTableMetadata,
    IcebergSchemaCreationError, IcebergValidationError,
    JDBC_TO_ICEBERG_TYPE_MAPPING, DEFAULT_DECIMAL_PRECISION_SCALE,
    map_jdbc_type_to_iceberg, create_iceberg_field
)

logger = logging.getLogger(__name__)


class IcebergSchemaManager:
    """Manages Iceberg table schema creation and validation.

    This class provides comprehensive schema management functionality for
    Iceberg tables, including conversion from JDBC metadata, data type
    mapping, identifier field ID management, and schema validation.
    """

    def __init__(self):
        """Initialize the Iceberg schema manager."""
        self.type_mapping = JDBC_TO_ICEBERG_TYPE_MAPPING.copy()
        logger.info("Initialized IcebergSchemaManager")
    def create_iceberg_schema_from_jdbc(self, jdbc_metadata: ResultSetMetaData,
                                      bookmark_column: Optional[str] = None,
                                      source_engine: str = 'postgresql',
                                      schema_id: int = 0) -> IcebergSchema:
        """Convert JDBC metadata to Iceberg schema with identifier-field-ids.

        Args:
            jdbc_metadata: JDBC ResultSetMetaData from source query
            bookmark_column: Optional bookmark column name for identifier-field-ids
            source_engine: Source database engine type for type mapping defaults
            schema_id: Schema ID for the Iceberg schema

        Returns:
            IcebergSchema: Complete Iceberg schema with fields and identifier-field-ids

        Raises:
            IcebergSchemaCreationError: If schema creation fails
        """
        try:
            logger.info(f"Creating Iceberg schema from JDBC metadata (source: {source_engine})")

            if not jdbc_metadata:
                raise IcebergValidationError(
                    "JDBC metadata cannot be None",
                    field="jdbc_metadata",
                    value=None
                )

            column_count = jdbc_metadata.getColumnCount()
            if column_count == 0:
                raise IcebergValidationError(
                    "JDBC metadata contains no columns",
                    field="column_count",
                    value=0
                )

            fields = []
            bookmark_field_id = None
            # Process each column from JDBC metadata
            for i in range(1, column_count + 1):  # JDBC columns are 1-indexed
                try:
                    # Extract column information
                    column_name = jdbc_metadata.getColumnName(i)
                    column_type = jdbc_metadata.getColumnTypeName(i)
                    precision = None
                    scale = None
                    is_nullable = True

                    # Get precision and scale for numeric types
                    try:
                        precision = jdbc_metadata.getPrecision(i)
                        scale = jdbc_metadata.getScale(i)
                    except Exception as e:
                        logger.debug(f"Could not get precision/scale for column {column_name}: {e}")

                    # Get nullable information
                    try:
                        nullable_value = jdbc_metadata.isNullable(i)
                        # JDBC nullable constants: 0=no nulls, 1=nullable, 2=unknown
                        is_nullable = nullable_value != 0
                    except Exception as e:
                        logger.debug(f"Could not get nullable info for column {column_name}: {e}")

                    # Create Iceberg field
                    field = create_iceberg_field(
                        field_id=i,  # Use 1-based indexing for field IDs
                        name=column_name,
                        jdbc_type=column_type,
                        is_nullable=is_nullable,
                        precision=precision,
                        scale=scale,
                        source_engine=source_engine,
                        doc=f"Converted from {source_engine} {column_type}"
                    )

                    fields.append(field)

                    # Track bookmark column field ID
                    if bookmark_column and column_name.lower() == bookmark_column.lower():
                        bookmark_field_id = field.id
                        logger.debug(f"Found bookmark column '{bookmark_column}' with field ID: {bookmark_field_id}")
                except Exception as e:
                    logger.error(f"Failed to process column {i}: {e}")
                    raise IcebergSchemaCreationError(
                        f"Failed to process column {i}: {str(e)}",
                        database="unknown",
                        table="unknown",
                        source_error=e
                    )

            # Create identifier field IDs list
            identifier_field_ids = []
            if bookmark_field_id is not None:
                identifier_field_ids = [bookmark_field_id]
                logger.info(f"Added bookmark column to identifier-field-ids: {bookmark_column} (ID: {bookmark_field_id})")

            # Create Iceberg schema
            iceberg_schema = IcebergSchema(
                schema_id=schema_id,
                fields=fields,
                identifier_field_ids=identifier_field_ids
            )

            logger.info(f"Successfully created Iceberg schema with {len(fields)} fields")
            if identifier_field_ids:
                logger.info(f"Schema includes identifier-field-ids: {identifier_field_ids}")

            return iceberg_schema
        except (IcebergValidationError, IcebergSchemaCreationError):
            raise
        except Exception as e:
            raise IcebergSchemaCreationError(
                f"Unexpected error creating Iceberg schema: {str(e)}",
                database="unknown",
                table="unknown",
                source_error=e
            )

    def map_jdbc_to_iceberg_types(self, jdbc_type: str, precision: Optional[int] = None,
                                 scale: Optional[int] = None, source_engine: str = 'postgresql') -> str:
        """Map JDBC data types to Iceberg data types.
        
        Args:
            jdbc_type: JDBC data type name
            precision: Optional numeric precision for decimal types
            scale: Optional numeric scale for decimal types
            source_engine: Source database engine for default precision/scale
            
        Returns:
            str: Corresponding Iceberg data type
            
        Raises:
            IcebergValidationError: If JDBC type is not supported
        """
        try:
            logger.debug(f"Mapping JDBC type to Iceberg: {jdbc_type} (engine: {source_engine})")
            
            if not jdbc_type or not jdbc_type.strip():
                raise IcebergValidationError(
                    "JDBC type cannot be empty",
                    field="jdbc_type",
                    value=jdbc_type
                )
            
            # Use the utility function from iceberg_models
            iceberg_type = map_jdbc_type_to_iceberg(
                jdbc_type=jdbc_type,
                precision=precision,
                scale=scale,
                source_engine=source_engine
            )
            
            logger.debug(f"Mapped {jdbc_type} -> {iceberg_type}")
            return iceberg_type
            
        except IcebergValidationError:
            raise
        except Exception as e:
            raise IcebergValidationError(
                f"Failed to map JDBC type {jdbc_type}: {str(e)}",
                field="jdbc_type",
                value=jdbc_type
            )
    
    def add_identifier_field_ids(self, schema: Union[IcebergSchema, Dict[str, Any]],
                                bookmark_column: str) -> Union[IcebergSchema, Dict[str, Any]]:
        """Add identifier-field-ids to schema for bookmark management.
        
        Args:
            schema: Iceberg schema (IcebergSchema object or dictionary)
            bookmark_column: Name of the bookmark column
            
        Returns:
            Updated schema with identifier-field-ids
            
        Raises:
            IcebergValidationError: If bookmark column is not found or schema is invalid
        """
        try:
            logger.info(f"Adding identifier-field-ids for bookmark column: {bookmark_column}")
            
            if not bookmark_column or not bookmark_column.strip():
                raise IcebergValidationError(
                    "Bookmark column name cannot be empty",
                    field="bookmark_column",
                    value=bookmark_column
                )
            
            # Handle IcebergSchema object
            if isinstance(schema, IcebergSchema):
                return self._add_identifier_field_ids_to_schema_object(schema, bookmark_column)
            
            # Handle dictionary schema
            elif isinstance(schema, dict):
                return self._add_identifier_field_ids_to_schema_dict(schema, bookmark_column)
            
            else:
                raise IcebergValidationError(
                    f"Schema must be IcebergSchema object or dictionary, got: {type(schema)}",
                    field="schema_type",
                    value=str(type(schema))
                )
                
        except IcebergValidationError:
            raise
        except Exception as e:
            raise IcebergValidationError(
                f"Failed to add identifier-field-ids: {str(e)}",
                field="bookmark_column",
                value=bookmark_column
            )
    
    def _add_identifier_field_ids_to_schema_object(self, schema: IcebergSchema,
                                                  bookmark_column: str) -> IcebergSchema:
        """Add identifier-field-ids to IcebergSchema object."""
        bookmark_field = schema.get_field_by_name(bookmark_column)
        if not bookmark_field:
            available_fields = [field.name for field in schema.fields]
            raise IcebergValidationError(
                f"Bookmark column '{bookmark_column}' not found in schema. "
                f"Available fields: {available_fields}",
                field="bookmark_column",
                value=bookmark_column
            )
        
        # Update identifier field IDs
        if bookmark_field.id not in (schema.identifier_field_ids or []):
            if schema.identifier_field_ids is None:
                schema.identifier_field_ids = []
            schema.identifier_field_ids.append(bookmark_field.id)
            logger.info(f"Added field ID {bookmark_field.id} to identifier-field-ids for column '{bookmark_column}'")
        else:
            logger.debug(f"Field ID {bookmark_field.id} already in identifier-field-ids")
        
        return schema
    
    def _add_identifier_field_ids_to_schema_dict(self, schema: Dict[str, Any],
                                                bookmark_column: str) -> Dict[str, Any]:
        """Add identifier-field-ids to schema dictionary."""
        fields = schema.get('fields', [])
        if not fields:
            raise IcebergValidationError(
                "Schema dictionary must contain 'fields' list",
                field="schema_fields",
                value=fields
            )
        
        # Find bookmark field
        bookmark_field_id = None
        for field in fields:
            if isinstance(field, dict) and field.get('name', '').lower() == bookmark_column.lower():
                bookmark_field_id = field.get('id')
                break
        
        if bookmark_field_id is None:
            available_fields = [field.get('name', 'unnamed') for field in fields if isinstance(field, dict)]
            raise IcebergValidationError(
                f"Bookmark column '{bookmark_column}' not found in schema. "
                f"Available fields: {available_fields}",
                field="bookmark_column",
                value=bookmark_column
            )
        
        # Update identifier field IDs
        identifier_field_ids = schema.get('identifier-field-ids', [])
        if bookmark_field_id not in identifier_field_ids:
            identifier_field_ids.append(bookmark_field_id)
            schema['identifier-field-ids'] = identifier_field_ids
            logger.info(f"Added field ID {bookmark_field_id} to identifier-field-ids for column '{bookmark_column}'")
        else:
            logger.debug(f"Field ID {bookmark_field_id} already in identifier-field-ids")
        
        return schema
    
    def validate_iceberg_schema(self, schema: Union[IcebergSchema, Dict[str, Any]],
                               table_name: Optional[str] = None) -> Dict[str, Any]:
        """Validate Iceberg schema for correctness and completeness.
        
        Args:
            schema: Iceberg schema to validate (IcebergSchema object or dictionary)
            table_name: Optional table name for error reporting
            
        Returns:
            Dict containing validation results with 'is_valid', 'errors', and 'warnings'
            
        Raises:
            IcebergValidationError: If schema format is invalid
        """
        try:
            table_name = table_name or "unknown"
            logger.info(f"Validating Iceberg schema for table: {table_name}")
            
            validation_result = {
                'is_valid': True,
                'errors': [],
                'warnings': [],
                'field_count': 0,
                'has_identifier_field_ids': False,
                'supported_types': [],
                'unsupported_types': []
            }
            
            # Convert to dictionary format for uniform processing
            if isinstance(schema, IcebergSchema):
                schema_dict = schema.to_dict()
            elif isinstance(schema, dict):
                schema_dict = schema
            else:
                raise IcebergValidationError(
                    f"Schema must be IcebergSchema object or dictionary, got: {type(schema)}",
                    field="schema_type",
                    value=str(type(schema))
                )
            
            # Validate schema structure
            if 'fields' not in schema_dict:
                validation_result['errors'].append("Schema must contain 'fields' list")
                validation_result['is_valid'] = False
                return validation_result
            
            fields = schema_dict['fields']
            if not isinstance(fields, list):
                validation_result['errors'].append("Schema 'fields' must be a list")
                validation_result['is_valid'] = False
                return validation_result
            
            if len(fields) == 0:
                validation_result['errors'].append("Schema must contain at least one field")
                validation_result['is_valid'] = False
                return validation_result
            
            validation_result['field_count'] = len(fields)
            
            # Validate individual fields
            field_ids = set()
            field_names = set()
            
            for i, field in enumerate(fields):
                if not isinstance(field, dict):
                    validation_result['errors'].append(f"Field {i} must be a dictionary")
                    validation_result['is_valid'] = False
                    continue
                
                # Validate required field properties
                field_id = field.get('id')
                field_name = field.get('name')
                field_type = field.get('type')
                
                if field_id is None:
                    validation_result['errors'].append(f"Field {i} missing required 'id' property")
                    validation_result['is_valid'] = False
                
                if not field_name:
                    validation_result['errors'].append(f"Field {i} missing required 'name' property")
                    validation_result['is_valid'] = False
                
                if not field_type:
                    validation_result['errors'].append(f"Field {i} missing required 'type' property")
                    validation_result['is_valid'] = False
                
                # Check for duplicate field IDs
                if field_id is not None:
                    if field_id in field_ids:
                        validation_result['errors'].append(f"Duplicate field ID: {field_id}")
                        validation_result['is_valid'] = False
                    else:
                        field_ids.add(field_id)
                
                # Check for duplicate field names
                if field_name:
                    if field_name.lower() in field_names:
                        validation_result['errors'].append(f"Duplicate field name: {field_name}")
                        validation_result['is_valid'] = False
                    else:
                        field_names.add(field_name.lower())
                
                # Validate field type
                if field_type:
                    if self._is_supported_iceberg_type(field_type):
                        validation_result['supported_types'].append(field_type)
                    else:
                        validation_result['unsupported_types'].append(field_type)
                        validation_result['warnings'].append(
                            f"Field '{field_name}' has potentially unsupported type: {field_type}"
                        )
            
            # Validate identifier field IDs
            identifier_field_ids = schema_dict.get('identifier-field-ids', [])
            if identifier_field_ids:
                validation_result['has_identifier_field_ids'] = True
                
                if not isinstance(identifier_field_ids, list):
                    validation_result['errors'].append("'identifier-field-ids' must be a list")
                    validation_result['is_valid'] = False
                else:
                    # Check that all identifier field IDs exist in schema
                    for field_id in identifier_field_ids:
                        if field_id not in field_ids:
                            validation_result['errors'].append(
                                f"identifier-field-id {field_id} not found in schema fields"
                            )
                            validation_result['is_valid'] = False
            
            # Generate summary
            if validation_result['is_valid']:
                logger.info(f"Schema validation passed for table {table_name}: "
                          f"{validation_result['field_count']} fields, "
                          f"identifier-field-ids: {validation_result['has_identifier_field_ids']}")
            else:
                logger.error(f"Schema validation failed for table {table_name}: "
                           f"{len(validation_result['errors'])} errors")
            
            if validation_result['warnings']:
                logger.warning(f"Schema validation warnings for table {table_name}: "
                             f"{len(validation_result['warnings'])} warnings")
            
            return validation_result
            
        except IcebergValidationError:
            raise
        except Exception as e:
            raise IcebergValidationError(
                f"Schema validation failed: {str(e)}",
                field="schema",
                value=str(schema)
            )
    
    def _is_supported_iceberg_type(self, iceberg_type: str) -> bool:
        """Check if an Iceberg type is supported."""
        if not iceberg_type:
            return False
        
        # Basic types
        basic_types = {
            'boolean', 'int', 'long', 'float', 'double', 'string',
            'binary', 'date', 'time', 'timestamp', 'timestamptz', 'uuid'
        }
        
        type_lower = iceberg_type.lower().strip()
        
        # Check basic types
        if type_lower in basic_types:
            return True
        
        # Check decimal types (decimal(precision,scale))
        if type_lower.startswith('decimal(') and type_lower.endswith(')'):
            return True
        
        # Check fixed types (fixed[length])
        if type_lower.startswith('fixed[') and type_lower.endswith(']'):
            return True
        
        return False
    
    def get_table_schema(self, table_metadata: IcebergTableMetadata) -> IcebergSchema:
        """Retrieve existing Iceberg table schema from metadata.
        
        Args:
            table_metadata: Iceberg table metadata containing schema information
            
        Returns:
            IcebergSchema: Parsed Iceberg schema object
            
        Raises:
            IcebergValidationError: If schema parsing fails
        """
        try:
            logger.info(f"Retrieving schema for Iceberg table: {table_metadata.database}.{table_metadata.table}")
            
            if not table_metadata.schema:
                raise IcebergValidationError(
                    "Table metadata does not contain schema information",
                    field="schema",
                    value=None
                )
            
            schema_dict = table_metadata.schema
            
            # Extract fields from schema
            fields_data = schema_dict.get('fields', [])
            if not fields_data:
                raise IcebergValidationError(
                    "Schema does not contain fields information",
                    field="fields",
                    value=fields_data
                )
            
            # Convert to IcebergSchemaField objects
            fields = []
            for field_data in fields_data:
                if isinstance(field_data, dict):
                    # Handle Glue catalog format
                    field = IcebergSchemaField(
                        id=len(fields) + 1,  # Generate ID if not present
                        name=field_data.get('name', ''),
                        type=field_data.get('type', ''),
                        required=True,  # Default to required
                        doc=field_data.get('comment', '')
                    )
                else:
                    # Handle other formats
                    logger.warning(f"Unexpected field format: {field_data}")
                    continue
                
                fields.append(field)
            
            # Create schema object
            iceberg_schema = IcebergSchema(
                schema_id=0,  # Default schema ID
                fields=fields,
                identifier_field_ids=table_metadata.identifier_field_ids
            )
            
            # Set bookmark column if available
            if table_metadata.bookmark_column:
                bookmark_field = iceberg_schema.get_field_by_name(table_metadata.bookmark_column)
                if bookmark_field and bookmark_field.id not in (iceberg_schema.identifier_field_ids or []):
                    if iceberg_schema.identifier_field_ids is None:
                        iceberg_schema.identifier_field_ids = []
                    iceberg_schema.identifier_field_ids.append(bookmark_field.id)
            
            logger.info(f"Successfully retrieved schema with {len(fields)} fields")
            return iceberg_schema
            
        except IcebergValidationError:
            raise
        except Exception as e:
            raise IcebergValidationError(
                f"Failed to retrieve table schema: {str(e)}",
                field="table_metadata",
                value=str(table_metadata)
            )
    
    def create_spark_schema_from_iceberg(self, iceberg_schema: IcebergSchema) -> StructType:
        """Convert Iceberg schema to Spark StructType.
        
        Args:
            iceberg_schema: Iceberg schema to convert
            
        Returns:
            StructType: Spark schema for DataFrame operations
            
        Raises:
            IcebergValidationError: If conversion fails
        """
        try:
            logger.debug("Converting Iceberg schema to Spark StructType")
            
            spark_fields = []
            
            for field in iceberg_schema.fields:
                spark_type = self._iceberg_type_to_spark_type(field.type)
                spark_field = StructField(
                    name=field.name,
                    dataType=spark_type,
                    nullable=not field.required
                )
                spark_fields.append(spark_field)
            
            spark_schema = StructType(spark_fields)
            logger.debug(f"Successfully converted to Spark schema with {len(spark_fields)} fields")
            
            return spark_schema
            
        except Exception as e:
            raise IcebergValidationError(
                f"Failed to convert Iceberg schema to Spark schema: {str(e)}",
                field="iceberg_schema",
                value=str(iceberg_schema)
            )
    
    def _iceberg_type_to_spark_type(self, iceberg_type: str) -> DataType:
        """Convert Iceberg data type to Spark DataType."""
        type_lower = iceberg_type.lower().strip()
        
        # Basic type mappings
        type_mappings = {
            'boolean': BooleanType(),
            'int': IntegerType(),
            'long': LongType(),
            'float': FloatType(),
            'double': DoubleType(),
            'string': StringType(),
            'binary': BinaryType(),
            'date': DateType(),
            'timestamp': TimestampType(),
            'timestamptz': TimestampType(),
            'time': StringType(),  # Spark doesn't have native time type
            'uuid': StringType()
        }
        
        if type_lower in type_mappings:
            return type_mappings[type_lower]
        
        # Handle decimal types
        if type_lower.startswith('decimal(') and type_lower.endswith(')'):
            try:
                # Extract precision and scale from decimal(precision,scale)
                params = type_lower[8:-1]  # Remove 'decimal(' and ')'
                if ',' in params:
                    precision_str, scale_str = params.split(',', 1)
                    precision = int(precision_str.strip())
                    scale = int(scale_str.strip())
                else:
                    precision = int(params.strip())
                    scale = 0
                
                return DecimalType(precision, scale)
            except (ValueError, IndexError):
                logger.warning(f"Invalid decimal type format: {iceberg_type}, using default decimal")
                return DecimalType(10, 0)
        
        # Default to string for unknown types
        logger.warning(f"Unknown Iceberg type: {iceberg_type}, using StringType")
        return StringType()