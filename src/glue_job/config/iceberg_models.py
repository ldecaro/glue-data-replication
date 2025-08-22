"""
Iceberg-specific configuration models and data structures

This module provides dataclasses, type mappings, and exception classes
specifically for Apache Iceberg table operations.
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any
from enum import Enum


class IcebergFormatVersion(Enum):
    """Supported Iceberg format versions."""
    V1 = "1"
    V2 = "2"


class IcebergOperation(Enum):
    """Supported Iceberg operations."""
    READ = "read"
    WRITE = "write"
    CREATE = "create"
    APPEND = "append"


@dataclass
class IcebergConfig:
    """Configuration for Iceberg database engine.

    This dataclass contains all the necessary configuration parameters
    for connecting to and operating on Iceberg tables through the
    AWS Glue Data Catalog.
    """
    database_name: str
    table_name: str
    warehouse_location: str
    catalog_id: Optional[str] = None  # AWS account ID for cross-account catalog access (same region)
    format_version: str = "2"
    enable_update_catalog: bool = True
    update_behavior: str = "UPDATE_IN_DATABASE"
    catalog_name: str = "glue_catalog"

    def __post_init__(self):
        """Validate configuration after initialization."""
        if not self.database_name or not self.database_name.strip():
            raise IcebergEngineError(
                "database_name cannot be empty",
                error_code="MISSING_DATABASE_NAME"
            )
        if not self.table_name or not self.table_name.strip():
            raise IcebergEngineError(
                "table_name cannot be empty", 
                error_code="MISSING_TABLE_NAME"
            )
        if not self.warehouse_location or not self.warehouse_location.strip():
            raise IcebergEngineError(
                "Warehouse location is required for Iceberg target engine",
                error_code="MISSING_WAREHOUSE_LOCATION"
            )
        if not self.warehouse_location.startswith('s3://'):
            raise IcebergEngineError(
                "warehouse_location must be a valid S3 URL",
                error_code="INVALID_WAREHOUSE_LOCATION"
            )
        if self.format_version not in ["1", "2"]:
            raise IcebergEngineError(
                "format_version must be '1' or '2'",
                error_code="INVALID_FORMAT_VERSION"
            )
        if self.catalog_id and (not self.catalog_id.isdigit() or len(self.catalog_id) != 12):
            raise IcebergEngineError(
                "catalog_id must be a 12-digit AWS account ID",
                error_code="INVALID_CATALOG_ID"
            )


@dataclass
class IcebergTableMetadata:
    """Metadata for Iceberg table operations.

    This dataclass contains metadata information about an Iceberg table,
    including schema, partitioning, and bookmark information.
    """
    database: str
    table: str
    location: str
    schema: Dict[str, Any]
    identifier_field_ids: Optional[List[int]] = None
    bookmark_column: Optional[str] = None
    partition_spec: Optional[Dict[str, Any]] = None
    table_properties: Optional[Dict[str, str]] = None
    created_at: Optional[str] = None
    last_updated: Optional[str] = None

    def __post_init__(self):
        """Initialize default values after creation."""
        if self.table_properties is None:
            self.table_properties = {}
        if self.partition_spec is None:
            self.partition_spec = {}


@dataclass
class IcebergSchemaField:
    """Represents a field in an Iceberg table schema.

    This dataclass represents a single field/column in an Iceberg table
    schema with all necessary metadata for type mapping and validation.
    """
    id: int
    name: str
    type: str
    required: bool = True
    doc: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert field to dictionary representation."""
        field_dict = {
            "id": self.id,
            "name": self.name,
            "type": self.type,
            "required": self.required
        }
        if self.doc:
            field_dict["doc"] = self.doc
        return field_dict


@dataclass
class IcebergSchema:
    """Represents an Iceberg table schema.

    This dataclass contains the complete schema definition for an
    Iceberg table including all fields and metadata.
    """
    schema_id: int
    fields: List[IcebergSchemaField]
    identifier_field_ids: Optional[List[int]] = None

    def __post_init__(self):
        """Initialize default values after creation."""
        if self.identifier_field_ids is None:
            self.identifier_field_ids = []

    def to_dict(self) -> Dict[str, Any]:
        """Convert schema to dictionary representation."""
        return {
            "schema-id": self.schema_id,
            "fields": [field.to_dict() for field in self.fields],
            "identifier-field-ids": self.identifier_field_ids or []
        }

    def get_field_by_name(self, name: str) -> Optional[IcebergSchemaField]:
        """Get field by name."""
        for field in self.fields:
            if field.name == name:
                return field
        return None

    def get_field_by_id(self, field_id: int) -> Optional[IcebergSchemaField]:
        """Get field by ID."""
        for field in self.fields:
            if field.id == field_id:
                return field
        return None


# JDBC to Iceberg type mapping
JDBC_TO_ICEBERG_TYPE_MAPPING: Dict[str, str] = {
    # String types
    'VARCHAR': 'string',
    'CHAR': 'string',
    'TEXT': 'string',
    'CLOB': 'string',
    'NVARCHAR': 'string',
    'NCHAR': 'string',
    'NTEXT': 'string',
    'NCLOB': 'string',

    # Integer types
    'TINYINT': 'int',
    'SMALLINT': 'int',
    'INTEGER': 'int',
    'INT': 'int',
    'BIGINT': 'long',

    # Decimal/Numeric types
    'DECIMAL': 'decimal({precision},{scale})',
    'NUMERIC': 'decimal({precision},{scale})',
    'NUMBER': 'decimal({precision},{scale})',

    # Floating point types
    'FLOAT': 'float',
    'REAL': 'float',
    'DOUBLE': 'double',
    'DOUBLE PRECISION': 'double',

    # Boolean type
    'BOOLEAN': 'boolean',
    'BIT': 'boolean',

    # Date/Time types
    'DATE': 'date',
    'TIMESTAMP': 'timestamp',
    'TIMESTAMP WITH TIME ZONE': 'timestamptz',
    'TIMESTAMP WITHOUT TIME ZONE': 'timestamp',
    'TIME': 'time',
    'TIME WITH TIME ZONE': 'time',
    'TIME WITHOUT TIME ZONE': 'time',

    # Binary types
    'BINARY': 'binary',
    'VARBINARY': 'binary',
    'BLOB': 'binary',
    'LONGVARBINARY': 'binary',

    # UUID type
    'UUID': 'uuid'
}

# Reverse mapping for Iceberg to JDBC types (for reference)
ICEBERG_TO_JDBC_TYPE_MAPPING: Dict[str, str] = {
    'string': 'VARCHAR',
    'int': 'INTEGER',
    'long': 'BIGINT',
    'float': 'FLOAT',
    'double': 'DOUBLE',
    'boolean': 'BOOLEAN',
    'date': 'DATE',
    'timestamp': 'TIMESTAMP',
    'timestamptz': 'TIMESTAMP WITH TIME ZONE',
    'time': 'TIME',
    'binary': 'VARBINARY',
    'uuid': 'UUID'
}

# Default precision and scale for decimal types by database engine
DEFAULT_DECIMAL_PRECISION_SCALE: Dict[str, Dict[str, int]] = {
    'oracle': {'precision': 38, 'scale': 0},
    'sqlserver': {'precision': 18, 'scale': 0},
    'postgresql': {'precision': 28, 'scale': 6},
    'db2': {'precision': 31, 'scale': 0}
}


class IcebergEngineError(Exception):
    """Base exception for Iceberg engine operations.

    This is the base exception class for all Iceberg-related errors.
    It provides a consistent error handling interface for Iceberg operations.
    """

    def __init__(self, message: str, error_code: Optional[str] = None,
                 context: Optional[Dict[str, Any]] = None):
        super().__init__(message)
        self.message = message
        self.error_code = error_code
        self.context = context or {}

    def __str__(self) -> str:
        if self.error_code:
            return f"[{self.error_code}] {self.message}"
        return self.message


class IcebergTableNotFoundError(IcebergEngineError):
    """Raised when Iceberg table is not found in catalog.

    This exception is raised when attempting to access an Iceberg table
    that doesn't exist in the Glue Data Catalog.
    """

    def __init__(self, database: str, table: str, catalog_id: Optional[str] = None):
        self.database = database
        self.table = table
        self.catalog_id = catalog_id

        message = f"Iceberg table '{database}.{table}' not found in catalog"
        if catalog_id:
            message += f" (catalog_id: {catalog_id})"

        super().__init__(
            message=message,
            error_code="TABLE_NOT_FOUND",
            context={
                "database": database,
                "table": table,
                "catalog_id": catalog_id
            }
        )


class IcebergSchemaCreationError(IcebergEngineError):
    """Raised when Iceberg table schema creation fails.

    This exception is raised when there's an error creating an Iceberg
    table schema from source metadata or during schema validation.
    """

    def __init__(self, message: str, database: str, table: str,
                 source_error: Optional[Exception] = None):
        self.database = database
        self.table = table
        self.source_error = source_error

        full_message = f"Failed to create schema for Iceberg table '{database}.{table}': {message}"
        if source_error:
            full_message += f" (caused by: {str(source_error)})"

        super().__init__(
            message=full_message,
            error_code="SCHEMA_CREATION_ERROR",
            context={
                "database": database,
                "table": table,
                "source_error": str(source_error) if source_error else None
            }
        )


class IcebergCatalogError(IcebergEngineError):
    """Raised when Glue Data Catalog operations fail.

    This exception is raised when there are errors interacting with
    the AWS Glue Data Catalog for Iceberg table operations.
    """

    def __init__(self, message: str, operation: str, catalog_id: Optional[str] = None,
                 aws_error: Optional[Exception] = None):
        self.operation = operation
        self.catalog_id = catalog_id
        self.aws_error = aws_error

        full_message = f"Glue Data Catalog operation '{operation}' failed: {message}"
        if catalog_id:
            full_message += f" (catalog_id: {catalog_id})"
        if aws_error:
            full_message += f" (AWS error: {str(aws_error)})"

        super().__init__(
            message=full_message,
            error_code="CATALOG_ERROR",
            context={
                "operation": operation,
                "catalog_id": catalog_id,
                "aws_error": str(aws_error) if aws_error else None
            }
        )


class IcebergConnectionError(IcebergEngineError):
    """Raised when Iceberg connection operations fail.

    This exception is raised when there are errors establishing or
    maintaining connections to Iceberg tables through Spark.
    """

    def __init__(self, message: str, warehouse_location: Optional[str] = None,
                 spark_error: Optional[Exception] = None):
        self.warehouse_location = warehouse_location
        self.spark_error = spark_error

        full_message = f"Iceberg connection error: {message}"
        if warehouse_location:
            full_message += f" (warehouse: {warehouse_location})"
        if spark_error:
            full_message += f" (Spark error: {str(spark_error)})"

        super().__init__(
            message=full_message,
            error_code="CONNECTION_ERROR",
            context={
                "warehouse_location": warehouse_location,
                "spark_error": str(spark_error) if spark_error else None
            }
        )


class IcebergValidationError(IcebergEngineError):
    """Raised when Iceberg configuration or data validation fails.

    This exception is raised when there are validation errors in
    Iceberg configuration parameters or data operations.
    """

    def __init__(self, message: str, field: Optional[str] = None,
                 value: Optional[Any] = None):
        self.field = field
        self.value = value

        full_message = f"Iceberg validation error: {message}"
        if field:
            full_message += f" (field: {field})"
        if value is not None:
            full_message += f" (value: {value})"

        super().__init__(
            message=full_message,
            error_code="VALIDATION_ERROR",
            context={
                "field": field,
                "value": str(value) if value is not None else None
            }
        )


def map_jdbc_type_to_iceberg(jdbc_type: str, precision: Optional[int] = None,
                           scale: Optional[int] = None,
                           source_engine: str = 'postgresql') -> str:
    """Map JDBC data type to Iceberg data type.

    Args:
        jdbc_type: JDBC data type name
        precision: Numeric precision (for decimal types)
        scale: Numeric scale (for decimal types)
        source_engine: Source database engine type for default precision/scale

    Returns:
        str: Corresponding Iceberg data type

    Raises:
        IcebergValidationError: If JDBC type is not supported
    """
    jdbc_type_upper = jdbc_type.upper().strip()
    # Handle decimal types with precision and scale
    if jdbc_type_upper in ['DECIMAL', 'NUMERIC', 'NUMBER']:
        if precision is None or scale is None:
            # Use default precision and scale based on source engine
            defaults = DEFAULT_DECIMAL_PRECISION_SCALE.get(source_engine,
                                                         DEFAULT_DECIMAL_PRECISION_SCALE['postgresql'])
            precision = precision or defaults['precision']
            scale = scale or defaults['scale']

        return f"decimal({precision},{scale})"

    # Handle other types
    iceberg_type = JDBC_TO_ICEBERG_TYPE_MAPPING.get(jdbc_type_upper)
    if not iceberg_type:
        raise IcebergValidationError(
            f"Unsupported JDBC type for Iceberg mapping: {jdbc_type}",
            field="jdbc_type",
            value=jdbc_type
        )

    return iceberg_type


def create_iceberg_field(field_id: int, name: str, jdbc_type: str,
                        is_nullable: bool = True, precision: Optional[int] = None,
                        scale: Optional[int] = None, source_engine: str = 'postgresql',
                        doc: Optional[str] = None) -> IcebergSchemaField:
    """Create an Iceberg schema field from JDBC metadata.

    Args:
        field_id: Unique field identifier
        name: Field name
        jdbc_type: JDBC data type
        is_nullable: Whether field can be null
        precision: Numeric precision (for decimal types)
        scale: Numeric scale (for decimal types)
        source_engine: Source database engine type
        doc: Optional field documentation

    Returns:
        IcebergSchemaField: Configured Iceberg schema field
    """
    iceberg_type = map_jdbc_type_to_iceberg(jdbc_type, precision, scale, source_engine)
    return IcebergSchemaField(
        id=field_id,
        name=name,
        type=iceberg_type,
        required=not is_nullable,
        doc=doc
    )