"""
Iceberg Connection Handler

This module provides the IcebergConnectionHandler class for managing
Iceberg table operations through Spark and AWS Glue Data Catalog.
"""

import logging
from typing import Dict, Any, Optional, List
import boto3
from botocore.exceptions import ClientError

# Conditional imports for PySpark and AWS Glue (only available in Glue runtime)
try:
    from pyspark.sql import SparkSession, DataFrame
    from pyspark.sql.types import StructType, StructField
    from awsglue.context import GlueContext
    from awsglue.dynamicframe import DynamicFrame
except ImportError:
    # Mock classes for local development/testing
    class SparkSession:
        def __init__(self):
            self.conf = MockSparkConf()
        
        def sql(self, query: str):
            return MockDataFrame()
    
    class DataFrame:
        def createOrReplaceTempView(self, name: str):
            pass
        
        def writeTo(self, table: str):
            return MockDataFrameWriter()
    
    class StructType:
        def __init__(self, fields=None):
            self.fields = fields or []
    
    class StructField:
        def __init__(self, name, dataType, nullable=True):
            self.name = name
            self.dataType = dataType
            self.nullable = nullable
    
    class GlueContext:
        def create_data_frame_from_catalog(self, database, table_name, additional_options=None):
            return MockDataFrame()
    
    class DynamicFrame:
        pass
    
    class MockSparkConf:
        def set(self, key: str, value: str):
            return self
    
    class MockDataFrame:
        def createOrReplaceTempView(self, name: str):
            pass
        
        def writeTo(self, table: str):
            return MockDataFrameWriter()
    
    class MockDataFrameWriter:
        def tableProperty(self, key: str, value: str):
            return self
        
        def create(self):
            pass
        
        def append(self):
            pass

from .iceberg_models import (
    IcebergConfig, IcebergTableMetadata, IcebergSchema, IcebergSchemaField,
    IcebergEngineError, IcebergTableNotFoundError, IcebergCatalogError,
    IcebergConnectionError, IcebergValidationError, IcebergSchemaCreationError
)

logger = logging.getLogger(__name__)


class IcebergConnectionHandler:
    """Handles Iceberg table operations through Glue Data Catalog and Spark.
    
    This class provides a comprehensive interface for working with Iceberg tables,
    including table creation, reading, writing, and catalog management operations.
    """
    
    def __init__(self, spark_session: SparkSession, glue_context: GlueContext):
        """Initialize the Iceberg connection handler.
        
        Args:
            spark_session: Active Spark session
            glue_context: AWS Glue context for catalog operations
        """
        self.spark = spark_session
        self.glue_context = glue_context
        self.catalog_name = "glue_catalog"
        self.glue_client = boto3.client('glue')
        self._configured_warehouses = set()
        
        logger.info("Initialized IcebergConnectionHandler")
    
    def configure_iceberg_catalog(self, warehouse_location: str, catalog_id: Optional[str] = None) -> None:
        """Configure Spark for Iceberg operations.
        
        Args:
            warehouse_location: S3 location for Iceberg warehouse
            catalog_id: Optional AWS account ID for cross-account access
            
        Raises:
            IcebergConnectionError: If configuration fails
        """
        try:
            warehouse_key = f"{warehouse_location}#{catalog_id or 'default'}"
            if warehouse_key in self._configured_warehouses:
                logger.debug(f"Iceberg catalog already configured for warehouse: {warehouse_location}")
                return
            
            logger.info(f"Configuring Iceberg catalog with warehouse: {warehouse_location}")
            
            # Validate Spark configuration for Iceberg
            self._validate_spark_iceberg_config()
            
            # Set Spark configuration for Iceberg
            spark_conf = self.spark.conf
            
            # Core Iceberg configuration
            # Note: spark.sql.extensions must be set as a Glue job parameter, not dynamically
            # It should be set to: org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
            
            logger.info(f"Setting Iceberg catalog configuration for: {self.catalog_name}")
            
            spark_conf.set(f"spark.sql.catalog.{self.catalog_name}", 
                          "org.apache.iceberg.spark.SparkCatalog")
            logger.info(f"Set catalog class: spark.sql.catalog.{self.catalog_name}=org.apache.iceberg.spark.SparkCatalog")
            
            spark_conf.set(f"spark.sql.catalog.{self.catalog_name}.catalog-impl", 
                          "org.apache.iceberg.aws.glue.GlueCatalog")
            logger.info(f"Set catalog implementation: catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog")
            
            spark_conf.set(f"spark.sql.catalog.{self.catalog_name}.io-impl", 
                          "org.apache.iceberg.aws.s3.S3FileIO")
            logger.info(f"Set IO implementation: io-impl=org.apache.iceberg.aws.s3.S3FileIO")
            
            spark_conf.set(f"spark.sql.catalog.{self.catalog_name}.warehouse", 
                          warehouse_location)
            logger.info(f"Set warehouse location: warehouse={warehouse_location}")
            
            # Set catalog ID for cross-account access if provided
            if catalog_id:
                spark_conf.set(f"spark.sql.catalog.{self.catalog_name}.glue.id", catalog_id)
                logger.info(f"Configured cross-account catalog access for account: {catalog_id}")
            
            # Performance and reliability settings
            # Note: These should be set as Glue job parameters, not dynamically:
            # --conf spark.serializer=org.apache.spark.serializer.KryoSerializer
            # --conf spark.sql.adaptive.enabled=true
            # --conf spark.sql.adaptive.coalescePartitions.enabled=true
            
            # Test catalog configuration by attempting to list databases
            try:
                logger.info(f"Testing Iceberg catalog configuration by listing databases")
                test_sql = f"SHOW DATABASES IN {self.catalog_name}"
                logger.info(f"Executing test SQL: {test_sql}")
                result = self.spark.sql(test_sql)
                databases = [row.databaseName for row in result.collect()]
                logger.info(f"Successfully tested catalog configuration. Found databases: {databases}")
            except Exception as test_error:
                logger.warning(f"Catalog test failed, but continuing: {str(test_error)}")
                # Don't fail the configuration, just log the warning
            
            self._configured_warehouses.add(warehouse_key)
            logger.info("Successfully configured Iceberg catalog")
            
        except Exception as e:
            raise IcebergConnectionError(
                f"Failed to configure Iceberg catalog: {str(e)}",
                warehouse_location=warehouse_location,
                spark_error=e
            )
    
    def _validate_spark_iceberg_config(self) -> None:
        """Validate that Spark is properly configured for Iceberg operations.
        
        Raises:
            IcebergConnectionError: If Spark configuration is invalid
        """
        try:
            spark_conf = self.spark.conf
            
            # Check if Iceberg extensions are configured
            extensions = spark_conf.get("spark.sql.extensions", "")
            if "IcebergSparkSessionExtensions" not in extensions:
                logger.warning(
                    "Iceberg Spark extensions not found in configuration. "
                    "This may cause table creation issues. "
                    "Expected: org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
                )
                # Don't fail here, just warn, as the job might still work
            else:
                logger.info("Iceberg Spark extensions properly configured")
            
            # Log current Spark configuration for debugging
            logger.info(f"Current Spark SQL extensions: {extensions}")
            
            # Check if serializer is set to Kryo (recommended for Iceberg)
            serializer = spark_conf.get("spark.serializer", "")
            if "KryoSerializer" not in serializer:
                logger.warning(f"Spark serializer is not set to KryoSerializer (current: {serializer})")
            
        except Exception as e:
            logger.warning(f"Failed to validate Spark Iceberg configuration: {str(e)}")
            # Don't fail the operation, just log the warning
    
    def _validate_catalog_configuration(self) -> None:
        """Validate that the Iceberg catalog is properly configured and accessible.
        
        Raises:
            IcebergConnectionError: If catalog configuration is invalid
        """
        try:
            logger.info(f"Validating Iceberg catalog configuration for: {self.catalog_name}")
            
            # Check if catalog is configured in Spark
            spark_conf = self.spark.conf
            catalog_class = spark_conf.get(f"spark.sql.catalog.{self.catalog_name}", "")
            
            if not catalog_class:
                raise IcebergConnectionError(
                    f"Iceberg catalog '{self.catalog_name}' is not configured in Spark session. "
                    f"Expected configuration: spark.sql.catalog.{self.catalog_name}=org.apache.iceberg.spark.SparkCatalog"
                )
            
            logger.info(f"Catalog class configured: {catalog_class}")
            
            # Log all catalog-related configurations for debugging
            catalog_configs = {}
            for key in ["catalog-impl", "io-impl", "warehouse", "glue.id"]:
                config_key = f"spark.sql.catalog.{self.catalog_name}.{key}"
                config_value = spark_conf.get(config_key, "")
                catalog_configs[key] = config_value
                logger.info(f"Catalog config {key}: {config_value}")
            
            # Validate essential configurations
            if not catalog_configs.get("catalog-impl"):
                logger.warning(f"Missing catalog-impl configuration for {self.catalog_name}")
            
            if not catalog_configs.get("warehouse"):
                logger.warning(f"Missing warehouse configuration for {self.catalog_name}")
            
            # Test basic catalog functionality
            try:
                logger.info(f"Testing catalog functionality by checking if catalog exists")
                test_sql = f"DESCRIBE CATALOG {self.catalog_name}"
                logger.info(f"Executing catalog test SQL: {test_sql}")
                result = self.spark.sql(test_sql)
                catalog_info = result.collect()
                logger.info(f"Catalog test successful. Catalog info: {[row.asDict() for row in catalog_info]}")
            except Exception as catalog_test_error:
                logger.warning(f"Catalog functionality test failed: {str(catalog_test_error)}")
                logger.warning(f"This may indicate catalog configuration issues")
                # Don't fail here, continue with table creation attempt
            
        except IcebergConnectionError:
            raise
        except Exception as e:
            logger.warning(f"Catalog validation failed with unexpected error: {str(e)}")
            # Don't fail the operation, just log the warning
    
    def _ensure_database_exists(self, database: str, catalog_id: Optional[str] = None) -> None:
        """Ensure the database exists in Glue Data Catalog.
        
        Args:
            database: Database name
            catalog_id: Optional catalog ID for cross-account access
        """
        try:
            logger.info(f"Checking if database exists: {database}")
            
            get_database_kwargs = {
                'Name': database
            }
            
            if catalog_id:
                get_database_kwargs['CatalogId'] = catalog_id
            
            try:
                response = self.glue_client.get_database(**get_database_kwargs)
                logger.info(f"Database {database} exists")
            except ClientError as e:
                error_code = e.response.get('Error', {}).get('Code', '')
                if error_code == 'EntityNotFoundException':
                    logger.info(f"Database {database} does not exist, creating it")
                    
                    create_database_kwargs = {
                        'DatabaseInput': {
                            'Name': database,
                            'Description': f'Database created automatically for Iceberg tables'
                        }
                    }
                    
                    if catalog_id:
                        create_database_kwargs['CatalogId'] = catalog_id
                    
                    self.glue_client.create_database(**create_database_kwargs)
                    logger.info(f"Successfully created database: {database}")
                else:
                    logger.warning(f"Error checking database existence: {str(e)}")
                    
        except Exception as e:
            logger.warning(f"Failed to ensure database exists: {str(e)}")
            # Don't fail the operation, just log the warning
    
    def table_exists(self, database: str, table: str, catalog_id: Optional[str] = None) -> bool:
        """Check if Iceberg table exists in Glue Data Catalog.
        
        Args:
            database: Database name
            table: Table name
            catalog_id: Optional catalog ID for cross-account access
            
        Returns:
            bool: True if table exists, False otherwise
            
        Raises:
            IcebergCatalogError: If catalog access fails
        """
        try:
            logger.info(f"Checking if Iceberg table exists: {database}.{table}")
            
            get_table_kwargs = {
                'DatabaseName': database,
                'Name': table
            }
            
            if catalog_id:
                get_table_kwargs['CatalogId'] = catalog_id
                logger.info(f"Using catalog ID for table check: {catalog_id}")
            
            logger.info(f"Calling Glue get_table with parameters: {get_table_kwargs}")
            response = self.glue_client.get_table(**get_table_kwargs)
            table_info = response.get('Table', {})
            
            # Check if it's an Iceberg table by looking at table properties
            table_properties = table_info.get('Parameters', {})
            table_type = table_properties.get('table_type', '').upper()
            
            logger.info(f"Table {database}.{table} found with type: {table_type}")
            
            if table_type == 'ICEBERG':
                logger.info(f"Iceberg table found: {database}.{table}")
                return True
            else:
                logger.info(f"Table {database}.{table} exists but is not an Iceberg table (type: {table_type})")
                return False
                
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', '')
            logger.info(f"AWS ClientError during table existence check: {error_code}")
            
            if error_code == 'EntityNotFoundException':
                logger.info(f"Iceberg table not found: {database}.{table}")
                return False
            else:
                logger.error(f"Failed to check table existence: {str(e)}")
                # Don't raise an exception here, just return False and let table creation proceed
                logger.warning(f"Assuming table {database}.{table} does not exist due to catalog error")
                return False
                
        except Exception as e:
            logger.error(f"Unexpected error checking table existence: {str(e)}")
            logger.error(f"Exception type: {type(e).__name__}")
            # Don't raise an exception here, just return False and let table creation proceed
            logger.warning(f"Assuming table {database}.{table} does not exist due to unexpected error")
            return False
    
    def read_table(self, database: str, table: str, catalog_id: Optional[str] = None,
                   additional_options: Optional[Dict[str, str]] = None) -> DataFrame:
        """Read data from Iceberg table.
        
        Args:
            database: Database name
            table: Table name
            catalog_id: Optional catalog ID for cross-account access
            additional_options: Optional additional read options
            
        Returns:
            DataFrame: Spark DataFrame containing table data
            
        Raises:
            IcebergTableNotFoundError: If table doesn't exist
            IcebergConnectionError: If read operation fails
        """
        try:
            logger.info(f"Reading Iceberg table: {database}.{table}")
            
            # Check if table exists first
            if not self.table_exists(database, table, catalog_id):
                raise IcebergTableNotFoundError(database, table, catalog_id)
            
            # Use Glue context to read the table
            read_options = additional_options or {}
            
            # Add catalog ID to options if provided
            if catalog_id:
                read_options['catalog_id'] = catalog_id
            
            df = self.glue_context.create_data_frame_from_catalog(
                database=database,
                table_name=table,
                additional_options=read_options
            )
            
            logger.info(f"Successfully read Iceberg table: {database}.{table}")
            return df
            
        except IcebergTableNotFoundError:
            raise
        except Exception as e:
            raise IcebergConnectionError(
                f"Failed to read Iceberg table {database}.{table}: {str(e)}",
                spark_error=e
            )
    
    def write_table(self, dataframe: DataFrame, database: str, table: str,
                   mode: str = "append", iceberg_config: Optional[IcebergConfig] = None) -> None:
        """Write data to Iceberg table.
        
        Args:
            dataframe: Spark DataFrame to write
            database: Database name
            table: Table name
            mode: Write mode ('create', 'append', 'overwrite')
            iceberg_config: Optional Iceberg configuration
            
        Raises:
            IcebergConnectionError: If write operation fails
            IcebergValidationError: If parameters are invalid
        """
        try:
            print(f"=== ICEBERG HANDLER WRITE_TABLE CALLED FOR {database}.{table} ===")
            logger.info(f"Starting write operation to Iceberg table: {database}.{table} (mode: {mode})")
            logger.info(f"DataFrame row count: {dataframe.count()}")
            
            # Validate mode
            valid_modes = ['create', 'append', 'overwrite']
            if mode not in valid_modes:
                raise IcebergValidationError(
                    f"Invalid write mode: {mode}. Must be one of: {valid_modes}",
                    field="mode",
                    value=mode
                )
            
            logger.info(f"Mode validation passed for {database}.{table}")
            
            # Configure catalog if config is provided
            if iceberg_config:
                try:
                    logger.info(f"Configuring Iceberg catalog for {database}.{table}")
                    self.configure_iceberg_catalog(
                        iceberg_config.warehouse_location,
                        iceberg_config.catalog_id
                    )
                    logger.info(f"Successfully configured Iceberg catalog for {database}.{table}")
                except Exception as catalog_error:
                    logger.error(f"Failed to configure Iceberg catalog: {str(catalog_error)}")
                    raise IcebergConnectionError(
                        f"Catalog configuration failed for {database}.{table}: {str(catalog_error)}",
                        warehouse_location=iceberg_config.warehouse_location,
                        spark_error=catalog_error
                    )
            
            # Ensure database exists first
            try:
                logger.info(f"Ensuring database exists: {database}")
                self._ensure_database_exists(database, iceberg_config.catalog_id if iceberg_config else None)
                logger.info(f"Database check completed for: {database}")
            except Exception as db_error:
                logger.error(f"Failed to ensure database exists: {str(db_error)}")
                # Don't fail the operation, just log the error
                logger.warning(f"Proceeding without database validation for {database}")
            
            # Check if table exists and handle table creation for overwrite mode
            try:
                table_exists = self.table_exists(database, table, iceberg_config.catalog_id if iceberg_config else None)
                logger.info(f"Table existence check for {database}.{table}: {table_exists}")
            except Exception as existence_error:
                logger.error(f"Error checking table existence for {database}.{table}: {str(existence_error)}")
                logger.info(f"Assuming table {database}.{table} does not exist and proceeding with creation")
                table_exists = False
            
            if mode == "overwrite" and not table_exists:
                logger.info(f"Table {database}.{table} does not exist, creating it first for overwrite mode")
                try:
                    # Create the table first using the DataFrame schema
                    self._create_table_from_dataframe(dataframe, database, table, iceberg_config)
                    # Verify table was created
                    table_exists = self.table_exists(database, table, iceberg_config.catalog_id if iceberg_config else None)
                    logger.info(f"After creation, table existence for {database}.{table}: {table_exists}")
                    if not table_exists:
                        raise IcebergConnectionError(
                            f"Table creation appeared to succeed but table {database}.{table} still not found",
                            warehouse_location=iceberg_config.warehouse_location if iceberg_config else None
                        )
                except Exception as create_error:
                    logger.error(f"Failed to create table {database}.{table}: {str(create_error)}")
                    raise IcebergConnectionError(
                        f"Table creation failed for {database}.{table}: {str(create_error)}",
                        warehouse_location=iceberg_config.warehouse_location if iceberg_config else None,
                        spark_error=create_error
                    )
            elif mode == "append" and not table_exists:
                logger.info(f"Table {database}.{table} does not exist, creating it first for append mode")
                try:
                    # Create the table first using the DataFrame schema
                    self._create_table_from_dataframe(dataframe, database, table, iceberg_config)
                    # Verify table was created
                    table_exists = self.table_exists(database, table, iceberg_config.catalog_id if iceberg_config else None)
                    logger.info(f"After creation, table existence for {database}.{table}: {table_exists}")
                    if not table_exists:
                        raise IcebergConnectionError(
                            f"Table creation appeared to succeed but table {database}.{table} still not found",
                            warehouse_location=iceberg_config.warehouse_location if iceberg_config else None
                        )
                except Exception as create_error:
                    logger.error(f"Failed to create table {database}.{table}: {str(create_error)}")
                    raise IcebergConnectionError(
                        f"Table creation failed for {database}.{table}: {str(create_error)}",
                        warehouse_location=iceberg_config.warehouse_location if iceberg_config else None,
                        spark_error=create_error
                    )
            
            # Construct full table name
            full_table_name = f"{self.catalog_name}.{database}.{table}"
            
            # Create temporary view for the DataFrame
            temp_view_name = f"temp_{database}_{table}_{mode}"
            dataframe.createOrReplaceTempView(temp_view_name)
            
            # Write using Spark SQL based on mode
            if mode == "create":
                if table_exists:
                    raise IcebergValidationError(
                        f"Table {database}.{table} already exists, cannot create",
                        field="table_exists",
                        value=True
                    )
                # Use writeTo API for table creation
                writer = dataframe.writeTo(full_table_name)
                
                # Set Iceberg-specific properties
                writer.tableProperty("format-version", "2")
                if iceberg_config:
                    writer.tableProperty("write.update.mode", "merge-on-read")
                    writer.tableProperty("write.delete.mode", "merge-on-read")
                
                writer.create()
                
            elif mode == "append":
                # Use INSERT INTO for append operations
                insert_sql = f"INSERT INTO {full_table_name} SELECT * FROM {temp_view_name}"
                self.spark.sql(insert_sql)
                
            elif mode == "overwrite":
                print(f"=== OVERWRITE MODE FOR {full_table_name} ===")
                logger.info(f"Starting overwrite operation for {full_table_name}")
                
                # Check if table exists before attempting SQL operations
                try:
                    table_exists_check = self.table_exists(database, table, iceberg_config.catalog_id if iceberg_config else None)
                    logger.info(f"Table existence check result: {table_exists_check}")
                except Exception as check_error:
                    logger.warning(f"Table existence check failed: {str(check_error)}, assuming table doesn't exist")
                    table_exists_check = False
                
                # If table doesn't exist, create it first using DataFrame operations
                if not table_exists_check:
                    logger.info(f"Table {full_table_name} doesn't exist, creating it using DataFrame operations")
                    try:
                        # Use DataFrame.write.saveAsTable() instead of SQL
                        dataframe.write \
                            .format("iceberg") \
                            .option("path", f"{iceberg_config.warehouse_location if iceberg_config else ''}/{database}/{table}") \
                            .saveAsTable(full_table_name)
                        
                        logger.info(f"Successfully created table {full_table_name} using DataFrame operations")
                        
                    except Exception as df_create_error:
                        logger.error(f"DataFrame table creation failed: {str(df_create_error)}")
                        
                        # Fallback to SQL approach
                        try:
                            logger.info("Falling back to SQL table creation")
                            create_sql = f"""
                            CREATE TABLE {full_table_name}
                            USING iceberg
                            TBLPROPERTIES (
                                'format-version'='2',
                                'write.update.mode'='merge-on-read',
                                'write.delete.mode'='merge-on-read'
                            )
                            AS SELECT * FROM {temp_view_name} LIMIT 0
                            """
                            
                            logger.info(f"Creating table with SQL: {create_sql}")
                            self.spark.sql(create_sql)
                            
                            # Now write the actual data
                            logger.info(f"Writing data to newly created table")
                            overwrite_sql = f"INSERT OVERWRITE {full_table_name} SELECT * FROM {temp_view_name}"
                            self.spark.sql(overwrite_sql)
                            
                        except Exception as sql_fallback_error:
                            logger.error(f"SQL fallback also failed: {str(sql_fallback_error)}")
                            raise IcebergConnectionError(
                                f"Both DataFrame and SQL approaches failed for {full_table_name}: {str(sql_fallback_error)}",
                                spark_error=sql_fallback_error
                            )
                else:
                    # Table exists, use INSERT OVERWRITE
                    logger.info(f"Table {full_table_name} exists, using INSERT OVERWRITE")
                    overwrite_sql = f"INSERT OVERWRITE {full_table_name} SELECT * FROM {temp_view_name}"
                    self.spark.sql(overwrite_sql)
            
            logger.info(f"Successfully wrote to Iceberg table: {database}.{table}")
            
            # Clean up temporary view
            try:
                self.spark.sql(f"DROP VIEW IF EXISTS {temp_view_name}")
            except Exception as cleanup_error:
                logger.warning(f"Failed to clean up temporary view {temp_view_name}: {cleanup_error}")
            
        except (IcebergValidationError, IcebergConnectionError):
            raise
        except Exception as e:
            logger.error(f"Unexpected exception in write_table for {database}.{table}: {str(e)}")
            logger.error(f"Exception type: {type(e).__name__}")
            logger.error(f"Exception occurred at: {e.__traceback__.tb_lineno if e.__traceback__ else 'unknown'}")
            raise IcebergConnectionError(
                f"Failed to write to Iceberg table {database}.{table}: {str(e)}",
                spark_error=e
            )
    
    def read_table_incremental(self, database: str, table: str, 
                              bookmark_column: str, last_processed_value: str,
                              catalog_id: Optional[str] = None) -> DataFrame:
        """Read incremental data from Iceberg table based on bookmark.
        
        Args:
            database: Database name
            table: Table name
            bookmark_column: Column to use for incremental filtering
            last_processed_value: Last processed value for incremental read
            catalog_id: Optional catalog ID for cross-account access
            
        Returns:
            DataFrame: Spark DataFrame containing incremental data
            
        Raises:
            IcebergTableNotFoundError: If table doesn't exist
            IcebergConnectionError: If read operation fails
        """
        try:
            logger.info(f"Reading incremental data from Iceberg table: {database}.{table}")
            logger.info(f"Bookmark column: {bookmark_column}, last value: {last_processed_value}")
            
            # Check if table exists first
            if not self.table_exists(database, table, catalog_id):
                raise IcebergTableNotFoundError(database, table, catalog_id)
            
            # Construct full table name
            full_table_name = f"{self.catalog_name}.{database}.{table}"
            
            # Build incremental query
            incremental_sql = f"""
            SELECT * FROM {full_table_name}
            WHERE {bookmark_column} > '{last_processed_value}'
            ORDER BY {bookmark_column}
            """
            
            logger.debug(f"Executing incremental SQL: {incremental_sql}")
            df = self.spark.sql(incremental_sql)
            
            logger.info(f"Successfully read incremental data from Iceberg table: {database}.{table}")
            return df
            
        except IcebergTableNotFoundError:
            raise
        except Exception as e:
            raise IcebergConnectionError(
                f"Failed to read incremental data from Iceberg table {database}.{table}: {str(e)}",
                spark_error=e
            )
    
    def get_table_metadata(self, database: str, table: str, 
                          catalog_id: Optional[str] = None) -> Dict[str, Any]:
        """Get metadata for an Iceberg table.
        
        Args:
            database: Database name
            table: Table name
            catalog_id: Optional catalog ID for cross-account access
            
        Returns:
            Dict[str, Any]: Table metadata information
            
        Raises:
            IcebergTableNotFoundError: If table doesn't exist
            IcebergCatalogError: If metadata retrieval fails
        """
        try:
            logger.info(f"Getting metadata for Iceberg table: {database}.{table}")
            
            # Get table from Glue catalog
            get_table_params = {
                'DatabaseName': database,
                'Name': table
            }
            
            if catalog_id:
                get_table_params['CatalogId'] = catalog_id
            
            response = self.glue_client.get_table(**get_table_params)
            table_info = response['Table']
            
            # Extract Iceberg-specific metadata
            metadata = {
                'database': database,
                'table': table,
                'location': table_info.get('StorageDescriptor', {}).get('Location', ''),
                'parameters': table_info.get('Parameters', {}),
                'columns': table_info.get('StorageDescriptor', {}).get('Columns', []),
                'partition_keys': table_info.get('PartitionKeys', [])
            }
            
            logger.info(f"Successfully retrieved metadata for Iceberg table: {database}.{table}")
            return metadata
            
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == 'EntityNotFoundException':
                raise IcebergTableNotFoundError(database, table, catalog_id)
            else:
                raise IcebergCatalogError(
                    f"Failed to get table metadata: {str(e)}",
                    operation="GetTable",
                    catalog_id=catalog_id,
                    aws_error=e
                )
        except Exception as e:
            raise IcebergCatalogError(
                f"Unexpected error getting table metadata: {str(e)}",
                operation="GetTable",
                catalog_id=catalog_id,
                aws_error=e
            )
        try:
            logger.debug(f"Getting metadata for Iceberg table: {database}.{table}")
            
            get_table_kwargs = {
                'DatabaseName': database,
                'Name': table
            }
            
            if catalog_id:
                get_table_kwargs['CatalogId'] = catalog_id
            
            response = self.glue_client.get_table(**get_table_kwargs)
            table_info = response.get('Table', {})
            
            # Verify it's an Iceberg table
            table_properties = table_info.get('Parameters', {})
            table_type = table_properties.get('table_type', '').upper()
            
            if table_type != 'ICEBERG':
                raise IcebergValidationError(
                    f"Table {database}.{table} is not an Iceberg table (type: {table_type})",
                    field="table_type",
                    value=table_type
                )
            
            # Extract metadata
            storage_descriptor = table_info.get('StorageDescriptor', {})
            location = storage_descriptor.get('Location', '')
            
            # Extract schema information
            columns = storage_descriptor.get('Columns', [])
            schema_dict = {
                'fields': [
                    {
                        'name': col.get('Name', ''),
                        'type': col.get('Type', ''),
                        'comment': col.get('Comment', '')
                    }
                    for col in columns
                ]
            }
            
            # Extract identifier field IDs if available
            identifier_field_ids = None
            if 'identifier-field-ids' in table_properties:
                try:
                    identifier_field_ids = [
                        int(x.strip()) for x in table_properties['identifier-field-ids'].split(',')
                        if x.strip().isdigit()
                    ]
                except (ValueError, AttributeError):
                    logger.warning(f"Failed to parse identifier-field-ids for table {database}.{table}")
            
            # Create metadata object
            metadata = IcebergTableMetadata(
                database=database,
                table=table,
                location=location,
                schema=schema_dict,
                identifier_field_ids=identifier_field_ids,
                table_properties=table_properties,
                created_at=table_info.get('CreateTime', ''),
                last_updated=table_info.get('UpdateTime', '')
            )
            
            logger.debug(f"Successfully retrieved metadata for Iceberg table: {database}.{table}")
            return metadata
            
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', '')
            if error_code == 'EntityNotFoundException':
                raise IcebergTableNotFoundError(database, table, catalog_id)
            else:
                raise IcebergCatalogError(
                    f"Failed to get table metadata: {str(e)}",
                    operation="GetTable",
                    catalog_id=catalog_id,
                    aws_error=e
                )
        except (IcebergTableNotFoundError, IcebergValidationError):
            raise
        except Exception as e:
            raise IcebergCatalogError(
                f"Unexpected error getting table metadata: {str(e)}",
                operation="GetTable",
                catalog_id=catalog_id,
                aws_error=e
            )
    
    def create_table_if_not_exists(self, database: str, table: str, 
                                  source_metadata: 'ResultSetMetaData',
                                  bookmark_column: Optional[str],
                                  iceberg_config: IcebergConfig,
                                  source_engine: str = 'postgresql') -> bool:
        """Create Iceberg table if it doesn't exist, using source metadata.
        
        This method integrates with IcebergSchemaManager to generate schema from
        source JDBC metadata and implements proper identifier-field-ids assignment
        based on the bookmark column.
        
        Args:
            database: Database name
            table: Table name
            source_metadata: JDBC ResultSetMetaData from source query
            bookmark_column: Optional bookmark column name for identifier-field-ids
            iceberg_config: Iceberg configuration
            source_engine: Source database engine type for type mapping
            
        Returns:
            bool: True if table was created, False if it already existed
            
        Raises:
            IcebergConnectionError: If table creation fails
            IcebergSchemaCreationError: If schema generation fails
        """
        try:
            logger.info(f"Creating Iceberg table if not exists: {database}.{table}")
            
            # Configure catalog first
            self.configure_iceberg_catalog(
                iceberg_config.warehouse_location,
                iceberg_config.catalog_id
            )
            
            # Check if table already exists
            if self.table_exists(database, table, iceberg_config.catalog_id):
                logger.info(f"Iceberg table {database}.{table} already exists, skipping creation")
                return False
            
            # Import schema manager here to avoid circular imports
            from .iceberg_schema_manager import IcebergSchemaManager
            
            # Create schema manager and generate Iceberg schema from JDBC metadata
            schema_manager = IcebergSchemaManager()
            iceberg_schema = schema_manager.create_iceberg_schema_from_jdbc(
                jdbc_metadata=source_metadata,
                bookmark_column=bookmark_column,
                source_engine=source_engine,
                schema_id=0
            )
            
            # Validate the generated schema
            validation_result = schema_manager.validate_iceberg_schema(
                schema=iceberg_schema,
                table_name=f"{database}.{table}"
            )
            
            if not validation_result['is_valid']:
                error_msg = f"Generated schema validation failed: {validation_result['errors']}"
                raise IcebergSchemaCreationError(
                    error_msg,
                    database=database,
                    table=table
                )
            
            # Log schema information
            logger.info(f"Generated Iceberg schema with {len(iceberg_schema.fields)} fields")
            if iceberg_schema.identifier_field_ids:
                logger.info(f"Schema includes identifier-field-ids: {iceberg_schema.identifier_field_ids}")
            
            # Create Spark schema for table creation
            spark_schema = schema_manager.create_spark_schema_from_iceberg(iceberg_schema)
            
            # Create table using Spark SQL with format-version=2
            full_table_name = f"{self.catalog_name}.{database}.{table}"
            
            # Build CREATE TABLE SQL with proper Iceberg configuration
            create_sql = self._build_create_table_sql(
                full_table_name=full_table_name,
                iceberg_schema=iceberg_schema,
                iceberg_config=iceberg_config
            )
            
            logger.debug(f"Executing CREATE TABLE SQL: {create_sql}")
            self.spark.sql(create_sql)
            
            # Update table properties with identifier-field-ids if present
            if iceberg_schema.identifier_field_ids:
                self._update_table_identifier_field_ids(
                    database=database,
                    table=table,
                    identifier_field_ids=iceberg_schema.identifier_field_ids,
                    catalog_id=iceberg_config.catalog_id
                )
            
            logger.info(f"Successfully created Iceberg table: {database}.{table}")
            return True
            
        except (IcebergConnectionError, IcebergSchemaCreationError, IcebergValidationError):
            raise
        except Exception as e:
            raise IcebergConnectionError(
                f"Failed to create Iceberg table {database}.{table}: {str(e)}",
                warehouse_location=iceberg_config.warehouse_location,
                spark_error=e
            )
    
    def _build_create_table_sql(self, full_table_name: str, 
                               iceberg_schema: 'IcebergSchema',
                               iceberg_config: IcebergConfig) -> str:
        """Build CREATE TABLE SQL statement for Iceberg table.
        
        Args:
            full_table_name: Fully qualified table name (catalog.database.table)
            iceberg_schema: Iceberg schema with field definitions
            iceberg_config: Iceberg configuration
            
        Returns:
            str: CREATE TABLE SQL statement
        """
        try:
            # Build column definitions
            column_definitions = []
            for field in iceberg_schema.fields:
                nullable_clause = "" if field.required else " NULL"
                comment_clause = f" COMMENT '{field.doc}'" if field.doc else ""
                
                column_def = f"`{field.name}` {field.type}{nullable_clause}{comment_clause}"
                column_definitions.append(column_def)
            
            columns_sql = ",\n    ".join(column_definitions)
            
            # Build table properties
            table_properties = [
                "'format-version'='2'",
                "'write.update.mode'='merge-on-read'",
                "'write.delete.mode'='merge-on-read'",
                "'write.merge.mode'='merge-on-read'"
            ]
            
            # Add custom properties from config if available
            if hasattr(iceberg_config, 'table_properties') and iceberg_config.table_properties:
                for key, value in iceberg_config.table_properties.items():
                    table_properties.append(f"'{key}'='{value}'")
            
            properties_sql = ",\n        ".join(table_properties)
            
            # Build complete CREATE TABLE statement
            create_sql = f"""
            CREATE TABLE {full_table_name} (
                {columns_sql}
            )
            USING iceberg
            TBLPROPERTIES (
                {properties_sql}
            )
            """.strip()
            
            return create_sql
            
        except Exception as e:
            raise IcebergConnectionError(
                f"Failed to build CREATE TABLE SQL: {str(e)}",
                spark_error=e
            )
    
    def _create_table_from_dataframe(self, dataframe: DataFrame, database: str, table: str,
                                    iceberg_config: Optional[IcebergConfig] = None) -> None:
        """Create Iceberg table from DataFrame schema (internal helper method).
        
        Args:
            dataframe: Source DataFrame
            database: Database name
            table: Table name
            iceberg_config: Optional Iceberg configuration
            
        Raises:
            IcebergConnectionError: If table creation fails
        """
        try:
            logger.info(f"Creating Iceberg table from DataFrame schema: {database}.{table}")
            
            # Configure catalog if config is provided
            if iceberg_config:
                logger.info(f"Configuring Iceberg catalog for table creation: warehouse={iceberg_config.warehouse_location}")
                self.configure_iceberg_catalog(
                    iceberg_config.warehouse_location,
                    iceberg_config.catalog_id
                )
            
            # Log DataFrame schema for debugging
            logger.info(f"DataFrame schema for table {database}.{table}: {dataframe.schema}")
            
            # Construct full table name
            full_table_name = f"{self.catalog_name}.{database}.{table}"
            logger.info(f"Creating table with full name: {full_table_name}")
            
            # Validate Spark configuration before attempting table creation
            self._validate_catalog_configuration()
            
            # Try multiple approaches for table creation
            creation_successful = False
            
            # Approach 1: Use writeTo API
            try:
                logger.info(f"Attempting table creation using writeTo API for {full_table_name}")
                writer = dataframe.writeTo(full_table_name)
                
                # Set Iceberg-specific properties
                writer.tableProperty("format-version", "2")
                writer.tableProperty("write.update.mode", "merge-on-read")
                writer.tableProperty("write.delete.mode", "merge-on-read")
                
                # Create the table (this will create an empty table with the DataFrame's schema)
                writer.create()
                creation_successful = True
                logger.info(f"Successfully created table using writeTo API: {database}.{table}")
                
            except Exception as writeto_error:
                logger.warning(f"writeTo API failed for {database}.{table}: {str(writeto_error)}")
                logger.warning(f"writeTo error type: {type(writeto_error).__name__}")
                logger.warning(f"writeTo error details: {str(writeto_error)}")
                
                # Approach 2: Use CREATE TABLE AS SELECT with LIMIT 0
                try:
                    logger.info(f"Attempting table creation using CTAS with LIMIT 0 for {full_table_name}")
                    
                    # Create a temporary view from the DataFrame
                    temp_view_name = f"temp_schema_{database}_{table}"
                    dataframe.createOrReplaceTempView(temp_view_name)
                    
                    # Create table using CTAS with LIMIT 0 to get schema without data
                    create_sql = f"""
                    CREATE TABLE {full_table_name}
                    USING iceberg
                    TBLPROPERTIES (
                        'format-version'='2',
                        'write.update.mode'='merge-on-read',
                        'write.delete.mode'='merge-on-read'
                    )
                    AS SELECT * FROM {temp_view_name} LIMIT 0
                    """
                    
                    logger.info(f"Executing CTAS SQL: {create_sql}")
                    self.spark.sql(create_sql)
                    creation_successful = True
                    logger.info(f"Successfully created table using CTAS: {database}.{table}")
                    
                    # Clean up temporary view
                    try:
                        self.spark.sql(f"DROP VIEW IF EXISTS {temp_view_name}")
                    except Exception as cleanup_error:
                        logger.warning(f"Failed to clean up temp view {temp_view_name}: {cleanup_error}")
                        
                except Exception as ctas_error:
                    logger.error(f"CTAS approach also failed for {database}.{table}: {str(ctas_error)}")
                    raise IcebergConnectionError(
                        f"Both writeTo API and CTAS approaches failed for table {database}.{table}. "
                        f"writeTo error: {str(writeto_error)}. CTAS error: {str(ctas_error)}",
                        warehouse_location=iceberg_config.warehouse_location if iceberg_config else None,
                        spark_error=ctas_error
                    )
            
            if creation_successful:
                logger.info(f"Successfully created Iceberg table: {database}.{table}")
                
                # Add a small delay to ensure table is fully created
                import time
                time.sleep(2)
            else:
                raise IcebergConnectionError(
                    f"Table creation failed for {database}.{table}: No successful creation method",
                    warehouse_location=iceberg_config.warehouse_location if iceberg_config else None
                )
            
        except IcebergConnectionError:
            raise
        except Exception as e:
            logger.error(f"Unexpected error during table creation for {database}.{table}: {str(e)}")
            logger.error(f"Exception type: {type(e).__name__}")
            raise IcebergConnectionError(
                f"Failed to create Iceberg table {database}.{table} from DataFrame: {str(e)}",
                warehouse_location=iceberg_config.warehouse_location if iceberg_config else None,
                spark_error=e
            )

    def create_table_from_dataframe(self, dataframe: DataFrame, database: str, table: str,
                                   iceberg_config: IcebergConfig,
                                   identifier_field_ids: Optional[List[int]] = None) -> None:
        """Create Iceberg table from DataFrame with optional identifier field IDs.
        
        Args:
            dataframe: Source DataFrame
            database: Database name
            table: Table name
            iceberg_config: Iceberg configuration
            identifier_field_ids: Optional list of field IDs for bookmark management
            
        Raises:
            IcebergConnectionError: If table creation fails
        """
        try:
            logger.info(f"Creating Iceberg table from DataFrame: {database}.{table}")
            
            # Configure catalog
            self.configure_iceberg_catalog(
                iceberg_config.warehouse_location,
                iceberg_config.catalog_id
            )
            
            # Check if table already exists
            if self.table_exists(database, table, iceberg_config.catalog_id):
                raise IcebergValidationError(
                    f"Iceberg table {database}.{table} already exists",
                    field="table",
                    value=f"{database}.{table}"
                )
            
            # Create table using write operation
            self.write_table(
                dataframe=dataframe,
                database=database,
                table=table,
                mode="create",
                iceberg_config=iceberg_config
            )
            
            # Update table properties with identifier field IDs if provided
            if identifier_field_ids:
                self._update_table_identifier_field_ids(
                    database, table, identifier_field_ids, iceberg_config.catalog_id
                )
            
            logger.info(f"Successfully created Iceberg table: {database}.{table}")
            
        except (IcebergValidationError, IcebergConnectionError):
            raise
        except Exception as e:
            raise IcebergConnectionError(
                f"Failed to create Iceberg table {database}.{table}: {str(e)}",
                warehouse_location=iceberg_config.warehouse_location,
                spark_error=e
            )
    
    def _update_table_identifier_field_ids(self, database: str, table: str,
                                          identifier_field_ids: List[int],
                                          catalog_id: Optional[str] = None) -> None:
        """Update table properties with identifier field IDs.
        
        Args:
            database: Database name
            table: Table name
            identifier_field_ids: List of field IDs
            catalog_id: Optional catalog ID
            
        Raises:
            IcebergCatalogError: If update fails
        """
        try:
            logger.debug(f"Updating identifier field IDs for table {database}.{table}: {identifier_field_ids}")
            
            # Get current table definition
            get_table_kwargs = {
                'DatabaseName': database,
                'Name': table
            }
            
            if catalog_id:
                get_table_kwargs['CatalogId'] = catalog_id
            
            response = self.glue_client.get_table(**get_table_kwargs)
            table_input = response['Table']
            
            # Remove read-only fields
            table_input.pop('DatabaseName', None)
            table_input.pop('CreateTime', None)
            table_input.pop('UpdateTime', None)
            table_input.pop('CreatedBy', None)
            table_input.pop('IsRegisteredWithLakeFormation', None)
            table_input.pop('CatalogId', None)
            table_input.pop('VersionId', None)
            
            # Update parameters with identifier field IDs
            if 'Parameters' not in table_input:
                table_input['Parameters'] = {}
            
            table_input['Parameters']['identifier-field-ids'] = ','.join(map(str, identifier_field_ids))
            
            # Update table
            update_table_kwargs = {
                'DatabaseName': database,
                'TableInput': table_input
            }
            
            if catalog_id:
                update_table_kwargs['CatalogId'] = catalog_id
            
            self.glue_client.update_table(**update_table_kwargs)
            
            logger.debug(f"Successfully updated identifier field IDs for table {database}.{table}")
            
        except Exception as e:
            raise IcebergCatalogError(
                f"Failed to update identifier field IDs: {str(e)}",
                operation="UpdateTable",
                catalog_id=catalog_id,
                aws_error=e
            )