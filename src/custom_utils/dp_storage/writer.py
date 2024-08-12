"""Functions related to writing to the Delta lake"""

from pyspark.sql.types import (
    StructType, StringType, IntegerType, ArrayType, StructField, BooleanType, DoubleType
)
from typing import Tuple
from .connector import get_mount_point_name
from pyspark.sql.functions import input_file_name, col
from pyspark.sql import DataFrame


def get_destination_path(destination_config: dict) -> str:
    """Extracts destination path from destination_config"""

    data_config = list(destination_config.values())[0]
    mount_point = get_mount_point_name(data_config["account"])
    destination_path = f'{mount_point}/{data_config["dataset"]}'

    return destination_path


def get_databricks_table_info(destination_config: dict) -> Tuple[str, str]:
    """Constructs database and table names to be used in Databricks."""

    data_config = list(destination_config.values())[0]
    mount_point = get_mount_point_name(data_config["account"])
    
    database_name = mount_point.split('/')[-1]
    table_name = data_config['dataset']
    
    return database_name, table_name


def json_schema_to_spark_struct(json_schema, definitions=None) -> StructType:
    """
    Converts a JSON schema to a PySpark StructType, with enhanced null handling.
    
    Args:
        json_schema (dict): The JSON schema.
        definitions (dict, optional): The schema definitions. Defaults to None.
    
    Returns:
        StructType: The corresponding PySpark StructType.
    """
    if definitions is None:
        definitions = json_schema.get('definitions', {})

    def resolve_ref(ref):
        ref_path = ref.split('/')[-1]
        return definitions.get(ref_path, {})

    def parse_type(field_props):
        if isinstance(field_props, list):
            valid_types = [parse_type(fp) for fp in field_props if fp.get('type') != 'null']
            return valid_types[0] if valid_types else StringType()
        
        if '$ref' in field_props:
            field_props = resolve_ref(field_props['$ref'])

        json_type = field_props.get('type')
        if isinstance(json_type, list):
            json_type = [t for t in json_type if t != 'null']
            json_type = json_type[0] if json_type else None

        if json_type is None:
            return StringType()
        elif json_type == "string":
            return StringType()
        elif json_type == "integer":
            return IntegerType()
        elif json_type == "boolean":
            return BooleanType()
        elif json_type == "number":
            return DoubleType()
        elif json_type == "array":
            items = field_props['items']
            if isinstance(items, list):
                valid_items = [item for item in items if item.get('type') != 'null']
                return ArrayType(parse_type(valid_items[0]) if valid_items else StringType())
            else:
                return ArrayType(parse_type(items))
        elif json_type == "object":
            return StructType([StructField(k, parse_type(v), True) for k, v in field_props.get('properties', {}).items()])
        else:
            return StringType()

    def parse_properties(properties):
        fields = [StructField(field_name, parse_type(field_props), True) for field_name, field_props in properties.items()]
        return StructType(fields)

    return parse_properties(json_schema.get('properties', {}))


def apply_type_mapping(schema: StructType, type_mapping: dict) -> StructType:
    """
    Applies a custom type mapping to a given StructType schema.
    
    Args:
        schema (StructType): The original schema to map.
        type_mapping (dict): A dictionary mapping original types to desired Spark SQL types.
    
    Returns:
        StructType: A new StructType with the custom types applied.
    """
    def map_field(field):
        field_type = field.dataType
        if isinstance(field_type, StructType):
            mapped_type = apply_type_mapping(field_type, type_mapping)
        elif isinstance(field_type, ArrayType):
            mapped_type = ArrayType(type_mapping.get(field_type.elementType.simpleString(), field_type.elementType), field_type.containsNull)
        else:
            mapped_type = type_mapping.get(field_type.simpleString(), field_type)
        return StructField(field.name, mapped_type, field.nullable)

    return StructType([map_field(field) for field in schema.fields])


def add_input_file_name_column(df: DataFrame) -> (DataFrame, str):
    """
    Adds a column 'input_file_name' to the DataFrame and positions it as the first column.
    Returns a DataFrame with 'input_file_name' column added as the first column,
    and also returns the columns of interest as a string.
    
    Args:
        df (DataFrame): A PySpark DataFrame after flattening.
    
    Returns:
        DataFrame: A DataFrame with 'input_file_name' column added as the first column.
        str: A string of columns of interest, excluding 'input_file_name'.
    """
    df_with_filename = df.withColumn("input_file_name", input_file_name().cast("string"))
    columns_of_interest = [col for col in df_with_filename.columns if col != 'input_file_name']
    columns_of_interest_str = ', '.join(columns_of_interest)
    
    print(f"Columns of interest (excluding 'input_file_name'): {columns_of_interest_str}")
    
    cols = ['input_file_name'] + columns_of_interest
    df_with_filename = df_with_filename.select(cols)
    
    return df_with_filename, columns_of_interest_str
