"""Functions to modify Pyspark dataframes"""

from pyspark.sql.types import ArrayType, StructType


def _get_array_and_struct_fields(df):
    """Return list with columns (names and types) of either ArrayType or StructType"""
    complex_fields = []
    for field in df.schema.fields:
        data_type = type(field.dataType)
        if data_type == ArrayType or data_type == StructType:
            complex_fields.append((field.name, data_type))
    
    return complex_fields


def _get_expanded_columns_with_aliases(df, column_name, layer_separator='_'):
    """Return list of columns (pyspark.sql.column.Column) in a struct. To be used in df.select().
    
    layer_separator is used in the aliases.
    """
    expanded_columns = []
    for struct_field in df.select(f'`{column_name}`.*').columns:
        expanded_column = f"`{column_name}`.`{struct_field}`"
        expanded_column_alias = f"{column_name}{layer_separator}{struct_field}"
        
        expanded_columns.append(F.col(expanded_column).alias(expanded_column_alias))
    
    return expanded_columns


def flatten_array(df, column_name):
    """Return dataframe with flattened array column."""
    df = df.withColumn(column_name, F.explode_outer(column_name))
    return df


def flatten_struct(df, column_name, layer_separator='_'):
    """Return dataframe with flattened struct column."""

    expanded_columns = _get_expanded_columns_with_aliases(df, column_name, layer_separator)

    df = df.select("*", *expanded_columns) \
           .drop(column_name)

    return df


def flatten_all(df, layer_separator='_'):
    """Return dataframe with flattened arrays and structs"""
    complex_fields = _get_array_and_struct_fields(df)

    while len(complex_fields) != 0:

        column_name, data_type = complex_fields[0]
        print(f"Processing: {column_name}, Type: {data_type}")

        if data_type == StructType:
            df = flatten_struct(df, column_name, layer_separator)
        elif data_type == ArrayType:
            df = flatten_array(df, column_name)
        
        complex_fields = _get_array_and_struct_fields(df)
    
    return df


def _string_replace(column_name: str, replacements: dict):
    """Return column_name with multiple string replacements."""
    for string_before, string_after in replacements.items():
        column_name = column_name.replace(string_before, string_after)
    
    return column_name


def rename_columns(df, replacements={'.': '_'}):
    """Return dataframe with columns renamed according to replacement dict."""
    for column_name in df.columns:
        df = df.withColumnRenamed(column_name, _string_replace(column_name, replacements))
    return df
