"""Functions to modify Pyspark dataframes"""

from typing import List

from pyspark.sql.types import ArrayType, StructType
import pyspark.sql.functions as F


def _get_array_and_struct_columns(df):
    """Return list with columns (names and types) of either ArrayType or StructType"""
    complex_columns = []
    for field in df.schema.fields:
        data_type = type(field.dataType)
        if data_type == ArrayType or data_type == StructType:
            complex_columns.append((field.name, data_type))

    return complex_columns


def _get_expanded_columns_with_aliases(df, column_name, layer_separator='_'):
    """Return list of nested columns (pyspark.sql.column.Column) in a column struct. To be used in df.select().

    layer_separator is used as the seperator in the new column name between the column and nested column names.
    """
    expanded_columns = []
    for nested_column in df.select(f'`{column_name}`.*').columns:
        expanded_column = f"`{column_name}`.`{nested_column}`"
        expanded_column_alias = f"{column_name}{layer_separator}{nested_column}"

        expanded_columns.append(F.col(expanded_column).alias(expanded_column_alias))

    return expanded_columns


def flatten_array_column(df, column_name):
    """Return dataframe with flattened array column."""
    df = df.withColumn(column_name, F.explode_outer(column_name))
    return df


def flatten_struct_column(df, column_name, layer_separator='_'):
    """Return dataframe with flattened struct column."""

    expanded_columns = _get_expanded_columns_with_aliases(df, column_name, layer_separator)

    df = df.select("*", *expanded_columns) \
           .drop(column_name)

    return df


def flatten(df, layer_separator='_'):
    """Return dataframe with flattened arrays and structs.

    Written with inspiration from https://www.youtube.com/watch?v=jD8JIw1FVVg.
    """
    complex_columns = _get_array_and_struct_columns(df)

    while len(complex_columns) != 0:

        column_name, data_type = complex_columns[0]

        if data_type == StructType:
            df = flatten_struct_column(df, column_name, layer_separator)
        elif data_type == ArrayType:
            df = flatten_array_column(df, column_name)

        complex_columns = _get_array_and_struct_columns(df)

    return df


def _string_replace(s: str, replacements: dict):
    """Return string with multiple replacements."""
    for string_before, string_after in replacements.items():
        s = s.replace(string_before, string_after)

    return s

def flatten_extended(df: DataFrame, col_types: dict, flatten_completely: bool = True) -> DataFrame:
    """Flatten the DataFrame. If `flatten_completely` is True, fully explode all nested columns.
    Args:
        df (DataFrame): Input DataFrame to flatten.
        col_types (dict): Dictionary of column names and their corresponding data types.
        flatten_completely (bool): Flag to control complete flattening.
    Returns:
        DataFrame: Flattened DataFrame.
    """
    
    if flatten_completely:
        df = dataframe.flatten(df)
    else:
        for col_name, col_type in dataframe._get_array_and_struct_columns(df):
            df = df.withColumn(col_name, to_json(col(col_name)))

    # Add input_file_name column
    df = df.withColumn("input_file_name", input_file_name().cast("string"))
    cols_typed = ['cast(input_file_name() as string) as input_file_name']

    # Ensure columns are present with correct data types
    for c, dtype in col_types.items():
        if c not in df.columns:
            print(f"Adding missing column {c} as {dtype}")
            df = df.withColumn(c, lit(None).cast('string'))
        else:
            df = df.withColumn(c, col(c).cast(dtype))
        cols_typed.append(f"cast({c} as {dtype}) as {c}")

    df = df.selectExpr(*cols_typed)
    
    # Rename columns
    df = dataframe.rename_columns(df, replacements={'__': '_'})
    df = dataframe.rename_columns(df, replacements={'.': '_'})

    # Print columns and their types after type enforcement
    columns_and_types = [(col_name, col_type) for col_name, col_type in df.dtypes]
    # print("Columns after type enforcement:", columns_and_types)

    return df


def rename_columns(df, replacements={'.': '_'}):
    """Return dataframe with columns renamed according to replacement dict."""
    for column_name in df.columns:
        df = df.withColumnRenamed(column_name, _string_replace(column_name, replacements))
    return df


def add_columns_that_are_not_in_df(df, column_names: List[str]):
    """Add columns in column_names that are not already in dataframe.

    The new columns are empty. This function can be used before a CAST statement to ensure that all
    expected columns are included in dataframe.
    """
    for column_name in column_names:
        if column_name not in df.columns:
            df = df.withColumn(column_name, F.lit(None))
            print(f'Column "{column_name}" was added.')
    return df
