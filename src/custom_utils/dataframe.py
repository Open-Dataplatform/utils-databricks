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