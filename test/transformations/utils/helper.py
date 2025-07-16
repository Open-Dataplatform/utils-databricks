from pyspark.sql import DataFrame
from pyspark.sql.functions import col

def equal_dataframes(df1: DataFrame, df2: DataFrame, order_by: str) -> bool:
    """Checks if two dataframes are equal

    Args:
        df1 (DataFrame): First dataframe.
        df2 (DataFrame): Second dataframe.
        order_by (str): Column to order dataframes by.

    Returns:
        bool: True if dataframes are equal, else False.
    """
    return (df1.schema == df2.schema) and (df1.orderBy(col(order_by)).collect() == df2.orderBy(col(order_by)).collect())

def count_json_rows(data: list|dict) -> int:
    """Counts flattened structure of json data

    Args:
        data (list | dict): list or dict containing json data.

    Returns:
        int: Number of rows in flattened structure.
    """
    count = 0
    if isinstance(data, dict):
        for val in data.values():
            if isinstance(val, dict):
                count += count_json_rows(val)
            elif isinstance(val, list):
                count += count_json_rows(val)
        if count == 0:
            count += 1
    elif isinstance(data, list):
        if len(data) == 0:
            count += 1
        elif isinstance(data[0], dict):
            for i in data:
                count += count_json_rows(i)
        else:
            count += len(data)
    return count