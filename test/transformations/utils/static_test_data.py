from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql.types import (
    StructField,
    StringType,
    DoubleType,
    DecimalType,
    StructType,
    ArrayType,
    IntegerType,
    TimestampType
)
from datetime import datetime
from typing import Any

def get_init_df() -> DataFrame:
    """Gets static inital test data

    Returns:
        DataFrame: Static test data
    """
    spark = SparkSession.builder.getOrCreate()
    data1: list[dict[str, list[dict[str, Any]]]] = [{"data" : [
            {
                "A": 0.942,
                "B": 0.421,
                "C": 33,
                "D": 3.366,
                "E": "milk",
                "F": "str_6",
                "G": datetime.strptime("2023-01-02T00:00:00", "%Y-%m-%dT%H:%M:%S"),
                "H": Row(id= "ebc8d44d-f77a-4b95-a4e8-3781234823c1", value= 1)
            },
            {
                "A": -1.397,
                "B": 0.618,
                "C": 32,
                "D": 4.363,
                "E": "water",
                "F": "str_9",
                "G": datetime.strptime("2023-01-03T00:00:00", "%Y-%m-%dT%H:%M:%S"),
                "H": Row(id= "e6254f19-ba12-46d9-af54-788f195a6f50", value= 9)
            },
            {
                "A": -0.43,
                "B": 0.553,
                "C": 56,
                "D": 2.005,
                "E": "oil",
                "F": "str_0",
                "G": datetime.strptime("2023-01-04T00:00:00", "%Y-%m-%dT%H:%M:%S"),
                "H": Row(id= "ceeb8cea-0317-48d7-a6b5-d3c8aba0009c", value= 7)
            }]
        }
    ]
    data2: list[dict[str, list[dict[str, Any]]]] = [{"data" : [                                            
            {
                "A": 1.288,
                "B": 0.764,
                "C": 32,
                "D": 0.125,
                "E": "sugar",
                "F": "str_7",
                "G": datetime.strptime("2023-01-02T00:00:00", "%Y-%m-%dT%H:%M:%S"),
                "H": Row(id= "78a661c9-3518-4c07-a4d5-636e9bc3c400", value= 7)
            },
            {
                "A": 1.449,
                "B": 0.266,
                "C": 29,
                "D": 0.894,
                "E": "flour",
                "F": "str_5",
                "G": datetime.strptime("2023-01-03T00:00:00", "%Y-%m-%dT%H:%M:%S"),
                "H": Row(id= "070506a6-8a02-40e1-a1af-37f86cb90787", value= 3)
            },
            {
                "A": 0.203,
                "B": 0.553,
                "C": 44,
                "D": 0.262,
                "E": "egg",
                "F": "str_7",
                "G": datetime.strptime("2023-01-04T00:00:00", "%Y-%m-%dT%H:%M:%S"),
                "H": Row(id= "f34aed05-6ad6-4a8e-aca4-192fa1feb9dc", value= 4)
            }
        ]
    }]
    schema: StructType = StructType([StructField('data', 
                                                 ArrayType(StructType([
                                                     StructField('A', DoubleType(), True), 
                                                     StructField('B', DoubleType(), True), 
                                                     StructField('C', IntegerType(), True), 
                                                     StructField('D', DoubleType(), True), 
                                                     StructField('E', StringType(), True), 
                                                     StructField('F', StringType(), True), 
                                                     StructField('G', TimestampType(), True), 
                                                     StructField('H', StructType([
                                                         StructField('id', StringType(), True), 
                                                         StructField('value', IntegerType(), True)]), 
                                                                 True)]), 
                                                           True), 
                                                 True)])
    df1: DataFrame = spark.createDataFrame(data1, schema)
    df2: DataFrame = spark.createDataFrame(data2, schema)
    df: DataFrame = df1.union(df2)
    return df

def get_flat_df() -> DataFrame:
    """Gets static flattened test data

    Returns:
        DataFrame: Static test data
    """
    spark = SparkSession.builder.getOrCreate()
    data: list[dict[str, Any]] = [
            {
                "data_A": 0.942,
                "data_B": 0.421,
                "data_C": 33,
                "data_D": 3.366,
                "data_E": "milk",
                "data_F": "str_6",
                "data_G": datetime.strptime("2023-01-02T00:00:00", "%Y-%m-%dT%H:%M:%S"),
                "data_H_id": "ebc8d44d-f77a-4b95-a4e8-3781234823c1",
                "data_H_value": 1
            },
            {
                "data_A": -1.397,
                "data_B": 0.618,
                "data_C": 32,
                "data_D": 4.363,
                "data_E": "water",
                "data_F": "str_9",
                "data_G": datetime.strptime("2023-01-03T00:00:00", "%Y-%m-%dT%H:%M:%S"),
                "data_H_id": "e6254f19-ba12-46d9-af54-788f195a6f50",
                "data_H_value": 9
            },
            {
                "data_A": -0.43,
                "data_B": 0.553,
                "data_C": 56,
                "data_D": 2.005,
                "data_E": "oil",
                "data_F": "str_0",
                "data_G": datetime.strptime("2023-01-04T00:00:00", "%Y-%m-%dT%H:%M:%S"),
                "data_H_id": "ceeb8cea-0317-48d7-a6b5-d3c8aba0009c",
                "data_H_value": 7
            },
            {
                "data_A": 1.288,
                "data_B": 0.764,
                "data_C": 32,
                "data_D": 0.125,
                "data_E": "sugar",
                "data_F": "str_7",
                "data_G": datetime.strptime("2023-01-02T00:00:00", "%Y-%m-%dT%H:%M:%S"),
                "data_H_id": "78a661c9-3518-4c07-a4d5-636e9bc3c400",
                "data_H_value": 7
            },
            {
                "data_A": 1.449,
                "data_B": 0.266,
                "data_C": 29,
                "data_D": 0.894,
                "data_E": "flour",
                "data_F": "str_5",
                "data_G": datetime.strptime("2023-01-03T00:00:00", "%Y-%m-%dT%H:%M:%S"),
                "data_H_id": "070506a6-8a02-40e1-a1af-37f86cb90787",
                "data_H_value": 3
            },
            {
                "data_A": 0.203,
                "data_B": 0.553,
                "data_C": 44,
                "data_D": 0.262,
                "data_E": "egg",
                "data_F": "str_7",
                "data_G": datetime.strptime("2023-01-04T00:00:00", "%Y-%m-%dT%H:%M:%S"),
                "data_H_id": "f34aed05-6ad6-4a8e-aca4-192fa1feb9dc",
                "data_H_value": 4
            }
        ]
    schema: StructType = StructType([StructField('data_A', DoubleType(), True), 
                         StructField('data_B', DoubleType(), True), 
                         StructField('data_C', IntegerType(), True), 
                         StructField('data_D', DoubleType(), True), 
                         StructField('data_E', StringType(), True), 
                         StructField('data_F', StringType(), True), 
                         StructField('data_G', TimestampType(), True), 
                         StructField('data_H_id', StringType(), True), 
                         StructField('data_H_value', IntegerType(), True)])
    df: DataFrame = spark.createDataFrame(data, schema)
    df = df.withColumn("data_A", df.data_A.cast(DecimalType(38,18)))
    df = df.withColumn("data_B", df.data_B.cast(DecimalType(38,18)))
    df = df.withColumn("data_D", df.data_D.cast(DecimalType(38,18)))
    return df