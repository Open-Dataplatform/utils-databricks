"""Help functions used to upload Egress data in DataBricks notebooks."""
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

from pyspark.sql import functions as F
from pyspark.sql.types import StructType
from pyspark.sql.utils import AnalysisException

ISO_FORMAT = "yyyy-MM-dd'T'HH:mm:ss'Z'"


def add_time_columns(df_spark, timestamp_column, time_resolution):
    """Adds year, month, and day columns depending on the input time_resolution."""
    if time_resolution == 'year':
        df_spark = (
            df_spark
            .withColumn('_datetime', F.to_timestamp(timestamp_column, ISO_FORMAT))
            .withColumn('year', F.year("_datetime"))
            .drop('_datetime')
        )
    elif time_resolution == 'month':
        df_spark = (
            df_spark
            .withColumn('_datetime', F.to_timestamp(timestamp_column, ISO_FORMAT))
            .withColumn('year', F.year("_datetime"))
            .withColumn('month', F.lpad(F.month("_datetime"), 2, '0'))
            .drop('_datetime')
        )
    elif time_resolution == 'day':
        df_spark = (
            df_spark
            .withColumn('_datetime', F.to_timestamp(timestamp_column, ISO_FORMAT))
            .withColumn('year', F.year("_datetime"))
            .withColumn('month', F.lpad(F.month("_datetime"), 2, '0'))
            .withColumn('day', F.lpad(F.dayofmonth("_datetime"), 2, '0'))
            .drop('_datetime')
        )
    else:
        raise Exception(f"The input {time_resolution = } is invalid!")

    return df_spark


def _get_covered_partitions(df, time_resolution):
    """Looks up all combinations of year, month, day (depending on time_resolution) in df.
    Returns list with rows (pyspark.sql.types.Row)."""
    partition_names = _get_partition_name_list(time_resolution)

    df_covered_partitions = df.select(*partition_names).distinct()
    partition_list = list(df_covered_partitions.toLocalIterator())
    return partition_list


def get_covered_partition_paths(df, time_resolution):
    """Generates relative paths to all partitons covered in df.
    """
    partition_list = _get_covered_partitions(df, time_resolution)

    if time_resolution == 'year':
        partition_paths = [f'year={p.year}' for p in partition_list]
    elif time_resolution == 'month':
        partition_paths = [f'year={p.year}/month={int(p.month):02}' for p in partition_list]
    elif time_resolution == 'day':
        partition_paths = [f'year={p.year}/month={int(p.month):02}/day={int(p.day):02}' for p in partition_list]
    return partition_paths


def read_egress_data_that_overlaps_with_new_data(egress_identifier, df_egress_new, time_resolution, mount_point, spark):
    """Reads data from all the partitions in egress that overlap with df_egress_new."""

    partition_paths = get_covered_partition_paths(df_egress_new, time_resolution)

    base_path = f'{mount_point}/{egress_identifier}'
    egress_paths = [f'{base_path}/{path}' for path in partition_paths]
    try:
        df_egress = spark.read.option("basePath", base_path).parquet(*egress_paths)
    except AnalysisException:  # Path does not exist
        # Define empty dataframe with same schema as Ingress data
        df_egress = spark.createDataFrame((), df_egress_new.schema)
    return df_egress


def _merge_with_existing_egress(df_egress_new, egress_identifier, index_columns, time_resolution, mount_point, spark):
    """Reads existing Egress data and merges it with df_egress_new."""
    df_egress_existing = read_egress_data_that_overlaps_with_new_data(egress_identifier, df_egress_new, time_resolution, mount_point, spark)

    df_merged = (
        df_egress_new
        .unionByName(df_egress_existing, allowMissingColumns=True)
        .dropDuplicates(index_columns)
    )

    return df_merged


def _get_partition_name_list(time_resolution):
    """Returns a list with column names used for data partitioning."""
    if time_resolution == 'year':
        partition_list = ['year']
    elif time_resolution == 'month':
        partition_list = ['year', 'month']
    elif time_resolution == 'day':
        partition_list = ['year', 'month', 'day']
    else:
        raise Exception(f"The input {time_resolution = } is invalid!")

    return partition_list


def merge_and_upload(df_egress_new, egress_identifier, timestamp_column, index_columns, time_resolution, mount_point, spark):
    """Reads existing Egress data, merges with the new data and uploads."""

    df_egress_new = add_time_columns(df_egress_new, timestamp_column, time_resolution)
    df_egress = _merge_with_existing_egress(df_egress_new, egress_identifier, index_columns, time_resolution, mount_point, spark)

    partition_name_list = _get_partition_name_list(time_resolution)

    # Make sure that only folders with new data get overwritten
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    df_egress \
        .write \
        .partitionBy(partition_name_list) \
        .parquet(f'{mount_point}/{egress_identifier}', mode='overwrite')


def upload_and_overwrite(df_egress_new, egress_identifier, timestamp_column, time_resolution, mount_point, spark):
    """Reads existing Egress data, merges with the new data and uploads."""
    df_egress_new = add_time_columns(df_egress_new, timestamp_column, time_resolution)

    partition_name_list = _get_partition_name_list(time_resolution)

    # Set mode to overwrite everything
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "static")

    df_egress_new \
        .write \
        .partitionBy(partition_name_list) \
        .parquet(f'{mount_point}/{egress_identifier}', mode='overwrite')
