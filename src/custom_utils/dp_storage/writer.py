"""Help functions used to upload Egress data in DataBricks notebooks."""
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

from pyspark.sql import functions as F
from pyspark.sql.types import StructType
from pyspark.sql.utils import AnalysisException

ISO_FORMAT = "yyyy-MM-dd'T'HH:mm:ss'Z'"


def _get_from_and_to_data_coverage(df_spark, timestamp_column, time_resolution):
    """Extracts time interval that the input dataframe covers. For time_resolution='year' e.g., the interval will be in
    full years. 1 second is subtracted from to_date so that the datetime is inside the year/month/day.
    Example for time_resolution='month': If df_spark has a week of data in January 2021, the output would be
                                         from_date = datetime(2021, 1, 1) and
                                         to_date = datetime(2021, 1, 31, 23, 59, 59)"""
    from_date, to_date = df_spark \
                             .withColumn('_datetime', F.to_timestamp(timestamp_column, ISO_FORMAT)) \
                             .select(F.min('_datetime'), F.max('_datetime')) \
                             .first()
    
    if time_resolution == 'year':
        from_date = datetime(from_date.year, 1, 1)
        to_date = datetime(to_date.year + 1, 1, 1) - timedelta(seconds=1)
    elif time_resolution == 'month':
        from_date = datetime(from_date.year, from_date.month, 1)
        to_date = to_date + relativedelta(months=1)
        to_date = datetime(to_date.year, to_date.month, 1) - timedelta(seconds=1)
    elif time_resolution == 'day':
        from_date = datetime(from_date.year, from_date.month, from_date.day)
        to_date = to_date + timedelta(days=1)
        to_date = datetime(to_date.year, to_date.month, to_date.day) - timedelta(seconds=1)
    else:
        raise Exception(f"The input {time_resolution = } is invalid!")

    return from_date, to_date


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


def read_all_egress_data(egress_identifier, mount_point, spark, schema: StructType):
    """Returns all egress data."""
    try:
        df_egress = spark.read.parquet(f'{mount_point}/{egress_identifier}')
    except AnalysisException:  # Path does not exist
        # Define empty dataframe with same schema as Ingress data
        df_egress = spark.createDataFrame((), schema)
    return df_egress


def read_egress_data_that_overlaps_with_new_data(egress_identifier, df_egress_new, timestamp_column, time_resolution, mount_point, spark):
    """Returns the egress data that overlaps (in years, months, or days depending on time_resolution) with the input df_egress_new."""
    df_egress_existing = read_all_egress_data(egress_identifier, mount_point, spark, df_egress_new.schema)

    # TODO: Make this more efficient by selecting on year, month, day columns
    from_date, to_date = _get_from_and_to_data_coverage(df_egress_new, timestamp_column, time_resolution)
    df_egress_existing = (
        df_egress_existing
        .withColumn('_datetime', F.to_timestamp(timestamp_column, ISO_FORMAT))
        .where(F.col('_datetime').between(from_date, to_date))
        .drop('_datetime')
    )
    return df_egress_existing


def _merge_with_existing_egress(df_egress_new, egress_identifier, timestamp_column, index_columns, time_resolution, mount_point, spark):
    """Reads existing Egress data and merges it with df_egress_new."""
    df_egress_existing = read_egress_data_that_overlaps_with_new_data(egress_identifier, df_egress_new, timestamp_column, time_resolution, mount_point, spark)

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


def merge_and_upload_egress_data(df_egress_new, egress_identifier, timestamp_column, index_columns, time_resolution, mount_point, spark):
    """Reads existing Egress data, merges with the new data and uploads."""
    df_egress_new = add_time_columns(df_egress_new, timestamp_column, time_resolution)
    df_egress = _merge_with_existing_egress(df_egress_new, egress_identifier, timestamp_column, index_columns, time_resolution, mount_point, spark)

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
