from functools import reduce
from typing import List, Optional

import dlt
import pandas as pd
import pyspark.sql.functions as F

from pyspark.sql import DataFrame
from pyspark.sql.functions import broadcast, col, pandas_udf
from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    IntegerType,
    MapType,
    StringType,
    StructField,
    StructType,
)

from pyspark.sql import DataFrame
from pyspark.sql.functions import broadcast
from typing import List, Optional
from pyspark.sql import functions as F
from pyspark.sql import Window

# Utility functions

# Function to sanitize column names because Databricks freaks out at spaces
def sanitize_column_names(df: DataFrame) -> DataFrame:
    """
    Returns a new DataFrame with all column names lowercased and spaces replaced with underscores.
    """
    renamed_cols = [col(c).alias(c.lower().replace(" ", "_")) for c in df.columns]
    return df.select(renamed_cols)

# Drop metadata columns for joins
def drop_metadata_columns(df: DataFrame) -> DataFrame:
    metadata_columns = [
        "file_name", "file_path", "file_size", "sourcing_system",
        "sampling_method", "platform", "ingest_platform", "ingest_id", "ingestion_timestamp",
        "scrape_timestamp", "sourcing_status", "failure_reason", "last_scraped"
    ]
    
    # Drop only columns that exist in the DataFrame
    columns_to_drop = [col for col in metadata_columns if col in df.columns]
    
    return df.drop(*columns_to_drop)

@dlt.table()
def comscore_sl_traffic_category_map():
    # Get config variables
    catalog = spark.conf.get("catalog")
    schema_bronze = spark.conf.get("schema_bronze")
    bronze_traffic_category = spark.conf.get("traffic_category_table")

    # Define full table paths
    input_traffic_category = f"{catalog}.{schema_bronze}.{bronze_traffic_category}"

    # Read the traffic category table
    df = (spark.table(input_traffic_category))
    df = drop_metadata_columns(df) # Dropped for pivot
    
    # Pivot table using windows
    w_min = Window.partitionBy('pattern_id').orderBy('level_id')
    w_max = Window.partitionBy('pattern_id').orderBy(F.col('level_id').desc())

    df_filter = (
        df.withColumn('top_category', F.first('category').over(w_min))
        .withColumn('bottom_category', F.first('category').over(w_max))
        .withColumn('top_subcategory', F.first('subcategory').over(w_min))
        .withColumn('bottom_subcategory', F.first('subcategory').over(w_max))
        .withColumn('magazine', F.first('Magazine').over(w_max))
        .withColumn('streaming_video', F.first('Streaming_Video').over(w_max))
        .withColumn('blog', F.first('Blog').over(w_max))
        .withColumn('streaming_audio', F.first('Streaming_Audio').over(w_max))
        .withColumn('cable_broadcast_tv', F.first('Cable_Broadcast_TV').over(w_max))
        .withColumn('radio', F.first('Radio').over(w_max))
        .withColumn('newspaper', F.first('Newspaper').over(w_max))
    )

    # Drop unneeded columns
    columns_to_drop = ["category","subcategory","level_id"]

    df_filter = df_filter.drop(*columns_to_drop) # Resulting table to pivot

    # Pivot the table
    df_pivoted = df_filter.groupBy("month_id", "pattern_id", "top_category", "bottom_category","top_subcategory","bottom_subcategory" ,"magazine","streaming_video","blog","streaming_audio","cable_broadcast_tv","radio","newspaper").pivot("level_name").agg(F.first("web_name"))

    # Rename columns to remove spaces and other things of the sort
    df_pivoted = sanitize_column_names(df_pivoted)

    return df_pivoted

@dlt.table()
def comscore_sl_mobile_day_session():
    # Get config variables
    catalog = spark.conf.get("catalog")
    schema_bronze = spark.conf.get("schema_bronze")
    bronze_mobile_day_session = spark.conf.get("mobile_day_session_table")
    bronze_mobile_demograic = spark.conf.get("mobile_demographic_table")

    # Define full table paths
    input_mobile_day_session = f"{catalog}.{schema_bronze}.{bronze_mobile_day_session}"
    input_mobile_demographic = f"{catalog}.{schema_bronze}.{bronze_mobile_demograic}"
    traffic_category_table = dlt.read("comscore_sl_traffic_category_map")

    # Read the mobile day session table
    mobile_day_session = (spark.table(input_mobile_day_session))
    mobile_demographic = (spark.table(input_mobile_demographic))
    mobile_demographic = drop_metadata_columns(mobile_demographic) # Dropped for joins

    # Join 1 with traffic category because we're going in order of largest cardinality
    mobile_day_session = mobile_day_session.join(traffic_category_table, on=["month_id", "pattern_id"], how="left")

    # Join 2 with demographic table
    mobile_day_session = mobile_day_session.alias("left")
    mobile_demographic = mobile_demographic.alias("right")

    # Join condition
    join_condition = (
        (col("left.month_id") == col("right.month_id")) &
        (col("left.device_id") == col("right.machine_id"))
    )

    # List of columns to select from left DataFrame
    left_columns = [col(f"left.{c}") for c in mobile_day_session.columns]

    # List of specific columns to select from right DataFrame
    right_columns = [
        col("right.machine_id"),
        col("right.age"),
        col("right.gender"),
        col("right.hh_income"),
        col("right.hh_size"),
        col("right.children_present"),
        col("right.region"),
        col("right.ethnicity_id"),
        col("right.race_id")
    ]

    # Perform join and select desired columns
    mobile_day_session = mobile_day_session.join(mobile_demographic, on=join_condition, how="left").select(*left_columns, *right_columns)

    mobile_day_session = sanitize_column_names(mobile_day_session)

    return mobile_day_session

@dlt.table()
def comscore_sl_mobile_traffic():
    # Get config variables
    catalog = spark.conf.get("catalog")
    schema_bronze = spark.conf.get("schema_bronze")
    bronze_ios_traffic = spark.conf.get("ios_traffic_table")
    bronze_android_traffic = spark.conf.get("android_traffic_table")

    # Tables to join
    bronze_mobile_demograic = spark.conf.get("mobile_demographic_table")
    time_lookup = spark.conf.get("time_lookup_table")
    browser_lookup = spark.conf.get("browser_lookup_table")


    # Define full table paths
    input_ios = f"{catalog}.{schema_bronze}.{bronze_ios_traffic}"
    input_android = f"{catalog}.{schema_bronze}.{bronze_android_traffic}"
    input_time_lookup = f"{catalog}.{schema_bronze}.{time_lookup}"
    input_browser_lookup = f"{catalog}.{schema_bronze}.{browser_lookup}"
    input_mobile_demographic = f"{catalog}.{schema_bronze}.{bronze_mobile_demograic}"
    

    # Read tables
    ios = (spark.table(input_ios)).withColumn("source_table", F.lit(input_ios))
    android = (spark.table(input_android)).withColumn("source_table", F.lit(input_android))
    time_lookup = (spark.table(input_time_lookup))
    browser_lookup = (spark.table(input_browser_lookup))
    mobile_demographic = (spark.table(input_mobile_demographic))
    traffic_category_table = dlt.read("comscore_sl_traffic_category_map")

    # Drop metadata columns for joins
    time_lookup = drop_metadata_columns(time_lookup)
    browser_lookup = drop_metadata_columns(browser_lookup)
    mobile_demographic = drop_metadata_columns(mobile_demographic)

    # Concatenate iOS and Android traffic data
    mobile_traffic = ios.unionByName(android, allowMissingColumns=True)

    # Joins

    # Join 1 with time lookup to allow later joins
    mobile_traffic = mobile_traffic.join(broadcast(time_lookup), on="time_id", how="left")

    # 1, traffic category
    mobile_traffic = mobile_traffic.alias("left")
    traffic_category_table = traffic_category_table.alias("right")

    # Join condition
    join_condition = (
        (col("left.month_num") == col("right.month_id")) &
        (col("left.pattern_id") == col("right.pattern_id"))
    )

    left_columns = [col(f"left.{c}") for c in mobile_traffic.columns]
    right_columns = [col(f"right.{c}") for c in traffic_category_table.columns if c not in ["month_id", "pattern_id"]]

    mobile_traffic = mobile_traffic.join(traffic_category_table, on=join_condition, how="left").select(*left_columns, *right_columns)

    # Join 2 with demographic table
    mobile_traffic = mobile_traffic.alias("left")
    mobile_demographic = mobile_demographic.alias("right")

    # Join condition
    join_condition = (
        (col("left.month_num") == col("right.month_id")) &
        (col("left.machine_id") == col("right.machine_id"))
    )

    # List of columns to select from left DataFrame
    left_columns = [col(f"left.{c}") for c in mobile_traffic.columns]

    # List of specific columns to select from right DataFrame
    right_columns = [
        # col("right.machine_id"),
        col("right.age"),
        col("right.gender"),
        col("right.hh_income"),
        col("right.hh_size"),
        col("right.children_present"),
        col("right.region"),
        col("right.ethnicity_id"),
        col("right.race_id")
    ]

    # Perform join and select desired columns
    mobile_traffic = mobile_traffic.join(mobile_demographic, on=join_condition, how="left").select(*left_columns, *right_columns)

    # Join 3 with browser lookup
    mobile_traffic = mobile_traffic.join(broadcast(browser_lookup), on="browser_type", how="left")

    mobile_traffic = sanitize_column_names(mobile_traffic)

    return mobile_traffic