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

def sanitize_column_names(df: DataFrame) -> DataFrame:
    """
    Returns a new DataFrame with all column names lowercased and spaces replaced with underscores.
    """
    renamed_cols = [col(c).alias(c.lower().replace(" ", "_")) for c in df.columns]
    return df.select(renamed_cols)

@dlt.table()
def comscore_sl_desktop_traffic():
    # Get config variables
    catalog = spark.conf.get("catalog")
    schema_bronze = spark.conf.get("schema_bronze")
    bronze_desktop_traffic = spark.conf.get("desktop_traffic_table") # Dekstop traffic
    bronze_desktop_demo = spark.conf.get("desktop_combined_demographic_table") # Desktop person demo
    bronze_os_lookup = spark.conf.get("desktop_os_lookup_table") # Desktop OS
    bronze_time_lookup = spark.conf.get("time_lookup_table") # Time

    # Define full table paths
    input_desktop_traffic = f"{catalog}.{schema_bronze}.{bronze_desktop_traffic}" # Dekstop traffic
    input_desktop_demo = f"{catalog}.{schema_bronze}.{bronze_desktop_demo}" # Desktop person demo
    input_os_lookup = f"{catalog}.{schema_bronze}.{bronze_os_lookup}" # Desktop OS
    input_time_lookup = f"{catalog}.{schema_bronze}.{bronze_time_lookup}" # Time
    traffic_category = spark.table('qa_comscore.silver.comscore_sl_traffic_category_map')

    # Read tables
    desktop_traffic = (spark.table(input_desktop_traffic))
    desktop_demo = (spark.table(input_desktop_demo))
    os_lookup = (spark.table(input_os_lookup))
    time_lookup = (spark.table(input_time_lookup))

    # Drop metadata columns for joins
    time_lookup = drop_metadata_columns(time_lookup)
    os_lookup = drop_metadata_columns(os_lookup)
    desktop_demo = drop_metadata_columns(desktop_demo)
    traffic_category = drop_metadata_columns(traffic_category)

    # Join 1 on time due to needing times for other joins
    desktop_traffic = desktop_traffic.join(broadcast(time_lookup),on='time_id',how='left')

    # Join 2 with traffic category
    desktop_traffic = desktop_traffic.alias("left")
    traffic_category = traffic_category.alias("right")

    # Join condition
    join_condition = (
        (col("left.month_num") == col("right.month_id")) &
        (col("left.pattern_id") == col("right.pattern_id"))
    )

    # List of columns to select from left DataFrame
    left_columns = [col(f"left.{c}") for c in desktop_traffic.columns]

    # List of specific columns to select from right DataFrame
    rightcolumns = [col(f"right.{c}") for c in traffic_category.columns if c != 'pattern_id']

    desktop_traffic = desktop_traffic.join(traffic_category,on=join_condition,how='left').select(*left_columns, *rightcolumns)

    # Join 3 with demographic table
    desktop_traffic = desktop_traffic.alias("left")
    desktop_demo = desktop_demo.alias("right")

    join_condition = (
        (col("left.month_num") == col("right.month_id")) & (
            (col("left.machine_id") == col("right.machine_id")) | (col("left.person_id") == col("right.person_id"))
        )
    )

    # List of columns to select from left DataFrame
    left_columns = [col(f"left.{c}") for c in desktop_traffic.columns]

    # List of specific columns to select from right DataFrame
    right_columns = [col(f"right.{c}") for c in desktop_demo.columns if c!= "person_id" and c!= "machine_id" and c!= "month_id"]


    desktop_traffic = desktop_traffic.join(desktop_demo,on=join_condition,how='left').select(*left_columns, *right_columns)

    # Join 4 with OS lookup
    desktop_traffic = desktop_traffic.alias("left")
    os_lookup = os_lookup.alias("right")

    # Join condition
    join_condition = (
        (col("left.month_num") == col("right.month_id")) &
        (col("left.machine_id") == col("right.machine_id"))
    )

    # List of columns to select from left DataFrame
    left_columns = [col(f"left.{c}") for c in desktop_traffic.columns]

    # List of specific columns to select from right DataFrame
    right_columns = [
        col("right.os_version_name")
    ]

    desktop_traffic = desktop_traffic.join(broadcast(os_lookup),on=join_condition,how='left').select(*left_columns, *right_columns)

    desktop_traffic = sanitize_column_names(desktop_traffic)

    return desktop_traffic

