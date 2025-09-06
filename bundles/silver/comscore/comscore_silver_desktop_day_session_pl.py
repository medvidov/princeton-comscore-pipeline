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
def comscore_sl_desktop__day_session():
    # Get config variables
    catalog = spark.conf.get("catalog")
    schema_bronze = spark.conf.get("schema_bronze")
    bronze_desktop_day_session = spark.conf.get("desktop_day_session_table") # Dekstop traffic
    bronze_desktop_demo = spark.conf.get("desktop_combined_demographic_table") # Desktop person demo
    bronze_time_lookup = spark.conf.get("time_lookup_table") # Time

    # Define full table paths
    input_desktop_day_session = f"{catalog}.{schema_bronze}.{bronze_desktop_day_session}" # Dekstop traffic
    input_desktop_demo = f"{catalog}.{schema_bronze}.{bronze_desktop_demo}" # Desktop person demo
    input_time_lookup = f"{catalog}.{schema_bronze}.{bronze_time_lookup}" # Time
    traffic_category = spark.table('qa_comscore.silver.comscore_sl_traffic_category_map')

    # Read tables
    desktop_day_session = (spark.table(input_desktop_day_session))
    desktop_demo = (spark.table(input_desktop_demo))
    time_lookup = (spark.table(input_time_lookup))

    # Drop metadata columns for joins
    time_lookup = drop_metadata_columns(time_lookup)
    desktop_demo = drop_metadata_columns(desktop_demo)
    traffic_category = drop_metadata_columns(traffic_category)

    # Join 1 on time due to needing times for other joins
    desktop_day_session = desktop_day_session.join(broadcast(time_lookup),on='time_id',how='left')

    # Join 2 with traffic category
    desktop_day_session = desktop_day_session.alias("left")
    traffic_category = traffic_category.alias("right")

    # Join condition
    join_condition = (
        (col("left.month_num") == col("right.month_id")) &
        (col("left.pattern_id") == col("right.pattern_id"))
    )

    # List of columns to select from left DataFrame
    left_columns = [col(f"left.{c}") for c in desktop_day_session.columns]

    # List of specific columns to select from right DataFrame
    rightcolumns = [col(f"right.{c}") for c in traffic_category.columns if c != 'pattern_id']

    desktop_day_session = desktop_day_session.join(traffic_category,on=join_condition,how='left').select(*left_columns, *rightcolumns)

    # Join 3 with demographic table
    desktop_day_session = desktop_day_session.alias("left")
    desktop_demo = desktop_demo.alias("right")

    join_condition = (
        (col("left.month_num") == col("right.month_id")) & (
            (col("left.machine_id") == col("right.machine_id")) | (col("left.person_id") == col("right.person_id"))
        )
    )

    # List of columns to select from left DataFrame
    left_columns = [col(f"left.{c}") for c in desktop_day_session.columns]

    # List of specific columns to select from right DataFrame
    right_columns = [col(f"right.{c}") for c in desktop_demo.columns if c!= "person_id" and c!= "machine_id" and c!= "month_id"]


    desktop_day_session = desktop_day_session.join(desktop_demo,on=join_condition,how='left').select(*left_columns, *right_columns)

    desktop_day_session = sanitize_column_names(desktop_day_session)

    return desktop_day_session