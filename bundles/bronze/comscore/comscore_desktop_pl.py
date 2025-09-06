import sys

import dlt
import pandas as pd
from pyspark.sql import Row
from pyspark.sql.functions import (
    col,
    concat,
    count,
    current_date,
    current_timestamp,
    date_format,
    input_file_name,
    lit,
    pandas_udf,
    regexp_extract,
    unix_timestamp,
)
from pyspark.sql.types import StringType
from pyspark.sql.utils import AnalysisException

sys.path.append("../../../src") # HACK: fix this
from utils import get_schema, load_sql


@dlt.table(
    name="comscore_desktop_traffic",
    comment="Raw Comscore desktop traffic data ingested from .csv files",
    partition_cols=["year", "month", "time_id"],
    table_properties={
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "quality": "bronze"
    },
)
def comscore_desktop_traffic():
    raw_data_path = spark.conf.get("desktop_data_path")
    file_pattern = spark.conf.get("file_pattern")
    format = spark.conf.get("file_format")
    sourcing_system = spark.conf.get("sourcing_system")
    sampling_method = spark.conf.get("sampling_method")
    platform = spark.conf.get("ingest_platform")
    platform_code = spark.conf.get("platform_code")
    schema_path = "dbfs:/Volumes/dev_catalog/default/comscore_raw/comscore_desktop_traffic_schema.json"
    schema = get_schema(spark, schema_path)

    try:
        df = (
            spark.readStream.format("cloudFiles")
            .schema(schema)
            .option("header", "false")
            .option("cloudFiles.format", format)
            .option("recursiveFileLookup", "true")
            .option("pathGlobFilter", file_pattern)
            .option("multiLine", "false")
            .option("sep", "\t")
            .option("ignoreCorruptFiles", "true")
            .option("ignoreMissingFiles", "true")
            .option("badRecordsPath", "dbfs:/Volumes/dev_catalog/default/comscore_raw/desktop_traffic/")
            .option("nullValue", r"\\N")
            #.option("maxFilesPerTrigger", "100")
            .load(raw_data_path)
            .withColumn("comscore_platform", lit("Desktop"))
            .withColumn("file_name", col("_metadata.file_name"))
            .withColumn("file_path", col("_metadata.file_path"))
            .withColumn("year", regexp_extract(col("file_path"), r".*/(\d{4})/(\d{2})/.*", 1))
            .withColumn("month", regexp_extract(col("file_path"), r".*/(\d{4})/(\d{2})/.*", 2))
            .withColumn("file_size", col("_metadata.file_size"))
            .withColumn("sourcing_system", lit(sourcing_system))
            .withColumn("sampling_method", lit(sampling_method))
            .withColumn("ingest_id", concat(lit(platform_code), date_format(current_timestamp(), "yyyyMMddHHmmss")))
            .withColumn("ingest_platform", lit(platform))
            .withColumn("ingestion_timestamp", current_timestamp())
            .withColumn("scrape_timestamp", col("_metadata.file_modification_time"))
            .withColumn("sourcing_status", lit("SUCCESS"))
            .withColumn("failure_reason", lit(""))
        )
        spark.conf.set("sourcing_status", "SUCCESS")
        spark.conf.set("failure_reason", "")

    except Exception as e:
        spark.conf.set("sourcing_status", "FAILURE")
        spark.conf.set("failure_reason", str(e))
        raise e

    return df
