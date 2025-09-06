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
    input_file_name,
    lit,
    pandas_udf,
    unix_timestamp,
)
from pyspark.sql.types import StringType
from pyspark.sql.utils import AnalysisException

sys.path.append("../../../src")
from utils import get_schema, load_sql


@dlt.table(
    name="comscore_desktop_day_session",
    comment="Raw Comscore desktop traffic data ingested from .csv files",
    partition_cols=["time_id"],
    table_properties={
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact":   "true",
        "quality": "bronze"
    },
)
def comscore_desktop_day_session():
    raw_data_path = spark.conf.get("desktop_day_session_data_path")
    text_file_pattern = '*.txt.gz*'
    format = spark.conf.get("file_format")
    sourcing_system = spark.conf.get("sourcing_system")
    sampling_method = spark.conf.get("sampling_method")
    ingest_platform = spark.conf.get("ingest_platform")
    platform_code = spark.conf.get("platform_code")
    schema_path = spark.conf.get("schema_path")
    schema = get_schema(spark, schema_path)
    ingest_id = spark.sql("SELECT date_format(current_timestamp(), 'yyyyMMddHHmmss')").collect()[0][0]

    try:
        df = (
            spark.readStream.format("cloudFiles")
            .schema(schema)
            .option("cloudFiles.format", format)
            .option("recursiveFileLookup", "true")
            .option("pathGlobFilter", text_file_pattern)
            .option("multiLine", "true")
            .option("sep", "\t")
            .option("ignoreCorruptFiles", "true")
            .option("ignoreMissingFiles", "true")
            #.option("badRecordsPath", "dbfs:/Volumes/dev_catalog/default/comscore_raw/desktop_day_session/")
            .load(raw_data_path)
            .withColumn("file_name", col("_metadata.file_name"))
            .withColumn("file_path", col("_metadata.file_path"))
            .withColumn("file_size", col("_metadata.file_size"))
            .withColumn("sourcing_system", lit(sourcing_system))
            .withColumn("sampling_method", lit(sampling_method))
            .withColumn("ingest_platform", lit(ingest_platform))
            .withColumn("ingest_id", concat(lit(platform_code), lit(ingest_id)))
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