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
    name="comscore_android_traffic",
    comment="Raw Comscore Android traffic data ingested from .csv files",
    partition_cols=["time_id"],
    table_properties={
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "quality": "bronze"
    },
)
def comscore_android_traffic():
    raw_data_path = spark.conf.get("android_data_path")
    file_pattern = spark.conf.get("file_pattern")
    format = spark.conf.get("file_format")
    sourcing_system = spark.conf.get("sourcing_system")  # Comscore
    sampling_method = spark.conf.get("sampling_method")  # N/A
    ingest_platform = spark.conf.get("ingest_platform")  # Comscore
    platform_code = spark.conf.get("platform_code")  # CS
    schema_path = spark.conf.get("schema_path")
    schema = get_schema(spark, schema_path)
    ingest_id = spark.sql("SELECT date_format(current_timestamp(), 'yyyyMMddHHmmss')").collect()[0][0]

    try:
        df = (
            spark.readStream.format("cloudFiles")
            .schema(schema)
            .option("cloudFiles.format", format)
            .option("recursiveFileLookup", "true")
            .option("pathGlobFilter", file_pattern)
            .option("multiLine", "true")
            .option("sep", "\t")
            .option("ignoreCorruptFiles", "true")
            .option("ignoreMissingFiles", "true")
            .option("badRecordsPath", "dbfs:/Volumes/dev_catalog/default/comscore_raw/android_traffic/")
            .load(raw_data_path)
            .withColumn("comscore_platform", lit("Android"))
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

@dlt.table(
    name="comscore_ios_traffic",
    comment="Raw Comscore iOS traffic data ingested from .csv files",
    partition_cols=["time_id"],
    table_properties={
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "quality": "bronze"
    },
)
def comscore_ios_traffic():
    raw_data_path = spark.conf.get("ios_data_path")
    file_pattern = spark.conf.get("file_pattern")
    format = spark.conf.get("file_format")
    sourcing_system = spark.conf.get("sourcing_system")  # Comscore
    sampling_method = spark.conf.get("sampling_method")  # N/A
    ingest_platform = spark.conf.get("ingest_platform")  # Comscore
    platform_code = spark.conf.get("platform_code")  # CS
    schema_path = spark.conf.get("schema_path")
    schema = get_schema(spark, schema_path)
    ingest_id = spark.sql("SELECT date_format(current_timestamp(), 'yyyyMMddHHmmss')").collect()[0][0]

    try:
        df = (
            spark.readStream.format("cloudFiles")
            .schema(schema)
            .option("cloudFiles.format", format)
            .option("recursiveFileLookup", "true")
            .option("pathGlobFilter", file_pattern)
            .option("multiLine", "true")
            .option("sep", "\t")
            .option("ignoreCorruptFiles", "true")
            .option("ignoreMissingFiles", "true")
            .option("badRecordsPath", "dbfs:/Volumes/dev_catalog/default/comscore_raw/ios_traffic/")
            .load(raw_data_path)
            .withColumn("comscore_platform", lit("iOS"))
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


@dlt.table(
    name="comscore_mobile_day_session",
    comment="Raw Comscore desktop traffic data ingested from .csv files",
    partition_cols=["time_id"],
    table_properties={
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "quality": "bronze"
    },
)
def comscore_mobile_day_session():
    raw_data_path = spark.conf.get("mobile_day_session_data_path")
    file_pattern = "*.txt.gz"
    format = spark.conf.get("file_format")
    sourcing_system = spark.conf.get("sourcing_system")  # Comscore
    sampling_method = spark.conf.get("sampling_method")  # N/A
    ingest_platform = spark.conf.get("ingest_platform")  # Comscore
    platform_code = spark.conf.get("platform_code")  # CS
    schema_path = "dbfs:/Volumes/dev_catalog/default/comscore_raw/comscore_mobile_day_session_schema.json"
    schema = get_schema(spark, schema_path)
    ingest_id = spark.sql("SELECT date_format(current_timestamp(), 'yyyyMMddHHmmss')").collect()[0][0]

    try:
        df = (
            spark.readStream.format("cloudFiles")
            .schema(schema)
            .option("cloudFiles.format", format)
            .option("recursiveFileLookup", "true")
            .option("pathGlobFilter", file_pattern)
            .option("multiLine", "true")
            .option("sep", "\t")
            .option("ignoreCorruptFiles", "true")
            .option("ignoreMissingFiles", "true")
            # .option("badRecordsPath", "dbfs:/Volumes/dev_catalog/default/comscore_raw/mobile_day_session/")
            .load(raw_data_path)
            .withColumn("file_name", col("_metadata.file_name"))
            .withColumn("file_path", col("_metadata.file_path"))
            .withColumn("file_size", col("_metadata.file_size"))
            .withColumn("sourcing_system", lit(sourcing_system))
            .withColumn("sampling_method", lit(sampling_method))
            .withColumn("ingest_id", concat(lit(platform_code), lit(ingest_id)))
            .withColumn("ingest_platform", lit(ingest_platform))
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


@dlt.table(
    name="comscore_mobile_demographic",
    comment="ComScore time lookups",
    partition_cols=["month_id"],
    table_properties={
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "quality": "bronze"
    },
)
def comscore_mobile_demographic():
    raw_data_path = "dbfs:/Volumes/raw_ingest_catalog/default/comscore/US/mobile_demographics/"
    file_pattern = "*.txt.gz"
    format = spark.conf.get("file_format")
    sourcing_system = spark.conf.get("sourcing_system")  # Comscore
    sampling_method = spark.conf.get("sampling_method")  # N/A
    ingest_platform = spark.conf.get("ingest_platform")  # Comscore
    platform_code = spark.conf.get("platform_code")  # CS
    schema_path = "dbfs:/Volumes/dev_catalog/default/comscore_raw/comscore_mobile_demographics_schema.json"
    schema = get_schema(spark, schema_path)
    ingest_id = spark.sql("SELECT date_format(current_timestamp(), 'yyyyMMddHHmmss')").collect()[0][0]

    try:
        df = (
            spark.readStream.format("cloudFiles")
            .schema(schema)
            .option("cloudFiles.format", format)
            .option("recursiveFileLookup", "true")
            .option("pathGlobFilter", file_pattern)
            .option("multiLine", "true")
            .option("sep", "\t")
            .option("ignoreCorruptFiles", "true")
            .option("ignoreMissingFiles", "true")
            .option("badRecordsPath", "dbfs:/Volumes/dev_catalog/default/comscore_raw/mobile_demographic/")
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
            .withColumn("last_scraped", lit(ingest_id))
        )
        spark.conf.set("sourcing_status", "SUCCESS")
        spark.conf.set("failure_reason", "")
    except Exception as e:
        spark.conf.set("sourcing_status", "FAILURE")
        spark.conf.set("failure_reason", str(e))
        raise e

    return df

# @dlt.table(
#     comment="Log of Comscore ingests",
#     table_properties={"quality": "bronze"},
# )
# def ingest_log_comscore():
#     df_tg_ingest = dlt.read(spark.conf.get("table_name"))
#     return df_tg_ingest.select(
#         "file_name",
#         "file_path",
#         "file_size",
#         "sourcing_system",
#         "sampling_method",
#         "platform",
#         "sourcing_status",
#         "failure_reason",
#         "ingest_id",
#         "ingestion_timestamp",
#         "scrape_timestamp",
#     ).groupBy(
#             "file_name",
#             "file_path",
#             "file_size",
#             "sourcing_system",
#             "sampling_method",
#             "platform",
#             "sourcing_status",
#             "failure_reason",
#             "ingest_id",
#             "ingestion_timestamp",
#             "scrape_timestamp"
#         ).count().withColumnRenamed("count", "ingested_record_count")
