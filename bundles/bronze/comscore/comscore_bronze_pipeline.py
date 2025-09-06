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

sys.path.append("../../../src") # HACK: fix this
import get_schema, load_sql

# TODO: Put schema paths into bundle.yml
# TODO: Integrate all tables into ingest log

# 1. Raw Android data
@dlt.table(
    name="comscore_bz_android_traffic",
    comment="Raw Comscore Android traffic data ingested from .csv files",
    partition_cols=["time_id"],
    table_properties={
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact":   "true",
        "quality": "bronze"
    },
)
def comscore_bz_android_traffic():
    raw_data_path = spark.conf.get("android_data_path")
    file_pattern = spark.conf.get("file_pattern")
    format = spark.conf.get("file_format")
    sourcing_system = spark.conf.get("sourcing_system") #Comscore
    sampling_method = spark.conf.get("sampling_method") #N/A
    ingest_platform = spark.conf.get("ingest_platform") #Comscore
    platform_code = spark.conf.get("platform_code") #CS
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

# 2. Raw iOS data
@dlt.table(
    name="comscore_bz_ios_traffic",
    comment="Raw Comscore iOS traffic data ingested from .csv files",
    partition_cols=["time_id"],
    table_properties={
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact":   "true",
        "quality": "bronze"
    },
)
def comscore_bz_ios_traffic():
    raw_data_path = spark.conf.get("ios_data_path")
    file_pattern = spark.conf.get("file_pattern")
    format = spark.conf.get("file_format")
    sourcing_system = spark.conf.get("sourcing_system") #Comscore
    sampling_method = spark.conf.get("sampling_method") #N/A
    ingest_platform = spark.conf.get("ingest_platform") #Comscore
    platform_code = spark.conf.get("platform_code") #CS
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

# 3. Raw desktop data
@dlt.table(
    name="comscore_bz_desktop_traffic",
    comment="Raw Comscore desktop traffic data ingested from .csv files",
    partition_cols=["time_id"],
    table_properties={
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact":   "true",
        "quality": "bronze"
    },
)
def comscore_bz_desktop_traffic():
    raw_data_path = spark.conf.get("desktop_data_path")
    file_pattern = spark.conf.get("file_pattern")
    format = spark.conf.get("file_format")
    sourcing_system = spark.conf.get("sourcing_system") #Comscore
    sampling_method = spark.conf.get("sampling_method") #N/A
    ingest_platform = spark.conf.get("ingest_platform") #Comscore
    platform_code = spark.conf.get("platform_code") #CS
    schema_path = "dbfs:/Volumes/dev_catalog/default/comscore_raw/comscore_desktop_traffic_schema.json"
    schema = get_schema(spark, schema_path)
    ingest_id = spark.sql("SELECT date_format(current_timestamp(), 'yyyyMMddHHmmss')").collect()[0][0]

    try:
        df = (
            spark.readStream.format("cloudFiles")
            .schema(schema)
            .option("cloudFiles.format", format)
            # .option("cloudFiles.maxBytesPerTrigger", "512MB")
            # .option("cloudFiles.maxFilesPerTrigger", "5000")
            .option("recursiveFileLookup", "true")
            .option("pathGlobFilter", file_pattern)
            .option("multiLine", "true")
            .option("sep", "\t")
            .option("ignoreCorruptFiles", "true")
            .option("ignoreMissingFiles", "true")
            .option("badRecordsPath", "dbfs:/Volumes/dev_catalog/default/comscore_raw/desktop_traffic/")
            .option("nullValue", r"\\N")
            .load(raw_data_path)
            .withColumn("platform", lit("Desktop"))
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

        # df = df.repartition(100)  # adjust based on cluster size and executor cores

    except Exception as e:
        spark.conf.set("sourcing_status", "FAILURE")
        spark.conf.set("failure_reason", str(e))
        raise e

    return df

# 4. Desktop day session
@dlt.table(
    name="comscore_bz_desktop_day_session",
    comment="Raw Comscore desktop traffic data ingested from .csv files",
    partition_cols=["time_id"],
    table_properties={
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact":   "true",
        "quality": "bronze"
    },
)
def comscore_bz_desktop_day_session():
    raw_data_path = spark.conf.get("desktop_day_session_data_path")
    file_pattern = spark.conf.get("file_pattern")
    format = spark.conf.get("file_format")
    sourcing_system = spark.conf.get("sourcing_system") #Comscore
    sampling_method = spark.conf.get("sampling_method") #N/A
    ingest_platform = spark.conf.get("ingest_platform") #Comscore
    platform_code = spark.conf.get("platform_code") #CS
    schema_path = "dbfs:/Volumes/dev_catalog/default/comscore_raw/comscore_desktop_day_session_schema.json"
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
            # .option("badRecordsPath", "dbfs:/Volumes/dev_catalog/default/comscore_raw/desktop_day_session/")
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

# 5. Mobile day session
@dlt.table(
    name="comscore_bz_mobile_day_session",
    comment="Raw Comscore desktop traffic data ingested from .csv files",
    partition_cols=["time_id"],
    table_properties={
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact":   "true",
        "quality": "bronze"
    },
)
def comscore_bz_mobile_day_session():
    raw_data_path = spark.conf.get("mobile_day_session_data_path")
    file_pattern = "*.txt.gz"
    format = spark.conf.get("file_format")
    sourcing_system = spark.conf.get("sourcing_system") #Comscore
    sampling_method = spark.conf.get("sampling_method") #N/A
    ingest_platform = spark.conf.get("ingest_platform") #Comscore
    platform_code = spark.conf.get("platform_code") #CS
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

# 6. Desktop search click
@dlt.table(
    name="comscore_bz_desktop_search_click",
    comment="Raw Comscore desktop click traffic data ingested from .txt files",
    partition_cols=["week_id"],
    table_properties={
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact":   "true",
        "quality": "bronze"
    },
)
def comscore_bz_desktop_search_click():
    raw_data_path = spark.conf.get("desktop_search_click_data_path")
    file_pattern = "*.txt.gz"
    format = spark.conf.get("file_format")
    sourcing_system = spark.conf.get("sourcing_system") #Comscore
    sampling_method = spark.conf.get("sampling_method") #N/A
    ingest_platform = spark.conf.get("ingest_platform") #Comscore
    platform_code = spark.conf.get("platform_code") #CS
    schema_path = "dbfs:/Volumes/dev_catalog/default/comscore_raw/comscore_desktop_search_click_schema.json"
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
            .option("badRecordsPath", "dbfs:/Volumes/dev_catalog/default/comscore_raw/desktop_search_click/")
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

# 7. Desktop search fact
@dlt.table(
    name="comscore_bz_desktop_search_fact",
    comment="Raw Comscore desktop click traffic data ingested from .txt files",
    partition_cols=["week_id"],
    table_properties={
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact":   "true",
        "quality": "bronze"
    },
)
def comscore_bz_desktop_search_fact():
    raw_data_path = spark.conf.get("desktop_search_fact_data_path")
    file_pattern = "*.txt.gz"
    format = spark.conf.get("file_format")
    sourcing_system = spark.conf.get("sourcing_system") #Comscore
    sampling_method = spark.conf.get("sampling_method") #N/A
    ingest_platform = spark.conf.get("ingest_platform") #Comscore
    platform_code = spark.conf.get("platform_code") #CS
    schema_path = "dbfs:/Volumes/dev_catalog/default/comscore_raw/comscore_desktop_search_fact_schema.json"
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
            .option("quote", "\"")
            .option("escape", "\\")  # Tells parser to treat \" correctly
            .option("badRecordsPath", "dbfs:/Volumes/dev_catalog/default/comscore_raw/desktop_search_fact/")
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

# TODO: Replace hard coded values with spark.conf.get and add to bundle.yml
# Small table, no read optimizations needed
# 8. Time lookup
@dlt.table(
    name="comscore_bz_time_lookup",
    comment="ComScore time lookups",
    table_properties={"quality": "bronze"},
)
def comscore_bz_time_lookup():
    raw_data_path = spark.conf.get("time_lookup_data_path")
    file_pattern = "*.txt.gz"
    format = spark.conf.get("file_format")
    sourcing_system = spark.conf.get("sourcing_system") #Comscore
    sampling_method = spark.conf.get("sampling_method") #N/A
    ingest_platform = spark.conf.get("ingest_platform") #Comscore
    platform_code = spark.conf.get("platform_code") #CS
    schema_path = "dbfs:/Volumes/dev_catalog/default/comscore_raw/comscore_time_lookup_schema.json"
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
            .option("badRecordsPath", "dbfs:/Volumes/dev_catalog/default/comscore_raw/time_lookup/")
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

# 9. Desktop web entity
@dlt.table(
    name="comscore_bz_desktop_web_entity",
    comment="Raw Comscore desktop click traffic data ingested from .txt files",
    partition_cols=["week_id"],
    table_properties={
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact":   "true",
        "quality": "bronze"
    },
)
def comscore_bz_desktop_web_entity():
    raw_data_path = spark.conf.get("desktop_web_entity_data_path")
    file_pattern = "*.txt.gz"
    format = spark.conf.get("file_format")
    sourcing_system = spark.conf.get("sourcing_system") #Comscore
    sampling_method = spark.conf.get("sampling_method") #N/A
    ingest_platform = spark.conf.get("ingest_platform") #Comscore
    platform_code = spark.conf.get("platform_code") #CS
    schema_path = "dbfs:/Volumes/dev_catalog/default/comscore_raw/comscore_desktop_web_entity_schema.json"
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
            .option("badRecordsPath", "dbfs:/Volumes/dev_catalog/default/comscore_raw/desktop_web_entity/")
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

# 10. Traffic category lookup
@dlt.table(
    name="comscore_bz_traffic_category_lookup",
    comment="Raw Comscore desktop click traffic data ingested from .txt files",
    partition_cols=["month_id"],
    table_properties={
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact":   "true",
        "quality": "bronze"
    },
)
def comscore_bz_desktop_web_entity():
    raw_data_path = "dbfs:/Volumes/raw_ingest_catalog/default/comscore/Lookups/traffic_category_map/"
    file_pattern = "*.txt.gz"
    format = spark.conf.get("file_format")
    sourcing_system = spark.conf.get("sourcing_system") #Comscore
    sampling_method = spark.conf.get("sampling_method") #N/A
    ingest_platform = spark.conf.get("ingest_platform") #Comscore
    platform_code = spark.conf.get("platform_code") #CS
    schema_path = "dbfs:/Volumes/dev_catalog/default/comscore_raw/comscore_traffic_category_schema.json"
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
            .option("badRecordsPath", "dbfs:/Volumes/dev_catalog/default/comscore_raw/desktop_web_entity/")
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

# 11. Desktop OS lookup
# Small table, no read optimizations needed
@dlt.table(
    name="comscore_bz_desktop_os_lookup",
    comment="ComScore time lookups",
    table_properties={"quality": "bronze"},
)
def comscore_bz_desktop_os_lookup():
    raw_data_path = "dbfs:/Volumes/raw_ingest_catalog/default/comscore/Lookups/desktop_OS_lookup/"
    file_pattern = "*.txt.gz"
    format = spark.conf.get("file_format")
    sourcing_system = spark.conf.get("sourcing_system") #Comscore
    sampling_method = spark.conf.get("sampling_method") #N/A
    ingest_platform = spark.conf.get("ingest_platform") #Comscore
    platform_code = spark.conf.get("platform_code") #CS
    schema_path = "dbfs:/Volumes/dev_catalog/default/comscore_raw/comscore_desktop_os_lookup_schema.json"
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
            .option("badRecordsPath", "dbfs:/Volumes/dev_catalog/default/comscore_raw/desktop_os_lookup/")
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

# 12. Browser type lookup
# Small table, no read optimizations needed
@dlt.table(
    name="comscore_bz_browser_lookup",
    comment="ComScore time lookups",
    table_properties={"quality": "bronze"},
)
def comscore_bz_browser_lookup():
    raw_data_path = "dbfs:/Volumes/raw_ingest_catalog/default/comscore/Lookups/browser_type/"
    file_pattern = "*.txt.gz"
    format = spark.conf.get("file_format")
    sourcing_system = spark.conf.get("sourcing_system") #Comscore
    sampling_method = spark.conf.get("sampling_method") #N/A
    ingest_platform = spark.conf.get("ingest_platform") #Comscore
    platform_code = spark.conf.get("platform_code") #CS
    schema_path = "dbfs:/Volumes/dev_catalog/default/comscore_raw/comscore_browser_lookup_schema.json"
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
            .option("badRecordsPath", "dbfs:/Volumes/dev_catalog/default/comscore_raw/browser_type_lookup/")
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

# 13. Desktop demographic
@dlt.table(
    name="comscore_bz_desktop_person_demographic",
    comment="ComScore time lookups",
    partition_cols=["month_id"],
    table_properties={
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact":   "true",
        "quality": "bronze"
    },
)
def comscore_bz_desktop_person_demographic():
    raw_data_path = "dbfs:/Volumes/raw_ingest_catalog/default/comscore/US/desktop_demographics/"
    file_pattern = "*.txt.gz"
    format = spark.conf.get("file_format")
    sourcing_system = spark.conf.get("sourcing_system") #Comscore
    sampling_method = spark.conf.get("sampling_method") #N/A
    ingest_platform = spark.conf.get("ingest_platform") #Comscore
    platform_code = spark.conf.get("platform_code") #CS
    schema_path = "dbfs:/Volumes/dev_catalog/default/comscore_raw/comscore_desktop_person_demographics_schema.json"
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
            .option("badRecordsPath", "dbfs:/Volumes/dev_catalog/default/comscore_raw/desktop_demographic/")
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

# 13. Desktop demographic
@dlt.table(
    name="comscore_bz_desktop_machine_demographic",
    comment="ComScore time lookups",
    partition_cols=["month_id"],
    table_properties={
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact":   "true",
        "quality": "bronze"
    },
)
def comscore_bz_desktop_machine_demographic():
    raw_data_path = "dbfs:/Volumes/raw_ingest_catalog/default/comscore/US/desktop_demographics/"
    file_pattern = "*.txt.gz"
    format = spark.conf.get("file_format")
    sourcing_system = spark.conf.get("sourcing_system") #Comscore
    sampling_method = spark.conf.get("sampling_method") #N/A
    ingest_platform = spark.conf.get("ingest_platform") #Comscore
    platform_code = spark.conf.get("platform_code") #CS
    schema_path = "dbfs:/Volumes/dev_catalog/default/comscore_raw/comscore_desktop_machine_demographics_schema.json"
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
            .option("badRecordsPath", "dbfs:/Volumes/dev_catalog/default/comscore_raw/desktop_demographic/")
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

# 14. Mobile demographic
@dlt.table(
    name="comscore_bz_mobile_demographic",
    comment="ComScore time lookups",
    partition_cols=["month_id"],
    table_properties={
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact":   "true",
        "quality": "bronze"
    },
)
def comscore_bz_mobile_demographic():
    raw_data_path = "dbfs:/Volumes/raw_ingest_catalog/default/comscore/US/mobile_demographics/"
    file_pattern = "*.txt.gz"
    format = spark.conf.get("file_format")
    sourcing_system = spark.conf.get("sourcing_system") #Comscore
    sampling_method = spark.conf.get("sampling_method") #N/A
    ingest_platform = spark.conf.get("ingest_platform") #Comscore
    platform_code = spark.conf.get("platform_code") #CS
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

# TODO: Log ingest
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
