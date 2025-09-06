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
from utils import get_schema, load_sql

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
    file_pattern = "*person_demos*.txt.gz"
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
            .option("badRecordsPath", "dbfs:/Volumes/dev_catalog/default/comscore_raw/desktop_person_demographic/")
            .load(raw_data_path)
            .withColumn("month_id", col("month_id").cast("int"))
            .withColumn("original_table", lit("person_demos"))
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
    file_pattern = "*machine_demos*.txt.gz"
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
            .option("badRecordsPath", "dbfs:/Volumes/dev_catalog/default/comscore_raw/desktop_machine_demographic/")
            .load(raw_data_path)
            .withColumn("month_id", col("month_id").cast("int"))
            .withColumn("person_id", lit(None).cast("long"))
            .withColumn("gender", lit(None).cast("string"))
            .withColumn("ethnicity_id", lit(None).cast("string"))
            .withColumn("race_id", lit(None).cast("string"))
            .withColumn("original_table", lit("machine_demos"))
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

# combined desktop demographic table
@dlt.table(
    name="comscore_bz_desktop_combined_demographic",
    comment="Union of both desktop_demo tables",
    table_properties={
        "quality": "bronze"
    }
)
def comscore_bz_desktop_combined_demographic():
    df1 = dlt.read("comscore_bz_desktop_person_demographic")
    df2 = dlt.read("comscore_bz_desktop_machine_demographic")
    
    combined_df = df1.unionByName(df2, allowMissingColumns=True)
    return combined_df

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