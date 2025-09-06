import json
import os
from pathlib import Path
from typing import Dict, Optional

from pyspark.sql.types import StructType


def get_schema(spark, schema_path: str) -> StructType:
    # Read the schema file into a DataFrame; it will have a single column "value"
    df = spark.read.text(schema_path)
    # Extract the lines from the "value" column
    schema_lines = [row.value for row in df.collect()]
    # Join the lines into a single string
    schema_str = "\n".join(schema_lines)
    # Parse the JSON string to a Python dict
    schema_dict = json.loads(schema_str)
    # Convert the dictionary to a StructType using fromJson
    schema = StructType.fromJson(schema_dict)

    return schema


def load_sql(filename: str, params: Optional[Dict[str, str]] = None) -> str:
    """
    Load SQL from a file and optionally substitute parameters.

    Args:
        filename: Name of the SQL file in the sql/views directory
        params: Optional dictionary of parameters to substitute

    Returns:
        The SQL string with any parameters substituted
    """
    try:
        with open(filename, 'r') as f:
            sql = f.read()

        if params:
            sql = sql.format(**params)

        return sql
    except FileNotFoundError:
        raise FileNotFoundError(f"SQL file not found: {filename}")
    except KeyError as e:
        raise ValueError(f"Missing required parameter: {str(e)}")


def get_all_files(dbutils, path, extensions="jsonl"):
    """
    Recursively traverses the given directory path on DBFS and returns a list
    of FileInfo objects for files that match the given extension(s).

    Args:
        dbutils: Databricks dbutils object.
        path (str): The root directory path to search.
        extensions (str or list): A single file extension (e.g., "jsonl") or a list of
                                  extensions (e.g., ["jsonl", "txt"]). The check is case-insensitive.

    Returns:
        List[FileInfo]: A list of DBFS FileInfo objects for each file found that matches the extension(s).
    """
    # Ensure we have a list of extensions in lowercase
    if isinstance(extensions, str):
        ext_list = [extensions.lower()]
    else:
        ext_list = [ext.lower() for ext in extensions]

    matching_files = []
    try:
        items = dbutils.fs.ls(path)
    except Exception as e:
        print(f"Error reading path {path}: {e}")
        return matching_files

    for item in items:
        if item.isDir():
            # Recurse into subdirectories
            matching_files.extend(get_all_files(dbutils=dbutils, path=item.path, extensions=extensions))
        else:
            # Check if the file's name ends with any of the desired extensions
            file_name = item.name.lower()
            for ext in ext_list:
                if file_name.endswith(f".{ext}"):
                    matching_files.append(item)
                    break  # No need to check other extensions if one matches

    return matching_files
