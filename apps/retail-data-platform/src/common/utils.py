from pyspark.sql import SparkSession


def get_pipeline_config(key: str, default: str = None) -> str:
    """
    Safely retrieves a DLT pipeline configuration parameter.
    If the parameter is not found (e.g. running in local PyTest without DLT context),
    it returns the default value.
    """
    spark = SparkSession.builder.getOrCreate()
    try:
        # Databricks DLT injects pipeline variables into the Spark config
        # but they are accessed via spark.conf or dlt.spark.conf depending on context.
        # Alternatively, DLT parameters can be accessed without prefix in older versions,
        # or with 'spark.dlt.' prefix. We will just use standard spark.conf.get.
        val = spark.conf.get(key)
        # If it returns empty string instead of exception, handle it
        if not val and default is not None:
            return default
        return val
    except Exception:
        return default


def get_bronze_expectations() -> dict:
    """
    Returns standard expectations for the bronze layer data quality.
    Records that fail these expectations will be tracked in the DLT event log.
    """
    return {
        # Ensures that the ADF payload includes an operation type for downstream Silver CDC merges
        "valid_operation_type": "operation_type IN ('INSERT', 'UPDATE', 'DELETE', 'SNAPSHOT')",
        # Ensures our ingestion metadata was successfully appended
        "valid_ingestion_metadata": "_source_file_path IS NOT NULL AND ingest_timestamp IS NOT NULL",
    }
