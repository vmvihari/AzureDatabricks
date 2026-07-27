import os
import sys

import dlt
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name

# Add the project root to the python path so we can import src.common.utils
# This is required because Databricks runs the file as a script in its own context.
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from src.common.utils import get_bronze_expectations, get_pipeline_config  # noqa: E402

spark = SparkSession.builder.getOrCreate()

# Retrieve configuration injected by the Databricks Asset Bundle (DAB) pipeline configuration
adf_drop_zone_path = get_pipeline_config("ADF_DROP_ZONE_PATH", "/tmp/mock_adls_path")
expectations = get_bronze_expectations()

# In a real retail scenario, these tables represent core operational entities
source_tables = ["orders", "customers", "inventory"]

for table in source_tables:
    # We define a function generator pattern to create multiple DLT tables dynamically
    def define_bronze_table(table_name):
        @dlt.table(
            name=f"bronze_{table_name}",
            comment=f"Raw Bronze CDC data for {table_name}, ingested via Auto Loader from ADF drop zone.",
            table_properties={
                "quality": "bronze",
                "pipelines.autoOptimize.managed": "true",
            },
        )
        @dlt.expect_all(
            expectations
        )  # We track violations but do not drop records in Bronze to preserve raw fidelity
        def read_bronze():
            return (
                spark.readStream.format("cloudFiles")
                # We assume ADF drops the data in efficient Parquet format
                .option("cloudFiles.format", "parquet")
                # Infer schema and automatically handle any new columns added in the source database
                .option("cloudFiles.inferColumnTypes", "true")
                .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
                .load(f"{adf_drop_zone_path}/{table_name}")
                # Append ingestion metadata for traceability and troubleshooting
                .withColumn("ingest_timestamp", current_timestamp())
                .withColumn("_source_file_path", input_file_name())
            )

        return read_bronze

    # Execute the generator to register the table in the DLT DAG
    define_bronze_table(table)
