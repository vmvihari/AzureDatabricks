import os
import shutil
import sys
import tempfile

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
os.environ["_JAVA_OPTIONS"] = "-Djava.net.preferIPv4Stack=true"


@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.appName("LocalTest").master("local[2]").getOrCreate()


@pytest.mark.skipif(
    sys.platform == "win32",
    reason="Spark local file I/O requires Hadoop winutils on Windows",
)
def test_cdc_ingestion_logic(spark):
    temp_dir = tempfile.mkdtemp()
    try:
        # 1. Arrange: Write a mock CSV to the temp directory simulating an ADF drop
        csv_content = "order_id,customer_id,total_amount,operation_type\nORD-1,CUST-1,150.50,INSERT\n"
        with open(os.path.join(temp_dir, "mock_orders.csv"), "w") as f:
            f.write(csv_content)

        order_schema = StructType(
            [
                StructField("order_id", StringType(), True),
                StructField("customer_id", StringType(), True),
                StructField("total_amount", DoubleType(), True),
                StructField("operation_type", StringType(), True),
            ]
        )

        # 2. Act: Simulate the Bronze streaming logic from ingest_cdc_bronze.py
        # We use 'csv' format here because 'cloudFiles' (Auto Loader) is proprietary to Databricks runtime.
        df_stream = (
            spark.readStream.format("csv")
            .option("header", "true")
            .schema(order_schema)
            .load(temp_dir)
            .withColumn("ingest_timestamp", current_timestamp())
            .withColumn("_source_file_path", input_file_name())
        )

        # Write to memory sink for verification
        query = (
            df_stream.writeStream.format("memory")
            .queryName("test_orders_stream")
            .outputMode("append")
            .start()
        )

        query.processAllAvailable()

        # 3. Assert
        result_df = spark.sql("SELECT * FROM test_orders_stream")
        assert result_df.count() == 1

        first_row = result_df.first()
        assert first_row["order_id"] == "ORD-1"
        assert first_row["operation_type"] == "INSERT"

        # Verify metadata columns were successfully appended
        assert first_row["ingest_timestamp"] is not None
        assert first_row["_source_file_path"] is not None

        query.stop()
    finally:
        shutil.rmtree(temp_dir)
