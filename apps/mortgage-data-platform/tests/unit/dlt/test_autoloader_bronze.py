import os
import shutil
import sys
import tempfile

import pytest
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)
from src.utils.spark import get_spark_session

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
os.environ["_JAVA_OPTIONS"] = "-Djava.net.preferIPv4Stack=true"


@pytest.fixture(scope="session")
def spark():
    return get_spark_session("LocalTest")


@pytest.mark.skipif(
    sys.platform == "win32",
    reason="Spark local file I/O requires Hadoop winutils on Windows",
)
def test_autoloader_memory_sink(spark):
    # Auto Loader testing requires writing actual files to a temp directory to simulate landing data
    temp_dir = tempfile.mkdtemp()
    try:
        # 1. Arrange: Write a mock CSV to the temp directory
        csv_content = "loan_id,applicant_ssn,loan_amount,credit_score\nL-999,123-45-6789,500000.0,750\n"
        with open(os.path.join(temp_dir, "mock_data.csv"), "w") as f:
            f.write(csv_content)

        loan_schema = StructType(
            [
                StructField("loan_id", StringType(), True),
                StructField("applicant_ssn", StringType(), True),
                StructField("loan_amount", DoubleType(), True),
                StructField("credit_score", IntegerType(), True),
            ]
        )

        # 2. Act: We simulate the `cloudFiles` reader, but since we don't have real cloudFiles locally,
        # we will use the standard `csv` format in the test to prove the streaming logic works.
        df_stream = (
            spark.readStream.format("csv")
            .option("header", "true")
            .schema(loan_schema)
            .load(temp_dir)
        )

        # Write to memory sink for testing
        query = (
            df_stream.writeStream.format("memory")
            .queryName("test_stream")
            .outputMode("append")
            .start()
        )

        query.processAllAvailable()

        # 3. Assert
        result_df = spark.sql("SELECT * FROM test_stream")
        assert result_df.count() == 1
        assert result_df.first()["loan_id"] == "L-999"

        query.stop()
    finally:
        shutil.rmtree(temp_dir)
