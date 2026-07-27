import os
import tempfile

from pyspark.sql.types import DoubleType

# Import the actual logic from our script
from src.bronze.ingest_loans_bronze import get_loan_schema


def test_bronze_schema_enforcement(spark):
    # 1. Arrange: Import the EXACT schema we built in our ingestion script
    loan_schema = get_loan_schema()

    # Create a mock CSV file (Bypasses PySpark RDD socket issues on Windows)
    with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".csv") as f:
        f.write("loan_id,applicant_ssn,loan_amount,credit_score\n")
        f.write("L-100,123-45-6789,250000.0,720\n")
        temp_path = f.name

    try:
        # 2. Act: Apply the schema to the mock data using native CSV reader
        df = spark.read.option("header", "true").schema(loan_schema).csv(temp_path)

        # 3. Assert: Verify the types were cast correctly
        assert df.schema["loan_amount"].dataType == DoubleType()
        assert df.first()["credit_score"] == 720
    finally:
        os.remove(temp_path)
