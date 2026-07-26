import dlt
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

spark = SparkSession.builder.getOrCreate()

loan_schema = StructType(
    [
        StructField("loan_id", StringType(), True),
        StructField("applicant_ssn", StringType(), True),
        StructField("loan_amount", DoubleType(), True),
        StructField("credit_score", IntegerType(), True),
    ]
)


@dlt.expect_or_drop("valid_ssn", "applicant_ssn IS NOT NULL")
@dlt.expect_or_drop("valid_loan_amount", "loan_amount > 0")
@dlt.table(
    name="bronze_loans",
    comment="Raw loan applications ingested via Auto Loader with strict quality checks.",
)
def bronze_loans():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .schema(loan_schema)
        .load(
            "abfss://bronze@stmortgagedata<your_initials>.dfs.core.windows.net/landing/loan_applications/"
        )
    )
