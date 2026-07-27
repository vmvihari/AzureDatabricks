# apps/mortgage-data-platform/src/dlt/autoloader_bronze.py
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)
from src.utils.spark import get_spark_session


def run_autoloader():
    spark = get_spark_session("AutoLoaderBronze")

    loan_schema = StructType(
        [
            StructField("loan_id", StringType(), True),
            StructField("applicant_ssn", StringType(), True),
            StructField("loan_amount", DoubleType(), True),
            StructField("credit_score", IntegerType(), True),
        ]
    )

    df_stream = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .schema(loan_schema)
        .load(
            "abfss://bronze@stmortgagedata<your_initials>.dfs.core.windows.net/landing/loan_applications/"
        )
    )

    (
        df_stream.writeStream.format("delta")
        .option(
            "checkpointLocation",
            "abfss://bronze@stmortgagedata<your_initials>.dfs.core.windows.net/_checkpoints/bronze_loans/",
        )
        .trigger(availableNow=True)
        .start(
            "abfss://bronze@stmortgagedata<your_initials>.dfs.core.windows.net/tables/bronze_loans"
        )
    )


if __name__ == "__main__":
    run_autoloader()
