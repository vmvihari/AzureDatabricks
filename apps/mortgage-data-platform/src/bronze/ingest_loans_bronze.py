from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)


def get_loan_schema():
    """
    Returns the strict schema for loan applications.
    Importing this function has zero side effects — safe for pytest.
    """
    return StructType(
        [
            StructField("loan_id", StringType(), True),
            StructField("applicant_ssn", StringType(), True),
            StructField("loan_amount", DoubleType(), True),
            StructField("credit_score", IntegerType(), True),
        ]
    )


if __name__ == "__main__":
    # Initialize Spark Session (Databricks runtime provides `spark` by default, but this is best practice)
    spark = SparkSession.builder.appName("IngestLoansBronze").getOrCreate()

    # 1. Define strict schema
    loan_schema = get_loan_schema()

    # 2. Read from Landing Zone
    df_loans = (
        spark.read.format("csv")
        .option("header", "true")
        .schema(loan_schema)
        .load(
            "abfss://bronze@stmortgagedata<your_initials>.dfs.core.windows.net/landing/loan_applications/"
        )
    )

    # 3. Write to Bronze Delta Table
    (
        df_loans.write.format("delta")
        .mode("append")
        .save(
            "abfss://bronze@stmortgagedata<your_initials>.dfs.core.windows.net/tables/bronze_loans"
        )
    )
