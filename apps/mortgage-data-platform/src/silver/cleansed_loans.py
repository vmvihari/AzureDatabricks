from pyspark.sql.functions import col, regexp_replace


def cleanse_loans(df):
    """
    Cleanses raw Bronze loan data into Silver quality.
    Accepts a DataFrame and returns a transformed DataFrame.
    Importing this function has zero side effects — safe for pytest.
    """
    return (
        df.dropDuplicates(["loan_id"])
        .filter(col("applicant_ssn").isNotNull())
        .withColumn("applicant_ssn", regexp_replace(col("applicant_ssn"), "-", ""))
    )


if __name__ == "__main__":
    from src.utils.spark import get_spark_session

    spark = get_spark_session("CleanseLoansSilver")

    df_bronze = spark.read.format("delta").load(
        "abfss://bronze@stmortgagedata<your_initials>.dfs.core.windows.net/tables/bronze_loans"
    )
    df_silver = cleanse_loans(df_bronze)

    (
        df_silver.write.format("delta")
        .mode("overwrite")
        .save(
            "abfss://silver@stmortgagedata<your_initials>.dfs.core.windows.net/tables/silver_loans"
        )
    )
