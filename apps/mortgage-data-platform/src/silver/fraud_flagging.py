from pyspark.sql.functions import broadcast, col, when


def flag_fraud(df_loans, df_blacklist):
    """
    Flags fraudulent loan applicants via a broadcast join against a blacklist.
    Accepts DataFrames and returns a transformed DataFrame.
    Importing this function has zero side effects — safe for pytest.
    """
    return (
        df_loans.join(
            broadcast(df_blacklist), df_loans.applicant_ssn == df_blacklist.ssn, "left"
        )
        .withColumn(
            "is_fraud_flagged", when(col("ssn").isNotNull(), True).otherwise(False)
        )
        .drop("ssn")
    )


if __name__ == "__main__":
    from src.utils.spark import get_spark_session

    spark = get_spark_session("FraudFlagging")

    df_silver = spark.read.format("delta").load(
        "abfss://silver@stmortgagedata<your_initials>.dfs.core.windows.net/tables/silver_loans"
    )
    df_black = spark.read.format("json").load(
        "abfss://bronze@stmortgagedata<your_initials>.dfs.core.windows.net/landing/fraud_blacklist/blacklist_today.json"
    )

    df_flagged = flag_fraud(df_silver, df_black)

    (
        df_flagged.write.format("delta")
        .mode("overwrite")
        .save(
            "abfss://silver@stmortgagedata<your_initials>.dfs.core.windows.net/tables/silver_loans"
        )
    )
