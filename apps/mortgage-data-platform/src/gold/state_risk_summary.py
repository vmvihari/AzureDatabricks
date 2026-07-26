from pyspark.sql.functions import avg, col, sum


def aggregate_risk_by_state(df_silver):
    """
    Aggregates Silver loan data into a Gold-level state risk summary.
    Accepts a DataFrame and returns a transformed DataFrame.
    Importing this function has zero side effects — safe for pytest.
    """
    return df_silver.groupBy("state").agg(
        sum("loan_amount").alias("total_exposure"),
        sum(col("is_fraud_flagged").cast("int")).alias("total_fraud_flags"),
        avg("credit_score").alias("average_credit_score"),
    )


if __name__ == "__main__":
    from src.utils.spark import get_spark_session

    spark = get_spark_session("StateRiskSummary")

    df_silver = spark.read.format("delta").load(
        "abfss://silver@stmortgagedata<your_initials>.dfs.core.windows.net/tables/silver_loans"
    )

    df_gold = aggregate_risk_by_state(df_silver)

    (
        df_gold.write.format("delta")
        .mode("overwrite")
        .save(
            "abfss://gold@stmortgagedata<your_initials>.dfs.core.windows.net/tables/gold_state_risk_summary"
        )
    )
