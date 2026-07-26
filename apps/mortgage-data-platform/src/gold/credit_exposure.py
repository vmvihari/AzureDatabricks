from pyspark.sql.functions import col
from pyspark.sql.functions import sum as _sum
from pyspark.sql.functions import when


def aggregate_exposure(df_loans, df_scores):
    # Join Loans to Credit Scores
    joined_df = df_loans.join(df_scores, on="applicant_ssn", how="inner")

    # Categorize Risk Tier
    risk_df = joined_df.withColumn(
        "credit_tier",
        when(col("credit_score") > 750, "Excellent")
        .when((col("credit_score") >= 650) & (col("credit_score") <= 750), "Fair")
        .otherwise("Poor"),
    )

    # Aggregate Total Exposure
    return risk_df.groupBy("credit_tier").agg(
        _sum("loan_amount").alias("total_exposure")
    )


if __name__ == "__main__":
    from src.utils.spark import get_spark_session

    spark = get_spark_session("Gold Credit Exposure")

    silver_loans_path = "abfss://silver@stmortgagedata<your_initials>.dfs.core.windows.net/tables/silver_loans"
    silver_scores_path = "abfss://silver@stmortgagedata<your_initials>.dfs.core.windows.net/tables/silver_credit_scores"
    gold_exposure_path = "abfss://gold@stmortgagedata<your_initials>.dfs.core.windows.net/tables/gold_credit_exposure"

    loans_df = spark.read.format("delta").load(silver_loans_path)
    scores_df = spark.read.format("delta").load(silver_scores_path)

    gold_df = aggregate_exposure(loans_df, scores_df)

    # Write to Gold
    (gold_df.write.format("delta").mode("overwrite").save(gold_exposure_path))
