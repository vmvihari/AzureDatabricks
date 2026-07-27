from pyspark.sql.functions import col


def cleanse_credit_scores(df):
    # Data Quality Rule: Filter out invalid SSNs or credit scores outside valid range (300-850)
    return (
        df.filter(col("ssn").isNotNull())
        .filter((col("credit_score") >= 300) & (col("credit_score") <= 850))
        .dropDuplicates(["ssn"])  # Assuming one score per SSN for this batch
        .withColumnRenamed("ssn", "applicant_ssn")
    )


if __name__ == "__main__":
    from src.utils.spark import get_spark_session

    spark = get_spark_session("Silver Credit Scores")

    bronze_path = "abfss://bronze@stmortgagedata<your_initials>.dfs.core.windows.net/tables/bronze_credit_scores"
    silver_path = "abfss://silver@stmortgagedata<your_initials>.dfs.core.windows.net/tables/silver_credit_scores"

    # Read from Bronze
    bronze_df = spark.read.format("delta").load(bronze_path)

    silver_df = cleanse_credit_scores(bronze_df)

    # Write to Silver (Overwrite for batch simplicity, though merge is preferred in production)
    (silver_df.write.format("delta").mode("overwrite").save(silver_path))
