# Project Task 3: Solution

Here is the best-practice PySpark solution for cleansing the credit scores (Silver) and aggregating them by risk tier (Gold).

## 1. The Silver Pipeline (Cleansing)

Create this script at `apps/mortgage-data-platform/src/silver/cleansed_credit_scores.py`.

```python
from pyspark.sql.functions import col

def cleanse_credit_scores(df):
    # Data Quality Rule: Filter out invalid SSNs or credit scores outside valid range (300-850)
    return (
        df
        .filter(col("ssn").isNotNull())
        .filter((col("credit_score") >= 300) & (col("credit_score") <= 850))
        .dropDuplicates(["ssn"]) # Assuming one score per SSN for this batch
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
    (
        silver_df.write
        .format("delta")
        .mode("overwrite")
        .save(silver_path)
    )
```

## 2. The Gold Pipeline (Aggregation)

Create this script at `apps/mortgage-data-platform/src/gold/credit_exposure.py`.

```python
from pyspark.sql.functions import col, sum as _sum, when

def aggregate_exposure(df_loans, df_scores):
    # Join Loans to Credit Scores
    joined_df = df_loans.join(df_scores, on="applicant_ssn", how="inner")

    # Categorize Risk Tier
    risk_df = joined_df.withColumn(
        "credit_tier",
        when(col("credit_score") > 750, "Excellent")
        .when((col("credit_score") >= 650) & (col("credit_score") <= 750), "Fair")
        .otherwise("Poor")
    )

    # Aggregate Total Exposure
    return (
        risk_df
        .groupBy("credit_tier")
        .agg(_sum("loan_amount").alias("total_exposure"))
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
    (
        gold_df.write
        .format("delta")
        .mode("overwrite")
        .save(gold_exposure_path)
    )
```

## 3. Unit Testing

Create the test for the Silver pipeline at `apps/mortgage-data-platform/tests/unit/silver/test_cleansed_credit_scores.py`.

```python
from src.silver.cleansed_credit_scores import cleanse_credit_scores

def test_cleanse_credit_scores(spark):
    df_in = spark.sql("""
        SELECT '123' as ssn, 700 as credit_score UNION ALL
        SELECT '123' as ssn, 750 as credit_score UNION ALL
        SELECT '456' as ssn, 900 as credit_score UNION ALL
        SELECT CAST(NULL AS STRING) as ssn, 600 as credit_score
    """)
    
    df_out = cleanse_credit_scores(df_in)
    
    assert df_out.count() == 1 # only one valid record remains (deduplicated SSN '123' since '456' has invalid score)
    assert "applicant_ssn" in df_out.columns
    assert "ssn" not in df_out.columns
```

Create the test for the Gold pipeline at `apps/mortgage-data-platform/tests/unit/gold/test_credit_exposure.py`.

```python
from src.gold.credit_exposure import aggregate_exposure

def test_aggregate_exposure(spark):
    df_loans = spark.sql("""
        SELECT '123' as applicant_ssn, CAST(100.0 AS DOUBLE) as loan_amount UNION ALL
        SELECT '456' as applicant_ssn, CAST(200.0 AS DOUBLE) as loan_amount
    """)
    
    df_scores = spark.sql("""
        SELECT '123' as applicant_ssn, 800 as credit_score UNION ALL
        SELECT '456' as applicant_ssn, 600 as credit_score
    """)
    
    df_out = aggregate_exposure(df_loans, df_scores)
    
    assert df_out.count() == 2
    
    excellent_row = df_out.filter(df_out.credit_tier == "Excellent").first()
    assert excellent_row.total_exposure == 100.0
    
    poor_row = df_out.filter(df_out.credit_tier == "Poor").first()
    assert poor_row.total_exposure == 200.0
```

---
[⬅️ Back to Project Task 3](project-task-03.md) | [🏠 Main Directory](../../README.md)
