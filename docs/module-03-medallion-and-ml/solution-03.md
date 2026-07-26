# Project Task 3: Solution

Here is the best-practice PySpark solution for cleansing the credit scores (Silver) and aggregating them by risk tier (Gold).

## 1. The Silver Pipeline (Cleansing)

Create this script at `apps/mortgage-data-platform/src/silver/cleansed_credit_scores.py`.

```python
from src.utils.spark import get_spark_session
from pyspark.sql.functions import col

spark = get_spark_session("Silver Credit Scores")

bronze_path = "abfss://bronze@stmortgagedata<your_initials>.dfs.core.windows.net/tables/bronze_credit_scores"
silver_path = "abfss://silver@stmortgagedata<your_initials>.dfs.core.windows.net/tables/silver_credit_scores"

# Read from Bronze
bronze_df = spark.read.format("delta").load(bronze_path)

# Data Quality Rule: Filter out invalid SSNs or credit scores outside valid range (300-850)
silver_df = (
    bronze_df
    .filter(col("ssn").isNotNull())
    .filter((col("credit_score") >= 300) & (col("credit_score") <= 850))
    .dropDuplicates(["ssn"]) # Assuming one score per SSN for this batch
)

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
from src.utils.spark import get_spark_session
from pyspark.sql.functions import col, sum as _sum, when

spark = get_spark_session("Gold Credit Exposure")

silver_loans_path = "abfss://silver@stmortgagedata<your_initials>.dfs.core.windows.net/tables/silver_loans"
silver_scores_path = "abfss://silver@stmortgagedata<your_initials>.dfs.core.windows.net/tables/silver_credit_scores"
gold_exposure_path = "abfss://gold@stmortgagedata<your_initials>.dfs.core.windows.net/tables/gold_credit_exposure"

loans_df = spark.read.format("delta").load(silver_loans_path)
scores_df = spark.read.format("delta").load(silver_scores_path)

# Join Loans to Credit Scores
joined_df = loans_df.join(scores_df, on="ssn", how="inner")

# Categorize Risk Tier
risk_df = joined_df.withColumn(
    "risk_tier",
    when(col("credit_score") >= 740, "Excellent")
    .when((col("credit_score") >= 670) & (col("credit_score") < 740), "Good")
    .when((col("credit_score") >= 580) & (col("credit_score") < 670), "Fair")
    .otherwise("Poor")
)

# Aggregate Total Exposure
gold_df = (
    risk_df
    .groupBy("risk_tier")
    .agg(_sum("loan_amount").alias("total_exposure"))
)

# Write to Gold
(
    gold_df.write
    .format("delta")
    .mode("overwrite")
    .save(gold_exposure_path)
)
```

---
[⬅️ Back to Project Task 3](project-task-03.md) | [🏠 Main Directory](../../README.md)
