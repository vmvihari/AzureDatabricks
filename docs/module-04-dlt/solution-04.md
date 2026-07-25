# Project Task 4: Solution

Here is the best-practice declarative Delta Live Tables (DLT) solution for the credit score pipeline.

## The DLT Script

Create this script at `apps/mortgage-data-platform/src/dlt/credit_score_pipeline.py`.

```python
import dlt
from pyspark.sql.functions import col, sum as _sum, when

# -------------------------------------------------------------
# 1. Bronze Layer (Auto Loader Ingestion)
# -------------------------------------------------------------
@dlt.table(
    name="bronze_credit_scores",
    comment="Raw credit score updates ingested from Azure Data Lake.",
    table_properties={"quality": "bronze"}
)
def bronze_credit_scores():
    # Use Auto Loader (cloudFiles) for streaming ingestion
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .load("abfss://bronze@stmortgagedata<your_initials>.dfs.core.windows.net/landing/credit_scores/")
    )

# -------------------------------------------------------------
# 2. Silver Layer (Cleansing & Data Quality)
# -------------------------------------------------------------
@dlt.table(
    name="silver_credit_scores",
    comment="Cleansed credit scores with invalid SSNs and out-of-bounds scores removed.",
    table_properties={"quality": "silver"}
)
# Define Expectations (Data Quality Rules)
@dlt.expect_or_drop("valid_ssn", "ssn IS NOT NULL")
@dlt.expect_or_drop("valid_score", "credit_score >= 300 AND credit_score <= 850")
def silver_credit_scores():
    # Read stream from the Bronze DLT table
    return dlt.read_stream("bronze_credit_scores")

# -------------------------------------------------------------
# 3. Gold Layer (Business Aggregations)
# -------------------------------------------------------------
@dlt.table(
    name="gold_credit_exposure",
    comment="Aggregated loan exposure mapped to credit score risk tiers.",
    table_properties={"quality": "gold"}
)
def gold_credit_exposure():
    # Read the cleansed Silver table as a static DataFrame (not a stream) for aggregation
    scores_df = dlt.read("silver_credit_scores")
    
    # Normally we would join with silver_loans here, but to keep the DLT 
    # pipeline self-contained for the task, we will just categorize and aggregate.
    risk_df = scores_df.withColumn(
        "risk_tier",
        when(col("credit_score") >= 740, "Excellent")
        .when((col("credit_score") >= 670) & (col("credit_score") < 740), "Good")
        .when((col("credit_score") >= 580) & (col("credit_score") < 670), "Fair")
        .otherwise("Poor")
    )
    
    # Count how many borrowers fall into each tier (since we didn't join loans for the simplified pipeline)
    return risk_df.groupBy("risk_tier").count().alias("borrower_count")
```

## Explanation
- **Bronze:** Uses `cloudFiles` (Databricks Auto Loader) to incrementally process new CSV files as they land, without needing manual watermarking or checkpoints.
- **Silver:** Uses DLT Expectations (`@dlt.expect_or_drop`) to enforce data quality declaratively. Invalid rows are automatically dropped and recorded in the DLT event log.
- **Gold:** Reads from Silver using `dlt.read()` (static) rather than `dlt.read_stream()` because aggregations across the entire dataset are often easier to manage as complete recalculations or materialized views in DLT.

---
[⬅️ Back to Project Task 4](project-task-04.md) | [🏠 Main Directory](../../README.md)
