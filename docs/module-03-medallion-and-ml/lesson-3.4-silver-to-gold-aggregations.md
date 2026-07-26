# Lesson 3.4: Silver to Gold - Business Aggregations

## Table of Contents
- [The Purpose of the Gold Layer](#the-purpose-of-the-gold-layer)
- [Views vs. Tables](#views-vs-tables)
- [Action Step: State Risk Summary](#action-step-state-risk-summary)
- [Interview Preparation](#interview-preparation)

---

## The Purpose of the Gold Layer

The **Gold Layer** is highly refined and aggregated data, optimized strictly for business-level reporting, dashboards (e.g., PowerBI, Tableau), and machine learning features.

Unlike the Silver layer (which contains hundreds of columns and millions of granular rows representing every single loan application), Gold tables are typically highly summarized. 

For example, a Chief Risk Officer does not want to query 10 million rows. They want a dashboard showing: "Total Loan Exposure by State."

---

## Views vs. Tables

When building the Gold layer, Data Engineers must choose between creating logical views or physical tables.

### Logical Views
You can define a view over the Silver table.
```sql
CREATE VIEW gold.state_risk_summary AS
SELECT state, SUM(loan_amount) FROM silver.loans GROUP BY state;
```
- **Pros:** Zero storage cost. Data is always real-time.
- **Cons:** Compute intensive. Every time a PowerBI dashboard refreshes, the Databricks cluster must recompute the aggregation over the massive Silver table.

### Physical Tables
You can materialize the aggregation as a physical Delta Table in the `gold` container.
- **Pros:** Lightning fast for BI tools. PowerBI queries the small, pre-calculated Gold table instead of forcing Databricks to recalculate the numbers.
- **Cons:** Data is only as fresh as the last time the batch job ran (e.g., updated nightly).

**Enterprise Standard:** We heavily favor materializing physical Delta Tables in the Gold layer to reduce cluster compute costs and ensure dashboard responsiveness.

---

## 🛠️ Action Step: State Risk Summary

Let's build a physical Gold table for our Chief Risk Officer.

1. Navigate to `apps/mortgage-data-platform/src/gold/` and create `state_risk_summary.py`.
2. Write PySpark code to read the `silver_loans` Delta table.
3. Group the data by `state`.
4. Calculate three aggregate columns:
   - `total_exposure` (Sum of `loan_amount`)
   - `total_fraud_flags` (Sum of `is_fraud_flagged` cast to integers)
   - `average_credit_score` (Avg of `credit_score`)
5. Write the summarized DataFrame to `abfss://gold@stmortgagedata<your_initials>.dfs.core.windows.net/tables/gold_state_risk_summary` as a Delta Table using `overwrite` mode.

```python
from pyspark.sql.functions import sum, avg, col


def aggregate_risk_by_state(df_silver):
    """
    Aggregates Silver loan data into a Gold-level state risk summary.
    Accepts a DataFrame and returns a transformed DataFrame.
    Importing this function has zero side effects — safe for pytest.
    """
    return df_silver.groupBy("state").agg(
        sum("loan_amount").alias("total_exposure"),
        sum(col("is_fraud_flagged").cast("int")).alias("total_fraud_flags"),
        avg("credit_score").alias("average_credit_score")
    )


if __name__ == "__main__":
    from pyspark.sql import SparkSession
    from delta import configure_spark_with_delta_pip

    # On Databricks Runtime, Delta is pre-configured. This builder pattern ensures
    # the script also runs correctly in a local development environment.
    builder = (
        SparkSession.builder
        .appName("StateRiskSummary")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    df_silver = spark.read.format("delta").load("abfss://silver@stmortgagedata<your_initials>.dfs.core.windows.net/tables/silver_loans")

    df_gold = aggregate_risk_by_state(df_silver)

    (df_gold.write
        .format("delta")
        .mode("overwrite")
        .save("abfss://gold@stmortgagedata<your_initials>.dfs.core.windows.net/tables/gold_state_risk_summary"))
```

---

## 5. 🛠️ Action Step: Validation & Testing

As always, aggregation logic should be validated locally before it hits the Databricks cluster.

1. Navigate to `apps/mortgage-data-platform/tests/unit/`.
2. Create `test_gold_aggregations.py`.
3. Write a `pytest` test that creates a mock DataFrame representing `silver_loans` with 3 rows (e.g., 2 loans in "TX" and 1 in "CA"). Ensure one of the TX loans is flagged for fraud.
4. Pass the mock DataFrame to your aggregation function and assert that the output DataFrame correctly has 2 rows (one for TX, one for CA), and that the TX `total_fraud_flags` equals 1.
5. Run `pytest tests/unit/test_gold_aggregations.py` to validate your math.

```python
import os
import sys
import pytest
from pyspark.sql import SparkSession
from src.gold.state_risk_summary import aggregate_risk_by_state

# Fix for Windows: Ensure Spark uses the current Python executable and IPv4 to avoid Socket errors
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
os.environ['_JAVA_OPTIONS'] = "-Djava.net.preferIPv4Stack=true"

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local[1]").appName("LocalTest").getOrCreate()

def test_aggregate_risk_by_state(spark):
    # Create the mock DataFrame using native Spark SQL
    # This completely bypasses PySpark's RDD Python worker socket on Windows
    df_in = spark.sql("""
        SELECT 'TX' as state, CAST(100.0 AS DOUBLE) as loan_amount, True as is_fraud_flagged, 700 as credit_score UNION ALL
        SELECT 'TX' as state, CAST(200.0 AS DOUBLE) as loan_amount, False as is_fraud_flagged, 720 as credit_score UNION ALL
        SELECT 'CA' as state, CAST(500.0 AS DOUBLE) as loan_amount, False as is_fraud_flagged, 800 as credit_score
    """)
    
    df_out = aggregate_risk_by_state(df_in)
    
    # Assert row count
    assert df_out.count() == 2
    
    # Assert TX aggregations
    tx_row = df_out.filter(df_out.state == "TX").first()
    assert tx_row["total_exposure"] == 300.0
    assert tx_row["total_fraud_flags"] == 1
    assert tx_row["average_credit_score"] == 710.0
```

---

## 🎯 Interview Preparation

> [!TIP]
> **Q1: Why do we persist Gold tables instead of just creating views over the Silver layer?**
> **Answer:** "While views ensure the data is perfectly real-time, they push the compute burden entirely onto the end-user's BI tool. If a dashboard with 10 visual charts executes 10 concurrent queries against a view, Databricks has to scan and aggregate the massive Silver table 10 times, causing latency and burning expensive compute units. By persisting (materializing) the Gold layer as physical Delta Tables, we pre-calculate the aggregations during the nightly batch window. The BI tool then queries a tiny, highly-optimized physical table, resulting in sub-second dashboard load times."

---
[⬅️ Previous: Lesson 3.3: Fraud API Integration](lesson-3.3-fraud-api-integration.md) | [🏠 Main Directory](../../README.md) | [➡️ Next: Lesson 3.5: Databricks Connect](lesson-3.5-databricks-connect.md)
