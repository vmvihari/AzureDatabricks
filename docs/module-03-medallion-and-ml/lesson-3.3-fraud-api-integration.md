# Lesson 3.3: Fraud API Integration & Spark Joins

## Table of Contents
- [Integrating Disparate Data](#integrating-disparate-data)
- [Spark Joins: Shuffle vs. Broadcast](#spark-joins-shuffle-vs-broadcast)
- [Action Step: Flagging Fraudulent Loans](#action-step-flagging-fraudulent-loans)
- [Interview Preparation](#interview-preparation)

---

## Integrating Disparate Data

The true power of the Silver layer is **integration**. 
We currently have:
1. `silver_loans`: A massive, cleansed Delta table of mortgage applications.
2. `blacklist_today.json`: A small JSON file in the Bronze landing zone containing SSNs of known fraudsters, pulled from our REST API in Lesson 2.4.

To calculate risk, we must join these two datasets together and flag any loan applicant who appears on the blacklist.

---

## Spark Joins: Shuffle vs. Broadcast

When you join two tables in PySpark, you must consider how the data moves across the cluster network.

### The Default: Shuffle Sort Merge Join
By default, Spark will hash the join keys (e.g., `SSN`) of both tables and physically move (shuffle) the data across the network so that matching keys end up on the exact same Worker Node to be joined. 
- **The Problem:** Shuffling data across the network is the single most expensive and slowest operation in Apache Spark.

### The Optimization: Broadcast Hash Join
If one of the tables is very small (like our Fraud Blacklist API payload, which is likely only a few megabytes), you should use a **Broadcast Join**.
- Instead of shuffling the massive `silver_loans` table, Spark copies (broadcasts) the entire small blacklist to the memory of *every single Worker Node*.
- The Worker Nodes can then join their local partitions of `silver_loans` against the blacklist instantly, with zero network shuffling.

```python
from pyspark.sql.functions import broadcast

df_joined = df_massive_loans.join(
    broadcast(df_small_blacklist),
    df_massive_loans.ssn == df_small_blacklist.ssn,
    "left"
)
```
*(Note: Databricks Adaptive Query Execution (AQE) often automatically converts joins to broadcast joins if the data statistics show one table is under 10MB, but explicitly defining it is a senior-level practice).*

---

## 🛠️ Action Step: Flagging Fraudulent Loans

Let's integrate our API data with our core pipeline.

1. Navigate to `apps/mortgage-data-platform/src/silver/` and create `fraud_flagging.py`.
2. Write PySpark code to read the `silver_loans` Delta table.
3. Read the JSON blacklist from `abfss://bronze@stmortgagedata<your_initials>.dfs.core.windows.net/landing/fraud_blacklist/blacklist_today.json`.
4. Perform a `Left Join` using the `broadcast` hint. Join on the SSN columns.
5. Create a new column named `is_fraud_flagged`. If the right side of the join is null, the flag is `False`; otherwise, `True`.
6. Write the resulting DataFrame back out, overwriting the `silver_loans` Delta table so it now includes the new fraud flag.

```python
from pyspark.sql.functions import broadcast, col, when


def flag_fraud(df_loans, df_blacklist):
    """
    Flags fraudulent loan applicants via a broadcast join against a blacklist.
    Accepts DataFrames and returns a transformed DataFrame.
    Importing this function has zero side effects — safe for pytest.
    """
    return df_loans.join(
        broadcast(df_blacklist),
        df_loans.applicant_ssn == df_blacklist.ssn,
        "left"
    ).withColumn(
        "is_fraud_flagged",
        when(col("ssn").isNotNull(), True).otherwise(False)
    ).drop("ssn")


if __name__ == "__main__":
    from src.utils.spark import get_spark_session

    spark = get_spark_session("FraudFlagging")

    df_silver = spark.read.format("delta").load("abfss://silver@stmortgagedata<your_initials>.dfs.core.windows.net/tables/silver_loans")
    df_black = spark.read.format("json").load("abfss://bronze@stmortgagedata<your_initials>.dfs.core.windows.net/landing/fraud_blacklist/blacklist_today.json")

    df_flagged = flag_fraud(df_silver, df_black)

    (df_flagged.write
        .format("delta")
        .mode("overwrite")
        .save("abfss://silver@stmortgagedata<your_initials>.dfs.core.windows.net/tables/silver_loans"))
```

---

## 🛠️ Action Step: Validation & Testing

Just like the cleansing pipeline, this integration script should be tested before deployment.

1. Navigate to `apps/mortgage-data-platform/tests/unit/` (create it if it doesn't exist).
2. Create `test_fraud_flagging.py`.
3. Write a `pytest` test that creates two mock DataFrames: one representing `silver_loans` and one representing the `blacklist`. 
4. Pass them to your flagging function and assert that the resulting DataFrame correctly adds the `is_fraud_flagged` boolean column, and correctly identifies the mock fraudster.
5. Run `pytest tests/unit/test_fraud_flagging.py` in your local terminal to validate the join logic.

```python
from src.silver.fraud_flagging import flag_fraud

def test_flag_fraud_broadcast_join(spark):
    # Create the mock DataFrames using native Spark SQL
    # This completely bypasses PySpark's RDD Python worker socket on Windows
    df_loans = spark.sql("""
        SELECT 'L-01' as loan_id, '12345' as applicant_ssn UNION ALL
        SELECT 'L-02' as loan_id, '99999' as applicant_ssn
    """)
    
    df_blacklist = spark.sql("""
        SELECT '99999' as ssn
    """)
    
    df_flagged = flag_fraud(df_loans, df_blacklist)
    
    # Assert L-01 is False
    assert (
        df_flagged.filter(df_flagged.loan_id == "L-01").first()["is_fraud_flagged"]
        is False
    )
    
    # Assert L-02 is True
    assert (
        df_flagged.filter(df_flagged.loan_id == "L-02").first()["is_fraud_flagged"]
        is True
    )
```

---

## 🎯 Interview Preparation

> [!TIP]
> **Q1: Explain the difference between a Shuffle Sort Merge Join and a Broadcast Hash Join.**
> **Answer:** "A Shuffle Sort Merge Join requires hashing the join keys of both tables and physically moving (shuffling) the data across the network to collocate matching keys on the same nodes. This is extremely slow and I/O intensive. A Broadcast Hash Join avoids the shuffle entirely by taking a small table (typically under 10MB) and copying it entirely into the memory of every worker node. The worker nodes then stream the large table through their memory, joining it instantly against the local copy of the small table."

> [!TIP]
> **Q2: What happens if you broadcast a table that is 50 Gigabytes?**
> **Answer:** "The pipeline will immediately crash with an Out Of Memory (OOM) error on the Driver node. Before a table is broadcasted to the workers, it must first be collected to the Driver node's memory. If the table is larger than the Driver's available RAM, the cluster will fail. Broadcast joins should strictly be reserved for small dimensional tables, configuration files, or API payloads."

---
[⬅️ Previous: Lesson 3.2: Bronze to Silver Cleansing](lesson-3.2-bronze-to-silver-cleansing.md) | [🏠 Main Directory](../../README.md) | [➡️ Next: Lesson 3.4: Silver to Gold Aggregations](lesson-3.4-silver-to-gold-aggregations.md)
