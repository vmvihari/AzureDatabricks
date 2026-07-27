# Lesson 2.5: Core PySpark Transformations

## Table of Contents
- [The DataFrame API](#the-dataframe-api)
- [Selecting and Renaming Columns](#selecting-and-renaming-columns)
- [Filtering Data](#filtering-data)
- [Creating and Modifying Columns](#creating-and-modifying-columns)
- [Aggregations (GroupBy)](#aggregations-groupby)
- [Interview Preparation](#interview-preparation)

---

## The DataFrame API

Once data is ingested into the Bronze layer, Data Engineers use PySpark's DataFrame API to cleanse, conform, and join data to build the Silver and Gold layers. 

To use PySpark's built-in functions, you must first import them:
```python
from pyspark.sql.functions import col, lit, current_timestamp, upper
```

---

## Selecting and Renaming Columns

You rarely need every column from a raw file. The `select()` and `withColumnRenamed()` methods allow you to project the exact schema you need for downstream tables.

```python
# Select specific columns and rename them to match the Silver schema
df_silver_loans = df_bronze_loans.select(
    col("loan_id").alias("LoanID"),
    col("applicant_ssn"),
    col("loan_amount"),
    col("status")
).withColumnRenamed("status", "LoanStatus")
```

---

## Filtering Data

Data Quality is paramount. The `filter()` (or `where()`) method is used to remove invalid or irrelevant rows.

```python
# Drop any loan applications where the SSN is missing, and only keep "APPROVED" loans
df_valid_loans = df_silver_loans.filter(
    col("applicant_ssn").isNotNull() & 
    (col("LoanStatus") == "APPROVED")
)
```

---

## Creating and Modifying Columns

The `withColumn()` method is the most frequently used transformation. It allows you to create new columns or overwrite existing ones.

```python
# 1. Standardize SSNs (Remove hyphens)
# 2. Add an ingestion timestamp for auditing
# 3. Categorize loans based on amount
from pyspark.sql.functions import regexp_replace, when

df_transformed = df_valid_loans \
    .withColumn("applicant_ssn", regexp_replace(col("applicant_ssn"), "-", "")) \
    .withColumn("ingestion_timestamp", current_timestamp()) \
    .withColumn("loan_tier", 
        when(col("loan_amount") > 500000, "JUMBO")
        .otherwise("STANDARD")
    )
```

---

## Aggregations (GroupBy)

In the Gold layer, we aggregate data for BI analysts. The `groupBy()` method, combined with aggregate functions like `sum()`, `count()`, and `avg()`, is used to build these summary tables.

```python
from pyspark.sql.functions import sum, avg

# Calculate total loan exposure and average loan size by State
df_gold_summary = df_transformed.groupBy("state").agg(
    sum("loan_amount").alias("Total_Exposure"),
    avg("loan_amount").alias("Average_Loan_Size"),
    count("loan_id").alias("Total_Loans_Approved")
)
```

---

## 🛠️ Action Step: Local Transformation Testing

Before we build the formal Medallion pipelines in Module 3, let's practice these PySpark transformations locally using `pytest`. 

Instead of deploying a massive cluster just to see if our `regexp_replace` syntax works, we will write a unit test.

1. Navigate to `apps/mortgage-data-platform/tests/unit/` and create `test_transformations.py`.
2. Write the following code to test a cleansing transformation and an aggregation locally:

```python
import pytest
from pyspark.sql.functions import col, regexp_replace, avg

# The logic we want to test
def cleanse_and_aggregate(df):
    df_clean = df.filter(col("status") == "APPROVED") \
                 .withColumn("applicant_ssn", regexp_replace(col("applicant_ssn"), "-", ""))
                 
    df_summary = df_clean.groupBy("state").agg(avg("loan_amount").alias("average_loan_amount"))
    return df_clean, df_summary

def test_transformations(spark):
    # 1. Arrange: Create mock data using Spark SQL to bypass Python worker socket issues on Windows
    df_in = spark.sql("""
        SELECT 1 as loan_id, '123-45' as applicant_ssn, CAST(100.0 AS DOUBLE) as loan_amount,
            'TX' as state, 'APPROVED' as status UNION ALL
        SELECT 2 as loan_id, '999-99' as applicant_ssn, CAST(200.0 AS DOUBLE) as loan_amount,
            'TX' as state, 'DENIED' as status UNION ALL
        SELECT 3 as loan_id, '111-11' as applicant_ssn, CAST(300.0 AS DOUBLE) as loan_amount,
            'TX' as state, 'APPROVED' as status
    """)
    
    # 2. Act
    df_clean, df_summary = cleanse_and_aggregate(df_in)
    
    # 3. Assert
    # Verify DENIED loans were dropped
    assert df_clean.count() == 2
    
    # Verify hyphens were removed
    assert df_clean.filter(col("loan_id") == 1).first()["applicant_ssn"] == "12345"
    
    # Verify aggregation math ((100 + 300) / 2 = 200)
    assert df_summary.filter(col("state") == "TX").first()["average_loan_amount"] == 200.0
```

3. Run the test:
```bash
pytest tests/unit/test_transformations.py
```

> [!NOTE]
> **Why a Scratchpad?** Notice how we combined filtering/cleansing logic and aggregation logic in the same script? In a formal production architecture, these belong in different layers! In **Module 3**, we will abandon this scratchpad and properly split this logic into the **Silver Layer** (Cleansing) and the **Gold Layer** (Aggregations).

---

## 🎯 Interview Preparation

> [!TIP]
> **Q1: What is the difference between `where()` and `filter()` in PySpark?**
> **Answer:** "In PySpark, `where()` is simply an alias for `filter()`. They are functionally identical and compile to the exact same physical execution plan in the Catalyst Optimizer. Some engineers prefer `where()` if they come from a strong SQL background, while those from a functional programming or Scala background tend to prefer `filter()`."

> [!TIP]
> **Q2: Why should you avoid chaining dozens of `withColumn()` calls in a row?**
> **Answer:** "Chaining many `withColumn()` statements consecutively can cause a massive performance overhead during the Catalyst Optimizer's planning phase, resulting in Driver node hangs. This is because each `withColumn` forces Spark to build a new internal projection in the logical plan. If I need to modify or create 20 columns, I will instead use a single `select()` statement with multiple `col()` manipulations, which compiles into a single projection step and is much faster to optimize."

---
[⬅️ Previous: Lesson 2.4: Ingesting HTTP APIs](lesson-2.4-ingest-http-fraud-api.md) | [🏠 Main Directory](../../README.md) | [➡️ Next: Lesson 2.6: Continuous Integration](lesson-2.6-github-actions-ci.md)
