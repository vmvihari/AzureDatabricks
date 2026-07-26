# Lesson 3.2: Bronze to Silver - Cleansing and Normalization

## Table of Contents
- [The Silver Layer Mandate](#the-silver-layer-mandate)
- [Data Deduplication](#data-deduplication)
- [Schema Enforcement & Evolution](#schema-enforcement--evolution)
- [Action Step: Cleansing the Loan Applications](#action-step-cleansing-the-loan-applications)
- [Interview Preparation](#interview-preparation)

---

## The Silver Layer Mandate

The **Bronze Layer** contains raw, untouched data exactly as it arrived from the source. It is often messy, duplicated, and contains malformed strings.

The **Silver Layer** represents the conformed, cleansed, "single source of truth" for the enterprise. 
In this layer, we:
1. Deduplicate records.
2. Standardize column names (e.g., snake_case).
3. Cast data types.
4. Cleanse strings (e.g., stripping hyphens from SSNs).

---

## Data Deduplication

If an upstream system accidentally sends the same mortgage application twice, it will land in Bronze twice. We must deduplicate it before writing to Silver using `dropDuplicates()`.

```python
df_deduped = df_bronze.dropDuplicates(["loan_id"])
```

---

## Schema Enforcement & Evolution

By default, Delta Lake strictly enforces schemas on write. If your Silver table expects an integer `credit_score`, but the Bronze data contains a string `"N/A"`, Delta Lake will block the write and throw an error to prevent data corruption.

However, if the business adds a *new* valid column (e.g., `applicant_email`), you can instruct Delta to safely evolve the schema dynamically:
```python
df_new.write \
  .format("delta") \
  .mode("append") \
  .option("mergeSchema", "true") \
  .save("abfss://silver@.../tables/silver_loans")
```

---

## 🛠️ Action Step: Cleansing the Loan Applications

Let's build the ETL pipeline that transforms our raw loan data into the Silver layer.

1. Navigate to `apps/mortgage-data-platform/src/silver/` and create `cleansed_loans.py`.
2. Write PySpark code to read the Delta table `abfss://bronze@stmortgagedata<your_initials>.dfs.core.windows.net/tables/bronze_loans`.
3. Apply the following transformations:
   - Use `dropDuplicates(["loan_id"])`.
   - Use `filter()` to drop any rows where `applicant_ssn` is null.
   - Use `withColumn()` and `regexp_replace()` to strip hyphens (`-`) from the `applicant_ssn`.
   - Ensure column names are strictly `snake_case`.
4. Write the resulting DataFrame to `abfss://silver@stmortgagedata<your_initials>.dfs.core.windows.net/tables/silver_loans` in Delta format using `mode("overwrite")` (for this initial batch load).

```python
from pyspark.sql.functions import col, regexp_replace


def cleanse_loans(df):
    """
    Cleanses raw Bronze loan data into Silver quality.
    Accepts a DataFrame and returns a transformed DataFrame.
    Importing this function has zero side effects — safe for pytest.
    """
    return (df.dropDuplicates(["loan_id"])
              .filter(col("applicant_ssn").isNotNull())
              .withColumn("applicant_ssn", regexp_replace(col("applicant_ssn"), "-", "")))


if __name__ == "__main__":
    from pyspark.sql import SparkSession
    from delta import configure_spark_with_delta_pip

    # On Databricks Runtime, Delta is pre-configured. This builder pattern ensures
    # the script also runs correctly in a local development environment.
    builder = (
        SparkSession.builder
        .appName("CleanseLoansSilver")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    df_bronze = spark.read.format("delta").load("abfss://bronze@stmortgagedata<your_initials>.dfs.core.windows.net/tables/bronze_loans")
    df_silver = cleanse_loans(df_bronze)

    (df_silver.write
        .format("delta")
        .mode("overwrite")
        .save("abfss://silver@stmortgagedata<your_initials>.dfs.core.windows.net/tables/silver_loans"))
```

---

## 5. 🛠️ Action Step: Validation & Testing

As discussed in Module 2, you must test your transformation logic locally before deploying.

### 1. Write the Test

1. Navigate to `apps/mortgage-data-platform/tests/unit/` (create it if it doesn't exist).
2. Create `test_silver_cleansing.py`.
3. Write a `pytest` test that passes a mock DataFrame containing a null SSN to your cleansing function, and asserts that the resulting DataFrame has dropped that row.
4. Run `pytest tests/unit/test_silver_cleansing.py` in your local terminal to validate your code.

```python
import os
import sys
import pytest
from pyspark.sql import SparkSession
from src.silver.cleansed_loans import cleanse_loans

# Fix for Windows: Ensure Spark uses the current Python executable and IPv4 to avoid Socket errors
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
os.environ['_JAVA_OPTIONS'] = "-Djava.net.preferIPv4Stack=true"

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local[1]").appName("LocalTest").getOrCreate()

def test_cleansing_drops_null_ssns_and_formats_strings(spark):
    # Create the mock DataFrame using native Spark SQL
    # This completely bypasses PySpark's RDD Python worker socket on Windows
    df_in = spark.sql("""
        SELECT 'L-01' as loan_id, '123-45-6789' as applicant_ssn UNION ALL
        SELECT 'L-02' as loan_id, CAST(NULL AS STRING) as applicant_ssn UNION ALL
        SELECT 'L-01' as loan_id, '123-45-6789' as applicant_ssn
    """)
    
    df_clean = cleanse_loans(df_in)
    
    # Assert duplicates and nulls are dropped
    assert df_clean.count() == 1
    
    # Assert hyphens are removed
    assert df_clean.first()["applicant_ssn"] == "123456789"
```

---

## 6. 🎯 Interview Preparation

> [!TIP]
> **Q1: What is the difference between Schema Enforcement and Schema Evolution in Delta Lake?**
> **Answer:** "Schema Enforcement is Delta Lake's default behavior; it acts as a gatekeeper, rejecting any writes where the DataFrame schema does not exactly match the target table's schema. This prevents accidental data corruption. Schema Evolution is an explicit override (`mergeSchema=true`) that tells Delta Lake it is safe to automatically alter the target table's schema to accommodate new columns being introduced by the upstream data source."

> [!TIP]
> **Q2: Why do we use `dropDuplicates()` on a specific column instead of just calling it empty?**
> **Answer:** "Calling `dropDuplicates()` without arguments compares every single column across the entire row. In distributed systems, this requires a massive, expensive shuffle of all data across the network. By specifying a primary key like `dropDuplicates(["loan_id"])`, Spark only has to shuffle and hash the `loan_id` column, which is significantly faster and achieves the exact business requirement of removing duplicate applications."

---
[⬅️ Previous: Lesson 3.1: Delta Lake Internals](lesson-3.1-delta-lake-internals.md) | [🏠 Main Directory](../../README.md) | [➡️ Next: Lesson 3.3: Fraud API Integration](lesson-3.3-fraud-api-integration.md)
