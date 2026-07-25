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

---

## 5. 🛠️ Action Step: Validation & Testing

As discussed in [Lesson 2.6: Local Unit Testing](../module-02-ingestion/lesson-2.6-local-pyspark-and-pytest.md), you must test your transformation logic locally before deploying.

1. Navigate to `apps/mortgage-data-platform/tests/` (create it if it doesn't exist).
2. Create `test_silver_cleansing.py`.
3. Write a `pytest` test that passes a mock DataFrame containing a null SSN to your cleansing function, and asserts that the resulting DataFrame has dropped that row.
4. Run `pytest tests/test_silver_cleansing.py` in your local terminal to validate your code.

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
