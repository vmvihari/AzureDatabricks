# Project Task 2: The Credit Bureau Feed

## Table of Contents
- [Objective](#objective)
- [The Challenge](#the-challenge)
- [Acceptance Criteria](#acceptance-criteria)

## Objective
Now that you have built the ingestion scripts for Loans, Servicing Events, and Fraud APIs using the step-by-step Action Steps in the lessons, it is time for a novel challenge. You must ingest a completely new dataset without a step-by-step guide.

## The Challenge
The Mortgage Data Platform needs to ingest daily Credit Score updates from a 3rd-party Credit Bureau (e.g., Equifax). 

You must write a PySpark script to ingest these credit scores into the Bronze layer securely and efficiently.

1. **The Dataset:**
   - Assume the data lands daily as CSV files in: `abfss://bronze@stmortgagedata<your_initials>.dfs.core.windows.net/landing/credit_scores/`.
   - The CSV contains three columns: `ssn` (String), `credit_score` (Integer), and `report_date` (Date).

2. **The Script:**
   - In `apps/mortgage-data-platform/src/bronze/`, create a new script named `ingest_credit_scores.py`.
   - Write PySpark code to read the CSV data.
   - **Crucial:** You must define a strict PySpark `StructType` schema. Do NOT use `inferSchema=True`.
   
3. **The Output:**
   - Write the DataFrame to the Bronze layer as a Delta Table at: `abfss://bronze@.../tables/bronze_credit_scores`.
   - Use the `append` mode so daily updates do not overwrite historical records.

## Acceptance Criteria
- [ ] `ingest_credit_scores.py` exists in the `src/bronze/` directory.
- [ ] The script uses `spark.read.format("csv")` but strictly defines a `StructType` schema.

- [ ] The output is written in `delta` format using `append` mode.

---

**[✅ View Solution](solution-02.md)**

---
[⬅️ Previous: Lesson 2.5: PySpark Transformations](lesson-2.5-pyspark-transformations.md) | [🏠 Main Directory](../../README.md) | [➡️ Next: Module 3: Medallion Architecture & ML](../module-03-medallion-and-ml/README.md)
