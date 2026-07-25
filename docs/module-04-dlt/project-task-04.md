# Project Task 4: The DLT Credit Score Pipeline

## Table of Contents
- [Objective](#objective)
- [The Challenge](#the-challenge)
- [Acceptance Criteria](#acceptance-criteria)

## Objective
In the Module 4 Action Steps, you transitioned the core Loan Applications pipeline from imperative batch PySpark to declarative Delta Live Tables using Auto Loader, Expectations, and CDC. 

Now, you must independently build a DLT pipeline for the Credit Bureau dataset.

## The Challenge
The Chief Risk Officer noted that a borrower's credit score fluctuates monthly. If we ingest the raw updates without CDC, we will end up with multiple scores for the same SSN in our Silver table, breaking the downstream Gold dashboards.

You must write a declarative DLT pipeline that streams the Credit Bureau data and uses CDC to maintain the absolute latest credit score for each borrower.

1. **The Pipeline Script (`src/dlt/dlt_credit_scores.py`):**
   - Create the file in the `src/dlt/` directory.
   - You must import `dlt`.

2. **The Bronze Ingestion:**
   - Define a `@dlt.table` named `bronze_credit_scores`.
   - Use `spark.readStream.format("cloudFiles")` to ingest the raw CSVs from `abfss://bronze@stmortgagedata<your_initials>.dfs.core.windows.net/landing/credit_scores/`.
   - **Data Quality constraint:** Add a `@dlt.expect_or_drop` constraint ensuring the `credit_score` is strictly between `300` and `850`.

3. **The Silver CDC Application:**
   - Define an empty streaming target table: `dlt.create_streaming_table("silver_current_credit_scores")`.
   - Use `dlt.apply_changes()` to apply SCD Type 1 updates from `bronze_credit_scores` into `silver_current_credit_scores`.
   - Use `ssn` as the primary key.
   - Use `report_date` as the sequence column to ensure an older report doesn't overwrite a newer one.

## Acceptance Criteria
- [ ] `dlt_credit_scores.py` exists and is formatted as a valid DLT pipeline (no `.writeStream` or `.save()` calls).
- [ ] The pipeline implements Auto Loader (`cloudFiles`).
- [ ] A strict DLT expectation drops impossible credit scores.

- [ ] The pipeline implements CDC using `apply_changes` and SCD Type 1 logic.

---

**[✅ View Solution](project-task-04-solution.md)**

---
[⬅️ Previous: Lesson 4.4: CDC & SCD](lesson-4.4-cdc-scd-type-1-2.md) | [🏠 Main Directory](../../README.md) | [➡️ Next: Module 5: Performance & Optimization](../module-05-performance/README.md)
