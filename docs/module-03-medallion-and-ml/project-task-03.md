# Project Task 3: The Credit Score Aggregation

## Table of Contents
- [Objective](#objective)
- [The Challenge](#the-challenge)
- [Acceptance Criteria](#acceptance-criteria)

## Objective
In the Module 3 Action Steps, you built the core `silver_loans` pipeline and the `gold_state_risk_summary` table. Now it is time to build a pipeline for the Credit Bureau data you ingested in Project Task 2.

## The Challenge
The Chief Risk Officer wants a new dashboard showing our total financial exposure (sum of all loan amounts) segmented by Credit Score tiers (e.g., how much money have we lent to people with "Excellent" vs "Poor" credit).

To do this, you must transform the raw Bronze credit data into a Silver table, and then join it with our `silver_loans` table to create a final Gold table.

1. **The Silver Pipeline (`src/silver/cleansed_credit_scores.py`):**
   - Write a testable function `cleanse_credit_scores(df)`.
   - Use `dropDuplicates(["ssn"])` to ensure we only have the latest credit score for each person.
   - Use `.withColumnRenamed("ssn", "applicant_ssn")` so it matches our loan data standard.
   - In the `__main__` block, read the Bronze table, run your function, and write it out to `abfss://silver@stmortgagedata<your_initials>.dfs.core.windows.net/tables/silver_credit_scores` in Delta format using `overwrite` mode.

2. **The Gold Pipeline (`src/gold/credit_exposure.py`):**
   - Write a testable function `aggregate_exposure(df_loans, df_scores)`.
   - Perform an `inner join` on `applicant_ssn`.
   - Use `withColumn` and the `when().otherwise()` functions to create a `credit_tier` column:
     - `> 750`: "Excellent"
     - `650 - 750`: "Fair"
     - `< 650`: "Poor"
   - Use `groupBy("credit_tier")` and `sum("loan_amount")`.
   - In the `__main__` block, read the Silver tables, run your function, and write the aggregated DataFrame to `abfss://gold@stmortgagedata<your_initials>.dfs.core.windows.net/tables/gold_credit_exposure`.

3. **Unit Testing (`tests/unit/`):**
   - Create `tests/unit/silver/test_cleansed_credit_scores.py`. Write a Pytest function that verifies your `cleanse_credit_scores` logic successfully drops duplicates and renames the SSN column, using a mock DataFrame.
   - Create `tests/unit/gold/test_credit_exposure.py`. Write a Pytest function that passes two mock DataFrames (loans and scores) to `aggregate_exposure` and verifies the math for at least one credit tier.

## Acceptance Criteria
- [ ] `cleansed_credit_scores.py` contains a testable function that deduplicates data.
- [ ] `credit_exposure.py` contains a testable function that aggregates the exposure by tier.
- [ ] Both Pytest files exist and successfully pass when running `pytest tests/unit/`.
- [ ] All code adheres to strict PEP 8 Python naming conventions (`snake_case` variables, no hardcoded secrets).

---

**[✅ View Solution](solution-03.md)**

---
[⬅️ Previous: Lesson 3.5: Databricks Connect](lesson-3.5-databricks-connect.md) | [🏠 Main Directory](../../README.md) | [➡️ Next: Module 4: Delta Live Tables](../module-04-dlt/README.md)
