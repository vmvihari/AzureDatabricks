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
   - Read the `bronze_credit_scores` Delta table.
   - Use `dropDuplicates(["ssn"])` to ensure we only have the latest credit score for each person.
   - Use `.withColumnRenamed("ssn", "applicant_ssn")` so it matches our loan data standard.
   - Write it out to `abfss://silver@stmortgagedata<your_initials>.dfs.core.windows.net/tables/silver_credit_scores` in Delta format using `overwrite` mode.

2. **The Gold Pipeline (`src/gold/credit_tier_exposure.py`):**
   - Read both `silver_loans` and `silver_credit_scores`.
   - Perform an `inner join` on `applicant_ssn`.
   - Use `withColumn` and the `when().otherwise()` functions to create a `credit_tier` column:
     - `> 750`: "Excellent"
     - `650 - 750`: "Fair"
     - `< 650`: "Poor"
   - Use `groupBy("credit_tier")` and `sum("loan_amount")`.
   - Write the aggregated DataFrame to `abfss://gold@stmortgagedata<your_initials>.dfs.core.windows.net/tables/gold_credit_exposure`.

## Acceptance Criteria
- [ ] `cleansed_credit_scores.py` exists and successfully deduplicates the Bronze data.
- [ ] `credit_tier_exposure.py` exists, successfully joins the two Silver tables, categorizes the scores, and aggregates the exposure.
- [ ] All code adheres to strict PEP 8 Python naming conventions (`snake_case` variables, no hardcoded secrets).

---

**[✅ View Solution](solution-03.md)**

---
[⬅️ Previous: Lesson 3.4: Gold Aggregations](lesson-3.4-silver-to-gold-aggregations.md) | [🏠 Main Directory](../../README.md) | [➡️ Next: Module 4: Delta Live Tables](../module-04-dlt/README.md)
