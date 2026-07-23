# Project Task 6: Governing the Credit Bureau Feed

## Table of Contents
- [Objective](#objective)
- [The Challenge](#the-challenge)
- [Acceptance Criteria](#acceptance-criteria)

## Objective
In the Module 6 Action Steps, you used SQL to configure the Unity Catalog 3-level namespace (`mortgage_prod.silver.loans`), established RBAC for the Data Analysts, and deployed a Dynamic Data Mask to protect Social Security Numbers.

Now, you must secure the Credit Bureau pipeline that you built in previous modules.

## The Challenge
The Credit Bureau pipeline creates a Silver table (`silver_current_credit_scores`) and a Gold table (`gold_credit_exposure`). 

The Chief Risk Officer needs their team (the `risk_team` group) to query the Gold dashboard table. However, the Silver table contains highly sensitive SSNs that the risk team is not legally authorized to view in raw format.

You must create a single SQL script to register the tables and enforce governance.

1. **The Governance Script:**
   - In `apps/mortgage-data-platform/src/governance/`, create a new script named `secure_credit_feed.sql`.

2. **Register the Tables:**
   - Write the SQL to register the two existing Delta tables into Unity Catalog using the `CREATE TABLE IF NOT EXISTS` syntax with the `LOCATION` keyword pointing to their `abfss://` ADLS paths.
   - Register them as:
     - `mortgage_prod.silver.current_credit_scores`
     - `mortgage_prod.gold.credit_exposure`

3. **Grant RBAC Access:**
   - Write the SQL `GRANT` statements to give the `risk_team` group usage on the catalog and gold schema, and `SELECT` access *only* on the Gold table.

4. **Apply Dynamic Data Masking:**
   - You do not need to create a new masking function. The `silver.mask_ssn` UDF you created in Lesson 6.3 already exists in Unity Catalog!
   - Write the `ALTER TABLE` SQL command to apply `silver.mask_ssn` to the `ssn` column of the `mortgage_prod.silver.current_credit_scores` table.

## Acceptance Criteria
- [ ] `secure_credit_feed.sql` exists.
- [ ] Uses the 3-level namespace (`catalog.schema.table`).
- [ ] Contains the correct `GRANT` statements for the `risk_team`.
- [ ] Applies the existing `mask_ssn` UDF to the Silver credit score table.

---
[⬅️ Previous: Lesson 6.4: Data Lineage](lesson-6.4-data-lineage.md) | [🏠 Main Directory](../../README.md) | [➡️ Next: Module 7 (Coming Soon)](#)
