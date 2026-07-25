# Project Task 5: Optimizing the Credit Bureau Feed

## Table of Contents
- [Objective](#objective)
- [The Challenge](#the-challenge)
- [Acceptance Criteria](#acceptance-criteria)

## Objective
In the Module 5 Action Steps, you enabled Liquid Clustering on the `silver_loans` table and wrote a maintenance script to perform bin-packing on the `bronze_loans` table.

Now, you must independently configure performance optimizations for the Credit Bureau dataset pipeline you built in Project Task 4.

## The Challenge
The BI team is complaining that querying the `silver_current_credit_scores` table is taking too long. They frequently query this table to find the credit score for a specific `ssn`.

You must optimize this table to support sub-second lookup times.

1. **Enable Liquid Clustering:**
   - In `apps/mortgage-data-platform/src/utils/`, create a new script named `optimize_credit_scores.py`.
   - Write PySpark SQL to `ALTER TABLE` the `silver_current_credit_scores` Delta Table.
   - You must apply Databricks Liquid Clustering to the table, using the `ssn` column as the clustering key.

2. **Configure Bin-Packing:**
   - In the same script, configure the Spark session property `spark.databricks.delta.optimize.maxFileSize` to perfectly tune the file size for BI dashboard reads (128MB).
   
3. **Execute Maintenance:**
   - Write the PySpark SQL command to run `OPTIMIZE` on the table to physically rewrite the layout according to your new Liquid Clustering key.

## Acceptance Criteria
- [ ] `optimize_credit_scores.py` exists in the `src/utils/` directory.
- [ ] The script uses `ALTER TABLE` to apply Liquid Clustering to the `ssn` column.
- [ ] The script configures the target file size to 128MB (`134217728` bytes).
- [ ] The script successfully executes the `OPTIMIZE` command.

---

**[✅ View Solution](solution-05.md)**

---
[⬅️ Previous: Lesson 5.4: Data Skew and Spill](lesson-5.4-data-skew-and-spill.md) | [🏠 Main Directory](../../README.md) | [➡️ Next: Module 6: Unity Catalog & Governance](../module-06-governance/README.md)
