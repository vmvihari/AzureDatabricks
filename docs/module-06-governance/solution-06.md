# Project Task 6: Solution

Here is the best-practice SQL solution for governing the Credit Bureau data using Unity Catalog.

## The Unity Catalog Script

Create this script at `apps/mortgage-data-platform/src/governance/register_credit_scores.sql`.

```sql
-- apps/mortgage-data-platform/src/governance/secure_credit_feed.sql

-- 1. Grant RBAC Access
-- Grant usage on the catalog and schema
GRANT USAGE ON CATALOG mortgage_prod TO `risk_team`;
GRANT USAGE ON SCHEMA mortgage_prod.gold TO `risk_team`;

-- Grant read access ONLY to the Gold dashboard table
GRANT SELECT ON TABLE mortgage_prod.gold.credit_exposure TO `risk_team`;

-- 2. Apply Dynamic Data Masking to Silver
-- Apply the existing mask_ssn UDF to the SSN column of the Silver table
ALTER TABLE mortgage_prod.silver.current_credit_scores 
ALTER COLUMN ssn SET MASK mortgage_prod.silver.mask_ssn;
```

## Explanation
- **Managed Tables:** Because we refactored our PySpark code to write Managed Tables, the tables (`current_credit_scores` and `credit_exposure`) already exist in Unity Catalog. We do not need to register them with `CREATE TABLE ... LOCATION`.
- **RBAC (Role-Based Access Control):** We use standard SQL `GRANT` statements. The `risk_team` gets access to the gold table, but they cannot query the silver table because we didn't grant them access to it.
- **Dynamic Data Masking (DDM):** Even if a highly privileged user queries the Silver table, the `MASK` ensures that the `ssn` column is redacted unless the user is in the `data_admins` group (based on the logic inside `mask_ssn`).

---
[⬅️ Back to Project Task 6](project-task-06.md) | [🏠 Main Directory](../../README.md)
