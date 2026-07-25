# Project Task 6: Solution

Here is the best-practice SQL solution for governing the Credit Bureau data using Unity Catalog.

## The Unity Catalog Script

Create this script at `apps/mortgage-data-platform/src/governance/register_credit_scores.sql`.

```sql
-- 1. Create the External Location for the Credit Scores Landing Zone (Optional, if not covered by broader locations)
-- Assuming the admin has already created the credential 'adls_cred'
CREATE EXTERNAL LOCATION IF NOT EXISTS credit_scores_landing
  URL 'abfss://bronze@stmortgagedata<your_initials>.dfs.core.windows.net/landing/credit_scores/'
  WITH (CREDENTIAL adls_cred);

-- 2. Register the Bronze Table (External Table)
CREATE TABLE IF NOT EXISTS mortgage_prod.bronze.credit_scores (
  loan_id STRING,
  ssn STRING,
  credit_score INT,
  report_date DATE,
  _ingestion_timestamp TIMESTAMP,
  _source_file STRING
)
USING DELTA
LOCATION 'abfss://bronze@stmortgagedata<your_initials>.dfs.core.windows.net/tables/bronze_credit_scores';

-- 3. Register the Silver Table (External Table)
CREATE TABLE IF NOT EXISTS mortgage_prod.silver.current_credit_scores (
  ssn STRING,
  credit_score INT,
  report_date DATE
)
USING DELTA
LOCATION 'abfss://silver@stmortgagedata<your_initials>.dfs.core.windows.net/tables/silver_current_credit_scores';

-- 4. Apply Dynamic Row-Level Security (RLS) to Silver
-- We only want branch managers to see credit scores for loans originating in their specific branch.
-- Assuming there is a mapping table or we join on loans to get the branch. 
-- For a simplified example directly on the SSN table (if it had a branch column):

-- First, create a mapping table to link users to branches (if it doesn't exist)
CREATE TABLE IF NOT EXISTS mortgage_prod.silver.branch_managers (
  manager_email STRING,
  branch_code STRING
);

-- Create the RLS function
CREATE OR REPLACE FUNCTION mortgage_prod.silver.credit_score_rls(branch_code_param STRING)
RETURN IF(
  is_account_group_member('data_admins'),
  true,
  EXISTS (
    SELECT 1 FROM mortgage_prod.silver.branch_managers 
    WHERE manager_email = current_user() AND branch_code = branch_code_param
  )
);

-- Apply the RLS function to the table
-- Note: In our specific Silver table schema defined above, we don't have a branch_code. 
-- To apply this in practice, you must alter the pipeline to include branch_code in the Silver table.
-- ALTER TABLE mortgage_prod.silver.current_credit_scores SET ROW FILTER mortgage_prod.silver.credit_score_rls ON (branch_code);

-- 5. Grant Access
-- Grant read access to the Data Science team so they can build risk models
GRANT SELECT ON TABLE mortgage_prod.silver.current_credit_scores TO `data_science_team`;

-- 6. Revoke Public Access (Zero Trust)
-- Ensure no one can access the raw bronze PII data
REVOKE SELECT ON TABLE mortgage_prod.bronze.credit_scores FROM `users`;
```

## Explanation
- **External Tables:** We register the Delta tables using `LOCATION`. If we drop these tables from Unity Catalog, the underlying Parquet files in ADLS Gen2 remain safe.
- **Row-Level Security:** We define a SQL UDF that checks `current_user()`. If the user is an admin, they see everything. Otherwise, they only see rows where their email matches the branch mapping.
- **Zero Trust:** We explicitly `REVOKE` access to the Bronze layer to prevent unauthorized users from viewing the raw PII data (SSNs).

---
[⬅️ Back to Project Task 6](project-task-06.md) | [🏠 Main Directory](../../README.md)
