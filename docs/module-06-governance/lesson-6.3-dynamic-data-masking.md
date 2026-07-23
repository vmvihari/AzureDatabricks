# Lesson 6.3: Dynamic Data Masking

## Table of Contents
- [The PII Dilemma](#the-pii-dilemma)
- [SQL User-Defined Functions (UDFs)](#sql-user-defined-functions-udfs)
- [Action Step: Masking the SSN](#action-step-masking-the-ssn)
- [Interview Preparation](#interview-preparation)

---

## The PII Dilemma

The `silver_loans` table contains a highly sensitive column: `applicant_ssn` (Social Security Number). 

If a Data Scientist needs to build a model on this table, they need the data, but legally, they are not allowed to view raw SSNs. 

Historically, data engineers solved this by building a completely separate ETL pipeline to create a "scrubbed" version of the table (e.g., `silver_loans_masked`). This doubled the storage costs and maintenance burden.

---

## SQL User-Defined Functions (UDFs)

Unity Catalog introduces **Dynamic Data Masking**. You do not need to copy the data. Instead, you create a SQL User-Defined Function (UDF) that intercepts the query at runtime.

The UDF checks: "Is the person running this query a member of the `compliance_team` group?"
- If YES: Return the raw SSN (`123-45-6789`).
- If NO: Return a masked string (`XXX-XX-6789`).

The data on the underlying ADLS storage remains untouched. The masking happens dynamically in the compute layer.

---

## 🛠️ Action Step: Masking the SSN

Let's protect the SSNs in our Silver layer. 

1. Navigate to `apps/mortgage-data-platform/src/governance/` and create `data_masking.sql`.
2. Write the SQL to create the masking function.
3. Apply the masking function to the `applicant_ssn` column in the `silver_loans` table.

```sql
-- apps/mortgage-data-platform/src/governance/data_masking.sql

USE CATALOG mortgage_prod;

-- 1. Create the masking function
CREATE FUNCTION IF NOT EXISTS silver.mask_ssn(ssn STRING)
  RETURN CASE 
    -- If they are in compliance, show the real SSN
    WHEN is_account_group_member('compliance_team') THEN ssn 
    
    -- Otherwise, mask the first 5 digits
    ELSE CONCAT('XXX-XX-', RIGHT(ssn, 4)) 
  END;

-- 2. Apply the mask to the table column
ALTER TABLE silver.loans 
ALTER COLUMN applicant_ssn 
SET MASK silver.mask_ssn;
```

---

## 🎯 Interview Preparation

> [!TIP]
> **Q1: How do you mask PII data in Databricks without physically deleting the data or maintaining a duplicate 'scrubbed' table?**
> **Answer:** "I use Unity Catalog's Dynamic Data Masking. Instead of physically copying and scrubbing the data—which wastes storage and compute—I create a SQL UDF that utilizes the `is_account_group_member()` function. I then bind this UDF to the sensitive column using `ALTER TABLE ... SET MASK`. When an unauthorized user queries the table, Unity Catalog dynamically obscures the column at runtime (e.g., replacing a string with 'REDACTED'), while authorized users see the raw data. The underlying Parquet files remain completely untouched."

---
[⬅️ Previous: Lesson 6.2: RBAC and Grants](lesson-6.2-rbac-and-grants.md) | [🏠 Main Directory](../../README.md) | [➡️ Next: Lesson 6.4: Data Lineage](lesson-6.4-data-lineage.md)
