# Lesson 6.1: Introduction to Unity Catalog

## Table of Contents
- [The Problem with the Hive Metastore](#the-problem-with-the-hive-metastore)
- [Unity Catalog: Centralized Governance](#unity-catalog-centralized-governance)
- [The 3-Level Namespace](#the-3-level-namespace)
- [Action Step: Setting up the Mortgage Catalog](#action-step-setting-up-the-mortgage-catalog)
- [Interview Preparation](#interview-preparation)

---

## The Problem with the Hive Metastore

Historically, Databricks used the **Hive Metastore** to manage tables. The fatal flaw of the Hive Metastore is that it was *workspace-local*.

If your company had three Databricks Workspaces (e.g., Data Engineering, Data Science, and BI Reporting), each workspace had its own isolated metastore. If the Engineering team created a `silver_loans` table, the BI team could not see it unless the engineers manually duplicated the metadata. Security was fragmented, and auditing was a nightmare.

---

## Unity Catalog: Centralized Governance

**Unity Catalog (UC)** is Databricks' modern, centralized governance solution. 

Instead of being tied to a single workspace, Unity Catalog sits at the Azure Account level. You register your Delta Tables once in Unity Catalog, and they are instantly available across *all* workspaces in your enterprise (Engineering, ML, BI), all governed by a single, centralized security model.

---

## The 3-Level Namespace

To accommodate this enterprise scale, Unity Catalog uses a **3-Level Namespace** for all objects: `catalog.schema.table`.

1. **Catalog:** The highest level. Usually maps to a business unit or environment (e.g., `mortgage_prod`, `mortgage_dev`).
2. **Schema:** The database layer. In the Medallion architecture, we map these to our layers (`bronze`, `silver`, `gold`).
3. **Table:** The actual Delta Table (`loans`, `fraud_flags`).

To query a table in UC, you use the full namespace:
```sql
SELECT * FROM mortgage_prod.silver.loans;
```

---

## 🛠️ Action Step (SQL Script): Setting up the Mortgage Catalog

Let's establish our Unity Catalog namespace for the Mortgage Data Platform.

1. Navigate to `apps/mortgage-data-platform/src/` and create a new directory named `governance`.
2. Inside this folder, create a SQL file named `setup_catalogs.sql`.
3. Write the SQL statements to create the production catalog and our Medallion schemas.

```sql
-- apps/mortgage-data-platform/src/governance/setup_catalogs.sql

-- 1. Create the top-level Catalog
CREATE CATALOG IF NOT EXISTS mortgage_prod;

-- 2. Set it as the default for this session
USE CATALOG mortgage_prod;

-- 3. Create the Medallion schemas
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- Example of creating a table in UC pointing to our ADLS data
CREATE TABLE IF NOT EXISTS silver.loans
USING DELTA
LOCATION 'abfss://silver@stmortgagedata<your_initials>.dfs.core.windows.net/tables/silver_loans';
```

---

## 5. 🎯 Interview Preparation

> [!TIP]
> **Q1: Why is Unity Catalog a massive upgrade over the legacy Hive Metastore in a multi-workspace architecture?**
> **Answer:** "The legacy Hive Metastore was scoped to a single workspace, creating data silos. If the Data Engineering workspace created a table, the Data Science workspace couldn't securely access it without complex, fragmented workarounds. Unity Catalog provides a centralized, account-level governance layer. You register a table and its access controls once, and it is universally and securely accessible across every workspace in the enterprise, utilizing a standard 3-level namespace (`catalog.schema.table`)."

---
[⬅️ Previous: Module 6 Overview](README.md) | [🏠 Main Directory](../../README.md) | [➡️ Next: Lesson 6.2: RBAC and Grants](lesson-6.2-rbac-and-grants.md)
