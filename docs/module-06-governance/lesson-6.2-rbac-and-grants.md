# Lesson 6.2: Role-Based Access Control (RBAC)

## Table of Contents
- [The Principle of Least Privilege](#the-principle-of-least-privilege)
- [SQL Grants in Unity Catalog](#sql-grants-in-unity-catalog)
- [Action Step: Securing the Lakehouse](#action-step-securing-the-lakehouse)
- [Interview Preparation](#interview-preparation)

---

## The Principle of Least Privilege

In a production environment, you should never grant a user more access than they absolutely need to perform their job. This is the **Principle of Least Privilege**.

- **Data Engineers:** Need `SELECT`, `MODIFY`, and `CREATE` privileges on the `bronze`, `silver`, and `gold` schemas to build pipelines.
- **Data Analysts / BI Tools:** Should only have `SELECT` (read-only) privileges, and strictly only on the `gold` schema. They have no business reading raw, messy `bronze` data.

---

## SQL Grants in Unity Catalog

Unity Catalog uses standard ANSI SQL to manage permissions, making it incredibly intuitive.

```sql
-- Grant usage on the catalog and schema
GRANT USAGE ON CATALOG mortgage_prod TO data_analysts;
GRANT USAGE ON SCHEMA mortgage_prod.gold TO data_analysts;

-- Grant read access to a specific table
GRANT SELECT ON TABLE mortgage_prod.gold.state_risk_summary TO data_analysts;
```

**What is `USAGE`?** 
In Unity Catalog, you cannot read a table unless you also have the `USAGE` privilege on both its parent schema and its parent catalog. This prevents a user from accidentally querying a table in a catalog they aren't supposed to know exists.

---

## 🛠️ Action Step (SQL Script): Securing the Lakehouse

Let's assume our Azure Active Directory (Entra ID) is synced with Databricks, providing us with a group named `data_analysts`. Let's secure our Medallion architecture using SQL.

1. Navigate to `apps/mortgage-data-platform/src/governance/` and create `permissions.sql`.
2. Write the SQL to give the analysts read access to the Gold layer, but explicitly prevent them from modifying anything.

```sql
-- apps/mortgage-data-platform/src/governance/permissions.sql

-- 1. Grant base usage to the analysts
GRANT USAGE ON CATALOG mortgage_prod TO data_analysts;
GRANT USAGE ON SCHEMA mortgage_prod.gold TO data_analysts;

-- 2. Grant SELECT on all current tables in the gold schema
GRANT SELECT ON SCHEMA mortgage_prod.gold TO data_analysts;

-- 3. Explicitly deny access to the raw data layers
REVOKE USAGE ON SCHEMA mortgage_prod.bronze FROM data_analysts;
REVOKE USAGE ON SCHEMA mortgage_prod.silver FROM data_analysts;
```

---

## 4. 🎯 Interview Preparation

> [!TIP]
> **Q1: Explain how you would safely expose Gold layer reporting tables to analysts without giving them access to the raw Parquet files in ADLS.**
> **Answer:** "Historically, securing the raw ADLS Gen2 storage accounts via Azure IAM was complex because Databricks clusters assumed a single identity. With Unity Catalog, I don't give the analysts any Azure IAM permissions to the storage account at all. Instead, the Databricks cluster uses a secure Storage Credential to read the Parquet files. I then use Unity Catalog SQL (`GRANT SELECT ON mortgage_prod.gold.table TO analysts`) to control access at the logical table layer. If an analyst tries to bypass UC and read the raw ADLS path directly, Azure will block them."

---
[⬅️ Previous: Lesson 6.1: Intro to Unity Catalog](lesson-6.1-intro-to-unity-catalog.md) | [🏠 Main Directory](../../README.md) | [➡️ Next: Lesson 6.3: Dynamic Data Masking](lesson-6.3-dynamic-data-masking.md)
