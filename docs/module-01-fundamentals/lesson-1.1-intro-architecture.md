# Lesson 1.1: Data Analytics in Mortgage & Medallion Architecture

## Table of Contents
- [1. Introduction: The Evolution of Data Architecture](#1-introduction-the-evolution-of-data-architecture)
- [2. The Medallion Architecture (Deep Dive)](#2-the-medallion-architecture-deep-dive)
- [3. Senior-Level Insights: Designing for Production](#3-senior-level-insights-designing-for-production)
- [4. 🎯 Interview Preparation](#4--interview-preparation)


## 1. Introduction: The Evolution of Data Architecture
To understand Azure Databricks and the Lakehouse, you must understand the problem it solves. Historically, data architecture evolved through two main paradigms before the Lakehouse:
1. **Data Warehouses (e.g., Teradata, Snowflake):** Great for structured data and ACID transactions, but highly rigid, expensive, and terrible for Machine Learning or unstructured data (images, JSON).
2. **Data Lakes (e.g., Hadoop, raw ADLS):** Cheap storage for unstructured data and great for ML, but lacked ACID transactions (updating/deleting rows was nearly impossible) and suffered from the "Data Swamp" problem (poor governance and data quality).

**The Data Lakehouse (Databricks + Delta Lake):** 
The Lakehouse paradigm merges the best of both worlds. It sits on top of cheap cloud object storage (Azure Data Lake Storage Gen2) but uses an open-source storage layer (**Delta Lake**) to bring ACID transactions, schema enforcement, and time travel to the data lake. This allows Data Engineers, Data Analysts, and Data Scientists to work on the exact same platform.

---

## 2. The Medallion Architecture (Deep Dive)
The Medallion Architecture is a logical data design pattern used in a Lakehouse to incrementally and progressively improve the structure and quality of data. 

### 🥉 Bronze Layer (Raw / Ingestion)
- **Purpose:** Provide a historical archive of raw source data.
- **Rules:** 
  - Data is strictly **append-only**. We do not update or delete records here.
  - Keep the data exactly as it arrived (JSON, CSV, Parquet) but wrap it in a Delta table.
  - Do not apply schema validation or data type casting (treat everything as strings if necessary) to prevent pipelines from failing when upstream systems change.
- **Mortgage Context:** Raw loan applications arrive daily as CSVs. Fraud API responses arrive as nested JSON. Both are dumped into Bronze tables. If a column name changes upstream, Bronze accepts it without breaking (Schema Evolution).

### 🥈 Silver Layer (Cleansed / Conformed)
- **Purpose:** Provide an "Enterprise view" of all key business entities.
- **Rules:**
  - Enforce schemas and strict data types.
  - Cleanse data: Handle NULL values, remove duplicates, and standardize formats (e.g., date formats).
  - **Change Data Capture (CDC):** This is where we handle updates. If a customer changes their address, we apply an SCD Type 1 (overwrite) or SCD Type 2 (track history) merge.
- **Mortgage Context:** We merge daily updates into our `silver_customers` table. We mask PII (SSNs). We parse the nested Fraud JSON into a clean, flat table. This layer is highly queried by Data Scientists.

### 🥇 Gold Layer (Curated / Business-level)
- **Purpose:** Serve final aggregations and project-specific databases for BI and reporting.
- **Rules:**
  - Highly denormalized and read-optimized (often using Star Schemas with Fact and Dimension tables).
  - Complex joins and business logic are applied here.
- **Mortgage Context:** We create a `gold_loan_risk_summary` table. We join the cleaned loan data with the fraud flags and run it through our AI prediction model to calculate a `default_probability`. PowerBI connects directly to this table.

---

## 3. Senior-Level Insights: Designing for Production
When designing this architecture, senior engineers must consider:
- **Idempotency:** If your pipeline fails halfway and you rerun it, it should not create duplicate data. Delta Lake’s `MERGE` statements ensure idempotency.
- **Late-Arriving Data:** What happens if a loan application from Tuesday arrives on Thursday? The architecture must gracefully upsert this into Silver without destroying Gold aggregations.
- **Data Contracts:** Bronze is notoriously brittle if upstream APIs change drastically. Senior teams implement data contracts (using tools like Unity Catalog schema enforcement) to reject bad payloads before they corrupt the lake.

---

## 4. 🎯 Interview Preparation

> [!TIP]
> **Common Interview Questions on Architecture**

**Q1: Why use the Medallion Architecture instead of just cleaning the data immediately upon ingestion?**
**Answer:** "The Bronze layer acts as a safety net. If a business logic bug is introduced in our Silver cleansing process, having the untouched Bronze data allows us to completely recalculate the Silver layer from scratch. If we cleaned the data immediately and threw away the raw payload, that data would be lost forever."

**Q2: How does a Lakehouse differ from a Data Warehouse?**
**Answer:** "A Data Warehouse tightly couples compute and storage, usually requiring data to be loaded into proprietary formats (like Snowflake). A Lakehouse decouples compute (Databricks) from storage (ADLS Gen2) and uses open formats (Parquet/Delta). This prevents vendor lock-in, allows seamless Machine Learning on the same data, and is significantly cheaper for massive datasets."

**Q3: How do you handle schema drift in your pipelines?**
**Answer:** "In the Bronze layer, I enable Delta Lake's `mergeSchema` option to automatically capture new columns added by the source system. However, in the Silver layer, I strictly enforce schemas to protect data quality. If an unexpected schema change occurs in Silver, the pipeline correctly fails, triggering an alert for the engineering team to review the upstream change."

[⬅️ Previous: Module 1 Overview](README.md) | [🏠 Main Directory](../../README.md) | [➡️ Next: Lesson 1.2: Azure Resources](lesson-1.2-azure-resources.md)
