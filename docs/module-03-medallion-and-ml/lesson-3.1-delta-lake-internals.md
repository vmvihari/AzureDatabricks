# Lesson 3.1: Delta Lake Internals

## Table of Contents
- [What is Delta Lake?](#what-is-delta-lake)
- [The Transaction Log (`_delta_log`)](#the-transaction-log-_delta_log)
- [ACID Transactions](#acid-transactions)
- [Time Travel](#time-travel)
- [Interview Preparation](#interview-preparation)

---

## What is Delta Lake?

In traditional data lakes, data is stored in raw Parquet or CSV files. If a Spark job crashes halfway through writing a Parquet file, the data lake is left in a corrupted, partial state. Furthermore, it is impossible for two Spark clusters to safely write to the same Parquet directory at the same time.

**Delta Lake** is an open-source storage layer that brings reliability to data lakes. It is simply Parquet data files paired with a transactional metadata layer. 

---

## The Transaction Log (`_delta_log`)

When you save a DataFrame as Delta (`df.write.format("delta").save(...)`), Delta Lake creates a hidden directory named `_delta_log`. 

Inside this directory are JSON files (000000.json, 000001.json, etc.). These files act as a master ledger. 
- When Spark wants to read a Delta table, it first reads the `_delta_log`.
- The log tells Spark exactly which Parquet files are considered "valid" and which files have been logically deleted.
- If a Parquet file exists in the directory but is *not* recorded in the transaction log, Spark completely ignores it. 

---

## ACID Transactions

Because of the `_delta_log`, Delta Lake provides **ACID** (Atomicity, Consistency, Isolation, Durability) guarantees:
- **Atomicity:** If our Mortgage Ingestion job fails at 99%, the transaction is never committed to the `_delta_log`. The downstream analysts querying the table will never see partial data. It is all or nothing.
- **Isolation (Concurrency):** Two jobs can write to `silver_loans` simultaneously. Delta Lake uses *Optimistic Concurrency Control*. If both jobs try to modify the exact same Parquet file, Delta Lake will resolve the conflict, allowing one to succeed and forcing the other to retry seamlessly.

---

## Time Travel

Because Delta Lake does not immediately delete physical Parquet files when you update or delete a row (it just marks them as deleted in the `_delta_log`), you can query historical versions of the table!

```python
# Query the table exactly as it looked yesterday
df_yesterday = spark.read \
  .format("delta") \
  .option("timestampAsOf", "2023-10-01") \
  .load("abfss://silver@stmortgagedata<your_initials>.dfs.core.windows.net/tables/silver_loans")
```

---

## 🛠️ Action Step: Setting up the Delta Environment
Before we write code in the next lesson, ensure your Databricks environment (or local PySpark session) is configured to use Delta Lake. 

1. Ensure the `delta-spark` package is installed.
2. In production, ensure you are utilizing the `abfss://` protocol to store your Delta tables in Azure Data Lake Storage, rather than local DBFS.

---

## 🎯 Interview Preparation

> [!TIP]
> **Q1: Explain how Delta Lake handles concurrent writes to the same table without corrupting data.**
> **Answer:** "Delta Lake uses Optimistic Concurrency Control via its `_delta_log` transaction log. When two clusters try to write simultaneously, they both optimistically make their changes to new Parquet files. When they attempt to commit to the transaction log, they check if the table has been modified by another job since they started. If there is a conflict (e.g., both modified the same partition), Delta Lake will throw a `ConcurrentModificationException` and force one of the jobs to retry. This ensures data integrity without locking the entire table."

> [!TIP]
> **Q2: If I delete a row using `DELETE FROM delta_table`, is the data physically gone from the storage account immediately?**
> **Answer:** "No. Delta Lake creates a new Parquet file without that row and adds a new commit to the `_delta_log` indicating the old Parquet file is logically removed. However, the old Parquet file remains in Azure storage. To physically delete the data (for GDPR/CCPA compliance or cost savings), you must explicitly run the `VACUUM` command, which deletes files no longer referenced in the active transaction log."

---
[⬅️ Previous: Module 3 Overview](README.md) | [🏠 Main Directory](../../README.md) | [➡️ Next: Lesson 3.2: Bronze to Silver Cleansing](lesson-3.2-bronze-to-silver-cleansing.md)
