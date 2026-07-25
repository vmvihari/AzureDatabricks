# Lesson 4.1: Auto Loader and Structured Streaming

## Table of Contents
- [The Problem with Batch Ingestion](#the-problem-with-batch-ingestion)
- [Spark Structured Streaming](#spark-structured-streaming)
- [Databricks Auto Loader (`cloudFiles`)](#databricks-auto-loader-cloudfiles)
- [Action Step: Streaming Loan Applications](#action-step-streaming-loan-applications)
- [Interview Preparation](#interview-preparation)

---

## The Problem with Batch Ingestion

In Module 2, we used `spark.read.format("csv")` to ingest the raw Bronze data. This is a **Batch** process. 

If new mortgage applications land in the ADLS container tomorrow, running the same script will read *all* the files again, including the ones we already processed yesterday. This leads to massive duplicate processing and wasted compute costs. We need a way to process data **incrementally**—only reading the files that have landed since the last run.

---

## Spark Structured Streaming

Spark Structured Streaming solves this by treating a directory of files as an infinite stream of data. Instead of `spark.read`, you use `spark.readStream`. 
Spark tracks exactly which files it has already processed by writing to a **Checkpoint** directory.

```python
# Standard Spark Streaming (Not Recommended for massive Data Lakes)
df_stream = spark.readStream \
    .format("csv") \
    .schema(loan_schema) \
    .load("abfss://bronze@stmortgagedata<your_initials>.dfs.core.windows.net/landing/loan_applications/")
```

---

## Databricks Auto Loader (`cloudFiles`)

While standard Spark Streaming is good, it struggles when a directory contains millions of files. It literally has to list (`ls`) the entire ADLS container every few seconds to find new files, which eventually bottlenecks the driver node.

Databricks solved this with **Auto Loader**. By simply changing the format to `cloudFiles`, Databricks bypasses the expensive directory listing. 

How does it work? Auto Loader can automatically subscribe to Azure Event Grid notifications. When a new file physically lands in ADLS, Azure sends a tiny message directly to Auto Loader saying, "Hey, a new file just arrived at this path." Auto Loader processes it instantly without ever needing to list the directory.

---

## 🛠️ Action Step: Streaming Loan Applications

Let's upgrade our original Bronze ingestion script to use Auto Loader.

1. Navigate to `apps/mortgage-data-platform/src/dlt/` (we are preparing for DLT in the next lesson).
2. Create a new PySpark script named `autoloader_bronze.py`.
3. Write the PySpark code to incrementally read the CSV loan applications using Auto Loader:
```python
df_stream = spark.readStream \
    .format("cloudFiles") \
    .option("cloudFiles.format", "csv") \
    .option("header", "true") \
    .schema(loan_schema) \
    .load("abfss://bronze@stmortgagedata<your_initials>.dfs.core.windows.net/landing/loan_applications/")
```
4. Write the stream out to the Bronze Delta table, explicitly defining the checkpoint location:
```python
df_stream.writeStream \
    .format("delta") \
    .option("checkpointLocation", "abfss://bronze@stmortgagedata<your_initials>.dfs.core.windows.net/_checkpoints/bronze_loans/") \
    .trigger(availableNow=True) \
    .start("abfss://bronze@stmortgagedata<your_initials>.dfs.core.windows.net/tables/bronze_loans")
```
*(Note: `.trigger(availableNow=True)` is a fantastic feature that tells the stream to process everything that has landed since the last run, and then shut down the cluster to save costs, rather than running 24/7).*

---

## 🛠️ Action Step: Validation & Testing

Structured Streaming logic can be complex to test. However, we must ensure our Auto Loader pipeline correctly processes data increments.

1. Navigate to `apps/mortgage-data-platform/tests/`.
2. Create `test_autoloader_bronze.py`.
3. In your local `pytest` environment, you can use the `MemorySink` or write to a temporary local Delta table to validate that the schema is enforced and rows are processed as expected.
4. Run `pytest tests/test_autoloader_bronze.py` in your local terminal.

---

## 🎯 Interview Preparation

> [!TIP]
> **Q1: Why is Databricks Auto Loader (`cloudFiles`) preferred over a standard PySpark `readStream` with a file source?**
> **Answer:** "A standard `readStream` performs a directory listing to discover new files. As the data lake grows to millions of files, this `ls` operation becomes a massive bottleneck, slowing down ingestion and hanging the Driver node. Auto Loader solves this by integrating directly with cloud notification services like Azure Event Grid. It receives lightweight event notifications exactly when a file lands, completely eliminating the need for expensive directory listing operations. It is significantly faster and more scalable."

---
[⬅️ Previous: Module 3 Overview](../module-03-medallion-and-ml/README.md) | [🏠 Main Directory](../../README.md) | [➡️ Next: Lesson 4.2: Intro to DLT](lesson-4.2-intro-to-dlt.md)
