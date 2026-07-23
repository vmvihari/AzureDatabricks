# Lesson 5.3: Adaptive Query Execution (AQE)

## Table of Contents
- [The Limitation of the Catalyst Optimizer](#the-limitation-of-the-catalyst-optimizer)
- [How AQE Fixes Pipelines at Runtime](#how-aqe-fixes-pipelines-at-runtime)
- [Action Step: Enabling AQE for Gold Aggregations](#action-step-enabling-aqe-for-gold-aggregations)
- [Interview Preparation](#interview-preparation)

---

## The Limitation of the Catalyst Optimizer

As you learned in Lesson 2.1, Spark uses **Lazy Evaluation**. The Catalyst Optimizer looks at your code and builds a physical execution plan *before* it processes any data. 

But what if the Catalyst Optimizer guesses wrong? 
- What if it assumed a join would produce 100 million rows, so it provisioned 200 shuffle partitions, but the filter condition was so strict that it actually produced only 5 rows? Spark will waste enormous amounts of time spinning up 200 empty partitions.
- What if it decided to use a Shuffle Sort Merge join, but after filtering, one side of the table became small enough to fit in memory? It missed the chance to use a lightning-fast Broadcast join.

Before Spark 3.0, you were stuck with the original, potentially terrible plan.

---

## How AQE Fixes Pipelines at Runtime

**Adaptive Query Execution (AQE)** solves this. It allows Spark to re-evaluate and change the execution plan *while the job is actively running*.

As Spark processes data and completes stages, AQE looks at the actual sizes of the newly created data and dynamically applies three major optimizations:
1. **Dynamically Coalescing Shuffle Partitions:** If AQE sees that 150 of your 200 partitions are nearly empty, it will combine them into 5 properly sized partitions, saving massive compute overhead.
2. **Dynamically Switching Join Strategies:** If a large table suddenly becomes small after a `filter()` operation mid-query, AQE will interrupt the job, scrap the Shuffle join plan, and switch to a Broadcast join instantly.
3. **Dynamically Optimizing Skew Joins:** (We will cover this in Lesson 5.4).

---

## 🛠️ Action Step: Enabling AQE for Gold Aggregations

Databricks enables AQE by default in modern runtime versions, but it is a senior-level practice to explicitly ensure it is enabled in your Spark configuration for heavy analytical queries.

1. Open `apps/mortgage-data-platform/src/gold/state_risk_summary.py` (which we created in Lesson 3.4).
2. At the top of the file, explicitly enable AQE on the `SparkSession`.

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, avg, count

spark = SparkSession.builder \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .getOrCreate()

# ... (rest of the script remains exactly the same)
```

---

## 🎯 Interview Preparation

> [!TIP]
> **Q1: How does Adaptive Query Execution (AQE) differ from the standard Catalyst Optimizer?**
> **Answer:** "The standard Catalyst Optimizer generates a static physical execution plan *before* the query starts executing. It relies on stale or estimated statistics, which can lead to highly inefficient plans (like over-provisioning shuffle partitions or missing broadcast join opportunities). AQE runs *during* execution. At the end of every query stage, AQE pauses, looks at the exact size and statistics of the actual materialized data, and dynamically updates the execution plan for the next stage on the fly—such as coalescing empty partitions or switching a Sort Merge Join to a Broadcast Join."

---
[⬅️ Previous: Lesson 5.2: Liquid Clustering](lesson-5.2-liquid-clustering.md) | [🏠 Main Directory](../../README.md) | [➡️ Next: Lesson 5.4: Data Skew and Spill](lesson-5.4-data-skew-and-spill.md)
