# Lesson 5.4: Data Skew and Disk Spill

## Table of Contents
- [Identifying Data Skew in the Spark UI](#identifying-data-skew-in-the-spark-ui)
- [The Consequence: Disk Spill](#the-consequence-disk-spill)
- [Mitigation Strategies](#mitigation-strategies)
- [Interview Preparation](#interview-preparation)

---

## Identifying Data Skew in the Spark UI

**Data Skew** is the silent killer of Big Data pipelines. 

Imagine you `groupBy("state")` to calculate loan exposure. California (CA) has 10,000,000 loans, while Wyoming (WY) has 500 loans. 
Because Spark distributes data by the group key, the Worker Node assigned to "CA" receives 99% of the data, while the other nodes receive almost nothing.

**How to spot it in the Spark UI:**
1. Open the **Spark History Server** for a completed job.
2. Look at the **Tasks** tab for the longest-running Stage.
3. If you see that the *Median* task duration is 2 seconds, but the *Max* task duration is 45 minutes, you have severe Data Skew. One single Executor is holding up the entire cluster.

---

## The Consequence: Disk Spill

When one Executor receives 10,000,000 rows, it will likely run out of RAM. 

When a Spark Executor runs out of RAM, it doesn't immediately crash. Instead, it starts writing the excess data to its local Solid State Drive (SSD). This is called **Disk Spill** (specifically `Spill (Memory)` and `Spill (Disk)` in the Spark UI).

Because SSDs are vastly slower than RAM, the job grinds to a halt. If the disk fills up, the job will finally crash with an `Out Of Memory (OOM)` error.

---

## Mitigation Strategies

If you identify Data Skew, how do you fix it?

### 1. AQE Skew Join Optimization
If AQE (from Lesson 5.3) is enabled, it attempts to detect skew automatically. If it sees that the "CA" partition is massively larger than the others, AQE will dynamically split the "CA" partition into smaller sub-partitions and assign them to multiple idle executors.

### 2. Explicit Skew Hints
If AQE fails to catch it, you can explicitly tell the Catalyst Optimizer that a table is skewed using a SQL hint:

```python
# Tell Spark that the silver_loans table is heavily skewed on the state column
df_joined = df_massive_loans.hint("skew", "state").join(
    df_other_table, 
    "state"
)
```

### 3. Salting (The Legacy Nuclear Option)
Before AQE and Hints, engineers used "Salting". They would append a random integer (1 to 10) to the heavily skewed key (e.g., `CA_1`, `CA_7`). This forced Spark to distribute California's data across 10 different executors. After the join/aggregation, they would strip the salt away. *(Note: AQE essentially does this automatically now).*

---

## 🎯 Interview Preparation

> [!TIP]
> **Q1: If a Spark job takes 10 minutes, but you notice in the Spark UI that 199 tasks finished in 1 minute while 1 task took 9 minutes, what is happening and how do you fix it?**
> **Answer:** "This is a textbook case of Data Skew. One executor received a disproportionately large partition of data because the join or group key was highly unevenly distributed (e.g., grouping by a null column, or a highly populated state like California). Because that executor was overloaded, it likely experienced Disk Spill, causing the 9-minute delay. To fix it, I would ensure Adaptive Query Execution (AQE) is enabled so it can dynamically split the skewed partition at runtime. If that fails, I would apply a `/*+ SKEW */` hint in the query or implement manual salting on the join key to evenly distribute the data."

---
[⬅️ Previous: Lesson 5.3: Adaptive Query Execution](lesson-5.3-adaptive-query-execution.md) | [🏠 Main Directory](../../README.md) | [➡️ Next: Project Task 5](project-task-05.md)
