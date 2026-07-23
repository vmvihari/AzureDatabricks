# Lesson 2.1: Apache Spark Architecture

## Table of Contents
- [The Fundamentals of Distributed Computing](#the-fundamentals-of-distributed-computing)
- [Spark Cluster Architecture](#spark-cluster-architecture)
- [Lazy Evaluation & The Catalyst Optimizer](#lazy-evaluation--the-catalyst-optimizer)
- [Transformations vs. Actions](#transformations-vs-actions)
- [Interview Preparation](#interview-preparation)

---

## The Fundamentals of Distributed Computing
When processing millions of mortgage applications, a single laptop or server will quickly run out of memory. Apache Spark solves this by splitting the massive dataset into smaller chunks (called **Partitions**) and distributing them across multiple computers (called **Worker Nodes**) to be processed in parallel.

---

## Spark Cluster Architecture

In a Databricks environment, a Spark Cluster consists of two primary components:

### 1. The Driver Node
The Driver is the "brain" of the cluster. When you execute a PySpark notebook, the Driver translates your Python code into a physical execution plan. 
- It tracks where all the data partitions are located.
- It delegates tasks to the Worker Nodes.
- It collects the final results (if requested).

### 2. The Worker Nodes (Executors)
The Workers are the "muscle." A cluster can have dozens or hundreds of Worker nodes.
- Each Worker runs one or more **Executors**.
- The Executors receive tasks from the Driver, process their specific data partition, and return the status to the Driver.
- If a Worker node crashes (e.g., due to Azure hardware failure), the Driver simply reassigns its task to another healthy Worker node. This provides extreme fault tolerance.

---

## Lazy Evaluation & The Catalyst Optimizer

Spark does not execute code the moment you write it. This is called **Lazy Evaluation**.

When you write a series of Dataframe transformations (e.g., filtering out denied loans, converting string dates to timestamps), Spark simply builds a logical plan (a DAG - Directed Acyclic Graph) of what you *want* to do. 

It passes this plan to the **Catalyst Optimizer**, which rewrites your plan to be as efficient as possible. Only when you explicitly ask Spark to return the data (an **Action**) does it actually execute the optimized plan.

---

## Transformations vs. Actions

Understanding the difference between these two operations is critical for writing efficient Spark code.

- **Transformations (Lazy):** Operations that create a new DataFrame from an existing one. 
  - Examples: `select()`, `filter()`, `withColumn()`, `join()`.
  - *No data is processed when a transformation is called.*
- **Actions (Eager):** Operations that trigger the Catalyst Optimizer to execute the physical plan and return data to the Driver or write it to storage.
  - Examples: `display()`, `count()`, `collect()`, `write()`.

---

## 🎯 Interview Preparation

> [!TIP]
> **Q1: Explain Lazy Evaluation and why it is critical for Spark's performance.**
> **Answer:** "Lazy evaluation means Spark does not immediately execute transformations. Instead, it builds a Directed Acyclic Graph (DAG) of the logical operations. This is critical because it allows the **Catalyst Optimizer** to analyze the entire query from start to finish before executing it. For example, if I join two massive tables and then filter for `state = 'CA'`, the Catalyst Optimizer will push that filter up to the storage level *before* the join occurs (Predicate Pushdown), drastically reducing the amount of data shuffled across the network."

> [!TIP]
> **Q2: What happens if a Worker Node fails during a long-running Databricks Job?**
> **Answer:** "Because Spark keeps track of the DAG (the lineage of transformations), it is inherently fault-tolerant. If a Worker node fails, the Driver node detects the failure and simply reassigns the lost partition's tasks to another healthy Worker node. The job will take slightly longer, but it will complete successfully without requiring human intervention."

---
[⬅️ Previous: Module 2 Overview](README.md) | [🏠 Main Directory](../../README.md) | [➡️ Next: Lesson 2.2: Reading Files from ADLS](lesson-2.2-ingest-files.md)
