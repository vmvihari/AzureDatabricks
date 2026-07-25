# Lesson 7.1: Databricks Workflows and Task Orchestration

## Table of Contents
- [Job Clusters vs. All-Purpose Clusters](#job-clusters-vs-all-purpose-clusters)
- [Directed Acyclic Graphs (DAGs)](#directed-acyclic-graphs-dags)
- [Action Step: Orchestrating the Mortgage ETL](#action-step-orchestrating-the-mortgage-etl)
- [Interview Preparation](#interview-preparation)

---

## Job Clusters vs. All-Purpose Clusters

When building and testing code, you use an **All-Purpose Cluster** (also known as an Interactive Cluster). These clusters are designed to stay online so you can run queries repeatedly without waiting for machines to boot up. Because they stay online, they are significantly more expensive.

When running automated production pipelines, you must use **Job Clusters**. 
A Job Cluster is ephemeral. When a Databricks Workflow starts, it spins up the Job Cluster, executes the Python script, and immediately terminates the virtual machines when finished. Job Clusters cost substantially less per DBU (Databricks Unit) than All-Purpose clusters.

**Rule:** Never run scheduled production jobs on an All-Purpose cluster.

---

## Directed Acyclic Graphs (DAGs)

In our Mortgage Data Platform, we cannot calculate the `gold_state_risk_summary` until the `silver_loans` table has finished updating. 

We must orchestrate these scripts in a specific sequence:
`Bronze Ingestion` -> `Silver Cleansing` -> `Gold Aggregation`.

In Databricks, we define this sequence using a **Workflow**. A Workflow creates a **DAG** (Directed Acyclic Graph), where each script is a "Task", and tasks are linked via dependencies (`depends_on`).

---

---

## 🛠️ Action Step (JSON / CLI): Orchestrating the Mortgage ETL

For quick iterations or when using Databricks Asset Bundles (DABs), engineers often define Workflows as JSON code.

1. Navigate to `apps/mortgage-data-platform/` and create a new directory named `deploy`.
2. Inside `deploy`, create `mortgage_workflow.json`.
3. Define the job, specifying a dedicated Job Cluster and sequencing the Medallion scripts.

```json
{
  "name": "mortgage_daily_etl",
  "job_clusters": [
    {
      "job_cluster_key": "mortgage_cluster",
      "new_cluster": {
        "spark_version": "14.3.x-scala2.12",
        "node_type_id": "Standard_DS3_v2",
        "num_workers": 2
      }
    }
  ],
  "tasks": [
    {
      "task_key": "bronze_ingestion",
      "job_cluster_key": "mortgage_cluster",
      "spark_python_task": {
        "python_file": "src/dlt/autoloader_bronze.py"
      }
    },
    {
      "task_key": "silver_cleansing",
      "depends_on": [{"task_key": "bronze_ingestion"}],
      "job_cluster_key": "mortgage_cluster",
      "spark_python_task": {
        "python_file": "src/silver/cleansed_loans.py"
      }
    },
    {
      "task_key": "gold_aggregation",
      "depends_on": [{"task_key": "silver_cleansing"}],
      "job_cluster_key": "mortgage_cluster",
      "spark_python_task": {
        "python_file": "src/gold/state_risk_summary.py"
      }
    }
  ]
}
```

---

## 4. 🛠️ Action Step: Validation & Testing

As discussed in [Lesson 4.5: Asset Bundles](../module-04-dlt/lesson-4.5-asset-bundles.md), you never test a workflow by deploying it directly to Production.

1. Add your Workflow definition to your `databricks.yml` Asset Bundle.
2. Run `databricks bundle deploy -t dev` from your terminal to deploy the job to your Dev workspace.
3. Run `databricks bundle run mortgage_daily_etl -t dev` to trigger the integration test.
4. Verify the tasks run sequentially (Bronze -> Silver -> Gold) and complete successfully.

---

## 5. 🎯 Interview Preparation

> [!TIP]
> **Q1: Why is it an anti-pattern to run production ETL pipelines on an All-Purpose (Interactive) cluster?**
> **Answer:** "All-Purpose clusters are designed for interactive, ad-hoc development and carry a significant premium cost per DBU. They also run the risk of resource contention if multiple jobs or users hit the cluster simultaneously. Production pipelines should strictly use Job Clusters. Job Clusters provide strict workload isolation (ensuring consistent performance), immediately terminate when the job completes to eliminate idle costs, and are billed at a much lower compute rate."

---
[⬅️ Previous: Module 7 Overview](README.md) | [🏠 Main Directory](../../README.md) | [➡️ Next: Lesson 7.2: Monitoring and Alerting](lesson-7.2-monitoring-and-alerting.md)
