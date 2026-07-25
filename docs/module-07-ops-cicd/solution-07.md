# Project Task 7: Solution

Here is the best-practice Databricks Asset Bundle (DAB) solution for orchestrating the Credit Score pipeline.

## The Workflow Definition

Add this block to your `apps/mortgage-data-platform/databricks.yml` file, under the `resources.jobs` section.

```yaml
resources:
  jobs:
    credit_bureau_daily_etl:
      name: credit_bureau_daily_etl
      
      # Define a reusable Job Cluster for the tasks
      job_clusters:
        - job_cluster_key: credit_cluster
          new_cluster:
            spark_version: "14.3.x-scala2.12"
            node_type_id: "Standard_DS3_v2"
            num_workers: 2

      # Schedule the job to run daily at 3:00 AM UTC
      schedule:
        quartz_cron_expression: "0 0 3 * * ?"
        timezone_id: "UTC"
        pause_status: "UNPAUSED"

      tasks:
        # Task 1: Bronze Ingestion (Raw CSVs)
        - task_key: ingest_bronze
          job_cluster_key: credit_cluster
          spark_python_task:
            python_file: "src/bronze/ingest_credit_scores.py"

        # Task 2: Silver Cleansing (Depends on Bronze)
        - task_key: cleanse_silver
          depends_on:
            - task_key: ingest_bronze
          job_cluster_key: credit_cluster
          spark_python_task:
            python_file: "src/silver/cleansed_credit_scores.py"

        # Task 3: Gold Aggregation (Depends on Silver)
        - task_key: aggregate_gold
          depends_on:
            - task_key: cleanse_silver
          job_cluster_key: credit_cluster
          spark_python_task:
            python_file: "src/gold/credit_exposure.py"
```

## Validation & Deployment

1. **Validate the YAML syntax:**
   ```bash
   databricks bundle validate -t dev
   ```

2. **Deploy to your Developer Sandbox:**
   ```bash
   databricks bundle deploy -t dev
   ```

3. **Trigger the Integration Test:**
   ```bash
   databricks bundle run credit_bureau_daily_etl -t dev
   ```
   *Verify in the Databricks UI that all three tasks run sequentially and succeed.*

## Explanation
- **Asset Bundles (DABs):** We define the workflow directly in `databricks.yml`. This completely replaces the need for Terraform to manage Databricks Jobs, keeping the Data Engineering workflow entirely within the Databricks CLI ecosystem.
- **Job Clusters:** We define a single `job_cluster_key` and attach all three tasks to it. Databricks will spin up this ephemeral cluster for the first task, reuse it for the second and third tasks, and then immediately terminate it, saving significant costs compared to running on an interactive cluster.
- **Dependencies:** We use `depends_on` to ensure strict execution order (Bronze -> Silver -> Gold).

---
[⬅️ Back to Project Task 7](project-task-07.md) | [🏠 Main Directory](../../README.md)
