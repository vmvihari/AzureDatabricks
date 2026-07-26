# Lesson 4.5: Integration Testing with Databricks Asset Bundles

## Table of Contents
- [The Problem with UI Deployments](#the-problem-with-ui-deployments)
- [Databricks Asset Bundles (DABs)](#databricks-asset-bundles-dabs)
- [Action Step: Validating the ETL Pipeline in Dev](#action-step-validating-the-etl-pipeline-in-dev)
- [Interview Preparation](#interview-preparation)

---

## The Problem with UI Deployments

You have written local `pytest` suites to validate your logic, and you have used Databricks Connect to verify your queries run successfully in the cloud. Are you ready to deploy your new DLT pipelines to production?

**No.** You have not tested the integration of the entire system. What if the Gold layer script fails because it doesn't have the correct Unity Catalog permissions to read the Silver layer table? What if the Workflow is misconfigured and the scripts run out of order? 

You need an isolated environment (a "Dev Workspace") to deploy and run the entire pipeline exactly as it would run in Production, without breaking the real Production tables.

---

## Databricks Asset Bundles (DABs)

Historically, deploying Databricks jobs involved writing complex custom Python scripts or CI/CD pipelines to zip up code and call REST APIs.

Databricks introduced **Databricks Asset Bundles (DABs)** to standardize this. DABs allow you to define your jobs, pipelines, and ML models in a declarative YAML file (`databricks.yml`). 

With the Databricks CLI installed, you can instantly deploy your code and jobs to a Dev workspace and run them from your local terminal.

---

## 🛠️ Action Step: Validating the ETL Pipeline in Dev

Let's assume your company has provided you with a "Dev" Databricks workspace. We will write a Bundle to deploy our Mortgage pipeline to Dev and run it for an integration test.

### Step 1: Create the Bundle Configuration
Navigate to the root of your `apps/mortgage-data-platform/` directory and create a file named `databricks.yml`.

```yaml
# apps/mortgage-data-platform/databricks.yml

bundle:
  name: mortgage-data-platform

workspace:
  host: <your_databricks_workspace_url>

resources:
  jobs:
    mortgage_daily_etl:
      name: "[DEV Integration Test] Mortgage Daily ETL"
      tasks:
        - task_key: "bronze_ingestion"
          spark_python_task:
            python_file: "src/dlt/autoloader_bronze.py"
          job_cluster_key: "dev_cluster"
        
        - task_key: "silver_cleansing"
          depends_on:
            - task_key: "bronze_ingestion"
          spark_python_task:
            python_file: "src/silver/cleansed_loans.py"
          job_cluster_key: "dev_cluster"
          
      job_clusters:
        - job_cluster_key: "dev_cluster"
          new_cluster:
            spark_version: "14.3.x-scala2.12"
            node_type_id: "Standard_DS3_v2"
            num_workers: 1
```

### Step 2: Deploy to the Dev Workspace
From your terminal, use the Databricks CLI to deploy the bundle:
```bash
databricks bundle deploy -t dev
```
*What happens here?* The CLI automatically syncs your local `src/` Python files into the Dev workspace and creates the Databricks Job as defined in the YAML.

### Step 3: Run the Integration Test
Trigger the job to run in Azure directly from your terminal:
```bash
databricks bundle run mortgage_daily_etl -t dev
```
You can now monitor the output in your terminal. If the pipeline runs from end-to-end without failing, you have successfully performed an integration test! You can now merge your pull request, allowing the CI/CD pipeline to deploy this exact same bundle to Production.

---

## 🎯 Interview Preparation

> [!TIP]
> **Q1: Explain how you perform end-to-end integration testing before deploying to production.**
> **Answer:** "I never test in production. I utilize Databricks Asset Bundles (DABs) to define my data pipelines as YAML. From my local machine or a CI pipeline, I run `databricks bundle deploy` targeting an isolated Development or Staging workspace. I then run `databricks bundle run` to execute the entire DAG. This validates that the Python code, the cluster configurations, and the Unity Catalog permissions all integrate flawlessly before the code ever reaches the Production workspace."

---
[⬅️ Previous: Lesson 4.4: CDC & SCDs](lesson-4.4-cdc-scd-type-1-2.md) | [🏠 Main Directory](../../README.md) | [➡️ Next: Lesson 4.6: Bonus - LakeFlow Connect](lesson-4.6-lakeflow-connect.md)
