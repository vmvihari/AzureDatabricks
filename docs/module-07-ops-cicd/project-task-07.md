# Project Task 7: Operationalizing the Credit Bureau Feed

## Table of Contents
- [Objective](#objective)
- [The Challenge](#the-challenge)
- [Acceptance Criteria](#acceptance-criteria)

## Objective
This is the final challenge of the curriculum. You must take the independent Credit Bureau pipelines you've built across Modules 3, 4, 5, and 6, and deploy them to production using a Databricks Workflow and GitHub Actions.

## The Challenge

1. **Create the Workflow JSON:**
   - In `apps/mortgage-data-platform/deploy/`, create `credit_feed_workflow.json`.
   - Define a job named `credit_bureau_daily_etl`.
   - Configure a dedicated **Job Cluster**.
   - Create a Task to run the DLT Pipeline (`src/dlt/dlt_credit_scores.py`). *Note: In a real JSON definition, running a DLT pipeline uses a `pipeline_task` object rather than a `spark_python_task`.*
   - Create a dependent Task (`depends_on`) to run the Gold aggregation script (`src/gold/credit_tier_exposure.py`).
   - Add an `email_notifications` block to alert the Risk Team (`risk-alerts@mortgagecorp.com`) on failure.

2. **Update the CI/CD Pipeline:**
   - Open `.github/workflows/deploy.yml`.
   - Add a new step to the YAML file to deploy the `credit_feed_workflow.json` using the Databricks CLI.

## Acceptance Criteria
- [ ] `credit_feed_workflow.json` exists and correctly sequences the tasks on a Job Cluster.
- [ ] The Workflow contains an alerting mechanism for failures.
- [ ] The GitHub Actions `deploy.yml` file is updated to deploy both workflows.

---

# 🎉 CONGRATULATIONS! 🎉
You have completed the **Azure Databricks Knowledge Repository** curriculum!

You have successfully built the **Mortgage Data Platform** from the ground up, implementing Medallion architectures, Delta Live Tables, advanced performance tuning, Unity Catalog governance, and CI/CD pipelines.

You are now equipped with the practical skills and theoretical knowledge expected of a Senior Azure Databricks Data Engineer.

---

**[✅ View Solution](solution-07.md)**

---
[⬅️ Previous: Lesson 7.3: Git and CI/CD](lesson-7.3-git-and-cicd.md) | [🏠 Main Directory](../../README.md)
