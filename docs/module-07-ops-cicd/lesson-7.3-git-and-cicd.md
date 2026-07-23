# Lesson 7.3: Git Integration and CI/CD

## Table of Contents
- [The Death of the `.dbc` Archive](#the-death-of-the-dbc-archive)
- [Databricks Git Folders](#databricks-git-folders)
- [Continuous Deployment via GitHub Actions](#continuous-deployment-via-github-actions)
- [Action Step: Building the CI/CD Pipeline](#action-step-building-the-cicd-pipeline)
- [Interview Preparation](#interview-preparation)

---

## The Death of the `.dbc` Archive

In the early days of Databricks, moving code from Development to Production involved clicking "Export as `.dbc` archive" in one workspace, and clicking "Import" in the production workspace.

This is a massive anti-pattern. There is no version control, no peer review, and no automated testing.

---

## Databricks Git Folders

Modern Databricks environments utilize **Git Folders**. 
You connect your Databricks workspace directly to your GitHub/Azure DevOps repository. 

Data Engineers write code in isolated feature branches directly in the Databricks UI (or via VSCode). When finished, they create a Pull Request in GitHub. The code is peer-reviewed, and once approved, merged into the `main` branch.

---

## Continuous Deployment via GitHub Actions

Once code merges to `main`, a human should not be logging into the Production workspace to pull the latest changes or update the Workflow definitions.

We use **CI/CD** (Continuous Integration / Continuous Deployment). 
We can write a GitHub Actions script that:
1. Detects when code merges to `main`.
2. Authenticates to the Databricks Production workspace using an Azure Service Principal (via a secret token).
3. Uses the Databricks CLI to instruct the Production workspace to pull the latest code and deploy the Workflow JSON file.

---

## 🛠️ Action Step: Building the CI/CD Pipeline

Let's build a GitHub Actions pipeline to automatically deploy our Mortgage ETL Workflow.

1. Navigate to `apps/mortgage-data-platform/` and create the hidden GitHub directory: `.github/workflows/`.
2. Create a file named `deploy.yml`.
3. Write the YAML configuration.

```yaml
# apps/mortgage-data-platform/.github/workflows/deploy.yml
name: Deploy Databricks Workflows

on:
  push:
    branches:
      - main

jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
      - name: Checkout Code
        uses: actions/checkout@v3

      - name: Setup Databricks CLI
        uses: databricks/setup-cli@main

      - name: Deploy Mortgage Workflow
        env:
          DATABRICKS_HOST: ${{ secrets.DATABRICKS_HOST }}
          DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TOKEN }}
        run: |
          databricks jobs create --json-file apps/mortgage-data-platform/deploy/mortgage_workflow.json
```

---

## 🎯 Interview Preparation

> [!TIP]
> **Q1: Describe your ideal CI/CD deployment process for a PySpark ETL pipeline going from a Dev environment to Production.**
> **Answer:** "My ideal process relies entirely on Git and automation, completely removing manual UI interactions. Engineers develop code in isolated branches within a Databricks Dev workspace connected via Git Folders. Upon opening a PR, a CI pipeline (e.g., GitHub Actions) runs unit tests via `pytest`. After peer review and merging to `main`, the CD pipeline triggers. The CD pipeline authenticates to the Production Databricks workspace using a Service Principal, updates the Production Git Folder, and uses the Databricks CLI to deploy or update the Workflow JSON definitions."

---
[⬅️ Previous: Lesson 7.2: Monitoring and Alerting](lesson-7.2-monitoring-and-alerting.md) | [🏠 Main Directory](../../README.md) | [➡️ Next: Project Task 7](project-task-07.md)
