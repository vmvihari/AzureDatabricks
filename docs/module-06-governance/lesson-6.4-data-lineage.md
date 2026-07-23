# Lesson 6.4: Automated Data Lineage

## Table of Contents
- [The Importance of Data Lineage](#the-importance-of-data-lineage)
- [How Unity Catalog Tracks Lineage](#how-unity-catalog-tracks-lineage)
- [Action Step: Exploring Lineage in the UI](#action-step-exploring-lineage-in-the-ui)
- [Interview Preparation](#interview-preparation)

---

## The Importance of Data Lineage

Imagine a scenario where the source Mortgage application suddenly stops collecting the `applicant_income` column. 

Before you drop that column from the Bronze ingestion layer, you must perform **Impact Analysis**. You need to know:
1. Does the `silver_loans` table use this column?
2. Which Gold dashboards depend on it?
3. Which Machine Learning models will break if this column disappears?

Historically, answering these questions required manually digging through thousands of lines of Python and SQL code.

---

## How Unity Catalog Tracks Lineage

Unity Catalog completely eliminates manual impact analysis. 

Because UC controls the namespace, every time a Databricks cluster executes a query (whether it is standard PySpark, Spark SQL, or Delta Live Tables), Unity Catalog analyzes the execution plan and automatically records the **Lineage**.

It tracks:
- **Table Lineage:** "Table C was created by joining Table A and Table B."
- **Column Lineage:** "The `average_income` column in Table C was derived from the `applicant_income` column in Table A."

---

## 🛠️ Action Step: Exploring Lineage in the UI

You don't write code for Data Lineage—it happens automatically! Let's view the lineage of the pipelines we built in Modules 3 and 4.

1. Open the Databricks Workspace UI.
2. On the left sidebar, click **Catalog** (Data Explorer).
3. Navigate through the 3-level namespace: `mortgage_prod` -> `gold` -> `state_risk_summary`.
4. Click on the **Lineage** tab.
5. Click **See Lineage Graph**.

You will see a visual, interactive DAG (Directed Acyclic Graph) showing that `gold.state_risk_summary` depends on `silver.loans`, which in turn depends on `bronze.loans`. You can click on the specific `total_exposure` column to trace it all the way back to the `loan_amount` column in the Bronze layer.

---

## 🎯 Interview Preparation

> [!TIP]
> **Q1: If the business wants to drop the `applicant_income` column from the Bronze layer, how do you determine which downstream dashboards will break?**
> **Answer:** "I do not need to manually parse through the repository's source code. Because we use Unity Catalog, data lineage is automatically captured at both the table and column level during query execution. I would navigate to the Data Explorer UI, select the Bronze table, and view the Column Lineage graph for `applicant_income`. The graph will visually trace every downstream Silver table, Gold table, and connected BI Dashboard that relies on that specific column, allowing me to perform a complete impact analysis in seconds."

---
[⬅️ Previous: Lesson 6.3: Dynamic Data Masking](lesson-6.3-dynamic-data-masking.md) | [🏠 Main Directory](../../README.md) | [➡️ Next: Project Task 6](project-task-06.md)
