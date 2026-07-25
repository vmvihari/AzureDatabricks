# Lesson 3.5: Interactive Debugging with Databricks Connect

## Table of Contents
- [The Limitation of Local Testing](#the-limitation-of-local-testing)
- [What is Databricks Connect?](#what-is-databricks-connect)
- [The Curriculum Gap: Where Does the Data Come From?](#the-curriculum-gap-where-does-the-data-come-from)
- [Action Step: Running Your Pipeline Remotely for the First Time](#action-step-running-your-pipeline-remotely-for-the-first-time)
- [Action Step: Interactive Querying from VS Code](#action-step-interactive-querying-from-vs-code)
- [Interview Preparation](#interview-preparation)

---

## The Limitation of Local Testing

In Lessons 3.2–3.4, we used `pytest` to validate pure transformation logic on mock datasets. This is fast and correct for unit testing individual functions.

But what if you need to debug a script that reads a 50GB Parquet file stored in ADLS Gen2? What if your code needs to query Unity Catalog? Your laptop cannot handle 50GB of data efficiently, and it does not have the Azure network credentials or Managed Identity required to bypass the storage account firewall.

We need the power of the cloud, but the convenience of our local IDE.

---

## What is Databricks Connect?

**Databricks Connect** is a client library that acts as a bridge between your local IDE and a remote Databricks Cluster.

When you run a Python script on your laptop using Databricks Connect:
1. Standard Python code (like print statements) executes locally on your CPU.
2. The moment it encounters a Spark operation (like `spark.read.format("delta")` or `df.groupBy()`), it intercepts the command and sends it over the network to your Databricks cluster in Azure.
3. The Azure cluster executes the heavy lifting with full network access to ADLS Gen2.
4. The cluster sends only the final result (e.g., the aggregated rows) back to your VS Code terminal.

This allows developers to leverage VS Code breakpoints, local git, and autocomplete — while harnessing the compute power and network access of the cloud.

---

## The Curriculum Gap: Where Does the Data Come From?

> [!IMPORTANT]
> **This is a critical point.** Up until now, we have written all our pipeline scripts (`ingest_loans_bronze.py`, `cleansed_loans.py`, `fraud_flagging.py`, `state_risk_summary.py`) but we have **never run them on Databricks**. This means there is currently no data in ADLS Gen2, no Bronze Delta tables, and no Silver or Gold tables. If we tried to run `spark.sql("SELECT * FROM mortgage_prod.gold.state_risk_summary")`, it would immediately fail with `Table or view not found`.
>
> **This lesson solves that problem.** We will use Databricks Connect to execute our pipeline scripts against the remote cluster for the **first time**, which populates our Delta tables end-to-end. From that point forward, Databricks Connect becomes a tool for interactive exploration.

In a real enterprise, this first run would be triggered by a CI/CD pipeline (GitHub Actions) or a scheduled Databricks Job. We will cover that in **Module 7: Operations & CI/CD**. For now, running it once manually is the fastest way to get data flowing so we can continue building on top of it.

---

## 🛠️ Action Step: Running Your Pipeline Remotely for the First Time

### Step 1: Install Databricks Connect
In your local terminal, install the library. **The version must match your cluster's Databricks Runtime (DBR) version:**
```bash
pip install databricks-connect==14.3.0
```
> [!TIP]
> Check your cluster's DBR version in the Azure Databricks workspace under **Compute → Your Cluster → Configuration → Databricks Runtime Version**.

### Step 2: Configure Authentication
Authenticate your laptop with the workspace using an OAuth U2M profile via the Databricks CLI:
```bash
databricks configure
```
Enter your Databricks workspace host URL (e.g., `https://adb-XXXX.azuredatabricks.net`) and follow the browser-based login flow.

### Step 3: Run the Bronze Ingestion Script Remotely
The Bronze ingestion script reads CSV files from the ADLS landing zone. Ensure you have uploaded a sample CSV file (`apps/mortgage-data-platform/data/sample_loans.csv`) to the ADLS `bronze` container's `landing/loan_applications/` path first via the Azure Portal or Azure Storage Explorer.

Then, from your local terminal:
```bash
python apps/mortgage-data-platform/src/bronze/ingest_loans_bronze.py
```

Notice that the script file runs on your **local** machine, but because `DatabricksSession` (or the Delta-configured `SparkSession` targeting a remote cluster) is active, all Spark operations execute on Azure. You will see Spark logs streaming back to your terminal.

### Step 4: Run Silver and Gold Pipeline Scripts

Run each script in order to build up the full Medallion stack:
```bash
# Cleanse Bronze → Silver
python apps/mortgage-data-platform/src/silver/cleansed_loans.py

# Enrich Silver with Fraud Flags
python apps/mortgage-data-platform/src/silver/fraud_flagging.py

# Aggregate Silver → Gold
python apps/mortgage-data-platform/src/gold/state_risk_summary.py
```

After all four scripts complete successfully, your Medallion Architecture is **live** in Azure for the first time.

---

## 🛠️ Action Step: Interactive Querying from VS Code

Now that the Gold table is populated, we can use Databricks Connect for its most powerful feature: **interactive exploration directly from VS Code**, with full access to real production-scale data.

Create a new scratch file `apps/mortgage-data-platform/src/scratch/remote_query.py`:

```python
# apps/mortgage-data-platform/src/scratch/remote_query.py
from databricks.connect import DatabricksSession

# Initialize the remote session using credentials from `databricks configure`
spark = DatabricksSession.builder.getOrCreate()

print("Connecting to Azure Databricks...")

# This executes entirely in Azure against the real Gold Delta table
df = spark.sql("SELECT state, total_exposure, total_fraud_flags FROM mortgage_prod.gold.state_risk_summary ORDER BY total_exposure DESC LIMIT 10")

print("Top 10 states by loan exposure (retrieved from the cloud):")
df.show()
```

Run it locally:
```bash
python apps/mortgage-data-platform/src/scratch/remote_query.py
```

You will see real aggregated data printed in your local terminal — data that was computed by a cluster in Azure reading actual Delta tables in ADLS Gen2.

> [!NOTE]
> The `src/scratch/` directory is for **throwaway exploration scripts** — never for production code. Add it to `.gitignore` to prevent these files from being committed to your repository.

---

## 🎯 Interview Preparation

> [!TIP]
> **Q1: Explain the purpose of Databricks Connect and when you would use it.**
> **Answer:** "Databricks Connect bridges the gap between local IDEs and cloud compute. It allows me to write PySpark code in VS Code with breakpoints and autocomplete, while offloading Spark execution to a remote cluster that has full access to ADLS Gen2, Unity Catalog, and the necessary Managed Identity credentials. I use it during active development when I need to test against real, large datasets that my local machine cannot process, or when I'm debugging a production script that depends on cloud-specific resources like Key Vault secrets or private storage account endpoints."

> [!TIP]
> **Q2: Why can't you just always use Databricks Connect instead of local `pytest`?**
> **Answer:** "Databricks Connect requires a running cluster, which costs money and takes 3–5 minutes to spin up. A `pytest` unit test runs in seconds and is free. The professional practice is to use both: `pytest` with mock data for fast, cheap unit testing of transformation logic, and Databricks Connect only when you specifically need to validate against real data at scale or test a cloud dependency. Running a cluster for every test would be prohibitively expensive in an enterprise environment."

---
[⬅️ Previous: Lesson 3.4: Silver to Gold Aggregations](lesson-3.4-silver-to-gold-aggregations.md) | [🏠 Main Directory](../../README.md) | [➡️ Next: Project Task 3](project-task-03.md)
