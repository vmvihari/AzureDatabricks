# Lesson 8.2: Interactive Debugging with Databricks Connect

## Table of Contents
- [The Limitation of Local Testing](#the-limitation-of-local-testing)
- [What is Databricks Connect?](#what-is-databricks-connect)
- [Action Step: Querying ADLS from VS Code](#action-step-querying-adls-from-vs-code)
- [Interview Preparation](#interview-preparation)

---

## The Limitation of Local Testing

In Lesson 8.1, we used `pytest` to validate our pure transformation logic on a mock dataset of 2 rows. 

But what if you need to test against a 50GB Parquet file stored in ADLS Gen2? What if your code needs to query Unity Catalog to read from the `gold` schema? 

Your laptop cannot handle 50GB of data efficiently, and it does not have the Azure network credentials or Managed Identity required to bypass the storage account firewall. We need the power of the cloud, but we want the convenience of our local IDE (VS Code).

---

## What is Databricks Connect?

**Databricks Connect** is a client library that acts as a bridge between your local IDE and a remote Databricks Cluster.

When you run a Python script on your laptop using Databricks Connect:
1. The standard Python code (like print statements) executes locally on your CPU.
2. The moment it encounters a Spark operation (like `df.read.parquet()` or `df.groupBy()`), it intercepts the command and sends it over the internet to your Databricks cluster in Azure.
3. The Azure cluster executes the heavy lifting.
4. The cluster sends only the final result (e.g., the aggregated table) back to your VS Code terminal.

This allows developers to leverage features like VS Code breakpoints, local git, and autocomplete, while harnessing the compute power and network access of the cloud.

---

## 🛠️ Action Step: Querying ADLS from VS Code

Let's configure your local environment to connect to the cloud.

### Step 1: Install Databricks Connect
In your local terminal, install the library (ensure the version matches your cluster's Databricks Runtime version):
```bash
pip install databricks-connect==14.3.0
```

### Step 2: Configure Authentication
You must authenticate your laptop with the workspace. The most secure way is using an OAuth U2M (User-to-Machine) profile via the Databricks CLI:
```bash
databricks configure
```

### Step 3: Write the Connected Script
Create a new file in your local workspace: `apps/mortgage-data-platform/src/scratch/remote_query.py`.

Notice that we import `DatabricksSession` instead of a standard `SparkSession`. This tells the code to route execution to the cloud.

```python
# apps/mortgage-data-platform/src/scratch/remote_query.py

from databricks.connect import DatabricksSession

# Initialize the remote session. It will use the credentials from `databricks configure`.
spark = DatabricksSession.builder.getOrCreate()

print("Connecting to Azure Databricks...")

# This command executes in Azure! It queries Unity Catalog directly.
df = spark.sql("SELECT state, risk_score FROM mortgage_prod.gold.state_risk_summary LIMIT 5")

print("Data retrieved from the cloud:")
df.show()
```

### Validation
Run the file locally: `python apps/mortgage-data-platform/src/scratch/remote_query.py`.
You will see the output print in your local terminal, proving that your laptop successfully commanded the Azure cluster to read Unity Catalog!

---

## 🎯 Interview Preparation

> [!TIP]
> **Q1: Explain the purpose of Databricks Connect.**
> **Answer:** "Databricks Connect bridges the gap between local IDEs and cloud compute. It allows me to write code in VS Code, set local breakpoints, and use local Git, while offloading the actual Spark execution and data processing to a remote cluster. This is essential when I need to develop against real, massive datasets in ADLS Gen2 or Unity Catalog that my local machine cannot process or authenticate against."

---
[⬅️ Previous: Lesson 8.1: Local Pytest](lesson-8.1-local-pyspark-and-pytest.md) | [🏠 Main Directory](../../README.md) | [➡️ Next: Lesson 8.3: Asset Bundles](lesson-8.3-asset-bundles-and-integration.md)
