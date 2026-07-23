# Lesson 2.3: Ingesting Data via JDBC

## Table of Contents
- [Connecting to Relational Databases](#connecting-to-relational-databases)
- [Zero Trust: Utilizing Databricks Secret Scopes](#zero-trust-utilizing-databricks-secret-scopes)
- [Reading a Table](#reading-a-table)
- [Performance Optimization: Partitioning JDBC Reads](#performance-optimization-partitioning-jdbc-reads)
- [Interview Preparation](#interview-preparation)

---

## Connecting to Relational Databases

Many enterprise systems (like Mortgage Loan Origination Systems) store operational data in relational databases (e.g., Azure SQL, Postgres, Oracle). To ingest this data into our Data Lakehouse, PySpark utilizes the JDBC (Java Database Connectivity) API.

You can instruct Spark to query a specific table or execute a custom SQL query directly against the source database.

---

## Zero Trust: Utilizing Databricks Secret Scopes

Connecting to a database requires a username and password. **Never hardcode credentials in your notebooks or source code.** 

In a production environment, Databricks integrates with Azure Key Vault using **Secret Scopes**. This allows you to retrieve credentials dynamically at runtime.

```python
# Best Practice: Fetch credentials from the 'mortgage-secrets' scope
db_user = dbutils.secrets.get(scope="mortgage-secrets", key="sql-db-username")
db_pass = dbutils.secrets.get(scope="mortgage-secrets", key="sql-db-password")
jdbc_url = "jdbc:sqlserver://mortgage-sql-server.database.windows.net:1433;database=MortgageDB"
```

---

## Reading a Table

Once credentials are secured, you use the PySpark DataFrameReader with `.format("jdbc")`:

```python
df_servicing = (spark.read
                .format("jdbc")
                .option("url", jdbc_url)
                .option("dbtable", "dbo.LoanServicingEvents")
                .option("user", db_user)
                .option("password", db_pass)
                .load())
```

---

## Performance Optimization: Partitioning JDBC Reads

By default, PySpark will use exactly **one** executor to run a `SELECT * FROM dbo.LoanServicingEvents` query against the database. If the table has 100 million rows, this will:
1. Choke the Spark driver/executor with a massive OOM (Out Of Memory) error.
2. Put enormous strain on the source Azure SQL database, potentially crashing it for operational users.

To fix this, you must configure Spark to read the data in parallel partitions. You define a numeric column (like `event_id`), specify bounds, and tell Spark how many parallel connections to make.

```python
df_servicing_optimized = (spark.read
    .format("jdbc")
    .option("url", jdbc_url)
    .option("dbtable", "dbo.LoanServicingEvents")
    .option("user", db_user)
    .option("password", db_pass)
    .option("partitionColumn", "event_id")  # A numeric, indexed column
    .option("lowerBound", "1")
    .option("upperBound", "10000000")
    .option("numPartitions", "10")          # Max number of concurrent connections
    .load())
```
This tells Spark to open 10 concurrent connections to the database, each querying a specific slice of the `event_id` column (e.g., IDs 1 to 1M, 1M to 2M, etc.).

---

## 🛠️ Action Step: Ingesting Servicing Events
Let's build the extraction script for our operational database.

1. Navigate to `apps/mortgage-data-platform/src/bronze/` and create `ingest_servicing_bronze.py`.
2. Write PySpark code to securely fetch the database username and password from the `mortgage-secrets` scope.
3. Read from the `dbo.LoanServicingEvents` table using `spark.read.format("jdbc")`.
4. **Crucial:** Implement the `numPartitions` and boundary options to ensure this read is distributed across the Spark cluster and doesn't crash the source DB.
5. Write the result to ADLS as a Delta table.

---

## 🎯 Interview Preparation

> [!TIP]
> **Q1: How do you prevent a Spark JDBC read from crashing the source transactional database?**
> **Answer:** "A naive PySpark JDBC read uses a single connection, which can lock tables and severely impact the performance of the source database. To prevent this, I explicitly configure the `numPartitions`, `partitionColumn`, `lowerBound`, and `upperBound` options. This distributes the read across multiple Spark executors. However, I carefully tune `numPartitions` in coordination with the database administrator to ensure I don't overwhelm the database's connection pool, finding a balance between Spark parallelism and database health."

> [!TIP]
> **Q2: How do you manage database credentials in your PySpark code?**
> **Answer:** "I adhere strictly to Zero Trust principles. I never hardcode usernames, passwords, or API tokens in the repository. Instead, I configure Databricks to connect to Azure Key Vault using a Secret Scope. In my code, I dynamically retrieve the credentials at runtime using `dbutils.secrets.get()`. This ensures credentials are obfuscated from developers and are automatically rotated via Azure policies without requiring code changes."

---
[⬅️ Previous: Lesson 2.2: Reading Files from ADLS](lesson-2.2-ingest-files.md) | [🏠 Main Directory](../../README.md) | [➡️ Next: Lesson 2.4: Ingesting HTTP APIs](lesson-2.4-ingest-http-fraud-api.md)
