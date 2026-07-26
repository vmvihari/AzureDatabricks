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

## 🛠️ Action Step 1: Provisioning the Source Database

Before we can ingest data, we need a source database that actually exists! We will use the Azure SQL Database to act as our simulated "Mortgage Loan Origination System".

> [!WARNING]
> **Cost Warning:** Azure SQL Databases cost money. Please ensure you select the **Basic** tier (approx $5/month) or **Serverless** tier when deploying, and delete it when you are finished with the course.

### The Production Standard: Infrastructure as Code (Terraform)
In an enterprise, you don't click through the Azure Portal to create databases. You write Terraform.
Add the following to your `infrastructure/main.tf` (if you are maintaining one), or run `terraform apply` in a new directory:

```hcl
resource "azurerm_mssql_server" "sql_server" {
  name                         = "az-sql-mortgage-db"
  resource_group_name          = azurerm_resource_group.rg.name
  location                     = azurerm_resource_group.rg.location
  version                      = "12.0"
  administrator_login          = "sqladmin"
  administrator_login_password = "SuperSecretPassword123!"
}

resource "azurerm_mssql_database" "sql_db" {
  name      = "MortgageServicing"
  server_id = azurerm_mssql_server.sql_server.id
  sku_name  = "Basic"
}
```

### Seeding the Data
Once the database is online, it is empty. We will use the repository's native data generator to populate it with realistic CDC events.

1. Open your local terminal.
2. Navigate to `apps/data-generator/` and run `python generate_servicing.py --rows 500`. This creates a CSV of realistic CDC events.
3. Open a python shell or Jupyter notebook locally, and use `pandas` to push that CSV into your new Azure SQL Database:

```python
import pandas as pd
from sqlalchemy import create_engine

# Load the generated data
df = pd.read_csv("data/raw/servicing_events.csv")

# Create connection (replace with your actual password and server name)
engine = create_engine("mssql+pyodbc://sqladmin:SuperSecretPassword123!@az-sql-mortgage-db.database.windows.net/MortgageServicing?driver=ODBC+Driver+17+for+SQL+Server")

# Bulk insert into the Azure SQL Database
df.to_sql("LoanServicingEvents", con=engine, schema="dbo", if_exists="replace", index=False)
```

---

## 🛠️ Action Step 2: Ingesting Servicing Events
Let's build the extraction script for our operational database.

1. Navigate to `apps/mortgage-data-platform/src/bronze/` and create `ingest_servicing_bronze.py`.
2. Write PySpark code to securely fetch the database username and password from the `mortgage-secrets` scope.
3. Read from the `dbo.LoanServicingEvents` table using `spark.read.format("jdbc")`.
4. **Crucial:** Implement the `numPartitions` and boundary options to ensure this read is distributed across the Spark cluster and doesn't crash the source DB.
5. Write the result to ADLS as a Delta table.

```python
from src.utils.spark import get_spark_session
from pyspark.sql.functions import col

def extract_recent_events(df):
    """
    Filters out loan servicing events older than 2020.
    Importing this function has zero side effects — safe for pytest.
    """
    return df.filter(col("event_year") >= 2020)

if __name__ == "__main__":
    # Initialize Spark Session
    spark = get_spark_session("IngestServicingBronze")

    # In a Databricks environment, dbutils is available by default.
    # For local testing, we would mock this.
    try:
        db_user = dbutils.secrets.get(scope="mortgage-secrets", key="az-sql-user")
        db_pass = dbutils.secrets.get(scope="mortgage-secrets", key="az-sql-pass")
    except NameError:
        db_user = "mock_user"
        db_pass = "mock_pass"

    jdbc_url = "jdbc:sqlserver://az-sql-mortgage-db.database.windows.net:1433;database=MortgageServicing"

    # Read from JDBC
    df_servicing = (spark.read
        .format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", "dbo.LoanServicingEvents")
        .option("user", db_user)
        .option("password", db_pass)
        .option("partitionColumn", "event_id")
        .option("lowerBound", "1")
        .option("upperBound", "10000000")
        .option("numPartitions", "10")
        .load())
        
    # Apply filtering logic
    df_recent_servicing = extract_recent_events(df_servicing)

    # Write to Bronze Delta Table
    (df_recent_servicing.write
        .format("delta")
        .mode("append")
        .save("abfss://bronze@stmortgagedata<your_initials>.dfs.core.windows.net/tables/bronze_servicing_events"))
```

---

## 🛠️ Action Step 3: Local Unit Testing for JDBC

Because your local laptop cannot easily connect to the secured Azure SQL Server (and shouldn't during a unit test!), we must test our JDBC logic by *mocking* the DataFrameReader.

1. Create a new file `tests/unit/test_ingest_jdbc.py`.
2. Add the following code to simulate the database read and ensure your Spark SQL logic functions correctly:

```python
import pytest

# Import the actual logic from our script
from src.bronze.ingest_servicing_bronze import extract_recent_events

def test_jdbc_date_filtering(spark):
    # 1. Arrange: Create mock data using Spark SQL to bypass Python worker socket issues on Windows
    df_mock_jdbc = spark.sql("""
        SELECT 1 as event_id, 2019 as event_year UNION ALL
        SELECT 2 as event_id, 2021 as event_year
    """)
    
    # 2. Act: Run our extraction logic on the mock DB data
    df_result = extract_recent_events(df_mock_jdbc)
    
    # 3. Assert: Verify the old event was filtered out
    assert df_result.count() == 1
    assert df_result.first()["event_id"] == 2
```

3. Run the test:
```bash
pytest tests/unit/test_ingest_jdbc.py
```

*(Note: We will cover integration testing—actually connecting to the database—using Databricks Connect in **Lesson 3.5**).*

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
