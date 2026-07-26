# Lesson 2.2: Reading Files from ADLS Gen2

## Table of Contents
- [The PySpark DataFrameReader](#the-pyspark-dataframereader)
- [Connecting to Azure Data Lake Storage](#connecting-to-azure-data-lake-storage)
- [Reading CSV vs. Parquet](#reading-csv-vs-parquet)
- [The Dangers of `inferSchema`](#the-dangers-of-inferschema)
- [Interview Preparation](#interview-preparation)

---

## The PySpark DataFrameReader
The core mechanism for reading data into Spark is the `DataFrameReader`, accessed via `spark.read`. 
The basic syntax follows this pattern:
```python
df = (spark.read
      .format("<file_format>")
      .option("<key>", "<value>")
      .load("<path_to_data>"))
```

---

## Connecting to Azure Data Lake Storage
In enterprise Databricks environments, you **never** read files from local storage (`file:///`). Instead, data resides in cloud object storage, such as Azure Data Lake Storage (ADLS) Gen2.

Databricks connects to ADLS securely using Managed Identities or Service Principals. Once authorized, you read data using the **Azure Blob File System (ABFS)** URI scheme:
`abfss://<container_name>@<storage_account_name>.dfs.core.windows.net/<directory_path>`

*Example for our Mortgage Platform:*
```python
# Production-first path referencing our 'bronze' container
loan_apps_path = "abfss://bronze@stmortgagedata<your_initials>.dfs.core.windows.net/landing/loan_applications/"
```

---

## Reading CSV vs. Parquet

### Reading CSV Files
CSV files are human-readable but highly inefficient for distributed processing because they do not contain embedded schema metadata and are not strictly columnar.
```python
df_loans = (spark.read
            .format("csv")
            .option("header", "true")
            .load(loan_apps_path))
```

### Reading Parquet Files
Parquet is the gold standard for Big Data. It is a highly compressed, columnar format that embeds the schema directly into the file.
```python
df_parquet = (spark.read
              .format("parquet")
              .load("abfss://bronze@stmortgagedata<your_initials>.dfs.core.windows.net/landing/some_parquet_data/"))
```

---

## The Dangers of `inferSchema`

When reading CSVs, you can set `.option("inferSchema", "true")`. This tells Spark to guess the data types of each column (e.g., Integer, String, Date).

**Do not use this in production.**
1. **Performance Cost:** To infer the schema, Spark must read the *entire* dataset first, drastically increasing ingestion time.
2. **Fragility:** If tomorrow's CSV file happens to have all integers in a column that is usually decimals, Spark will infer Integer. If downstream tables expect Decimals, the pipeline will crash.

Instead, always explicitly define the schema using `pyspark.sql.types.StructType`:

```python
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

loan_schema = StructType([
    StructField("loan_id", StringType(), True),
    StructField("applicant_ssn", StringType(), True),
    StructField("loan_amount", DoubleType(), True),
    StructField("credit_score", IntegerType(), True)
])

df_loans = (spark.read
            .format("csv")
            .option("header", "true")
            .schema(loan_schema)
            .load(loan_apps_path))
```

---

## 🛠️ Action Step: Ingesting Loan Applications
Now let's write our first actual pipeline script.

1. Navigate to the `apps/mortgage-data-platform/src/bronze/` directory.
2. Create a new file named `ingest_loans_bronze.py`.
3. In this file, write the PySpark code to read the CSV files from our ADLS `bronze` container (`abfss://bronze@stmortgagedata<your_initials>.dfs.core.windows.net/landing/loan_applications/`).
4. **Requirement:** Do NOT use `inferSchema=True`. You must define the `StructType` explicitly based on the CSV structure.
5. Write the dataframe back out to the ADLS bronze container as a **Delta table** using:
   `.write.format("delta").mode("append").save("abfss://bronze@.../tables/bronze_loans")`

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

def get_loan_schema():
    """
    Returns the strict schema for loan applications.
    Importing this function has zero side effects — safe for pytest.
    """
    return StructType([
        StructField("loan_id", StringType(), True),
        StructField("applicant_ssn", StringType(), True),
        StructField("loan_amount", DoubleType(), True),
        StructField("credit_score", IntegerType(), True)
    ])

if __name__ == "__main__":
    # Initialize Spark Session (Databricks runtime provides `spark` by default, but this is best practice)
    spark = SparkSession.builder.appName("IngestLoansBronze").getOrCreate()

    # 1. Define strict schema
    loan_schema = get_loan_schema()

    # 2. Read from Landing Zone
    df_loans = (spark.read
                .format("csv")
                .option("header", "true")
                .schema(loan_schema)
                .load("abfss://bronze@stmortgagedata<your_initials>.dfs.core.windows.net/landing/loan_applications/"))

    # 3. Write to Bronze Delta Table
    (df_loans.write
        .format("delta")
        .mode("append")
        .save("abfss://bronze@stmortgagedata<your_initials>.dfs.core.windows.net/tables/bronze_loans"))
```

---

## 🛠️ Action Step: Local Unit Testing with Pytest

In real-world Data Engineering, you **never** write a pipeline script without testing it. Waiting 5 minutes for a Databricks cluster to spin up just to see if your schema is defined correctly is a waste of time. Instead, Senior Data Engineers use `pytest` and a local in-memory Spark session to test their code instantly on their laptops.

Let's test our `ingest_loans_bronze.py` logic.

### 1. Install Testing Libraries
Open your local terminal and install the PySpark and Pytest libraries:
```bash
pip install pyspark pytest
```

### 2. Configure Pytest Module Discovery (Do This Once)

Before you can import from your `src` directory in a test, Python needs two things in place. **Do this once at the start of the project — you will not need to repeat it for future lessons.**

**Step A:** Create a `conftest.py` at the root of `apps/mortgage-data-platform/`. This tells `pytest` to add the project root to Python's module search path:

```python
# apps/mortgage-data-platform/conftest.py
import sys
import os

# Add the project root to sys.path so that `from src.X.Y import Z` imports work
sys.path.insert(0, os.path.dirname(__file__))
```

**Step B:** Create empty `__init__.py` files to make each `src` subdirectory a proper Python package. Without these, Python does not recognize them as importable modules:

```text
apps/mortgage-data-platform/
├── conftest.py          ← created in Step A
└── src/
    ├── __init__.py      ← create this
    └── bronze/
        └── __init__.py  ← create this
```

Each `__init__.py` can simply contain a single comment:
```python
# package marker
```

> [!NOTE]
> **Why is this needed?** In Python, a directory is only treated as a "package" (importable with dot notation like `src.bronze.ingest_loans_bronze`) if it contains an `__init__.py` file. Without it, `from src.bronze... import ...` will always fail with `ModuleNotFoundError: No module named 'src'`, even if the file physically exists.

### 3. Write the Test
In order to test our script without actually hitting the ADLS cloud storage, we will refactor our script's logic into a function that takes a DataFrame, and then we will feed it a "mock" DataFrame.

1. Create a `tests/unit/` directory at the root of `apps/mortgage-data-platform/`.
2. Inside `tests/unit/`, create `test_ingest_loans_bronze.py`.
3. Add the following code:

```python
import os
import sys
import tempfile
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType

# Import the actual logic from our script
from src.bronze.ingest_loans_bronze import get_loan_schema

# Fix for Windows: Ensure Spark uses the current Python executable
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
os.environ['_JAVA_OPTIONS'] = "-Djava.net.preferIPv4Stack=true"

@pytest.fixture(scope="session")
def spark():
    """Spins up a lightning-fast, local-only Spark session in your laptop's RAM."""
    return SparkSession.builder.master("local[1]").appName("LocalTest").getOrCreate()

def test_bronze_schema_enforcement(spark):
    # 1. Arrange: Import the EXACT schema we built in our ingestion script
    loan_schema = get_loan_schema()
    
    # Create a mock CSV file (Bypasses PySpark RDD socket issues on Windows)
    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix=".csv") as f:
        f.write("loan_id,applicant_ssn,loan_amount,credit_score\n")
        f.write("L-100,123-45-6789,250000.0,720\n")
        temp_path = f.name
        
    try:
        # 2. Act: Apply the schema to the mock data using native CSV reader
        df = spark.read.option("header", "true").schema(loan_schema).csv(temp_path)
        
        # 3. Assert: Verify the types were cast correctly
        assert df.schema["loan_amount"].dataType == DoubleType()
        assert df.first()["credit_score"] == 720
    finally:
        os.remove(temp_path)
```

### 3. Run the Test
Run the test from your terminal:
```bash
pytest tests/unit/test_ingest_loans_bronze.py
```
You should see a green dot `.` indicating your test passed instantly, proving your schema logic is sound without ever touching Azure!

---

## 🚀 Action Step: Deploy & Verify — First Real Data in the Pipeline

Your local tests pass. Now it's time to run this script against the real Azure infrastructure and populate the Bronze Delta table for the first time.

> [!IMPORTANT]
> **This is the first actual code deployment of the course.** Up until now we have only verified infrastructure (Lesson 1.4). This step produces real data in ADLS Gen2.

### Step 1: Install Databricks Connect (One-Time Setup)
Databricks Connect allows you to run PySpark code locally while all Spark operations execute on your Azure Databricks cluster.

```bash
# Install — version must match your cluster's Databricks Runtime version
pip install databricks-connect==14.3.0

# Authenticate (browser-based OAuth login)
databricks configure
```
Enter your workspace URL (e.g., `https://adb-XXXX.azuredatabricks.net`) and follow the browser login flow.

> [!TIP]
> Check your cluster's DBR version in the Databricks workspace under **Compute → Your Cluster → Configuration → Databricks Runtime Version**.

### Step 2: Run the Ingestion Script Against the Cloud

```bash
python apps/mortgage-data-platform/src/bronze/ingest_loans_bronze.py
```

What happens:
- Python code executes **locally** on your machine
- The moment Spark reads `abfss://bronze@...` (the ADLS landing zone), the operation is **sent to your Azure cluster**
- The cluster reads `sample_loans.csv` (uploaded in Lesson 1.4), applies your schema, and writes a Bronze Delta table back to ADLS
- You will see Spark logs streaming back to your local terminal

### Step 3: Verify in Databricks

Open a Databricks notebook and confirm the Bronze Delta table was created:

```python
# Run in a Databricks notebook to verify
files = dbutils.fs.ls("abfss://bronze@stmortgagedata<your_initials>.dfs.core.windows.net/tables/bronze_loans/")
display(files)

# Read the Delta table and display a sample
df = spark.read.format("delta").load("abfss://bronze@stmortgagedata<your_initials>.dfs.core.windows.net/tables/bronze_loans/")
df.show()
```

**Expected output:** 5 rows from `sample_loans.csv` with your enforced schema — `loan_id`, `applicant_ssn`, `loan_amount`, `credit_score`, `state`, `status`.

✅ **Bronze is live.** The Medallion Architecture has real data flowing through it for the first time.

---

## 🎯 Interview Preparation

> [!TIP]
> **Q1: Why is Parquet preferred over CSV in a Data Lakehouse?**
> **Answer:** "Parquet is a columnar storage format, which is vastly superior to row-based CSVs for analytics. If a BI Analyst queries the average `loan_amount`, Parquet allows Spark to scan *only* the `loan_amount` column, completely skipping all other columns. This drastically reduces I/O. Furthermore, Parquet files are highly compressed and embed their own schema, eliminating the need to define a `StructType` upon reading."

> [!TIP]
> **Q2: Why do you avoid using `inferSchema=True` when reading CSVs in production pipelines?**
> **Answer:** "Using `inferSchema=True` requires Spark to perform a full extra pass over the data to determine the data types, doubling the I/O cost of ingestion. More importantly, it creates fragile pipelines. If an upstream system accidentally drops a few decimal points in a float column for one day's file, Spark might infer the column as an Integer, which will cause a type-mismatch failure when attempting to write to our strict Silver Delta Tables. I always explicitly define schemas using `StructType` to guarantee predictability."

---
[⬅️ Previous: Lesson 2.1: Spark Architecture](lesson-2.1-spark-architecture.md) | [🏠 Main Directory](../../README.md) | [➡️ Next: Lesson 2.3: Ingesting JDBC](lesson-2.3-ingest-jdbc.md)
