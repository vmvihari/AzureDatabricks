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
loan_apps_path = "abfss://bronze@stmortgagedataprod.dfs.core.windows.net/landing/loan_applications/"
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
              .load("abfss://bronze@stmortgagedataprod.dfs.core.windows.net/landing/some_parquet_data/"))
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
3. In this file, write the PySpark code to read the CSV files from our ADLS `bronze` container (`abfss://bronze@stmortgagedataprod.dfs.core.windows.net/landing/loan_applications/`).
4. **Requirement:** Do NOT use `inferSchema=True`. You must define the `StructType` explicitly based on the CSV structure.
5. Write the dataframe back out to the ADLS bronze container as a **Delta table** using:
   `.write.format("delta").mode("append").save("abfss://bronze@.../tables/bronze_loans")`

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
