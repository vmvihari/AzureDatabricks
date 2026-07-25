# Lesson 4.2: Introduction to Delta Live Tables (DLT)

## Table of Contents
- [Imperative vs. Declarative Engineering](#imperative-vs-declarative-engineering)
- [How DLT Works](#how-dlt-works)
- [Action Step: Building the DLT Pipeline](#action-step-building-the-dlt-pipeline)
- [Interview Preparation](#interview-preparation)

---

## Imperative vs. Declarative Engineering

In previous modules, we wrote **Imperative** PySpark code. We had to tell Spark *exactly* how to do everything:
- "Read from this path."
- "Write to this specific ADLS path."
- "Save the checkpoint in this specific `_checkpoints` directory."
- "Append to the table."

This requires a lot of boilerplate code and careful management. If someone deletes the checkpoint directory, the entire pipeline breaks.

**Delta Live Tables (DLT)** introduces **Declarative** pipeline engineering. 
With DLT, you simply write SQL or PySpark to define *what* the data should look like. Databricks automatically handles the *how*—it manages the checkpoints, the physical storage paths, the cluster infrastructure, and the lineage graph.

---

## How DLT Works

In DLT, you do not write `.load()` or `.save()`. Instead, you use the `@dlt.table` Python decorator above a function that returns a DataFrame. Databricks handles the rest.

If you are reading from an upstream DLT table, you simply use `dlt.read()`.

```python
import dlt

@dlt.table(
  name="silver_loans",
  comment="Cleansed mortgage applications."
)
def create_silver_loans():
    # Notice we don't specify ADLS paths. DLT manages the storage locations!
    return (
        dlt.read("bronze_loans")
        .dropDuplicates(["loan_id"])
    )
```

---

## 🛠️ Action Step: Building the DLT Pipeline

Let's convert our manual Auto Loader script from Lesson 4.1 into a declarative DLT pipeline.

1. Navigate to `apps/mortgage-data-platform/src/dlt/` and create `dlt_loans_pipeline.py`.
2. Import the `dlt` library and write the pipeline to ingest the Bronze data using Auto Loader inside a DLT table:

```python
import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

loan_schema = StructType([
    StructField("loan_id", StringType(), True),
    StructField("applicant_ssn", StringType(), True),
    StructField("loan_amount", DoubleType(), True),
    StructField("credit_score", IntegerType(), True)
])

@dlt.table(
  name="bronze_loans",
  comment="Raw loan applications ingested via Auto Loader."
)
def bronze_loans():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .schema(loan_schema)
        .load("abfss://bronze@stmortgagedata<your_initials>.dfs.core.windows.net/landing/loan_applications/")
    )
```
*(Notice how we entirely deleted the `.writeStream`, the `.checkpointLocation`, and the target ADLS path. DLT handles all of that for us behind the scenes).*

---

## 5. 🛠️ Action Step: Validation & Testing

As discussed in [Lesson 4.5: Asset Bundles](lesson-4.5-asset-bundles.md), you should test this pipeline using a Databricks Asset Bundle before deploying to production.

1. Open your `databricks.yml` file.
2. Add a DLT pipeline deployment target to your Dev environment configuration.

```yaml
# databricks.yml
bundle:
  name: mortgage-data-platform

resources:
  pipelines:
    mortgage_dlt_pipeline:
      name: mortgage_dlt_pipeline_${workspace.current_user.short_name}
      development: true
      libraries:
        - file: ./src/dlt/dlt_loans_pipeline.py

targets:
  dev:
    mode: development
    default: true
    workspace:
      host: https://adb-<workspace-id>.azuredatabricks.net
```

3. Run `databricks bundle deploy -t dev`.
4. Run `databricks bundle run mortgage_dlt_pipeline -t dev`.
5. Open the Databricks UI in your Dev workspace to visually verify the DAG executing successfully without corrupting production data.

---

## 6. 🎯 Interview Preparation

> [!TIP]
> **Q1: What is the primary architectural difference between a standard Databricks Workflow job and a Delta Live Tables pipeline?**
> **Answer:** "A standard PySpark Workflow job is imperative. The engineer is responsible for managing the physical storage paths, the checkpoint directories for streaming, the data lineage, and the error handling logic. Delta Live Tables is declarative. The engineer simply defines the transformations using `@dlt.table` decorators. Databricks automatically provisions the infrastructure, manages the checkpoints, visualizes the DAG (Directed Acyclic Graph) lineage in the UI, and handles retries. This vastly reduces boilerplate code and operational overhead."

---
[⬅️ Previous: Lesson 4.1: Auto Loader](lesson-4.1-autoloader-streaming.md) | [🏠 Main Directory](../../README.md) | [➡️ Next: Lesson 4.3: DLT Expectations](lesson-4.3-dlt-expectations.md)
