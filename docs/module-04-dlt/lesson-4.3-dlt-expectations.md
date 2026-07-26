# Lesson 4.3: Data Quality with DLT Expectations

## Table of Contents
- [The Challenge of Data Quality](#the-challenge-of-data-quality)
- [DLT Expectations](#dlt-expectations)
- [Action Step: Adding Expectations to the Pipeline](#action-step-adding-expectations-to-the-pipeline)
- [Interview Preparation](#interview-preparation)

---

## The Challenge of Data Quality

In a standard PySpark pipeline, what happens if a CSV file arrives where the `loan_amount` is suddenly a negative number (`-500000`) due to a bug in the upstream application?

1. If you don't check for it, the bad data pollutes the Bronze, Silver, and Gold layers, destroying the accuracy of the BI dashboards.
2. If you explicitly write a PySpark `filter(col("loan_amount") > 0)` to drop the bad records, they vanish silently into the void. You have no idea how many records were dropped, which infuriates the data governance team.
3. If you write an explicit `assert` or exception block, the pipeline crashes entirely, waking you up at 3:00 AM for an on-call alert.

---

## DLT Expectations

Delta Live Tables solves this elegantly using **Expectations**. Expectations are data quality constraints defined directly on the table.

There are three types of expectations:
- `@dlt.expect(name, constraint)`: If a row fails, it still passes through to the table, but the failure is recorded in the DLT Event Log metrics.
- `@dlt.expect_or_drop(name, constraint)`: If a row fails, it is dropped from the dataset. The failure is recorded in the metrics. (This is the most common).
- `@dlt.expect_or_fail(name, constraint)`: If a row fails, the entire pipeline immediately halts and fails. Use this for absolutely critical constraints (e.g., if the `primary_key` is completely missing, something catastrophic has occurred).

The beauty of DLT is that the Databricks UI provides a visual dashboard showing exactly how many rows passed and failed each expectation on every run.

---

## 🛠️ Action Step: Adding Expectations to the Pipeline

Let's enforce strict data quality rules on our loan applications so bad data never even makes it into the Bronze layer.

1. Open `apps/mortgage-data-platform/src/dlt/dlt_loans_pipeline.py`.
2. Add the `@dlt.expect_or_drop` decorators directly above the `@dlt.table` decorator:

```python
import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

# (loan_schema defined here...)

@dlt.expect_or_drop("valid_ssn", "applicant_ssn IS NOT NULL")
@dlt.expect_or_drop("valid_loan_amount", "loan_amount > 0")
@dlt.table(
  name="bronze_loans",
  comment="Raw loan applications ingested via Auto Loader with strict quality checks."
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

Now, any row with a missing SSN or negative loan amount will be safely dropped, and the exact count of dropped rows will be visible in the DLT pipeline UI.

---

## 5. 🛠️ Action Step: Validation & Testing

To test DLT Expectations, the best approach is to visualize them in the Databricks Pipeline UI.

1. Create a local file named `bad_loan.csv` with the following content:
    ```csv
    loan_id,applicant_ssn,loan_amount,credit_score
    L-999,123-45-6789,-50000.0,750
    ```
2. Upload this `bad_loan.csv` file directly into your ADLS Gen2 `bronze` container under the `landing/loan_applications/` folder using **Azure Storage Explorer** or the **Azure Portal**.
3. Deploy and run your DLT pipeline using Databricks Asset Bundles (`databricks bundle run`).
4. Open the Delta Live Tables UI in your Databricks workspace.
5. Click on the `bronze_loans` table in the DAG.
6. In the right-hand panel under "Data Quality", verify that your expectation `valid_loan_amount` caught the bad row and dropped it, and that the UI successfully recorded the exact count of dropped records.

---

## 🎯 Interview Preparation

> [!TIP]
> **Q1: If a single bad row (e.g., a negative loan amount) enters your pipeline, how does Delta Live Tables provide a more resilient and observable alternative to standard PySpark?**
> **Answer:** "In standard PySpark, you either have to write a hard failure that crashes the entire job, or you silently filter out the bad records using `.where()`, which creates an observability black hole. DLT solves this via the `@dlt.expect_or_drop` constraint. It safely filters out the bad row without crashing the pipeline, while simultaneously logging the exact failure metrics into the DLT Event Log. This allows the governance team to monitor data quality degradation over time without requiring the engineering team to build custom auditing frameworks."

---
[⬅️ Previous: Lesson 4.2: Intro to DLT](lesson-4.2-intro-to-dlt.md) | [🏠 Main Directory](../../README.md) | [➡️ Next: Lesson 4.4: CDC & SCD](lesson-4.4-cdc-scd-type-1-2.md)
