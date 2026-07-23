# Lesson 8.1: Local Unit Testing with Pytest

## Table of Contents
- [Why Local Testing Matters](#why-local-testing-matters)
- [Mocking DataFrames in Memory](#mocking-dataframes-in-memory)
- [Action Step: Writing a Pytest Suite for the Silver Layer](#action-step-writing-a-pytest-suite-for-the-silver-layer)
- [Interview Preparation](#interview-preparation)

---

## Why Local Testing Matters

Waiting for a Databricks cluster to spin up (which can take 3-5 minutes) just to test if a simple `filter` condition works is a massive drain on developer productivity. 

Senior Data Engineers write their transformation logic as pure Python functions and test them **locally** on their laptops using an in-memory Spark session. This provides instant feedback (milliseconds) without incurring any cloud costs.

---

## Mocking DataFrames in Memory

To test locally, we use the `pytest` framework. 
1. We install the open-source `pyspark` library to our local machine (`pip install pyspark`).
2. We spin up a local Spark session (`SparkSession.builder.master("local[*]").getOrCreate()`). 
   - *Note on `local[*]`*: In production, Spark distributes work across dozens of cloud VMs. Using `master("local[*]")` tells Spark to run a tiny, single-node version of itself entirely inside your laptop's RAM (memory), utilizing all available local CPU cores. It boots instantly and costs nothing.
3. We create small, fake dataframes ("mock data") that represent the edge cases we want to test.
4. We pass the mock dataframe to our transformation function and assert that the output matches our expectations.

---

## 🛠️ Action Step: Writing a Pytest Suite for the Silver Layer

In Module 3, we built the Silver layer which drops rows where `loan_id` is null. Let's write a unit test to prove this logic works *before* we deploy it to the cloud.

1. Navigate to `apps/mortgage-data-platform/` and create a `tests/` directory.
2. Inside `tests/`, create a file named `test_cleansed_loans.py`.
3. Add the following code to test the transformation locally.

```python
# apps/mortgage-data-platform/tests/test_cleansed_loans.py

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import Row

# Note: In a real project, you would import the function from your src/ code.
# For this example, we define the transformation function directly here.
def cleanse_loans(df):
    """Drops rows with null loan_ids."""
    return df.dropna(subset=["loan_id"])

@pytest.fixture(scope="session")
def spark():
    """Provides a local Spark session for testing."""
    return SparkSession.builder.master("local[1]").appName("LocalTest").getOrCreate()

def test_cleanse_loans_drops_nulls(spark):
    # 1. Arrange: Create mock data with one valid row and one invalid (null) row.
    mock_data = [
        Row(loan_id="L-100", amount=250000),
        Row(loan_id=None, amount=300000)
    ]
    df_in = spark.createDataFrame(mock_data)

    # 2. Act: Run the transformation
    df_out = cleanse_loans(df_in)

    # 3. Assert: Verify only 1 row remains and it is the valid one
    assert df_out.count() == 1
    assert df_out.first()["loan_id"] == "L-100"
```

### Validating Your Work
To validate this locally on your laptop:
1. Ensure you have `pytest` and `pyspark` installed in your local Python environment (`pip install pytest pyspark`).
2. Run the tests from your terminal:
   ```bash
   pytest tests/test_cleansed_loans.py
   ```
3. You should see a green dot indicating the test passed, proving your logic is sound without ever touching Azure.

---

## 🎯 Interview Preparation

> [!TIP]
> **Q1: How do you accelerate the development feedback loop when building PySpark pipelines?**
> **Answer:** "I separate my I/O (reading/writing to ADLS) from my business logic. By writing business logic as pure Python functions that accept and return DataFrames, I can write local `pytest` suites using `pyspark` in local mode. This allows me to test edge cases instantly on my laptop using mock dataframes, avoiding the 5-minute cluster boot time and providing immediate feedback before I deploy to Databricks."

---
[⬅️ Previous: Module 7 Overview](../README.md) | [🏠 Main Directory](../../README.md) | [➡️ Next: Lesson 8.2: Databricks Connect](lesson-8.2-databricks-connect.md)
