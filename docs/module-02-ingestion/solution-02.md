# Project Task 2: Solution

Here is the best-practice PySpark solution for ingesting raw Credit Score CSV files into the Bronze Medallion layer.

## The PySpark Script

Create this script at `apps/mortgage-data-platform/src/bronze/ingest_credit_scores.py`.

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# 1. Initialize the Spark Session
spark = SparkSession.builder.appName("Ingest Bronze Credit Scores").getOrCreate()

# 2. Define strict schema for ingestion (Schema on Read)
credit_schema = StructType([
    StructField("loan_id", StringType(), True),
    StructField("ssn", StringType(), True),
    StructField("credit_score", IntegerType(), True),
    StructField("report_date", StringType(), True)
])

# 3. Define paths
landing_path = "abfss://bronze@stmortgagedata<your_initials>.dfs.core.windows.net/landing/credit_scores/"
bronze_table_path = "abfss://bronze@stmortgagedata<your_initials>.dfs.core.windows.net/tables/bronze_credit_scores"

# 4. Read the CSV data
raw_df = (
    spark.read
    .format("csv")
    .option("header", "true")
    .schema(credit_schema)
    .load(landing_path)
)

# 5. Append audit metadata columns
enriched_df = (
    raw_df
    .withColumn("_ingestion_timestamp", current_timestamp())
    .withColumn("_source_file", input_file_name())
)

# 6. Write to the Bronze Table in Delta format
(
    enriched_df.write
    .format("delta")
    .mode("append")
    .save(bronze_table_path)
)

print(f"Successfully ingested credit scores to {bronze_table_path}")
```

## Explanation
- **Schema on Read:** We explicitly enforce the `StructType` rather than using `inferSchema`, preventing unexpected data types from corrupting the Bronze layer.
- **Audit Columns:** We inject `_ingestion_timestamp` and `_source_file`. If a bad batch of data lands in the table, we can easily query exactly which file it came from and when it was ingested to debug the issue.
- **Append Mode:** Because this is raw ingestion, we never use `overwrite`. We append daily drops to the historical Delta table.

---
[⬅️ Back to Project Task 2](project-task-02.md) | [🏠 Main Directory](../../README.md)
