# Project Task 5: Solution

Here is the best-practice PySpark solution for optimizing the Credit Scores table using Liquid Clustering.

## The Optimization Script

Create this script at `apps/mortgage-data-platform/src/optimization/cluster_credit_scores.py`.

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Optimize Credit Scores").getOrCreate()

# 1. Define the path to the Silver Credit Scores table
silver_scores_path = "abfss://silver@stmortgagedata<your_initials>.dfs.core.windows.net/tables/silver_credit_scores"

# 2. Enable Liquid Clustering on the table
# Since we frequently join this table on 'ssn' with the loans table, 'ssn' is the optimal clustering key.
print(f"Enabling Liquid Clustering on {silver_scores_path} using key: 'ssn'...")
spark.sql(f"ALTER TABLE delta.`{silver_scores_path}` CLUSTER BY (ssn)")

# 3. Trigger the OPTIMIZE command to rewrite the data using the new clustering key
print("Running OPTIMIZE to cluster the data...")
spark.sql(f"OPTIMIZE delta.`{silver_scores_path}`")

# 4. Trigger the VACUUM command to remove the old unclustered data files (with a 0 retention override for immediate cleanup in this learning environment)
print("Running VACUUM to remove old unclustered files...")
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
spark.sql(f"VACUUM delta.`{silver_scores_path}` RETAIN 0 HOURS")

print("Optimization complete!")
```

## Explanation
- **Clustering Key Selection:** Liquid Clustering dynamically groups data by the specified columns. We chose `ssn` because it is the primary key used to join the `silver_credit_scores` table to the `silver_loans` table in the Gold pipeline.
- **OPTIMIZE:** Simply running `ALTER TABLE` only tells Databricks how future data should be clustered. We must run `OPTIMIZE` to actually rewrite the existing data files into the new clustered layout.
- **VACUUM:** When `OPTIMIZE` rewrites the data, the old files still exist for time travel. Running `VACUUM` immediately reclaims that storage space (in production, we leave the retention at the default 7 days, but set it to 0 here for the exercise).

---
[⬅️ Back to Project Task 5](project-task-05.md) | [🏠 Main Directory](../../README.md)
