import dlt
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()


# 1. Ingest the raw CDC stream into Bronze using Auto Loader
@dlt.table(name="bronze_servicing_events")
def bronze_servicing_events():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .load(
            "abfss://bronze@stmortgagedata<your_initials>.dfs.core.windows.net/landing/servicing_cdc/"
        )
    )


# 2. Define the empty target Silver table (required for apply_changes)
dlt.create_streaming_table("silver_current_loan_balances")

# 3. Apply the CDC changes using SCD Type 1 (Overwrite with latest state)
dlt.apply_changes(
    target="silver_current_loan_balances",
    source="bronze_servicing_events",
    keys=["loan_id"],  # The primary key to merge on
    sequence_by="event_timestamp",  # Ensures out-of-order events don't overwrite newer data
    apply_as_deletes=None,  # Optional: expression to flag deleted records
    except_column_list=None,
)
