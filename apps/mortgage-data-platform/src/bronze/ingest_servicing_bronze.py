from pyspark.sql.functions import col
from src.utils.spark import get_spark_session


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
    df_servicing = (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", "dbo.LoanServicingEvents")
        .option("user", db_user)
        .option("password", db_pass)
        .option("partitionColumn", "event_id")
        .option("lowerBound", "1")
        .option("upperBound", "10000000")
        .option("numPartitions", "10")
        .load()
    )

    # Apply filtering logic
    df_recent_servicing = extract_recent_events(df_servicing)

    # Write to Bronze Delta Table
    (
        df_recent_servicing.write.format("delta")
        .mode("append")
        .save(
            "abfss://bronze@stmortgagedata<your_initials>.dfs.core.windows.net/tables/bronze_servicing_events"
        )
    )
