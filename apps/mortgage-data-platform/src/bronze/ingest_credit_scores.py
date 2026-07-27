from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name
from pyspark.sql.types import IntegerType, StringType, StructField, StructType


# 2. Define strict schema for ingestion (Schema on Read)
def get_credit_schema():
    return StructType(
        [
            StructField("loan_id", StringType(), True),
            StructField("ssn", StringType(), True),
            StructField("credit_score", IntegerType(), True),
            StructField("report_date", StringType(), True),
        ]
    )


if __name__ == "__main__":
    # 3. Initialize the Spark Session
    spark = SparkSession.builder.appName("Ingest Bronze Credit Scores").getOrCreate()

    # 4. Define paths
    landing_path = "abfss://bronze@stmortgagedata<your_initials>.dfs.core.windows.net/landing/credit_scores/"
    bronze_table_path = "abfss://bronze@stmortgagedata<your_initials>.dfs.core.windows.net/tables/bronze_credit_scores"

    # 5. Read the CSV data
    raw_df = (
        spark.read.format("csv")
        .option("header", "true")
        .schema(get_credit_schema())
        .load(landing_path)
    )

    # 6. Append audit metadata columns
    enriched_df = raw_df.withColumn(
        "_ingestion_timestamp", current_timestamp()
    ).withColumn("_source_file", input_file_name())

    # 7. Write to the Bronze Table in Delta format
    (enriched_df.write.format("delta").mode("append").save(bronze_table_path))

    print(f"Successfully ingested credit scores to {bronze_table_path}")
