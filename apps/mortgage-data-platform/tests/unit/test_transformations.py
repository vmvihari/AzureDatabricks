from pyspark.sql.functions import avg, col, regexp_replace


# The logic we want to test
def cleanse_and_aggregate(df):
    df_clean = df.filter(col("status") == "APPROVED").withColumn(
        "applicant_ssn", regexp_replace(col("applicant_ssn"), "-", "")
    )

    df_summary = df_clean.groupBy("state").agg(
        avg("loan_amount").alias("average_loan_amount")
    )
    return df_clean, df_summary


def test_transformations(spark):
    # 1. Arrange: Create mock data using Spark SQL to bypass Python worker socket issues on Windows
    df_in = spark.sql(
        """
        SELECT 1 as loan_id, '123-45' as applicant_ssn, CAST(100.0 AS DOUBLE) as loan_amount,
            'TX' as state, 'APPROVED' as status UNION ALL
        SELECT 2 as loan_id, '999-99' as applicant_ssn, CAST(200.0 AS DOUBLE) as loan_amount,
            'TX' as state, 'DENIED' as status UNION ALL
        SELECT 3 as loan_id, '111-11' as applicant_ssn, CAST(300.0 AS DOUBLE) as loan_amount,
            'TX' as state, 'APPROVED' as status
    """
    )

    # 2. Act
    df_clean, df_summary = cleanse_and_aggregate(df_in)

    # 3. Assert
    # Verify DENIED loans were dropped
    assert df_clean.count() == 2

    # Verify hyphens were removed
    assert df_clean.filter(col("loan_id") == 1).first()["applicant_ssn"] == "12345"

    # Verify aggregation math ((100 + 300) / 2 = 200)
    assert (
        df_summary.filter(col("state") == "TX").first()["average_loan_amount"] == 200.0
    )
