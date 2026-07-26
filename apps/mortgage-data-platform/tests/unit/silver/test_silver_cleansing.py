from src.silver.cleansed_loans import cleanse_loans


def test_cleansing_drops_null_ssns_and_formats_strings(spark):
    # Create the mock DataFrame using native Spark SQL
    # This completely bypasses PySpark's RDD Python worker socket on Windows
    df_in = spark.sql(
        """
        SELECT 'L-01' as loan_id, '123-45-6789' as applicant_ssn UNION ALL
        SELECT 'L-02' as loan_id, CAST(NULL AS STRING) as applicant_ssn UNION ALL
        SELECT 'L-01' as loan_id, '123-45-6789' as applicant_ssn
    """
    )

    df_clean = cleanse_loans(df_in)

    # Assert duplicates and nulls are dropped
    assert df_clean.count() == 1

    # Assert hyphens are removed
    assert df_clean.first()["applicant_ssn"] == "123456789"
