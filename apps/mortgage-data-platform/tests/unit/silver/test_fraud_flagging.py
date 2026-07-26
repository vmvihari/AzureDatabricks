from src.silver.fraud_flagging import flag_fraud


def test_flag_fraud_broadcast_join(spark):
    # Create the mock DataFrames using native Spark SQL
    # This completely bypasses PySpark's RDD Python worker socket on Windows
    df_loans = spark.sql(
        """
        SELECT 'L-01' as loan_id, '12345' as applicant_ssn UNION ALL
        SELECT 'L-02' as loan_id, '99999' as applicant_ssn
    """
    )

    df_blacklist = spark.sql(
        """
        SELECT '99999' as ssn
    """
    )

    df_flagged = flag_fraud(df_loans, df_blacklist)

    # Assert L-01 is False
    assert (
        df_flagged.filter(df_flagged.loan_id == "L-01").first()["is_fraud_flagged"]
        is False
    )

    # Assert L-02 is True
    assert (
        df_flagged.filter(df_flagged.loan_id == "L-02").first()["is_fraud_flagged"]
        is True
    )
