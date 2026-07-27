from src.gold.credit_exposure import aggregate_exposure


def test_aggregate_exposure(spark):
    df_loans = spark.sql(
        """
        SELECT '123' as applicant_ssn, CAST(100.0 AS DOUBLE) as loan_amount UNION ALL
        SELECT '456' as applicant_ssn, CAST(200.0 AS DOUBLE) as loan_amount
    """
    )

    df_scores = spark.sql(
        """
        SELECT '123' as applicant_ssn, 800 as credit_score UNION ALL
        SELECT '456' as applicant_ssn, 600 as credit_score
    """
    )

    df_out = aggregate_exposure(df_loans, df_scores)

    assert df_out.count() == 2

    excellent_row = df_out.filter(df_out.credit_tier == "Excellent").first()
    assert excellent_row.total_exposure == 100.0

    poor_row = df_out.filter(df_out.credit_tier == "Poor").first()
    assert poor_row.total_exposure == 200.0
