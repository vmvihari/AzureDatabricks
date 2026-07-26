from src.silver.cleansed_credit_scores import cleanse_credit_scores


def test_cleanse_credit_scores(spark):
    df_in = spark.sql(
        """
        SELECT '123' as ssn, 700 as credit_score UNION ALL
        SELECT '123' as ssn, 750 as credit_score UNION ALL
        SELECT '456' as ssn, 900 as credit_score UNION ALL
        SELECT CAST(NULL AS STRING) as ssn, 600 as credit_score
    """
    )

    df_out = cleanse_credit_scores(df_in)

    assert (
        df_out.count() == 1
    )  # only one valid record remains (deduplicated SSN '123' since '456' has invalid score)
    assert "applicant_ssn" in df_out.columns
    assert "ssn" not in df_out.columns
