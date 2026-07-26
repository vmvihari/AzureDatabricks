from src.gold.state_risk_summary import aggregate_risk_by_state


def test_aggregate_risk_by_state(spark):
    # Create the mock DataFrame using native Spark SQL
    # This completely bypasses PySpark's RDD Python worker socket on Windows
    df_in = spark.sql(
        """
        SELECT 'TX' as state, CAST(100.0 AS DOUBLE) as loan_amount,
            True as is_fraud_flagged, 700 as credit_score UNION ALL
        SELECT 'TX' as state, CAST(200.0 AS DOUBLE) as loan_amount,
            False as is_fraud_flagged, 720 as credit_score UNION ALL
        SELECT 'CA' as state, CAST(500.0 AS DOUBLE) as loan_amount,
            False as is_fraud_flagged, 800 as credit_score
    """
    )

    df_out = aggregate_risk_by_state(df_in)

    # Assert row count
    assert df_out.count() == 2

    # Assert TX aggregations
    tx_row = df_out.filter(df_out.state == "TX").first()
    assert tx_row["total_exposure"] == 300.0
    assert tx_row["total_fraud_flags"] == 1
    assert tx_row["average_credit_score"] == 710.0
