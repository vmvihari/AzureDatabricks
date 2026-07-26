# Import the actual logic from our script
from src.bronze.ingest_servicing_bronze import extract_recent_events


def test_jdbc_date_filtering(spark):
    # 1. Arrange: Create mock data using Spark SQL to bypass Python worker socket issues on Windows
    df_mock_jdbc = spark.sql(
        """
        SELECT 1 as event_id, 2019 as event_year UNION ALL
        SELECT 2 as event_id, 2021 as event_year
    """
    )

    # 2. Act: Run our extraction logic on the mock DB data
    df_result = extract_recent_events(df_mock_jdbc)

    # 3. Assert: Verify the old event was filtered out
    assert df_result.count() == 1
    assert df_result.first()["event_id"] == 2
