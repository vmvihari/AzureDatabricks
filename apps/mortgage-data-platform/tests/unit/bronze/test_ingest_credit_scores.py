from pyspark.sql.types import IntegerType

from src.bronze.ingest_credit_scores import get_credit_schema


def test_credit_schema():
    schema = get_credit_schema()

    # Verify the schema has the correct number of fields
    assert len(schema.fields) == 4

    # Verify credit_score is an IntegerType (preventing string/double ingestion issues)
    assert isinstance(schema["credit_score"].dataType, IntegerType)
