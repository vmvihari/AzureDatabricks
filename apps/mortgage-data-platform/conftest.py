import os
import sys

import pytest
from pyspark.sql import SparkSession

# Add the project root to sys.path so that `from src.X.Y import Z` imports work
sys.path.insert(0, os.path.dirname(__file__))

# Fix for Windows: Ensure Spark uses the current Python executable
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local[1]").appName("LocalTest").getOrCreate()
