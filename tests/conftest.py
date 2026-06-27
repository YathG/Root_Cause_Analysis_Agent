import os
import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark_session():
    """
    Provides a local SparkSession for running unit tests.
    """
    # Ensure environment is set to local
    os.environ["RCA_ENVIRONMENT"] = "local"
    
    # Import settings to trigger dotenv loading
    from rca_agent.config import settings
    
    builder = SparkSession.builder \
        .appName("RCA_Test_Session") \
        .master("local[1]") \
        .config("spark.sql.shuffle.partitions", "1") \
        .config("spark.default.parallelism", "1") \
        .config("spark.sql.warehouse.dir", "./data/spark-warehouse-test")
        
    spark = builder.getOrCreate()
    
    # Initialize databases
    spark.sql("CREATE DATABASE IF NOT EXISTS bronze")
    spark.sql("CREATE DATABASE IF NOT EXISTS silver")
    spark.sql("CREATE DATABASE IF NOT EXISTS gold")
    
    yield spark
    
    spark.stop()
