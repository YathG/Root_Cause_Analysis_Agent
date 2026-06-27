import os
import sys
from pyspark.sql import SparkSession

def get_spark_session(app_name="RCA_Project") -> SparkSession:
    """
    Retrieves the active SparkSession if running in Databricks,
    or initializes a local SparkSession with Delta Lake support if running locally.
    """
    # 1. Check if we are running in Databricks (or if a session already exists)
    try:
        active_session = SparkSession.getActiveSession()
        if active_session is not None:
            return active_session
    except Exception:
        pass

    # 2. Check if we are on a Databricks cluster using system modules
    if 'dbutils' in globals() or 'dbutils' in sys.modules:
        try:
            # Try to get SparkSession from globals
            if 'spark' in globals():
                return globals()['spark']
        except Exception:
            pass

    # 3. Running locally - Initialize SparkSession
    import os
    if not os.environ.get("HADOOP_HOME"):
        local_hadoop = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "hadoop"))
        if os.path.exists(local_hadoop):
            os.environ["HADOOP_HOME"] = local_hadoop
            bin_path = os.path.join(local_hadoop, "bin")
            if bin_path not in os.environ.get("PATH", ""):
                os.environ["PATH"] = bin_path + os.path.pathsep + os.environ["PATH"]
                
    # Configure Spark workers to use the active virtual environment's python executable
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
                
    print("No active SparkSession found. Building local SparkSession...")
    
    builder = SparkSession.builder \
        .appName(app_name) \
        .master("local[*]") \
        .config("spark.sql.shuffle.partitions", "2") \
        .config("spark.default.parallelism", "2") \
        .config("spark.sql.warehouse.dir", "./data/spark-warehouse")

    # Local SparkSession initialization (Parquet mode to prevent Scala/Delta jar conflicts)
    try:
        spark = builder.getOrCreate()
        print("SparkSession initialized successfully (local parquet mode).")
        _setup_local_spark(spark)
        return spark
    except Exception as e:
        print(f"Error initializing local SparkSession: {e}")
        raise e

def _setup_local_spark(spark: SparkSession):
    """
    Sets up local Spark environment by creating databases and monkey-patching
    3-level namespace table operations to run on a local 2-level namespace.
    """
    # 1. Create local databases
    spark.sql("CREATE DATABASE IF NOT EXISTS bronze")
    spark.sql("CREATE DATABASE IF NOT EXISTS silver")
    spark.sql("CREATE DATABASE IF NOT EXISTS gold")
    
    # 2. Monkey patch spark.table
    original_table = spark.table
    def local_table(tableName):
        parts = tableName.split(".")
        if len(parts) == 3:
            # ecommerce.bronze.brz_brands -> bronze.brz_brands
            tableName = f"{parts[1]}.{parts[2]}"
        return original_table(tableName)
    spark.table = local_table

    # 3. Monkey patch spark.sql
    original_sql = spark.sql
    def local_sql(sqlQuery, *args, **kwargs):
        sqlQuery = sqlQuery.replace("ecommerce.bronze.", "bronze.")
        sqlQuery = sqlQuery.replace("ecommerce.silver.", "silver.")
        sqlQuery = sqlQuery.replace("ecommerce.gold.", "gold.")
        return original_sql(sqlQuery, *args, **kwargs)
    spark.sql = local_sql

    # 4. Monkey patch DataFrameWriter.saveAsTable
    from pyspark.sql.readwriter import DataFrameWriter
    original_save_as_table = DataFrameWriter.saveAsTable
    def local_save_as_table(self, tableName, *args, **kwargs):
        parts = tableName.split(".")
        if len(parts) == 3:
            tableName = f"{parts[1]}.{parts[2]}"
        
        # Check if the table exists in the catalog. If not, clean up directory to prevent LOCATION_ALREADY_EXISTS.
        try:
            if not spark.catalog.tableExists(tableName):
                import shutil
                warehouse_dir = os.path.abspath("./data/spark-warehouse")
                subparts = tableName.split(".")
                paths_to_check = []
                if len(subparts) == 2:
                    paths_to_check.append(os.path.join(warehouse_dir, f"{subparts[0]}.db", subparts[1]))
                elif len(subparts) == 1:
                    paths_to_check.append(os.path.join(warehouse_dir, subparts[0]))
                    paths_to_check.append(os.path.join(warehouse_dir, "default.db", subparts[0]))
                    
                for p in paths_to_check:
                    abs_p = os.path.abspath(p)
                    if os.path.exists(abs_p):
                        print(f"Local Spark Cleanup: Removing orphan table directory {abs_p}")
                        shutil.rmtree(abs_p, ignore_errors=True)
        except Exception as ce:
            print(f"Warning during local Spark saveAsTable directory cleanup: {ce}")
            
        return original_save_as_table(self, tableName, *args, **kwargs)
    DataFrameWriter.saveAsTable = local_save_as_table

    # 5. Monkey patch DataFrameWriter.format (redirect delta to parquet locally)
    original_format = DataFrameWriter.format
    def local_format(self, sourceName):
        if sourceName.lower() == "delta":
            sourceName = "parquet"
        return original_format(self, sourceName)
    DataFrameWriter.format = local_format

    # 6. Monkey patch DataFrameReader.table
    from pyspark.sql.readwriter import DataFrameReader
    original_read_table = DataFrameReader.table
    def local_read_table(self, tableName):
        parts = tableName.split(".")
        if len(parts) == 3:
            tableName = f"{parts[1]}.{parts[2]}"
        return original_read_table(self, tableName)
    DataFrameReader.table = local_read_table

