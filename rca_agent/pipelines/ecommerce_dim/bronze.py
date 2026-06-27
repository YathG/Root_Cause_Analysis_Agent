import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
from pyspark.sql import SparkSession
from rca_agent.observability.wrappers import run_bronze_pipeline_with_observability
from rca_agent.config import settings

# Define Schemas
BRANDS_SCHEMA = StructType([
    StructField("brand_code", StringType(), False),
    StructField("brand_name", StringType(), True),
    StructField("category_code", StringType(), True),
])

CATEGORY_SCHEMA = StructType([
    StructField("category_code", StringType(), False),
    StructField("category_name", StringType(), True)
])

PRODUCTS_SCHEMA = StructType([
    StructField("product_id", StringType(), False),
    StructField("sku", StringType(), True),
    StructField("category_code", StringType(), True),
    StructField("brand_code", StringType(), True),
    StructField("color", StringType(), True),
    StructField("size", StringType(), True),
    StructField("material", StringType(), True),
    StructField("weight_grams", StringType(), True),  # String due to anomalies
    StructField("length_cm", StringType(), True),      # String due to anomalies
    StructField("width_cm", FloatType(), True),
    StructField("height_cm", FloatType(), True),
    StructField("rating_count", IntegerType(), True)
])

CUSTOMERS_SCHEMA = StructType([
    StructField("customer_id", StringType(), False),
    StructField("phone", StringType(), True),
    StructField("country_code", StringType(), True),
    StructField("country", StringType(), True),
    StructField("state", StringType(), True)
])

DATE_SCHEMA = StructType([
    StructField("date", StringType(), True),
    StructField("year", IntegerType(), True),
    StructField("day_name", StringType(), True),
    StructField("quarter", IntegerType(), True),
    StructField("week_of_year", IntegerType(), True),
])

# Base Loader Functions (Matching original notebooks)

def load_bronze_brands(spark: SparkSession, source_path: str, target_table: str, schema: StructType, delimiter: str = ","):
    df = spark.read.option("header", "true").option("delimiter", delimiter).schema(schema).csv(source_path)
    df = df.withColumn("_source_file", F.col("_metadata.file_path")) \
           .withColumn("ingested_at", F.current_timestamp())
    df.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(target_table)
    return df

def load_bronze_category(spark: SparkSession, source_path: str, target_table: str, schema: StructType, delimiter: str = ","):
    df = spark.read.option("header", "true").option("delimiter", delimiter).schema(schema).csv(source_path)
    df = df.withColumn("_ingested_at", F.current_timestamp()) \
           .withColumn("_source_file", F.col("_metadata.file_path"))
    df.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(target_table)
    return df

def load_bronze_products(spark: SparkSession, source_path: str, target_table: str, schema: StructType, delimiter: str = ","):
    df = spark.read.option("header", "true").option("delimiter", delimiter).schema(schema).csv(source_path)
    df = df.withColumn("_ingested_at", F.current_timestamp()) \
           .withColumn("_source_file", F.col("_metadata.file_path"))
    df.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(target_table)
    return df

def load_bronze_customers(spark: SparkSession, source_path: str, target_table: str, schema: StructType, delimiter: str = ","):
    df = spark.read.option("header", "true").option("delimiter", delimiter).schema(schema).csv(source_path)
    df = df.withColumn("_ingested_at", F.current_timestamp()) \
           .withColumn("_source_file", F.col("_metadata.file_path"))
    df.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(target_table)
    return df

def load_bronze_date(spark: SparkSession, source_path: str, target_table: str, schema: StructType, delimiter: str = ","):
    df = spark.read.option("header", "true").option("delimiter", delimiter).schema(schema).csv(source_path)
    df = df.withColumn("_ingested_at", F.current_timestamp()) \
           .withColumn("_source_file", F.col("_metadata.file_path"))
    df.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(target_table)
    return df

# Main Bronze Runner (observability wrapped)

def run_ecommerce_dim_bronze(spark: SparkSession, batch_id: str, stage_run_id: str):
    # 1. Brands
    run_bronze_pipeline_with_observability(
        batch_id=batch_id,
        stage_run_id=stage_run_id,
        pipeline_func=load_bronze_brands,
        pipeline_name="bronze_brands_pipeline",
        source_path=settings.get_raw_path("brands"),
        target_table=settings.get_table_name("brz_brands"),
        expected_schema=BRANDS_SCHEMA,
        primary_key="brand_code",
        spark=spark
    )
    
    # 2. Category
    run_bronze_pipeline_with_observability(
        batch_id=batch_id,
        stage_run_id=stage_run_id,
        pipeline_func=load_bronze_category,
        pipeline_name="bronze_category_pipeline",
        source_path=settings.get_raw_path("category"),
        target_table=settings.get_table_name("brz_category"),
        expected_schema=CATEGORY_SCHEMA,
        primary_key="category_code",
        spark=spark
    )
    
    # 3. Products
    run_bronze_pipeline_with_observability(
        batch_id=batch_id,
        stage_run_id=stage_run_id,
        pipeline_func=load_bronze_products,
        pipeline_name="bronze_products_pipeline",
        source_path=settings.get_raw_path("products"),
        target_table=settings.get_table_name("brz_products"),
        expected_schema=PRODUCTS_SCHEMA,
        primary_key="product_id",
        spark=spark
    )
    
    # 4. Customers
    run_bronze_pipeline_with_observability(
        batch_id=batch_id,
        stage_run_id=stage_run_id,
        pipeline_func=load_bronze_customers,
        pipeline_name="bronze_customers_pipeline",
        source_path=settings.get_raw_path("customers"),
        target_table=settings.get_table_name("brz_customers"),
        expected_schema=CUSTOMERS_SCHEMA,
        primary_key="customer_id",
        spark=spark
    )
    
    # 5. Date
    run_bronze_pipeline_with_observability(
        batch_id=batch_id,
        stage_run_id=stage_run_id,
        pipeline_func=load_bronze_date,
        pipeline_name="bronze_date_pipeline",
        source_path=settings.get_raw_path("date"),
        target_table=settings.get_table_name("brz_calendar"),  # target brz_calendar as per original date notebook
        expected_schema=DATE_SCHEMA,
        primary_key="date",
        spark=spark
    )
