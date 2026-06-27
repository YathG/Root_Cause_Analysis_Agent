import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql import SparkSession
from rca_agent.observability.wrappers import run_bronze_pipeline_with_observability
from rca_agent.config import settings

ORDER_ITEMS_SCHEMA = StructType([
    StructField("dt",                  StringType(), True),
    StructField("order_ts",            StringType(), True),
    StructField("customer_id",         StringType(), True),
    StructField("order_id",            StringType(), True),
    StructField("item_seq",            StringType(), True),
    StructField("product_id",          StringType(), True),
    StructField("quantity",            StringType(), True),
    StructField("unit_price_currency", StringType(), True),
    StructField("unit_price",          StringType(), True),
    StructField("discount_pct",        StringType(), True),
    StructField("tax_amount",          StringType(), True),
    StructField("channel",             StringType(), True),
    StructField("coupon_code",         StringType(), True),
])

def load_bronze_order_items(spark: SparkSession, source_path: str, target_table: str, schema: StructType, delimiter: str = ","):
    df = spark.read.option("header", "true").option("delimiter", delimiter).schema(schema).csv(source_path)
    df = df.withColumn("file_name", F.col("_metadata.file_path")) \
           .withColumn("ingest_timestamp", F.current_timestamp())
    df.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(target_table)
    return df

def run_ecommerce_fact_bronze(spark: SparkSession, batch_id: str, stage_run_id: str):
    run_bronze_pipeline_with_observability(
        batch_id=batch_id,
        stage_run_id=stage_run_id,
        pipeline_func=load_bronze_order_items,
        pipeline_name="bronze_order_items_pipeline",
        source_path=settings.get_raw_path("order_items"),
        target_table=settings.get_table_name("brz_order_items"),
        expected_schema=ORDER_ITEMS_SCHEMA,
        primary_key=["order_id", "item_seq"],
        spark=spark
    )
