import pyspark.sql.functions as F
from pyspark.sql.types import StringType, IntegerType, DoubleType, DateType, TimestampType
from pyspark.sql import SparkSession
from rca_agent.observability.wrappers import run_silver_pipeline_with_observability
from rca_agent.config import settings

def load_silver_order_items(spark: SparkSession, source_tables: list, target_table: str):
    df = spark.table(source_tables[0])
    
    # Transformation: Drop any duplicates
    df = df.dropDuplicates(["order_id", "item_seq"])
    
    # Transformation: Convert 'Two' -> 2 and cast to Integer
    df = df.withColumn(
        "quantity",
        F.when(F.col("quantity") == "Two", 2).otherwise(F.col("quantity")).cast("int")
    )
    
    # Transformation: Remove any '$' or other symbols from unit_price, keep only numeric
    df = df.withColumn(
        "unit_price",
        F.regexp_replace("unit_price", "[$]", "").cast("double")
    )
    
    # Transformation: Remove '%' from discount_pct and cast to double
    df = df.withColumn(
        "discount_pct",
        F.regexp_replace("discount_pct", "%", "").cast("double")
    )
    
    # Transformation: coupon code processing (convert to lower)
    df = df.withColumn(
        "coupon_code", F.lower(F.trim(F.col("coupon_code")))
    )
    
    # Transformation: channel processing 
    df = df.withColumn(
        "channel",
        F.when(F.col("channel") == "web", "Website")
        .when(F.col("channel") == "app", "Mobile")
        .otherwise(F.col("channel")),
    )
    
    # Transformation: datatype conversions
    # 1) Convert dt (string -> date)
    df = df.withColumn(
        "dt",
        F.to_date("dt", "yyyy-MM-dd")     
    )
    
    # 2) Convert order_ts (string -> timestamp)
    df = df.withColumn(
        "order_ts",
        F.coalesce(
            F.to_timestamp("order_ts", "yyyy-MM-dd HH:mm:ss"),  # matches 2025-08-01 22:53:52
            F.to_timestamp("order_ts", "dd-MM-yyyy HH:mm")      # fallback for 01-08-2025 22:53
        )
    )
    
    # 3) Convert item_seq (string -> integer)
    df = df.withColumn(
        "item_seq",
        F.col("item_seq").cast("int")
    )
    
    # 4) Convert tax_amount (string -> double, strip non-numeric characters)
    df = df.withColumn(
        "tax_amount",
        F.regexp_replace("tax_amount", r"[^0-9.\-]", "").cast("double")
    )
    
    # Transformation: Add processed time 
    df = df.withColumn(
        "processed_time", F.current_timestamp()
    )
    
    df.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(target_table)
    return df

def run_ecommerce_fact_silver(spark: SparkSession, batch_id: str, stage_run_id: str):
    run_silver_pipeline_with_observability(
        batch_id=batch_id,
        stage_run_id=stage_run_id,
        pipeline_func=load_silver_order_items,
        pipeline_name="silver_order_items_pipeline",
        source_tables=[settings.get_table_name("brz_order_items")],
        target_table=settings.get_table_name("slv_fact_order_items"),
        primary_key=["order_id", "item_seq"],
        business_critical_columns=["order_id", "product_id", "quantity", "unit_price"],
        spark=spark
    )
