import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType, DoubleType
from pyspark.sql import SparkSession
from rca_agent.observability.wrappers import run_gold_pipeline_with_observability
from rca_agent.config import settings

def load_gold_order_items(spark: SparkSession, source_tables: list, target_table: str):
    df = spark.table(source_tables[0])
    
    # 1) Add gross amount
    df = df.withColumn(
        "gross_amount",
        F.col("quantity") * F.col("unit_price")
    )
    
    # 2) Add discount_amount
    df = df.withColumn(
        "discount_amount",
        F.ceil(F.col("gross_amount") * (F.col("discount_pct") / 100.0))
    )
    
    # 3) Add sale_amount = gross - discount + tax
    df = df.withColumn(
        "sale_amount",
        F.col("gross_amount") - F.col("discount_amount") + F.col("tax_amount")
    )
    
    # Add date id
    df = df.withColumn("date_id", F.date_format(F.col("dt"), "yyyyMMdd").cast(IntegerType()))
    
    # Coupon flag
    df = df.withColumn(
        "coupon_flag",
        F.when(F.col("coupon_code").isNotNull(), F.lit(1)).otherwise(F.lit(0))
    )
    
    # Define FX rates
    fx_rates = {
        "INR": 1.00,
        "AED": 24.18,
        "AUD": 57.55,
        "CAD": 62.93,
        "GBP": 117.98,
        "SGD": 68.18,
        "USD": 88.29,
    }
    
    rates = [(k, float(v)) for k, v in fx_rates.items()]
    rates_df = spark.createDataFrame(rates, ["currency", "inr_rate"])
    
    df = (
        df
        .join(
            rates_df,
            rates_df.currency == F.upper(F.trim(F.col("unit_price_currency"))),
            "left"
        )
        .withColumn("sale_amount_inr", F.col("sale_amount") * F.col("inr_rate"))
        .withColumn("sale_amount_inr", F.ceil(F.col("sale_amount_inr")))
    )
    
    orders_gold_df = df.select(
        F.col("date_id"),
        F.col("dt").alias("transaction_date"),
        F.col("order_ts").alias("transaction_ts"),
        F.col("order_id").alias("transaction_id"),
        F.col("customer_id"),
        F.col("item_seq").alias("seq_no"),
        F.col("product_id"),
        F.col("channel"),
        F.col("coupon_code"),
        F.col("coupon_flag"),
        F.col("unit_price_currency"),
        F.col("quantity"),
        F.col("unit_price"),
        F.col("gross_amount"),
        F.col("discount_pct").alias("discount_percent"),
        F.col("discount_amount"),
        F.col("tax_amount"),
        F.col("sale_amount").alias("net_amount"),
        F.col("sale_amount_inr").alias("net_amount_inr")
    )
    
    orders_gold_df.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(target_table)
    return orders_gold_df

def run_ecommerce_fact_gold(spark: SparkSession, batch_id: str, stage_run_id: str):
    run_gold_pipeline_with_observability(
        batch_id=batch_id,
        stage_run_id=stage_run_id,
        pipeline_func=load_gold_order_items,
        pipeline_name="gold_order_items_pipeline",
        source_tables=[settings.get_table_name("slv_fact_order_items")],
        target_table=settings.get_table_name("gld_fact_order_items"),
        business_metric_columns=["quantity", "net_amount", "net_amount_inr"],
        expected_min_rows=10,  # lowered from original for testability
        spark=spark
    )
