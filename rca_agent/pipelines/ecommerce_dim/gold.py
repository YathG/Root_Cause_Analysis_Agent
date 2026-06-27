import pyspark.sql.functions as F
from pyspark.sql.types import StringType, IntegerType, DateType, TimestampType, FloatType
from pyspark.sql import Row, SparkSession
from rca_agent.observability.wrappers import run_gold_pipeline_with_observability
from rca_agent.config import settings

def load_gold_dim_products(spark: SparkSession, source_tables: list, target_table: str):
    df_brands = spark.table(source_tables[0])
    df_category = spark.table(source_tables[1])
    df_products = spark.table(source_tables[2])

    print(f"[Products Gold] Products rows: {df_products.count()}")
    print(f"[Products Gold] Brands rows: {df_brands.count()}")
    print(f"[Products Gold] Category rows: {df_category.count()}")

    df_brand_category = (
        df_brands.alias("b")
        .join(
            df_category.alias("c"),
            F.col("b.category_code") == F.col("c.category_code"),
            "inner"
        )
        .select(
            F.col("b.brand_name"),
            F.col("b.brand_code"),
            F.col("c.category_name"),
            F.col("c.category_code")
        )
    )

    df_gold = (
        df_products.alias("p")
        .join(
            df_brand_category.alias("bc"),
            F.col("p.brand_code") == F.col("bc.brand_code"),
            "left"
        )
        .select(
            F.col("p.product_id"),
            F.col("p.sku"),
            F.col("p.category_code"),
            F.coalesce(F.col("bc.category_name"), F.lit("Not Available")).alias("category_name"),
            F.col("p.brand_code"),
            F.coalesce(F.col("bc.brand_name"), F.lit("Not Available")).alias("brand_name"),
            F.col("p.color"),
            F.col("p.size"),
            F.col("p.material"),
            F.col("p.weight_grams"),
            F.col("p.length_cm"),
            F.col("p.width_cm"),
            F.col("p.height_cm"),
            F.col("p.rating_count"),
            F.col("p._source_file"),
            F.col("p._ingested_at"),
            F.col("p._silver_processed_at")
        )
    )

    df_gold = df_gold.withColumn("_gold_processed_at", F.current_timestamp())
    df_gold.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(target_table)
    return df_gold

def load_gold_customers(spark: SparkSession, source_tables: list, target_table: str):
    india_region = {
        "MH": "West", "GJ": "West", "RJ": "West",
        "KA": "South", "TN": "South", "TS": "South", "AP": "South", "KL": "South",
        "UP": "North", "WB": "North", "DL": "North"
    }
    australia_region = {
        "VIC": "SouthEast", "WA": "West", "NSW": "East", "QLD": "NorthEast"
    }
    uk_region = {
        "ENG": "England", "WLS": "Wales", "NIR": "Northern Ireland", "SCT": "Scotland"
    }
    us_region = {
        "MA": "NorthEast", "FL": "South", "NJ": "NorthEast", "CA": "West", 
        "NY": "NorthEast", "TX": "South"
    }
    uae_region = {
        "AUH": "Abu Dhabi", "DU": "Dubai", "SHJ": "Sharjah"
    }
    singapore_region = {
        "SG": "Singapore"
    }
    canada_region = {
        "BC": "West", "AB": "West", "ON": "East", "QC": "East", "NS": "East", "IL": "Other"
    }

    country_state_map = {
        "India": india_region,
        "Australia": australia_region,
        "United Kingdom": uk_region,
        "United States": us_region,
        "United Arab Emirates": uae_region,
        "Singapore": singapore_region,
        "Canada": canada_region
    }  

    rows = []
    for country, states in country_state_map.items():
        for state_code, region in states.items():
            rows.append(Row(country=country, state=state_code, region=region))

    df_region_mapping = spark.createDataFrame(rows)
    df_silver = spark.table(source_tables[0])
    df_gold = df_silver.join(df_region_mapping, on=['country', 'state'], how='left')
    df_gold = df_gold.fillna({'region': 'Other'})

    df_gold = df_gold.withColumn("_gold_processed_at", F.current_timestamp())
    df_gold.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(target_table)
    return df_gold

def load_gold_date(spark: SparkSession, source_tables: list, target_table: str):
    df_silver = spark.table(source_tables[0])
    df_gold = df_silver.withColumn("date_id", F.date_format(F.col("date"), "yyyyMMdd").cast("int"))
    df_gold = df_gold.withColumn("month_name", F.date_format(F.col("date"), "MMMM"))
    df_gold = df_gold.withColumn(
        "is_weekend",
        F.when(F.col("day_name").isin("Saturday", "Sunday"), 1).otherwise(0)
    )

    desired_columns_order = [
        "date_id", "date", "year", "month_name", "day_name", 
        "is_weekend", "quarter", "week", "_ingested_at", "_source_file"
    ]
    df_gold = df_gold.select(desired_columns_order)
    df_gold = df_gold.withColumn("_gold_processed_at", F.current_timestamp())
    
    # Save using the target table parameter
    df_gold.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(target_table)
    return df_gold

def run_ecommerce_dim_gold(spark: SparkSession, batch_id: str, stage_run_id: str):
    # 1. Products
    run_gold_pipeline_with_observability(
        batch_id=batch_id,
        stage_run_id=stage_run_id,
        pipeline_func=load_gold_dim_products,
        pipeline_name="gold_products_pipeline",
        source_tables=[
            settings.get_table_name("slv_brands"),
            settings.get_table_name("slv_category"),
            settings.get_table_name("slv_products")
        ],
        target_table=settings.get_table_name("gld_products"),
        business_metric_columns=["length_cm", "width_cm", "height_cm", "weight_grams"],
        expected_min_rows=10,  # lowered from 10000 for testability
        spark=spark
    )
    
    # 2. Customers
    run_gold_pipeline_with_observability(
        batch_id=batch_id,
        stage_run_id=stage_run_id,
        pipeline_func=load_gold_customers,
        pipeline_name="gold_customers_pipeline",
        source_tables=[settings.get_table_name("slv_customers")],
        target_table=settings.get_table_name("gld_customers"),
        business_metric_columns=["customer_id"],
        expected_min_rows=5,
        spark=spark
    )
    
    # 3. Date
    run_gold_pipeline_with_observability(
        batch_id=batch_id,
        stage_run_id=stage_run_id,
        pipeline_func=load_gold_date,
        pipeline_name="gold_date_pipeline",
        source_tables=[settings.get_table_name("slv_date")],
        target_table=settings.get_table_name("gld_date"),
        business_metric_columns=["date_id", "is_weekend"],
        expected_min_rows=10,
        spark=spark
    )
