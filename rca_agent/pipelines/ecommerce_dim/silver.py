import pyspark.sql.functions as F
from pyspark.sql.types import StringType, IntegerType, DateType, TimestampType, FloatType
from pyspark.sql import SparkSession
from rca_agent.observability.wrappers import run_silver_pipeline_with_observability
from rca_agent.config import settings

def load_silver_brands(spark: SparkSession, source_tables: list, target_table: str):
    df_bronze = spark.table(source_tables[0])
    df_silver = df_bronze.withColumn("brand_name", F.trim(F.col("brand_name")))
    df_silver = df_silver.withColumn("brand_code", F.regexp_replace(F.col("brand_code"), r'[^A-Za-z0-9]', ''))
    
    anomalies = {
        "GROCERY": "GRCY",
        "BOOKS": "BKS",
        "TOYS": "TOY"
    }
    df_silver = df_silver.replace(to_replace=anomalies, subset=["category_code"])
    df_silver = df_silver.withColumn("_silver_processed_at", F.current_timestamp())
    
    df_silver.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(target_table)
    return df_silver

def load_silver_category(spark: SparkSession, source_tables: list, target_table: str):
    df_bronze = spark.table(source_tables[0])
    df_silver = df_bronze.dropDuplicates(['category_code'])
    df_silver = df_silver.withColumn("category_code", F.upper(F.col("category_code")))
    df_silver = df_silver.withColumn("_silver_processed_at", F.current_timestamp())
    
    df_silver.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(target_table)
    return df_silver

def load_silver_products(spark: SparkSession, source_tables: list, target_table: str):
    df_bronze = spark.read.table(source_tables[0])
    row_count = df_bronze.count()
    column_count = len(df_bronze.columns)
    print(f"[Products] Rows: {row_count}, Columns: {column_count}")

    df_silver = df_bronze.withColumn(
        "weight_grams",
        F.regexp_replace(F.col("weight_grams"), "g", "").cast(IntegerType())
    )
    df_silver = df_silver.withColumn(
        "length_cm",
        F.regexp_replace(F.col("length_cm"), ",", ".").cast(FloatType())
    )
    df_silver = df_silver.withColumn("category_code", F.upper(F.col("category_code"))) \
                         .withColumn("brand_code", F.upper(F.col("brand_code")))
    
    df_silver = df_silver.withColumn(
        "material",
        F.when(F.col("material") == "Coton", "Cotton")
        .when(F.col("material") == "Alumium", "Aluminum")
        .when(F.col("material") == "Ruber", "Rubber")
        .otherwise(F.col("material"))
    )
    df_silver = df_silver.withColumn(
        "rating_count",
        F.when(F.col("rating_count").isNotNull(), F.abs(F.col("rating_count")))
        .otherwise(F.lit(0))
    )
    
    df_silver = df_silver.withColumn("_silver_processed_at", F.current_timestamp())
    df_silver.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(target_table)
    return df_silver

def load_silver_customers(spark: SparkSession, source_tables: list, target_table: str):
    df_bronze = spark.read.table(source_tables[0])
    row_count = df_bronze.count()
    column_count = len(df_bronze.columns)
    print(f"[Customers] Rows: {row_count}, Columns: {column_count}")

    df_silver = df_bronze.dropna(subset=["customer_id"])
    df_silver = df_silver.fillna("Not Available", subset=["phone"])
    df_silver = df_silver.withColumn("_silver_processed_at", F.current_timestamp())
    
    df_silver.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(target_table)
    return df_silver

def load_silver_date(spark: SparkSession, source_tables: list, target_table: str):
    df_bronze = spark.read.table(source_tables[0])
    row_count = df_bronze.count()
    column_count = len(df_bronze.columns)
    print(f"[Date] Rows: {row_count}, Columns: {column_count}")

    df_silver = df_bronze.withColumn("date", F.to_date(df_bronze["date"], "dd-MM-yyyy"))
    df_silver = df_silver.dropDuplicates(['date'])
    df_silver = df_silver.withColumn("day_name", F.initcap(F.col("day_name")))
    df_silver = df_silver.withColumn("week_of_year", F.abs(F.col("week_of_year")))
    
    # Matching original quarter/week concats
    # Use nested concats as per original notebook
    df_silver = df_silver.withColumn("quarter", F.concat_ws("", F.concat(F.lit("Q"), F.col("quarter"), F.lit("-"), F.col("year"))))
    df_silver = df_silver.withColumn("week_of_year", F.concat_ws("-", F.concat(F.lit("Week"), F.col("week_of_year"), F.lit("-"), F.col("year"))))
    df_silver = df_silver.withColumnRenamed("week_of_year", "week")
    
    df_silver = df_silver.withColumn("_silver_processed_at", F.current_timestamp())
    df_silver.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(target_table)
    return df_silver

def run_ecommerce_dim_silver(spark: SparkSession, batch_id: str, stage_run_id: str):
    # 1. Brands
    run_silver_pipeline_with_observability(
        batch_id=batch_id,
        stage_run_id=stage_run_id,
        pipeline_func=load_silver_brands,
        pipeline_name="silver_brands_pipeline",
        source_tables=[settings.get_table_name("brz_brands")],
        target_table=settings.get_table_name("slv_brands"),
        primary_key="brand_code",
        business_critical_columns=["brand_code", "brand_name"],
        spark=spark
    )
    
    # 2. Category
    run_silver_pipeline_with_observability(
        batch_id=batch_id,
        stage_run_id=stage_run_id,
        pipeline_func=load_silver_category,
        pipeline_name="silver_category_pipeline",
        source_tables=[settings.get_table_name("brz_category")],
        target_table=settings.get_table_name("slv_category"),
        primary_key="category_code",
        business_critical_columns=["category_code", "category_name"],
        spark=spark
    )
    
    # 3. Products
    run_silver_pipeline_with_observability(
        batch_id=batch_id,
        stage_run_id=stage_run_id,
        pipeline_func=load_silver_products,
        pipeline_name="silver_products_pipeline",
        source_tables=[settings.get_table_name("brz_products")],
        target_table=settings.get_table_name("slv_products"),
        primary_key="product_id",
        business_critical_columns=["product_id", "sku", "category_code"],
        spark=spark
    )
    
    # 4. Customers
    run_silver_pipeline_with_observability(
        batch_id=batch_id,
        stage_run_id=stage_run_id,
        pipeline_func=load_silver_customers,
        pipeline_name="silver_customers_pipeline",
        source_tables=[settings.get_table_name("brz_customers")],
        target_table=settings.get_table_name("slv_customers"),
        primary_key="customer_id",
        business_critical_columns=["customer_id", "country"],
        spark=spark
    )
    
    # 5. Date
    run_silver_pipeline_with_observability(
        batch_id=batch_id,
        stage_run_id=stage_run_id,
        pipeline_func=load_silver_date,
        pipeline_name="silver_date_pipeline",
        source_tables=[settings.get_table_name("brz_calendar")],
        target_table=settings.get_table_name("slv_date"),  # slv_date corresponds to target_table
        primary_key="date",
        business_critical_columns=["date", "year", "quarter"],
        spark=spark
    )
