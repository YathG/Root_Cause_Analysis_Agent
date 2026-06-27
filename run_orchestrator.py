import sys

# Ensure UTF-8 output encoding for Windows terminals
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8")

import uuid
import argparse
from rca_agent.utils import get_spark_session
from rca_agent.pipelines import get_pipeline, list_pipelines

def get_dbutils(spark):
    try:
        from pyspark.dbutils import DBUtils
        return DBUtils(spark)
    except Exception:
        return None

def main():
    # 1. Setup argparse for command line execution
    parser = argparse.ArgumentParser(description="Orchestrate Medallion Data Pipelines")
    parser.add_argument("--pipeline", type=str, default="ecommerce_dimensions", 
                        help=f"Pipeline name to run. Registered options: {list_pipelines()}")
    parser.add_argument("--batch-id", type=str, default=None, 
                        help="Optional Batch UUID. Generated automatically if not provided.")
    args, unknown = parser.parse_known_args()

    # 2. Get Spark session
    spark = get_spark_session()
    
    # 3. Check for Databricks widgets override
    pipeline_name = args.pipeline
    batch_id = args.batch_id
    
    dbutils = get_dbutils(spark)
    if dbutils is not None:
        try:
            # Databricks widget setup
            dbutils.widgets.text("pipeline", "ecommerce_dimensions")
            dbutils.widgets.text("batch_id", "")
            
            w_pipeline = dbutils.widgets.get("pipeline")
            w_batch_id = dbutils.widgets.get("batch_id")
            
            if w_pipeline:
                pipeline_name = w_pipeline
            if w_batch_id:
                batch_id = w_batch_id
        except Exception as e:
            print(f"Non-fatal widget load error: {e}")

    # Generate batch_id if none provided
    if not batch_id:
        batch_id = str(uuid.uuid4())
        print(f"Generated new Batch ID: {batch_id}")
    else:
        print(f"Using provided Batch ID: {batch_id}")

    print(f"Running pipeline: {pipeline_name}")
    
    # 4. Run the registered pipeline
    try:
        pipeline = get_pipeline(pipeline_name)
        stage_runs = pipeline.run_all(spark, batch_id)
        
        print("\n=== EXECUTION SUCCESS ===")
        print(f"Pipeline: {pipeline_name}")
        print(f"Batch ID: {batch_id}")
        for stage, run_id in stage_runs.items():
            print(f"  Stage {stage.capitalize()} Run ID: {run_id}")
            
    except Exception as e:
        print(f"\n=== EXECUTION FAILURE: {e} ===")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
