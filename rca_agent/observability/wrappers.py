import time
import traceback
import uuid
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

from rca_agent.utils import get_spark_session
from rca_agent.observability.telemetry import (
    log_execution_event,
    log_schema_event,
    log_lineage_event,
    log_anomaly_event,
    quarantine_record
)

def run_bronze_pipeline_with_observability(
    batch_id: str,
    stage_run_id: str,
    pipeline_func,
    pipeline_name: str,
    source_path: str,
    target_table: str,
    expected_schema: StructType,
    primary_key: str | list[str],
    delimiter: str = ",",
    spark=None
):
    if spark is None:
        spark = get_spark_session()

    artifact_id = str(uuid.uuid4())
    start_time = time.time()

    try:
        # ====================================================
        # DISCOVERY READ (To detect schema changes before loading)
        # ====================================================
        df_discovery = spark.read \
            .option("header", "true") \
            .option("delimiter", delimiter) \
            .option("inferSchema", "true") \
            .csv(source_path)

        # ====================================================
        # SCHEMA COMPARISON
        # ====================================================
        expected_schema_dict = {
            field.name: str(field.dataType)
            for field in expected_schema.fields
        }

        actual_schema_dict = {
            field.name: str(field.dataType)
            for field in df_discovery.schema.fields
        }

        missing_columns = list(
            set(expected_schema_dict.keys()) -
            set(actual_schema_dict.keys())
        )

        extra_columns = list(
            set(actual_schema_dict.keys()) -
            set(expected_schema_dict.keys())
        )

        type_changes = {}
        for col in expected_schema_dict:
            if col in actual_schema_dict:
                if expected_schema_dict[col] != actual_schema_dict[col]:
                    type_changes[col] = {
                        "expected": expected_schema_dict[col],
                        "actual": actual_schema_dict[col]
                    }

        # ====================================================
        # SCHEMA DRIFT LOGGING
        # ====================================================
        if missing_columns or extra_columns or type_changes:
            log_schema_event(
                batch_id=batch_id,
                stage_run_id=stage_run_id,
                artifact_id=artifact_id,
                pipeline_name=pipeline_name,
                stage_name="bronze",
                expected_schema=expected_schema_dict,
                actual_schema=actual_schema_dict,
                missing_columns=missing_columns,
                extra_columns=extra_columns,
                type_changes=type_changes
            )

        # ====================================================
        # RUN MAIN PIPELINE
        # ====================================================
        df = pipeline_func(spark, source_path, target_table, expected_schema, delimiter=delimiter)

        # ====================================================
        # SIMPLE BRONZE ANOMALY CHECK
        # ====================================================
        row_count = df.count()

        if row_count == 0:
            log_anomaly_event(
                batch_id=batch_id,
                stage_run_id=stage_run_id,
                artifact_id=artifact_id,
                pipeline_name=pipeline_name,
                stage_name="bronze",
                anomaly_type="EMPTY_DATASET",
                anomaly_details={
                    "source_path": source_path
                }
            )

        # ====================================================
        # PRIMARY KEY QUARANTINE
        # ====================================================
        if isinstance(primary_key, list):
            filter_cond = F.col(primary_key[0]).isNull()
            for col_name in primary_key[1:]:
                filter_cond = filter_cond | F.col(col_name).isNull()
            bad_records = df.filter(filter_cond)
        else:
            bad_records = df.filter(F.col(primary_key).isNull())
        bad_count = bad_records.count()

        if bad_count > 0:
            records = bad_records.limit(10).collect()
            for row in records:
                quarantine_record(
                    batch_id=batch_id,
                    stage_run_id=stage_run_id,
                    artifact_id=artifact_id,
                    pipeline_name=pipeline_name,
                    source_file=source_path,
                    error_type="NULL_PRIMARY_KEY",
                    raw_record=row.asDict(),
                    repair_attempted=False,
                    repair_status="NOT_ATTEMPTED"
                )

        # ====================================================
        # LINEAGE LOGGING
        # ====================================================
        log_lineage_event(
            batch_id=batch_id,
            stage_run_id=stage_run_id,
            artifact_id=artifact_id,
            upstream_tables=[source_path],
            downstream_table=target_table,
            operation="raw_to_bronze_load"
        )

        # ====================================================
        # EXECUTION SUCCESS
        # ====================================================
        execution_time = round(time.time() - start_time, 2)
        log_execution_event(
            batch_id=batch_id,
            stage_run_id=stage_run_id,
            artifact_id=artifact_id,
            pipeline_name=pipeline_name,
            stage_name="bronze",
            status="SUCCESS",
            execution_time=execution_time,
            row_count=row_count,
            retry_count=0
        )

        return df

    except Exception as e:
        execution_time = round(time.time() - start_time, 2)

        # Failure Anomaly Logging
        log_anomaly_event(
            batch_id=batch_id,
            stage_run_id=stage_run_id,
            artifact_id=artifact_id,
            pipeline_name=pipeline_name,
            stage_name="bronze",
            anomaly_type="PIPELINE_FAILURE",
            anomaly_details={
                "error": traceback.format_exc()
            }
        )

        # Execution Failure Logging
        log_execution_event(
            batch_id=batch_id,
            stage_run_id=stage_run_id,
            artifact_id=artifact_id,
            pipeline_name=pipeline_name,
            stage_name="bronze",
            status="FAILED",
            execution_time=execution_time,
            row_count=0,
            retry_count=0
        )

        # Lineage Logging
        log_lineage_event(
            batch_id=batch_id,
            stage_run_id=stage_run_id,
            artifact_id=artifact_id,
            upstream_tables=[source_path],
            downstream_table=target_table,
            operation="raw_to_bronze_load"
        )

        raise e

def run_silver_pipeline_with_observability(
    batch_id: str,
    stage_run_id: str,
    pipeline_func,
    pipeline_name: str,
    source_tables: list,
    target_table: str,
    primary_key: str | list[str],
    business_critical_columns: list,
    spark=None
):
    if spark is None:
        spark = get_spark_session()

    artifact_id = str(uuid.uuid4())
    start_time = time.time()

    try:
        # ====================================================
        # RUN MAIN PIPELINE
        # ====================================================
        df = pipeline_func(spark, source_tables, target_table)
        row_count = df.count()

        # ====================================================
        # EMPTY DATASET CHECK
        # ====================================================
        if row_count == 0:
            log_anomaly_event(
                batch_id=batch_id,
                stage_run_id=stage_run_id,
                artifact_id=artifact_id,
                pipeline_name=pipeline_name,
                stage_name="silver",
                anomaly_type="EMPTY_DATASET",
                anomaly_details={
                    "source_tables": source_tables
                }
            )

        # ====================================================
        # PRIMARY KEY VALIDATION
        # ====================================================
        if isinstance(primary_key, list):
            filter_cond = F.col(primary_key[0]).isNull()
            for col_name in primary_key[1:]:
                filter_cond = filter_cond | F.col(col_name).isNull()
            null_pk_count = df.filter(filter_cond).count()
        else:
            null_pk_count = df.filter(F.col(primary_key).isNull()).count()

        if null_pk_count > 0:
            log_anomaly_event(
                batch_id=batch_id,
                stage_run_id=stage_run_id,
                artifact_id=artifact_id,
                pipeline_name=pipeline_name,
                stage_name="silver",
                anomaly_type="NULL_PRIMARY_KEY",
                anomaly_details={
                    "primary_key": primary_key,
                    "failed_rows": null_pk_count
                }
            )

        # ====================================================
        # DUPLICATE PRIMARY KEYS
        # ====================================================
        group_cols = primary_key if isinstance(primary_key, list) else [primary_key]
        duplicate_count = (
            df.groupBy(*group_cols)
            .count()
            .filter(F.col("count") > 1)
            .count()
        )

        if duplicate_count > 0:
            log_anomaly_event(
                batch_id=batch_id,
                stage_run_id=stage_run_id,
                artifact_id=artifact_id,
                pipeline_name=pipeline_name,
                stage_name="silver",
                anomaly_type="DUPLICATE_PRIMARY_KEYS",
                anomaly_details={
                    "duplicate_count": duplicate_count
                }
            )

        # ====================================================
        # BUSINESS CRITICAL COLUMN VALIDATION
        # ====================================================
        for column_name in business_critical_columns:
            null_count = df.filter(F.col(column_name).isNull()).count()
            if null_count > 0:
                log_anomaly_event(
                    batch_id=batch_id,
                    stage_run_id=stage_run_id,
                    artifact_id=artifact_id,
                    pipeline_name=pipeline_name,
                    stage_name="silver",
                    anomaly_type="BUSINESS_RULE_VIOLATION",
                    anomaly_details={
                        "column_name": column_name,
                        "failed_rows": null_count
                    }
                )

        # ====================================================
        # LINEAGE LOGGING
        # ====================================================
        log_lineage_event(
            batch_id=batch_id,
            stage_run_id=stage_run_id,
            artifact_id=artifact_id,
            upstream_tables=source_tables,
            downstream_table=target_table,
            operation="bronze_to_silver_transformation"
        )

        # ====================================================
        # EXECUTION SUCCESS
        # ====================================================
        execution_time = round(time.time() - start_time, 2)
        log_execution_event(
            batch_id=batch_id,
            stage_run_id=stage_run_id,
            artifact_id=artifact_id,
            pipeline_name=pipeline_name,
            stage_name="silver",
            status="SUCCESS",
            execution_time=execution_time,
            row_count=row_count,
            retry_count=0
        )

        return df

    except Exception as e:
        execution_time = round(time.time() - start_time, 2)

        # Failure Anomaly Logging
        log_anomaly_event(
            batch_id=batch_id,
            stage_run_id=stage_run_id,
            artifact_id=artifact_id,
            pipeline_name=pipeline_name,
            stage_name="silver",
            anomaly_type="PIPELINE_FAILURE",
            anomaly_details={
                "error": traceback.format_exc()
            }
        )

        # Execution Failure Logging
        log_execution_event(
            batch_id=batch_id,
            stage_run_id=stage_run_id,
            artifact_id=artifact_id,
            pipeline_name=pipeline_name,
            stage_name="silver",
            status="FAILED",
            execution_time=execution_time,
            row_count=0,
            retry_count=0
        )

        # Lineage Logging
        log_lineage_event(
            batch_id=batch_id,
            stage_run_id=stage_run_id,
            artifact_id=artifact_id,
            upstream_tables=source_tables,
            downstream_table=target_table,
            operation="bronze_to_silver_transformation"
        )

        raise e

def run_gold_pipeline_with_observability(
    batch_id: str,
    stage_run_id: str,
    pipeline_func,
    pipeline_name: str,
    source_tables: list,
    target_table: str,
    business_metric_columns: list,
    expected_min_rows: int,
    spark=None
):
    if spark is None:
        spark = get_spark_session()

    artifact_id = str(uuid.uuid4())
    start_time = time.time()

    try:
        # ====================================================
        # RUN MAIN PIPELINE
        # ====================================================
        df = pipeline_func(spark, source_tables, target_table)
        row_count = df.count()

        # ====================================================
        # EMPTY DATASET CHECK
        # ====================================================
        if row_count == 0:
            log_anomaly_event(
                batch_id=batch_id,
                stage_run_id=stage_run_id,
                artifact_id=artifact_id,
                pipeline_name=pipeline_name,
                stage_name="gold",
                anomaly_type="EMPTY_GOLD_DATASET",
                anomaly_details={
                    "source_tables": source_tables
                }
            )

        # ====================================================
        # LOW ROW COUNT CHECK
        # ====================================================
        if row_count < expected_min_rows:
            log_anomaly_event(
                batch_id=batch_id,
                stage_run_id=stage_run_id,
                artifact_id=artifact_id,
                pipeline_name=pipeline_name,
                stage_name="gold",
                anomaly_type="LOW_ROW_COUNT",
                anomaly_details={
                    "expected_min_rows": expected_min_rows,
                    "actual_row_count": row_count
                }
            )

        # ====================================================
        # BUSINESS KPI VALIDATION
        # ====================================================
        for metric_col in business_metric_columns:
            null_metric_count = df.filter(F.col(metric_col).isNull()).count()
            if null_metric_count > 0:
                log_anomaly_event(
                    batch_id=batch_id,
                    stage_run_id=stage_run_id,
                    artifact_id=artifact_id,
                    pipeline_name=pipeline_name,
                    stage_name="gold",
                    anomaly_type="MISSING_BUSINESS_METRIC",
                    anomaly_details={
                        "metric_column": metric_col,
                        "failed_rows": null_metric_count
                    }
                )

        # ====================================================
        # NEGATIVE METRIC CHECK
        # ====================================================
        from pyspark.sql.types import NumericType
        for metric_col in business_metric_columns:
            if isinstance(df.schema[metric_col].dataType, NumericType):
                negative_count = df.filter(F.col(metric_col) < 0).count()
                if negative_count > 0:
                    log_anomaly_event(
                        batch_id=batch_id,
                        stage_run_id=stage_run_id,
                        artifact_id=artifact_id,
                        pipeline_name=pipeline_name,
                        stage_name="gold",
                        anomaly_type="NEGATIVE_BUSINESS_METRIC",
                        anomaly_details={
                            "metric_column": metric_col,
                            "failed_rows": negative_count
                        }
                    )

        # ====================================================
        # LINEAGE LOGGING
        # ====================================================
        log_lineage_event(
            batch_id=batch_id,
            stage_run_id=stage_run_id,
            artifact_id=artifact_id,
            upstream_tables=source_tables,
            downstream_table=target_table,
            operation="silver_to_gold_aggregation"
        )

        # ====================================================
        # EXECUTION SUCCESS
        # ====================================================
        execution_time = round(time.time() - start_time, 2)
        log_execution_event(
            batch_id=batch_id,
            stage_run_id=stage_run_id,
            artifact_id=artifact_id,
            pipeline_name=pipeline_name,
            stage_name="gold",
            status="SUCCESS",
            execution_time=execution_time,
            row_count=row_count,
            retry_count=0
        )

        return df

    except Exception as e:
        execution_time = round(time.time() - start_time, 2)

        # Failure Anomaly Logging
        log_anomaly_event(
            batch_id=batch_id,
            stage_run_id=stage_run_id,
            artifact_id=artifact_id,
            pipeline_name=pipeline_name,
            stage_name="gold",
            anomaly_type="PIPELINE_FAILURE",
            anomaly_details={
                "error": traceback.format_exc()
            }
        )

        # Execution Failure Logging
        log_execution_event(
            batch_id=batch_id,
            stage_run_id=stage_run_id,
            artifact_id=artifact_id,
            pipeline_name=pipeline_name,
            stage_name="gold",
            status="FAILED",
            execution_time=execution_time,
            row_count=0,
            retry_count=0
        )

        # Lineage Logging
        log_lineage_event(
            batch_id=batch_id,
            stage_run_id=stage_run_id,
            artifact_id=artifact_id,
            upstream_tables=source_tables,
            downstream_table=target_table,
            operation="silver_to_gold_aggregation"
        )

        raise e
