import os
import json
import uuid
from datetime import datetime
from rca_agent.config import settings

def write_json_event(folder: str, event: dict) -> str:
    """
    Writes a dictionary as a JSON event file to settings.logs_base_path/folder.
    Creates directories if they do not exist.
    Returns the path to the written file.
    """
    base_path = settings.logs_base_path
    full_folder = os.path.join(base_path, folder).replace("\\", "/")
    os.makedirs(full_folder, exist_ok=True)
    
    file_name = f"{uuid.uuid4()}.json"
    file_path = os.path.join(full_folder, file_name).replace("\\", "/")
    
    with open(file_path, "w") as f:
        json.dump(event, f, indent=4, default=str)
        
    return file_path

# Stores Execution Metadata
def log_execution_event(
    batch_id,
    stage_run_id,
    artifact_id,
    pipeline_name,
    stage_name,
    status,
    execution_time,
    row_count=None,
    retry_count=0
):
    event = {
        "batch_id": batch_id,
        "stage_run_id": stage_run_id,
        "artifact_id": artifact_id,
        "event_type": "execution",
        "timestamp": str(datetime.now()),
        "pipeline_name": pipeline_name,
        "stage_name": stage_name,
        "status": status,
        "execution_time_sec": execution_time,
        "row_count": row_count,
        "retry_count": retry_count
    }
    write_json_event("logs/execution", event)

def log_schema_event(
    batch_id,
    stage_run_id,
    artifact_id,
    pipeline_name,
    stage_name,
    expected_schema,
    actual_schema,
    missing_columns,
    extra_columns,
    type_changes
):
    event = {
        "batch_id": batch_id,
        "stage_run_id": stage_run_id,
        "artifact_id": artifact_id,
        "event_type": "schema_drift",
        "timestamp": str(datetime.now()),
        "pipeline_name": pipeline_name,
        "stage_name": stage_name,
        "expected_schema": expected_schema,
        "actual_schema": actual_schema,
        "missing_columns": missing_columns,
        "extra_columns": extra_columns,
        "type_changes": type_changes
    }
    write_json_event("logs/schema", event)

def log_validation_event(
    batch_id,
    stage_run_id,
    artifact_id,
    pipeline_name,
    stage_name,
    validation_type,
    status,
    failed_rows=None,
    details=None
):
    event = {
        "batch_id": batch_id,
        "stage_run_id": stage_run_id,
        "artifact_id": artifact_id,
        "event_type": "validation",
        "timestamp": str(datetime.now()),
        "pipeline_name": pipeline_name,
        "stage_name": stage_name,
        "validation_type": validation_type,
        "status": status,
        "failed_rows": failed_rows,
        "details": details
    }
    write_json_event("logs/validation", event)

def log_lineage_event(
    batch_id,
    stage_run_id,
    artifact_id,
    upstream_tables,
    downstream_table,
    operation
):
    event = {
        "batch_id": batch_id,
        "stage_run_id": stage_run_id,
        "artifact_id": artifact_id,
        "event_type": "lineage",
        "timestamp": str(datetime.now()),
        "upstream_tables": upstream_tables,
        "downstream_table": downstream_table,
        "operation": operation
    }
    write_json_event("logs/lineage", event)

def log_anomaly_event(
    batch_id,
    stage_run_id,
    artifact_id,
    pipeline_name,
    stage_name,
    anomaly_type,
    anomaly_details
):
    event = {
        "batch_id": batch_id,
        "stage_run_id": stage_run_id,
        "artifact_id": artifact_id,
        "event_type": "anomaly",
        "timestamp": str(datetime.now()),
        "pipeline_name": pipeline_name,
        "stage_name": stage_name,
        "anomaly_type": anomaly_type,
        "details": anomaly_details
    }
    write_json_event("logs/anomalies", event)

def log_repair_attempt(
    batch_id,
    stage_run_id,
    artifact_id,
    repair_type,
    target_stage,
    repair_details,
    status
):
    event = {
        "batch_id": batch_id,
        "stage_run_id": stage_run_id,
        "artifact_id": artifact_id,
        "event_type": "repair_attempt",
        "timestamp": str(datetime.now()),
        "repair_type": repair_type,
        "target_stage": target_stage,
        "repair_details": repair_details,
        "status": status
    }
    folder = "repairs/successful" if status == "SUCCESS" else "repairs/failed"
    write_json_event(folder, event)

def quarantine_record(
    batch_id,
    stage_run_id,
    artifact_id,
    pipeline_name,
    source_file,
    error_type,
    raw_record,
    repair_attempted,
    repair_status
):
    event = {
        "batch_id": batch_id,
        "stage_run_id": stage_run_id,
        "artifact_id": artifact_id,
        "event_type": "quarantine_record",
        "timestamp": str(datetime.now()),
        "pipeline_name": pipeline_name,
        "source_file": source_file,
        "error_type": error_type,
        "raw_record": raw_record,
        "repair_attempted": repair_attempted,
        "repair_status": repair_status
    }
    write_json_event("quarantine", event)

def save_schema_config(
    batch_id,
    stage_run_id,
    artifact_id,
    table_name,
    schema_version,
    columns,
    primary_key,
    layer
):
    event = {
        "batch_id": batch_id,
        "stage_run_id": stage_run_id,
        "artifact_id": artifact_id,
        "table_name": table_name,
        "schema_version": schema_version,
        "layer": layer,
        "primary_key": primary_key,
        "columns": columns,
        "created_at": str(datetime.now())
    }
    write_json_event("configs/schemas", event)

def save_schema_evolution_rules(
    batch_id,
    stage_run_id,
    artifact_id,
    allow_new_columns,
    allow_missing_columns,
    auto_cast_types,
    strict_mode
):
    event = {
        "batch_id": batch_id,
        "stage_run_id": stage_run_id,
        "artifact_id": artifact_id,
        "allow_new_columns": allow_new_columns,
        "allow_missing_columns": allow_missing_columns,
        "auto_cast_types": auto_cast_types,
        "strict_mode": strict_mode,
        "created_at": str(datetime.now())
    }
    write_json_event("configs/schemas", event)

def save_validation_rule(
    batch_id,
    stage_run_id,
    artifact_id,
    table_name,
    column_name,
    rule_type,
    rule_value,
    severity
):
    event = {
        "batch_id": batch_id,
        "stage_run_id": stage_run_id,
        "artifact_id": artifact_id,
        "table_name": table_name,
        "column_name": column_name,
        "rule_type": rule_type,
        "rule_value": rule_value,
        "severity": severity,
        "created_at": str(datetime.now())
    }
    write_json_event("configs/validation_rules", event)

def save_regex_validation_rule(
    batch_id,
    stage_run_id,
    artifact_id,
    table_name,
    column_name,
    regex_pattern,
    severity
):
    event = {
        "batch_id": batch_id,
        "stage_run_id": stage_run_id,
        "artifact_id": artifact_id,
        "table_name": table_name,
        "column_name": column_name,
        "rule_type": "regex",
        "regex_pattern": regex_pattern,
        "severity": severity,
        "created_at": str(datetime.now())
    }
    write_json_event("configs/validation_rules", event)

def save_anomaly_rule(
    batch_id,
    stage_run_id,
    artifact_id,
    table_name,
    column_name,
    threshold,
    detection_method
):
    event = {
        "batch_id": batch_id,
        "stage_run_id": stage_run_id,
        "artifact_id": artifact_id,
        "table_name": table_name,
        "column_name": column_name,
        "rule_type": "anomaly_detection",
        "threshold": threshold,
        "detection_method": detection_method,
        "created_at": str(datetime.now())
    }
    write_json_event("configs/validation_rules", event)

def save_pipeline_config(
    batch_id,
    stage_run_id,
    artifact_id,
    pipeline_name,
    source_path,
    target_path,
    file_format,
    delimiter
):
    event = {
        "batch_id": batch_id,
        "stage_run_id": stage_run_id,
        "artifact_id": artifact_id,
        "pipeline_name": pipeline_name,
        "source_path": source_path,
        "target_path": target_path,
        "file_format": file_format,
        "delimiter": delimiter,
        "created_at": str(datetime.now())
    }
    write_json_event("configs/pipeline_configs", event)

def save_retry_policy(
    batch_id,
    stage_run_id,
    artifact_id,
    pipeline_name,
    max_retries,
    retry_delay_seconds
):
    event = {
        "batch_id": batch_id,
        "stage_run_id": stage_run_id,
        "artifact_id": artifact_id,
        "pipeline_name": pipeline_name,
        "max_retries": max_retries,
        "retry_delay_seconds": retry_delay_seconds,
        "created_at": str(datetime.now())
    }
    write_json_event("configs/pipeline_configs", event)

def save_agent_routing_config(
    batch_id,
    stage_run_id,
    artifact_id,
    error_type,
    assigned_agent,
    priority
):
    event = {
        "batch_id": batch_id,
        "stage_run_id": stage_run_id,
        "artifact_id": artifact_id,
        "error_type": error_type,
        "assigned_agent": assigned_agent,
        "priority": priority,
        "created_at": str(datetime.now())
    }
    write_json_event("configs/pipeline_configs", event)

def save_repair_strategy(
    batch_id,
    stage_run_id,
    artifact_id,
    repair_type,
    enabled,
    auto_reprocess
):
    event = {
        "batch_id": batch_id,
        "stage_run_id": stage_run_id,
        "artifact_id": artifact_id,
        "repair_type": repair_type,
        "enabled": enabled,
        "auto_reprocess": auto_reprocess,
        "created_at": str(datetime.now())
    }
    write_json_event("configs/pipeline_configs", event)

def save_alerting_config(
    batch_id,
    stage_run_id,
    artifact_id,
    slack_alerts,
    email_alerts,
    critical_failure_threshold
):
    event = {
        "batch_id": batch_id,
        "stage_run_id": stage_run_id,
        "artifact_id": artifact_id,
        "slack_alerts": slack_alerts,
        "email_alerts": email_alerts,
        "critical_failure_threshold": critical_failure_threshold,
        "created_at": str(datetime.now())
    }
    write_json_event("configs/pipeline_configs", event)
