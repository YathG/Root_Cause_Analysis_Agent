import os
import json
import traceback
from datetime import datetime
from openai import OpenAI

from rca_agent.config import settings

# Helper: Get OpenRouter Client
def _get_client() -> OpenAI:
    api_key = settings.openrouter_api_key
    if not api_key:
        raise ValueError(
            "OPENROUTER_API_KEY environment variable is not set and was not found in Databricks secrets. "
            "Please configure it in a .env file or environment variable."
        )
    return OpenAI(
        base_url=settings.openrouter_base_url,
        api_key=api_key
    )

# Helper: Get logs directory path
def _get_logs_path() -> str:
    # settings.logs_base_path is eg: './data/RCA_Logging_Artifacts'
    # The agent expects to read logs from settings.logs_base_path/logs/
    return os.path.join(settings.logs_base_path, "logs").replace("\\", "/")

# Helper: Load all JSON files in a folder
def load_folder(folder_name: str) -> list:
    logs_dir = _get_logs_path()
    folder_path = os.path.join(logs_dir, folder_name).replace("\\", "/")
    artifacts = []

    if not os.path.exists(folder_path):
        return artifacts

    for file in os.listdir(folder_path):
        if file.endswith(".json"):
            file_path = os.path.join(folder_path, file)
            try:
                with open(file_path, "r") as f:
                    artifacts.append(json.load(f))
            except Exception as e:
                print(f"Error loading {file}: {e}")

    return artifacts

# Helper: Get latest failure
def get_latest_failure() -> dict:
    execution_logs = load_folder("execution")
    failures = [x for x in execution_logs if x.get("status") == "FAILED"]
    if not failures:
        return None
    return max(failures, key=lambda x: x["timestamp"])

# Helper: Get latest anomaly
def get_latest_anomaly() -> dict:
    anomalies_logs = load_folder("anomalies")
    if not anomalies_logs:
        return None
    return max(anomalies_logs, key=lambda x: x["timestamp"])

# Helper: Get stage run context
def get_stage_context(stage_run_id: str) -> dict:
    execution = [x for x in load_folder("execution") if x.get("stage_run_id") == stage_run_id]
    schema = [x for x in load_folder("schema") if x.get("stage_run_id") == stage_run_id]
    anomalies = [x for x in load_folder("anomalies") if x.get("stage_run_id") == stage_run_id]
    lineage = [x for x in load_folder("lineage") if x.get("stage_run_id") == stage_run_id]

    return {
        "execution": execution,
        "schema": schema,
        "anomalies": anomalies,
        "lineage": lineage
    }

# Helper: Get lineage record
def get_lineage_record(stage_run_id: str, artifact_id: str) -> dict:
    lineage_logs = load_folder("lineage")
    for log in lineage_logs:
        if log.get("stage_run_id") == stage_run_id and log.get("artifact_id") == artifact_id:
            return log
    return None

# Helper: Find upstream lineage
def get_upstream_lineage(upstream_table: str, batch_id: str) -> list:
    lineage_logs = load_folder("lineage")
    matches = []
    for log in lineage_logs:
        if log.get("batch_id") == batch_id and log.get("downstream_table") == upstream_table:
            matches.append(log)
    return matches

# ============================================================
# LANGGRAPH NODE FUNCTIONS
# ============================================================

def find_incident(state: dict) -> dict:
    print("[Agent Node] Finding latest incident...")
    failure = get_latest_failure()
    anomaly = get_latest_anomaly()

    if failure and anomaly:
        if failure["timestamp"] > anomaly["timestamp"]:
            incident = failure
            incident_type = "failure"
        else:
            incident = anomaly
            incident_type = "anomaly"
    elif failure:
        incident = failure
        incident_type = "failure"
    elif anomaly:
        incident = anomaly
        incident_type = "anomaly"
    else:
        raise Exception("No failures or anomalies found in telemetry logs.")

    stage_run_id = incident["stage_run_id"]
    artifact_id = incident["artifact_id"]
    lineage = get_lineage_record(stage_run_id, artifact_id)
    
    print(f"[Agent Node] Incident detected: Type={incident_type}, StageRunID={stage_run_id}")
    print(f"[Agent Node] Lineage found: {lineage}")

    return {
        "incident": incident,
        "incident_type": incident_type,
        "current_stage_run_id": stage_run_id,
        "current_lineage": lineage,
        "lineage_queue": [lineage] if lineage else [],
        "evidence": [],
        "visited_stage_runs": [stage_run_id]
    }

def investigate(state: dict) -> dict:
    stage_run_id = state["current_stage_run_id"]
    print(f"[Agent Node] Investigating Stage Run ID: {stage_run_id}...")
    
    context = get_stage_context(stage_run_id)
    evidence = state["evidence"] + [context]

    prompt = f"""
    You are a senior DataOps Root Cause Analysis Agent.

    Your goal is to identify the most likely root cause of the incident.

    Important:
    - The stage where an incident is observed is not necessarily where it originated.
    - Distinguish between symptoms and root causes.
    - A plausible explanation is not a definitive root cause.
    - Prefer to continue the investigation if the issue may have originated upstream.
    - Stop only when there is very strong evidence to confidently identify the originating cause.

    You do not have access to any tools, databases, logs, APIs, or external systems.

    All available evidence has already been provided.

    Do not request additional information.
    Do not call tools.
    Do not generate code, XML, function calls, SQL, or commands.

    Incident Type:
    {state["incident_type"]}

    Current Evidence:
    {json.dumps(evidence, indent=2)}

    Current Lineage:
    {json.dumps(state["current_lineage"], indent=2)}

    Return ONLY valid JSON in the exact format below:
    {{
        "continue_search": true/false,
        "reason": "Why you are continuing or stopping the investigation",
        "current_hypothesis": "Current best explanation based on available evidence",
        "definitive_hypothesis": "Final root cause if confidently identified, otherwise null"
    }}
    """

    client = _get_client()
    model = settings.llm_model
    
    response = client.chat.completions.create(
        model=model,
        messages=[
            {
                "role": "user",
                "content": prompt
            }
        ],
        extra_body={"reasoning": {"enabled": True}}
    )

    text = response.choices[0].message.content
    print(f"[Agent Node] Investigator response:\n{text}")
    
    # Check boolean flag inside generated response string
    continue_search = '"continue_search": true' in text.lower()

    return {
        "evidence": evidence,
        "diagnosis": text,
        "continue_search": continue_search
    }

def move_upstream(state: dict) -> dict:
    print("[Agent Node] Decision: Moving Upstream...")
    queue = list(state["lineage_queue"])
    batch_id = state["incident"]["batch_id"]

    if not queue:
        print("[Agent Node] Lineage queue empty. Ending upstream search.")
        return {"continue_search": False}

    current_lineage = queue.pop(0)
    upstream_tables = current_lineage.get("upstream_tables", [])
    visited = set(state["visited_stage_runs"])

    for table in upstream_tables:
        next_lineages = get_upstream_lineage(table, batch_id)
        if not next_lineages:
            continue
        
        next_lineage = next_lineages[0]
        next_stage_run = next_lineage["stage_run_id"]

        if next_stage_run not in visited:
            queue.append(next_lineage)
            visited.add(next_stage_run)

    if not queue:
        print("[Agent Node] No new upstream lineage stages to visit. Ending search.")
        return {"continue_search": False}

    next_lineage = queue.pop(0)
    print(f"[Agent Node] Upstream stage found: StageRunID={next_lineage['stage_run_id']}")

    return {
        "current_stage_run_id": next_lineage["stage_run_id"],
        "current_lineage": next_lineage,
        "lineage_queue": queue,
        "visited_stage_runs": list(visited)
    }

def route(state: dict) -> str:
    # Router node function for conditional edges
    if state["continue_search"]:
        return "move_upstream"
    return "__end__"
