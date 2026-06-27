from typing import TypedDict

class PipelineState(TypedDict):
    """
    Represents the state of the Root Cause Analysis (RCA) pipeline investigation.
    """
    # Incident metadata (e.g. details of the failed stage run or anomalous KPI)
    incident: dict

    # 'failure' or 'anomaly'
    incident_type: str

    # The stage run ID currently being investigated
    current_stage_run_id: str

    # Lineage record for the current stage run
    current_lineage: dict

    # Queue of upstream lineage paths yet to be searched
    lineage_queue: list

    # Accumulated stage run context/logs collected as evidence
    evidence: list

    # Visited stage run IDs (to prevent infinite loops in cyclic graphs)
    visited_stage_runs: list

    # Whether the LLM investigator decides to continue searching upstream
    continue_search: bool

    # Final diagnosis text or intermediate decision JSON returned by the LLM
    diagnosis: str
