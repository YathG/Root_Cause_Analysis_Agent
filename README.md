# Root Cause Analysis (RCA) AI Agent and Data Pipeline Project

This repository contains a production-grade, modular implementation of a Root Cause Analysis (RCA) AI Agent that works in tandem with PySpark Medallion pipelines. This codebase works as a standard Python package (`rca_agent`) that runs both **locally** and on a **Databricks cluster**. The AI agent is built using Langgraph framework.

---

## Architecture & Package Structure

The project is structured as follows:

```
.
├── config.yaml                  # Global configuration (Local vs Databricks profiles)
├── .env.example                 # Template for environment variables (API keys)
├── .gitignore                   # Excludes runtime data and logs from version control
├── requirements.txt             # Project dependencies
├── run_orchestrator.py          # Entrypoint script to run registered data pipelines
├── run_agent.py                 # Entrypoint script to run the LangGraph RCA Agent
│
├── rca_agent/                   # Core Python Package
│   ├── __init__.py
│   │
│   ├── config/                  # Configuration Management
│   │   ├── __init__.py
│   │   └── settings.py          # Unified config/env variable loader
│   │
│   ├── utils/                   # Spark Utilities
│   │   ├── __init__.py
│   │   └── spark.py             # Local/Remote SparkSession builder with Delta Lake patching
│   │
│   ├── observability/           # DataOps Telemetry
│   │   ├── __init__.py
│   │   ├── telemetry.py         # JSON logger (lineage, schema drift, anomalies)
│   │   └── wrappers.py          # Observability wrappers for Bronze/Silver/Gold pipeline runs
│   │
│   ├── agent/                   # LangGraph RCA Agent
│   │   ├── __init__.py
│   │   ├── state.py             # PipelineState schema definition
│   │   ├── nodes.py             # Agent node operations (investigate, move upstream)
│   │   └── graph.py             # LangGraph compiler
│   │
│   └── pipelines/               # PySpark Data Pipelines (Extensible)
│       ├── __init__.py
│       ├── base.py              # Base class/registration interface for pipelines
│       ├── ecommerce_dim/       # E-Commerce dimensions pipeline (Bronze -> Silver -> Gold)
│       └── ecommerce_fact/      # E-Commerce facts pipeline (Bronze -> Silver -> Gold)
│
└── tests/                       # Pytest unit tests
    ├── conftest.py
    └── test_pipelines.py
```

---

## Getting Started

### 1. Installation
Clone the repository and install dependencies locally:
```bash
pip install -r requirements.txt
```

### 2. Configure Environment Variables
Copy `.env.example` to `.env`:
```bash
cp .env.example .env
```
Open `.env` and fill in your OpenRouter API key:
```env
OPENROUTER_API_KEY=your_actual_api_key_here
```

### 3. Profile Setup (`config.yaml`)
Review `config.yaml`.
* **Local Run**: Set `environment: local`. Local directories will be created under `RCA_Logging_Artifacts/` and `./data/spark-warehouse/` for Delta/Parquet tables.
* **Databricks Run**: Set `environment: databricks`. The codebase automatically routes paths to Unity Catalog (`catalog_name.schema.table`) and loads keys using Databricks Secrets scope.

---

## Executing the Code

### Running Data Pipelines
Run the orchestrator script to execute a registered medallion data pipeline (e.g. `ecommerce_dimensions` or `ecommerce_facts`):
```bash
python run_orchestrator.py --pipeline ecommerce_dimensions
```
For Databricks jobs, you can pass parameters or use Databricks widgets `pipeline` and `batch_id`.

### Running the RCA AI Agent
Once your data pipeline has generated telemetry logs (schema changes, execution details, quarantine, lineage), you can run the LangGraph RCA Agent:
```bash
python run_agent.py
```
The agent automatically:
1. Scans telemetry logs for the latest failure or anomaly.
2. Formulates a graph traversal queue mapping upstream dependencies using lineage logs.
3. Queries the LLM investigator to decide whether to check upstream tables.
4. Stops and returns a final diagnosis once the root cause is isolated.

---

## How to Register Custom Data Pipelines

This project is built to be **fully extensible**. You can add any custom pipeline and run the RCA agent on it out of the box.

1. **Inherit `BasePipeline`**: Create a subclass of `BasePipeline` from `rca_agent.pipelines.base`.
2. **Implement Medallion Stages**: Implement `run_bronze`, `run_silver`, and `run_gold` using the observability wrappers under `rca_agent.observability.wrappers`.
3. **Register**: Annotate your class with `@register_pipeline`.

Example:
```python
from rca_agent.pipelines import BasePipeline, register_pipeline

@register_pipeline
class MyCustomPipeline(BasePipeline):
    @property
    def name(self) -> str:
        return "my_custom_pipeline"

    def run_bronze(self, spark, batch_id, stage_run_id):
        # Ingest raw files with observability wrappers
        pass

    def run_silver(self, spark, batch_id, stage_run_id):
        # Transform/clean tables
        pass

    def run_gold(self, spark, batch_id, stage_run_id):
        # Create final business metrics
        pass
```

---

## Local Spark session Monkey-Patching

To allow 100% compatibility with Databricks Unity Catalog 3-level namespaces (`catalog.database.table`) during local development, `rca_agent/utils/spark.py` monkey-patches PySpark's table resolution methods:
- `spark.table("catalog.schema.table")` translates to local database namespace `schema.table`.
- `spark.sql(...)` replaces catalog paths dynamically.
- `df.write.saveAsTable("catalog.schema.table")` saves locally to `schema.table`.
This allows the same business logic to run locally or in the cloud without modifying query strings.
