import os
from rca_agent.config import settings
from rca_agent.pipelines import list_pipelines, get_pipeline

def test_settings_load():
    assert settings.environment in ("local", "databricks")
    assert settings.is_local or settings.is_databricks
    assert "RCA_Logging_Artifacts" in settings.logs_base_path

def test_pipeline_registration():
    pipelines = list_pipelines()
    assert "ecommerce_dimensions" in pipelines
    assert "ecommerce_facts" in pipelines

def test_get_registered_pipeline():
    pipeline = get_pipeline("ecommerce_dimensions")
    assert pipeline.name == "ecommerce_dimensions"
