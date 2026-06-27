import uuid
from abc import ABC, abstractmethod
from pyspark.sql import SparkSession

class BasePipeline(ABC):
    """
    Abstract Base Class for defining data pipelines.
    Subclasses must implement the three Medallion stages: Bronze, Silver, and Gold.
    This architecture enables the telemetry and RCA agent to automatically trace
    lineage, validation, and errors for any registered pipeline.
    """
    @property
    @abstractmethod
    def name(self) -> str:
        """Returns the name of the pipeline (e.g., 'ecommerce_dimensions')."""
        pass

    @abstractmethod
    def run_bronze(self, spark: SparkSession, batch_id: str, stage_run_id: str):
        """Runs the Bronze ingestion stage."""
        pass

    @abstractmethod
    def run_silver(self, spark: SparkSession, batch_id: str, stage_run_id: str):
        """Runs the Silver cleaning and transformation stage."""
        pass

    @abstractmethod
    def run_gold(self, spark: SparkSession, batch_id: str, stage_run_id: str):
        """Runs the Gold aggregation and business KPI stage."""
        pass

    def run_all(self, spark: SparkSession, batch_id: str) -> dict:
        """
        Runs the entire medallion pipeline (Bronze -> Silver -> Gold)
        generating a unique stage_run_id for each stage.
        """
        stage_runs = {}
        
        # Bronze stage
        bronze_run_id = str(uuid.uuid4())
        stage_runs["bronze"] = bronze_run_id
        print(f"[{self.name}] Running Bronze Stage (Run ID: {bronze_run_id})")
        self.run_bronze(spark, batch_id, bronze_run_id)
        
        # Silver stage
        silver_run_id = str(uuid.uuid4())
        stage_runs["silver"] = silver_run_id
        print(f"[{self.name}] Running Silver Stage (Run ID: {silver_run_id})")
        self.run_silver(spark, batch_id, silver_run_id)
        
        # Gold stage
        gold_run_id = str(uuid.uuid4())
        stage_runs["gold"] = gold_run_id
        print(f"[{self.name}] Running Gold Stage (Run ID: {gold_run_id})")
        self.run_gold(spark, batch_id, gold_run_id)
        
        print(f"[{self.name}] Medallion Pipeline run complete for batch {batch_id}.")
        return stage_runs

# Simple registry pattern
_PIPELINES = {}

def register_pipeline(pipeline_class):
    instance = pipeline_class()
    _PIPELINES[instance.name] = instance
    return pipeline_class

def get_pipeline(name: str) -> BasePipeline:
    if name not in _PIPELINES:
        raise ValueError(f"Pipeline '{name}' is not registered. Registered pipelines: {list(_PIPELINES.keys())}")
    return _PIPELINES[name]

def list_pipelines():
    return list(_PIPELINES.keys())
