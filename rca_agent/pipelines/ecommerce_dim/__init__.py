from rca_agent.pipelines.base import BasePipeline, register_pipeline
from .bronze import run_ecommerce_dim_bronze
from .silver import run_ecommerce_dim_silver
from .gold import run_ecommerce_dim_gold

@register_pipeline
class EcommerceDimPipeline(BasePipeline):
    @property
    def name(self) -> str:
        return "ecommerce_dimensions"

    def run_bronze(self, spark, batch_id, stage_run_id):
        run_ecommerce_dim_bronze(spark, batch_id, stage_run_id)

    def run_silver(self, spark, batch_id, stage_run_id):
        run_ecommerce_dim_silver(spark, batch_id, stage_run_id)

    def run_gold(self, spark, batch_id, stage_run_id):
        run_ecommerce_dim_gold(spark, batch_id, stage_run_id)
