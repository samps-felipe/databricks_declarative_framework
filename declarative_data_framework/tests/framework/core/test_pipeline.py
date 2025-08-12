from framework.core.pipeline import Pipeline
from framework.models.pydantic_models import PipelineModel
from framework.engines.spark_engine import SparkEngine

def test_create_pipeline(spark):
    """
    Testa a criação de um objeto Pipeline.
    """
    pipeline_model = PipelineModel(name="test_pipeline", steps=[])
    spark_engine = SparkEngine(spark)
    pipeline = Pipeline(pipeline_model, spark_engine)
    assert pipeline.pipeline_model.name == "test_pipeline"
    assert pipeline.engine == spark_engine
