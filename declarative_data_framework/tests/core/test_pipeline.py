# Testes para Pipeline genérico
import pytest
from declarative_data_framework.core.pipeline import Pipeline
from declarative_data_framework.models.pydantic_models import PipelineConfig

def test_pipeline_instantiation():
    config = PipelineConfig(pipeline_name='test', engine='pandas', pipeline_type='silver', source={}, sink={}, columns=[])
    pipeline = Pipeline(config, None)
    assert pipeline.pipeline_config.pipeline_name == 'test'
