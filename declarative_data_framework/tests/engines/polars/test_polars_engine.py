# Testes para PolarsEngine
import pytest
from declarative_data_framework.engines.polars import PolarsEngine
from declarative_data_framework.models.pydantic_models import PipelineConfig

def test_polars_engine_instantiation():
    config = PipelineConfig(pipeline_name='test', engine='polars', pipeline_type='silver', source={}, sink={}, columns=[])
    engine = PolarsEngine(config)
    assert engine.config.pipeline_name == 'test'
