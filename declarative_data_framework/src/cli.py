import yaml
from pyspark.sql import SparkSession
from .core.pipeline import Pipeline
from .engines.spark_engine import SparkEngine
from .models.pydantic_models import PipelineConfig


def get_engine(engine_name: str, config):
    """Factory to instantiate the correct engine based on config."""
    if engine_name == 'spark':
        # SparkEngine should handle its own SparkSession creation
        return SparkEngine(config)
    else:
        raise ValueError(f"Engine '{engine_name}' not supported.")


def run(config_path: str):
    """Run the pipeline using the provided YAML configuration."""
    config = _load_and_validate_config(config_path)
    engine = get_engine(config.engine, config)
    pipeline = Pipeline(config, engine)
    pipeline.run()


def create(config_path: str):
    """Create pipeline resources using the provided YAML configuration."""
    config = _load_and_validate_config(config_path)
    engine = get_engine(config.engine, config)
    pipeline = Pipeline(config, engine)
    pipeline.create()


def update(config_path: str):
    """Update pipeline resources using the provided YAML configuration."""
    config = _load_and_validate_config(config_path)
    engine = get_engine(config.engine, config)
    pipeline = Pipeline(config, engine)
    pipeline.update()


def test(config_path: str):
    """Test the pipeline using the provided YAML configuration."""
    config = _load_and_validate_config(config_path)
    engine = get_engine(config.engine, config)
    pipeline = Pipeline(config, engine)
    pipeline.test()


def _load_and_validate_config(config_path: str):
    """Load and validate the pipeline configuration from a YAML file."""
    with open(config_path, 'r') as f:
        config_dict = yaml.safe_load(f)
    try:
        config = PipelineConfig(**config_dict)
    except Exception as e:
        raise ValueError(f"YAML validation error: {e}")
    return config
