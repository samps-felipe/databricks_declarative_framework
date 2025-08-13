from pyspark.sql import DataFrame
from ..core.step import BaseStep
from ..core.engine import BaseEngine
from ..models.pydantic_models import PipelineConfig
from ..logger import get_logger

logger = get_logger(__name__)

class WriterStep(BaseStep):
    """Step responsible for writing data to the destination."""
    def execute(self, df: DataFrame, engine: BaseEngine, config: PipelineConfig, validation_log_df: DataFrame = None):
        logger.info("--- Step: Write ---")
        engine.write(df, config, validation_log_df)
        logger.info("Write to destination completed.")
        return df
