from pyspark.sql import DataFrame
from ..core.step import BaseStep
from ..core.engine import BaseEngine
from ..models.pydantic_models import PipelineConfig
from ..exceptions import ConfigurationError
from ..logger import get_logger

logger = get_logger(__name__)

class ReadStep(BaseStep):
    """Step responsible for reading data from the source."""
    def execute(self, engine: BaseEngine, config: PipelineConfig):
        logger.info("--- Step: Read ---")
        df = engine.read(config)

        # Validate column presence (generic for both Spark and Pandas)
        source_config = config.source
        if source_config and source_config.expected_columns:
            # Try to get columns attribute (works for Spark and Pandas)
            columns = getattr(df, 'columns', None)
            if columns is not None:
                num_actual_columns = len(columns)
                if num_actual_columns != source_config.expected_columns:
                    delimiter = source_config.options.get('delimiter', '[NOT SPECIFIED]')
                    msg = (
                        f"Schema validation failed! File was read with {num_actual_columns} columns, "
                        f"but {source_config.expected_columns} were expected. "
                        f"Please check if the delimiter ('{delimiter}') is correct."
                    )
                    logger.error(msg)
                    raise ConfigurationError(msg)
        logger.info("Read and initial schema validation completed.")
        return df
