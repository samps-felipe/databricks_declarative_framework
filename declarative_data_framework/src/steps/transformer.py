import re
from ..core.step import BaseStep
from ..core.engine import BaseEngine
from ..models.pydantic_models import PipelineConfig
from ..logger import get_logger

logger = get_logger(__name__)

class TransformStep(BaseStep):
    def execute(self, df, engine: BaseEngine, config: PipelineConfig):
        logger.info("--- Step: Transform ---")
        return engine.process(df, config)
        current_df = engine.execute_gold_transformation(config)
        
        logger.info("Applying type casting to Gold DataFrame.")
        for spec in config.columns:
            final_name = spec.rename
            if final_name in current_df.columns:
                cast_func = "try_cast" if spec.try_cast else "cast"
                current_df = current_df.withColumn(final_name, expr(f"{cast_func}({final_name} AS {spec.type})"))

        final_df = self._add_control_columns(current_df, config)
        final_df = final_df.withColumn("created_at", col("updated_at")) # Specific to Gold
        
        logger.info("Gold transformations completed.")
        return final_df
