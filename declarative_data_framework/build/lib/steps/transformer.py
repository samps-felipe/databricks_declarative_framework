from pyspark.sql import DataFrame, Column
from pyspark.sql.functions import col, expr, sha2, concat_ws, current_timestamp
from ..core.step import BaseStep
from ..core.engine import BaseEngine
from ..models.pydantic_models import PipelineConfig, ColumnConfig
from ..exceptions import ConfigurationError
from ..logger import get_logger
import re

logger = get_logger(__name__)

def _apply_rename_pattern(name: str) -> str:
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    s2 = re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1)
    return s2.replace(" ", "_").lower()

class TransformStep(BaseStep):
    def execute(self, df: DataFrame, engine: BaseEngine, config: PipelineConfig) -> DataFrame:
        logger.info("--- Step: Transform ---")
        if config.pipeline_type == 'silver':
            return self._transform_silver(df, config)
        elif config.pipeline_type == 'gold':
            return self._transform_gold(engine, config)
        else:
            raise NotImplementedError(f"Transformation for type '{config.pipeline_type}' is not implemented.")

    def _build_column_expression(self, spec: ColumnConfig, source_columns: list) -> Column:
        """Builds a single Spark column expression based on its specification."""
        final_name = spec.rename
        original_name = spec.name
        
        base_expr_str = ""
        if spec.is_derived:
            base_expr_str = spec.transform
        elif spec.optional and (original_name is None or original_name not in source_columns):
            base_expr_str = "NULL"
            if original_name: 
                logger.warning(f"Optional column '{original_name}' not found. Creating '{final_name}' with null values.")
        elif original_name not in source_columns:
            raise ConfigurationError(f"Required column '{original_name}' not found in source DataFrame.")
        else:
            base_expr_str = spec.transform or f"`{original_name}`"

        if spec.udf:
            base_expr_str = f"{spec.udf}({base_expr_str})"

        cast_func = "try_cast" if spec.try_cast else "cast"
        final_expr_str = f"{cast_func}({base_expr_str} AS {spec.type})"
        
        return expr(final_expr_str).alias(final_name)

    def _add_control_columns(self, df: DataFrame, config: PipelineConfig) -> DataFrame:
        """Adds framework control columns like hash_key and updated_at."""
        current_df = df
        pk_cols = [spec.rename for spec in config.columns if spec.pk]
        if pk_cols:
            logger.debug("Generating hash_key.")
            current_df = current_df.withColumn("hash_key", sha2(concat_ws("||", *sorted(pk_cols)), 256))

        if config.sink.scd and config.sink.scd.type == '2':
            track_cols = config.sink.scd.track_columns
            logger.debug("Generating data_hash for SCD Type 2.")
            current_df = current_df.withColumn("data_hash", sha2(concat_ws("||", *sorted(track_cols)), 256))

        current_df = current_df.withColumn("updated_at", current_timestamp())
        return current_df

    def _transform_silver(self, df: DataFrame, config: PipelineConfig) -> DataFrame:
        """Executes the transformation for Silver pipelines."""
        source_columns = df.columns
        select_exprs = [
            self._build_column_expression(spec, source_columns) for spec in config.columns
        ]

        logger.info("Applying column transformations for Silver pipeline...")
        transformed_df = df.select(*select_exprs)
        
        final_df = self._add_control_columns(transformed_df, config)
        
        logger.info("Silver transformations completed.")
        return final_df

    def _transform_gold(self, engine: BaseEngine, config: PipelineConfig) -> DataFrame:
        """Executes Gold transformation, applies type casting, and adds control columns."""
        logger.info("Executing Gold transformation from SQL.")
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
