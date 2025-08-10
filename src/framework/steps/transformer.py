from pyspark.sql import DataFrame
from pyspark.sql.functions import col, expr, sha2, concat_ws, current_timestamp
from ..core.step import BaseStep
from ..core.engine import BaseEngine
from ..models.pydantic_models import PipelineConfig
import re

def _apply_rename_pattern(name: str) -> str:
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    s2 = re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1)
    return s2.replace(" ", "_").lower()

class TransformStep(BaseStep):
    def execute(self, df: DataFrame, engine: BaseEngine, config: PipelineConfig) -> DataFrame:
        current_df = df
        defaults = config.defaults
        
        # Renomeação e Casting
        for spec in config.columns:
            original_name = spec.name
            final_name = spec.rename or (_apply_rename_pattern(original_name) if defaults.column_rename_pattern == 'snake_case' else original_name)
            
            # Aplica transformação se houver
            transform_expr = spec.transform or f"`{original_name}`"
            current_df = current_df.withColumn(final_name, expr(transform_expr))

            # Aplica casting (normal ou try_cast)
            cast_func = "try_cast" if spec.try_cast else "cast"
            current_df = current_df.withColumn(final_name, expr(f"{cast_func}({final_name} AS {spec.type})"))

        # Seleciona apenas as colunas renomeadas e transformadas
        final_columns = [spec.rename or (_apply_rename_pattern(spec.name) if defaults.column_rename_pattern == 'snake_case' else spec.name) for spec in config.columns]
        current_df = current_df.select(*final_columns)

        # Adiciona colunas internas
        pk_cols = [spec.rename or spec.name for spec in config.columns if spec.pk]
        if pk_cols:
            current_df = current_df.withColumn("hash_key", sha2(concat_ws("||", *pk_cols), 256))

        if config.sink.scd and config.sink.scd.type == '2':
            # Cria um hash dos dados das colunas monitoradas para detectar mudanças
            track_cols = config.sink.scd.track_columns
            current_df = current_df.withColumn("data_hash", sha2(concat_ws("||", *sorted(track_cols)), 256))


        current_df = current_df.withColumn("updated_at", current_timestamp())
        # 'created_at' será adicionado na lógica de merge/insert
        
        return current_df