from pyspark.sql import DataFrame
from pyspark.sql.functions import col, expr, sha2, concat_ws, current_timestamp, lit
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
        if config.pipeline_type == 'silver':
            return self._transform_silver(df, engine, config)
        elif config.pipeline_type == 'gold':
            return self._transform_gold(engine, config)
        else:
            raise NotImplementedError(f"Transformação para o tipo '{config.pipeline_type}' não implementada.")

    def _transform_silver(self, df: DataFrame, engine: BaseEngine, config: PipelineConfig) -> DataFrame:
        """
        Executa a transformação para pipelines Silver.
        Constrói uma lista de expressões e as executa em um único select para eficiência.
        """
        select_exprs = []

        for spec in config.columns:
            final_name = spec.rename
            original_name = spec.name
            
            # Passo 1: Define a expressão base como uma string
            base_expr_str = ""
            if spec.is_derived:
                base_expr_str = spec.transform
            elif spec.optional and (original_name is None or original_name not in df.columns):
                # Para colunas opcionais ausentes, a base é NULL
                base_expr_str = "NULL"
                if original_name: print(f"  -> Coluna opcional '{original_name}' não encontrada. Criando '{final_name}' com valores nulos.")
            elif original_name not in df.columns:
                raise ValueError(f"Coluna obrigatória '{original_name}' não encontrada no DataFrame de origem.")
            else:
                # Para colunas existentes, usa a transformação ou o nome original
                base_expr_str = spec.transform or f"`{original_name}`"

            # Passo 2: Envolve com a UDF, se especificada
            if spec.udf:
                base_expr_str = f"{spec.udf}({base_expr_str})"

            # Passo 3: Envolve com a função de casting
            cast_func = "try_cast" if spec.try_cast else "cast"
            final_expr_str = f"{cast_func}({base_expr_str} AS {spec.type})"
            
            # Passo 4: Adiciona a expressão final à lista com o alias correto
            select_exprs.append(expr(final_expr_str).alias(final_name))

        # Executa todas as transformações de uma vez
        current_df = df.select(*select_exprs)
        
        # Adiciona as colunas de controle do framework
        pk_cols = [spec.rename for spec in config.columns if spec.pk]
        if pk_cols:
            current_df = current_df.withColumn("hash_key", sha2(concat_ws("||", *sorted(pk_cols)), 256))

        if config.sink.scd and config.sink.scd.type == '2':
            track_cols = config.sink.scd.track_columns
            current_df = current_df.withColumn("data_hash", sha2(concat_ws("||", *sorted(track_cols)), 256))

        current_df = current_df.withColumn("updated_at", current_timestamp())
        
        return current_df

    def _transform_gold(self, engine: BaseEngine, config: PipelineConfig) -> DataFrame:
        """Executa a transformação Gold, aplica o casting de tipos e adiciona as colunas de controle."""
        current_df = engine.execute_gold_transformation(config)
        
        # Aplica o casting de tipos definido no YAML para garantir o schema correto.
        for spec in config.columns:
            final_name = spec.rename
            if final_name in current_df.columns:
                cast_func = "try_cast" if spec.try_cast else "cast"
                current_df = current_df.withColumn(final_name, expr(f"{cast_func}({final_name} AS {spec.type})"))

        # Adiciona as colunas de controle do framework
        pk_cols = [spec.rename for spec in config.columns if spec.pk]
        if pk_cols:
            current_df = current_df.withColumn("hash_key", sha2(concat_ws("||", *sorted(pk_cols)), 256))

        current_df = current_df.withColumn("updated_at", current_timestamp())
        current_df = current_df.withColumn("created_at", col("updated_at"))
        
        return current_df