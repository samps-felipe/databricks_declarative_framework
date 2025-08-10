from pyspark.sql import DataFrame
from ..core.step import BaseStep
from ..core.engine import BaseEngine
from ..models.pydantic_models import PipelineConfig

class WriterStep(BaseStep):
    """Passo responsável por escrever os dados no destino."""
    def execute(self, df: DataFrame, engine: BaseEngine, config: PipelineConfig, validation_log_df: DataFrame = None):
        print("--- Passo: Escrita ---")
        engine.write(df, config, validation_log_df)
        print("Escrita no destino concluída.")
        # Retornar o DataFrame não é estritamente necessário, mas pode ser útil
        return df