from pyspark.sql import DataFrame
from ..core.step import BaseStep
from ..core.engine import BaseEngine
from ..models.pydantic_models import PipelineConfig

class ReadStep(BaseStep):
    """Passo responsável por ler os dados da fonte."""
    def execute(self, engine: BaseEngine, config: PipelineConfig) -> DataFrame:
        print("--- Passo: Leitura ---")
        df = engine.read(config)
        
        # Validação da presença de colunas (garantir que o delimitador está correto)
        source_config = config.source
        if source_config and source_config.expected_columns:
            num_actual_columns = len(df.columns)
            if num_actual_columns != source_config.expected_columns:
                delimiter = source_config.options.get('delimiter', '[NÃO ESPECIFICADO]')
                raise ValueError(
                    f"Validação de schema falhou! O arquivo foi lido com {num_actual_columns} colunas, "
                    f"mas eram esperadas {source_config.expected_columns}. "
                    f"Verifique se o delimitador ('{delimiter}') está correto."
                )
        print("Leitura e validação de schema inicial concluídas.")
        return df
