from abc import ABC, abstractmethod
from pyspark.sql import DataFrame
from ..models.pydantic_models import PipelineConfig

class BaseEngine(ABC):
    """
    Classe Abstrata que define o contrato para qualquer motor de processamento (Engine).
    Qualquer novo engine (ex: PandasEngine) deve implementar estes métodos.
    """
    @abstractmethod
    def read(self, config: PipelineConfig) -> DataFrame:
        """Lê os dados da fonte especificada."""
        pass

    @abstractmethod
    def write(self, df: DataFrame, config: PipelineConfig, validation_log_df: DataFrame = None):
        """Escreve o DataFrame final no destino."""
        pass

    @abstractmethod
    def create_table(self, config: PipelineConfig):
        """Cria a estrutura da tabela (schema) no destino."""
        pass

    @abstractmethod
    def update_table(self, config: PipelineConfig):
        """Aplica alterações de schema ou metadados em uma tabela existente."""
        pass

    @abstractmethod
    def read_table(self, table_name: str) -> DataFrame:
        """Lê uma tabela completa para uso em testes."""
        pass

    @abstractmethod
    def compare_dataframes(self, df_actual: DataFrame, df_expected: DataFrame) -> bool:
        """Compara dois DataFrames e retorna True se forem idênticos."""
        pass

    @abstractmethod
    def show_differences(self, df_actual: DataFrame, df_expected: DataFrame):
        """Exibe as diferenças entre dois DataFrames."""
        pass

    @abstractmethod
    def execute_gold_transformation(self, config: PipelineConfig) -> DataFrame:
        """Executa a lógica de transformação para um pipeline Gold."""
        pass