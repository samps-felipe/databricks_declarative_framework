from abc import ABC, abstractmethod
from pyspark.sql import DataFrame
from ..models.pydantic_models import PipelineConfig

class BaseEngine(ABC):
    """
    Classe Abstrata que define o contrato para qualquer motor de processamento (Engine).
    Qualquer novo engine (ex: PandasEngine) deve implementar estes métodos.
    """
    @abstractmethod
    def read(self, config: PipelineConfig):
        """Reads data from the specified source."""
        pass

    @abstractmethod
    def process(self, df, config: PipelineConfig):
        """Processes the data using pipeline steps."""
        pass

    @abstractmethod
    def validate(self, df, config: PipelineConfig):
        """Applies data quality validations using the engine."""
        pass

    @abstractmethod
    def write(self, df, config: PipelineConfig, validation_log_df=None):
        """Writes the final DataFrame to the destination."""
        pass

    @abstractmethod
    def create_table(self, config: PipelineConfig):
        """Creates the table schema at the destination."""
        pass

    @abstractmethod
    def update_table(self, config: PipelineConfig):
        """Applies schema or metadata changes to an existing table."""
        pass

    @abstractmethod
    def read_table(self, table_name: str):
        """Reads a complete table for testing purposes."""
        pass

    @abstractmethod
    def compare_dataframes(self, df_actual, df_expected) -> bool:
        """Compares two DataFrames and returns True if they are identical."""
        pass

    @abstractmethod
    def show_differences(self, df_actual, df_expected):
        """Displays differences between two DataFrames."""
        pass

    @abstractmethod
    def execute_gold_transformation(self, config: PipelineConfig):
        """Executes transformation logic for a Gold pipeline."""
        pass