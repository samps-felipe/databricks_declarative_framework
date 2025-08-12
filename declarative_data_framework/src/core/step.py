from abc import ABC, abstractmethod
from pyspark.sql import DataFrame
from .engine import BaseEngine
from ..models.pydantic_models import PipelineConfig

class BaseStep(ABC):
    """
    Classe Abstrata que define o contrato para qualquer passo do pipeline.
    """
    @abstractmethod
    def execute(self, *args, **kwargs) -> DataFrame:
        """
        Executa a lógica do passo.
        A assinatura pode variar, mas geralmente recebe um DataFrame e retorna outro.
        """
        pass