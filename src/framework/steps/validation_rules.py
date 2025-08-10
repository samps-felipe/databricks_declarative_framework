from abc import ABC, abstractmethod
from pyspark.sql import DataFrame, functions as F
from typing import Tuple
import ast # Usado para converter strings em listas de forma segura

# --- Classes Abstratas para Definir o Contrato de Validação ---

class BaseValidation(ABC):
    """Classe base para todas as validações de coluna."""
    @abstractmethod
    def apply(self, df: DataFrame, column_name: str) -> Tuple[DataFrame, DataFrame]:
        """
        Aplica a regra de validação.
        Retorna:
            - DataFrame com os registros que falharam na validação.
            - DataFrame com os registros que passaram na validação.
        """
        pass

class BaseTableValidation(ABC):
    """Classe base para todas as validações de tabela."""
    @abstractmethod
    def apply(self, df: DataFrame):
        """
        Aplica a regra de validação na tabela inteira.
        Pode retornar nada e lançar uma exceção em caso de falha.
        """
        pass

# --- Implementações de Validações de Coluna ---

class NotNullValidation(BaseValidation):
    def apply(self, df: DataFrame, column_name: str) -> Tuple[DataFrame, DataFrame]:
        condition = F.col(column_name).isNotNull()
        failures_df = df.filter(~condition)
        success_df = df.filter(condition)
        return failures_df, success_df

class PatternValidation(BaseValidation):
    def __init__(self, pattern: str):
        self.pattern = pattern

    def apply(self, df: DataFrame, column_name: str) -> Tuple[DataFrame, DataFrame]:
        condition = F.col(column_name).rlike(self.pattern)
        failures_df = df.filter(~condition)
        success_df = df.filter(condition)
        return failures_df, success_df

class IsInValidation(BaseValidation):
    """Valida se o valor da coluna está em uma lista de valores permitidos."""
    def __init__(self, allowed_values_str: str):
        try:
            # Usa ast.literal_eval para converter a string '[]' em uma lista Python de forma segura
            self.allowed_values = ast.literal_eval(allowed_values_str)
            if not isinstance(self.allowed_values, list):
                raise TypeError
        except (ValueError, SyntaxError, TypeError):
            raise ValueError(f"Parâmetro inválido para a regra 'isin'. Esperado um formato de lista, ex: ['A', 'B']. Recebido: {allowed_values_str}")

    def apply(self, df: DataFrame, column_name: str) -> Tuple[DataFrame, DataFrame]:
        condition = F.col(column_name).isin(self.allowed_values)
        failures_df = df.filter(~condition)
        success_df = df.filter(condition)
        return failures_df, success_df

class GreaterThanOrEqualToValidation(BaseValidation):
    """Valida se o valor da coluna é maior ou igual a um valor numérico."""
    def __init__(self, value_str: str):
        try:
            self.value = float(value_str)
        except ValueError:
            raise ValueError(f"Parâmetro inválido para 'greater_than_or_equal_to'. Esperado um número. Recebido: {value_str}")

    def apply(self, df: DataFrame, column_name: str) -> Tuple[DataFrame, DataFrame]:
        condition = F.col(column_name) >= self.value
        failures_df = df.filter(~condition)
        success_df = df.filter(condition)
        return failures_df, success_df

class IsBetweenValidation(BaseValidation):
    """Valida se o valor da coluna está entre um limite inferior e superior."""
    def __init__(self, bounds_str: str):
        try:
            bounds = ast.literal_eval(bounds_str)
            if not isinstance(bounds, list) or len(bounds) != 2:
                raise TypeError
            self.lower_bound = float(bounds[0])
            self.upper_bound = float(bounds[1])
        except (ValueError, SyntaxError, TypeError):
            raise ValueError(f"Parâmetro inválido para 'isbetween'. Esperado uma lista com dois números, ex: [0, 100]. Recebido: {bounds_str}")

    def apply(self, df: DataFrame, column_name: str) -> Tuple[DataFrame, DataFrame]:
        condition = F.col(column_name).between(self.lower_bound, self.upper_bound)
        failures_df = df.filter(~condition)
        success_df = df.filter(condition)
        return failures_df, success_df

# --- Implementações de Validações de Tabela ---

class DuplicateCheckValidation(BaseTableValidation):
    def __init__(self, columns: list):
        self.columns = columns

    def apply(self, df: DataFrame):
        duplicate_count = df.groupBy(*self.columns).count().filter("count > 1").count()
        if duplicate_count > 0:
            # A lógica de 'fail' ou 'warn' será tratada pelo orquestrador (validator.py)
            raise ValueError(f"Verificação de duplicatas falhou para as colunas: {self.columns}. {duplicate_count} duplicatas encontradas.")