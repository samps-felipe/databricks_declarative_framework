#
# Arquivo: src/framework/steps_handler.py
#
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, expr
from . import quality

def handle_rename_columns(df: DataFrame, renames: dict) -> DataFrame:
    """Renomeia as colunas do DataFrame com base em um dicionário."""
    print("  -> Executando: handle_rename_columns")
    for old_name, new_name in renames.items():
        if old_name in df.columns:
            df = df.withColumnRenamed(old_name, new_name)
    return df

def handle_cast_columns(df: DataFrame, schema: dict) -> DataFrame:
    """Converte o tipo das colunas do DataFrame."""
    print("  -> Executando: handle_cast_columns")
    for column_name, column_type in schema.items():
        if column_name in df.columns:
            df = df.withColumn(column_name, col(column_name).cast(column_type))
    return df

def handle_add_columns(df: DataFrame, columns: dict) -> DataFrame:
    """Adiciona novas colunas ao DataFrame usando expressões SQL."""
    print("  -> Executando: handle_add_columns")
    for column_name, sql_expression in columns.items():
        df = df.withColumn(column_name, expr(sql_expression))
    return df

def handle_validate_data(df: DataFrame, rules: list, on_fail: str) -> DataFrame:
    """Aplica um conjunto de regras de qualidade de dados."""
    print(f"  -> Executando: handle_validate_data (modo: {on_fail})")
    return quality.apply_rules(df, rules, on_fail)

def handle_select_columns(df: DataFrame, columns: list) -> DataFrame:
    """Seleciona um subconjunto de colunas do DataFrame."""
    print("  -> Executando: handle_select_columns")
    return df.select(*columns)

# Mapeia o 'step_type' do YAML para a função Python correspondente
STEP_HANDLERS = {
    "rename_columns": handle_rename_columns,
    "cast_columns": handle_cast_columns,
    "add_columns": handle_add_columns,
    "validate_data": handle_validate_data,
    "select_columns": handle_select_columns,
}