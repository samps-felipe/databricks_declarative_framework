#
# Arquivo: src/framework/steps_handler.py
#
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, expr, to_date
from . import quality

def _apply_rename_pattern(name: str) -> str:
    """Converte um nome de coluna para o formato snake_case."""
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    s2 = re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1)
    return s2.replace(" ", "_").lower()


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

def handle_silver_transformation(df: DataFrame, config: dict) -> DataFrame:
    """
    Processa um DataFrame para a camada Silver.
    
    Esta função orquestra uma sequência de operações:
    1. Renomeia colunas conforme especificado.
    2. Aplica transformações SQL customizadas.
    3. Converte os tipos de dados (casting), com tratamento especial para datas.
    4. Aplica as regras de validação de qualidade de dados, chamando o módulo 'quality'.
    5. Seleciona e reordena as colunas para o schema final.

    Args:
        df (DataFrame): O DataFrame de entrada, vindo da fonte de dados.
        config (dict): Dicionario das configuracoes da base.

    Returns:
        DataFrame: O DataFrame processado, limpo e validado.
    """
    print("  -> Iniciando transformações da camada Silver...")
    current_df = df
    
    # Pega as configurações globais do YAML, com valores padrão seguros
    defaults = config.get("defaults", {})
    global_date_format = defaults.get("date_format")
    rename_pattern = defaults.get("column_rename_pattern", "none")

    final_columns_after_rename = {} # Mapeia {nome_original: nome_final}

    # Primeira passada: Renomear, transformar e tipar colunas
    for spec in config["columns"]:
        original_name = spec['name']
        
        # Lógica de renomeação:
        # 1. Usa o 'rename' manual se existir.
        # 2. Se não, aplica o padrão global 'snake_case'.
        # 3. Se não, mantém o nome original.
        if "rename" in spec:
            final_name = spec["rename"]
        elif rename_pattern == "snake_case":
            final_name = _apply_rename_pattern(original_name)
        else:
            final_name = original_name
        
        final_columns_after_rename[original_name] = final_name
        
        # Aplica a transformação SQL usando o nome original da coluna
        # É importante usar o nome original aqui, pois o DataFrame ainda não foi renomeado.
        transform_expr = spec.get('transform', f"`{original_name}`")
        current_df = current_df.withColumn(final_name, expr(transform_expr))

        # Lógica de casting de tipo:
        # 1. Usa o 'format' da coluna se existir.
        # 2. Se não, usa o 'date_format' global.
        target_type = spec['type']
        column_date_format = spec.get('format')
        
        if target_type == 'date':
            date_format_to_use = column_date_format or global_date_format
            if not date_format_to_use:
                raise ValueError(f"Nenhum formato de data especificado para a coluna '{original_name}' e nenhum padrão global foi definido.")
            current_df = current_df.withColumn(final_name, to_date(col(final_name), date_format_to_use))
        else:
            current_df = current_df.withColumn(final_name, col(final_name).cast(target_type))

    # Seleciona e renomeia as colunas para o resultado final
    final_selection = [col(v).alias(v) for k, v in final_columns_after_rename.items()]
    current_df = current_df.select(*final_selection)

    # Segunda passada: Aplicar validações de qualidade de dados
    print("  -> Aplicando regras de qualidade de dados...")
    for spec in config["columns"]:
        if 'validate' in spec and spec.get('validate'):
            # Cria uma cópia da spec para passar para a função de validação,
            # garantindo que ela use o nome final da coluna.
            validation_spec = spec.copy()
            validation_spec['rename'] = final_columns_after_rename[spec['name']]
            current_df = quality.apply_rules(current_df, validation_spec)
            
    return current_df

def handle_gold_transformation(spark: SparkSession, config: dict) -> DataFrame:
    """
    Processa uma transformação para a camada Gold.

    Esta função executa uma lógica de agregação ou junção, geralmente em SQL:
    1. Lê as tabelas da camada Silver listadas como dependências.
    2. Registra cada dependência como uma view temporária para que possam ser usadas no SQL.
    3. Executa a consulta SQL principal definida no arquivo de configuração.

    Args:
        spark (SparkSession): A sessão Spark ativa.
        config (dict): A configuração completa do pipeline Gold.

    Returns:
        DataFrame: O DataFrame resultante da transformação Gold.
    """
    print("  -> Lendo dependências da camada Silver...")
    # Registra as tabelas de dependência como views temporárias
    for dep in config.get('dependencies', []):
        table_name = dep.split('.')[-1]
        spark.read.table(dep).createOrReplaceTempView(table_name)
        print(f"  -> Dependência registrada: Tabela '{dep}' como view SQL '{table_name}'")

    transform_config = config['transformation']
    if transform_config['type'] == 'sql':
        print("  -> Executando transformação SQL da camada Gold.")
        return spark.sql(transform_config['sql'])
    elif transform_config['type'] == 'python_file':
        # Esta seção pode ser estendida para suportar transformações em Python
        raise NotImplementedError("Transformação do tipo 'python_file' ainda não implementada.")
    else:
        raise ValueError(f"Tipo de transformação desconhecido para a camada Gold: {transform_config['type']}")
