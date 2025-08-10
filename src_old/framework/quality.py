from pyspark.sql import DataFrame
from pyspark.sql.functions import col

def _parse_rule(rule_string: str) -> tuple:
    """
    Parseia uma string de regra para separar o nome da restrição e seu parâmetro.
    Exemplos:
        - "not_null" -> ("not_null", None)
        - "isin:['A', 'B']" -> ("isin", "['A', 'B']")
        - "greater_than_or_equal_to:0" -> ("greater_than_or_equal_to", "0")

    Args:
        rule_string (str): A regra como definida no arquivo YAML.

    Returns:
        tuple: Uma tupla contendo o nome da restrição e o parâmetro (ou None).
    """
    parts = rule_string.split(':', 1)
    constraint = parts[0].strip()
    param = parts[1].strip() if len(parts) > 1 else None
    return constraint, param

def apply_rules(df: DataFrame, column_spec: dict, on_fail: str = 'drop') -> DataFrame:
    """
    Aplica um conjunto de regras de validação a uma coluna específica de um DataFrame.

    Args:
        df (DataFrame): O DataFrame a ser validado.
        column_spec (dict): A especificação completa da coluna, vinda do YAML.
        on_fail (str): Ação a ser tomada em caso de falha na validação. 
                       Atualmente, apenas 'drop' (remover a linha) é implementado.

    Returns:
        DataFrame: O DataFrame após a aplicação das regras de validação.
    """
    column_name = column_spec['rename']
    validated_df = df

    # Itera sobre cada regra de validação definida para a coluna
    for rule_str in column_spec.get('validate', []):
        constraint, param = _parse_rule(rule_str)
        
        condition = None
        
        # Constrói a condição do Spark com base na regra
        if constraint == "not_null":
            condition = col(column_name).isNotNull()
        elif constraint == "isin":
            # Cuidado: eval pode ser um risco de segurança. 
            # Use apenas com fontes de configuração confiáveis.
            try:
                allowed_values = eval(param)
                if not isinstance(allowed_values, list):
                    raise TypeError("O parâmetro para 'isin' deve ser uma lista.")
                condition = col(column_name).isin(allowed_values)
            except Exception as e:
                print(f"  [AVISO] Erro ao parsear a regra '{rule_str}' para a coluna '{column_name}': {e}. Pulando regra.")
                continue
        elif constraint == "greater_than_or_equal_to":
            condition = col(column_name) >= float(param)
        else:
            print(f"  [AVISO] Constraint '{constraint}' não reconhecida para a coluna '{column_name}'. Pulando regra.")
            continue
        
        # Se uma condição válida foi criada, aplica-a
        if condition is not None:
            # Conta quantos registros violam a regra antes de aplicar o filtro
            invalid_count = validated_df.filter(~condition).count()
            
            if invalid_count > 0:
                print(f"  [VALIDAÇÃO] Falha na regra '{rule_str}' para a coluna '{column_name}'. {invalid_count} registros inválidos encontrados.")
                if on_fail == 'drop':
                    validated_df = validated_df.filter(condition)
                    print(f"  [VALIDAÇÃO] {invalid_count} registros inválidos foram removidos.")
                # Outras ações como 'fail' ou 'log' poderiam ser implementadas aqui
    
    return validated_df