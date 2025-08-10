#
# Arquivo: src/framework/dlt_generator.py
# NOVO ARQUIVO: Responsável por gerar código SQL para DLT.
#
def _parse_rule(rule_string: str):
    """Parseia uma string de regra como 'not_null' ou 'greater_than:10'."""
    parts = rule_string.split(':', 1)
    return parts[0], (parts[1] if len(parts) > 1 else None)

def generate_silver_dlt_sql(config: dict) -> str:
    """Gera um script SQL para uma tabela Silver em um pipeline DLT."""
    sink_config = config['sink']
    table_name = f"{sink_config['schema']}.{sink_config['table']}"
    
    source_config = config['source']
    source_path = source_config['path']
    source_format = source_config['options']['cloudFiles.format']
    source_options = ", ".join([f"'{k}', '{v}'" for k, v in source_config.get('options', {}).items()])

    # Constrói as transformações de coluna
    sql_transforms = []
    constraints = []
    for spec in config['columns']:
        new_name = spec['rename']
        transform_expr = spec.get('transform', f"`{spec['name']}`")
        
        # Casting de tipo
        cast_expr = f"CAST({transform_expr} AS {spec['type']})"
        if spec['type'] == 'date' and 'format' in spec:
            cast_expr = f"to_date({transform_expr}, '{spec['format']}')"
        sql_transforms.append(f"  {cast_expr} AS {new_name}")

        # Constrói as validações de qualidade (DLT Expectations)
        if 'validate' in spec:
            for rule_str in spec['validate']:
                constraint_name = f"{new_name}_{rule_str.replace(':', '_').replace('[','').replace(']','')}"
                rule, param = _parse_rule(rule_str)
                if rule == 'not_null':
                    constraints.append(f"CONSTRAINT {constraint_name} EXPECT ({new_name} IS NOT NULL) ON VIOLATION DROP ROW")
                elif rule == 'isin':
                    # Transforma a lista string em uma tupla SQL
                    sql_list = param.replace('[', '(').replace(']', ')')
                    constraints.append(f"CONSTRAINT {constraint_name} EXPECT ({new_name} IN {sql_list}) ON VIOLATION DROP ROW")
                elif rule == 'greater_than_or_equal_to':
                    constraints.append(f"CONSTRAINT {constraint_name} EXPECT ({new_name} >= {param}) ON VIOLATION DROP ROW")

    sql_transforms_str = ",\n".join(sql_transforms)
    constraints_str = "\n".join(constraints)

    # Monta o script SQL final
    return f"""-- DLT Pipeline gerado para: {config['pipeline_name']}
CREATE OR REFRESH STREAMING LIVE TABLE {table_name}
COMMENT "{config['description']}"
{constraints_str}
AS SELECT
{sql_transforms_str}
FROM cloud_files("{source_path}", "{source_format}", map({source_options}))
"""

def generate_gold_dlt_sql(config: dict) -> str:
    """Gera um script SQL para uma tabela Gold em um pipeline DLT."""
    sink_config = config['sink']
    table_name = f"{sink_config['schema']}.{sink_config['table']}"
    
    query = config['transformation']['sql']
    
    # Substitui os nomes das tabelas de dependência para a sintaxe DLT 'live.table_name'
    for dep in config['dependencies']:
        original_name = ".".join(dep.split('.')[1:]) # ex: silver.clientes
        dlt_name = f"live.{dep.split('.')[-1]}"      # ex: live.clientes
        query = query.replace(original_name, dlt_name)
        query = query.replace(dep, dlt_name) # Garante a substituição do nome completo também

    return f"""-- DLT Pipeline gerado para: {config['pipeline_name']}
CREATE OR REFRESH LIVE TABLE {table_name}
COMMENT "{config['description']}"
AS
{query}
"""