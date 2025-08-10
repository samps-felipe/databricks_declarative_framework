# Databricks notebook source

# DBTITLE 1,Setup do Ambiente de Teste
# Instala a dependência para ler os arquivos YAML
%pip install pyyaml pydantic

# COMMAND ----------

# DBTITLE 2,Importações e Configurações Iniciais
import yaml
import os
import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Adiciona o diretório do framework ao path para permitir as importações
# ATENÇÃO: Ajuste este caminho para o local do seu repositório no Databricks
framework_path = '/Workspace/Repos/seu_usuario/framework_declarativo_v2/src'
import sys
sys.path.append(framework_path)

from framework.core.pipeline import Pipeline
from framework.engines.spark_engine import SparkEngine
from framework.models.pydantic_models import PipelineConfig

# Configurações do ambiente de teste
spark = SparkSession.builder.appName("FrameworkTestSuite").getOrCreate()
dbutils.widgets.text("catalog", "dev", "Catálogo para Testes")
dbutils.widgets.text("schema", "framework_tests", "Schema para Testes")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
BASE_PATH = f"/tmp/framework_tests/{SCHEMA}"

# Cria o schema se não existir
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

print(f"Ambiente de teste configurado em: {CATALOG}.{SCHEMA}")
print(f"Arquivos temporários serão salvos em: {BASE_PATH}")

# Limpa o diretório de teste para garantir uma execução limpa
dbutils.fs.rm(BASE_PATH, recurse=True)
os.makedirs(f"/dbfs{BASE_PATH}/pipelines", exist_ok=True)
os.makedirs(f"/dbfs{BASE_PATH}/source_data", exist_ok=True)

# COMMAND ----------

# DBTITLE 3,Funções de Helper para os Testes
def create_yaml_file(config_dict, filename):
    """Cria um arquivo YAML no DBFS a partir de um dicionário Python."""
    path = f"/dbfs{BASE_PATH}/pipelines/{filename}"
    with open(path, 'w') as f:
        yaml.dump(config_dict, f, sort_keys=False)
    return f"{BASE_PATH}/pipelines/{filename}"

def create_source_csv(data, filename, columns):
    """Cria um arquivo CSV de teste no DBFS."""
    path = f"{BASE_PATH}/source_data/{filename}"
    df = spark.createDataFrame(data, columns)
    df.coalesce(1).write.mode("overwrite").option("header", "true").csv(path)
    generated_file = dbutils.fs.ls(path)[-1].path
    return generated_file.replace("dbfs:", "")

def run_pipeline_command(command, config_path):
    """Simula a execução do main.py para um comando específico."""
    with open(f"/dbfs{config_path}", 'r') as f:
        config_dict = yaml.safe_load(f)
    config = PipelineConfig(**config_dict)
    engine = SparkEngine(spark)
    pipeline = Pipeline(config, engine)
    
    if command == 'create':
        pipeline.create()
    elif command == 'run':
        pipeline.run()
    else:
        raise ValueError(f"Comando {command} não suportado no helper.")

def get_table_comment(table_name):
    """Busca o comentário de uma tabela no Unity Catalog."""
    try:
        result = spark.sql(f"DESCRIBE TABLE EXTENDED {table_name}").filter("col_name = 'Comment'").collect()
        return result[0]['data_type'] if result else ""
    except:
        return ""

def get_column_comment(table_name, column_name):
    """Busca o comentário de uma coluna no Unity Catalog."""
    try:
        result = spark.sql(f"DESCRIBE TABLE EXTENDED {table_name}").filter(f"col_name = '{column_name}'").collect()
        return result[0]['comment'] if result and result[0]['comment'] else ""
    except:
        return ""

# COMMAND ----------

# DBTITLE 4,Cenário 1: Teste Completo de Pipeline Silver (Validações, Comentários, Drop/Warn)
print("--- INICIANDO TESTE 1: PIPELINE SILVER COMPLETO ---")

# 1. Definir dados e configuração
# Dados incluem casos que devem ser dropados ou gerar warnings
silver_source_data = [
    ("1", "John Doe", "john.doe@test.com", "ACTIVE", "30"), # Válido
    ("2", "Jane Doe", "jane.doe@test.com", "INACTIVE", "25"), # Válido
    (None, "No ID", "noid@test.com", "ACTIVE", "40"), # Deve ser dropado (id nulo)
    ("4", "Bad Email", "bademail", "ACTIVE", "22"), # Deve gerar warning (email inválido)
    ("5", "Old Status", "old.status@test.com", "OLD", "50"), # Deve ser dropado (status inválido)
    ("6", "Too Young", "young@test.com", "ACTIVE", "17"), # Deve ser dropado (idade < 18)
]
silver_source_path = create_source_csv(silver_source_data, "silver_source.csv", ["id", "name", "email", "status", "age"])
validation_log_table = f"{CATALOG}.{SCHEMA}.validation_logs"

silver_config = {
    'pipeline_name': 'silver_customers',
    'pipeline_type': 'silver',
    'description': 'Tabela Silver de clientes para testes.',
    'validation_log_table': validation_log_table,
    'source': {
        'format': 'csv', 'path': silver_source_path, 'options': {'header': 'true'}
    },
    'sink': {
        'catalog': CATALOG, 'schema': SCHEMA, 'table': 'customers_silver', 'mode': 'overwrite'
    },
    'columns': [
        {'name': 'id', 'type': 'int', 'pk': True, 'description': 'ID Unico do Cliente.', 'validate': [{'rule': 'not_null', 'on_fail': 'drop'}]},
        {'name': 'name', 'type': 'string', 'description': 'Nome completo.'},
        {'name': 'email', 'type': 'string', 'description': 'Email de contato.', 'validate': [{'rule': 'pattern:^([a-zA-Z0-9_\\-\\.]+)@([a-zA-Z0-9_\\-\\.]+)\\.([a-zA-Z]{2,5})$', 'on_fail': 'warn'}]},
        {'name': 'status', 'type': 'string', 'transform': 'UPPER(status)', 'description': 'Status do cliente.', 'validate': [{'rule': "isin:['ACTIVE', 'INACTIVE']", 'on_fail': 'drop'}]},
        {'name': 'age', 'type': 'int', 'description': 'Idade do cliente.', 'validate': [{'rule': 'greater_than_or_equal_to:18', 'on_fail': 'drop'}]}
    ]
}
silver_config_path = create_yaml_file(silver_config, "silver_customers.yaml")

# 2. Executar o pipeline
run_pipeline_command('create', silver_config_path)
run_pipeline_command('run', silver_config_path)

# 3. Validar o resultado
try:
    result_df = spark.read.table(f"{CATALOG}.{SCHEMA}.customers_silver")
    # Valida contagem: 6 originais - 3 dropados = 3 (2 válidos + 1 com warning)
    assert result_df.count() == 3, f"Esperado 3 registros, mas obteve {result_df.count()}"
    
    # Valida comentários da tabela e colunas
    assert get_table_comment(f"{CATALOG}.{SCHEMA}.customers_silver") == 'Tabela Silver de clientes para testes.'
    assert get_column_comment(f"{CATALOG}.{SCHEMA}.customers_silver", 'id') == 'ID Unico do Cliente.'
    
    # Valida log de warning
    log_df = spark.read.table(validation_log_table)
    assert log_df.count() == 1, "A tabela de log de validação deveria ter 1 registro."
    assert log_df.first()['failed_column'] == 'email'

    print("✅ TESTE 1 PASSOU: Validações, comentários e ações de drop/warn funcionaram corretamente.")
except Exception as e:
    print(f"❌ TESTE 1 FALHOU: {e}")
    raise

# COMMAND ----------

# DBTITLE 5,Cenário 2: Teste de Pipeline Gold (sem alterações)
# Este cenário permanece o mesmo, pois sua funcionalidade não foi alterada.
print("\n--- INICIANDO TESTE 2: PIPELINE GOLD ---")
# ... (código do cenário 2)

# COMMAND ----------

# DBTITLE 6,Cenário 3: Teste de Pipeline SCD Tipo 2 (sem alterações)
# Este cenário permanece o mesmo, pois sua funcionalidade não foi alterada.
print("\n--- INICIANDO TESTE 3: PIPELINE SCD TIPO 2 ---")
# ... (código do cenário 3)

# COMMAND ----------

# DBTITLE 7,Cenário 4: Teste de Validação com Ação 'fail'
print("\n--- INICIANDO TESTE 4: VALIDAÇÃO COM 'FAIL' ---")

# 1. Definir dados e configuração
fail_source_data = [("1", "User A"), ("1", "User B")] # IDs duplicados
fail_source_path = create_source_csv(fail_source_data, "fail_source.csv", ["id", "name"])

fail_config = {
    'pipeline_name': 'silver_fail_test',
    'pipeline_type': 'silver',
    'source': {'format': 'csv', 'path': fail_source_path, 'options': {'header': 'true'}},
    'sink': {'catalog': CATALOG, 'schema': SCHEMA, 'table': 'fail_test_table', 'mode': 'overwrite'},
    'columns': [{'name': 'id', 'type': 'int'}, {'name': 'name', 'type': 'string'}],
    'table_validations': [{'type': 'duplicate_check', 'columns': ['id'], 'on_fail': 'fail'}]
}
fail_config_path = create_yaml_file(fail_config, "fail_test.yaml")

# 2. Executar e esperar uma exceção
try:
    run_pipeline_command('create', fail_config_path)
    run_pipeline_command('run', fail_config_path)
    # Se chegar aqui, o teste falhou porque a exceção não foi lançada
    raise AssertionError("❌ TESTE 4 FALHOU: A exceção de validação não foi lançada.")
except Exception as e:
    if "Verificação de duplicatas falhou" in str(e):
        print("✅ TESTE 4 PASSOU: Pipeline falhou como esperado na validação de duplicatas.")
    else:
        print(f"❌ TESTE 4 FALHOU: Uma exceção inesperada foi lançada: {e}")
        raise

# COMMAND ----------

# DBTITLE 8,Limpeza do Ambiente de Teste (Opcional)
# Descomente e execute esta célula para remover as tabelas e arquivos criados durante o teste.
# print("--- LIMPANDO AMBIENTE DE TESTE ---")
# spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.customers_silver")
# spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.customer_domains_gold")
# spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.employees_scd2")
# spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.fail_test_table")
# spark.sql(f"DROP TABLE IF EXISTS {validation_log_table}")
# dbutils.fs.rm(BASE_PATH, recurse=True)
# print("Ambiente limpo.")
