# Databricks notebook source

# DBTITLE 1,Instalar Dependências
# Instala a biblioteca para ler arquivos YAML
%pip install pyyaml

# COMMAND ----------

# DBTITLE 2,Configuração do Widget
# O widget permite escolher qual pipeline executar de forma interativa ou via parâmetros de job.
# O caminho é relativo à raiz do repositório.
dbutils.widgets.text("pipeline_config", "pipelines/silver_contas.yaml", "Caminho do arquivo YAML do Pipeline")

# COMMAND ----------

# DBTITLE 3,Importações e Inicialização
import sys
from pyspark.sql import SparkSession

# Adiciona o diretório 'src' ao path para que possamos importar os módulos do framework.
# Ajuste o caminho conforme o nome do seu usuário/repo.
# Exemplo: /Workspace/Repos/seu_email@empresa.com/framework_declarativo/src
sys.path.append('/Workspace/Repos/seu_usuario/framework_declarativo/src')

from framework.engine import PipelineEngine

# Obtém a Spark Session ativa no Databricks
spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# DBTITLE 4,Execução do Pipeline
# Pega o caminho do arquivo YAML a partir do widget
pipeline_config_path = dbutils.widgets.get("pipeline_config")
# Constrói o caminho absoluto dentro do Databricks Workspace
full_path = f"/Workspace/Repos/seu_usuario/framework_declarativo/{pipeline_config_path}"

print(f"Iniciando a execução do pipeline definido em: {full_path}")

try:
    # Cria a instância do motor do pipeline
    engine = PipelineEngine(spark, full_path)
    
    # Executa o pipeline
    engine.run()
    
    print("Execução finalizada com sucesso.")
    # Sinaliza sucesso para o Databricks Jobs
    dbutils.notebook.exit("SUCESSO")
except Exception as e:
    # Em caso de erro, o notebook irá falhar e reportar o erro
    dbutils.notebook.exit(f"FALHA: {e}")

