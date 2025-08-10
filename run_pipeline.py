# Databricks notebook source
# DBTITLE 1,Instalar Dependências
# Instala a biblioteca para ler arquivos YAML
%pip install pyyaml

# COMMAND ----------

# DBTITLE 1,Configuração do Widget
# O widget permite escolher qual pipeline executar de forma interativa ou via parâmetros de job.
# O caminho é relativo à raiz do repositório.
dbutils.widgets.text("pipeline_config", "pipelines/silver_contas.yaml", "Caminho do arquivo YAML do Pipeline")

# COMMAND ----------

# DBTITLE 1,Importações e Inicialização
import sys
from pyspark.sql import SparkSession
from src.framework.engine import PipelineEngine

# Obtém a Spark Session ativa no Databricks
spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

import pdb

# COMMAND ----------

# DBTITLE 1,Execução do Pipeline
# Pega o caminho do arquivo YAML a partir do widget
pipeline_config_path = dbutils.widgets.get("pipeline_config")
# Constrói o caminho absoluto dentro do Databricks Workspace
full_path = f"/Workspace/Users/sampaio.motion@gmail.com/databricks_declarative_framework/{pipeline_config_path}"

print(f"Iniciando a execução do pipeline definido em: {full_path}")

try:
    # Cria a instância do motor do pipeline
    engine = PipelineEngine(spark, full_path)
    
    # Executa o pipeline
    # pdb.set_trace() 
    engine.run()
    
    print("Execução finalizada com sucesso.")
    # Sinaliza sucesso para o Databricks Jobs
    dbutils.notebook.exit("SUCESSO")
except Exception as e:
    # Em caso de erro, o notebook irá falhar e reportar o erro
    dbutils.notebook.exit(f"FALHA: {e}")


# COMMAND ----------

engine.config
