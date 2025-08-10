import sys
import yaml
from pyspark.sql import SparkSession
from framework.core.pipeline import Pipeline
from framework.engines.spark_engine import SparkEngine
from framework.models.pydantic_models import PipelineConfig

def get_engine(engine_name: str, spark: SparkSession):
    if engine_name == 'spark':
        return SparkEngine(spark)
    else:
        raise ValueError(f"Engine '{engine_name}' não suportado.")

def main():
    if len(sys.argv) < 3:
        print("Uso: python main.py <comando> <caminho_para_o_yaml>")
        print("Comandos disponíveis: run, create, update, test") # <-- Comando 'test' adicionado
        sys.exit(1)

    command = sys.argv[1]
    config_path = sys.argv[2]

    print(f"Carregando e validando configuração de: {config_path}")
    with open(config_path, 'r') as f:
        config_dict = yaml.safe_load(f)
    
    try:
        config = PipelineConfig(**config_dict)
    except Exception as e:
        print(f"Erro de validação no arquivo YAML: {e}")
        sys.exit(1)

    spark = SparkSession.builder.appName(config.pipeline_name).getOrCreate()
    engine = get_engine(config.engine, spark)
    
    pipeline = Pipeline(config, engine)

    if command == 'run':
        pipeline.run()
    elif command == 'create':
        pipeline.create()
    elif command == 'update':
        pipeline.update()
    elif command == 'test': # <-- Nova lógica para o comando 'test'
        pipeline.test()
    else:
        print(f"Comando '{command}' não reconhecido.")

if __name__ == "__main__":
    main()
