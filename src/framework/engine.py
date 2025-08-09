import yaml
import os
from pyspark.sql import SparkSession, DataFrame
from . import steps_handler, dlt_generator

class PipelineEngine:
    """
    Motor principal do framework declarativo.
    Lê uma configuração YAML e orquestra a execução de um pipeline
    usando o motor Spark ou gerando um script para Delta Live Tables (DLT).
    """
    def __init__(self, spark: SparkSession, config_path: str):
        """
        Inicializa o motor do pipeline.

        Args:
            spark (SparkSession): A sessão Spark ativa.
            config_path (str): O caminho para o arquivo de configuração YAML do pipeline.
        """
        self.spark = spark
        self.config_path = config_path
        self.config = self._load_config(config_path)
        self.pipeline_type = self.config['pipeline_type']
        self.engine_type = self.config.get('engine', 'spark') # 'spark' é o padrão
        print(f"Motor iniciado para o pipeline '{self.config['pipeline_name']}' (Tipo: {self.pipeline_type}, Motor: {self.engine_type}).")

    def _load_config(self, config_path: str) -> dict:
        """Carrega e parseia o arquivo de configuração YAML."""
        with open(config_path, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)

    def run(self):
        """
        Ponto de entrada principal.
        Executa o pipeline no modo 'spark' ou gera o script para o modo 'dlt'.
        """
        if self.engine_type == 'dlt':
            print("Modo DLT detectado. Gerando script de pipeline...")
            self.generate_dlt_script()
            return

        # Verifica se há um script customizado para o motor Spark
        if self.config.get('custom_script_override'):
            print(f"Pipeline será executado por script customizado: {self.config['custom_script_override']}")
            print("[AVISO] A execução de script customizado deve ser implementada no orquestrador (ex: dbutils.notebook.run).")
            return

        # Execução padrão com motor Spark
        try:
            processed_df = self._process_spark()
            self._load_spark(processed_df)
            print("\nPipeline Spark concluído com sucesso!")
        except Exception as e:
            print(f"\n[ERRO] O pipeline Spark falhou: {e}")
            raise

    def generate_dlt_script(self):
        """
        Gera e salva um script SQL para ser usado em um pipeline DLT.
        O script é gerado com base na configuração YAML.
        """
        if self.pipeline_type == 'silver':
            script_content = dlt_generator.generate_silver_dlt_sql(self.config)
        elif self.pipeline_type == 'gold':
            script_content = dlt_generator.generate_gold_dlt_sql(self.config)
        else:
            raise ValueError(f"Geração DLT não suportada para o tipo de pipeline: {self.pipeline_type}")

        # Define o nome e o caminho do arquivo de saída
        output_filename = f"{self.config['pipeline_name']}.sql"
        # Garante que o script seja salvo em uma pasta relativa ao repositório
        repo_root = self.config_path.split('/pipelines/')[0]
        output_path = os.path.join(repo_root, 'dlt_generated', output_filename)
        
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(script_content)
        
        print("\nScript DLT gerado com sucesso!")
        print(f"Caminho do arquivo: {output_path}")
        print("\nPróximos passos:")
        print("1. No Databricks, vá para 'Workflows' > 'Delta Live Tables' e crie um novo Pipeline.")
        print(f"2. Em 'Paths', aponte para o script gerado: '{output_path}'.")
        print(f"3. Em 'Target', defina o schema de destino (ex: '{self.config['sink']['schema']}_dlt').")
        print("4. Configure e inicie o pipeline.")

    # --- Métodos específicos do motor Spark ---

    def _ingest_spark(self) -> DataFrame:
        """Lógica de ingestão para o motor Spark."""
        source_config = self.config['source']
        reader = self.spark.read
        
        # Para consistência, tratamos 'cloudFiles' como uma leitura de batch aqui
        if source_config['format'] == 'cloudFiles':
            print("Lendo com Auto Loader em modo batch (para motor Spark).")
            return self.spark.read.format("csv").options(**source_config['options']).load(source_config['path'])
        
        return reader.format(source_config['format']).options(**source_config.get('options', {})).load(source_config['path'])

    def _process_spark(self) -> DataFrame:
        """Orquestra o processamento para o motor Spark."""
        if self.pipeline_type == 'silver':
            df = self._ingest_spark()
            return steps_handler.handle_silver_transformation(df, self.config['columns'])
        elif self.pipeline_type == 'gold':
            return steps_handler.handle_gold_transformation(self.spark, self.config)
        
        raise ValueError(f"Tipo de pipeline desconhecido para o motor Spark: '{self.pipeline_type}'")

    def _load_spark(self, df: DataFrame):
        """Lógica de carregamento (sink) para o motor Spark."""
        sink_config = self.config['sink']
        target_table = f"{sink_config['catalog']}.{sink_config['schema']}.{sink_config['table']}"
        
        writer = df.write.mode(sink_config['mode'])
        
        if sink_config.get('zorder_by'):
            # A otimização Z-Order é aplicada após a escrita da tabela
            writer.saveAsTable(target_table)
            zorder_cols = ",".join(sink_config['zorder_by'])
            print(f"Otimizando a tabela {target_table} com Z-ORDER BY ({zorder_cols})...")
            self.spark.sql(f"OPTIMIZE {target_table} ZORDER BY ({zorder_cols})")
        else:
            writer.saveAsTable(target_table)