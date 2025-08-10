from .engine import BaseEngine
from ..models.pydantic_models import PipelineConfig
from ..steps.reader import ReadStep
from ..steps.transformer import TransformStep
from ..steps.validator import ValidateStep
from ..steps.writer import WriterStep
from importlib import import_module

class Pipeline:
    def __init__(self, config: PipelineConfig, engine: BaseEngine):
        self.config = config
        self.engine = engine

    def run(self):
        print(f"--- Iniciando execução do pipeline: {self.config.pipeline_name} ---")
        reader = ReadStep()
        transformer = TransformStep()
        validator = ValidateStep()
        writer = WriterStep()

        df_transformed = None

        try:
            if self.config.pipeline_type == 'silver':
                df_source = reader.execute(engine=self.engine, config=self.config)
            else: # Gold
                df_source = None # Gold não lê de arquivos
        except Exception as e:
            raise Exception(f"Falha no passo de LEITURA do pipeline '{self.config.pipeline_name}': {e}") from e
        
        try:
            if self.config.custom_transform_script:
                print(f"Executando script de transformação customizado: {self.config.custom_transform_script}")
                module_path, func_name = self.config.custom_transform_script.rsplit('.', 1)
                custom_module = import_module(module_path)
                custom_transform_func = getattr(custom_module, func_name)
                df_transformed = custom_transform_func(df_source, self.engine, self.config)
            else:
                df_transformed = transformer.execute(df_source, engine=self.engine, config=self.config)
        except Exception as e:
            raise Exception(f"Falha no passo de TRANSFORMAÇÃO do pipeline '{self.config.pipeline_name}': {e}") from e
        

        try:
            validated_df, validation_log_df = validator.execute(df_transformed, engine=self.engine, config=self.config)
        except Exception as e:
            raise Exception(f"Falha no passo de VALIDAÇÃO do pipeline '{self.config.pipeline_name}': {e}") from e

        try:
            writer.execute(validated_df, engine=self.engine, config=self.config, validation_log_df=validation_log_df)
        except Exception as e:
            raise Exception(f"Falha no passo de ESCRITA do pipeline '{self.config.pipeline_name}': {e}") from e

        print(f"--- Pipeline {self.config.pipeline_name} concluído com sucesso. ---")


    def create(self):
        print(f"--- Iniciando criação da tabela para: {self.config.pipeline_name} ---")
        self.engine.create_table(self.config)
        print("--- Criação da tabela concluída. ---")

    def update(self):
        print(f"--- Iniciando atualização da tabela para: {self.config.pipeline_name} ---")
        self.engine.update_table(self.config)
        print("--- Atualização da tabela concluída. ---")

    def test(self):
        """Executa o pipeline em modo de teste."""
        if not self.config.test:
            print("Erro: Configuração de teste ('test:') não encontrada no arquivo YAML.")
            raise ValueError("Configuração de teste ausente.")

        print(f"--- Iniciando modo de teste para o pipeline: {self.config.pipeline_name} ---")
        
        # Cria uma cópia da configuração e substitui o caminho da fonte pelos dados de teste
        test_run_config = self.config.copy(deep=True)
        test_run_config.source.path = self.config.test.source_data_path
        
        # Executa os passos de leitura, transformação e validação com os dados de teste
        reader = ReadStep()
        transformer = TransformStep()
        validator = ValidateStep()

        df_source = reader.execute(engine=self.engine, config=test_run_config)
        df_transformed = transformer.execute(df_source, engine=self.engine, config=test_run_config)
        df_actual_output, _ = validator.execute(df_transformed, engine=self.engine, config=test_run_config)

        # Carrega a tabela com os resultados esperados
        print(f"Carregando resultados esperados de: {self.config.test.expected_results_table}")
        df_expected_output = self.engine.read_table(self.config.test.expected_results_table)

        # Remove colunas voláteis (como timestamps) que podem variar a cada execução
        volatile_cols = ["updated_at", "created_at", "start_date", "end_date", "log_timestamp"]
        actual_to_compare = df_actual_output.drop(*[c for c in volatile_cols if c in df_actual_output.columns])
        expected_to_compare = df_expected_output.drop(*[c for c in volatile_cols if c in df_expected_output.columns])

        print("Comparando resultado atual com o esperado...")
        are_equal = self.engine.compare_dataframes(actual_to_compare, expected_to_compare)

        if are_equal:
            print("TESTE PASSOU: O resultado atual corresponde ao esperado.")
        else:
            print("TESTE FALHOU: O resultado atual é diferente do esperado.")
            self.engine.show_differences(actual_to_compare, expected_to_compare)
            raise AssertionError("O resultado do teste não corresponde ao esperado.")
