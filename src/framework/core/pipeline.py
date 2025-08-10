from .engine import BaseEngine
from ..models.pydantic_models import PipelineConfig
from ..steps.reader import ReadStep
from ..steps.transformer import TransformStep
from ..steps.validator import ValidateStep
from ..steps.writer import WriterStep

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

        df = reader.execute(engine=self.engine, config=self.config)
        transformed_df = transformer.execute(df, engine=self.engine, config=self.config)
        validated_df, validation_log_df = validator.execute(transformed_df, engine=self.engine, config=self.config)
        writer.execute(validated_df, engine=self.engine, config=self.config, validation_log_df=validation_log_df)
        print(f"--- Pipeline {self.config.pipeline_name} concluído com sucesso. ---")

    def create(self):
        print(f"--- Iniciando criação da tabela para: {self.config.pipeline_name} ---")
        self.engine.create_table(self.config)
        print("--- Criação da tabela concluída. ---")

    def update(self):
        print(f"--- Iniciando atualização da tabela para: {self.config.pipeline_name} ---")
        self.engine.update_table(self.config)
        print("--- Atualização da tabela concluída. ---")

    # --- NOVO ---
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

        # Compara o resultado atual com o esperado
        print("Comparando resultado atual com o esperado...")
        are_equal = self.engine.compare_dataframes(df_actual_output, df_expected_output)

        if are_equal:
            print("TESTE PASSOU: O resultado atual corresponde ao esperado.")
        else:
            print("TESTE FALHOU: O resultado atual é diferente do esperado.")
            self.engine.show_differences(df_actual_output, df_expected_output)
            raise AssertionError("O resultado do teste não corresponde ao esperado.")
