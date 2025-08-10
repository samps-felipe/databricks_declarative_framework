from pyspark.sql import DataFrame, functions as F
from ..core.step import BaseStep
from ..core.engine import BaseEngine
from ..models.pydantic_models import PipelineConfig
from . import validation_rules # Importa o novo módulo de regras

class ValidateStep(BaseStep):
    """
    Passo que orquestra a execução de validações de coluna e de tabela.
    Ele usa um padrão de fábrica (factory) para instanciar a classe de validação correta.
    """
    def _parse_rule(self, rule_string: str) -> tuple:
        """Extrai o nome da regra e o parâmetro. Ex: 'pattern:regex' -> ('pattern', 'regex')"""
        parts = rule_string.split(':', 1)
        return parts[0], parts[1] if len(parts) > 1 else None

    def execute(self, df: DataFrame, engine: BaseEngine, config: PipelineConfig) -> (DataFrame, DataFrame):
        print("--- Passo: Validação ---")
        df_to_validate = df
        all_failures_list = []

        # --- 1. VALIDAÇÕES DE COLUNA ---
        print("Executando validações de coluna...")
        for spec in config.columns:
            final_column_name = engine._get_final_column_name(spec, config.defaults)
            for validation in spec.validate:
                rule_name, param = self._parse_rule(validation.rule)
                
                validator = None
                if rule_name == "not_null":
                    validator = validation_rules.NotNullValidation()
                elif rule_name == "pattern":
                    validator = validation_rules.PatternValidation(pattern=param)
                elif rule_name == "isin":
                    validator = validation_rules.IsInValidation(allowed_values_str=param)
                elif rule_name == "greater_than_or_equal_to":
                    validator = validation_rules.GreaterThanOrEqualToValidation(value_str=param)
                elif rule_name == "isbetween":
                    validator = validation_rules.IsBetweenValidation(bounds_str=param)
                # Para adicionar uma nova validação de coluna, adicione um 'elif' aqui.

                if validator:
                    failures_df, success_df = validator.apply(df_to_validate, final_column_name)
                    
                    if failures_df.count() > 0:
                        print(f"  -> {failures_df.count()} registros falharam na regra '{validation.rule}' para a coluna '{final_column_name}'. Ação: {validation.on_fail}")
                        if validation.on_fail == 'fail':
                            failures_df.show()
                            raise Exception(f"Validação crítica '{validation.rule}' falhou para a coluna '{final_column_name}'.")
                        elif validation.on_fail == 'drop':
                            df_to_validate = success_df # Continua o pipeline apenas com os registros válidos
                        elif validation.on_fail == 'warn':
                            # Adiciona informações de log e acumula as falhas
                            log_info_df = failures_df.withColumn("pipeline_name", F.lit(config.pipeline_name)) \
                                                     .withColumn("validation_rule", F.lit(validation.rule)) \
                                                     .withColumn("failed_column", F.lit(final_column_name)) \
                                                     .withColumn("log_timestamp", F.current_timestamp())
                            all_failures_list.append(log_info_df)

        # --- 2. VALIDAÇÕES DE TABELA ---
        print("Executando validações de tabela...")
        for table_val in config.table_validations:
            validator = None
            if table_val.type == "duplicate_check":
                validator = validation_rules.DuplicateCheckValidation(columns=table_val.columns)
            # Para adicionar uma nova validação de tabela, adicione um 'elif' aqui.

            if validator:
                try:
                    validator.apply(df_to_validate)
                except ValueError as e:
                    print(f"  -> Falha na validação de tabela '{table_val.type}'. Ação: {table_val.on_fail}")
                    if table_val.on_fail == 'fail':
                        raise e # Lança a exceção para parar o pipeline
                    else: # warn
                        print(f"  [AVISO] {e}")

        # --- 3. CONSOLIDAÇÃO DO LOG DE FALHAS ---
        final_log_df = None
        if all_failures_list:
            print("Consolidando logs de validação...")
            # Usa unionByName para juntar DataFrames de falhas que podem ter schemas diferentes
            from functools import reduce
            final_log_df = reduce(lambda df1, df2: df1.unionByName(df2, allowMissingColumns=True), all_failures_list)

        print("Validações concluídas.")
        return df_to_validate, final_log_df