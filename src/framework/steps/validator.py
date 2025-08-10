from pyspark.sql import DataFrame, functions as F
from ..core.step import BaseStep
from ..core.engine import BaseEngine
from ..models.pydantic_models import PipelineConfig
from . import validation_rules

class ValidateStep(BaseStep):
    """
    Passo que orquestra a execução de validações de coluna e de tabela.
    A execução é ordenada por tipo de ação (warn, drop, fail) para garantir um comportamento previsível.
    """
    def _parse_rule(self, rule_string: str) -> tuple:
        """Extrai o nome da regra e o parâmetro. Ex: 'pattern:regex' -> ('pattern', 'regex')"""
        parts = rule_string.split(':', 1)
        return parts[0], parts[1] if len(parts) > 1 else None

    def _get_validator(self, rule_name: str, param: str):
        """Fábrica de validadores para instanciar a classe de regra correta."""
        if rule_name == "not_null":
            return validation_rules.NotNullValidation()
        elif rule_name == "pattern":
            return validation_rules.PatternValidation(pattern=param)
        elif rule_name == "isin":
            return validation_rules.IsInValidation(allowed_values_str=param)
        elif rule_name == "greater_than_or_equal_to":
            return validation_rules.GreaterThanOrEqualToValidation(value_str=param)
        elif rule_name == "isbetween":
            return validation_rules.IsBetweenValidation(bounds_str=param)
        return None

    def execute(self, df: DataFrame, engine: BaseEngine, config: PipelineConfig) -> (DataFrame, DataFrame):
        print("--- Passo: Validação ---")
        df_to_validate = df
        all_failures_list = []

        # --- 1. PROCESSAR VALIDAÇÕES 'WARN' ---
        print("Executando validações 'warn'...")
        for spec in config.columns:
            final_column_name = engine._get_final_column_name(spec, config.defaults)
            for validation in [v for v in spec.validation_rules if v.on_fail == 'warn']:
                rule_name, param = self._parse_rule(validation.rule)
                validator = self._get_validator(rule_name, param)
                if validator:
                    failures_df, _ = validator.apply(df_to_validate, final_column_name)
                    # --- CORREÇÃO APLICADA AQUI ---
                    if failures_df.count() > 0:
                        print(f"  -> [AVISO] {failures_df.count()} registros falharam na regra '{validation.rule}' para a coluna '{final_column_name}'.")
                        pk_cols = [engine._get_final_column_name(c, config.defaults) for c in config.columns if c.pk]
                        log_info_df = failures_df.select(
                            *pk_cols, F.col(final_column_name).alias("failed_value")
                        ).withColumn("pipeline_name", F.lit(config.pipeline_name)) \
                         .withColumn("validation_rule", F.lit(validation.rule)) \
                         .withColumn("failed_column", F.lit(final_column_name)) \
                         .withColumn("log_timestamp", F.current_timestamp())
                        all_failures_list.append(log_info_df)

        # --- 2. PROCESSAR VALIDAÇÕES 'DROP' ---
        print("Executando validações 'drop'...")
        for spec in config.columns:
            final_column_name = engine._get_final_column_name(spec, config.defaults)
            for validation in [v for v in spec.validation_rules if v.on_fail == 'drop']:
                rule_name, param = self._parse_rule(validation.rule)
                validator = self._get_validator(rule_name, param)
                if validator:
                    failures_df, success_df = validator.apply(df_to_validate, final_column_name)
                    # --- CORREÇÃO APLICADA AQUI ---
                    if failures_df.count() > 0:
                        print(f"  -> [DROP] {failures_df.count()} registros removidos pela regra '{validation.rule}' na coluna '{final_column_name}'.")
                        df_to_validate = success_df

        # --- 3. PROCESSAR VALIDAÇÕES 'FAIL' ---
        print("Executando validações 'fail'...")
        for spec in config.columns:
            final_column_name = engine._get_final_column_name(spec, config.defaults)
            for validation in [v for v in spec.validation_rules if v.on_fail == 'fail']:
                rule_name, param = self._parse_rule(validation.rule)
                validator = self._get_validator(rule_name, param)
                if validator:
                    failures_df, _ = validator.apply(df_to_validate, final_column_name)
                    # --- CORREÇÃO APLICADA AQUI ---
                    if failures_df.count() > 0:
                        print(f"  -> [FALHA] {failures_df.count()} registros falharam na regra crítica '{validation.rule}' para a coluna '{final_column_name}'.")
                        failures_df.show()
                        raise Exception(f"Validação crítica '{validation.rule}' falhou para a coluna '{final_column_name}'.")

        # --- 4. VALIDAÇÕES DE TABELA ---
        print("Executando validações de tabela...")
        for table_val in config.table_validations:
            validator = None
            if table_val.type == "duplicate_check":
                validator = validation_rules.DuplicateCheckValidation(columns=table_val.columns)

            if validator:
                try:
                    validator.apply(df_to_validate)
                except ValueError as e:
                    print(f"  -> Falha na validação de tabela '{table_val.type}'. Ação: {table_val.on_fail}")
                    if table_val.on_fail == 'fail':
                        raise e
                    else:
                        print(f"  [AVISO] {e}")

        # --- 5. CONSOLIDAÇÃO DO LOG DE FALHAS ---
        final_log_df = None
        if all_failures_list:
            print("Consolidando logs de validação...")
            from functools import reduce
            final_log_df = reduce(lambda df1, df2: df1.unionByName(df2, allowMissingColumns=True), all_failures_list)

        print("Validações concluídas.")
        return df_to_validate, final_log_df
