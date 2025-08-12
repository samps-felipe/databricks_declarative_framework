from pyspark.sql import DataFrame, functions as F
from ..core.step import BaseStep
from ..core.engine import BaseEngine
from ..models.pydantic_models import PipelineConfig
from ..quality import get_rule_class
from ..exceptions import ValidationError, ConfigurationError
from ..logger import get_logger

logger = get_logger(__name__)

class ValidateStep(BaseStep):
    """
    Orchestrates the execution of column and table validations.
    """
    def _parse_rule(self, rule_string: str) -> tuple:
        """Extracts rule name and parameter. Ex: 'pattern:regex' -> ('pattern', 'regex')"""
        parts = rule_string.split(':', 1)
        return parts[0], parts[1] if len(parts) > 1 else None

    def _create_validator(self, rule_name: str, param: str):
        """Creates a validator instance using the registry."""
        rule_class = get_rule_class(rule_name)
        if not rule_class:
            logger.warning(f"Unknown validation rule '{rule_name}'. Skipping.")
            return None
        
        # This is a simplified way to pass parameters. 
        # It assumes that if a parameter exists, the class constructor can accept it as a keyword argument.
        try:
            if param:
                # Handles rules like pattern, isin, etc.
                # The constructor in the rule class must match the parameter name.
                # e.g., PatternValidation(pattern=param) is not ideal.
                # A better approach is to pass the param directly and let the class handle it.
                # Let's create a dictionary of parameters.
                # For now, this is a simple approach that works for the current rules.
                # A more robust solution would involve a more sophisticated parameter mapping.
                if rule_name == 'pattern':
                    return rule_class(pattern=param)
                elif rule_name == 'isin':
                    return rule_class(allowed_values_str=param)
                elif rule_name == 'greater_than_or_equal_to':
                    return rule_class(value_str=param)
                elif rule_name == 'isbetween':
                    return rule_class(bounds_str=param)
            else:
                # Handles rules like not_null
                return rule_class()
        except (TypeError, ValueError) as e:
            raise ConfigurationError(f"Failed to instantiate rule '{rule_name}' with param '{param}'. Error: {e}") from e

        return None # Should not be reached

    def execute(self, df: DataFrame, engine: BaseEngine, config: PipelineConfig) -> (DataFrame, DataFrame):
        logger.info("--- Step: Validation ---")
        df_to_validate = df
        all_failures_list = []

        # --- 1. PROCESS 'WARN' VALIDATIONS ---
        logger.info("Executing 'warn' validations...")
        for spec in config.columns:
            final_column_name = engine._get_final_column_name(spec, config.defaults)
            for validation in [v for v in spec.validation_rules if v.on_fail == 'warn']:
                rule_name, param = self._parse_rule(validation.rule)
                validator = self._create_validator(rule_name, param)
                if validator:
                    failures_df, _ = validator.apply(df_to_validate, final_column_name)
                    if failures_df.count() > 0:
                        logger.warning(f"{failures_df.count()} records failed rule '{validation.rule}' for column '{final_column_name}'.")
                        
                        log_select_exprs = [
                            F.lit(config.pipeline_name).alias("pipeline_name"),
                            F.lit(validation.rule).alias("validation_rule"),
                            F.lit(final_column_name).alias("failed_column"),
                            F.col(final_column_name).cast("string").alias("failed_value"),
                            F.current_timestamp().alias("log_timestamp")
                        ]

                        if "hash_key" in failures_df.columns:
                            log_select_exprs.append(F.col("hash_key"))
                        else:
                            log_select_exprs.append(F.lit(None).cast("string").alias("hash_key"))
                        
                        log_info_df = failures_df.select(*log_select_exprs)
                        all_failures_list.append(log_info_df)

        # --- 2. PROCESS 'DROP' VALIDATIONS ---
        logger.info("Executing 'drop' validations...")
        for spec in config.columns:
            final_column_name = engine._get_final_column_name(spec, config.defaults)
            for validation in [v for v in spec.validation_rules if v.on_fail == 'drop']:
                rule_name, param = self._parse_rule(validation.rule)
                validator = self._create_validator(rule_name, param)
                if validator:
                    failures_df, success_df = validator.apply(df_to_validate, final_column_name)
                    if failures_df.count() > 0:
                        logger.warning(f"{failures_df.count()} records dropped by rule '{validation.rule}' on column '{final_column_name}'.")
                        df_to_validate = success_df

        # --- 3. PROCESS 'FAIL' VALIDATIONS ---
        logger.info("Executing 'fail' validations...")
        for spec in config.columns:
            final_column_name = engine._get_final_column_name(spec, config.defaults)
            for validation in [v for v in spec.validation_rules if v.on_fail == 'fail']:
                rule_name, param = self._parse_rule(validation.rule)
                validator = self._create_validator(rule_name, param)
                if validator:
                    failures_df, _ = validator.apply(df_to_validate, final_column_name)
                    if failures_df.count() > 0:
                        msg = f"{failures_df.count()} records failed critical rule '{validation.rule}' for column '{final_column_name}'."
                        logger.error(msg)
                        failures_df.show()
                        raise ValidationError(msg)

        # --- 4. TABLE VALIDATIONS ---
        logger.info("Executing table validations...")
        for table_val in config.table_validations:
            rule_class = get_rule_class(table_val.type)
            if rule_class:
                validator = rule_class(columns=table_val.columns)
                try:
                    validator.apply(df_to_validate)
                except ValueError as e:
                    logger.error(f"Table validation '{table_val.type}' failed. Action: {table_val.on_fail}", exc_info=True)
                    if table_val.on_fail == 'fail':
                        raise ValidationError(f"Table validation '{table_val.type}' failed.") from e
                    else:
                        logger.warning(f"Table validation '{table_val.type}' failed: {e}")

        # --- 5. CONSOLIDATE FAILURE LOGS ---
        final_log_df = None
        if all_failures_list:
            logger.info("Consolidating validation logs...")
            from functools import reduce
            
            base_log_schema = ["pipeline_name", "validation_rule", "failed_column", "failed_value", "log_timestamp", "hash_key"]
            
            def standardize_df(df_to_std):
                for col_name in base_log_schema:
                    if col_name not in df_to_std.columns:
                        df_to_std = df_to_std.withColumn(col_name, F.lit(None))
                return df_to_std.select(*base_log_schema)

            standardized_dfs = [standardize_df(df) for df in all_failures_list]
            final_log_df = reduce(lambda df1, df2: df1.unionByName(df2), standardized_dfs)

        logger.info("Validations completed.")
        return df_to_validate, final_log_df
