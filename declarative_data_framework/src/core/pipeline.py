from importlib import import_module
from .engine import BaseEngine
from ..models.pydantic_models import PipelineConfig
from ..steps.reader import ReadStep
from ..steps.transformer import TransformStep
from ..steps.validator import ValidateStep
from ..steps.writer import WriterStep
from ..logger import get_logger
from ..exceptions import StepExecutionError, PipelineError, ConfigurationError

logger = get_logger(__name__)

class Pipeline:
    def __init__(self, config: PipelineConfig, engine: BaseEngine):
        self.config = config
        self.engine = engine
        self.logger = get_logger(f"pipeline.{self.config.pipeline_name}")

    def run(self):
        self.logger.info(f"Starting execution for pipeline: {self.config.pipeline_name}")
        reader = ReadStep()
        transformer = TransformStep()
        validator = ValidateStep()
        writer = WriterStep()

        df_transformed = None

        try:
            if self.config.pipeline_type == 'silver':
                df_source = reader.execute(engine=self.engine, config=self.config)
            else: # Gold
                df_source = None # Gold does not read from files
        except Exception as e:
            self.logger.exception("Failure in READ step for pipeline '%s'", self.config.pipeline_name)
            raise StepExecutionError(f"Failure in READ step: {e}") from e
        
        try:
            if self.config.custom_transform_script:
                self.logger.info(f"Executing custom transform script: {self.config.custom_transform_script}")
                module_path, func_name = self.config.custom_transform_script.rsplit('.', 1)
                custom_module = import_module(module_path)
                custom_transform_func = getattr(custom_module, func_name)
                df_transformed = custom_transform_func(df_source, self.engine, self.config)
            else:
                df_transformed = transformer.execute(df_source, engine=self.engine, config=self.config)
        except Exception as e:
            self.logger.exception("Failure in TRANSFORM step for pipeline '%s'", self.config.pipeline_name)
            raise StepExecutionError(f"Failure in TRANSFORM step: {e}") from e

        try:
            validated_df, validation_log_df = validator.execute(df_transformed, engine=self.engine, config=self.config)
        except Exception as e:
            self.logger.exception("Failure in VALIDATION step for pipeline '%s'", self.config.pipeline_name)
            raise StepExecutionError(f"Failure in VALIDATION step: {e}") from e

        try:
            writer.execute(validated_df, engine=self.engine, config=self.config, validation_log_df=validation_log_df)
        except Exception as e:
            self.logger.exception("Failure in WRITE step for pipeline '%s'", self.config.pipeline_name)
            raise StepExecutionError(f"Failure in WRITE step: {e}") from e

        self.logger.info(f"Pipeline {self.config.pipeline_name} finished successfully.")


    def create(self):
        self.logger.info(f"Starting table creation for: {self.config.pipeline_name}")
        self.engine.create_table(self.config)
        self.logger.info("Table creation finished.")

    def update(self):
        self.logger.info(f"Starting table update for: {self.config.pipeline_name}")
        self.engine.update_table(self.config)
        self.logger.info("Table update finished.")

    def test(self):
        """Executes the pipeline in test mode."""
        if not self.config.test:
            self.logger.error("Test configuration ('test:') not found in YAML file.")
            raise ConfigurationError("Test configuration is missing.")

        self.logger.info(f"Starting test mode for pipeline: {self.config.pipeline_name}")
        
        test_run_config = self.config.copy(deep=True)
        test_run_config.source.path = self.config.test.source_data_path
        
        reader = ReadStep()
        transformer = TransformStep()
        validator = ValidateStep()

        self.logger.debug("Executing steps with test data...")
        df_source = reader.execute(engine=self.engine, config=test_run_config)
        df_transformed = transformer.execute(df_source, engine=self.engine, config=test_run_config)
        df_actual_output, _ = validator.execute(df_transformed, engine=self.engine, config=test_run_config)

        self.logger.info(f"Loading expected results from: {self.config.test.expected_results_table}")
        df_expected_output = self.engine.read_table(self.config.test.expected_results_table)

        volatile_cols = ["updated_at", "created_at", "start_date", "end_date", "log_timestamp"]
        actual_to_compare = df_actual_output.drop(*[c for c in volatile_cols if c in df_actual_output.columns])
        expected_to_compare = df_expected_output.drop(*[c for c in volatile_cols if c in df_expected_output.columns])

        self.logger.info("Comparing actual result with expected result...")
        are_equal = self.engine.compare_dataframes(actual_to_compare, expected_to_compare)

        if are_equal:
            self.logger.info("TEST PASSED: The actual result matches the expected result.")
        else:
            self.logger.error("TEST FAILED: The actual result is different from the expected result.")
            self.engine.show_differences(actual_to_compare, expected_to_compare)
            raise AssertionError("The test result does not match the expected result.")
